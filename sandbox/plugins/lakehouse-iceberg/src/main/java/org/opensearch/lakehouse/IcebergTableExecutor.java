/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.exec.ExternalScanContext;
import org.opensearch.analytics.exec.ExternalTableExecutor;
import org.opensearch.analytics.schema.ExternalTable;
import org.opensearch.lakehouse.catalog.AwsCredentials;
import org.opensearch.lakehouse.catalog.CatalogConfig;
import org.opensearch.lakehouse.catalog.IcebergCatalogConnector;
import org.opensearch.lakehouse.catalog.LakehouseCredentialsProvider;
import org.opensearch.lakehouse.distributed.DistributedQueryCoordinator;
import org.opensearch.lakehouse.distributed.PhysicalPlanSplitter;
import org.opensearch.lakehouse.scan.CalciteToIcebergPredicateConverter;
import org.opensearch.lakehouse.scan.IcebergScanPlan;
import org.opensearch.lakehouse.schema.IcebergCalciteTable;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.opensearch.lakehouse.exec.DataFusionSqlDialect;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Prepares Iceberg scan contexts for the analytics engine.
 *
 * <p>Discovered via SPI ({@code META-INF/services/ExternalTableExecutor}).
 * Called by {@code DefaultPlanExecutor} when a query references an external table.
 */
public class IcebergTableExecutor implements ExternalTableExecutor {

    private static final Logger logger = LogManager.getLogger(IcebergTableExecutor.class);

    /** No-arg constructor required by SPI ({@code SPIClassIterator}). */
    public IcebergTableExecutor() {}

    @Override
    @SuppressWarnings("removal")
    public ExternalScanContext prepareScan(RelNode logicalPlan, ExternalTable externalTable) {
        long t0 = System.nanoTime();
        if (!(externalTable instanceof IcebergCalciteTable)) {
            throw new IllegalArgumentException("Expected IcebergCalciteTable but got: " + externalTable.getClass().getSimpleName());
        }
        IcebergCalciteTable icebergTable = (IcebergCalciteTable) externalTable;
        Table table = icebergTable.getIcebergTable();
        logger.info("[IcebergTableExecutor] ========== PREPARE SCAN START ==========");
        logger.info("[IcebergTableExecutor] Table: {}", table.name());
        logger.info("[IcebergTableExecutor] Calcite logical plan:\n{}", logicalPlan.explain());

        IcebergCatalogConnector connector = LakehouseState.instance().catalogConnector();

        // 1. Extract Iceberg predicates for manifest-level pruning
        Expression filterExpr = extractIcebergFilter(logicalPlan);
        List<Expression> predicates = filterExpr != null ? List.of(filterExpr) : List.of();
        logger.debug("[IcebergTableExecutor] Iceberg filter expression: {}", filterExpr != null ? filterExpr : "none (full scan)");

        // 2. Plan scan — resolves manifests to pruned S3 Parquet file paths.
        //    Set per-catalog credentials on ThreadLocal so LakehouseCredentialsProvider
        //    can return them to the Iceberg SDK. The privileged executor propagates
        //    the ThreadLocal to executor threads for parallel manifest reads.
        String scanCatalogName = icebergTable.getCatalogConfig() != null
            ? icebergTable.getCatalogConfig().catalogName() : null;
        if (scanCatalogName != null) {
            connector.setCredentialsOnThread(scanCatalogName);
        }
        IcebergScanPlan scanPlan;
        try {
            @SuppressWarnings("removal")
            IcebergScanPlan plan = AccessController.doPrivileged(
                (PrivilegedAction<IcebergScanPlan>) () -> LakehouseState.instance().scanPlanner().planScan(
                    table,
                    icebergTable.getPinnedSnapshotId(),
                    predicates,
                    null  // all columns — DataFusion handles projection from SQL query
                )
            );
            scanPlan = plan;
        } finally {
            if (scanCatalogName != null) {
                connector.clearCredentialsOnThread();
            }
        }
        logger.info("[IcebergTableExecutor] Scan plan: {} files, {} bytes total",
            scanPlan.fileCount(), scanPlan.getTotalFileSize());
        if (logger.isDebugEnabled()) {
            List<String> filePaths = scanPlan.getDataFilePaths();
            logger.debug("[IcebergTableExecutor] Scan plan file paths ({}):", filePaths.size());
            for (int i = 0; i < filePaths.size(); i++) {
                logger.debug("[IcebergTableExecutor]   [{}] {}", i, filePaths.get(i));
            }
        }

        // 3. Extract table name from the Calcite plan
        String tableName = extractTableName(logicalPlan);

        // 4. Convert Calcite RelNode to DataFusion SQL
        String sqlQuery;
        try {
            SqlDialect dialect = DataFusionSqlDialect.DEFAULT;
            RelToSqlConverter converter = new RelToSqlConverter(dialect);
            SqlNode sqlNode = converter.visitRoot(logicalPlan).asStatement();
            sqlQuery = sqlNode.toSqlString(dialect).getSql();
            // RelToSqlConverter may produce schema-qualified names (e.g. "opensearch"."nyc_taxi")
            // but DataFusion registers tables with just the leaf name. Replace all qualified refs.
            sqlQuery = stripSchemaQualifiers(sqlQuery, tableName);
            logger.debug("[IcebergTableExecutor] Generated SQL for DataFusion: {}", sqlQuery);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert query plan to SQL", e);
        }

        // 5. Build storage config from CatalogConfig
        Map<String, String> storageConfig = buildStorageConfig(connector, icebergTable, scanPlan);

        logger.debug("[IcebergTableExecutor] Storage config: region={}, bucket={}, credentials={}, endpoint={}",
            storageConfig.get("s3Region"), storageConfig.get("s3Bucket"),
            storageConfig.containsKey("s3AccessKeyId") ? "present" : "absent",
            storageConfig.getOrDefault("s3Endpoint", "default"));
        logger.debug("[IcebergTableExecutor] ExternalScanContext: table={}, files={}, sqlQuery={}, storageConfigKeys={}",
            tableName, scanPlan.getDataFilePaths().size(), sqlQuery, storageConfig.keySet());

        ExternalScanContext scanContext = new ExternalScanContext(tableName, scanPlan.getDataFilePaths(), sqlQuery, storageConfig);

        // 6. Analyze query for distributed execution via PhysicalPlanSplitter
        DistributedQueryCoordinator coordinator = LakehouseState.instance().distributedCoordinator();
        if (coordinator != null && coordinator.shouldDistribute(scanPlan.getFiles())) {
            try {
                PhysicalPlanSplitter.SplitPlan splitPlan = PhysicalPlanSplitter.split(logicalPlan, tableName);
                logger.debug("[IcebergTableExecutor] Split plan: {}", splitPlan);

                if (splitPlan.canDistribute()) {
                    logger.info("[IcebergTableExecutor] Using distributed execution for {} files: workerSql={}, coordinatorSql={}",
                        scanPlan.fileCount(), splitPlan.getWorkerSql(), splitPlan.getCoordinatorSql());
                    Iterable<Object[]> distributedResults = coordinator.execute(
                        splitPlan, scanPlan.getFiles(), storageConfig, tableName
                    );
                    scanContext.setPreComputedResults(distributedResults);
                    logger.info("[IcebergTableExecutor] Distributed execution completed successfully");
                } else {
                    logger.info("[IcebergTableExecutor] Query cannot be distributed (unsupported aggregates), using single-node execution");
                }
            } catch (Exception e) {
                logger.warn("[IcebergTableExecutor] Distributed execution failed, falling back to single-node: {}", e.getMessage(), e);
            }
        }

        return scanContext;
    }

    private Expression extractIcebergFilter(RelNode node) {
        // Only push down filters directly above a table scan (WHERE clauses).
        // HAVING filters sit above aggregates and reference computed columns
        // (e.g. "cnt") that don't exist in the Iceberg table schema.
        if (node instanceof Filter) {
            Filter filter = (Filter) node;
            if (filter.getInput() instanceof org.apache.calcite.rel.core.TableScan) {
                RelDataType inputRowType = filter.getInput().getRowType();
                return CalciteToIcebergPredicateConverter.convert(filter.getCondition(), inputRowType);
            }
        }
        for (RelNode input : node.getInputs()) {
            Expression result = extractIcebergFilter(input);
            if (result != null) return result;
        }
        return null;
    }

    /**
     * Strips schema qualifiers from the generated SQL so table references match
     * the leaf name registered in DataFusion. PPL wraps tables under "opensearch"
     * schema, producing {@code "opensearch"."nyc_taxi"} — DataFusion only knows "nyc_taxi".
     */
    private String stripSchemaQualifiers(String sql, String tableName) {
        String quotedTable = "\"" + tableName + "\"";
        // "schema"."table" → "table"
        sql = sql.replaceAll("\"\\w+\"\\." + java.util.regex.Pattern.quote(quotedTable), quotedTable);
        return sql;
    }

    private String extractTableName(RelNode node) {
        if (node instanceof org.apache.calcite.rel.core.TableScan) {
            List<String> qn = node.getTable().getQualifiedName();
            return qn.get(qn.size() - 1);
        }
        for (RelNode input : node.getInputs()) {
            String name = extractTableName(input);
            if (name != null) return name;
        }
        throw new IllegalArgumentException("No TableScan found in plan");
    }

    private Map<String, String> buildStorageConfig(
        IcebergCatalogConnector connector, IcebergCalciteTable icebergTable, IcebergScanPlan scanPlan
    ) {
        Map<String, String> config = new HashMap<>();
        CatalogConfig catalogConfig = icebergTable.getCatalogConfig();
        if (catalogConfig != null) {
            if (catalogConfig.region() != null) config.put("s3Region", catalogConfig.region());
        }
        // Extract bucket from first file path
        List<String> paths = scanPlan.getDataFilePaths();
        if (!paths.isEmpty()) {
            String firstPath = paths.get(0);
            if (firstPath.startsWith("s3://")) {
                String withoutScheme = firstPath.substring(5);
                int slashIdx = withoutScheme.indexOf('/');
                if (slashIdx > 0) config.put("s3Bucket", withoutScheme.substring(0, slashIdx));
            }
        }
        // For file:// paths (local testing), set endpoint for local S3
        if (!paths.isEmpty() && paths.get(0).startsWith("file://")) {
            config.put("s3Endpoint", "file://");
        }
        // Pass per-catalog AWS credentials to DataFusion's Rust S3 client
        if (catalogConfig != null) {
            AwsCredentials creds = connector.getCredentials(catalogConfig.catalogName());
            if (creds != null && creds.isComplete()) {
                config.put("s3AccessKeyId", creds.getAccessKeyId());
                config.put("s3SecretAccessKey", creds.getSecretAccessKey());
                if (creds.getSessionToken() != null) {
                    config.put("s3SessionToken", creds.getSessionToken());
                }
                logger.debug("[IcebergTableExecutor] Passing per-catalog credentials for [{}] to DataFusion",
                    catalogConfig.catalogName());
            } else {
                logger.warn("[IcebergTableExecutor] No credentials found for catalog [{}] — DataFusion will use default credentials",
                    catalogConfig.catalogName());
            }
        }
        return config;
    }

    @SuppressWarnings("unchecked")
    private <T> T findNode(RelNode node, Class<T> clazz) {
        if (clazz.isInstance(node)) {
            return (T) node;
        }
        for (RelNode input : node.getInputs()) {
            T found = findNode(input, clazz);
            if (found != null) {
                return found;
            }
        }
        return null;
    }
}

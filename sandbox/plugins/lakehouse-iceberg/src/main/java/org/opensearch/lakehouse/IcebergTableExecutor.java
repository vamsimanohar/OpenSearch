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
import org.opensearch.lakehouse.distributed.MultiStageCoordinator;
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
        long tTotal0 = System.nanoTime();
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
        long tManifest0 = System.nanoTime();
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
        long tManifest1 = System.nanoTime();
        logger.info("[IcebergTableExecutor] [TIMING] Manifest scan: {} ms — {} files, {} bytes total",
            (tManifest1 - tManifest0) / 1_000_000, scanPlan.fileCount(), scanPlan.getTotalFileSize());
        if (logger.isDebugEnabled()) {
            List<String> filePaths = scanPlan.getDataFilePaths();
            logger.debug("[IcebergTableExecutor] Scan plan file paths ({}):", filePaths.size());
            for (int i = 0; i < filePaths.size(); i++) {
                logger.debug("[IcebergTableExecutor]   [{}] {}", i, filePaths.get(i));
            }
        }

        // 3. Extract table name from the Calcite plan
        String tableName = extractTableName(logicalPlan);

        // 4. Build storage config from CatalogConfig (needed by both paths)
        Map<String, String> storageConfig = buildStorageConfig(connector, icebergTable, scanPlan);

        logger.debug("[IcebergTableExecutor] Storage config: region={}, bucket={}, credentials={}, endpoint={}",
            storageConfig.get("s3Region"), storageConfig.get("s3Bucket"),
            storageConfig.containsKey("s3AccessKeyId") ? "present" : "absent",
            storageConfig.getOrDefault("s3Endpoint", "default"));

        // 5. Try distributed execution first — avoids unnecessary single-node SQL conversion
        DistributedQueryCoordinator coordinator = LakehouseState.instance().distributedCoordinator();
        if (coordinator != null && coordinator.shouldDistribute(scanPlan.getFiles())) {
            try {
                long tSplit0 = System.nanoTime();
                PhysicalPlanSplitter.SplitPlan splitPlan = PhysicalPlanSplitter.split(logicalPlan, tableName);
                long tSplit1 = System.nanoTime();
                logger.info("[IcebergTableExecutor] [TIMING] Plan splitting: {} ms", (tSplit1 - tSplit0) / 1_000_000);
                logger.info("[IcebergTableExecutor] Split plan: canDistribute={}, workerSql={}, coordinatorSql={}",
                    splitPlan.canDistribute(), splitPlan.getWorkerSql(), splitPlan.getCoordinatorSql());

                if (splitPlan.canDistribute()) {
                    logger.info("[IcebergTableExecutor] >>> DISTRIBUTED EXECUTION for {} files across cluster <<<",
                        scanPlan.fileCount());
                    long tDist0 = System.nanoTime();
                    Iterable<Object[]> distributedResults;
                    MultiStageCoordinator multiStage = LakehouseState.instance().multiStageCoordinator();
                    if (multiStage != null) {
                        logger.info("[IcebergTableExecutor] Using multi-stage coordinator (Mini-Trino engine)");
                        distributedResults = multiStage.execute(
                            logicalPlan, tableName, scanPlan.getFiles(), storageConfig, splitPlan
                        );
                    } else {
                        distributedResults = coordinator.execute(
                            splitPlan, scanPlan.getFiles(), storageConfig, tableName
                        );
                    }
                    long tDist1 = System.nanoTime();
                    // Distributed path succeeded — build scanContext with pre-computed results
                    // Use the coordinator SQL as the sqlQuery (it won't be executed again,
                    // but ExternalScanContext requires a non-null value)
                    ExternalScanContext scanContext = new ExternalScanContext(
                        tableName, scanPlan.getDataFilePaths(), splitPlan.getCoordinatorSql(), storageConfig
                    );
                    scanContext.setPreComputedResults(distributedResults);
                    logger.info("[IcebergTableExecutor] [TIMING] Distributed execution: {} ms",
                        (tDist1 - tDist0) / 1_000_000);
                    long tTotal1 = System.nanoTime();
                    logger.info("[IcebergTableExecutor] [TIMING] Total prepareScan: {} ms", (tTotal1 - tTotal0) / 1_000_000);
                    logger.info("[IcebergTableExecutor] ========== PREPARE SCAN END (distributed) ==========");
                    return scanContext;
                } else {
                    logger.info("[IcebergTableExecutor] Query cannot be distributed (unsupported aggregates), falling through to single-node");
                }
            } catch (Exception e) {
                logger.warn("[IcebergTableExecutor] Distributed execution failed, falling back to single-node: {}", e.getMessage(), e);
            }
        } else {
            logger.info("[IcebergTableExecutor] Single-node execution (coordinator={}, files={})",
                coordinator != null, scanPlan.fileCount());
        }

        // 6. Single-node path: convert Calcite RelNode to DataFusion SQL
        //    Only reached if distributed execution was skipped, unsupported, or failed.
        long tSql0 = System.nanoTime();
        String sqlQuery;
        try {
            SqlDialect dialect = DataFusionSqlDialect.DEFAULT;
            RelToSqlConverter converter = new RelToSqlConverter(dialect);
            SqlNode sqlNode = converter.visitRoot(logicalPlan).asStatement();
            sqlQuery = sqlNode.toSqlString(dialect).getSql();
            sqlQuery = stripSchemaQualifiers(sqlQuery, tableName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert query plan to SQL", e);
        }
        long tSql1 = System.nanoTime();
        logger.info("[IcebergTableExecutor] [TIMING] Calcite→SQL conversion (single-node): {} ms — SQL: {}",
            (tSql1 - tSql0) / 1_000_000, sqlQuery);

        ExternalScanContext scanContext = new ExternalScanContext(tableName, scanPlan.getDataFilePaths(), sqlQuery, storageConfig);

        long tTotal1 = System.nanoTime();
        logger.info("[IcebergTableExecutor] [TIMING] Total prepareScan: {} ms", (tTotal1 - tTotal0) / 1_000_000);
        logger.info("[IcebergTableExecutor] ========== PREPARE SCAN END ==========");
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

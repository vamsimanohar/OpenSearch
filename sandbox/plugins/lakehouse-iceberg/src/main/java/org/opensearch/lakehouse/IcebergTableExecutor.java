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
import org.opensearch.lakehouse.distributed.DistributedPlanSplitter;
import org.opensearch.lakehouse.distributed.DistributedQueryCoordinator;
import org.opensearch.lakehouse.distributed.DistributionPlan;
import org.opensearch.lakehouse.scan.CalciteToIcebergPredicateConverter;
import org.opensearch.lakehouse.scan.IcebergScanPlan;
import org.opensearch.lakehouse.schema.IcebergCalciteTable;
import org.opensearch.lakehouse.substrait.CalciteSubstraitConverter;

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
        if (!(externalTable instanceof IcebergCalciteTable)) {
            throw new IllegalArgumentException("Expected IcebergCalciteTable but got: " + externalTable.getClass().getSimpleName());
        }
        IcebergCalciteTable icebergTable = (IcebergCalciteTable) externalTable;
        Table table = icebergTable.getIcebergTable();
        logger.info("[IcebergTableExecutor] Preparing scan for Iceberg table: {}", table.name());
        logger.debug("[IcebergTableExecutor] Calcite logical plan:\n{}", logicalPlan.explain());

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
                    null  // all columns — DataFusion handles projection from Substrait plan
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

        // 3. Convert Calcite RelNode to Substrait bytes
        byte[] substraitBytes;
        try {
            substraitBytes = CalciteSubstraitConverter.toSubstrait(logicalPlan);
            logger.debug("[IcebergTableExecutor] Substrait plan generated: {} bytes", substraitBytes.length);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert query plan to Substrait", e);
        }

        // 4. Build storage config from CatalogConfig
        Map<String, String> storageConfig = buildStorageConfig(connector, icebergTable, scanPlan);

        // 5. Extract table name from the Calcite plan (must match Substrait reference)
        String tableName = extractTableName(logicalPlan);

        logger.debug("[IcebergTableExecutor] Storage config: region={}, bucket={}, credentials={}, endpoint={}",
            storageConfig.get("s3Region"), storageConfig.get("s3Bucket"),
            storageConfig.containsKey("s3AccessKeyId") ? "present" : "absent",
            storageConfig.getOrDefault("s3Endpoint", "default"));
        logger.debug("[IcebergTableExecutor] ExternalScanContext: table={}, files={}, substraitBytes={}, storageConfigKeys={}",
            tableName, scanPlan.getDataFilePaths().size(), substraitBytes.length, storageConfig.keySet());

        ExternalScanContext scanContext = new ExternalScanContext(tableName, scanPlan.getDataFilePaths(), substraitBytes, storageConfig);

        // 6. Analyze query for distribution strategy and check if distributed execution is appropriate
        DistributionPlan distPlan = DistributedPlanSplitter.analyze(logicalPlan);
        logger.debug("[IcebergTableExecutor] Distribution plan: {}", distPlan);

        DistributedQueryCoordinator coordinator = LakehouseState.instance().distributedCoordinator();
        if (coordinator != null && distPlan.getQueryType() != DistributionPlan.QueryType.UNSUPPORTED
            && coordinator.shouldDistribute(scanPlan.getFiles())) {
            logger.info("[IcebergTableExecutor] Using distributed execution for {} files across cluster nodes (plan={})",
                scanPlan.fileCount(), distPlan.getQueryType());
            Iterable<Object[]> distributedResults = coordinator.execute(scanContext, scanPlan.getFiles(), distPlan);
            scanContext.setPreComputedResults(distributedResults);
            logger.info("[IcebergTableExecutor] Distributed execution completed successfully");
        }

        return scanContext;
    }

    private Expression extractIcebergFilter(RelNode node) {
        Filter filter = findNode(node, Filter.class);
        if (filter == null) {
            return null;
        }
        RelDataType inputRowType = filter.getInput().getRowType();
        return CalciteToIcebergPredicateConverter.convert(filter.getCondition(), inputRowType);
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

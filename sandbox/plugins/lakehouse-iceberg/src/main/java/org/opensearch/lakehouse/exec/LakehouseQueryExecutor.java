/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.exec;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.iceberg.expressions.Expression;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.EngineContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.lakehouse.LakehouseState;
import org.opensearch.lakehouse.catalog.CatalogConfig;
import org.opensearch.lakehouse.catalog.IcebergCatalogConnector;
import org.opensearch.lakehouse.distributed.DistributedScanExecutor;
import org.opensearch.lakehouse.scan.IcebergScanPlan;
import org.opensearch.lakehouse.schema.IcebergCalciteTable;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.UnifiedQueryPlanner;
import org.opensearch.sql.executor.QueryType;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Executes SQL and PPL queries against Iceberg tables.
 * <p>
 * Owns the full lakehouse query lifecycle:
 * <ol>
 *   <li>Parse SQL/PPL to Calcite RelNode (via {@link UnifiedQueryPlanner})</li>
 *   <li>Iceberg scan planning (manifest pruning to data file paths)</li>
 *   <li>Convert RelNode to DataFusion SQL</li>
 *   <li>Execute via {@link DistributedScanExecutor} (handles both single-node and multi-node)</li>
 * </ol>
 * <p>
 * Fully asynchronous: results are delivered through {@link ActionListener} callbacks.
 *
 * @opensearch.internal
 */
public class LakehouseQueryExecutor {

    private static final Logger logger = LogManager.getLogger(LakehouseQueryExecutor.class);
    private static final String DEFAULT_CATALOG = "opensearch";

    private final EngineContext engineContext;
    private final DistributedScanExecutor scanExecutor;

    public LakehouseQueryExecutor(EngineContext engineContext, DistributedScanExecutor scanExecutor) {
        this.engineContext = engineContext;
        this.scanExecutor = scanExecutor;
    }

    /**
     * Executes a SQL query asynchronously.
     */
    public void executeSql(String sql, ActionListener<PPLResponse> listener) {
        executeInternal(sql, QueryType.SQL, listener);
    }

    /**
     * Executes a PPL query asynchronously.
     */
    public void executePpl(String ppl, ActionListener<PPLResponse> listener) {
        executeInternal(ppl, QueryType.PPL, listener);
    }

    private void executeInternal(String queryText, QueryType queryType, ActionListener<PPLResponse> listener) {
        long t0 = System.currentTimeMillis();
        SchemaPlus schema = engineContext.getSchema();

        UnifiedQueryContext context = UnifiedQueryContext.builder()
            .language(queryType)
            .catalog(DEFAULT_CATALOG, schema)
            .defaultNamespace(DEFAULT_CATALOG)
            .build();

        try {
            // 1. Parse query to Calcite RelNode (lightweight, sync)
            UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);
            RelNode logicalPlan = planner.plan(queryText);
            long t1 = System.currentTimeMillis();
            logger.info("[PERF] Parse+plan: {}ms", t1 - t0);

            // 2. Extract column names from plan
            List<String> columns = logicalPlan.getRowType().getFieldNames();

            // 3. Execute lakehouse pipeline asynchronously
            executeLakehouse(logicalPlan, ActionListener.wrap(
                result -> {
                    try {
                        List<Object[]> rows = new ArrayList<>();
                        for (Object[] row : result) {
                            rows.add(row);
                        }
                        logger.info("[PERF] Total query: {}ms, {} rows", System.currentTimeMillis() - t0, rows.size());
                        listener.onResponse(new PPLResponse(columns, rows));
                    } catch (Exception e) {
                        listener.onFailure(e);
                    } finally {
                        try { context.close(); } catch (Exception ignored) {}
                    }
                },
                e -> {
                    try { context.close(); } catch (Exception ignored) {}
                    listener.onFailure(e);
                }
            ));
        } catch (Exception e) {
            try { context.close(); } catch (Exception ignored) {}
            if (e instanceof RuntimeException) {
                listener.onFailure(e);
            } else {
                listener.onFailure(new RuntimeException("Failed to plan " + queryType + " query: " + e.getMessage(), e));
            }
        }
    }

    /**
     * Executes the Iceberg scan pipeline asynchronously: predicate pushdown, file pruning,
     * DataFusion SQL generation, distributed/single-node execution.
     * <p>
     * Pipeline: visit plan -> plan scan -> convert SQL -> build config -> execute async.
     */
    @SuppressWarnings("removal")
    void executeLakehouse(RelNode logicalPlan, ActionListener<Iterable<Object[]>> listener) {
        // 1. Visit plan — extract table, filter, and name in one traversal
        IcebergPlanVisitor visitor = new IcebergPlanVisitor();
        visitor.go(logicalPlan);

        IcebergCalciteTable icebergTable = visitor.getIcebergTable();
        if (icebergTable == null) {
            listener.onFailure(new IllegalArgumentException("No Iceberg table found in query plan"));
            return;
        }
        String tableName = visitor.getTableName();
        if (tableName == null) {
            listener.onFailure(new IllegalArgumentException("No TableScan found in plan"));
            return;
        }

        IcebergCatalogConnector connector = LakehouseState.instance().catalogConnector();

        // 2. Plan scan — resolves manifests to pruned data file paths
        Expression filterExpr = visitor.getIcebergFilter();
        List<Expression> predicates = filterExpr != null ? List.of(filterExpr) : List.of();

        CatalogConfig catalogConfig = icebergTable.catalogConfig();
        if (catalogConfig != null) connector.setCredentialsOnThread(catalogConfig);

        long t1 = System.currentTimeMillis();
        IcebergScanPlan scanPlan;
        try {
            scanPlan = AccessController.doPrivileged(
                (PrivilegedAction<IcebergScanPlan>) () -> LakehouseState.instance()
                    .scanPlanner()
                    .planScan(icebergTable.icebergTable(), icebergTable.snapshotId(), predicates, null)
            );
        } finally {
            if (catalogConfig != null) connector.clearCredentialsOnThread();
        }
        long t2 = System.currentTimeMillis();
        logger.info("[PERF] Iceberg scan planning: {}ms ({} files, {} bytes)", t2 - t1, scanPlan.fileCount(), scanPlan.getTotalFileSize());

        // 3. Convert Calcite RelNode to DataFusion SQL
        String sqlQuery = convertToDataFusionSql(logicalPlan, tableName);

        // 4. Build storage config
        Map<String, String> storageConfig = StorageConfigBuilder.buildStorageConfig(icebergTable, scanPlan);

        // 5. Normalize file paths
        long[] fileSizes = scanPlan.getFiles().stream().mapToLong(IcebergScanPlan.FileInfo::getFileSizeInBytes).toArray();
        List<String> filePaths = StorageConfigBuilder.normalizeFilePaths(scanPlan.getDataFilePaths());

        // 6. Execute asynchronously — single-node or distributed based on cluster size
        scanExecutor.executeAsync(logicalPlan, sqlQuery, filePaths, fileSizes, storageConfig, tableName, listener);
    }

    private String convertToDataFusionSql(RelNode logicalPlan, String tableName) {
        try {
            SqlDialect dialect = DataFusionSqlDialect.DEFAULT;
            RelToSqlConverter converter = new RelToSqlConverter(dialect);
            SqlNode sqlNode = converter.visitRoot(logicalPlan).asStatement();
            String sql = sqlNode.toSqlString(dialect).getSql();
            return stripSchemaQualifiers(sql, tableName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert query plan to SQL", e);
        }
    }

    private String stripSchemaQualifiers(String sql, String tableName) {
        String quotedTable = "\"" + tableName + "\"";
        return sql.replaceAll("\"\\w+\"\\." + java.util.regex.Pattern.quote(quotedTable), quotedTable);
    }

}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.exec;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.iceberg.expressions.Expression;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.EngineContext;
import org.opensearch.analytics.exec.DataWarehouseQueryEngine;
import org.opensearch.analytics.exec.DataWarehouseScanContext;
import org.opensearch.lakehouse.LakehouseState;
import org.opensearch.lakehouse.catalog.CatalogConfig;
import org.opensearch.lakehouse.catalog.IcebergCatalogConnector;
import org.opensearch.lakehouse.distributed.DistributedScanExecutor;
import org.opensearch.lakehouse.scan.CalciteToIcebergPredicateConverter;
import org.opensearch.lakehouse.scan.IcebergScanPlan;
import org.opensearch.lakehouse.schema.IcebergCalciteTable;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.UnifiedQueryPlanner;
import org.opensearch.sql.executor.QueryType;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
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
 *   <li>Execute via {@link DistributedScanExecutor} or single-node via {@link DataWarehouseQueryEngine}</li>
 * </ol>
 *
 * @opensearch.internal
 */
public class LakehouseQueryExecutor {

    private static final Logger logger = LogManager.getLogger(LakehouseQueryExecutor.class);
    private static final String DEFAULT_CATALOG = "opensearch";

    private final EngineContext engineContext;
    private final DataWarehouseQueryEngine queryEngine;

    public LakehouseQueryExecutor(EngineContext engineContext, DataWarehouseQueryEngine queryEngine) {
        this.engineContext = engineContext;
        this.queryEngine = queryEngine;
    }

    /**
     * Executes a SQL query and returns the response.
     */
    public PPLResponse executeSql(String sql) {
        return executeInternal(sql, QueryType.SQL);
    }

    /**
     * Executes a PPL query and returns the response.
     */
    public PPLResponse executePpl(String ppl) {
        return executeInternal(ppl, QueryType.PPL);
    }

    private PPLResponse executeInternal(String queryText, QueryType queryType) {
        long t0 = System.currentTimeMillis();
        SchemaPlus schema = engineContext.getSchema();

        UnifiedQueryContext context = UnifiedQueryContext.builder()
            .language(queryType)
            .catalog(DEFAULT_CATALOG, schema)
            .defaultNamespace(DEFAULT_CATALOG)
            .build();

        try {
            // 1. Parse query to Calcite RelNode
            UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);
            RelNode logicalPlan = planner.plan(queryText);
            long t1 = System.currentTimeMillis();
            logger.info("[PERF] Parse+plan: {}ms", t1 - t0);

            // 2. Extract column names from plan
            List<String> columns = logicalPlan.getRowType().getFieldNames();

            // 3. Execute lakehouse-specific pipeline
            Iterable<Object[]> result = executeLakehouse(logicalPlan);

            // 4. Build response
            List<Object[]> rows = new ArrayList<>();
            for (Object[] row : result) {
                rows.add(row);
            }
            logger.info("[PERF] Total query: {}ms, {} rows", System.currentTimeMillis() - t0, rows.size());
            return new PPLResponse(columns, rows);
        } catch (Exception e) {
            if (e instanceof RuntimeException) throw (RuntimeException) e;
            throw new RuntimeException("Failed to execute " + queryType + " query: " + e.getMessage(), e);
        } finally {
            try { context.close(); } catch (Exception ignored) {}
        }
    }

    /**
     * Executes the Iceberg scan pipeline: predicate pushdown, file pruning,
     * DataFusion SQL generation, distributed/single-node execution.
     */
    @SuppressWarnings("removal")
    Iterable<Object[]> executeLakehouse(RelNode logicalPlan) {
        // Find the IcebergCalciteTable in the plan
        IcebergCalciteTable icebergTable = extractIcebergTable(logicalPlan);
        if (icebergTable == null) {
            throw new IllegalArgumentException("No Iceberg table found in query plan");
        }

        IcebergCatalogConnector connector = LakehouseState.instance().catalogConnector();

        // 1. Extract Iceberg predicates for manifest-level file pruning
        Expression filterExpr = extractIcebergFilter(logicalPlan);
        List<Expression> predicates = filterExpr != null ? List.of(filterExpr) : List.of();

        // 2. Plan scan — resolves manifests to pruned data file paths
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
        String tableName = extractTableName(logicalPlan);
        String sqlQuery = convertToDataFusionSql(logicalPlan, tableName);

        // 4. Build storage config
        Map<String, String> storageConfig = buildStorageConfig(connector, icebergTable, scanPlan);

        // 5. Normalize file paths
        long[] fileSizes = scanPlan.getFiles().stream().mapToLong(IcebergScanPlan.FileInfo::getFileSizeInBytes).toArray();
        List<String> filePaths = normalizeFilePaths(scanPlan.getDataFilePaths());

        // 6. Execute via distributed or single-node
        DistributedScanExecutor scanExecutor = LakehouseState.instance().distributedScanExecutor();
        if (scanExecutor != null) {
            return scanExecutor.execute(logicalPlan, sqlQuery, filePaths, fileSizes, storageConfig, tableName);
        }

        // Fallback: single-node via backend directly
        DataWarehouseScanContext scanContext = new DataWarehouseScanContext(tableName, filePaths, fileSizes, sqlQuery, storageConfig);
        return queryEngine.executeQuery(scanContext);
    }

    // --- Helper methods ---

    private IcebergCalciteTable extractIcebergTable(RelNode node) {
        if (node instanceof TableScan) {
            org.apache.calcite.schema.Table table = node.getTable().unwrap(org.apache.calcite.schema.Table.class);
            if (table instanceof IcebergCalciteTable) return (IcebergCalciteTable) table;
        }
        for (RelNode input : node.getInputs()) {
            IcebergCalciteTable found = extractIcebergTable(input);
            if (found != null) return found;
        }
        return null;
    }

    private Expression extractIcebergFilter(RelNode node) {
        if (node instanceof Filter) {
            Filter filter = (Filter) node;
            if (filter.getInput() instanceof TableScan) {
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

    private String extractTableName(RelNode node) {
        if (node instanceof TableScan) {
            List<String> qn = node.getTable().getQualifiedName();
            return qn.get(qn.size() - 1);
        }
        for (RelNode input : node.getInputs()) {
            String name = extractTableName(input);
            if (name != null) return name;
        }
        throw new IllegalArgumentException("No TableScan found in plan");
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

    private Map<String, String> buildStorageConfig(
        IcebergCatalogConnector connector, IcebergCalciteTable icebergTable, IcebergScanPlan scanPlan
    ) {
        Map<String, String> config = new HashMap<>();
        CatalogConfig catalogConfig = icebergTable.catalogConfig();
        if (catalogConfig != null && catalogConfig.region() != null) config.put("s3Region", catalogConfig.region());
        List<String> paths = scanPlan.getDataFilePaths();
        if (!paths.isEmpty()) {
            String firstPath = paths.get(0);
            if (firstPath.startsWith("s3://")) {
                String withoutScheme = firstPath.substring(5);
                int slashIdx = withoutScheme.indexOf('/');
                if (slashIdx > 0) config.put("s3Bucket", withoutScheme.substring(0, slashIdx));
            }
            if (firstPath.startsWith("file:") || firstPath.startsWith("/")) config.put("localMode", "true");
        }
        if (catalogConfig != null) {
            config.put("indexName", catalogConfig.indexName());
            config.put("authType", catalogConfig.authType());
        }
        return config;
    }

    private List<String> normalizeFilePaths(List<String> paths) {
        return paths.stream()
            .map(p -> {
                if (p.startsWith("file:/") && !p.startsWith("file://")) return "file://" + p.substring("file:".length());
                else if (p.startsWith("/")) return "file://" + p;
                return p;
            })
            .toList();
    }
}

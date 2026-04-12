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
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.iceberg.expressions.Expression;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.exec.ExternalScanContext;
import org.opensearch.analytics.exec.ExternalTableExecutor;
import org.opensearch.analytics.schema.ExternalTable;
import org.opensearch.analytics.schema.SchemaContributor;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Setting;
import org.opensearch.lakehouse.catalog.AwsCredentials;
import org.opensearch.lakehouse.catalog.CatalogConfig;
import org.opensearch.lakehouse.catalog.IcebergCatalogConnector;
import org.opensearch.lakehouse.exec.DataFusionSqlDialect;
import org.opensearch.lakehouse.action.LakehouseQueryAction;
import org.opensearch.lakehouse.action.LakehouseQueryTransportAction;
import org.opensearch.lakehouse.action.LakehousePplRestAction;
import org.opensearch.lakehouse.action.LakehouseSqlRestAction;
import org.opensearch.lakehouse.scan.CalciteToIcebergPredicateConverter;
import org.opensearch.lakehouse.scan.IcebergScanPlan;
import org.opensearch.lakehouse.schema.IcebergCalciteTable;
import org.opensearch.lakehouse.schema.IcebergSchemaContributor;
import org.opensearch.action.ActionRequest;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;

import java.io.Closeable;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Lakehouse plugin that enables reading external Apache Iceberg tables via SQL and PPL.
 * <p>
 * Iceberg tables are registered as OpenSearch indices with special settings
 * ({@code index.lakehouse.enabled=true}), which gives them automatic security
 * plugin integration (RBAC, DLS, FLS, audit logging). All table configuration
 * (catalog type, region, warehouse, namespace, table) is index-scoped.
 * <p>
 * Implements both {@link SchemaContributor} (to register Iceberg tables into Calcite) and
 * {@link ExternalTableExecutor} (to build scan plans for those tables). Both interfaces are
 * discovered via ExtensiblePlugin.ExtensionLoader by the analytics-engine.
 */
public class LakehousePlugin extends Plugin implements SchemaContributor, ExternalTableExecutor, ActionPlugin, Closeable {

    private static final Logger logger = LogManager.getLogger(LakehousePlugin.class);

    private static final IcebergSchemaContributor schemaContributor = new IcebergSchemaContributor(
        LakehouseState.instance().catalogConnector()
    );

    /** Creates a new LakehousePlugin instance. */
    public LakehousePlugin() {}

    @Override
    public List<Setting<?>> getSettings() {
        return LakehouseSettings.all();
    }

    @Override
    public boolean claims(IndexMetadata indexMetadata) {
        return schemaContributor.claims(indexMetadata);
    }

    @Override
    public boolean supports(ExternalTable externalTable) {
        return "iceberg".equals(externalTable.format());
    }

    @Override
    public void contributeSchema(SchemaPlus schema, Object clusterState) {
        schemaContributor.contributeSchema(schema, (ClusterState) clusterState);
    }

    @Override
    @SuppressWarnings("removal")
    public ExternalScanContext prepareScan(RelNode logicalPlan, ExternalTable externalTable) {
        if (!(externalTable instanceof IcebergCalciteTable)) {
            throw new IllegalArgumentException("Expected IcebergCalciteTable but got: " + externalTable.getClass().getSimpleName());
        }
        IcebergCalciteTable icebergTable = (IcebergCalciteTable) externalTable;
        long t0 = System.currentTimeMillis();
        logger.info("[LakehousePlugin] Preparing scan for Iceberg table: {}", icebergTable.icebergTable().name());
        logger.info("[LakehousePlugin] logicalPlan class={}, explain:\n{}", logicalPlan.getClass().getSimpleName(), logicalPlan.explain());

        IcebergCatalogConnector connector = LakehouseState.instance().catalogConnector();

        // 1. Extract Iceberg predicates for manifest-level file pruning.
        Expression filterExpr = extractIcebergFilter(logicalPlan);
        List<Expression> predicates = filterExpr != null ? List.of(filterExpr) : List.of();

        // 2. Plan scan — resolves manifests to pruned data file paths.
        CatalogConfig catalogConfig = icebergTable.catalogConfig();
        if (catalogConfig != null) {
            connector.setCredentialsOnThread(catalogConfig);
        }
        long t1 = System.currentTimeMillis();
        IcebergScanPlan scanPlan;
        try {
            scanPlan = AccessController.doPrivileged(
                (PrivilegedAction<IcebergScanPlan>) () -> LakehouseState.instance()
                    .scanPlanner()
                    .planScan(
                        icebergTable.icebergTable(),
                        icebergTable.snapshotId(),
                        predicates,
                        null  // all columns — DataFusion handles projection from SQL
                    )
            );
        } finally {
            if (catalogConfig != null) {
                connector.clearCredentialsOnThread();
            }
        }
        long t2 = System.currentTimeMillis();
        logger.info("[PERF] Iceberg scan planning: {}ms ({} files, {} bytes)", t2 - t1, scanPlan.fileCount(), scanPlan.getTotalFileSize());

        // 3. Convert Calcite RelNode to DataFusion SQL
        String tableName = extractTableName(logicalPlan);
        String sqlQuery;
        try {
            SqlDialect dialect = DataFusionSqlDialect.DEFAULT;
            RelToSqlConverter converter = new RelToSqlConverter(dialect);
            SqlNode sqlNode = converter.visitRoot(logicalPlan).asStatement();
            sqlQuery = sqlNode.toSqlString(dialect).getSql();
            sqlQuery = stripSchemaQualifiers(sqlQuery, tableName);
            logger.info("[LakehousePlugin] Generated SQL for DataFusion: {}", sqlQuery);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert query plan to SQL", e);
        }
        long t3 = System.currentTimeMillis();
        logger.info("[PERF] SQL generation: {}ms", t3 - t2);

        // 4. Build storage config from catalog settings
        Map<String, String> storageConfig = buildStorageConfig(connector, icebergTable, scanPlan);
        logger.info("[PERF] prepareScan total: {}ms", System.currentTimeMillis() - t0);

        // Extract file sizes from Iceberg manifest (avoids S3 HEAD calls in Rust)
        long[] fileSizes = scanPlan.getFiles().stream().mapToLong(IcebergScanPlan.FileInfo::getFileSizeInBytes).toArray();

        // Normalize file paths: Iceberg Hadoop catalog uses "file:" prefix, DataFusion expects "file://"
        List<String> filePaths = scanPlan.getDataFilePaths().stream()
            .map(p -> {
                if (p.startsWith("file:/") && !p.startsWith("file://")) {
                    return "file://" + p.substring("file:".length());
                } else if (p.startsWith("/")) {
                    return "file://" + p;
                }
                return p;
            })
            .toList();

        return new ExternalScanContext(tableName, filePaths, fileSizes, sqlQuery, storageConfig);
    }

    private Expression extractIcebergFilter(RelNode node) {
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
     * Strips schema qualifiers from generated SQL so table references match the
     * leaf name registered in DataFusion. PPL wraps tables under "opensearch" schema,
     * producing "opensearch"."nyc_taxi" — DataFusion only knows "nyc_taxi".
     */
    private String stripSchemaQualifiers(String sql, String tableName) {
        String quotedTable = "\"" + tableName + "\"";
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
        IcebergCatalogConnector connector,
        IcebergCalciteTable icebergTable,
        IcebergScanPlan scanPlan
    ) {
        Map<String, String> config = new HashMap<>();
        CatalogConfig catalogConfig = icebergTable.catalogConfig();
        if (catalogConfig != null && catalogConfig.region() != null) {
            config.put("s3Region", catalogConfig.region());
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
            if (firstPath.startsWith("file:") || firstPath.startsWith("/")) {
                config.put("localMode", "true");
            }
        }
        // Pass per-catalog AWS credentials to DataFusion's Rust S3 client
        if (catalogConfig != null) {
            AwsCredentials creds = connector.getCredentials(catalogConfig);
            if (creds != null && creds.isComplete()) {
                config.put("s3AccessKeyId", creds.getAccessKeyId());
                config.put("s3SecretAccessKey", creds.getSecretAccessKey());
                if (creds.getSessionToken() != null) {
                    config.put("s3SessionToken", creds.getSessionToken());
                }
            }
        }
        return config;
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(new ActionHandler<>(LakehouseQueryAction.INSTANCE, LakehouseQueryTransportAction.class));
    }

    @Override
    public List<RestHandler> getRestHandlers(
        org.opensearch.common.settings.Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return List.of(new LakehouseSqlRestAction(), new LakehousePplRestAction());
    }

    @Override
    public void close() {
        LakehouseState.instance().close();
    }
}

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
import org.apache.calcite.schema.SchemaPlus;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.exec.ExternalScanContext;
import org.opensearch.analytics.exec.ExternalTableExecutor;
import org.opensearch.analytics.schema.ExternalTable;
import org.opensearch.analytics.schema.SchemaContributor;
import org.opensearch.lakehouse.scan.CalciteToIcebergPredicateConverter;
import org.opensearch.lakehouse.scan.IcebergScanPlan;
import org.opensearch.lakehouse.scan.IcebergScanPlanner;
import org.opensearch.lakehouse.substrait.CalciteSubstraitConverter;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.lakehouse.action.RegisterCatalogAction;
import org.opensearch.lakehouse.action.RegisterTableAction;
import org.opensearch.lakehouse.catalog.CatalogConfig;
import org.opensearch.lakehouse.catalog.CatalogType;
import org.opensearch.lakehouse.catalog.IcebergCatalogConnector;
import org.opensearch.lakehouse.cluster.LakehouseMetadata;
import org.opensearch.lakehouse.schema.IcebergCalciteTable;
import org.opensearch.lakehouse.schema.IcebergSchemaEnricher;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

/**
 * Plugin for reading external Apache Iceberg tables via the analytics engine.
 * Implements {@link ExternalTableExecutor} so that the analytics-engine hub can
 * route queries referencing Iceberg tables to this plugin.
 */
public class LakehousePlugin extends Plugin implements ActionPlugin, ExternalTableExecutor, SchemaContributor {

    private static final Logger logger = LogManager.getLogger(LakehousePlugin.class);

    private ClusterService clusterService;
    private final IcebergCatalogConnector catalogConnector = new IcebergCatalogConnector();
    private final ExecutorService scanExecutor = java.util.concurrent.Executors.newFixedThreadPool(
        Math.max(2, Runtime.getRuntime().availableProcessors() / 2));
    private final IcebergScanPlanner scanPlanner = new IcebergScanPlanner(scanExecutor);

    /** Creates a new lakehouse plugin instance. */
    public LakehousePlugin() {}

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        this.clusterService = clusterService;
        return Collections.emptyList();
    }

    @Override
    public ExternalScanContext prepareScan(RelNode logicalPlan, ExternalTable externalTable) {
        if (!(externalTable instanceof IcebergCalciteTable)) {
            throw new IllegalArgumentException("Expected IcebergCalciteTable but got: " + externalTable.getClass().getSimpleName());
        }
        IcebergCalciteTable icebergTable = (IcebergCalciteTable) externalTable;
        Table table = icebergTable.getIcebergTable();
        logger.info("[LakehousePlugin] Preparing scan for Iceberg table: {}", table.name());

        // 1. Extract Iceberg predicates for manifest-level pruning
        Expression filterExpr = extractIcebergFilter(logicalPlan);
        List<Expression> predicates = filterExpr != null ? List.of(filterExpr) : List.of();

        // 2. Plan scan — resolves manifests to pruned S3 Parquet file paths
        IcebergScanPlan scanPlan = scanPlanner.planScan(
            table,
            icebergTable.getPinnedSnapshotId(),
            predicates,
            null  // all columns — DataFusion handles projection from Substrait plan
        );
        logger.info("[LakehousePlugin] Scan plan: {} files, {} bytes total",
            scanPlan.fileCount(), scanPlan.getTotalFileSize());

        // 3. Convert Calcite RelNode to Substrait bytes
        byte[] substraitBytes;
        try {
            substraitBytes = CalciteSubstraitConverter.toSubstrait(logicalPlan);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert query plan to Substrait", e);
        }

        // 4. Build storage config from CatalogConfig
        Map<String, String> storageConfig = buildStorageConfig(icebergTable, scanPlan);

        // 5. Extract table name from the Calcite plan (must match Substrait reference)
        String tableName = extractTableName(logicalPlan);

        return new ExternalScanContext(tableName, scanPlan.getDataFilePaths(), substraitBytes, storageConfig);
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

    private Map<String, String> buildStorageConfig(IcebergCalciteTable icebergTable, IcebergScanPlan scanPlan) {
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
        return config;
    }

    /**
     * Finds the first node of the given type in the RelNode tree (depth-first).
     */
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

    @Override
    public void contributeSchema(SchemaPlus schema, ClusterState clusterState) {
        LakehouseMetadata metadata = clusterState.metadata().custom(LakehouseMetadata.TYPE);
        if (metadata == null) {
            return;
        }

        // Ensure catalogs are registered in the connector
        for (Map.Entry<String, Map<String, String>> entry : metadata.catalogs().entrySet()) {
            String name = entry.getKey();
            if (!catalogConnector.listCatalogs().contains(name)) {
                Map<String, String> config = entry.getValue();
                try {
                    CatalogConfig catalogConfig = new CatalogConfig(
                        name,
                        CatalogType.valueOf(config.getOrDefault("type", "GLUE").toUpperCase(java.util.Locale.ROOT)),
                        config.get("uri"),
                        config.get("warehouse"),
                        config.get("region"),
                        config.get("database"),
                        config.getOrDefault("credential_provider", "default"),
                        Duration.ofMinutes(5)
                    );
                    catalogConnector.registerCatalog(name, catalogConfig);
                } catch (Exception e) {
                    logger.warn("[LakehousePlugin] Failed to register catalog [{}]: {}", name, e.getMessage());
                    continue;
                }
            }
        }

        // Load and add each registered table
        Map<String, Table> icebergTables = new HashMap<>();
        Map<String, CatalogConfig> tableCatalogConfigs = new HashMap<>();
        for (Map.Entry<String, Map<String, String>> entry : metadata.tables().entrySet()) {
            String tableName = entry.getKey();
            Map<String, String> binding = entry.getValue();
            String catalogName = binding.get("catalog");
            String namespace = binding.getOrDefault("namespace", "default");
            String icebergTableName = binding.getOrDefault("table", tableName);

            try {
                Table table = catalogConnector.loadTable(catalogName, TableIdentifier.of(namespace, icebergTableName));
                icebergTables.put(tableName, table);
                // Find the CatalogConfig for this table's catalog
                Map<String, String> catalogConfigMap = metadata.catalogs().get(catalogName);
                if (catalogConfigMap != null) {
                    CatalogConfig config = new CatalogConfig(
                        catalogName,
                        CatalogType.valueOf(catalogConfigMap.getOrDefault("type", "GLUE").toUpperCase(java.util.Locale.ROOT)),
                        catalogConfigMap.get("uri"),
                        catalogConfigMap.get("warehouse"),
                        catalogConfigMap.get("region"),
                        catalogConfigMap.get("database"),
                        catalogConfigMap.getOrDefault("credential_provider", "default"),
                        Duration.ofMinutes(5)
                    );
                    tableCatalogConfigs.put(tableName, config);
                }
            } catch (Exception e) {
                logger.warn("[LakehousePlugin] Failed to load table [{}] from catalog [{}]: {}", tableName, catalogName, e.getMessage());
            }
        }

        if (!icebergTables.isEmpty()) {
            IcebergSchemaEnricher.enrich(schema, icebergTables, tableCatalogConfigs);
        }
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return List.of(
            new RegisterCatalogAction(clusterService),
            new RegisterTableAction(clusterService)
        );
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(Metadata.Custom.class, LakehouseMetadata.TYPE, LakehouseMetadata::new),
            new NamedWriteableRegistry.Entry(
                org.opensearch.cluster.NamedDiff.class,
                LakehouseMetadata.TYPE,
                LakehouseMetadata::readDiffFrom
            )
        );
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return List.of(
            new NamedXContentRegistry.Entry(
                Metadata.Custom.class,
                new org.opensearch.core.ParseField(LakehouseMetadata.TYPE),
                parser -> LakehouseMetadata.fromXContent(parser)
            )
        );
    }
}

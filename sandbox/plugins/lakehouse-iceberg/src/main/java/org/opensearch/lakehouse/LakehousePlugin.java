/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.exec.ExternalTableExecutor;
import org.opensearch.analytics.schema.ExternalTable;
import org.opensearch.analytics.schema.SchemaContributor;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    public Iterable<Object[]> execute(RelNode logicalPlan, ExternalTable externalTable) {
        if (!(externalTable instanceof IcebergCalciteTable)) {
            throw new IllegalArgumentException("Expected IcebergCalciteTable but got: " + externalTable.getClass().getSimpleName());
        }
        IcebergCalciteTable icebergTable = (IcebergCalciteTable) externalTable;
        Table table = icebergTable.getIcebergTable();
        logger.info("[LakehousePlugin] Executing query against Iceberg table: {}", table.name());

        // Phase 1: Read data using Iceberg's native read API.
        // Phase 2+: Use Substrait + DataFusion JNI for predicate pushdown and projection.
        List<Types.NestedField> columns = table.schema().columns();
        List<Object[]> results = new ArrayList<>();
        try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
            for (Record record : records) {
                Object[] row = new Object[columns.size()];
                for (int i = 0; i < columns.size(); i++) {
                    row[i] = record.get(i);
                }
                results.add(row);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to read Iceberg table: " + table.name(), e);
        }
        logger.info("[LakehousePlugin] Read {} rows from Iceberg table: {}", results.size(), table.name());
        return results;
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
        for (Map.Entry<String, Map<String, String>> entry : metadata.tables().entrySet()) {
            String tableName = entry.getKey();
            Map<String, String> binding = entry.getValue();
            String catalogName = binding.get("catalog");
            String namespace = binding.getOrDefault("namespace", "default");
            String icebergTableName = binding.getOrDefault("table", tableName);

            try {
                Table table = catalogConnector.loadTable(catalogName, TableIdentifier.of(namespace, icebergTableName));
                icebergTables.put(tableName, table);
            } catch (Exception e) {
                logger.warn("[LakehousePlugin] Failed to load table [{}] from catalog [{}]: {}", tableName, catalogName, e.getMessage());
            }
        }

        if (!icebergTables.isEmpty()) {
            IcebergSchemaEnricher.enrich(schema, icebergTables);
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

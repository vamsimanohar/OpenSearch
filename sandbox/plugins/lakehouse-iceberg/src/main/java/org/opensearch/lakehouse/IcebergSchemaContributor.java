/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.schema.SchemaContributor;
import org.opensearch.cluster.ClusterState;
import org.opensearch.lakehouse.catalog.CatalogConfig;
import org.opensearch.lakehouse.catalog.CatalogType;
import org.opensearch.lakehouse.catalog.IcebergCatalogConnector;
import org.opensearch.lakehouse.cluster.LakehouseMetadata;
import org.opensearch.lakehouse.schema.IcebergSchemaEnricher;

import java.time.Duration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Contributes Iceberg tables to the Calcite schema used by the analytics engine.
 *
 * <p>Discovered via SPI ({@code META-INF/services/SchemaContributor}).
 * Called on every SQL query to build the schema from cluster state metadata.
 */
public class IcebergSchemaContributor implements SchemaContributor {

    private static final Logger logger = LogManager.getLogger(IcebergSchemaContributor.class);

    /** No-arg constructor required by SPI ({@code SPIClassIterator}). */
    public IcebergSchemaContributor() {}

    @Override
    public void contributeSchema(SchemaPlus schema, ClusterState clusterState) {
        LakehouseMetadata metadata = clusterState.metadata().custom(LakehouseMetadata.TYPE);
        if (metadata == null) {
            return;
        }

        IcebergCatalogConnector connector = LakehouseState.instance().catalogConnector();

        // Ensure catalogs are registered in the connector
        for (Map.Entry<String, Map<String, String>> entry : metadata.catalogs().entrySet()) {
            String name = entry.getKey();
            if (!connector.listCatalogs().contains(name)) {
                Map<String, String> config = entry.getValue();
                try {
                    CatalogConfig catalogConfig = new CatalogConfig(
                        name,
                        CatalogType.valueOf(config.getOrDefault("type", "GLUE").toUpperCase(Locale.ROOT)),
                        config.get("uri"),
                        config.get("warehouse"),
                        config.get("region"),
                        config.get("database"),
                        config.getOrDefault("credential_provider", "default"),
                        Duration.ofMinutes(5)
                    );
                    connector.registerCatalog(name, catalogConfig);
                } catch (Exception e) {
                    logger.warn("[IcebergSchemaContributor] Failed to register catalog [{}]: {}", name, e.getMessage());
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
                Table table = connector.loadTable(catalogName, TableIdentifier.of(namespace, icebergTableName));
                icebergTables.put(tableName, table);
                CatalogConfig cachedConfig = connector.getCatalogConfig(catalogName);
                if (cachedConfig != null) {
                    tableCatalogConfigs.put(tableName, cachedConfig);
                }
            } catch (Exception e) {
                logger.warn("[IcebergSchemaContributor] Failed to load table [{}] from catalog [{}]: {}",
                    tableName, catalogName, e.getMessage());
            }
        }

        if (!icebergTables.isEmpty()) {
            IcebergSchemaEnricher.enrich(schema, icebergTables, tableCatalogConfigs);
        }
    }
}

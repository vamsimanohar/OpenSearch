/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.schema;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.lakehouse.LakehouseSettings;
import org.opensearch.lakehouse.catalog.CatalogConfig;
import org.opensearch.lakehouse.catalog.IcebergCatalogConnector;

/**
 * Contributes Iceberg tables to the Calcite schema.
 * <p>
 * For each OpenSearch index with {@code index.lakehouse.enabled=true},
 * registers a lazy {@link IcebergCalciteTable} into the Calcite schema.
 * The expensive Iceberg SDK call ({@code loadTable()}) is deferred until
 * Calcite actually resolves the table during query parsing — so only
 * tables referenced by the query pay the cost.
 */
public class IcebergSchemaContributor {

    private static final Logger logger = LogManager.getLogger(IcebergSchemaContributor.class);

    private final IcebergCatalogConnector catalogConnector;

    /**
     * Creates a contributor with the given catalog connector.
     *
     * @param catalogConnector the shared catalog connector
     */
    public IcebergSchemaContributor(IcebergCatalogConnector catalogConnector) {
        this.catalogConnector = catalogConnector;
    }

    /**
     * Returns true if the index is a lakehouse index ({@code index.lakehouse.enabled=true}).
     *
     * @param indexMetadata the index metadata to check
     * @return true if this is a lakehouse index
     */
    public boolean claims(IndexMetadata indexMetadata) {
        return LakehouseSettings.INDEX_LAKEHOUSE_ENABLED.get(indexMetadata.getSettings());
    }

    /**
     * Registers lazy Iceberg table placeholders for all lakehouse indices.
     * No Iceberg SDK calls are made here — metadata is loaded on demand
     * when Calcite resolves a table referenced by a query.
     *
     * @param schema       the mutable Calcite schema
     * @param clusterState the current cluster state
     */
    public void contributeSchema(SchemaPlus schema, ClusterState clusterState) {
        for (IndexMetadata indexMetadata : clusterState.metadata().indices().values()) {
            if (!claims(indexMetadata)) {
                continue;
            }
            String indexName = indexMetadata.getIndex().getName();
            try {
                CatalogConfig config = CatalogConfig.fromIndexSettings(indexMetadata);
                schema.add(indexName, new IcebergCalciteTable(config, catalogConnector));
                logger.debug("[SchemaContributor] Registered lazy Iceberg table for index [{}]", indexName);
            } catch (Exception e) {
                logger.warn("[SchemaContributor] Failed to register Iceberg table for index [{}]: {}", indexName, e.getMessage());
            }
        }
    }
}

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
import org.opensearch.analytics.exec.ExternalScanContext;
import org.opensearch.analytics.exec.ExternalTableExecutor;
import org.opensearch.analytics.schema.ExternalTable;
import org.opensearch.analytics.schema.SchemaContributor;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Setting;
import org.opensearch.lakehouse.catalog.IcebergCatalogConnector;
import org.opensearch.lakehouse.schema.IcebergSchemaContributor;
import org.opensearch.plugins.Plugin;

import java.util.List;

/**
 * Lakehouse plugin that enables reading external Apache Iceberg tables via SQL and PPL.
 * <p>
 * Iceberg tables are registered as OpenSearch indices with special settings
 * ({@code index.lakehouse.enabled=true}), which gives them automatic security
 * plugin integration (RBAC, DLS, FLS, audit logging). All table configuration
 * (catalog type, region, warehouse, namespace, table) is index-scoped.
 * <p>
 * Authentication supports three modes via {@code index.lakehouse.auth_type}:
 * {@code role} (IAM assume-role), {@code keys} (keystore-backed credentials),
 * or {@code default} (environment credentials chain).
 * <p>
 * Implements both {@link SchemaContributor} (to register Iceberg tables into Calcite) and
 * {@link ExternalTableExecutor} (to build scan plans for those tables). Both interfaces are
 * discovered via ExtensiblePlugin.ExtensionLoader by the analytics-engine.
 */
public class LakehousePlugin extends Plugin implements SchemaContributor, ExternalTableExecutor {

    /**
     * Shared catalog connector — static because SPI creates separate instances
     * for SchemaContributor and ExternalTableExecutor interfaces.
     */
    private static final IcebergCatalogConnector catalogConnector = new IcebergCatalogConnector();

    private static final IcebergSchemaContributor schemaContributor = new IcebergSchemaContributor(catalogConnector);

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
    public ExternalScanContext prepareScan(RelNode logicalPlan, ExternalTable externalTable) {
        // Will be implemented in a later PR (scan planning)
        throw new UnsupportedOperationException("Iceberg scan planning not yet implemented");
    }

    /** Returns the shared catalog connector (for use by ExternalTableExecutor in later PRs). */
    static IcebergCatalogConnector getCatalogConnector() {
        return catalogConnector;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse;

import org.apache.calcite.schema.SchemaPlus;
import org.opensearch.analytics.schema.SchemaContributor;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Setting;
import org.opensearch.plugins.Plugin;

import java.io.Closeable;
import java.util.List;

/**
 * Lakehouse plugin that enables reading external Apache Iceberg tables via SQL and PPL.
 * <p>
 * Iceberg tables are registered as OpenSearch indices with special settings
 * ({@code index.lakehouse.enabled=true}), which gives them automatic security
 * plugin integration (RBAC, DLS, FLS, audit logging). All table configuration
 * (catalog type, region, warehouse, namespace, table) is index-scoped.
 * <p>
 * Implements {@link SchemaContributor} to register Iceberg tables into Calcite.
 */
public class LakehousePlugin extends Plugin implements SchemaContributor, Closeable {

    /** Creates a new LakehousePlugin instance. */
    public LakehousePlugin() {}

    @Override
    public List<Setting<?>> getSettings() {
        return LakehouseSettings.all();
    }

    @Override
    public boolean claims(IndexMetadata indexMetadata) {
        return false;
    }

    @Override
    public void contributeSchema(SchemaPlus schema, Object clusterState) {}

    @Override
    public void close() {
        LakehouseState.instance().close();
    }
}

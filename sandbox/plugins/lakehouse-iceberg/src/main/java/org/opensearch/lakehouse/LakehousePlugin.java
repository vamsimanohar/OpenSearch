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
import org.opensearch.common.settings.Setting;
import org.opensearch.plugins.Plugin;

import java.util.List;

/**
 * Lakehouse plugin that enables reading external Apache Iceberg tables via SQL and PPL.
 * <p>
 * Iceberg tables are registered as OpenSearch indices with special settings
 * ({@code index.lakehouse.enabled=true}), which gives them automatic security
 * plugin integration (RBAC, DLS, FLS, audit logging).
 * <p>
 * Catalog configuration is stored in node-scoped settings ({@code lakehouse.catalog.{name}.*}),
 * with credentials in the opensearch-keystore for security.
 * <p>
 * Implements both {@link SchemaContributor} (to register Iceberg tables into Calcite) and
 * {@link ExternalTableExecutor} (to build scan plans for those tables). Both interfaces are
 * discovered via ExtensiblePlugin.ExtensionLoader by the analytics-engine.
 */
public class LakehousePlugin extends Plugin implements SchemaContributor, ExternalTableExecutor {

    /** Creates a new LakehousePlugin instance. */
    public LakehousePlugin() {}

    @Override
    public List<Setting<?>> getSettings() {
        return LakehouseSettings.all();
    }

    @Override
    public boolean supports(ExternalTable externalTable) {
        return "iceberg".equals(externalTable.format());
    }

    @Override
    public void contributeSchema(SchemaPlus schema, Object clusterState) {
        // Will be implemented in a later PR (Iceberg catalog connector + schema discovery)
    }

    @Override
    public ExternalScanContext prepareScan(RelNode logicalPlan, ExternalTable externalTable) {
        // Will be implemented in a later PR (scan planning)
        throw new UnsupportedOperationException("Iceberg scan planning not yet implemented");
    }
}

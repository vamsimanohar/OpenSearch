/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LakehousePluginTests extends OpenSearchTestCase {

    public void testPluginCreation() throws IOException {
        try (LakehousePlugin plugin = new LakehousePlugin()) {
            assertNotNull(plugin);
        }
    }

    public void testGetSettingsReturnsAllSettings() throws IOException {
        try (LakehousePlugin plugin = new LakehousePlugin()) {
            List<Setting<?>> settings = plugin.getSettings();
            assertNotNull(settings);
            assertEquals(12, settings.size());
        }
    }

    public void testClaimsLakehouseIndex() throws IOException {
        try (LakehousePlugin plugin = new LakehousePlugin()) {
            IndexMetadata idx = indexMetadata("iceberg_table", Settings.builder().put("index.lakehouse.enabled", true));
            assertTrue(plugin.claims(idx));
        }
    }

    public void testDoesNotClaimNormalIndex() throws IOException {
        try (LakehousePlugin plugin = new LakehousePlugin()) {
            IndexMetadata idx = indexMetadata("normal_index", Settings.builder());
            assertFalse(plugin.claims(idx));
        }
    }

    public void testContributeSchemaWithNoLakehouseIndices() throws IOException {
        try (LakehousePlugin plugin = new LakehousePlugin()) {
            IndexMetadata normalIdx = indexMetadata("normal_index", Settings.builder());
            ClusterState state = clusterState(Map.of("normal_index", normalIdx));
            SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();

            plugin.contributeSchema(schema, state);

            assertNull("Normal index should not be registered", schema.getTable("normal_index"));
        }
    }

    public void testContributeSchemaWithLakehouseIndex() throws IOException {
        try (LakehousePlugin plugin = new LakehousePlugin()) {
            IndexMetadata lakehouseIdx = indexMetadata(
                "my_iceberg",
                Settings.builder()
                    .put("index.lakehouse.enabled", true)
                    .put("index.lakehouse.type", "glue")
                    .put("index.lakehouse.warehouse", "s3://bucket")
                    .put("index.lakehouse.namespace", "db")
                    .put("index.lakehouse.table", "events")
            );
            ClusterState state = clusterState(Map.of("my_iceberg", lakehouseIdx));
            SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();

            plugin.contributeSchema(schema, state);

            assertNotNull("Lakehouse index should be registered", schema.getTable("my_iceberg"));
        }
    }

    public void testGetCatalogConnectorViaState() {
        assertNotNull(LakehouseState.instance().catalogConnector());
    }

    public void testGetActionsIncludesWorkerQueryAction() throws IOException {
        try (LakehousePlugin plugin = new LakehousePlugin()) {
            var actions = plugin.getActions();
            assertEquals(2, actions.size());
            assertEquals("cluster:internal/lakehouse/query", actions.get(0).getAction().name());
            assertEquals("cluster:internal/lakehouse/worker/query", actions.get(1).getAction().name());
        }
    }

    public void testAdditionalSettingsRegistersNodeAttribute() throws IOException {
        try (LakehousePlugin plugin = new LakehousePlugin()) {
            Settings settings = plugin.additionalSettings();
            assertEquals("true", settings.get("node.attr.lakehouse.worker"));
        }
    }

    private static ClusterState clusterState(Map<String, IndexMetadata> indices) {
        Metadata.Builder metadataBuilder = Metadata.builder();
        for (Map.Entry<String, IndexMetadata> entry : indices.entrySet()) {
            metadataBuilder.put(entry.getValue(), false);
        }
        ClusterState state = mock(ClusterState.class);
        Metadata metadata = metadataBuilder.build();
        when(state.metadata()).thenReturn(metadata);
        return state;
    }

    private static IndexMetadata indexMetadata(String name, Settings.Builder extraSettings) {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(extraSettings.build())
            .build();
        return IndexMetadata.builder(name).settings(settings).build();
    }
}

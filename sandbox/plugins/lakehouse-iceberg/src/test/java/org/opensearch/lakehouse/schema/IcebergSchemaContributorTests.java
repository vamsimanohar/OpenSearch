/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.schema;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.lakehouse.catalog.IcebergCatalogConnector;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IcebergSchemaContributorTests extends OpenSearchTestCase {

    private IcebergCatalogConnector connector;
    private IcebergSchemaContributor contributor;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        connector = mock(IcebergCatalogConnector.class);
        contributor = new IcebergSchemaContributor(connector);
    }

    public void testClaimsLakehouseIndex() {
        IndexMetadata idx = indexMetadata("iceberg_table", Settings.builder().put("index.lakehouse.enabled", true));
        assertTrue(contributor.claims(idx));
    }

    public void testDoesNotClaimNormalIndex() {
        IndexMetadata idx = indexMetadata("normal_index", Settings.builder());
        assertFalse(contributor.claims(idx));
    }

    public void testDoesNotClaimExplicitlyDisabled() {
        IndexMetadata idx = indexMetadata("disabled_table", Settings.builder().put("index.lakehouse.enabled", false));
        assertFalse(contributor.claims(idx));
    }

    public void testContributeSchemaRegistersLakehouseIndices() {
        IndexMetadata lakehouseIdx = indexMetadata(
            "my_iceberg",
            Settings.builder()
                .put("index.lakehouse.enabled", true)
                .put("index.lakehouse.type", "glue")
                .put("index.lakehouse.warehouse", "s3://bucket")
                .put("index.lakehouse.namespace", "db")
                .put("index.lakehouse.table", "events")
        );
        IndexMetadata normalIdx = indexMetadata("logs", Settings.builder());

        ClusterState state = clusterState(Map.of("my_iceberg", lakehouseIdx, "logs", normalIdx));
        SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();

        contributor.contributeSchema(schema, state);

        Table registered = schema.getTable("my_iceberg");
        assertNotNull("Lakehouse index should be registered", registered);
        assertTrue(registered instanceof IcebergCalciteTable);

        assertNull("Normal index should not be registered by contributor", schema.getTable("logs"));
    }

    public void testContributeSchemaSkipsNormalIndices() {
        IndexMetadata normalIdx = indexMetadata("logs", Settings.builder());

        ClusterState state = clusterState(Map.of("logs", normalIdx));
        SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();

        contributor.contributeSchema(schema, state);

        assertNull(schema.getTable("logs"));
    }

    public void testContributeSchemaHandlesInvalidConfig() {
        // Missing required settings — fromIndexSettings will throw
        IndexMetadata badIdx = indexMetadata("bad_table", Settings.builder().put("index.lakehouse.enabled", true)
        // missing type, warehouse, namespace, table
        );

        ClusterState state = clusterState(Map.of("bad_table", badIdx));
        SchemaPlus schema = CalciteSchema.createRootSchema(true).plus();

        // Should not throw — error is caught and logged
        contributor.contributeSchema(schema, state);

        assertNull("Invalid config should not register a table", schema.getTable("bad_table"));
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
}

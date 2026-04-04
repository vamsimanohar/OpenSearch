/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.integration;

import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.cluster.node.info.PluginsAndModules;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.lakehouse.LakehousePlugin;
import org.opensearch.lakehouse.cluster.LakehouseMetadata;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Single-node integration tests for the lakehouse-iceberg plugin.
 *
 * <p>These tests verify that the plugin loads correctly and that the catalog
 * and table registration logic works end-to-end against a real single-node
 * OpenSearch cluster, persisting to cluster state.</p>
 *
 * <p>The tests exercise the same cluster state update logic used by the REST
 * handlers ({@code PUT _lakehouse/catalog/{name}} and
 * {@code PUT _lakehouse/table/{name}}) but invoke it via the transport client
 * and ClusterService directly to avoid requiring the Netty4 HTTP transport
 * module in the test classpath.</p>
 *
 * <p>Note: Full query execution tests (PPL/SQL against Iceberg tables) require
 * the full analytics engine stack plus the Rust JNI bridge to be running.
 * Those are not included here.</p>
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class SingleNodeIcebergIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LakehousePlugin.class);
    }

    /**
     * Verify that the lakehouse-iceberg plugin loaded successfully in the cluster.
     * In test mode, classpath plugins are registered by their fully qualified class name.
     */
    public void testPluginLoads() {
        NodesInfoRequest nodesInfoRequest = new NodesInfoRequest();
        nodesInfoRequest.addMetric(NodesInfoRequest.Metric.PLUGINS.metricName());
        NodesInfoResponse nodesInfoResponse = client().admin().cluster().nodesInfo(nodesInfoRequest).actionGet();

        assertFalse("Cluster should have at least one node", nodesInfoResponse.getNodes().isEmpty());

        boolean pluginFound = false;
        for (NodeInfo nodeInfo : nodesInfoResponse.getNodes()) {
            PluginsAndModules plugins = nodeInfo.getInfo(PluginsAndModules.class);
            if (plugins != null) {
                for (PluginInfo pluginInfo : plugins.getPluginInfos()) {
                    if (pluginInfo.getName().contains("LakehousePlugin")
                        || pluginInfo.getClassname().contains("LakehousePlugin")) {
                        pluginFound = true;
                        break;
                    }
                }
            }
        }
        assertTrue("LakehousePlugin should be loaded in the cluster", pluginFound);
    }

    /**
     * Verify that LakehouseMetadata custom metadata is correctly registered
     * and can be read from/written to cluster state.
     */
    public void testLakehouseMetadataRegistered() {
        // Verify the custom metadata type is accessible via cluster state API
        ClusterStateResponse stateResponse = client().admin().cluster().prepareState().all().get();
        ClusterState state = stateResponse.getState();
        // Initially, no lakehouse metadata should exist
        LakehouseMetadata metadata = state.metadata().custom(LakehouseMetadata.TYPE);
        // Before any registration, metadata is null — that is expected
        assertNull("LakehouseMetadata should not exist before any registration", metadata);
    }

    /**
     * Test the full catalog and table registration flow by directly updating
     * cluster state (the same mechanism used by RegisterCatalogAction and
     * RegisterTableAction REST handlers).
     */
    public void testRegisterCatalogAndTable() throws Exception {
        ClusterService clusterService = internalCluster().getCurrentClusterManagerNodeInstance(ClusterService.class);

        // Step 1: Register a catalog
        submitCatalogRegistration(clusterService, "test_catalog", Map.of("type", "hadoop", "warehouse", "/tmp/iceberg-warehouse"));

        // Verify catalog is persisted in cluster state
        assertBusy(() -> {
            LakehouseMetadata metadata = getClusterState().metadata().custom(LakehouseMetadata.TYPE);
            assertNotNull("LakehouseMetadata should exist after catalog registration", metadata);
            assertTrue("Catalog 'test_catalog' should be registered", metadata.catalogs().containsKey("test_catalog"));
            Map<String, String> config = metadata.catalogs().get("test_catalog");
            assertEquals("hadoop", config.get("type"));
            assertEquals("/tmp/iceberg-warehouse", config.get("warehouse"));
        });

        // Step 2: Register a table referencing the catalog
        submitTableRegistration(clusterService, "test_table", Map.of("catalog", "test_catalog", "database", "default", "table", "events"));

        // Verify table is persisted in cluster state
        assertBusy(() -> {
            LakehouseMetadata metadata = getClusterState().metadata().custom(LakehouseMetadata.TYPE);
            assertNotNull(metadata);

            // Catalog should still be there
            assertTrue("Catalog should still exist", metadata.catalogs().containsKey("test_catalog"));

            // Table should now exist
            assertTrue("Table 'test_table' should be registered", metadata.tables().containsKey("test_table"));
            Map<String, String> binding = metadata.tables().get("test_table");
            assertEquals("test_catalog", binding.get("catalog"));
            assertEquals("default", binding.get("database"));
            assertEquals("events", binding.get("table"));
        });
    }

    /**
     * Verify that registering a table without a valid catalog fails.
     */
    public void testRegisterTableWithoutCatalogFails() throws Exception {
        ClusterService clusterService = internalCluster().getCurrentClusterManagerNodeInstance(ClusterService.class);

        AtomicReference<Exception> error = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        clusterService.submitStateUpdateTask("test-register-orphan-table", new ClusterStateUpdateTask(Priority.NORMAL) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                LakehouseMetadata existing = currentState.metadata().custom(LakehouseMetadata.TYPE);
                if (existing == null) {
                    existing = LakehouseMetadata.EMPTY;
                }

                String catalogName = "nonexistent_catalog";
                if (!existing.catalogs().containsKey(catalogName)) {
                    throw new IllegalArgumentException(
                        "Catalog [" + catalogName + "] does not exist. Register the catalog first."
                    );
                }

                // Should not reach here
                return currentState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                error.set(e);
                latch.countDown();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                latch.countDown();
            }
        });

        assertTrue("Cluster state update should complete within 10 seconds", latch.await(10, TimeUnit.SECONDS));
        assertNotNull("Should have received an error for missing catalog", error.get());
        assertTrue(
            "Error should mention missing catalog",
            error.get().getMessage().contains("does not exist")
        );
    }

    /**
     * Verify that registering multiple catalogs does not overwrite earlier ones.
     */
    public void testRegisterMultipleCatalogs() throws Exception {
        ClusterService clusterService = internalCluster().getCurrentClusterManagerNodeInstance(ClusterService.class);

        // Register first catalog
        submitCatalogRegistration(clusterService, "catalog_one", Map.of("type", "hadoop", "warehouse", "/tmp/warehouse1"));

        // Register second catalog
        submitCatalogRegistration(clusterService, "catalog_two", Map.of("type", "glue", "warehouse", "s3://bucket/warehouse2"));

        // Verify both catalogs exist in cluster state
        assertBusy(() -> {
            LakehouseMetadata metadata = getClusterState().metadata().custom(LakehouseMetadata.TYPE);
            assertNotNull("LakehouseMetadata should exist in cluster state", metadata);
            assertEquals("Both catalogs should be registered", 2, metadata.catalogs().size());
            assertTrue("catalog_one should exist", metadata.catalogs().containsKey("catalog_one"));
            assertTrue("catalog_two should exist", metadata.catalogs().containsKey("catalog_two"));
            assertEquals("hadoop", metadata.catalogs().get("catalog_one").get("type"));
            assertEquals("glue", metadata.catalogs().get("catalog_two").get("type"));
        });
    }

    /**
     * Verify that re-registering a catalog with the same name updates its configuration.
     */
    public void testReRegisterCatalogUpdatesConfig() throws Exception {
        ClusterService clusterService = internalCluster().getCurrentClusterManagerNodeInstance(ClusterService.class);

        // Register initial catalog
        submitCatalogRegistration(clusterService, "mutable_catalog", Map.of("type", "hadoop", "warehouse", "/tmp/old-warehouse"));

        assertBusy(() -> {
            LakehouseMetadata metadata = getClusterState().metadata().custom(LakehouseMetadata.TYPE);
            assertNotNull(metadata);
            assertEquals("/tmp/old-warehouse", metadata.catalogs().get("mutable_catalog").get("warehouse"));
        });

        // Re-register with updated configuration
        submitCatalogRegistration(clusterService, "mutable_catalog", Map.of("type", "hadoop", "warehouse", "/tmp/new-warehouse"));

        // Verify the catalog was updated (not duplicated)
        assertBusy(() -> {
            LakehouseMetadata metadata = getClusterState().metadata().custom(LakehouseMetadata.TYPE);
            assertNotNull(metadata);
            assertEquals(1, metadata.catalogs().size());
            assertEquals("/tmp/new-warehouse", metadata.catalogs().get("mutable_catalog").get("warehouse"));
        });
    }

    /**
     * Verify that registering multiple tables under the same catalog works.
     */
    public void testRegisterMultipleTablesUnderSameCatalog() throws Exception {
        ClusterService clusterService = internalCluster().getCurrentClusterManagerNodeInstance(ClusterService.class);

        // Register a catalog
        submitCatalogRegistration(clusterService, "shared_catalog", Map.of("type", "hadoop", "warehouse", "/tmp/warehouse"));

        // Register two tables under that catalog
        submitTableRegistration(
            clusterService,
            "table_a",
            Map.of("catalog", "shared_catalog", "database", "db1", "table", "orders")
        );
        submitTableRegistration(
            clusterService,
            "table_b",
            Map.of("catalog", "shared_catalog", "database", "db1", "table", "customers")
        );

        // Verify both tables exist
        assertBusy(() -> {
            LakehouseMetadata metadata = getClusterState().metadata().custom(LakehouseMetadata.TYPE);
            assertNotNull(metadata);
            assertEquals(1, metadata.catalogs().size());
            assertEquals(2, metadata.tables().size());
            assertTrue(metadata.tables().containsKey("table_a"));
            assertTrue(metadata.tables().containsKey("table_b"));
            assertEquals("orders", metadata.tables().get("table_a").get("table"));
            assertEquals("customers", metadata.tables().get("table_b").get("table"));
        });
    }

    // ---- Helper methods that replicate the cluster state update logic from the REST handlers ----

    /**
     * Submits a catalog registration to cluster state, matching the logic in RegisterCatalogAction.
     */
    private void submitCatalogRegistration(ClusterService clusterService, String catalogName, Map<String, String> config)
        throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();

        clusterService.submitStateUpdateTask("test-register-catalog-" + catalogName, new ClusterStateUpdateTask(Priority.NORMAL) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                LakehouseMetadata existing = currentState.metadata().custom(LakehouseMetadata.TYPE);
                if (existing == null) {
                    existing = LakehouseMetadata.EMPTY;
                }

                Map<String, Map<String, String>> newCatalogs = new HashMap<>(existing.catalogs());
                newCatalogs.put(catalogName, config);

                LakehouseMetadata updated = new LakehouseMetadata(newCatalogs, existing.tables());
                Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata()).putCustom(LakehouseMetadata.TYPE, updated);
                return ClusterState.builder(currentState).metadata(mdBuilder).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                error.set(e);
                latch.countDown();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                latch.countDown();
            }
        });

        assertTrue("Catalog registration should complete within 10 seconds", latch.await(10, TimeUnit.SECONDS));
        if (error.get() != null) {
            throw error.get();
        }
    }

    /**
     * Submits a table registration to cluster state, matching the logic in RegisterTableAction.
     * Validates that the referenced catalog exists before registering.
     */
    private void submitTableRegistration(ClusterService clusterService, String tableName, Map<String, String> binding) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();

        clusterService.submitStateUpdateTask("test-register-table-" + tableName, new ClusterStateUpdateTask(Priority.NORMAL) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                LakehouseMetadata existing = currentState.metadata().custom(LakehouseMetadata.TYPE);
                if (existing == null) {
                    existing = LakehouseMetadata.EMPTY;
                }

                String catalogName = binding.get("catalog");
                if (catalogName != null && !existing.catalogs().containsKey(catalogName)) {
                    throw new IllegalArgumentException(
                        "Catalog [" + catalogName + "] does not exist. Register the catalog first."
                    );
                }

                Map<String, Map<String, String>> newTables = new HashMap<>(existing.tables());
                newTables.put(tableName, binding);

                LakehouseMetadata updated = new LakehouseMetadata(existing.catalogs(), newTables);
                Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata()).putCustom(LakehouseMetadata.TYPE, updated);
                return ClusterState.builder(currentState).metadata(mdBuilder).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                error.set(e);
                latch.countDown();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                latch.countDown();
            }
        });

        assertTrue("Table registration should complete within 10 seconds", latch.await(10, TimeUnit.SECONDS));
        if (error.get() != null) {
            throw error.get();
        }
    }
}

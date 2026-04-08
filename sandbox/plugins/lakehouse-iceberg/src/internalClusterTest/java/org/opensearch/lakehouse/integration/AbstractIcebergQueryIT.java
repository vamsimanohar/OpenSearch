/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.integration;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.dsl.DslQueryExecutorPlugin;
import org.opensearch.lakehouse.LakehousePlugin;
import org.opensearch.lakehouse.cluster.LakehouseMetadata;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.ppl.TestPPLPlugin;
import org.opensearch.ppl.action.PPLRequest;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.ppl.action.UnifiedPPLExecuteAction;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Base class for Iceberg SQL and PPL integration tests.
 *
 * <p>Sets up a single-node cluster with all required plugins, registers a Hadoop
 * catalog pointing to local test data, and provides helper methods for executing
 * SQL and PPL queries against Iceberg tables.</p>
 */
@ThreadLeakFilters(filters = { AbstractIcebergQueryIT.HadoopThreadLeakFilter.class })
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public abstract class AbstractIcebergQueryIT extends OpenSearchIntegTestCase {

    private static final Logger logger = LogManager.getLogger(AbstractIcebergQueryIT.class);

    protected static final String TABLE_NAME = "nyc_taxi";
    protected static final String CATALOG_NAME = "test_catalog";
    protected static final String WAREHOUSE_PATH = "/tmp/iceberg-test-warehouse";
    protected static final long TIMEOUT_SECONDS = 60;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // LakehousePlugin is NOT listed here — it's loaded via additionalNodePlugins()
        // with extendedPlugins=['analytics-engine class name'] so the SPI extension
        // mechanism properly wires IcebergSchemaContributor into AnalyticsPlugin.
        return List.of(
            AnalyticsPlugin.class,
            DataFusionPlugin.class,
            DslQueryExecutorPlugin.class,
            TestPPLPlugin.class,
            TestSqlPlugin.class
        );
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        // Provide LakehousePlugin with extendedPlugins so AnalyticsPlugin discovers
        // IcebergSchemaContributor via SPI extension loading.
        return List.of(
            new PluginInfo(
                LakehousePlugin.class.getName(),
                "lakehouse iceberg plugin",
                "NA",
                Version.CURRENT,
                "1.8",
                LakehousePlugin.class.getName(),
                null,
                List.of(AnalyticsPlugin.class.getName()),
                false
            )
        );
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        registerCatalogAndTableOnce();
    }

    /**
     * Registers the Hadoop catalog and test table if not already present in cluster state.
     * Checks actual cluster state rather than relying solely on a static flag, since the
     * test framework may restart the cluster between test class suites.
     */
    private void registerCatalogAndTableOnce() throws Exception {
        ClusterService clusterService = internalCluster().getCurrentClusterManagerNodeInstance(ClusterService.class);
        LakehouseMetadata current = clusterService.state().metadata().custom(LakehouseMetadata.TYPE);
        if (current != null && current.catalogs().containsKey(CATALOG_NAME)
                && current.tables().containsKey(TABLE_NAME)) {
            return; // Already registered in this cluster instance
        }

            // Register Hadoop catalog pointing to local warehouse
            submitClusterStateUpdate(clusterService, "register-test-catalog", currentState -> {
                LakehouseMetadata existing = currentState.metadata().custom(LakehouseMetadata.TYPE);
                if (existing == null) {
                    existing = LakehouseMetadata.EMPTY;
                }
                Map<String, Map<String, String>> catalogs = new HashMap<>(existing.catalogs());
                catalogs.put(CATALOG_NAME, Map.of(
                    "type", "hadoop",
                    "warehouse", "file://" + WAREHOUSE_PATH
                ));
                LakehouseMetadata updated = new LakehouseMetadata(catalogs, existing.tables());
                Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata())
                    .putCustom(LakehouseMetadata.TYPE, updated);
                return ClusterState.builder(currentState).metadata(mdBuilder).build();
            });

            // Register table
            submitClusterStateUpdate(clusterService, "register-test-table", currentState -> {
                LakehouseMetadata existing = currentState.metadata().custom(LakehouseMetadata.TYPE);
                if (existing == null) {
                    existing = LakehouseMetadata.EMPTY;
                }
                Map<String, Map<String, String>> tables = new HashMap<>(existing.tables());
                tables.put(TABLE_NAME, Map.of(
                    "catalog", CATALOG_NAME,
                    "namespace", "default",
                    "table", "test_events"
                ));
                LakehouseMetadata updated = new LakehouseMetadata(existing.catalogs(), tables);
                Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata())
                    .putCustom(LakehouseMetadata.TYPE, updated);
                return ClusterState.builder(currentState).metadata(mdBuilder).build();
            });

            logger.info("Registered catalog [{}] and table [{}] for integration tests", CATALOG_NAME, TABLE_NAME);
    }

    // ---- Query execution helpers ----

    /**
     * Executes a SQL query via the test transport action.
     */
    protected SqlResponse executeSql(String sql) {
        logger.info("[Test] Executing SQL: {}", sql);
        SqlRequest request = new SqlRequest(sql);
        return client().execute(TestSqlAction.INSTANCE, request).actionGet(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Executes a PPL query and returns the response.
     */
    protected PPLResponse executePpl(String ppl) {
        logger.info("[Test] Executing PPL: {}", ppl);
        PPLRequest request = new PPLRequest(ppl);
        return client().execute(UnifiedPPLExecuteAction.INSTANCE, request).actionGet(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    // ---- SQL assertion helpers ----

    protected void assertSqlNotEmpty(SqlResponse response) {
        assertNotNull("SQL response should not be null", response);
        assertNotNull("Columns should not be null", response.getColumns());
        assertFalse("Columns should not be empty", response.getColumns().isEmpty());
        assertFalse("Rows should not be empty", response.getRows().isEmpty());
    }

    protected void assertSqlHasRows(SqlResponse response) {
        assertNotNull("SQL response should not be null", response);
        assertFalse("Rows should not be empty", response.getRows().isEmpty());
    }

    protected void assertSqlColumnCount(SqlResponse response, int expected) {
        assertEquals("Column count mismatch", expected, response.getColumns().size());
    }

    protected void assertSqlRowCount(SqlResponse response, int expected) {
        assertEquals("Row count mismatch", expected, response.getTotal());
    }

    protected void assertSqlMaxRows(SqlResponse response, int maxRows) {
        assertTrue("Expected at most " + maxRows + " rows but got " + response.getTotal(),
            response.getTotal() <= maxRows);
    }

    protected void assertSqlSingleRow(SqlResponse response) {
        assertEquals("Expected exactly 1 row", 1, response.getTotal());
    }

    // ---- Value assertion helpers ----

    protected Object getSqlValue(SqlResponse response, int row, int col) {
        return response.getRows().get(row)[col];
    }

    protected double getSqlDouble(SqlResponse response, int row, int col) {
        Object val = getSqlValue(response, row, col);
        if (val instanceof Number) return ((Number) val).doubleValue();
        return Double.parseDouble(val.toString());
    }

    protected long getSqlLong(SqlResponse response, int row, int col) {
        Object val = getSqlValue(response, row, col);
        if (val instanceof Number) return ((Number) val).longValue();
        return Long.parseLong(val.toString());
    }

    protected String getSqlString(SqlResponse response, int row, int col) {
        Object val = getSqlValue(response, row, col);
        return val == null ? null : val.toString();
    }

    protected int getSqlColumnIndex(SqlResponse response, String columnName) {
        List<String> cols = response.getColumns();
        for (int i = 0; i < cols.size(); i++) {
            if (cols.get(i).equalsIgnoreCase(columnName)) return i;
        }
        fail("Column not found: " + columnName + " in " + cols);
        return -1;
    }

    protected void assertSqlValueEquals(String msg, long expected, SqlResponse response, int row, int col) {
        assertEquals(msg, expected, getSqlLong(response, row, col));
    }

    protected void assertSqlValueClose(String msg, double expected, SqlResponse response, int row, int col, double tolerance) {
        assertEquals(msg, expected, getSqlDouble(response, row, col), tolerance);
    }

    protected void assertSqlColumnOrdered(SqlResponse response, int col, boolean ascending) {
        List<Object[]> rows = response.getRows();
        for (int i = 1; i < rows.size(); i++) {
            double prev = ((Number) rows.get(i - 1)[col]).doubleValue();
            double curr = ((Number) rows.get(i)[col]).doubleValue();
            if (ascending) {
                assertTrue("Row " + i + " not in ASC order: " + prev + " > " + curr, prev <= curr);
            } else {
                assertTrue("Row " + i + " not in DESC order: " + prev + " < " + curr, prev >= curr);
            }
        }
    }

    protected void assertSqlAllRowsEqual(SqlResponse response, int col, Object expected) {
        for (int i = 0; i < response.getRows().size(); i++) {
            Object val = response.getRows().get(i)[col];
            if (expected instanceof Number && val instanceof Number) {
                assertEquals("Row " + i + " col " + col + " mismatch",
                    ((Number) expected).longValue(), ((Number) val).longValue());
            } else {
                assertEquals("Row " + i + " col " + col + " mismatch", expected, val);
            }
        }
    }

    protected void assertSqlAllRowsSatisfy(SqlResponse response, int col, java.util.function.Predicate<Object> predicate, String description) {
        for (int i = 0; i < response.getRows().size(); i++) {
            Object val = response.getRows().get(i)[col];
            assertTrue("Row " + i + " failed: " + description + " (value=" + val + ")", predicate.test(val));
        }
    }

    protected void assertSqlNoNulls(SqlResponse response, int col) {
        for (int i = 0; i < response.getRows().size(); i++) {
            assertNotNull("Row " + i + " col " + col + " should not be null", response.getRows().get(i)[col]);
        }
    }

    protected void assertSqlAllNulls(SqlResponse response, int col) {
        for (int i = 0; i < response.getRows().size(); i++) {
            assertNull("Row " + i + " col " + col + " should be null", response.getRows().get(i)[col]);
        }
    }

    // ---- PPL assertion helpers ----

    protected void assertPplNotEmpty(PPLResponse response) {
        assertNotNull("PPL response should not be null", response);
        assertNotNull("Columns should not be null", response.getColumns());
        assertFalse("Columns should not be empty", response.getColumns().isEmpty());
        assertFalse("Rows should not be empty", response.getRows().isEmpty());
    }

    protected void assertPplHasRows(PPLResponse response) {
        assertNotNull("PPL response should not be null", response);
        assertFalse("Rows should not be empty", response.getRows().isEmpty());
    }

    protected void assertPplColumnCount(PPLResponse response, int expected) {
        assertEquals("Column count mismatch", expected, response.getColumns().size());
    }

    protected void assertPplRowCount(PPLResponse response, int expected) {
        assertEquals("PPL row count mismatch", expected, response.getRows().size());
    }

    protected double getPplDouble(PPLResponse response, int row, int col) {
        Object val = response.getRows().get(row)[col];
        if (val instanceof Number) return ((Number) val).doubleValue();
        return Double.parseDouble(val.toString());
    }

    protected long getPplLong(PPLResponse response, int row, int col) {
        Object val = response.getRows().get(row)[col];
        if (val instanceof Number) return ((Number) val).longValue();
        return Long.parseLong(val.toString());
    }

    protected String getPplString(PPLResponse response, int row, int col) {
        Object val = response.getRows().get(row)[col];
        return val == null ? null : val.toString();
    }

    // ---- Cluster state helpers ----

    @FunctionalInterface
    protected interface ClusterStateTransform {
        ClusterState apply(ClusterState currentState);
    }

    private void submitClusterStateUpdate(ClusterService clusterService, String source, ClusterStateTransform transform)
        throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();

        clusterService.submitStateUpdateTask(source, new ClusterStateUpdateTask(Priority.NORMAL) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return transform.apply(currentState);
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

        assertTrue("Cluster state update [" + source + "] should complete within 10 seconds",
            latch.await(10, TimeUnit.SECONDS));
        if (error.get() != null) {
            throw error.get();
        }
    }

    /** Filters out Hadoop's StatisticsDataReferenceCleaner daemon thread that cannot be stopped. */
    public static class HadoopThreadLeakFilter implements com.carrotsearch.randomizedtesting.ThreadFilter {
        @Override
        public boolean reject(Thread t) {
            return t.getName().contains("StatisticsDataReferenceCleaner");
        }
    }
}

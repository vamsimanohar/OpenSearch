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
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.common.settings.Settings;
import org.opensearch.dsl.DslQueryExecutorPlugin;
import org.opensearch.Version;
import org.opensearch.lakehouse.LakehousePlugin;
import org.opensearch.lakehouse.action.LakehouseQueryAction;
import org.opensearch.lakehouse.action.LakehouseQueryRequest;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.List;

/**
 * Base class for lakehouse integration tests.
 * <p>
 * Sets up a single-node cluster with all required plugins and creates an
 * OpenSearch index with lakehouse settings pointing to a local Hadoop catalog
 * or a remote S3/Glue catalog.
 * <p>
 * The test data must be pre-generated at {@code /tmp/iceberg-test-warehouse}
 * using the pyiceberg script or a similar tool. The {@code test_events} table
 * contains a NYC-taxi-like schema with 20 columns and 5000 rows.
 * <p>
 * Supports two modes via the {@code lakehouse.test.mode} system property:
 * <ul>
 *   <li>{@code local} (default) -- uses a local Hadoop catalog with existing Parquet data</li>
 *   <li>{@code s3} -- uses a Glue catalog with S3 data (requires AWS credentials)</li>
 * </ul>
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
@ThreadLeakFilters(filters = { LakehouseIntegTestBase.HadoopThreadLeakFilter.class })
public abstract class LakehouseIntegTestBase extends OpenSearchIntegTestCase {

    private static final Logger logger = LogManager.getLogger(LakehouseIntegTestBase.class);

    /** Index name used in SQL and PPL queries. */
    protected static final String INDEX_NAME = "nyc_taxi";

    /** S3 warehouse path (used only when lakehouse.test.mode=s3). */
    private static final String S3_WAREHOUSE = "s3://iceberg-benchmark-test-263689514295/iceberg-warehouse";
    private static final String S3_NAMESPACE = "iceberg_benchmark_db";
    private static final String S3_TABLE = "hits";
    private static final String S3_REGION = "us-west-2";

    /**
     * Filter for Hadoop's StatisticsDataReferenceCleaner daemon thread
     * which is not properly shut down by Iceberg/Hadoop.
     */
    public static class HadoopThreadLeakFilter implements com.carrotsearch.randomizedtesting.ThreadFilter {
        @Override
        public boolean reject(Thread t) {
            String name = t.getName();
            return name != null && (name.contains("StatisticsDataReferenceCleaner") || name.contains("Hadoop"));
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            FlightStreamPlugin.class,
            AnalyticsPlugin.class,
            DataFusionPlugin.class,
            DslQueryExecutorPlugin.class
        );
    }

    /**
     * Provides LakehousePlugin with correct extendedPlugins metadata so that
     * AnalyticsPlugin discovers it as a SchemaContributor and ExternalTableExecutor
     * via the ExtensiblePlugin extension loading mechanism.
     * <p>
     * The default test framework creates PluginInfo with empty extendedPlugins,
     * which breaks the extension discovery. This override provides the correct metadata.
     */
    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            new PluginInfo(
                LakehousePlugin.class.getName(),
                "Lakehouse Iceberg plugin for integration tests",
                "NA",
                Version.CURRENT,
                "21",
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
        verifyTestData();
        createLakehouseIndex();
        ensureGreen();
    }

    /**
     * Verifies that local test data exists (only for local mode).
     */
    private void verifyTestData() {
        if (isS3Mode()) {
            return; // S3 data assumed to exist
        }
        IcebergTestDataGenerator.verifyTestData();
        logger.info("[LakehouseIT] Test data verified at {}", IcebergTestDataGenerator.WAREHOUSE);
    }

    /**
     * Creates the lakehouse index with appropriate settings.
     */
    private void createLakehouseIndex() {
        if (indexExists(INDEX_NAME)) {
            return;
        }
        Settings.Builder settings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.lakehouse.enabled", true);

        if (isS3Mode()) {
            settings.put("index.lakehouse.type", "glue")
                .put("index.lakehouse.region", S3_REGION)
                .put("index.lakehouse.warehouse", S3_WAREHOUSE)
                .put("index.lakehouse.namespace", S3_NAMESPACE)
                .put("index.lakehouse.table", S3_TABLE)
                .put("index.lakehouse.auth_type", "default");
        } else {
            settings.put("index.lakehouse.type", "hadoop")
                .put("index.lakehouse.warehouse", "file://" + IcebergTestDataGenerator.WAREHOUSE)
                .put("index.lakehouse.namespace", IcebergTestDataGenerator.NAMESPACE)
                .put("index.lakehouse.table", IcebergTestDataGenerator.TABLE_NAME);
        }

        prepareCreate(INDEX_NAME).setSettings(settings).get();
    }

    /**
     * Returns true if running in S3/Glue mode.
     */
    protected static boolean isS3Mode() {
        return "s3".equalsIgnoreCase(System.getProperty("lakehouse.test.mode", "local"));
    }

    // ---- Helper methods for subclasses ----

    /**
     * Executes a SQL query via the lakehouse transport action.
     *
     * @param query the SQL query text
     * @return the PPLResponse containing columns and rows
     */
    protected PPLResponse executeSql(String query) {
        logger.info("[LakehouseIT] SQL: {}", query);
        LakehouseQueryRequest request = new LakehouseQueryRequest(query, true);
        PPLResponse response = client().execute(LakehouseQueryAction.INSTANCE, request).actionGet();
        logger.info("[LakehouseIT] Result: {} columns, {} rows", response.getColumns().size(), response.getRows().size());
        return response;
    }

    /**
     * Executes a PPL query via the lakehouse transport action.
     *
     * @param query the PPL query text
     * @return the PPLResponse containing columns and rows
     */
    protected PPLResponse executePpl(String query) {
        logger.info("[LakehouseIT] PPL: {}", query);
        LakehouseQueryRequest request = new LakehouseQueryRequest(query, false);
        PPLResponse response = client().execute(LakehouseQueryAction.INSTANCE, request).actionGet();
        logger.info("[LakehouseIT] Result: {} columns, {} rows", response.getColumns().size(), response.getRows().size());
        return response;
    }

    /**
     * Asserts that the response has no error (non-null, has columns).
     */
    protected void assertNoError(PPLResponse response) {
        assertNotNull("Response should not be null", response);
        assertNotNull("Columns should not be null", response.getColumns());
        assertFalse("Columns should not be empty", response.getColumns().isEmpty());
        assertNotNull("Rows should not be null", response.getRows());
    }

    /**
     * Asserts that the response contains the expected number of rows.
     */
    protected void assertRowCount(PPLResponse response, int expected) {
        assertEquals("Row count mismatch", expected, response.getRows().size());
    }

    /**
     * Asserts that the response has at least the given number of rows.
     */
    protected void assertMinRowCount(PPLResponse response, int minRows) {
        assertTrue(
            "Expected at least " + minRows + " rows but got " + response.getRows().size(),
            response.getRows().size() >= minRows
        );
    }

    /**
     * Asserts that the response columns match the expected names (case-insensitive).
     */
    protected void assertColumnNames(PPLResponse response, String... expectedNames) {
        List<String> actual = response.getColumns();
        assertEquals("Column count mismatch", expectedNames.length, actual.size());
        for (int i = 0; i < expectedNames.length; i++) {
            assertEquals(
                "Column name mismatch at index " + i,
                expectedNames[i].toLowerCase(),
                actual.get(i).toLowerCase()
            );
        }
    }

    /**
     * Asserts that the first value of the first row equals the expected value.
     */
    protected void assertSingleValue(PPLResponse response, Object expected) {
        assertMinRowCount(response, 1);
        Object[] firstRow = response.getRows().get(0);
        assertTrue("First row should have at least one column", firstRow.length >= 1);
        assertEquals("Single value mismatch", expected, firstRow[0]);
    }

    /**
     * Returns the total row count in the test data.
     */
    protected int expectedTotalRows() {
        return IcebergTestDataGenerator.ROW_COUNT;
    }

}

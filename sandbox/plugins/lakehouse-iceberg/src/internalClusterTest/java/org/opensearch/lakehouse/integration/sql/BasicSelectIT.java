/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.integration.sql;

import org.opensearch.lakehouse.integration.AbstractIcebergQueryIT;
import org.opensearch.lakehouse.integration.SqlResponse;
import org.opensearch.test.OpenSearchIntegTestCase;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class BasicSelectIT extends AbstractIcebergQueryIT {

    public void testSelectStar() throws Exception {
        SqlResponse response = executeSql("SELECT * FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
        assertSqlColumnCount(response, 20);
    }

    public void testSelectSpecificColumns() throws Exception {
        SqlResponse response = executeSql("SELECT vendorid, trip_distance, total_amount FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 3);
        // Verify vendorid values are in the valid set {1, 2, 3}
        assertSqlAllRowsSatisfy(response, 0,
            v -> v instanceof Number && ((Number) v).intValue() >= 1 && ((Number) v).intValue() <= 3,
            "vendorid should be 1, 2, or 3");
    }

    public void testSelectWithAlias() throws Exception {
        SqlResponse response = executeSql("SELECT vendorid AS vendor, trip_distance AS dist FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 2);
        // Verify aliased column names
        assertTrue("First column should be 'vendor'", response.getColumns().get(0).equalsIgnoreCase("vendor"));
        assertTrue("Second column should be 'dist'", response.getColumns().get(1).equalsIgnoreCase("dist"));
    }

    public void testSelectDistinct() throws Exception {
        // vendorid has exactly 3 distinct values: 1, 2, 3
        SqlResponse response = executeSql("SELECT DISTINCT vendorid FROM " + TABLE_NAME);
        assertSqlNotEmpty(response);
        assertSqlRowCount(response, 3);
    }

    public void testSelectWithLimit() throws Exception {
        SqlResponse response = executeSql("SELECT * FROM " + TABLE_NAME + " LIMIT 5");
        assertSqlNotEmpty(response);
        assertSqlRowCount(response, 5);
    }

    public void testSelectCountStar() throws Exception {
        // Total rows = 5000
        SqlResponse response = executeSql("SELECT COUNT(*) FROM " + TABLE_NAME);
        assertSqlSingleRow(response);
        assertSqlColumnCount(response, 1);
        assertSqlValueEquals("COUNT(*) should be 5000", 5000, response, 0, 0);
    }

    public void testSelectLiteral() throws Exception {
        SqlResponse response = executeSql("SELECT 1 AS num, 'hello' AS greeting FROM " + TABLE_NAME + " LIMIT 1");
        assertSqlSingleRow(response);
        assertSqlColumnCount(response, 2);
        assertSqlValueEquals("Literal 1", 1, response, 0, 0);
        assertEquals("Literal 'hello'", "hello", getSqlString(response, 0, 1));
    }

    public void testSelectExpression() throws Exception {
        SqlResponse response = executeSql(
            "SELECT fare_amount, tip_amount, fare_amount + tip_amount AS total_with_tip FROM " + TABLE_NAME + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 3);
        // Verify expression: col2 = col0 + col1
        for (int i = 0; i < response.getRows().size(); i++) {
            double fare = getSqlDouble(response, i, 0);
            double tip = getSqlDouble(response, i, 1);
            double total = getSqlDouble(response, i, 2);
            assertEquals("Row " + i + ": fare + tip should equal total_with_tip", fare + tip, total, 0.01);
        }
    }

    public void testSelectAllColumnsExplicit() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, "
                + "ratecodeid, store_and_fwd_flag, pulocationid, dolocationid, payment_type, fare_amount, extra, "
                + "mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, "
                + "airport_fee, cbd_congestion_fee FROM "
                + TABLE_NAME
                + " LIMIT 5"
        );
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 20);
        assertSqlRowCount(response, 5);
    }

    public void testSelectWithTableAlias() throws Exception {
        SqlResponse response = executeSql("SELECT t.vendorid, t.trip_distance FROM " + TABLE_NAME + " t LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 2);
        // Values should be valid
        assertSqlAllRowsSatisfy(response, 0,
            v -> v instanceof Number && ((Number) v).intValue() >= 1 && ((Number) v).intValue() <= 3,
            "vendorid should be 1, 2, or 3");
        assertSqlAllRowsSatisfy(response, 1,
            v -> v instanceof Number && ((Number) v).doubleValue() >= 0.5,
            "trip_distance should be >= 0.5");
    }

    public void testSelectDistinctMultipleColumns() throws Exception {
        // 3 vendorids * 4 payment_types = 12 distinct combinations
        SqlResponse response = executeSql("SELECT DISTINCT vendorid, payment_type FROM " + TABLE_NAME);
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 2);
        assertSqlRowCount(response, 12);
    }

    public void testSelectWithNullHandling() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, COALESCE(congestion_surcharge, 0) AS surcharge FROM " + TABLE_NAME + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 2);
        // COALESCE should never return null
        assertSqlNoNulls(response, 1);
    }
}

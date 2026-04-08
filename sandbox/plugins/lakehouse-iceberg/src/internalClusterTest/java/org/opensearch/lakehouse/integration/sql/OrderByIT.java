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
public class OrderByIT extends AbstractIcebergQueryIT {

    public void testOrderByAsc() throws Exception {
        SqlResponse response = executeSql("SELECT trip_distance FROM " + TABLE_NAME + " ORDER BY trip_distance ASC LIMIT 5");
        assertSqlRowCount(response, 5);
        // Verify ascending order
        assertSqlColumnOrdered(response, 0, true);
        // First value should be near 0.5 (known min)
        assertTrue("First row should be near 0.5", getSqlDouble(response, 0, 0) < 1.0);
    }

    public void testOrderByDesc() throws Exception {
        SqlResponse response = executeSql("SELECT total_amount FROM " + TABLE_NAME + " ORDER BY total_amount DESC LIMIT 10");
        assertSqlRowCount(response, 10);
        assertSqlColumnOrdered(response, 0, false);
    }

    public void testOrderByDefault() throws Exception {
        // Default is ASC
        SqlResponse response = executeSql("SELECT fare_amount FROM " + TABLE_NAME + " ORDER BY fare_amount LIMIT 10");
        assertSqlRowCount(response, 10);
        assertSqlColumnOrdered(response, 0, true);
        // MIN(fare_amount) = 1.00
        assertSqlValueClose("First fare should be min", 1.00, response, 0, 0, 0.01);
    }

    public void testOrderByMultipleColumns() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, trip_distance FROM " + TABLE_NAME + " ORDER BY vendorid ASC, trip_distance DESC LIMIT 10"
        );
        assertSqlRowCount(response, 10);
        // First rows should all be vendorid=1 with desc trip_distance
        assertSqlAllRowsEqual(response, 0, 1);
        assertSqlColumnOrdered(response, 1, false);
    }

    public void testOrderByAlias() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, trip_distance AS dist FROM " + TABLE_NAME + " ORDER BY dist DESC LIMIT 5"
        );
        assertSqlRowCount(response, 5);
        // Top trip_distance should be ~64.9
        assertTrue("Top trip_distance should be > 60", getSqlDouble(response, 0, 1) > 60.0);
        assertSqlColumnOrdered(response, 1, false);
    }

    public void testOrderByExpression() throws Exception {
        SqlResponse response = executeSql(
            "SELECT fare_amount, tip_amount, fare_amount + tip_amount AS total_with_tip FROM " + TABLE_NAME
                + " ORDER BY total_with_tip DESC LIMIT 10"
        );
        assertSqlRowCount(response, 10);
        assertSqlColumnOrdered(response, 2, false);
    }

    public void testOrderByWithNulls() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, congestion_surcharge FROM " + TABLE_NAME + " ORDER BY congestion_surcharge LIMIT 20"
        );
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 20);
    }

    public void testOrderByNullsFirst() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, congestion_surcharge FROM " + TABLE_NAME + " ORDER BY congestion_surcharge NULLS FIRST LIMIT 20"
        );
        assertSqlRowCount(response, 20);
        // First rows should have NULL congestion_surcharge
        assertNull("First row should have NULL congestion_surcharge", getSqlValue(response, 0, 1));
    }

    public void testOrderByNullsLast() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, congestion_surcharge FROM " + TABLE_NAME + " ORDER BY congestion_surcharge NULLS LAST LIMIT 20"
        );
        assertSqlRowCount(response, 20);
        // First rows should NOT be NULL
        assertSqlNoNulls(response, 1);
    }

    public void testOrderByColumnOrdinal() throws Exception {
        SqlResponse response = executeSql("SELECT vendorid, trip_distance FROM " + TABLE_NAME + " ORDER BY 2 DESC LIMIT 10");
        assertSqlRowCount(response, 10);
        // ORDER BY 2 = ORDER BY trip_distance DESC
        assertSqlColumnOrdered(response, 1, false);
        assertTrue("Top trip_distance should be > 60", getSqlDouble(response, 0, 1) > 60.0);
    }
}

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
public class SubqueryIT extends AbstractIcebergQueryIT {

    public void testScalarSubquery() throws Exception {
        SqlResponse response = executeSql(
            "SELECT trip_distance, (SELECT AVG(trip_distance) FROM " + TABLE_NAME + ") AS avg_dist FROM " + TABLE_NAME + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
        // The scalar subquery column should be the same value for all rows (~13.987)
        double firstAvg = getSqlDouble(response, 0, 1);
        assertEquals("Scalar subquery should return AVG(trip_distance)", 13.987, firstAvg, 0.1);
        for (int i = 1; i < response.getRows().size(); i++) {
            assertEquals("Row " + i + ": scalar subquery should be constant",
                firstAvg, getSqlDouble(response, i, 1), 0.001);
        }
    }

    public void testInSubquery() throws Exception {
        // All 3 vendors have trips > 20 miles, so IN subquery returns all vendors
        SqlResponse response = executeSql(
            "SELECT DISTINCT vendorid FROM " + TABLE_NAME + " WHERE vendorid IN (SELECT DISTINCT vendorid FROM " + TABLE_NAME
                + " WHERE trip_distance > 20) ORDER BY vendorid"
        );
        assertSqlRowCount(response, 3);
        assertSqlValueEquals("vendorid=1", 1, response, 0, 0);
        assertSqlValueEquals("vendorid=2", 2, response, 1, 0);
        assertSqlValueEquals("vendorid=3", 3, response, 2, 0);
    }

    public void testExistsSubquery() throws Exception {
        // All vendors have trip_distance > 50, so EXISTS returns rows for all vendors
        SqlResponse response = executeSql(
            "SELECT DISTINCT vendorid FROM " + TABLE_NAME + " t WHERE EXISTS (SELECT 1 FROM " + TABLE_NAME
                + " WHERE vendorid = t.vendorid AND trip_distance > 50) ORDER BY vendorid"
        );
        assertSqlRowCount(response, 3);
    }

    public void testDerivedTable() throws Exception {
        // 3 vendors -> 3 avg_fare values
        SqlResponse response = executeSql(
            "SELECT avg_fare FROM (SELECT vendorid, AVG(fare_amount) AS avg_fare FROM " + TABLE_NAME
                + " GROUP BY vendorid) sub ORDER BY avg_fare"
        );
        assertSqlRowCount(response, 3);
        // All avg fares should be in the 40-45 range
        for (int i = 0; i < 3; i++) {
            double avgFare = getSqlDouble(response, i, 0);
            assertTrue("avg_fare should be > 40, got " + avgFare, avgFare > 40.0);
            assertTrue("avg_fare should be < 45, got " + avgFare, avgFare < 45.0);
        }
        assertSqlColumnOrdered(response, 0, true);
    }

    public void testSubqueryInFrom() throws Exception {
        SqlResponse response = executeSql(
            "SELECT sub.vendor, sub.cnt FROM (SELECT vendorid AS vendor, COUNT(*) AS cnt FROM " + TABLE_NAME
                + " GROUP BY vendorid) sub ORDER BY sub.cnt DESC"
        );
        assertSqlRowCount(response, 3);
        assertSqlColumnOrdered(response, 1, false);
        // Top counts should be 1667
        assertTrue("Top count >= 1666", getSqlLong(response, 0, 1) >= 1666);
    }

    public void testNestedSubqueries() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, trip_distance FROM (SELECT vendorid, trip_distance FROM " + TABLE_NAME
                + " WHERE trip_distance > 5 LIMIT 100) sub WHERE sub.vendorid = 1 LIMIT 10"
        );
        assertSqlNotEmpty(response);
        // All rows should have vendorid=1 and trip_distance > 5
        assertSqlAllRowsEqual(response, 0, 1);
        assertSqlAllRowsSatisfy(response, 1,
            v -> ((Number) v).doubleValue() > 5.0,
            "trip_distance should be > 5");
    }

    public void testSubqueryWithAggregation() throws Exception {
        // WHERE fare_amount > (SELECT AVG(fare_amount)) then GROUP BY vendorid
        // AVG(fare_amount) ~ 42.80, total rows above avg = 2400
        SqlResponse response = executeSql(
            "SELECT vendorid, COUNT(*) AS cnt FROM " + TABLE_NAME + " WHERE fare_amount > (SELECT AVG(fare_amount) FROM "
                + TABLE_NAME + ") GROUP BY vendorid ORDER BY vendorid"
        );
        assertSqlRowCount(response, 3);
        // Total across all vendors should be ~2400
        long total = 0;
        for (int i = 0; i < 3; i++) {
            total += getSqlLong(response, i, 1);
        }
        assertEquals("Total rows above avg fare should be 2400", 2400, total);
    }

    public void testCorrelatedSubquery() throws Exception {
        // Rows where fare > per-vendor avg fare
        SqlResponse response = executeSql(
            "SELECT vendorid, fare_amount FROM " + TABLE_NAME + " t WHERE fare_amount > (SELECT AVG(fare_amount) FROM "
                + TABLE_NAME + " WHERE vendorid = t.vendorid) LIMIT 10"
        );
        assertSqlNotEmpty(response);
        // All returned fares should be above some reasonable threshold (> 30)
        assertSqlAllRowsSatisfy(response, 1,
            v -> ((Number) v).doubleValue() > 30.0,
            "fare should be above vendor avg (~41-43)");
    }
}

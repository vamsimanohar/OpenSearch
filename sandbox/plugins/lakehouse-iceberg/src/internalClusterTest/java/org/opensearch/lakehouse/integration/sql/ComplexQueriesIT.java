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
public class ComplexQueriesIT extends AbstractIcebergQueryIT {

    public void testGroupByWithHavingAndOrderBy() throws Exception {
        // All 3 vendors have >1000 rows, so HAVING filters none
        SqlResponse response = executeSql(
            "SELECT vendorid, COUNT(*) AS cnt, AVG(fare_amount) AS avg_fare FROM " + TABLE_NAME
                + " GROUP BY vendorid HAVING COUNT(*) > 1000 ORDER BY avg_fare DESC"
        );
        assertSqlRowCount(response, 3);
        assertSqlColumnOrdered(response, 2, false);
        // All counts should be > 1000
        assertSqlAllRowsSatisfy(response, 1,
            v -> ((Number) v).longValue() > 1000,
            "count should be > 1000");
    }

    public void testNestedAggregations() throws Exception {
        // AVG of per-vendor counts: (1667+1667+1666)/3 = 1666.67
        SqlResponse response = executeSql(
            "SELECT AVG(cnt) AS avg_trips FROM (SELECT vendorid, COUNT(*) AS cnt FROM " + TABLE_NAME + " GROUP BY vendorid) sub"
        );
        assertSqlSingleRow(response);
        assertSqlValueClose("AVG of per-vendor counts", 1666.67, response, 0, 0, 0.1);
    }

    public void testMultipleCaseInSelect() throws Exception {
        SqlResponse response = executeSql(
            "SELECT CASE WHEN vendorid = 1 THEN 'CMT' ELSE 'VTS' END AS vendor, "
                + "CASE WHEN tip_amount > 0 THEN 'tipped' ELSE 'no_tip' END AS tip_status, COUNT(*) AS cnt FROM " + TABLE_NAME
                + " GROUP BY vendorid, CASE WHEN tip_amount > 0 THEN 'tipped' ELSE 'no_tip' END ORDER BY vendor, tip_status"
        );
        assertSqlNotEmpty(response);
        // vendor has 2 values (CMT, VTS), tip_status has 2 values -> up to 4 groups
        assertTrue("Should have at least 3 groups", response.getTotal() >= 3);
        // All counts should be positive
        for (int i = 0; i < response.getTotal(); i++) {
            assertTrue("Row " + i + ": count should be > 0", getSqlLong(response, i, 2) > 0);
        }
    }

    public void testComplexWhereWithParentheses() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, trip_distance, fare_amount FROM " + TABLE_NAME
                + " WHERE (vendorid = 1 AND trip_distance > 5) OR (vendorid = 2 AND fare_amount > 20) LIMIT 10"
        );
        assertSqlNotEmpty(response);
        for (int i = 0; i < response.getRows().size(); i++) {
            int vid = (int) getSqlLong(response, i, 0);
            double dist = getSqlDouble(response, i, 1);
            double fare = getSqlDouble(response, i, 2);
            boolean cond = (vid == 1 && dist > 5) || (vid == 2 && fare > 20);
            assertTrue("Row " + i + ": should match (vid=1 AND dist>5) OR (vid=2 AND fare>20)", cond);
        }
    }

    public void testWindowWithGroupBy() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendor, cnt, ROW_NUMBER() OVER (ORDER BY cnt DESC) AS rnk FROM (SELECT vendorid AS vendor, COUNT(*) AS cnt FROM "
                + TABLE_NAME + " GROUP BY vendorid) sub"
        );
        assertSqlRowCount(response, 3);
        // ROW_NUMBER values should be 1, 2, 3
        for (int i = 0; i < 3; i++) {
            long rnk = getSqlLong(response, i, 2);
            assertTrue("ROW_NUMBER should be 1-3, got " + rnk, rnk >= 1 && rnk <= 3);
        }
    }

    public void testSubqueryWithWindowFunction() throws Exception {
        SqlResponse response = executeSql(
            "SELECT * FROM (SELECT vendorid, trip_distance, ROW_NUMBER() OVER (PARTITION BY vendorid ORDER BY trip_distance DESC) AS rn FROM "
                + TABLE_NAME + " LIMIT 100) sub WHERE rn <= 3"
        );
        assertSqlNotEmpty(response);
        // All returned rows should have rn <= 3
        for (int i = 0; i < response.getRows().size(); i++) {
            long rn = getSqlLong(response, i, 2);
            assertTrue("Row " + i + ": rn should be <= 3, got " + rn, rn <= 3);
        }
    }

    public void testMultipleSubqueries() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, fare_amount, (SELECT AVG(fare_amount) FROM " + TABLE_NAME
                + ") AS global_avg, (SELECT MAX(fare_amount) FROM " + TABLE_NAME + ") AS global_max FROM " + TABLE_NAME + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
        // global_avg should be constant ~42.80
        double globalAvg = getSqlDouble(response, 0, 2);
        assertSqlValueClose("global_avg", 42.80, response, 0, 2, 0.1);
        // global_max should be constant 174.17
        assertSqlValueClose("global_max", 174.17, response, 0, 3, 0.1);
        // Verify they're constant across all rows
        for (int i = 1; i < response.getRows().size(); i++) {
            assertEquals("Row " + i + ": global_avg should be constant",
                globalAvg, getSqlDouble(response, i, 2), 0.001);
        }
    }

    public void testComplexExpressionInGroupBy() throws Exception {
        // Distance buckets: short(<2)=284, medium(2-10)=1560, long(>=10)=3156
        SqlResponse response = executeSql(
            "SELECT CASE WHEN trip_distance < 2 THEN 'short' WHEN trip_distance < 10 THEN 'medium' ELSE 'long' END AS dist_bucket, "
                + "COUNT(*) AS cnt, AVG(fare_amount) AS avg_fare FROM " + TABLE_NAME
                + " GROUP BY CASE WHEN trip_distance < 2 THEN 'short' WHEN trip_distance < 10 THEN 'medium' ELSE 'long' END "
                + "ORDER BY dist_bucket"
        );
        assertSqlRowCount(response, 3);
        // Verify bucket names and rough counts
        assertEquals("First bucket", "long", getSqlString(response, 0, 0).trim());
        assertEquals("Second bucket", "medium", getSqlString(response, 1, 0).trim());
        assertEquals("Third bucket", "short", getSqlString(response, 2, 0).trim());
        // Total across buckets should be 5000
        long total = getSqlLong(response, 0, 1) + getSqlLong(response, 1, 1) + getSqlLong(response, 2, 1);
        assertEquals("Total across buckets should be 5000", 5000, total);
    }

    public void testDeepNestedSubquery() throws Exception {
        SqlResponse response = executeSql(
            "SELECT avg_fare FROM (SELECT vendorid, AVG(fare_amount) AS avg_fare FROM (SELECT * FROM " + TABLE_NAME
                + " WHERE trip_distance > 1 LIMIT 1000) sub1 GROUP BY vendorid) sub2 ORDER BY avg_fare DESC"
        );
        assertSqlNotEmpty(response);
        assertSqlColumnOrdered(response, 0, false);
    }

    public void testAnalyticsStyleQuery() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, payment_type, COUNT(*) AS trips, AVG(trip_distance) AS avg_dist, SUM(total_amount) AS revenue, "
                + "AVG(tip_amount / NULLIF(total_amount, 0)) AS avg_tip_pct FROM " + TABLE_NAME
                + " WHERE fare_amount > 0 GROUP BY vendorid, payment_type ORDER BY revenue DESC LIMIT 20"
        );
        assertSqlNotEmpty(response);
        // All trips counts should be positive
        assertSqlAllRowsSatisfy(response, 2,
            v -> ((Number) v).longValue() > 0,
            "trip count should be > 0");
        // Revenue should be ordered descending
        assertSqlColumnOrdered(response, 4, false);
    }
}

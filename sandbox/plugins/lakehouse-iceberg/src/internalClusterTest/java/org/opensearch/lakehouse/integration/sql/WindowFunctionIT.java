/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.integration.sql;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.lakehouse.integration.AbstractIcebergQueryIT;
import org.opensearch.lakehouse.integration.SqlResponse;
import org.opensearch.test.OpenSearchIntegTestCase;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class WindowFunctionIT extends AbstractIcebergQueryIT {

    public void testRowNumber() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, trip_distance, ROW_NUMBER() OVER (ORDER BY trip_distance DESC) AS rn FROM " + TABLE_NAME + " LIMIT 20"
        );
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 3);
        assertSqlMaxRows(response, 20);
        // ROW_NUMBER should be sequential 1,2,3,...
        for (int i = 0; i < response.getRows().size(); i++) {
            assertSqlValueEquals("Row " + i + " ROW_NUMBER", i + 1, response, i, 2);
        }
        // trip_distance should be descending (ORDER BY DESC)
        assertSqlColumnOrdered(response, 1, false);
    }

    public void testRank() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, trip_distance, RANK() OVER (ORDER BY trip_distance DESC) AS rnk FROM " + TABLE_NAME + " LIMIT 20"
        );
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 3);
        // RANK of first row should be 1
        assertSqlValueEquals("First rank should be 1", 1, response, 0, 2);
        // trip_distance should be descending
        assertSqlColumnOrdered(response, 1, false);
    }

    public void testDenseRank() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, trip_distance, DENSE_RANK() OVER (ORDER BY trip_distance DESC) AS drnk FROM " + TABLE_NAME + " LIMIT 20"
        );
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 3);
        assertSqlValueEquals("First dense_rank should be 1", 1, response, 0, 2);
        assertSqlColumnOrdered(response, 1, false);
    }

    public void testPartitionBy() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, trip_distance, ROW_NUMBER() OVER (PARTITION BY vendorid ORDER BY trip_distance DESC) AS rn FROM "
                + TABLE_NAME + " LIMIT 20"
        );
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 3);
        // All row numbers should be >= 1
        assertSqlAllRowsSatisfy(response, 2,
            v -> ((Number) v).longValue() >= 1,
            "ROW_NUMBER should be >= 1");
    }

    @LuceneTestCase.AwaitsFix(bugUrl = "DataFusion decomposes SUM window into sub-expressions which fails in physical planner")
    public void testSumOver() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, fare_amount, SUM(fare_amount) OVER (PARTITION BY vendorid ORDER BY fare_amount) AS running_total FROM "
                + TABLE_NAME + " LIMIT 20"
        );
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 3);
        assertSqlMaxRows(response, 20);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = "DataFusion decomposes AVG window into SUM/COUNT which fails in physical planner")
    public void testAvgOver() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, fare_amount, AVG(fare_amount) OVER (PARTITION BY vendorid) AS avg_by_vendor FROM "
                + TABLE_NAME + " LIMIT 20"
        );
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 3);
        assertSqlMaxRows(response, 20);
    }

    public void testCountOver() throws Exception {
        // COUNT(*) OVER (PARTITION BY vendorid) should give the total per vendor
        SqlResponse response = executeSql(
            "SELECT vendorid, COUNT(*) OVER (PARTITION BY vendorid) AS vendor_count FROM " + TABLE_NAME + " LIMIT 20"
        );
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 2);
        // vendor_count should be either 1666 or 1667
        assertSqlAllRowsSatisfy(response, 1,
            v -> {
                long cnt = ((Number) v).longValue();
                return cnt == 1666 || cnt == 1667;
            },
            "vendor_count should be 1666 or 1667");
    }

    public void testLag() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, trip_distance, LAG(trip_distance) OVER (ORDER BY trip_distance) AS prev_dist FROM "
                + TABLE_NAME + " LIMIT 20"
        );
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 3);
        // First row's LAG should be NULL (no previous)
        assertNull("First LAG should be NULL", getSqlValue(response, 0, 2));
        // Subsequent rows: prev_dist should equal the previous row's trip_distance
        for (int i = 1; i < response.getRows().size(); i++) {
            double prevRowDist = getSqlDouble(response, i - 1, 1);
            double lagVal = getSqlDouble(response, i, 2);
            assertEquals("Row " + i + ": LAG should equal previous row's trip_distance",
                prevRowDist, lagVal, 0.001);
        }
    }

    public void testLead() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, trip_distance, LEAD(trip_distance) OVER (ORDER BY trip_distance) AS next_dist FROM "
                + TABLE_NAME + " LIMIT 20"
        );
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 3);
        // For rows 0..N-2: LEAD should equal the next row's trip_distance
        for (int i = 0; i < response.getRows().size() - 1; i++) {
            double nextRowDist = getSqlDouble(response, i + 1, 1);
            double leadVal = getSqlDouble(response, i, 2);
            assertEquals("Row " + i + ": LEAD should equal next row's trip_distance",
                nextRowDist, leadVal, 0.001);
        }
    }

    public void testNtile() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, trip_distance, NTILE(4) OVER (ORDER BY trip_distance) AS quartile FROM " + TABLE_NAME + " LIMIT 20"
        );
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 3);
        // NTILE(4) should produce values 1-4
        assertSqlAllRowsSatisfy(response, 2,
            v -> {
                long q = ((Number) v).longValue();
                return q >= 1 && q <= 4;
            },
            "NTILE(4) should be 1-4");
    }
}

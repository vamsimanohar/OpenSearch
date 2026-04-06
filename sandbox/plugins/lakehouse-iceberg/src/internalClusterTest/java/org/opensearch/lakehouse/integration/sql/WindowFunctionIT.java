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
    }

    public void testRank() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, trip_distance, RANK() OVER (ORDER BY trip_distance DESC) AS rnk FROM " + TABLE_NAME + " LIMIT 20"
        );
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 3);
        assertSqlMaxRows(response, 20);
    }

    public void testDenseRank() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, trip_distance, DENSE_RANK() OVER (ORDER BY trip_distance DESC) AS drnk FROM " + TABLE_NAME + " LIMIT 20"
        );
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 3);
        assertSqlMaxRows(response, 20);
    }

    public void testPartitionBy() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, trip_distance, ROW_NUMBER() OVER (PARTITION BY vendorid ORDER BY trip_distance DESC) AS rn FROM "
                + TABLE_NAME + " LIMIT 20"
        );
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 3);
        assertSqlMaxRows(response, 20);
    }

    public void testSumOver() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, fare_amount, SUM(fare_amount) OVER (PARTITION BY vendorid ORDER BY fare_amount) AS running_total FROM "
                + TABLE_NAME + " LIMIT 20"
        );
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 3);
        assertSqlMaxRows(response, 20);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/apache/datafusion/issues/15077")
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
        SqlResponse response = executeSql(
            "SELECT vendorid, COUNT(*) OVER (PARTITION BY vendorid) AS vendor_count FROM " + TABLE_NAME + " LIMIT 20"
        );
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 2);
        assertSqlMaxRows(response, 20);
    }

    public void testLag() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, trip_distance, LAG(trip_distance) OVER (ORDER BY trip_distance) AS prev_dist FROM "
                + TABLE_NAME + " LIMIT 20"
        );
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 3);
        assertSqlMaxRows(response, 20);
    }

    public void testLead() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, trip_distance, LEAD(trip_distance) OVER (ORDER BY trip_distance) AS next_dist FROM "
                + TABLE_NAME + " LIMIT 20"
        );
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 3);
        assertSqlMaxRows(response, 20);
    }

    public void testNtile() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, trip_distance, NTILE(4) OVER (ORDER BY trip_distance) AS quartile FROM " + TABLE_NAME + " LIMIT 20"
        );
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 3);
        assertSqlMaxRows(response, 20);
    }
}

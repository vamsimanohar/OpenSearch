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
        SqlResponse response = executeSql(
            "SELECT vendorid, COUNT(*) AS cnt, AVG(fare_amount) AS avg_fare FROM " + TABLE_NAME
                + " GROUP BY vendorid HAVING COUNT(*) > 1000 ORDER BY avg_fare DESC"
        );
        assertSqlNotEmpty(response);
        assertSqlHasRows(response);
    }

    public void testNestedAggregations() throws Exception {
        SqlResponse response = executeSql(
            "SELECT AVG(cnt) AS avg_trips FROM (SELECT vendorid, COUNT(*) AS cnt FROM " + TABLE_NAME + " GROUP BY vendorid) sub"
        );
        assertSqlNotEmpty(response);
        assertSqlHasRows(response);
    }

    public void testMultipleCaseInSelect() throws Exception {
        SqlResponse response = executeSql(
            "SELECT CASE WHEN vendorid = 1 THEN 'CMT' ELSE 'VTS' END AS vendor, "
                + "CASE WHEN tip_amount > 0 THEN 'tipped' ELSE 'no_tip' END AS tip_status, COUNT(*) FROM " + TABLE_NAME
                + " GROUP BY vendorid, CASE WHEN tip_amount > 0 THEN 'tipped' ELSE 'no_tip' END"
        );
        assertSqlNotEmpty(response);
        assertSqlHasRows(response);
    }

    public void testComplexWhereWithParentheses() throws Exception {
        SqlResponse response = executeSql(
            "SELECT * FROM " + TABLE_NAME
                + " WHERE (vendorid = 1 AND trip_distance > 5) OR (vendorid = 2 AND fare_amount > 20) LIMIT 10"
        );
        assertSqlNotEmpty(response);
        assertSqlHasRows(response);
    }

    public void testWindowWithGroupBy() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendor, cnt, ROW_NUMBER() OVER (ORDER BY cnt DESC) AS rnk FROM (SELECT vendorid AS vendor, COUNT(*) AS cnt FROM "
                + TABLE_NAME + " GROUP BY vendorid) sub"
        );
        assertSqlNotEmpty(response);
        assertSqlHasRows(response);
    }

    public void testSubqueryWithWindowFunction() throws Exception {
        SqlResponse response = executeSql(
            "SELECT * FROM (SELECT vendorid, trip_distance, ROW_NUMBER() OVER (PARTITION BY vendorid ORDER BY trip_distance DESC) AS rn FROM "
                + TABLE_NAME + " LIMIT 100) sub WHERE rn <= 3"
        );
        assertSqlNotEmpty(response);
        assertSqlHasRows(response);
    }

    public void testMultipleSubqueries() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, fare_amount, (SELECT AVG(fare_amount) FROM " + TABLE_NAME
                + ") AS global_avg, (SELECT MAX(fare_amount) FROM " + TABLE_NAME + ") AS global_max FROM " + TABLE_NAME + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
        assertSqlHasRows(response);
    }

    public void testComplexExpressionInGroupBy() throws Exception {
        SqlResponse response = executeSql(
            "SELECT CASE WHEN trip_distance < 2 THEN 'short' WHEN trip_distance < 10 THEN 'medium' ELSE 'long' END AS dist_bucket, "
                + "COUNT(*), AVG(fare_amount) FROM " + TABLE_NAME
                + " GROUP BY CASE WHEN trip_distance < 2 THEN 'short' WHEN trip_distance < 10 THEN 'medium' ELSE 'long' END"
        );
        assertSqlNotEmpty(response);
        assertSqlHasRows(response);
    }

    public void testDeepNestedSubquery() throws Exception {
        SqlResponse response = executeSql(
            "SELECT avg_fare FROM (SELECT vendorid, AVG(fare_amount) AS avg_fare FROM (SELECT * FROM " + TABLE_NAME
                + " WHERE trip_distance > 1 LIMIT 1000) sub1 GROUP BY vendorid) sub2 ORDER BY avg_fare DESC"
        );
        assertSqlNotEmpty(response);
        assertSqlHasRows(response);
    }

    public void testAnalyticsStyleQuery() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, payment_type, COUNT(*) AS trips, AVG(trip_distance) AS avg_dist, SUM(total_amount) AS revenue, "
                + "AVG(tip_amount / NULLIF(total_amount, 0)) AS avg_tip_pct FROM " + TABLE_NAME
                + " WHERE fare_amount > 0 GROUP BY vendorid, payment_type ORDER BY revenue DESC LIMIT 20"
        );
        assertSqlNotEmpty(response);
        assertSqlHasRows(response);
    }
}

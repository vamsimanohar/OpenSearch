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
public class AggregationIT extends AbstractIcebergQueryIT {

    public void testCount() throws Exception {
        SqlResponse response = executeSql("SELECT COUNT(*) AS cnt FROM " + TABLE_NAME);
        assertSqlSingleRow(response);
        assertSqlColumnCount(response, 1);
    }

    public void testCountColumn() throws Exception {
        SqlResponse response = executeSql("SELECT COUNT(congestion_surcharge) AS cnt FROM " + TABLE_NAME);
        assertSqlSingleRow(response);
        assertSqlColumnCount(response, 1);
    }

    public void testCountDistinct() throws Exception {
        SqlResponse response = executeSql("SELECT COUNT(DISTINCT vendorid) AS cnt FROM " + TABLE_NAME);
        assertSqlSingleRow(response);
        assertSqlColumnCount(response, 1);
    }

    public void testSum() throws Exception {
        SqlResponse response = executeSql("SELECT SUM(total_amount) AS total FROM " + TABLE_NAME);
        assertSqlSingleRow(response);
        assertSqlColumnCount(response, 1);
    }

    public void testAvg() throws Exception {
        SqlResponse response = executeSql("SELECT AVG(trip_distance) AS avg_dist FROM " + TABLE_NAME);
        assertSqlSingleRow(response);
        assertSqlColumnCount(response, 1);
    }

    public void testMin() throws Exception {
        SqlResponse response = executeSql("SELECT MIN(fare_amount) AS min_fare FROM " + TABLE_NAME);
        assertSqlSingleRow(response);
        assertSqlColumnCount(response, 1);
    }

    public void testMax() throws Exception {
        SqlResponse response = executeSql("SELECT MAX(fare_amount) AS max_fare FROM " + TABLE_NAME);
        assertSqlSingleRow(response);
        assertSqlColumnCount(response, 1);
    }

    public void testMultipleAggregations() throws Exception {
        SqlResponse response = executeSql("SELECT COUNT(*), SUM(total_amount), AVG(trip_distance) FROM " + TABLE_NAME);
        assertSqlSingleRow(response);
        assertSqlColumnCount(response, 3);
    }

    public void testGroupBy() throws Exception {
        SqlResponse response = executeSql("SELECT vendorid, COUNT(*) AS cnt FROM " + TABLE_NAME + " GROUP BY vendorid");
        assertSqlHasRows(response);
    }

    public void testGroupByMultipleColumns() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, payment_type, COUNT(*) FROM " + TABLE_NAME + " GROUP BY vendorid, payment_type"
        );
        assertSqlHasRows(response);
    }

    public void testGroupByWithHaving() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, COUNT(*) AS cnt FROM " + TABLE_NAME + " GROUP BY vendorid HAVING COUNT(*) > 100"
        );
        assertSqlHasRows(response);
    }

    public void testGroupByWithOrderBy() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, COUNT(*) AS cnt FROM " + TABLE_NAME + " GROUP BY vendorid ORDER BY cnt DESC"
        );
        assertSqlHasRows(response);
    }

    public void testGroupByWithSum() throws Exception {
        SqlResponse response = executeSql(
            "SELECT payment_type, SUM(total_amount) AS total FROM " + TABLE_NAME + " GROUP BY payment_type"
        );
        assertSqlHasRows(response);
    }

    public void testGroupByWithAvg() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, AVG(fare_amount) AS avg_fare FROM " + TABLE_NAME + " GROUP BY vendorid"
        );
        assertSqlHasRows(response);
    }

    public void testGroupByWithMinMax() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, MIN(trip_distance), MAX(trip_distance) FROM " + TABLE_NAME + " GROUP BY vendorid"
        );
        assertSqlHasRows(response);
    }

    public void testGroupByWithMultipleAggs() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, COUNT(*), AVG(fare_amount), SUM(tip_amount) FROM " + TABLE_NAME + " GROUP BY vendorid"
        );
        assertSqlHasRows(response);
    }

    public void testGroupByWithAlias() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid AS vendor, COUNT(*) AS trips FROM " + TABLE_NAME + " GROUP BY vendorid"
        );
        assertSqlHasRows(response);
    }

    public void testHavingWithMultipleConditions() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, COUNT(*) AS cnt, AVG(fare_amount) AS avg_f FROM "
                + TABLE_NAME
                + " GROUP BY vendorid HAVING cnt > 100 AND avg_f > 10"
        );
        assertSqlHasRows(response);
    }
}

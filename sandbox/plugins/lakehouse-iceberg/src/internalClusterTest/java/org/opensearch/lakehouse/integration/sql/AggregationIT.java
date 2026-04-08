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

import java.util.HashSet;
import java.util.Set;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class AggregationIT extends AbstractIcebergQueryIT {

    public void testCount() throws Exception {
        SqlResponse response = executeSql("SELECT COUNT(*) AS cnt FROM " + TABLE_NAME);
        assertSqlSingleRow(response);
        assertSqlValueEquals("COUNT(*) should be 5000", 5000, response, 0, 0);
    }

    public void testCountColumn() throws Exception {
        // 500 rows have NULL congestion_surcharge (every 10th row where i%10==5)
        SqlResponse response = executeSql("SELECT COUNT(congestion_surcharge) AS cnt FROM " + TABLE_NAME);
        assertSqlSingleRow(response);
        assertSqlValueEquals("COUNT(congestion_surcharge) should be 4500", 4500, response, 0, 0);
    }

    public void testCountDistinct() throws Exception {
        SqlResponse response = executeSql("SELECT COUNT(DISTINCT vendorid) AS cnt FROM " + TABLE_NAME);
        assertSqlSingleRow(response);
        assertSqlValueEquals("COUNT(DISTINCT vendorid) should be 3", 3, response, 0, 0);
    }

    public void testSum() throws Exception {
        SqlResponse response = executeSql("SELECT SUM(total_amount) AS total FROM " + TABLE_NAME);
        assertSqlSingleRow(response);
        assertSqlValueClose("SUM(total_amount)", 243109.17, response, 0, 0, 1.0);
    }

    public void testAvg() throws Exception {
        SqlResponse response = executeSql("SELECT AVG(trip_distance) AS avg_dist FROM " + TABLE_NAME);
        assertSqlSingleRow(response);
        assertSqlValueClose("AVG(trip_distance)", 13.987, response, 0, 0, 0.01);
    }

    public void testMin() throws Exception {
        SqlResponse response = executeSql("SELECT MIN(fare_amount) AS min_fare FROM " + TABLE_NAME);
        assertSqlSingleRow(response);
        assertSqlValueClose("MIN(fare_amount)", 1.00, response, 0, 0, 0.01);
    }

    public void testMax() throws Exception {
        SqlResponse response = executeSql("SELECT MAX(fare_amount) AS max_fare FROM " + TABLE_NAME);
        assertSqlSingleRow(response);
        assertSqlValueClose("MAX(fare_amount)", 174.17, response, 0, 0, 0.01);
    }

    public void testMultipleAggregations() throws Exception {
        SqlResponse response = executeSql("SELECT COUNT(*), SUM(total_amount), AVG(trip_distance) FROM " + TABLE_NAME);
        assertSqlSingleRow(response);
        assertSqlColumnCount(response, 3);
        assertSqlValueEquals("COUNT(*)", 5000, response, 0, 0);
        assertSqlValueClose("SUM(total_amount)", 243109.17, response, 0, 1, 1.0);
        assertSqlValueClose("AVG(trip_distance)", 13.987, response, 0, 2, 0.01);
    }

    public void testGroupBy() throws Exception {
        // vendorid has 3 groups: 1->1667, 2->1667, 3->1666
        SqlResponse response = executeSql("SELECT vendorid, COUNT(*) AS cnt FROM " + TABLE_NAME + " GROUP BY vendorid ORDER BY vendorid");
        assertSqlRowCount(response, 3);
        assertSqlValueEquals("vendorid=1", 1, response, 0, 0);
        assertSqlValueEquals("vendorid=1 count", 1667, response, 0, 1);
        assertSqlValueEquals("vendorid=2", 2, response, 1, 0);
        assertSqlValueEquals("vendorid=2 count", 1667, response, 1, 1);
        assertSqlValueEquals("vendorid=3", 3, response, 2, 0);
        assertSqlValueEquals("vendorid=3 count", 1666, response, 2, 1);
    }

    public void testGroupByMultipleColumns() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, payment_type, COUNT(*) FROM " + TABLE_NAME + " GROUP BY vendorid, payment_type"
        );
        // 3 vendors * 4 payment types = 12 groups
        assertSqlRowCount(response, 12);
    }

    public void testGroupByWithHaving() throws Exception {
        // All 3 vendors have >100 rows, so HAVING COUNT(*)>100 returns all 3
        SqlResponse response = executeSql(
            "SELECT vendorid, COUNT(*) AS cnt FROM " + TABLE_NAME + " GROUP BY vendorid HAVING COUNT(*) > 100 ORDER BY vendorid"
        );
        assertSqlRowCount(response, 3);
        // Verify all counts > 100
        assertSqlAllRowsSatisfy(response, 1,
            v -> v instanceof Number && ((Number) v).longValue() > 100,
            "count should be > 100");
    }

    public void testGroupByWithOrderBy() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, COUNT(*) AS cnt FROM " + TABLE_NAME + " GROUP BY vendorid ORDER BY cnt DESC"
        );
        assertSqlRowCount(response, 3);
        // Verify descending order
        assertSqlColumnOrdered(response, 1, false);
    }

    public void testGroupByWithSum() throws Exception {
        SqlResponse response = executeSql(
            "SELECT payment_type, SUM(total_amount) AS total FROM " + TABLE_NAME + " GROUP BY payment_type ORDER BY payment_type"
        );
        assertSqlRowCount(response, 4);
        // payment_type=1 sum is ~73178.81
        assertSqlValueClose("payment_type=1 sum", 73178.81, response, 0, 1, 1.0);
    }

    public void testGroupByWithAvg() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, AVG(fare_amount) AS avg_fare FROM " + TABLE_NAME + " GROUP BY vendorid ORDER BY vendorid"
        );
        assertSqlRowCount(response, 3);
        assertSqlValueClose("vendorid=1 avg_fare", 41.81, response, 0, 1, 0.1);
        assertSqlValueClose("vendorid=2 avg_fare", 43.18, response, 1, 1, 0.1);
        assertSqlValueClose("vendorid=3 avg_fare", 43.40, response, 2, 1, 0.1);
    }

    public void testGroupByWithMinMax() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, MIN(trip_distance), MAX(trip_distance) FROM " + TABLE_NAME + " GROUP BY vendorid ORDER BY vendorid"
        );
        assertSqlRowCount(response, 3);
        // All vendors should have min around 0.5 and max around 64
        for (int i = 0; i < 3; i++) {
            double min = getSqlDouble(response, i, 1);
            double max = getSqlDouble(response, i, 2);
            assertTrue("Min should be near 0.5, got " + min, min < 1.5);
            assertTrue("Max should be > 50, got " + max, max > 50);
            assertTrue("Max should be > min", max > min);
        }
    }

    public void testGroupByWithMultipleAggs() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, COUNT(*), AVG(fare_amount), SUM(tip_amount) FROM " + TABLE_NAME + " GROUP BY vendorid ORDER BY vendorid"
        );
        assertSqlRowCount(response, 3);
        // vendorid=1: cnt=1667, avg_fare~41.81, sum_tip~3585.36
        assertSqlValueEquals("vendorid=1 count", 1667, response, 0, 1);
        assertSqlValueClose("vendorid=1 avg_fare", 41.81, response, 0, 2, 0.1);
        assertSqlValueClose("vendorid=1 sum_tip", 3585.36, response, 0, 3, 1.0);
    }

    public void testGroupByWithAlias() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid AS vendor, COUNT(*) AS trips FROM " + TABLE_NAME + " GROUP BY vendorid ORDER BY vendor"
        );
        assertSqlRowCount(response, 3);
        assertTrue("Column should be aliased as 'vendor'", response.getColumns().get(0).equalsIgnoreCase("vendor"));
        assertTrue("Column should be aliased as 'trips'", response.getColumns().get(1).equalsIgnoreCase("trips"));
    }

    public void testHavingWithMultipleConditions() throws Exception {
        // All 3 vendors have cnt>100 and avg_fare>10, so all 3 should be returned
        SqlResponse response = executeSql(
            "SELECT vendorid, COUNT(*) AS cnt, AVG(fare_amount) AS avg_f FROM "
                + TABLE_NAME
                + " GROUP BY vendorid HAVING cnt > 100 AND avg_f > 10"
        );
        assertSqlRowCount(response, 3);
    }
}

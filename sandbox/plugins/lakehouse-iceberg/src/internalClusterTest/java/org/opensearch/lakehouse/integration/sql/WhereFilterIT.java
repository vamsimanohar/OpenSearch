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
public class WhereFilterIT extends AbstractIcebergQueryIT {

    // vendorid=1 has 1667 rows
    public void testWhereEquals() throws Exception {
        SqlResponse response = executeSql("SELECT vendorid, trip_distance FROM " + TABLE_NAME + " WHERE vendorid = 1 LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
        // Every returned row must have vendorid=1
        assertSqlAllRowsEqual(response, 0, 1);
    }

    public void testWhereNotEquals() throws Exception {
        SqlResponse response = executeSql("SELECT vendorid FROM " + TABLE_NAME + " WHERE vendorid <> 1 LIMIT 10");
        assertSqlNotEmpty(response);
        // No row should have vendorid=1
        assertSqlAllRowsSatisfy(response, 0,
            v -> v instanceof Number && ((Number) v).intValue() != 1,
            "vendorid should not be 1");
    }

    public void testWhereGreaterThan() throws Exception {
        SqlResponse response = executeSql("SELECT trip_distance FROM " + TABLE_NAME + " WHERE trip_distance > 10 LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlAllRowsSatisfy(response, 0,
            v -> v instanceof Number && ((Number) v).doubleValue() > 10.0,
            "trip_distance should be > 10");
    }

    public void testWhereLessThan() throws Exception {
        SqlResponse response = executeSql("SELECT fare_amount FROM " + TABLE_NAME + " WHERE fare_amount < 5 LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlAllRowsSatisfy(response, 0,
            v -> v instanceof Number && ((Number) v).doubleValue() < 5.0,
            "fare_amount should be < 5");
    }

    public void testWhereGreaterThanOrEqual() throws Exception {
        SqlResponse response = executeSql("SELECT passenger_count FROM " + TABLE_NAME + " WHERE passenger_count >= 5 LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlAllRowsSatisfy(response, 0,
            v -> v instanceof Number && ((Number) v).intValue() >= 5,
            "passenger_count should be >= 5");
    }

    public void testWhereLessThanOrEqual() throws Exception {
        SqlResponse response = executeSql("SELECT tip_amount FROM " + TABLE_NAME + " WHERE tip_amount <= 0 LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlAllRowsSatisfy(response, 0,
            v -> v instanceof Number && ((Number) v).doubleValue() <= 0.0,
            "tip_amount should be <= 0");
    }

    public void testWhereAnd() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, trip_distance FROM " + TABLE_NAME + " WHERE vendorid = 1 AND trip_distance > 5 LIMIT 10"
        );
        assertSqlNotEmpty(response);
        assertSqlAllRowsEqual(response, 0, 1);
        assertSqlAllRowsSatisfy(response, 1,
            v -> v instanceof Number && ((Number) v).doubleValue() > 5.0,
            "trip_distance should be > 5");
    }

    public void testWhereOr() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid FROM " + TABLE_NAME + " WHERE vendorid = 1 OR vendorid = 2 LIMIT 10"
        );
        assertSqlNotEmpty(response);
        assertSqlAllRowsSatisfy(response, 0,
            v -> v instanceof Number && (((Number) v).intValue() == 1 || ((Number) v).intValue() == 2),
            "vendorid should be 1 or 2");
    }

    public void testWhereNot() throws Exception {
        SqlResponse response = executeSql("SELECT vendorid FROM " + TABLE_NAME + " WHERE NOT vendorid = 1 LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlAllRowsSatisfy(response, 0,
            v -> v instanceof Number && ((Number) v).intValue() != 1,
            "vendorid should not be 1");
    }

    public void testWhereIn() throws Exception {
        SqlResponse response = executeSql("SELECT payment_type FROM " + TABLE_NAME + " WHERE payment_type IN (1, 2, 3) LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlAllRowsSatisfy(response, 0,
            v -> {
                int pt = ((Number) v).intValue();
                return pt == 1 || pt == 2 || pt == 3;
            },
            "payment_type should be in {1,2,3}");
    }

    public void testWhereNotIn() throws Exception {
        SqlResponse response = executeSql("SELECT payment_type FROM " + TABLE_NAME + " WHERE payment_type NOT IN (1, 2) LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlAllRowsSatisfy(response, 0,
            v -> {
                int pt = ((Number) v).intValue();
                return pt != 1 && pt != 2;
            },
            "payment_type should not be in {1,2}");
    }

    public void testWhereBetween() throws Exception {
        SqlResponse response = executeSql("SELECT fare_amount FROM " + TABLE_NAME + " WHERE fare_amount BETWEEN 10 AND 50 LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlAllRowsSatisfy(response, 0,
            v -> {
                double fa = ((Number) v).doubleValue();
                return fa >= 10.0 && fa <= 50.0;
            },
            "fare_amount should be between 10 and 50");
    }

    // 500 rows have NULL congestion_surcharge
    public void testWhereIsNull() throws Exception {
        SqlResponse response = executeSql(
            "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE congestion_surcharge IS NULL"
        );
        assertSqlSingleRow(response);
        assertSqlValueEquals("IS NULL count should be 500", 500, response, 0, 0);
    }

    // 4500 rows have non-NULL congestion_surcharge
    public void testWhereIsNotNull() throws Exception {
        SqlResponse response = executeSql(
            "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE congestion_surcharge IS NOT NULL"
        );
        assertSqlSingleRow(response);
        assertSqlValueEquals("IS NOT NULL count should be 4500", 4500, response, 0, 0);
    }

    // store_and_fwd_flag alternates Y/N, 2500 each
    public void testWhereLike() throws Exception {
        SqlResponse response = executeSql(
            "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE store_and_fwd_flag LIKE 'Y%'"
        );
        assertSqlSingleRow(response);
        assertSqlValueEquals("LIKE 'Y%' count should be 2500", 2500, response, 0, 0);
    }
}

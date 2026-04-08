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
public class TypeCastIT extends AbstractIcebergQueryIT {

    public void testCastIntToDouble() throws Exception {
        SqlResponse response = executeSql("SELECT CAST(vendorid AS DOUBLE) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 1);
        // vendorid values are 1,2,3 — cast to double should be 1.0, 2.0, or 3.0
        assertSqlAllRowsSatisfy(response, 0,
            v -> {
                double d = ((Number) v).doubleValue();
                return d == 1.0 || d == 2.0 || d == 3.0;
            },
            "CAST(vendorid AS DOUBLE) should be 1.0, 2.0, or 3.0");
    }

    public void testCastDoubleToInt() throws Exception {
        SqlResponse response = executeSql("SELECT CAST(trip_distance AS INTEGER) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 1);
        // All cast values should be non-negative integers
        assertSqlAllRowsSatisfy(response, 0,
            v -> v instanceof Number && ((Number) v).longValue() >= 0,
            "CAST(trip_distance AS INTEGER) should be >= 0");
    }

    public void testCastToVarchar() throws Exception {
        SqlResponse response = executeSql("SELECT CAST(vendorid AS VARCHAR) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 1);
        // Should produce '1', '2', or '3'
        assertSqlAllRowsSatisfy(response, 0,
            v -> {
                String s = v.toString().trim();
                return "1".equals(s) || "2".equals(s) || "3".equals(s);
            },
            "CAST(vendorid AS VARCHAR) should be '1', '2', or '3'");
    }

    public void testCastTimestampToDate() throws Exception {
        SqlResponse response = executeSql("SELECT CAST(tpep_pickup_datetime AS DATE) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 1);
        // Date may come back as epoch days (int) or string — just check non-null
        assertSqlNoNulls(response, 0);
    }

    public void testCastStringToInt() throws Exception {
        SqlResponse response = executeSql("SELECT CAST('123' AS INTEGER) FROM " + TABLE_NAME + " LIMIT 1");
        assertSqlSingleRow(response);
        assertSqlValueEquals("CAST('123' AS INTEGER) should be 123", 123, response, 0, 0);
    }

    public void testCastInExpression() throws Exception {
        SqlResponse response = executeSql("SELECT CAST(vendorid AS DOUBLE) + 0.5 FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 1);
        // Values should be 1.5, 2.5, or 3.5
        assertSqlAllRowsSatisfy(response, 0,
            v -> {
                double d = ((Number) v).doubleValue();
                return d == 1.5 || d == 2.5 || d == 3.5;
            },
            "CAST(vendorid AS DOUBLE)+0.5 should be 1.5, 2.5, or 3.5");
    }

    public void testCastBigintToInt() throws Exception {
        SqlResponse response = executeSql("SELECT CAST(passenger_count AS INTEGER) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 1);
        // passenger_count is 1-6
        assertSqlAllRowsSatisfy(response, 0,
            v -> {
                long pc = ((Number) v).longValue();
                return pc >= 1 && pc <= 6;
            },
            "CAST(passenger_count AS INTEGER) should be 1-6");
    }

    public void testCastInWhere() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid FROM " + TABLE_NAME + " WHERE CAST(vendorid AS BIGINT) = 1 LIMIT 10"
        );
        assertSqlNotEmpty(response);
        assertSqlAllRowsEqual(response, 0, 1);
    }
}

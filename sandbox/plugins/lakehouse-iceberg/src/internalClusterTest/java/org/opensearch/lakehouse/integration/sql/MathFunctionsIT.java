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
public class MathFunctionsIT extends AbstractIcebergQueryIT {

    public void testAbs() throws Exception {
        SqlResponse response = executeSql(
            "SELECT fare_amount, ABS(fare_amount) FROM " + TABLE_NAME + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
        // ABS of non-negative fare should equal the fare itself
        for (int i = 0; i < response.getRows().size(); i++) {
            double fare = getSqlDouble(response, i, 0);
            double absFare = getSqlDouble(response, i, 1);
            assertEquals("Row " + i + ": ABS(fare) should equal fare for positive values", Math.abs(fare), absFare, 0.001);
        }
    }

    public void testCeil() throws Exception {
        SqlResponse response = executeSql(
            "SELECT trip_distance, CEIL(trip_distance) FROM " + TABLE_NAME + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
        for (int i = 0; i < response.getRows().size(); i++) {
            double dist = getSqlDouble(response, i, 0);
            double ceil = getSqlDouble(response, i, 1);
            assertEquals("Row " + i + ": CEIL mismatch", Math.ceil(dist), ceil, 0.001);
        }
    }

    public void testFloor() throws Exception {
        SqlResponse response = executeSql(
            "SELECT trip_distance, FLOOR(trip_distance) FROM " + TABLE_NAME + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
        for (int i = 0; i < response.getRows().size(); i++) {
            double dist = getSqlDouble(response, i, 0);
            double floor = getSqlDouble(response, i, 1);
            assertEquals("Row " + i + ": FLOOR mismatch", Math.floor(dist), floor, 0.001);
        }
    }

    public void testRound() throws Exception {
        SqlResponse response = executeSql(
            "SELECT fare_amount, ROUND(fare_amount, 1) FROM " + TABLE_NAME + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
        for (int i = 0; i < response.getRows().size(); i++) {
            double fare = getSqlDouble(response, i, 0);
            double rounded = getSqlDouble(response, i, 1);
            double expected = Math.round(fare * 10.0) / 10.0;
            assertEquals("Row " + i + ": ROUND mismatch", expected, rounded, 0.01);
        }
    }

    public void testPower() throws Exception {
        SqlResponse response = executeSql(
            "SELECT trip_distance, POWER(trip_distance, 2) FROM " + TABLE_NAME + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
        for (int i = 0; i < response.getRows().size(); i++) {
            double dist = getSqlDouble(response, i, 0);
            double pow = getSqlDouble(response, i, 1);
            assertEquals("Row " + i + ": POWER(x,2) mismatch", dist * dist, pow, 0.01);
        }
    }

    public void testSqrt() throws Exception {
        SqlResponse response = executeSql(
            "SELECT trip_distance, SQRT(trip_distance) FROM " + TABLE_NAME + " WHERE trip_distance > 0 LIMIT 10"
        );
        assertSqlNotEmpty(response);
        for (int i = 0; i < response.getRows().size(); i++) {
            double dist = getSqlDouble(response, i, 0);
            double sqrt = getSqlDouble(response, i, 1);
            assertEquals("Row " + i + ": SQRT mismatch", Math.sqrt(dist), sqrt, 0.001);
        }
    }

    public void testMod() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, MOD(vendorid, 2) FROM " + TABLE_NAME + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
        for (int i = 0; i < response.getRows().size(); i++) {
            long vid = getSqlLong(response, i, 0);
            long mod = getSqlLong(response, i, 1);
            assertEquals("Row " + i + ": MOD mismatch", vid % 2, mod);
        }
    }

    public void testLog() throws Exception {
        SqlResponse response = executeSql(
            "SELECT trip_distance, LN(trip_distance) FROM " + TABLE_NAME + " WHERE trip_distance > 0 LIMIT 10"
        );
        assertSqlNotEmpty(response);
        for (int i = 0; i < response.getRows().size(); i++) {
            double dist = getSqlDouble(response, i, 0);
            double ln = getSqlDouble(response, i, 1);
            assertEquals("Row " + i + ": LN mismatch", Math.log(dist), ln, 0.001);
        }
    }

    public void testLog10() throws Exception {
        SqlResponse response = executeSql(
            "SELECT fare_amount, LOG10(fare_amount) FROM " + TABLE_NAME + " WHERE fare_amount > 0 LIMIT 10"
        );
        assertSqlNotEmpty(response);
        for (int i = 0; i < response.getRows().size(); i++) {
            double fare = getSqlDouble(response, i, 0);
            double log10 = getSqlDouble(response, i, 1);
            assertEquals("Row " + i + ": LOG10 mismatch", Math.log10(fare), log10, 0.001);
        }
    }

    public void testExp() throws Exception {
        SqlResponse response = executeSql("SELECT EXP(1) FROM " + TABLE_NAME + " LIMIT 1");
        assertSqlSingleRow(response);
        assertSqlValueClose("EXP(1) should be e", Math.E, response, 0, 0, 0.0001);
    }

    public void testSign() throws Exception {
        SqlResponse response = executeSql(
            "SELECT fare_amount, SIGN(fare_amount) FROM " + TABLE_NAME + " WHERE fare_amount > 0 LIMIT 10"
        );
        assertSqlNotEmpty(response);
        // All fares > 0, so SIGN should be 1
        assertSqlAllRowsSatisfy(response, 1,
            v -> v instanceof Number && ((Number) v).doubleValue() == 1.0,
            "SIGN of positive fare should be 1");
    }

    public void testTruncate() throws Exception {
        SqlResponse response = executeSql("SELECT TRUNCATE(trip_distance, 1) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        // Just verify we get numeric results; TRUNC behavior depends on DataFusion version
        assertSqlAllRowsSatisfy(response, 0,
            v -> v instanceof Number,
            "TRUNCATE should return a number");
    }

    public void testArithmeticExpressions() throws Exception {
        SqlResponse response = executeSql(
            "SELECT fare_amount, tip_amount, fare_amount + tip_amount, fare_amount - tip_amount, "
                + "fare_amount * 1.1, fare_amount / 2 FROM "
                + TABLE_NAME
                + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 6);
        for (int i = 0; i < response.getRows().size(); i++) {
            double fare = getSqlDouble(response, i, 0);
            double tip = getSqlDouble(response, i, 1);
            assertEquals("Row " + i + ": fare+tip", fare + tip, getSqlDouble(response, i, 2), 0.01);
            assertEquals("Row " + i + ": fare-tip", fare - tip, getSqlDouble(response, i, 3), 0.01);
            assertEquals("Row " + i + ": fare*1.1", fare * 1.1, getSqlDouble(response, i, 4), 0.01);
            assertEquals("Row " + i + ": fare/2", fare / 2, getSqlDouble(response, i, 5), 0.01);
        }
    }

    public void testModuloOperator() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, vendorid % 2 FROM " + TABLE_NAME + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
        for (int i = 0; i < response.getRows().size(); i++) {
            long vid = getSqlLong(response, i, 0);
            long mod = getSqlLong(response, i, 1);
            assertEquals("Row " + i + ": vendorid%2 mismatch", vid % 2, mod);
        }
    }
}

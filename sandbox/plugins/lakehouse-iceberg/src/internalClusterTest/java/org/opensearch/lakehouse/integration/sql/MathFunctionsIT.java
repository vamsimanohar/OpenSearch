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
        SqlResponse response = executeSql("SELECT ABS(fare_amount) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
    }

    public void testCeil() throws Exception {
        SqlResponse response = executeSql("SELECT CEIL(trip_distance) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
    }

    public void testFloor() throws Exception {
        SqlResponse response = executeSql("SELECT FLOOR(trip_distance) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
    }

    public void testRound() throws Exception {
        SqlResponse response = executeSql("SELECT ROUND(fare_amount, 1) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
    }

    public void testPower() throws Exception {
        SqlResponse response = executeSql("SELECT POWER(trip_distance, 2) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
    }

    public void testSqrt() throws Exception {
        SqlResponse response = executeSql("SELECT SQRT(trip_distance) FROM " + TABLE_NAME + " WHERE trip_distance > 0 LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
    }

    public void testMod() throws Exception {
        SqlResponse response = executeSql("SELECT MOD(vendorid, 2) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
    }

    public void testLog() throws Exception {
        SqlResponse response = executeSql("SELECT LN(trip_distance) FROM " + TABLE_NAME + " WHERE trip_distance > 0 LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
    }

    public void testLog10() throws Exception {
        SqlResponse response = executeSql("SELECT LOG10(fare_amount) FROM " + TABLE_NAME + " WHERE fare_amount > 0 LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
    }

    public void testExp() throws Exception {
        SqlResponse response = executeSql("SELECT EXP(1) FROM " + TABLE_NAME + " LIMIT 1");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 1);
    }

    public void testSign() throws Exception {
        SqlResponse response = executeSql("SELECT SIGN(fare_amount) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
    }

    public void testTruncate() throws Exception {
        SqlResponse response = executeSql("SELECT TRUNCATE(trip_distance, 1) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
    }

    public void testArithmeticExpressions() throws Exception {
        SqlResponse response = executeSql(
            "SELECT fare_amount + tip_amount, fare_amount - tip_amount, fare_amount * 1.1, fare_amount / 2 FROM "
                + TABLE_NAME
                + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 4);
        assertSqlMaxRows(response, 10);
    }

    public void testModuloOperator() throws Exception {
        SqlResponse response = executeSql("SELECT vendorid % 2 FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
    }
}

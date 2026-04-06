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
        assertSqlMaxRows(response, 10);
    }

    public void testCastDoubleToInt() throws Exception {
        SqlResponse response = executeSql("SELECT CAST(trip_distance AS INTEGER) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 1);
        assertSqlMaxRows(response, 10);
    }

    public void testCastToVarchar() throws Exception {
        SqlResponse response = executeSql("SELECT CAST(vendorid AS VARCHAR) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 1);
        assertSqlMaxRows(response, 10);
    }

    public void testCastTimestampToDate() throws Exception {
        SqlResponse response = executeSql("SELECT CAST(tpep_pickup_datetime AS DATE) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 1);
        assertSqlMaxRows(response, 10);
    }

    public void testCastStringToInt() throws Exception {
        SqlResponse response = executeSql("SELECT CAST('123' AS INTEGER) FROM " + TABLE_NAME + " LIMIT 1");
        assertSqlNotEmpty(response);
        assertSqlSingleRow(response);
    }

    public void testCastInExpression() throws Exception {
        SqlResponse response = executeSql("SELECT CAST(vendorid AS DOUBLE) + 0.5 FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 1);
        assertSqlMaxRows(response, 10);
    }

    public void testCastBigintToInt() throws Exception {
        SqlResponse response = executeSql("SELECT CAST(passenger_count AS INTEGER) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 1);
        assertSqlMaxRows(response, 10);
    }

    public void testCastInWhere() throws Exception {
        SqlResponse response = executeSql("SELECT * FROM " + TABLE_NAME + " WHERE CAST(vendorid AS BIGINT) = 1 LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
    }
}

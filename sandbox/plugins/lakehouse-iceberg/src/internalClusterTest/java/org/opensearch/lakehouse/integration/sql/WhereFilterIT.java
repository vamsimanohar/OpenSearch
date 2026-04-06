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

    public void testWhereEquals() throws Exception {
        SqlResponse response = executeSql("SELECT * FROM " + TABLE_NAME + " WHERE vendorid = 1 LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
    }

    public void testWhereNotEquals() throws Exception {
        SqlResponse response = executeSql("SELECT * FROM " + TABLE_NAME + " WHERE vendorid <> 1 LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
    }

    public void testWhereGreaterThan() throws Exception {
        SqlResponse response = executeSql("SELECT * FROM " + TABLE_NAME + " WHERE trip_distance > 10 LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
    }

    public void testWhereLessThan() throws Exception {
        SqlResponse response = executeSql("SELECT * FROM " + TABLE_NAME + " WHERE fare_amount < 5 LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
    }

    public void testWhereGreaterThanOrEqual() throws Exception {
        SqlResponse response = executeSql("SELECT * FROM " + TABLE_NAME + " WHERE passenger_count >= 5 LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
    }

    public void testWhereLessThanOrEqual() throws Exception {
        SqlResponse response = executeSql("SELECT * FROM " + TABLE_NAME + " WHERE tip_amount <= 0 LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
    }

    public void testWhereAnd() throws Exception {
        SqlResponse response = executeSql("SELECT * FROM " + TABLE_NAME + " WHERE vendorid = 1 AND trip_distance > 5 LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
    }

    public void testWhereOr() throws Exception {
        SqlResponse response = executeSql("SELECT * FROM " + TABLE_NAME + " WHERE vendorid = 1 OR vendorid = 2 LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
    }

    public void testWhereNot() throws Exception {
        SqlResponse response = executeSql("SELECT * FROM " + TABLE_NAME + " WHERE NOT vendorid = 1 LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
    }

    public void testWhereIn() throws Exception {
        SqlResponse response = executeSql("SELECT * FROM " + TABLE_NAME + " WHERE payment_type IN (1, 2, 3) LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
    }

    public void testWhereNotIn() throws Exception {
        SqlResponse response = executeSql("SELECT * FROM " + TABLE_NAME + " WHERE payment_type NOT IN (1, 2) LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
    }

    public void testWhereBetween() throws Exception {
        SqlResponse response = executeSql("SELECT * FROM " + TABLE_NAME + " WHERE fare_amount BETWEEN 10 AND 50 LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
    }

    public void testWhereIsNull() throws Exception {
        SqlResponse response = executeSql("SELECT * FROM " + TABLE_NAME + " WHERE congestion_surcharge IS NULL LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
    }

    public void testWhereIsNotNull() throws Exception {
        SqlResponse response = executeSql("SELECT * FROM " + TABLE_NAME + " WHERE congestion_surcharge IS NOT NULL LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
    }

    public void testWhereLike() throws Exception {
        SqlResponse response = executeSql("SELECT * FROM " + TABLE_NAME + " WHERE store_and_fwd_flag LIKE 'Y%' LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
    }
}

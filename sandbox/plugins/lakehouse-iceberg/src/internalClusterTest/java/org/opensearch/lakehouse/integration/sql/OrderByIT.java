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
public class OrderByIT extends AbstractIcebergQueryIT {

    public void testOrderByAsc() throws Exception {
        SqlResponse response = executeSql("SELECT * FROM " + TABLE_NAME + " ORDER BY trip_distance ASC LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
    }

    public void testOrderByDesc() throws Exception {
        SqlResponse response = executeSql("SELECT * FROM " + TABLE_NAME + " ORDER BY total_amount DESC LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
    }

    public void testOrderByDefault() throws Exception {
        SqlResponse response = executeSql("SELECT * FROM " + TABLE_NAME + " ORDER BY fare_amount LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
    }

    public void testOrderByMultipleColumns() throws Exception {
        SqlResponse response = executeSql(
            "SELECT * FROM " + TABLE_NAME + " ORDER BY vendorid ASC, trip_distance DESC LIMIT 10"
        );
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
    }

    public void testOrderByAlias() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, trip_distance AS dist FROM " + TABLE_NAME + " ORDER BY dist DESC LIMIT 10"
        );
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
    }

    public void testOrderByExpression() throws Exception {
        SqlResponse response = executeSql(
            "SELECT *, fare_amount + tip_amount AS total_with_tip FROM " + TABLE_NAME + " ORDER BY total_with_tip DESC LIMIT 10"
        );
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
    }

    public void testOrderByWithNulls() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, congestion_surcharge FROM " + TABLE_NAME + " ORDER BY congestion_surcharge LIMIT 20"
        );
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 20);
    }

    public void testOrderByNullsFirst() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, congestion_surcharge FROM " + TABLE_NAME + " ORDER BY congestion_surcharge NULLS FIRST LIMIT 20"
        );
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 20);
    }

    public void testOrderByNullsLast() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, congestion_surcharge FROM " + TABLE_NAME + " ORDER BY congestion_surcharge NULLS LAST LIMIT 20"
        );
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 20);
    }

    public void testOrderByColumnOrdinal() throws Exception {
        SqlResponse response = executeSql("SELECT vendorid, trip_distance FROM " + TABLE_NAME + " ORDER BY 2 DESC LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
    }
}

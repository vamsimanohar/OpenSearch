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
public class SetOperationsIT extends AbstractIcebergQueryIT {

    public void testUnion() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, trip_distance FROM " + TABLE_NAME + " WHERE vendorid = 1 LIMIT 5 UNION SELECT vendorid, trip_distance FROM "
                + TABLE_NAME + " WHERE vendorid = 2 LIMIT 5"
        );
        assertSqlNotEmpty(response);
        assertSqlHasRows(response);
    }

    public void testUnionAll() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid FROM " + TABLE_NAME + " WHERE vendorid = 1 LIMIT 5 UNION ALL SELECT vendorid FROM "
                + TABLE_NAME + " WHERE vendorid = 2 LIMIT 5"
        );
        assertSqlNotEmpty(response);
        assertSqlHasRows(response);
    }

    public void testIntersect() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid FROM " + TABLE_NAME + " WHERE trip_distance > 10 LIMIT 10 INTERSECT SELECT vendorid FROM "
                + TABLE_NAME + " WHERE fare_amount > 30 LIMIT 10"
        );
        assertSqlNotEmpty(response);
        assertSqlHasRows(response);
    }

    public void testExcept() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid FROM " + TABLE_NAME + " LIMIT 10 EXCEPT SELECT vendorid FROM " + TABLE_NAME
                + " WHERE vendorid = 1 LIMIT 10"
        );
        assertSqlNotEmpty(response);
        assertSqlHasRows(response);
    }

    public void testUnionWithAggregation() throws Exception {
        SqlResponse response = executeSql(
            "SELECT 'short' AS category, COUNT(*) FROM " + TABLE_NAME
                + " WHERE trip_distance < 2 UNION ALL SELECT 'long', COUNT(*) FROM " + TABLE_NAME + " WHERE trip_distance >= 10"
        );
        assertSqlNotEmpty(response);
        assertSqlHasRows(response);
    }
}

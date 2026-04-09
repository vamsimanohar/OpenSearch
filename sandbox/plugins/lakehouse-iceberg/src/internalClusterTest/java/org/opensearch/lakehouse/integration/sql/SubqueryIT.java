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
public class SubqueryIT extends AbstractIcebergQueryIT {

    public void testScalarSubquery() throws Exception {
        SqlResponse response = executeSql(
            "SELECT *, (SELECT AVG(trip_distance) FROM " + TABLE_NAME + ") AS avg_dist FROM " + TABLE_NAME + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
        assertSqlHasRows(response);
    }

    public void testInSubquery() throws Exception {
        SqlResponse response = executeSql(
            "SELECT * FROM " + TABLE_NAME + " WHERE vendorid IN (SELECT DISTINCT vendorid FROM " + TABLE_NAME
                + " WHERE trip_distance > 20) LIMIT 10"
        );
        assertSqlNotEmpty(response);
        assertSqlHasRows(response);
    }

    public void testExistsSubquery() throws Exception {
        SqlResponse response = executeSql(
            "SELECT * FROM " + TABLE_NAME + " t WHERE EXISTS (SELECT 1 FROM " + TABLE_NAME
                + " WHERE vendorid = t.vendorid AND trip_distance > 50) LIMIT 10"
        );
        assertSqlNotEmpty(response);
        assertSqlHasRows(response);
    }

    public void testDerivedTable() throws Exception {
        SqlResponse response = executeSql(
            "SELECT avg_fare FROM (SELECT vendorid, AVG(fare_amount) AS avg_fare FROM " + TABLE_NAME
                + " GROUP BY vendorid) sub LIMIT 10"
        );
        assertSqlNotEmpty(response);
        assertSqlHasRows(response);
    }

    public void testSubqueryInFrom() throws Exception {
        SqlResponse response = executeSql(
            "SELECT sub.vendor, sub.cnt FROM (SELECT vendorid AS vendor, COUNT(*) AS cnt FROM " + TABLE_NAME
                + " GROUP BY vendorid) sub ORDER BY sub.cnt DESC"
        );
        assertSqlNotEmpty(response);
        assertSqlHasRows(response);
    }

    public void testNestedSubqueries() throws Exception {
        SqlResponse response = executeSql(
            "SELECT * FROM (SELECT vendorid, trip_distance FROM " + TABLE_NAME
                + " WHERE trip_distance > 5 LIMIT 100) sub WHERE sub.vendorid = 1 LIMIT 10"
        );
        assertSqlNotEmpty(response);
        assertSqlHasRows(response);
    }

    public void testSubqueryWithAggregation() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, COUNT(*) FROM " + TABLE_NAME + " WHERE fare_amount > (SELECT AVG(fare_amount) FROM "
                + TABLE_NAME + ") GROUP BY vendorid"
        );
        assertSqlNotEmpty(response);
        assertSqlHasRows(response);
    }

    public void testCorrelatedSubquery() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, fare_amount FROM " + TABLE_NAME + " t WHERE fare_amount > (SELECT AVG(fare_amount) FROM "
                + TABLE_NAME + " WHERE vendorid = t.vendorid) LIMIT 10"
        );
        assertSqlNotEmpty(response);
        assertSqlHasRows(response);
    }
}

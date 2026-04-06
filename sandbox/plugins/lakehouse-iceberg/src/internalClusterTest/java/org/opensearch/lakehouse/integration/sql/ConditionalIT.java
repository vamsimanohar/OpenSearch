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
public class ConditionalIT extends AbstractIcebergQueryIT {

    public void testCaseWhen() throws Exception {
        SqlResponse response = executeSql(
            "SELECT CASE WHEN vendorid = 1 THEN 'CMT' WHEN vendorid = 2 THEN 'VTS' ELSE 'Other' END FROM "
                + TABLE_NAME
                + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
    }

    public void testCaseWhenInGroupBy() throws Exception {
        SqlResponse response = executeSql(
            "SELECT CASE WHEN vendorid = 1 THEN 'CMT' ELSE 'VTS' END AS vendor_name, COUNT(*) FROM "
                + TABLE_NAME
                + " GROUP BY CASE WHEN vendorid = 1 THEN 'CMT' ELSE 'VTS' END"
        );
        assertSqlHasRows(response);
        assertSqlColumnCount(response, 2);
    }

    public void testCoalesce() throws Exception {
        SqlResponse response = executeSql("SELECT COALESCE(congestion_surcharge, 0) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
    }

    public void testNullIf() throws Exception {
        SqlResponse response = executeSql("SELECT NULLIF(vendorid, 1) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
    }

    public void testNestedCase() throws Exception {
        SqlResponse response = executeSql(
            "SELECT CASE WHEN trip_distance < 1 THEN 'short' WHEN trip_distance < 5 THEN 'medium' "
                + "WHEN trip_distance < 10 THEN 'long' ELSE 'very_long' END FROM "
                + TABLE_NAME
                + " LIMIT 20"
        );
        assertSqlNotEmpty(response);
    }

    public void testCaseWithAggregation() throws Exception {
        SqlResponse response = executeSql(
            "SELECT SUM(CASE WHEN tip_amount > 0 THEN 1 ELSE 0 END) AS tipped, COUNT(*) AS total FROM " + TABLE_NAME
        );
        assertSqlSingleRow(response);
        assertSqlColumnCount(response, 2);
    }

    public void testCoalesceMultipleArgs() throws Exception {
        SqlResponse response = executeSql("SELECT COALESCE(congestion_surcharge, airport_fee, 0) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
    }

    public void testCaseInWhereClause() throws Exception {
        SqlResponse response = executeSql(
            "SELECT * FROM " + TABLE_NAME + " WHERE CASE WHEN vendorid = 1 THEN trip_distance ELSE 0 END > 5 LIMIT 10"
        );
        assertSqlNotEmpty(response);
    }

    public void testNullIfInAggregation() throws Exception {
        SqlResponse response = executeSql("SELECT AVG(NULLIF(tip_amount, 0)) FROM " + TABLE_NAME);
        assertSqlSingleRow(response);
        assertSqlColumnCount(response, 1);
    }

    public void testCaseWithNull() throws Exception {
        SqlResponse response = executeSql(
            "SELECT CASE WHEN congestion_surcharge IS NULL THEN 'missing' ELSE 'present' END FROM " + TABLE_NAME + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
    }
}

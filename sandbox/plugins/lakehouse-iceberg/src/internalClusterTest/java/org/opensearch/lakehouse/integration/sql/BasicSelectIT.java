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
public class BasicSelectIT extends AbstractIcebergQueryIT {

    public void testSelectStar() throws Exception {
        SqlResponse response = executeSql("SELECT * FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 10);
        assertSqlColumnCount(response, 20);
    }

    public void testSelectSpecificColumns() throws Exception {
        SqlResponse response = executeSql("SELECT vendorid, trip_distance, total_amount FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 3);
    }

    public void testSelectWithAlias() throws Exception {
        SqlResponse response = executeSql("SELECT vendorid AS vendor, trip_distance AS dist FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 2);
    }

    public void testSelectDistinct() throws Exception {
        SqlResponse response = executeSql("SELECT DISTINCT vendorid FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
    }

    public void testSelectWithLimit() throws Exception {
        SqlResponse response = executeSql("SELECT * FROM " + TABLE_NAME + " LIMIT 5");
        assertSqlNotEmpty(response);
        assertSqlMaxRows(response, 5);
    }

    public void testSelectCountStar() throws Exception {
        SqlResponse response = executeSql("SELECT COUNT(*) FROM " + TABLE_NAME);
        assertSqlSingleRow(response);
        assertSqlColumnCount(response, 1);
    }

    public void testSelectLiteral() throws Exception {
        SqlResponse response = executeSql("SELECT 1 AS num, 'hello' AS greeting FROM " + TABLE_NAME + " LIMIT 1");
        assertSqlSingleRow(response);
        assertSqlColumnCount(response, 2);
    }

    public void testSelectExpression() throws Exception {
        SqlResponse response = executeSql("SELECT fare_amount + tip_amount AS total_with_tip FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 1);
    }

    public void testSelectAllColumnsExplicit() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, "
                + "ratecodeid, store_and_fwd_flag, pulocationid, dolocationid, payment_type, fare_amount, extra, "
                + "mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, "
                + "airport_fee, cbd_congestion_fee FROM "
                + TABLE_NAME
                + " LIMIT 5"
        );
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 20);
        assertSqlMaxRows(response, 5);
    }

    public void testSelectWithTableAlias() throws Exception {
        SqlResponse response = executeSql("SELECT t.vendorid, t.trip_distance FROM " + TABLE_NAME + " t LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 2);
    }

    public void testSelectDistinctMultipleColumns() throws Exception {
        SqlResponse response = executeSql("SELECT DISTINCT vendorid, payment_type FROM " + TABLE_NAME + " LIMIT 20");
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 2);
    }

    public void testSelectWithNullHandling() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, COALESCE(congestion_surcharge, 0) AS surcharge FROM " + TABLE_NAME + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 2);
    }
}

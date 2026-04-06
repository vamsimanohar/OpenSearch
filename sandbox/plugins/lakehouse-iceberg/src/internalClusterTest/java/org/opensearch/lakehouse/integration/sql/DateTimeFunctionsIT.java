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
public class DateTimeFunctionsIT extends AbstractIcebergQueryIT {

    public void testExtractYear() throws Exception {
        SqlResponse response = executeSql("SELECT EXTRACT(YEAR FROM tpep_pickup_datetime) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 1);
        assertSqlMaxRows(response, 10);
    }

    public void testExtractMonth() throws Exception {
        SqlResponse response = executeSql("SELECT EXTRACT(MONTH FROM tpep_pickup_datetime) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 1);
        assertSqlMaxRows(response, 10);
    }

    public void testExtractDay() throws Exception {
        SqlResponse response = executeSql("SELECT EXTRACT(DAY FROM tpep_pickup_datetime) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 1);
        assertSqlMaxRows(response, 10);
    }

    public void testExtractHour() throws Exception {
        SqlResponse response = executeSql("SELECT EXTRACT(HOUR FROM tpep_pickup_datetime) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 1);
        assertSqlMaxRows(response, 10);
    }

    public void testExtractMinute() throws Exception {
        SqlResponse response = executeSql("SELECT EXTRACT(MINUTE FROM tpep_pickup_datetime) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 1);
        assertSqlMaxRows(response, 10);
    }

    public void testCurrentTimestamp() throws Exception {
        SqlResponse response = executeSql("SELECT CURRENT_TIMESTAMP FROM " + TABLE_NAME + " LIMIT 1");
        assertSqlSingleRow(response);
        assertSqlColumnCount(response, 1);
    }

    public void testCurrentDate() throws Exception {
        SqlResponse response = executeSql("SELECT CURRENT_DATE FROM " + TABLE_NAME + " LIMIT 1");
        assertSqlSingleRow(response);
        assertSqlColumnCount(response, 1);
    }

    public void testDateDiff() throws Exception {
        SqlResponse response = executeSql(
            "SELECT tpep_dropoff_datetime - tpep_pickup_datetime FROM " + TABLE_NAME + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 1);
        assertSqlMaxRows(response, 10);
    }

    public void testGroupByDatePart() throws Exception {
        SqlResponse response = executeSql(
            "SELECT EXTRACT(HOUR FROM tpep_pickup_datetime) AS hr, COUNT(*) FROM "
                + TABLE_NAME
                + " GROUP BY EXTRACT(HOUR FROM tpep_pickup_datetime) ORDER BY hr"
        );
        assertSqlHasRows(response);
        assertSqlColumnCount(response, 2);
    }

    public void testCastToDate() throws Exception {
        SqlResponse response = executeSql("SELECT CAST(tpep_pickup_datetime AS DATE) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 1);
        assertSqlMaxRows(response, 10);
    }
}

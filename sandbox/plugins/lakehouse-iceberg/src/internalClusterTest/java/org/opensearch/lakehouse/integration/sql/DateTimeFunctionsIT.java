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

    // All pickup dates are in 2024
    public void testExtractYear() throws Exception {
        SqlResponse response = executeSql(
            "SELECT DISTINCT EXTRACT(YEAR FROM tpep_pickup_datetime) AS yr FROM " + TABLE_NAME
        );
        assertSqlRowCount(response, 1);
        assertSqlValueEquals("All pickups are in 2024", 2024, response, 0, 0);
    }

    // Pickup months span January through April
    public void testExtractMonth() throws Exception {
        SqlResponse response = executeSql(
            "SELECT DISTINCT EXTRACT(MONTH FROM tpep_pickup_datetime) AS mo FROM " + TABLE_NAME + " ORDER BY mo"
        );
        assertSqlNotEmpty(response);
        assertSqlRowCount(response, 4);
        assertSqlValueEquals("First month is January", 1, response, 0, 0);
        assertSqlValueEquals("Last month is April", 4, response, 3, 0);
    }

    public void testExtractDay() throws Exception {
        SqlResponse response = executeSql("SELECT EXTRACT(DAY FROM tpep_pickup_datetime) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        // Day values should be between 1-31
        assertSqlAllRowsSatisfy(response, 0,
            v -> v instanceof Number && ((Number) v).intValue() >= 1 && ((Number) v).intValue() <= 31,
            "Day should be 1-31");
    }

    public void testExtractHour() throws Exception {
        SqlResponse response = executeSql("SELECT EXTRACT(HOUR FROM tpep_pickup_datetime) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        // Hour values should be between 0-23
        assertSqlAllRowsSatisfy(response, 0,
            v -> v instanceof Number && ((Number) v).intValue() >= 0 && ((Number) v).intValue() <= 23,
            "Hour should be 0-23");
    }

    public void testExtractMinute() throws Exception {
        SqlResponse response = executeSql("SELECT EXTRACT(MINUTE FROM tpep_pickup_datetime) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlAllRowsSatisfy(response, 0,
            v -> v instanceof Number && ((Number) v).intValue() >= 0 && ((Number) v).intValue() <= 59,
            "Minute should be 0-59");
    }

    public void testCurrentTimestamp() throws Exception {
        SqlResponse response = executeSql("SELECT CURRENT_TIMESTAMP FROM " + TABLE_NAME + " LIMIT 1");
        assertSqlSingleRow(response);
        assertSqlColumnCount(response, 1);
        assertNotNull("CURRENT_TIMESTAMP should not be null", getSqlValue(response, 0, 0));
    }

    public void testCurrentDate() throws Exception {
        SqlResponse response = executeSql("SELECT CURRENT_DATE FROM " + TABLE_NAME + " LIMIT 1");
        assertSqlSingleRow(response);
        assertSqlColumnCount(response, 1);
        assertNotNull("CURRENT_DATE should not be null", getSqlValue(response, 0, 0));
    }

    // TIMESTAMPDIFF returns minutes between pickup and dropoff; generated data has 5-44 minute trips
    public void testDateDiff() throws Exception {
        SqlResponse response = executeSql(
            "SELECT TIMESTAMPDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) FROM " + TABLE_NAME + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 1);
        // Trip durations are 5 + random(40) minutes
        assertSqlAllRowsSatisfy(response, 0,
            v -> v instanceof Number && ((Number) v).intValue() >= 5 && ((Number) v).intValue() <= 44,
            "Trip duration should be 5-44 minutes");
    }

    // GROUP BY hour: base pickup is 8:00, trips every ~30 min for 5000 rows
    public void testGroupByDatePart() throws Exception {
        SqlResponse response = executeSql(
            "SELECT EXTRACT(HOUR FROM tpep_pickup_datetime) AS hr, COUNT(*) FROM "
                + TABLE_NAME
                + " GROUP BY EXTRACT(HOUR FROM tpep_pickup_datetime) ORDER BY hr"
        );
        assertSqlHasRows(response);
        assertSqlColumnCount(response, 2);
        // Should have multiple hours represented
        assertTrue("Should have at least 10 distinct hours", response.getTotal() >= 10);
        // Verify ordering
        assertSqlColumnOrdered(response, 0, true);
    }

    public void testCastToDate() throws Exception {
        SqlResponse response = executeSql(
            "SELECT CAST(tpep_pickup_datetime AS DATE) FROM " + TABLE_NAME + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 1);
        // Date may come back as epoch days integer (e.g., 19737 = 2024-01-15)
        // or as a date string. Just verify non-null and in 2024 range.
        // 2024-01-01 = epoch day 19723, 2024-12-31 = 20088
        assertSqlAllRowsSatisfy(response, 0,
            v -> {
                if (v == null) return false;
                if (v instanceof Number) {
                    long days = ((Number) v).longValue();
                    return days >= 19723 && days <= 20088;
                }
                return v.toString().contains("2024");
            },
            "Cast date should be in 2024 range");
    }
}

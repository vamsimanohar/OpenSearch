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
            "SELECT vendorid, CASE WHEN vendorid = 1 THEN 'CMT' WHEN vendorid = 2 THEN 'VTS' ELSE 'Other' END AS label FROM "
                + TABLE_NAME
                + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
        for (int i = 0; i < response.getRows().size(); i++) {
            int vid = (int) getSqlLong(response, i, 0);
            String label = getSqlString(response, i, 1);
            String trimmed = label.trim();
            switch (vid) {
                case 1: assertEquals("Row " + i + ": vendorid=1 -> CMT", "CMT", trimmed); break;
                case 2: assertEquals("Row " + i + ": vendorid=2 -> VTS", "VTS", trimmed); break;
                default: assertEquals("Row " + i + ": vendorid=3 -> Other", "Other", trimmed); break;
            }
        }
    }

    public void testCaseWhenInGroupBy() throws Exception {
        // CASE splits into 2 groups: 'CMT' (vendorid=1, 1667 rows), 'VTS' (vendorid=2+3, 3333 rows)
        SqlResponse response = executeSql(
            "SELECT CASE WHEN vendorid = 1 THEN 'CMT' ELSE 'VTS' END AS vendor_name, COUNT(*) AS cnt FROM "
                + TABLE_NAME
                + " GROUP BY CASE WHEN vendorid = 1 THEN 'CMT' ELSE 'VTS' END ORDER BY vendor_name"
        );
        assertSqlRowCount(response, 2);
        assertEquals("First group should be CMT", "CMT", getSqlString(response, 0, 0).trim());
        assertSqlValueEquals("CMT count", 1667, response, 0, 1);
        assertEquals("Second group should be VTS", "VTS", getSqlString(response, 1, 0).trim());
        assertSqlValueEquals("VTS count", 3333, response, 1, 1);
    }

    public void testCoalesce() throws Exception {
        // congestion_surcharge is NULL for every 10th row (i%10==5)
        SqlResponse response = executeSql(
            "SELECT congestion_surcharge, COALESCE(congestion_surcharge, 0) AS coalesced FROM " + TABLE_NAME + " LIMIT 20"
        );
        assertSqlNotEmpty(response);
        for (int i = 0; i < response.getRows().size(); i++) {
            Object orig = getSqlValue(response, i, 0);
            double coalesced = getSqlDouble(response, i, 1);
            if (orig == null) {
                assertEquals("Row " + i + ": NULL should become 0", 0.0, coalesced, 0.001);
            } else {
                assertEquals("Row " + i + ": non-NULL should stay same",
                    ((Number) orig).doubleValue(), coalesced, 0.001);
            }
        }
    }

    public void testNullIf() throws Exception {
        SqlResponse response = executeSql(
            "SELECT vendorid, NULLIF(vendorid, 1) FROM " + TABLE_NAME + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
        for (int i = 0; i < response.getRows().size(); i++) {
            int vid = (int) getSqlLong(response, i, 0);
            Object nullifResult = getSqlValue(response, i, 1);
            if (vid == 1) {
                assertNull("Row " + i + ": NULLIF(1,1) should be NULL", nullifResult);
            } else {
                assertEquals("Row " + i + ": NULLIF(x,1) should be x for x!=1",
                    vid, ((Number) nullifResult).intValue());
            }
        }
    }

    public void testNestedCase() throws Exception {
        SqlResponse response = executeSql(
            "SELECT trip_distance, CASE WHEN trip_distance < 1 THEN 'short' WHEN trip_distance < 5 THEN 'medium' "
                + "WHEN trip_distance < 10 THEN 'long' ELSE 'very_long' END AS bucket FROM "
                + TABLE_NAME
                + " LIMIT 20"
        );
        assertSqlNotEmpty(response);
        for (int i = 0; i < response.getRows().size(); i++) {
            double dist = getSqlDouble(response, i, 0);
            String bucket = getSqlString(response, i, 1);
            String trimmed = bucket.trim();
            if (dist < 1) assertEquals("Row " + i, "short", trimmed);
            else if (dist < 5) assertEquals("Row " + i, "medium", trimmed);
            else if (dist < 10) assertEquals("Row " + i, "long", trimmed);
            else assertEquals("Row " + i, "very_long", trimmed);
        }
    }

    public void testCaseWithAggregation() throws Exception {
        // tip_amount > 0 only for payment_type=1 (1250 rows), rest have tip=0
        SqlResponse response = executeSql(
            "SELECT SUM(CASE WHEN tip_amount > 0 THEN 1 ELSE 0 END) AS tipped, COUNT(*) AS total FROM " + TABLE_NAME
        );
        assertSqlSingleRow(response);
        assertSqlColumnCount(response, 2);
        assertSqlValueEquals("tipped count should be 1250", 1250, response, 0, 0);
        assertSqlValueEquals("total count should be 5000", 5000, response, 0, 1);
    }

    public void testCoalesceMultipleArgs() throws Exception {
        SqlResponse response = executeSql(
            "SELECT COALESCE(congestion_surcharge, airport_fee, 0) FROM " + TABLE_NAME + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
        // COALESCE should never return null
        assertSqlNoNulls(response, 0);
    }

    public void testCaseInWhereClause() throws Exception {
        // CASE WHEN vendorid=1 THEN trip_distance ELSE 0 END > 5
        // This selects only vendorid=1 rows where trip_distance > 5
        SqlResponse response = executeSql(
            "SELECT vendorid, trip_distance FROM " + TABLE_NAME
                + " WHERE CASE WHEN vendorid = 1 THEN trip_distance ELSE 0 END > 5 LIMIT 10"
        );
        assertSqlNotEmpty(response);
        assertSqlAllRowsEqual(response, 0, 1);
        assertSqlAllRowsSatisfy(response, 1,
            v -> ((Number) v).doubleValue() > 5.0,
            "trip_distance should be > 5");
    }

    public void testNullIfInAggregation() throws Exception {
        // AVG(NULLIF(tip_amount, 0)): excludes 0 tips, averages only the tipped rides
        SqlResponse response = executeSql("SELECT AVG(NULLIF(tip_amount, 0)) FROM " + TABLE_NAME);
        assertSqlSingleRow(response);
        double avg = getSqlDouble(response, 0, 0);
        // Only payment_type=1 has tips (1250 rows). Average tip should be reasonable (> 3, < 30)
        assertTrue("AVG of non-zero tips should be > 3, got " + avg, avg > 3.0);
        assertTrue("AVG of non-zero tips should be < 30, got " + avg, avg < 30.0);
    }

    public void testCaseWithNull() throws Exception {
        SqlResponse response = executeSql(
            "SELECT congestion_surcharge, "
                + "CASE WHEN congestion_surcharge IS NULL THEN 'missing' ELSE 'present' END AS status FROM "
                + TABLE_NAME + " LIMIT 20"
        );
        assertSqlNotEmpty(response);
        for (int i = 0; i < response.getRows().size(); i++) {
            Object cs = getSqlValue(response, i, 0);
            String status = getSqlString(response, i, 1);
            String trimmed = status.trim();
            if (cs == null) {
                assertEquals("Row " + i + ": NULL -> missing", "missing", trimmed);
            } else {
                assertEquals("Row " + i + ": non-NULL -> present", "present", trimmed);
            }
        }
    }
}

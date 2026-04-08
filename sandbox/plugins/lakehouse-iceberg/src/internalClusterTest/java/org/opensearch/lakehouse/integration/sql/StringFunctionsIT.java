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
public class StringFunctionsIT extends AbstractIcebergQueryIT {

    // store_and_fwd_flag is already uppercase 'Y' or 'N'
    public void testUpper() throws Exception {
        SqlResponse response = executeSql(
            "SELECT store_and_fwd_flag, UPPER(store_and_fwd_flag) FROM " + TABLE_NAME + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
        for (int i = 0; i < response.getRows().size(); i++) {
            String orig = getSqlString(response, i, 0);
            String upper = getSqlString(response, i, 1);
            assertEquals("Row " + i + ": UPPER should preserve uppercase", orig.toUpperCase(), upper);
        }
    }

    public void testLower() throws Exception {
        SqlResponse response = executeSql(
            "SELECT store_and_fwd_flag, LOWER(store_and_fwd_flag) FROM " + TABLE_NAME + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
        for (int i = 0; i < response.getRows().size(); i++) {
            String orig = getSqlString(response, i, 0);
            String lower = getSqlString(response, i, 1);
            assertEquals("Row " + i + ": LOWER mismatch", orig.toLowerCase(), lower);
        }
    }

    // store_and_fwd_flag is always 1 character ('Y' or 'N')
    public void testLength() throws Exception {
        SqlResponse response = executeSql(
            "SELECT store_and_fwd_flag, LENGTH(store_and_fwd_flag) FROM " + TABLE_NAME + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
        assertSqlAllRowsSatisfy(response, 1,
            v -> v instanceof Number && ((Number) v).intValue() == 1,
            "LENGTH of store_and_fwd_flag should be 1");
    }

    public void testCharLength() throws Exception {
        SqlResponse response = executeSql(
            "SELECT store_and_fwd_flag, CHAR_LENGTH(store_and_fwd_flag) FROM " + TABLE_NAME + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
        assertSqlAllRowsSatisfy(response, 1,
            v -> v instanceof Number && ((Number) v).intValue() == 1,
            "CHAR_LENGTH of store_and_fwd_flag should be 1");
    }

    public void testTrim() throws Exception {
        SqlResponse response = executeSql(
            "SELECT store_and_fwd_flag, TRIM(store_and_fwd_flag) FROM " + TABLE_NAME + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
        // TRIM of a single char with no whitespace should return the same char
        for (int i = 0; i < response.getRows().size(); i++) {
            assertEquals("Row " + i + ": TRIM should not change value",
                getSqlString(response, i, 0), getSqlString(response, i, 1));
        }
    }

    public void testSubstring() throws Exception {
        SqlResponse response = executeSql(
            "SELECT store_and_fwd_flag, SUBSTRING(store_and_fwd_flag, 1, 1) FROM " + TABLE_NAME + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
        // SUBSTRING of a 1-char string from pos 1 for 1 = the same string
        for (int i = 0; i < response.getRows().size(); i++) {
            assertEquals("Row " + i + ": SUBSTRING(x,1,1) mismatch",
                getSqlString(response, i, 0), getSqlString(response, i, 1));
        }
    }

    public void testConcat() throws Exception {
        SqlResponse response = executeSql(
            "SELECT store_and_fwd_flag, CAST(vendorid AS VARCHAR), "
                + "CONCAT(store_and_fwd_flag, '-', CAST(vendorid AS VARCHAR)) FROM "
                + TABLE_NAME + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
        for (int i = 0; i < response.getRows().size(); i++) {
            String flag = getSqlString(response, i, 0);
            String vid = getSqlString(response, i, 1);
            String concat = getSqlString(response, i, 2);
            assertEquals("Row " + i + ": CONCAT mismatch", flag + "-" + vid, concat);
        }
    }

    public void testReplace() throws Exception {
        SqlResponse response = executeSql(
            "SELECT store_and_fwd_flag, REPLACE(store_and_fwd_flag, 'Y', 'YES') FROM " + TABLE_NAME + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
        for (int i = 0; i < response.getRows().size(); i++) {
            String orig = getSqlString(response, i, 0);
            String replaced = getSqlString(response, i, 1);
            if ("Y".equals(orig)) {
                assertEquals("Row " + i + ": REPLACE Y->YES", "YES", replaced);
            } else {
                assertEquals("Row " + i + ": REPLACE should not change N", "N", replaced);
            }
        }
    }

    public void testPosition() throws Exception {
        SqlResponse response = executeSql(
            "SELECT store_and_fwd_flag, POSITION('Y' IN store_and_fwd_flag) FROM " + TABLE_NAME + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
        for (int i = 0; i < response.getRows().size(); i++) {
            String flag = getSqlString(response, i, 0);
            long pos = getSqlLong(response, i, 1);
            if ("Y".equals(flag)) {
                assertEquals("Row " + i + ": POSITION of 'Y' in 'Y' should be 1", 1, pos);
            } else {
                assertEquals("Row " + i + ": POSITION of 'Y' in 'N' should be 0", 0, pos);
            }
        }
    }

    public void testOverlay() throws Exception {
        SqlResponse response = executeSql(
            "SELECT store_and_fwd_flag, OVERLAY(store_and_fwd_flag PLACING 'X' FROM 1 FOR 1) FROM " + TABLE_NAME + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
        // Replacing first char with 'X' should always produce 'X'
        assertSqlAllRowsSatisfy(response, 1,
            v -> "X".equals(v.toString()),
            "OVERLAY should replace first char with X");
    }

    public void testInitcap() throws Exception {
        SqlResponse response = executeSql("SELECT INITCAP(store_and_fwd_flag) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        // INITCAP of 'Y' = 'Y', INITCAP of 'N' = 'N' (single char, already uppercase first letter)
        assertSqlAllRowsSatisfy(response, 0,
            v -> "Y".equals(v.toString()) || "N".equals(v.toString()),
            "INITCAP of single char should be Y or N");
    }

    public void testConcatWithOperator() throws Exception {
        SqlResponse response = executeSql(
            "SELECT store_and_fwd_flag, CAST(vendorid AS VARCHAR), "
                + "store_and_fwd_flag || '-' || CAST(vendorid AS VARCHAR) FROM "
                + TABLE_NAME + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
        for (int i = 0; i < response.getRows().size(); i++) {
            String flag = getSqlString(response, i, 0);
            String vid = getSqlString(response, i, 1);
            String concat = getSqlString(response, i, 2);
            assertEquals("Row " + i + ": || concat mismatch", flag + "-" + vid, concat);
        }
    }
}

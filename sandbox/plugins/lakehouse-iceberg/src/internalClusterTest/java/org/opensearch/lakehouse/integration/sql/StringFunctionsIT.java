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

    public void testUpper() throws Exception {
        SqlResponse response = executeSql("SELECT UPPER(store_and_fwd_flag) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 1);
        assertSqlMaxRows(response, 10);
    }

    public void testLower() throws Exception {
        SqlResponse response = executeSql("SELECT LOWER(store_and_fwd_flag) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 1);
        assertSqlMaxRows(response, 10);
    }

    public void testLength() throws Exception {
        SqlResponse response = executeSql("SELECT LENGTH(store_and_fwd_flag) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 1);
        assertSqlMaxRows(response, 10);
    }

    public void testCharLength() throws Exception {
        SqlResponse response = executeSql("SELECT CHAR_LENGTH(store_and_fwd_flag) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 1);
        assertSqlMaxRows(response, 10);
    }

    public void testTrim() throws Exception {
        SqlResponse response = executeSql("SELECT TRIM(store_and_fwd_flag) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 1);
        assertSqlMaxRows(response, 10);
    }

    public void testSubstring() throws Exception {
        SqlResponse response = executeSql("SELECT SUBSTRING(store_and_fwd_flag, 1, 1) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 1);
        assertSqlMaxRows(response, 10);
    }

    public void testConcat() throws Exception {
        SqlResponse response = executeSql(
            "SELECT CONCAT(store_and_fwd_flag, '-', CAST(vendorid AS VARCHAR)) FROM " + TABLE_NAME + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 1);
        assertSqlMaxRows(response, 10);
    }

    public void testReplace() throws Exception {
        SqlResponse response = executeSql("SELECT REPLACE(store_and_fwd_flag, 'Y', 'YES') FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 1);
        assertSqlMaxRows(response, 10);
    }

    public void testPosition() throws Exception {
        SqlResponse response = executeSql("SELECT POSITION('Y' IN store_and_fwd_flag) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 1);
        assertSqlMaxRows(response, 10);
    }

    public void testOverlay() throws Exception {
        SqlResponse response = executeSql(
            "SELECT OVERLAY(store_and_fwd_flag PLACING 'X' FROM 1 FOR 1) FROM " + TABLE_NAME + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 1);
        assertSqlMaxRows(response, 10);
    }

    public void testInitcap() throws Exception {
        SqlResponse response = executeSql("SELECT INITCAP(store_and_fwd_flag) FROM " + TABLE_NAME + " LIMIT 10");
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 1);
        assertSqlMaxRows(response, 10);
    }

    public void testConcatWithOperator() throws Exception {
        SqlResponse response = executeSql(
            "SELECT store_and_fwd_flag || '-' || CAST(vendorid AS VARCHAR) FROM " + TABLE_NAME + " LIMIT 10"
        );
        assertSqlNotEmpty(response);
        assertSqlColumnCount(response, 1);
        assertSqlMaxRows(response, 10);
    }
}

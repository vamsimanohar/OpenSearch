/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed.merge;

import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class DistinctExpanderTests extends OpenSearchTestCase {

    // ---- rewriteWorkerSql tests ----

    public void testRewriteGlobalCountDistinct() {
        String sql = "SELECT COUNT(DISTINCT \"userid\") FROM \"hits\"";
        String result = DistinctExpander.rewriteWorkerSql(sql);
        assertEquals("SELECT DISTINCT \"userid\" FROM \"hits\"", result);
    }

    public void testRewriteGroupByCountDistinct() {
        String sql = "SELECT \"regionid\", COUNT(DISTINCT \"userid\") FROM \"hits\" GROUP BY \"regionid\" ORDER BY COUNT(DISTINCT \"userid\") DESC LIMIT 10";
        String result = DistinctExpander.rewriteWorkerSql(sql);
        assertEquals("SELECT DISTINCT \"regionid\", \"userid\" FROM \"hits\"", result);
    }

    public void testRewriteWithWhereClause() {
        String sql = "SELECT \"mobilephonemodel\", COUNT(DISTINCT \"userid\") FROM \"hits\" WHERE \"mobilephonemodel\" <> '' GROUP BY \"mobilephonemodel\" ORDER BY COUNT(DISTINCT \"userid\") DESC LIMIT 10";
        String result = DistinctExpander.rewriteWorkerSql(sql);
        assertEquals("SELECT DISTINCT \"mobilephonemodel\", \"userid\" FROM \"hits\" WHERE \"mobilephonemodel\" <> ''", result);
    }

    public void testRewriteNoCountDistinctPassesThrough() {
        String sql = "SELECT COUNT(*) FROM \"hits\"";
        String result = DistinctExpander.rewriteWorkerSql(sql);
        assertEquals(sql, result);
    }

    public void testRewriteMultipleGroupByColumns() {
        String sql = "SELECT \"mobilephone\", \"mobilephonemodel\", COUNT(DISTINCT \"userid\") FROM \"hits\" WHERE \"mobilephonemodel\" <> '' GROUP BY \"mobilephone\", \"mobilephonemodel\" ORDER BY COUNT(DISTINCT \"userid\") DESC LIMIT 10";
        String result = DistinctExpander.rewriteWorkerSql(sql);
        assertEquals("SELECT DISTINCT \"mobilephone\", \"mobilephonemodel\", \"userid\" FROM \"hits\" WHERE \"mobilephonemodel\" <> ''", result);
    }

    // ---- generateMergeSql tests ----

    public void testGenerateMergeSqlGlobalCountDistinct() {
        List<String> workerColumns = List.of("userid");
        String originalSql = "SELECT COUNT(DISTINCT \"userid\") FROM \"hits\"";
        String result = DistinctExpander.generateMergeSql(workerColumns, originalSql);
        assertEquals("SELECT COUNT(DISTINCT \"userid\") FROM input", result);
    }

    public void testGenerateMergeSqlGroupByCountDistinct() {
        List<String> workerColumns = List.of("regionid", "userid");
        String originalSql = "SELECT \"regionid\", COUNT(DISTINCT \"userid\") FROM \"hits\" GROUP BY \"regionid\" ORDER BY COUNT(DISTINCT \"userid\") DESC LIMIT 10";
        String result = DistinctExpander.generateMergeSql(workerColumns, originalSql);
        assertTrue(result.contains("\"regionid\""));
        assertTrue(result.contains("COUNT(DISTINCT \"userid\")"));
        assertTrue(result.contains("GROUP BY \"regionid\""));
        assertTrue(result.contains("ORDER BY"));
        assertTrue(result.contains("LIMIT 10"));
    }

    public void testGenerateMergeSqlMultipleGroupByKeys() {
        List<String> workerColumns = List.of("mobilephone", "mobilephonemodel", "userid");
        String originalSql = "SELECT \"mobilephone\", \"mobilephonemodel\", COUNT(DISTINCT \"userid\") FROM \"hits\" GROUP BY \"mobilephone\", \"mobilephonemodel\" ORDER BY COUNT(DISTINCT \"userid\") DESC LIMIT 10";
        String result = DistinctExpander.generateMergeSql(workerColumns, originalSql);
        assertTrue(result.contains("\"mobilephone\""));
        assertTrue(result.contains("\"mobilephonemodel\""));
        assertTrue(result.contains("COUNT(DISTINCT \"userid\")"));
        assertTrue(result.contains("GROUP BY \"mobilephone\", \"mobilephonemodel\""));
    }
}

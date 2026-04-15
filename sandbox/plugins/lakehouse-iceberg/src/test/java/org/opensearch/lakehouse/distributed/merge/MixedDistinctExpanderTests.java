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

public class MixedDistinctExpanderTests extends OpenSearchTestCase {

    // ---- rewriteWorkerSql ----

    public void testRewriteWorkerSql_removesCountDistinctAddsToGroupBy() {
        // q23-like: COUNT(*) + COUNT(DISTINCT)
        String sql = "SELECT \"searchphrase\", COUNT(*) AS c, COUNT(DISTINCT \"userid\") FROM \"hits\" "
            + "WHERE \"searchphrase\" <> '' GROUP BY \"searchphrase\" ORDER BY c DESC LIMIT 10";

        String result = MixedDistinctExpander.rewriteWorkerSql(sql);

        assertTrue("Should contain searchphrase in SELECT", result.contains("\"searchphrase\""));
        assertTrue("Should contain COUNT(*) AS c", result.contains("COUNT(*) AS c"));
        assertTrue("Should contain userid in SELECT", result.contains("\"userid\""));
        assertFalse("Should NOT contain COUNT(DISTINCT", result.toUpperCase().contains("COUNT(DISTINCT"));
        assertTrue("Should GROUP BY searchphrase and userid",
            result.contains("GROUP BY \"searchphrase\", \"userid\""));
        assertFalse("Should NOT contain ORDER BY", result.toUpperCase().contains("ORDER BY"));
        assertFalse("Should NOT contain LIMIT", result.toUpperCase().contains("LIMIT"));
    }

    public void testRewriteWorkerSql_withSumCountAvgAndCountDistinct() {
        // q10-like: SUM + COUNT + AVG + COUNT(DISTINCT)
        String sql = "SELECT \"regionid\", SUM(\"advengineid\"), COUNT(*) AS c, AVG(\"resolutionwidth\"), "
            + "COUNT(DISTINCT \"userid\") FROM \"hits\" GROUP BY \"regionid\" ORDER BY c DESC LIMIT 10";

        String result = MixedDistinctExpander.rewriteWorkerSql(sql);

        assertTrue("Should contain regionid", result.contains("\"regionid\""));
        assertTrue("Should contain SUM", result.contains("SUM(\"advengineid\")"));
        assertTrue("Should contain COUNT(*)", result.contains("COUNT(*)"));
        assertFalse("Should NOT contain COUNT(DISTINCT", result.toUpperCase().contains("COUNT(DISTINCT"));
        assertFalse("Should NOT contain AVG(", result.toUpperCase().contains("AVG(\"RESOLUTIONWIDTH\")"));
        assertTrue("Should contain __avg_sum_", result.contains("__avg_sum_"));
        assertTrue("Should contain __avg_count_", result.contains("__avg_count_"));
        assertTrue("Should contain userid in SELECT", result.contains("\"userid\""));
        assertTrue("Should GROUP BY regionid and userid",
            result.contains("GROUP BY \"regionid\", \"userid\""));
        assertFalse("Should NOT contain ORDER BY", result.toUpperCase().contains("ORDER BY"));
        assertFalse("Should NOT contain LIMIT", result.toUpperCase().contains("LIMIT"));
    }

    public void testRewriteWorkerSql_noGroupByAddsGroupByForDistinctCols() {
        // Global: SUM + COUNT(DISTINCT)
        String sql = "SELECT SUM(\"x\"), COUNT(DISTINCT \"y\") FROM \"table\"";

        String result = MixedDistinctExpander.rewriteWorkerSql(sql);

        assertTrue("Should contain SUM(\"x\")", result.contains("SUM(\"x\")"));
        assertTrue("Should contain \"y\"", result.contains("\"y\""));
        assertFalse("Should NOT contain COUNT(DISTINCT", result.toUpperCase().contains("COUNT(DISTINCT"));
        assertTrue("Should have GROUP BY \"y\"", result.contains("GROUP BY \"y\""));
    }

    public void testRewriteWorkerSql_countDistinctAtMiddleOfSelect() {
        String sql = "SELECT \"key\", COUNT(DISTINCT \"uid\"), COUNT(*) FROM \"t\" GROUP BY \"key\"";

        String result = MixedDistinctExpander.rewriteWorkerSql(sql);

        assertTrue("Should contain key", result.contains("\"key\""));
        assertTrue("Should contain COUNT(*)", result.contains("COUNT(*)"));
        assertTrue("Should contain uid", result.contains("\"uid\""));
        assertFalse("Should NOT contain COUNT(DISTINCT", result.toUpperCase().contains("COUNT(DISTINCT"));
        assertTrue("Should GROUP BY key, uid", result.contains("GROUP BY \"key\", \"uid\""));
    }

    public void testRewriteWorkerSql_stripsHaving() {
        String sql = "SELECT \"key\", COUNT(*) AS c, COUNT(DISTINCT \"uid\") FROM \"t\" "
            + "GROUP BY \"key\" HAVING COUNT(*) > 100 ORDER BY c DESC LIMIT 10";

        String result = MixedDistinctExpander.rewriteWorkerSql(sql);

        assertFalse("Should NOT contain HAVING", result.toUpperCase().contains("HAVING"));
        assertFalse("Should NOT contain ORDER BY", result.toUpperCase().contains("ORDER BY"));
        assertFalse("Should NOT contain LIMIT", result.toUpperCase().contains("LIMIT"));
    }

    public void testRewriteWorkerSql_noCountDistinctPassesThrough() {
        String sql = "SELECT COUNT(*) FROM \"t\"";
        assertEquals(sql, MixedDistinctExpander.rewriteWorkerSql(sql));
    }

    public void testRewriteWorkerSql_preservesWhereClause() {
        String sql = "SELECT \"key\", COUNT(*) AS c, COUNT(DISTINCT \"uid\") FROM \"t\" "
            + "WHERE \"active\" = 1 GROUP BY \"key\"";

        String result = MixedDistinctExpander.rewriteWorkerSql(sql);

        assertTrue("Should preserve WHERE clause", result.toUpperCase().contains("WHERE \"ACTIVE\" = 1"));
    }

    // ---- generateMergeSql ----

    public void testGenerateMergeSql_simpleCountAndCountDistinct() {
        // q23: workers return [searchphrase, c, userid]
        List<String> workerCols = List.of("searchphrase", "c", "userid");
        String originalSql = "SELECT \"searchphrase\", COUNT(*) AS c, COUNT(DISTINCT \"userid\") "
            + "FROM \"hits\" WHERE \"searchphrase\" <> '' GROUP BY \"searchphrase\" ORDER BY c DESC LIMIT 10";

        String result = MixedDistinctExpander.generateMergeSql(workerCols, originalSql);

        assertTrue("Should have searchphrase as group key", result.contains("\"searchphrase\""));
        assertTrue("Should re-aggregate c with SUM", result.contains("SUM(\"c\") AS \"c\""));
        assertTrue("Should have COUNT(DISTINCT userid)", result.contains("COUNT(DISTINCT \"userid\")"));
        assertTrue("Should have GROUP BY", result.contains("GROUP BY \"searchphrase\""));
        assertTrue("Should have ORDER BY", result.contains("ORDER BY c DESC"));
        assertTrue("Should have LIMIT", result.contains("LIMIT 10"));
    }

    public void testGenerateMergeSql_withAvgDecomposition() {
        // q10: workers return [regionid, SUM(hits.advengineid), c, __avg_sum_0, __avg_count_0, userid]
        List<String> workerCols = List.of(
            "regionid", "SUM(hits.advengineid)", "c", "__avg_sum_0", "__avg_count_0", "userid"
        );
        String originalSql = "SELECT \"regionid\", SUM(\"advengineid\"), COUNT(*) AS c, AVG(\"resolutionwidth\"), "
            + "COUNT(DISTINCT \"userid\") FROM \"hits\" GROUP BY \"regionid\" ORDER BY c DESC LIMIT 10";

        String result = MixedDistinctExpander.generateMergeSql(workerCols, originalSql);

        assertTrue("Should have regionid as group key", result.contains("\"regionid\""));
        assertTrue("Should re-aggregate SUM column", result.contains("SUM(\"SUM(hits.advengineid)\")"));
        assertTrue("Should re-aggregate c with SUM", result.contains("SUM(\"c\") AS \"c\""));
        assertTrue("Should have AVG merge", result.contains("CAST(SUM(\"__avg_sum_0\") AS DOUBLE) / SUM(\"__avg_count_0\")"));
        assertTrue("Should have COUNT(DISTINCT userid)", result.contains("COUNT(DISTINCT \"userid\")"));
        assertTrue("Should have GROUP BY regionid", result.contains("GROUP BY \"regionid\""));
        assertTrue("Should have ORDER BY", result.contains("ORDER BY c DESC"));
        assertTrue("Should have LIMIT 10", result.contains("LIMIT 10"));
    }

    public void testGenerateMergeSql_noGroupBy() {
        // Global: SUM(x) + COUNT(DISTINCT y) → workers return [SUM(table.x), y]
        List<String> workerCols = List.of("SUM(table.x)", "y");
        String originalSql = "SELECT SUM(\"x\"), COUNT(DISTINCT \"y\") FROM \"table\"";

        String result = MixedDistinctExpander.generateMergeSql(workerCols, originalSql);

        assertTrue("Should re-aggregate SUM column", result.contains("SUM(\"SUM(table.x)\")"));
        assertTrue("Should have COUNT(DISTINCT y)", result.contains("COUNT(DISTINCT \"y\")"));
        assertFalse("Should NOT have GROUP BY", result.toUpperCase().contains("GROUP BY"));
    }

    public void testGenerateMergeSql_withMinColumn() {
        // Workers return a MIN aggregate
        List<String> workerCols = List.of("key", "MIN(hits.url)", "c", "userid");
        String originalSql = "SELECT \"key\", MIN(\"url\"), COUNT(*) AS c, COUNT(DISTINCT \"userid\") "
            + "FROM \"hits\" GROUP BY \"key\" ORDER BY c DESC LIMIT 5";

        String result = MixedDistinctExpander.generateMergeSql(workerCols, originalSql);

        assertTrue("Should re-aggregate MIN with MIN", result.contains("MIN(\"MIN(hits.url)\")"));
        assertTrue("Should re-aggregate c with SUM", result.contains("SUM(\"c\") AS \"c\""));
        assertTrue("Should have COUNT(DISTINCT userid)", result.contains("COUNT(DISTINCT \"userid\")"));
    }

    public void testGenerateMergeSql_withMaxColumn() {
        List<String> workerCols = List.of("key", "MAX(hits.score)", "userid");
        String originalSql = "SELECT \"key\", MAX(\"score\"), COUNT(DISTINCT \"userid\") "
            + "FROM \"hits\" GROUP BY \"key\"";

        String result = MixedDistinctExpander.generateMergeSql(workerCols, originalSql);

        assertTrue("Should re-aggregate MAX with MAX", result.contains("MAX(\"MAX(hits.score)\")"));
        assertTrue("Should have COUNT(DISTINCT userid)", result.contains("COUNT(DISTINCT \"userid\")"));
    }

    // ---- inferReAggFunction ----

    public void testInferReAggFunction() {
        assertEquals("MIN", MixedDistinctExpander.inferReAggFunction("min(hits.url)"));
        assertEquals("MAX", MixedDistinctExpander.inferReAggFunction("max(hits.score)"));
        assertEquals("SUM", MixedDistinctExpander.inferReAggFunction("sum(hits.x)"));
        assertEquals("SUM", MixedDistinctExpander.inferReAggFunction("c"));
        assertEquals("SUM", MixedDistinctExpander.inferReAggFunction("count(*)"));
    }
}

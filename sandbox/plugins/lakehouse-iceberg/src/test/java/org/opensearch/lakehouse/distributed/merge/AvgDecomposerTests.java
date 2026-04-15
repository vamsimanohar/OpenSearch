/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed.merge;

import org.apache.calcite.sql.SqlKind;
import org.opensearch.lakehouse.distributed.AnalysisResultFactory;
import org.opensearch.lakehouse.distributed.QueryAnalyzer;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class AvgDecomposerTests extends OpenSearchTestCase {

    // ---- hasAvg tests ----

    public void testHasAvgReturnsTrueWhenAvgPresent() {
        SqlKind[] aggKinds = new SqlKind[] { SqlKind.SUM, SqlKind.AVG };
        QueryAnalyzer.AnalysisResult analysis = AnalysisResultFactory.create(
            MergeStrategy.GLOBAL_MERGE, aggKinds, null, null, 0
        );
        assertTrue(AvgDecomposer.hasAvg(analysis));
    }

    public void testHasAvgReturnsFalseWhenNoAvg() {
        SqlKind[] aggKinds = new SqlKind[] { SqlKind.SUM, SqlKind.COUNT };
        QueryAnalyzer.AnalysisResult analysis = AnalysisResultFactory.create(
            MergeStrategy.GLOBAL_MERGE, aggKinds, null, null, 0
        );
        assertFalse(AvgDecomposer.hasAvg(analysis));
    }

    public void testHasAvgReturnsFalseWhenNullAggKinds() {
        QueryAnalyzer.AnalysisResult analysis = AnalysisResultFactory.create(MergeStrategy.CONCAT);
        assertFalse(AvgDecomposer.hasAvg(analysis));
    }

    // ---- decomposeWorkerSql tests ----

    public void testDecomposeSingleAvg() {
        String sql = "SELECT AVG(\"resolutionwidth\") FROM \"hits\"";
        String result = AvgDecomposer.decomposeWorkerSql(sql);
        assertEquals(
            "SELECT SUM(\"resolutionwidth\") AS \"__avg_sum_0\", COUNT(\"resolutionwidth\") AS \"__avg_count_0\" FROM \"hits\"",
            result
        );
    }

    public void testDecomposeAvgWithNestedFunction() {
        String sql = "SELECT AVG(length(\"url\")) AS \"l\", COUNT(*) AS \"c\" FROM \"hits\"";
        String result = AvgDecomposer.decomposeWorkerSql(sql);
        assertEquals(
            "SELECT SUM(length(\"url\")) AS \"__avg_sum_0\", COUNT(length(\"url\")) AS \"__avg_count_0\", COUNT(*) AS \"c\" FROM \"hits\"",
            result
        );
    }

    public void testDecomposeMultipleAvgs() {
        String sql = "SELECT AVG(\"col1\"), AVG(\"col2\") FROM \"hits\"";
        String result = AvgDecomposer.decomposeWorkerSql(sql);
        assertEquals(
            "SELECT SUM(\"col1\") AS \"__avg_sum_0\", COUNT(\"col1\") AS \"__avg_count_0\", "
                + "SUM(\"col2\") AS \"__avg_sum_1\", COUNT(\"col2\") AS \"__avg_count_1\" FROM \"hits\"",
            result
        );
    }

    public void testDecomposeNoAvgPassesThrough() {
        String sql = "SELECT SUM(\"col1\"), COUNT(*) FROM \"hits\"";
        String result = AvgDecomposer.decomposeWorkerSql(sql);
        assertEquals(sql, result);
    }

    public void testDecomposeAvgMixedWithOtherAggs() {
        String sql = "SELECT SUM(\"advengineid\"), COUNT(*), AVG(\"resolutionwidth\") FROM \"hits\"";
        String result = AvgDecomposer.decomposeWorkerSql(sql);
        assertEquals(
            "SELECT SUM(\"advengineid\"), COUNT(*), SUM(\"resolutionwidth\") AS \"__avg_sum_0\", COUNT(\"resolutionwidth\") AS \"__avg_count_0\" FROM \"hits\"",
            result
        );
    }

    public void testDecomposeAvgInGroupByQuery() {
        String sql = "SELECT \"searchengineid\", \"clientip\", COUNT(*) AS \"c\", SUM(\"isrefresh\"), AVG(\"resolutionwidth\") FROM \"hits\" WHERE \"searchphrase\" <> '' GROUP BY \"searchengineid\", \"clientip\" ORDER BY \"c\" DESC LIMIT 10";
        String result = AvgDecomposer.decomposeWorkerSql(sql);
        assertTrue(result.contains("SUM(\"resolutionwidth\") AS \"__avg_sum_0\""));
        assertTrue(result.contains("COUNT(\"resolutionwidth\") AS \"__avg_count_0\""));
        assertFalse(result.contains("AVG"));
    }

    // ---- buildMergeColumns tests ----

    public void testBuildMergeColumnsForGlobalAvg() {
        // Global: AVG(UserID) — workers return __avg_sum_0, __avg_count_0
        SqlKind[] aggKinds = new SqlKind[] { SqlKind.AVG };
        QueryAnalyzer.AnalysisResult analysis = AnalysisResultFactory.create(
            MergeStrategy.GLOBAL_MERGE, aggKinds, null, null, 0
        );
        List<String> workerColumns = List.of("__avg_sum_0", "__avg_count_0");

        AvgDecomposer.MergeColumnInfo info = AvgDecomposer.buildMergeColumns(analysis, workerColumns);

        assertEquals(1, info.selectExprs.size());
        assertTrue(info.selectExprs.get(0).contains("CAST(SUM(\"__avg_sum_0\") AS DOUBLE) / SUM(\"__avg_count_0\")"));
        assertTrue(info.groupByExprs.isEmpty());
    }

    public void testBuildMergeColumnsForMixedGlobalAggs() {
        // q3: SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth)
        // Workers return: advengineid, count_star, __avg_sum_0, __avg_count_0
        SqlKind[] aggKinds = new SqlKind[] { SqlKind.SUM, SqlKind.COUNT, SqlKind.AVG };
        QueryAnalyzer.AnalysisResult analysis = AnalysisResultFactory.create(
            MergeStrategy.GLOBAL_MERGE, aggKinds, null, null, 0
        );
        List<String> workerColumns = List.of("advengineid", "count_star", "__avg_sum_0", "__avg_count_0");

        AvgDecomposer.MergeColumnInfo info = AvgDecomposer.buildMergeColumns(analysis, workerColumns);

        assertEquals(3, info.selectExprs.size());
        assertEquals("SUM(\"advengineid\") AS \"advengineid\"", info.selectExprs.get(0));
        assertEquals("SUM(\"count_star\") AS \"count_star\"", info.selectExprs.get(1));
        assertTrue(info.selectExprs.get(2).contains("CAST(SUM(\"__avg_sum_0\") AS DOUBLE) / SUM(\"__avg_count_0\")"));
    }

    public void testBuildMergeColumnsForGroupByWithAvg() {
        // q31: GROUP BY SearchEngineID, ClientIP — COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth)
        // Workers return: searchengineid, clientip, c, isrefresh, __avg_sum_0, __avg_count_0
        boolean[] isGroupKey = new boolean[] { true, true, false, false, false };
        SqlKind[] aggKinds = new SqlKind[] { null, null, SqlKind.COUNT, SqlKind.SUM, SqlKind.AVG };
        QueryAnalyzer.AnalysisResult analysis = AnalysisResultFactory.create(
            MergeStrategy.TWO_PHASE_GROUP_BY, aggKinds, null, null, 0, isGroupKey
        );
        List<String> workerColumns = List.of("searchengineid", "clientip", "c", "isrefresh", "__avg_sum_0", "__avg_count_0");

        AvgDecomposer.MergeColumnInfo info = AvgDecomposer.buildMergeColumns(analysis, workerColumns);

        assertEquals(5, info.selectExprs.size());
        assertEquals("\"searchengineid\"", info.selectExprs.get(0));
        assertEquals("\"clientip\"", info.selectExprs.get(1));
        assertEquals("SUM(\"c\") AS \"c\"", info.selectExprs.get(2));
        assertEquals("SUM(\"isrefresh\") AS \"isrefresh\"", info.selectExprs.get(3));
        assertTrue(info.selectExprs.get(4).contains("SUM(\"__avg_sum_0\")"));
        assertEquals(2, info.groupByExprs.size());
    }
}

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

public class MergeSqlGeneratorTests extends OpenSearchTestCase {

    // ---- CONCAT ----

    public void testConcatGeneratesSelectStarFromInput() {
        QueryAnalyzer.AnalysisResult analysis = AnalysisResultFactory.create(MergeStrategy.CONCAT);
        List<String> columns = List.of("col1", "col2");

        String sql = MergeSqlGenerator.generate(analysis, columns);

        assertEquals("SELECT * FROM input", sql);
    }

    // ---- GLOBAL_MERGE ----

    public void testGlobalMergeWithSumCountMinMax() {
        SqlKind[] aggKinds = new SqlKind[] { SqlKind.SUM, SqlKind.COUNT, SqlKind.MIN, SqlKind.MAX };
        QueryAnalyzer.AnalysisResult analysis = AnalysisResultFactory.create(MergeStrategy.GLOBAL_MERGE, aggKinds, null, null, 0);
        List<String> columns = List.of("total_sales", "row_count", "min_price", "max_price");

        String sql = MergeSqlGenerator.generate(analysis, columns);

        assertEquals(
            "SELECT SUM(\"total_sales\") AS \"total_sales\", "
                + "SUM(\"row_count\") AS \"row_count\", "
                + "MIN(\"min_price\") AS \"min_price\", "
                + "MAX(\"max_price\") AS \"max_price\" FROM input",
            sql
        );
    }

    public void testGlobalMergeWithNullAggKindsDefaultsToSum() {
        QueryAnalyzer.AnalysisResult analysis = AnalysisResultFactory.create(MergeStrategy.GLOBAL_MERGE, null, null, null, 0);
        List<String> columns = List.of("a", "b");

        String sql = MergeSqlGenerator.generate(analysis, columns);

        assertEquals("SELECT SUM(\"a\") AS \"a\", SUM(\"b\") AS \"b\" FROM input", sql);
    }

    public void testGlobalMergeWithFewerAggKindsThanColumnsDefaultsToSum() {
        SqlKind[] aggKinds = new SqlKind[] { SqlKind.MIN };
        QueryAnalyzer.AnalysisResult analysis = AnalysisResultFactory.create(MergeStrategy.GLOBAL_MERGE, aggKinds, null, null, 0);
        List<String> columns = List.of("col_min", "col_extra");

        String sql = MergeSqlGenerator.generate(analysis, columns);

        assertEquals("SELECT MIN(\"col_min\") AS \"col_min\", SUM(\"col_extra\") AS \"col_extra\" FROM input", sql);
    }

    public void testGlobalMergeCountUsesSumForMerge() {
        SqlKind[] aggKinds = new SqlKind[] { SqlKind.COUNT };
        QueryAnalyzer.AnalysisResult analysis = AnalysisResultFactory.create(MergeStrategy.GLOBAL_MERGE, aggKinds, null, null, 0);
        List<String> columns = List.of("cnt");

        String sql = MergeSqlGenerator.generate(analysis, columns);

        assertEquals("SELECT SUM(\"cnt\") AS \"cnt\" FROM input", sql);
    }

    // ---- TOPK_MERGE ----

    public void testTopKMergeGeneratesOrderByDescWithLimit() {
        int[] sortColumns = new int[] { 1 };
        boolean[] sortAsc = new boolean[] { false };
        QueryAnalyzer.AnalysisResult analysis = AnalysisResultFactory.create(MergeStrategy.TOPK_MERGE, null, sortColumns, sortAsc, 10);
        List<String> columns = List.of("name", "score");

        String sql = MergeSqlGenerator.generate(analysis, columns);

        assertEquals("SELECT * FROM input ORDER BY \"score\" DESC LIMIT 10", sql);
    }

    public void testTopKMergeAscending() {
        int[] sortColumns = new int[] { 0 };
        boolean[] sortAsc = new boolean[] { true };
        QueryAnalyzer.AnalysisResult analysis = AnalysisResultFactory.create(MergeStrategy.TOPK_MERGE, null, sortColumns, sortAsc, 5);
        List<String> columns = List.of("id");

        String sql = MergeSqlGenerator.generate(analysis, columns);

        assertEquals("SELECT * FROM input ORDER BY \"id\" ASC LIMIT 5", sql);
    }

    public void testTopKMergeWithMultipleSortColumns() {
        int[] sortColumns = new int[] { 2, 0 };
        boolean[] sortAsc = new boolean[] { true, false };
        QueryAnalyzer.AnalysisResult analysis = AnalysisResultFactory.create(MergeStrategy.TOPK_MERGE, null, sortColumns, sortAsc, 20);
        List<String> columns = List.of("a", "b", "c");

        String sql = MergeSqlGenerator.generate(analysis, columns);

        assertEquals("SELECT * FROM input ORDER BY \"c\" ASC, \"a\" DESC LIMIT 20", sql);
    }

    public void testTopKMergeWithZeroLimitOmitsLimitClause() {
        int[] sortColumns = new int[] { 0 };
        boolean[] sortAsc = new boolean[] { true };
        QueryAnalyzer.AnalysisResult analysis = AnalysisResultFactory.create(MergeStrategy.TOPK_MERGE, null, sortColumns, sortAsc, 0);
        List<String> columns = List.of("x");

        String sql = MergeSqlGenerator.generate(analysis, columns);

        assertEquals("SELECT * FROM input ORDER BY \"x\" ASC", sql);
    }

    // ---- TWO_PHASE_GROUP_BY ----

    public void testTwoPhaseGroupByWithCountOrderByLimit() {
        boolean[] isGroupKey = new boolean[] { true, false };
        SqlKind[] aggKinds = new SqlKind[] { null, SqlKind.COUNT };
        int[] sortColumns = new int[] { 1 };
        boolean[] sortAsc = new boolean[] { false };
        QueryAnalyzer.AnalysisResult analysis = AnalysisResultFactory.create(
            MergeStrategy.TWO_PHASE_GROUP_BY, aggKinds, sortColumns, sortAsc, 10, isGroupKey
        );
        List<String> columns = List.of("searchphrase", "c");

        String sql = MergeSqlGenerator.generate(analysis, columns);

        assertEquals(
            "SELECT \"searchphrase\", SUM(\"c\") AS \"c\" FROM input GROUP BY \"searchphrase\" ORDER BY \"c\" DESC LIMIT 10",
            sql
        );
    }

    public void testTwoPhaseGroupByWithMultipleKeysAndAggs() {
        boolean[] isGroupKey = new boolean[] { true, true, false, false };
        SqlKind[] aggKinds = new SqlKind[] { null, null, SqlKind.COUNT, SqlKind.MIN };
        int[] sortColumns = new int[] { 2 };
        boolean[] sortAsc = new boolean[] { false };
        QueryAnalyzer.AnalysisResult analysis = AnalysisResultFactory.create(
            MergeStrategy.TWO_PHASE_GROUP_BY, aggKinds, sortColumns, sortAsc, 10, isGroupKey
        );
        List<String> columns = List.of("searchengineid", "searchphrase", "c", "min_url");

        String sql = MergeSqlGenerator.generate(analysis, columns);

        assertEquals(
            "SELECT \"searchengineid\", \"searchphrase\", SUM(\"c\") AS \"c\", MIN(\"min_url\") AS \"min_url\" "
                + "FROM input GROUP BY \"searchengineid\", \"searchphrase\" ORDER BY \"c\" DESC LIMIT 10",
            sql
        );
    }

    public void testTwoPhaseGroupByWithoutOrderByOrLimit() {
        boolean[] isGroupKey = new boolean[] { true, false };
        SqlKind[] aggKinds = new SqlKind[] { null, SqlKind.COUNT };
        QueryAnalyzer.AnalysisResult analysis = AnalysisResultFactory.create(
            MergeStrategy.TWO_PHASE_GROUP_BY, aggKinds, null, null, 0, isGroupKey
        );
        List<String> columns = List.of("userid", "c");

        String sql = MergeSqlGenerator.generate(analysis, columns);

        assertEquals("SELECT \"userid\", SUM(\"c\") AS \"c\" FROM input GROUP BY \"userid\"", sql);
    }

    // ---- SINGLE_NODE ----

    public void testSingleNodeThrowsIllegalArgumentException() {
        QueryAnalyzer.AnalysisResult analysis = AnalysisResultFactory.create(MergeStrategy.SINGLE_NODE);
        List<String> columns = List.of("col");

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> MergeSqlGenerator.generate(analysis, columns));

        assertEquals("SINGLE_NODE should not reach merge SQL generation", ex.getMessage());
    }

    // ---- Special characters in column names ----

    public void testColumnNamesWithSpacesAreProperlyQuoted() {
        SqlKind[] aggKinds = new SqlKind[] { SqlKind.SUM };
        QueryAnalyzer.AnalysisResult analysis = AnalysisResultFactory.create(MergeStrategy.GLOBAL_MERGE, aggKinds, null, null, 0);
        List<String> columns = List.of("my column");

        String sql = MergeSqlGenerator.generate(analysis, columns);

        assertEquals("SELECT SUM(\"my column\") AS \"my column\" FROM input", sql);
    }

    public void testColumnNameWithDoubleQuotesIsEscaped() {
        SqlKind[] aggKinds = new SqlKind[] { SqlKind.MAX };
        QueryAnalyzer.AnalysisResult analysis = AnalysisResultFactory.create(MergeStrategy.GLOBAL_MERGE, aggKinds, null, null, 0);
        List<String> columns = List.of("col\"name");

        String sql = MergeSqlGenerator.generate(analysis, columns);

        // Double quotes inside identifier are escaped by doubling: col"name -> "col""name"
        assertEquals("SELECT MAX(\"col\"\"name\") AS \"col\"\"name\" FROM input", sql);
    }

    public void testTopKMergeWithSpecialCharacterColumnName() {
        int[] sortColumns = new int[] { 0 };
        boolean[] sortAsc = new boolean[] { true };
        QueryAnalyzer.AnalysisResult analysis = AnalysisResultFactory.create(MergeStrategy.TOPK_MERGE, null, sortColumns, sortAsc, 3);
        List<String> columns = List.of("order-total");

        String sql = MergeSqlGenerator.generate(analysis, columns);

        assertEquals("SELECT * FROM input ORDER BY \"order-total\" ASC LIMIT 3", sql);
    }
}

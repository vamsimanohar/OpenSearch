/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class ResultMergerTests extends OpenSearchTestCase {

    // --- CONCAT tests ---

    public void testConcatMultipleResponses() {
        WorkerQueryResponse r1 = makeResponse(List.of("col"), List.of("Integer"), new Object[][]{{1, 2}}, 2);
        WorkerQueryResponse r2 = makeResponse(List.of("col"), List.of("Integer"), new Object[][]{{3, 4}}, 2);

        WorkerQueryResponse merged = ResultMerger.merge(List.of(r1, r2), MergeStrategy.CONCAT, null, null, 0);

        assertEquals(4, merged.getRowCount());
        assertEquals(1, merged.getColumnData()[0][0]);
        assertEquals(2, merged.getColumnData()[0][1]);
        assertEquals(3, merged.getColumnData()[0][2]);
        assertEquals(4, merged.getColumnData()[0][3]);
    }

    public void testConcatSingleResponse() {
        WorkerQueryResponse r1 = makeResponse(List.of("a"), List.of("String"), new Object[][]{{"x", "y"}}, 2);

        WorkerQueryResponse merged = ResultMerger.merge(List.of(r1), MergeStrategy.CONCAT, null, null, 0);

        assertEquals(2, merged.getRowCount());
        assertEquals("x", merged.getColumnData()[0][0]);
        assertEquals("y", merged.getColumnData()[0][1]);
    }

    public void testConcatMultipleColumns() {
        WorkerQueryResponse r1 = makeResponse(List.of("a", "b"), List.of("Integer", "String"), new Object[][]{{1}, {"x"}}, 1);
        WorkerQueryResponse r2 = makeResponse(List.of("a", "b"), List.of("Integer", "String"), new Object[][]{{2}, {"y"}}, 1);

        WorkerQueryResponse merged = ResultMerger.merge(List.of(r1, r2), MergeStrategy.CONCAT, null, null, 0);

        assertEquals(2, merged.getRowCount());
        assertEquals(1, merged.getColumnData()[0][0]);
        assertEquals(2, merged.getColumnData()[0][1]);
        assertEquals("x", merged.getColumnData()[1][0]);
        assertEquals("y", merged.getColumnData()[1][1]);
    }

    // --- GLOBAL_MERGE tests ---

    public void testGlobalMergeSumCounts() {
        // Two workers each return COUNT(*) = 50
        WorkerQueryResponse r1 = makeResponse(List.of("cnt"), List.of("Long"), new Object[][]{{50L}}, 1);
        WorkerQueryResponse r2 = makeResponse(List.of("cnt"), List.of("Long"), new Object[][]{{30L}}, 1);

        WorkerQueryResponse merged = ResultMerger.merge(List.of(r1, r2), MergeStrategy.GLOBAL_MERGE, null, null, 0);

        assertEquals(1, merged.getRowCount());
        assertEquals(80L, merged.getColumnData()[0][0]);
    }

    public void testGlobalMergeSumIntegers() {
        WorkerQueryResponse r1 = makeResponse(List.of("total"), List.of("Integer"), new Object[][]{{10}}, 1);
        WorkerQueryResponse r2 = makeResponse(List.of("total"), List.of("Integer"), new Object[][]{{20}}, 1);

        WorkerQueryResponse merged = ResultMerger.merge(List.of(r1, r2), MergeStrategy.GLOBAL_MERGE, null, null, 0);

        assertEquals(1, merged.getRowCount());
        assertEquals(30, merged.getColumnData()[0][0]);
    }

    public void testGlobalMergeSumDoubles() {
        WorkerQueryResponse r1 = makeResponse(List.of("amt"), List.of("Double"), new Object[][]{{1.5}}, 1);
        WorkerQueryResponse r2 = makeResponse(List.of("amt"), List.of("Double"), new Object[][]{{2.5}}, 1);

        WorkerQueryResponse merged = ResultMerger.merge(List.of(r1, r2), MergeStrategy.GLOBAL_MERGE, null, null, 0);

        assertEquals(1, merged.getRowCount());
        assertEquals(4.0, (double) merged.getColumnData()[0][0], 0.001);
    }

    public void testGlobalMergeSumFloats() {
        WorkerQueryResponse r1 = makeResponse(List.of("f"), List.of("Float"), new Object[][]{{1.0f}}, 1);
        WorkerQueryResponse r2 = makeResponse(List.of("f"), List.of("Float"), new Object[][]{{2.0f}}, 1);

        WorkerQueryResponse merged = ResultMerger.merge(List.of(r1, r2), MergeStrategy.GLOBAL_MERGE, null, null, 0);

        assertEquals(1, merged.getRowCount());
        assertEquals(3.0f, (float) merged.getColumnData()[0][0], 0.001f);
    }

    public void testGlobalMergeMultipleColumns() {
        // COUNT(*), SUM(amount)
        WorkerQueryResponse r1 = makeResponse(
            List.of("cnt", "total"),
            List.of("Long", "Double"),
            new Object[][]{{100L}, {1000.0}},
            1
        );
        WorkerQueryResponse r2 = makeResponse(
            List.of("cnt", "total"),
            List.of("Long", "Double"),
            new Object[][]{{200L}, {2000.0}},
            1
        );

        WorkerQueryResponse merged = ResultMerger.merge(List.of(r1, r2), MergeStrategy.GLOBAL_MERGE, null, null, 0);

        assertEquals(1, merged.getRowCount());
        assertEquals(300L, merged.getColumnData()[0][0]);
        assertEquals(3000.0, (double) merged.getColumnData()[1][0], 0.001);
    }

    public void testGlobalMergeWithNullValues() {
        WorkerQueryResponse r1 = makeResponse(List.of("val"), List.of("Long"), new Object[][]{{null}}, 1);
        WorkerQueryResponse r2 = makeResponse(List.of("val"), List.of("Long"), new Object[][]{{50L}}, 1);

        WorkerQueryResponse merged = ResultMerger.merge(List.of(r1, r2), MergeStrategy.GLOBAL_MERGE, null, null, 0);

        assertEquals(1, merged.getRowCount());
        assertEquals(50L, merged.getColumnData()[0][0]);
    }

    public void testGlobalMergeAllNullsReturnsNull() {
        WorkerQueryResponse r1 = makeResponse(List.of("val"), List.of("Long"), new Object[][]{{null}}, 1);
        WorkerQueryResponse r2 = makeResponse(List.of("val"), List.of("Long"), new Object[][]{{null}}, 1);

        WorkerQueryResponse merged = ResultMerger.merge(List.of(r1, r2), MergeStrategy.GLOBAL_MERGE, null, null, 0);

        assertEquals(1, merged.getRowCount());
        assertNull(merged.getColumnData()[0][0]);
    }

    public void testGlobalMergeNonNumericReturnsFirstNonNull() {
        WorkerQueryResponse r1 = makeResponse(List.of("s"), List.of("String"), new Object[][]{{"hello"}}, 1);
        WorkerQueryResponse r2 = makeResponse(List.of("s"), List.of("String"), new Object[][]{{"world"}}, 1);

        WorkerQueryResponse merged = ResultMerger.merge(List.of(r1, r2), MergeStrategy.GLOBAL_MERGE, null, null, 0);

        assertEquals(1, merged.getRowCount());
        assertEquals("hello", merged.getColumnData()[0][0]);
    }

    // --- TOPK_MERGE tests ---

    public void testTopKMergeSorted() {
        // Worker 1: rows [1, 3], Worker 2: rows [2, 4], sort by col 0 asc, limit 3
        WorkerQueryResponse r1 = makeResponse(List.of("val"), List.of("Integer"), new Object[][]{{1, 3}}, 2);
        WorkerQueryResponse r2 = makeResponse(List.of("val"), List.of("Integer"), new Object[][]{{2, 4}}, 2);

        WorkerQueryResponse merged = ResultMerger.merge(
            List.of(r1, r2),
            MergeStrategy.TOPK_MERGE,
            new int[]{0},
            new boolean[]{true},
            3
        );

        assertEquals(3, merged.getRowCount());
        assertEquals(1, merged.getColumnData()[0][0]);
        assertEquals(2, merged.getColumnData()[0][1]);
        assertEquals(3, merged.getColumnData()[0][2]);
    }

    public void testTopKMergeDescending() {
        WorkerQueryResponse r1 = makeResponse(List.of("val"), List.of("Integer"), new Object[][]{{3, 1}}, 2);
        WorkerQueryResponse r2 = makeResponse(List.of("val"), List.of("Integer"), new Object[][]{{4, 2}}, 2);

        WorkerQueryResponse merged = ResultMerger.merge(
            List.of(r1, r2),
            MergeStrategy.TOPK_MERGE,
            new int[]{0},
            new boolean[]{false},
            2
        );

        assertEquals(2, merged.getRowCount());
        assertEquals(4, merged.getColumnData()[0][0]);
        assertEquals(3, merged.getColumnData()[0][1]);
    }

    public void testTopKMergeLimitExceedsRows() {
        WorkerQueryResponse r1 = makeResponse(List.of("val"), List.of("Integer"), new Object[][]{{1}}, 1);

        WorkerQueryResponse merged = ResultMerger.merge(
            List.of(r1),
            MergeStrategy.TOPK_MERGE,
            new int[]{0},
            new boolean[]{true},
            100
        );

        assertEquals(1, merged.getRowCount());
    }

    public void testTopKMergeNoSortColumns() {
        // Without sort columns, just takes first N rows
        WorkerQueryResponse r1 = makeResponse(List.of("val"), List.of("Integer"), new Object[][]{{3, 1}}, 2);
        WorkerQueryResponse r2 = makeResponse(List.of("val"), List.of("Integer"), new Object[][]{{4, 2}}, 2);

        WorkerQueryResponse merged = ResultMerger.merge(
            List.of(r1, r2),
            MergeStrategy.TOPK_MERGE,
            null,
            null,
            3
        );

        assertEquals(3, merged.getRowCount());
    }

    public void testTopKMergeZeroLimit() {
        // limit=0 means no limit → return all rows
        WorkerQueryResponse r1 = makeResponse(List.of("val"), List.of("Integer"), new Object[][]{{1, 2}}, 2);

        WorkerQueryResponse merged = ResultMerger.merge(
            List.of(r1),
            MergeStrategy.TOPK_MERGE,
            new int[]{0},
            new boolean[]{true},
            0
        );

        assertEquals(2, merged.getRowCount());
    }

    // --- SINGLE_NODE tests ---

    public void testSingleNodePassthrough() {
        WorkerQueryResponse r1 = makeResponse(List.of("val"), List.of("Integer"), new Object[][]{{42}}, 1);

        WorkerQueryResponse merged = ResultMerger.merge(List.of(r1), MergeStrategy.SINGLE_NODE, null, null, 0);

        assertSame(r1, merged);
    }

    // --- Empty response tests ---

    public void testEmptyResponsesReturnEmpty() {
        WorkerQueryResponse empty1 = makeResponse(List.of("col"), List.of("Integer"), new Object[0][], 0);
        WorkerQueryResponse empty2 = makeResponse(List.of("col"), List.of("Integer"), new Object[0][], 0);

        WorkerQueryResponse merged = ResultMerger.merge(List.of(empty1, empty2), MergeStrategy.CONCAT, null, null, 0);

        assertEquals(0, merged.getRowCount());
        assertEquals(List.of("col"), merged.getColumnNames());
    }

    public void testEmptyResponseListReturnsEmpty() {
        WorkerQueryResponse merged = ResultMerger.merge(List.of(), MergeStrategy.CONCAT, null, null, 0);
        // Empty list with no metadata → empty column names too
        assertTrue(merged.getColumnNames().isEmpty());
        assertEquals(0, merged.getRowCount());
    }

    public void testMixedEmptyAndNonEmptyResponses() {
        WorkerQueryResponse empty = makeResponse(List.of("val"), List.of("Integer"), new Object[0][], 0);
        WorkerQueryResponse nonEmpty = makeResponse(List.of("val"), List.of("Integer"), new Object[][]{{42}}, 1);

        WorkerQueryResponse merged = ResultMerger.merge(List.of(empty, nonEmpty), MergeStrategy.CONCAT, null, null, 0);

        assertEquals(1, merged.getRowCount());
        assertEquals(42, merged.getColumnData()[0][0]);
    }

    // --- Comparator tests ---

    public void testCompareValuesNulls() {
        assertEquals(0, ResultMerger.compareValues(null, null));
        assertEquals(1, ResultMerger.compareValues(null, "a"));
        assertEquals(-1, ResultMerger.compareValues("a", null));
    }

    public void testCompareValuesComparable() {
        assertTrue(ResultMerger.compareValues(1, 2) < 0);
        assertTrue(ResultMerger.compareValues(2, 1) > 0);
        assertEquals(0, ResultMerger.compareValues(5, 5));
    }

    public void testCompareValuesNonComparableUsesToString() {
        // Objects that aren't Comparable — falls back to toString comparison
        Object a = new Object() {
            @Override
            public String toString() {
                return "apple";
            }
        };
        Object b = new Object() {
            @Override
            public String toString() {
                return "banana";
            }
        };
        assertTrue(ResultMerger.compareValues(a, b) < 0);
    }

    public void testBuildComparator() {
        Comparator<Object[]> cmp = ResultMerger.buildComparator(new int[]{0}, new boolean[]{true});
        Object[] row1 = {1};
        Object[] row2 = {2};
        assertTrue(cmp.compare(row1, row2) < 0);
    }

    public void testBuildComparatorDescending() {
        Comparator<Object[]> cmp = ResultMerger.buildComparator(new int[]{0}, new boolean[]{false});
        Object[] row1 = {1};
        Object[] row2 = {2};
        assertTrue(cmp.compare(row1, row2) > 0);
    }

    public void testBuildComparatorMultiColumn() {
        Comparator<Object[]> cmp = ResultMerger.buildComparator(new int[]{0, 1}, new boolean[]{true, false});
        Object[] row1 = {1, "b"};
        Object[] row2 = {1, "a"};
        // First column equal, second column descending: "b" > "a" but reversed → row1 before row2 (negative)
        assertTrue(cmp.compare(row1, row2) < 0);
    }

    public void testBuildComparatorNullSortAsc() {
        // null sortAsc defaults to ascending
        Comparator<Object[]> cmp = ResultMerger.buildComparator(new int[]{0}, null);
        Object[] row1 = {1};
        Object[] row2 = {2};
        assertTrue(cmp.compare(row1, row2) < 0);
    }

    public void testFilterNonEmpty() {
        WorkerQueryResponse empty = makeResponse(List.of("col"), List.of("Integer"), new Object[0][], 0);
        WorkerQueryResponse nonEmpty = makeResponse(List.of("col"), List.of("Integer"), new Object[][]{{1}}, 1);

        List<WorkerQueryResponse> result = ResultMerger.filterNonEmpty(List.of(empty, nonEmpty));
        assertEquals(1, result.size());
        assertSame(nonEmpty, result.get(0));
    }

    public void testEmptyResponseFromEmptyList() {
        WorkerQueryResponse r = ResultMerger.emptyResponse(List.of());
        assertEquals(0, r.getRowCount());
        assertTrue(r.getColumnNames().isEmpty());
    }

    public void testEmptyResponsePreservesMetadata() {
        WorkerQueryResponse r1 = makeResponse(List.of("a", "b"), List.of("Integer", "String"), new Object[0][], 0);
        WorkerQueryResponse r = ResultMerger.emptyResponse(List.of(r1));
        assertEquals(List.of("a", "b"), r.getColumnNames());
        assertEquals(List.of("Integer", "String"), r.getColumnTypes());
    }

    // --- Helper ---

    private WorkerQueryResponse makeResponse(List<String> names, List<String> types, Object[][] columnData, int rowCount) {
        return new WorkerQueryResponse(names, types, rowCount, columnData);
    }
}

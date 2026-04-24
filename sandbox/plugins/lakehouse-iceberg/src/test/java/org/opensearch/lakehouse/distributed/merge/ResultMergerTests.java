/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed.merge;

import org.opensearch.lakehouse.distributed.worker.WorkerQueryResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Comparator;
import java.util.List;

public class ResultMergerTests extends OpenSearchTestCase {

    // --- ResultMerger dispatch tests ---

    public void testConcatThrowsIllegalState() {
        WorkerQueryResponse r1 = makeResponse(List.of("col"), List.of("Integer"), new Object[][]{{1, 2}}, 2);
        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> ResultMerger.merge(List.of(r1), MergeStrategy.CONCAT)
        );
        assertTrue(ex.getMessage().contains("DataFusion"));
    }

    public void testGlobalMergeThrowsIllegalState() {
        WorkerQueryResponse r1 = makeResponse(List.of("cnt"), List.of("Long"), new Object[][]{{50L}}, 1);
        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> ResultMerger.merge(List.of(r1), MergeStrategy.GLOBAL_MERGE)
        );
        assertTrue(ex.getMessage().contains("DataFusion"));
    }

    public void testTopKMergeThrowsIllegalState() {
        WorkerQueryResponse r1 = makeResponse(List.of("val"), List.of("Integer"), new Object[][]{{1}}, 1);
        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> ResultMerger.merge(List.of(r1), MergeStrategy.TOPK_MERGE)
        );
        assertTrue(ex.getMessage().contains("DataFusion"));
    }

    public void testSingleNodePassthrough() {
        WorkerQueryResponse r1 = makeResponse(List.of("val"), List.of("Integer"), new Object[][]{{42}}, 1);
        WorkerQueryResponse merged = ResultMerger.merge(List.of(r1), MergeStrategy.SINGLE_NODE);
        assertSame(r1, merged);
    }

    public void testSingleNodeSkipsEmpty() {
        WorkerQueryResponse empty = makeResponse(List.of("val"), List.of("Integer"), new Object[0][], 0);
        WorkerQueryResponse nonEmpty = makeResponse(List.of("val"), List.of("Integer"), new Object[][]{{42}}, 1);
        WorkerQueryResponse merged = ResultMerger.merge(List.of(empty, nonEmpty), MergeStrategy.SINGLE_NODE);
        assertSame(nonEmpty, merged);
    }

    public void testSingleNodeAllEmptyReturnsEmpty() {
        WorkerQueryResponse empty1 = makeResponse(List.of("col"), List.of("Integer"), new Object[0][], 0);
        WorkerQueryResponse empty2 = makeResponse(List.of("col"), List.of("Integer"), new Object[0][], 0);
        WorkerQueryResponse merged = ResultMerger.merge(List.of(empty1, empty2), MergeStrategy.SINGLE_NODE);
        assertEquals(0, merged.getRowCount());
        assertEquals(List.of("col"), merged.getColumnNames());
    }

    public void testEmptyResponseListReturnsEmpty() {
        WorkerQueryResponse merged = ResultMerger.merge(List.of(), MergeStrategy.SINGLE_NODE);
        assertTrue(merged.getColumnNames().isEmpty());
        assertEquals(0, merged.getRowCount());
    }

    public void testEmptyResponsePreservesMetadata() {
        WorkerQueryResponse r = ResultMerger.emptyResponse(
            List.of(makeResponse(List.of("a", "b"), List.of("Integer", "String"), new Object[0][], 0))
        );
        assertEquals(List.of("a", "b"), r.getColumnNames());
        assertEquals(List.of("Integer", "String"), r.getColumnTypes());
    }

    public void testEmptyResponseFromEmptyList() {
        WorkerQueryResponse r = ResultMerger.emptyResponse(List.of());
        assertEquals(0, r.getRowCount());
        assertTrue(r.getColumnNames().isEmpty());
    }

    // --- AggregationReducer tests (standalone, not via ResultMerger) ---

    public void testSumColumnLongs() {
        WorkerQueryResponse r1 = makeResponse(List.of("v"), List.of("Long"), new Object[][]{{50L}}, 1);
        WorkerQueryResponse r2 = makeResponse(List.of("v"), List.of("Long"), new Object[][]{{30L}}, 1);
        assertEquals(80L, AggregationReducer.sumColumn(List.of(r1, r2), 0));
    }

    public void testSumColumnIntegers() {
        WorkerQueryResponse r1 = makeResponse(List.of("v"), List.of("Integer"), new Object[][]{{10}}, 1);
        WorkerQueryResponse r2 = makeResponse(List.of("v"), List.of("Integer"), new Object[][]{{20}}, 1);
        assertEquals(30L, AggregationReducer.sumColumn(List.of(r1, r2), 0));
    }

    public void testSumColumnDoubles() {
        WorkerQueryResponse r1 = makeResponse(List.of("v"), List.of("Double"), new Object[][]{{1.5}}, 1);
        WorkerQueryResponse r2 = makeResponse(List.of("v"), List.of("Double"), new Object[][]{{2.5}}, 1);
        assertEquals(4.0, (double) AggregationReducer.sumColumn(List.of(r1, r2), 0), 0.001);
    }

    public void testSumColumnFloats() {
        WorkerQueryResponse r1 = makeResponse(List.of("v"), List.of("Float"), new Object[][]{{1.0f}}, 1);
        WorkerQueryResponse r2 = makeResponse(List.of("v"), List.of("Float"), new Object[][]{{2.0f}}, 1);
        assertEquals(3.0f, (float) AggregationReducer.sumColumn(List.of(r1, r2), 0), 0.001f);
    }

    public void testSumColumnWithNulls() {
        WorkerQueryResponse r1 = makeResponse(List.of("v"), List.of("Long"), new Object[][]{{null}}, 1);
        WorkerQueryResponse r2 = makeResponse(List.of("v"), List.of("Long"), new Object[][]{{50L}}, 1);
        assertEquals(50L, AggregationReducer.sumColumn(List.of(r1, r2), 0));
    }

    public void testSumColumnAllNulls() {
        WorkerQueryResponse r1 = makeResponse(List.of("v"), List.of("Long"), new Object[][]{{null}}, 1);
        WorkerQueryResponse r2 = makeResponse(List.of("v"), List.of("Long"), new Object[][]{{null}}, 1);
        assertNull(AggregationReducer.sumColumn(List.of(r1, r2), 0));
    }

    public void testSumColumnNonNumeric() {
        WorkerQueryResponse r1 = makeResponse(List.of("v"), List.of("String"), new Object[][]{{"hello"}}, 1);
        WorkerQueryResponse r2 = makeResponse(List.of("v"), List.of("String"), new Object[][]{{"world"}}, 1);
        assertEquals("hello", AggregationReducer.sumColumn(List.of(r1, r2), 0));
    }

    public void testMinColumn() {
        WorkerQueryResponse r1 = makeResponse(List.of("v"), List.of("Integer"), new Object[][]{{30}}, 1);
        WorkerQueryResponse r2 = makeResponse(List.of("v"), List.of("Integer"), new Object[][]{{10}}, 1);
        WorkerQueryResponse r3 = makeResponse(List.of("v"), List.of("Integer"), new Object[][]{{20}}, 1);
        assertEquals(10, AggregationReducer.minColumn(List.of(r1, r2, r3), 0));
    }

    public void testMaxColumn() {
        WorkerQueryResponse r1 = makeResponse(List.of("v"), List.of("Integer"), new Object[][]{{30}}, 1);
        WorkerQueryResponse r2 = makeResponse(List.of("v"), List.of("Integer"), new Object[][]{{10}}, 1);
        assertEquals(30, AggregationReducer.maxColumn(List.of(r1, r2), 0));
    }

    public void testMinColumnStrings() {
        WorkerQueryResponse r1 = makeResponse(List.of("v"), List.of("String"), new Object[][]{{"2013-07-05"}}, 1);
        WorkerQueryResponse r2 = makeResponse(List.of("v"), List.of("String"), new Object[][]{{"2013-07-01"}}, 1);
        assertEquals("2013-07-01", AggregationReducer.minColumn(List.of(r1, r2), 0));
    }

    public void testMaxColumnStrings() {
        WorkerQueryResponse r1 = makeResponse(List.of("v"), List.of("String"), new Object[][]{{"2013-07-20"}}, 1);
        WorkerQueryResponse r2 = makeResponse(List.of("v"), List.of("String"), new Object[][]{{"2013-07-30"}}, 1);
        assertEquals("2013-07-30", AggregationReducer.maxColumn(List.of(r1, r2), 0));
    }

    public void testMinColumnWithNulls() {
        WorkerQueryResponse r1 = makeResponse(List.of("v"), List.of("Long"), new Object[][]{{null}}, 1);
        WorkerQueryResponse r2 = makeResponse(List.of("v"), List.of("Long"), new Object[][]{{50L}}, 1);
        assertEquals(50L, AggregationReducer.minColumn(List.of(r1, r2), 0));
    }

    public void testMinColumnAllNulls() {
        WorkerQueryResponse r1 = makeResponse(List.of("v"), List.of("Long"), new Object[][]{{null}}, 1);
        WorkerQueryResponse r2 = makeResponse(List.of("v"), List.of("Long"), new Object[][]{{null}}, 1);
        assertNull(AggregationReducer.minColumn(List.of(r1, r2), 0));
    }

    // --- TopKMerger tests (standalone) ---

    public void testTopKMergeSorted() {
        WorkerQueryResponse r1 = makeResponse(List.of("val"), List.of("Integer"), new Object[][]{{1, 3}}, 2);
        WorkerQueryResponse r2 = makeResponse(List.of("val"), List.of("Integer"), new Object[][]{{2, 4}}, 2);
        WorkerQueryResponse merged = TopKMerger.merge(List.of(r1, r2), new int[]{0}, new boolean[]{true}, 3);
        assertEquals(3, merged.getRowCount());
        assertEquals(1, merged.getColumnData()[0][0]);
        assertEquals(2, merged.getColumnData()[0][1]);
        assertEquals(3, merged.getColumnData()[0][2]);
    }

    public void testTopKMergeDescending() {
        WorkerQueryResponse r1 = makeResponse(List.of("val"), List.of("Integer"), new Object[][]{{3, 1}}, 2);
        WorkerQueryResponse r2 = makeResponse(List.of("val"), List.of("Integer"), new Object[][]{{4, 2}}, 2);
        WorkerQueryResponse merged = TopKMerger.merge(List.of(r1, r2), new int[]{0}, new boolean[]{false}, 2);
        assertEquals(2, merged.getRowCount());
        assertEquals(4, merged.getColumnData()[0][0]);
        assertEquals(3, merged.getColumnData()[0][1]);
    }

    public void testTopKMergeLimitExceedsRows() {
        WorkerQueryResponse r1 = makeResponse(List.of("val"), List.of("Integer"), new Object[][]{{1}}, 1);
        WorkerQueryResponse merged = TopKMerger.merge(List.of(r1), new int[]{0}, new boolean[]{true}, 100);
        assertEquals(1, merged.getRowCount());
    }

    public void testTopKMergeNoSortColumns() {
        WorkerQueryResponse r1 = makeResponse(List.of("val"), List.of("Integer"), new Object[][]{{3, 1}}, 2);
        WorkerQueryResponse r2 = makeResponse(List.of("val"), List.of("Integer"), new Object[][]{{4, 2}}, 2);
        WorkerQueryResponse merged = TopKMerger.merge(List.of(r1, r2), null, null, 3);
        assertEquals(3, merged.getRowCount());
    }

    public void testTopKMergeZeroLimit() {
        WorkerQueryResponse r1 = makeResponse(List.of("val"), List.of("Integer"), new Object[][]{{1, 2}}, 2);
        WorkerQueryResponse merged = TopKMerger.merge(List.of(r1), new int[]{0}, new boolean[]{true}, 0);
        assertEquals(2, merged.getRowCount());
    }

    // --- Comparator tests ---

    public void testCompareValuesNulls() {
        assertEquals(0, TopKMerger.compareValues(null, null));
        assertEquals(1, TopKMerger.compareValues(null, "a"));
        assertEquals(-1, TopKMerger.compareValues("a", null));
    }

    public void testCompareValuesComparable() {
        assertTrue(TopKMerger.compareValues(1, 2) < 0);
        assertTrue(TopKMerger.compareValues(2, 1) > 0);
        assertEquals(0, TopKMerger.compareValues(5, 5));
    }

    public void testCompareValuesNonComparableUsesToString() {
        Object a = new Object() { @Override public String toString() { return "apple"; } };
        Object b = new Object() { @Override public String toString() { return "banana"; } };
        assertTrue(TopKMerger.compareValues(a, b) < 0);
    }

    public void testBuildComparator() {
        Comparator<Object[]> cmp = TopKMerger.buildComparator(new int[]{0}, new boolean[]{true});
        assertTrue(cmp.compare(new Object[]{1}, new Object[]{2}) < 0);
    }

    public void testBuildComparatorDescending() {
        Comparator<Object[]> cmp = TopKMerger.buildComparator(new int[]{0}, new boolean[]{false});
        assertTrue(cmp.compare(new Object[]{1}, new Object[]{2}) > 0);
    }

    public void testBuildComparatorMultiColumn() {
        Comparator<Object[]> cmp = TopKMerger.buildComparator(new int[]{0, 1}, new boolean[]{true, false});
        assertTrue(cmp.compare(new Object[]{1, "b"}, new Object[]{1, "a"}) < 0);
    }

    public void testBuildComparatorNullSortAsc() {
        Comparator<Object[]> cmp = TopKMerger.buildComparator(new int[]{0}, null);
        assertTrue(cmp.compare(new Object[]{1}, new Object[]{2}) < 0);
    }

    // --- Helper ---

    private WorkerQueryResponse makeResponse(List<String> names, List<String> types, Object[][] columnData, int rowCount) {
        return new WorkerQueryResponse(names, types, rowCount, columnData);
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Comparator;
import java.util.List;

public class TopKMergerTests extends OpenSearchTestCase {

    // --- merge tests ---

    public void testMergeSortedAscending() {
        WorkerQueryResponse r1 = makeResponse(List.of("val"), List.of("Integer"), new Object[][]{{1, 3}}, 2);
        WorkerQueryResponse r2 = makeResponse(List.of("val"), List.of("Integer"), new Object[][]{{2, 4}}, 2);

        WorkerQueryResponse merged = TopKMerger.merge(List.of(r1, r2), new int[]{0}, new boolean[]{true}, 3);

        assertEquals(3, merged.getRowCount());
        assertEquals(1, merged.getColumnData()[0][0]);
        assertEquals(2, merged.getColumnData()[0][1]);
        assertEquals(3, merged.getColumnData()[0][2]);
    }

    public void testMergeSortedDescending() {
        WorkerQueryResponse r1 = makeResponse(List.of("val"), List.of("Integer"), new Object[][]{{3, 1}}, 2);
        WorkerQueryResponse r2 = makeResponse(List.of("val"), List.of("Integer"), new Object[][]{{4, 2}}, 2);

        WorkerQueryResponse merged = TopKMerger.merge(List.of(r1, r2), new int[]{0}, new boolean[]{false}, 2);

        assertEquals(2, merged.getRowCount());
        assertEquals(4, merged.getColumnData()[0][0]);
        assertEquals(3, merged.getColumnData()[0][1]);
    }

    public void testMergeLimitExceedsRows() {
        WorkerQueryResponse r1 = makeResponse(List.of("val"), List.of("Integer"), new Object[][]{{1}}, 1);

        WorkerQueryResponse merged = TopKMerger.merge(List.of(r1), new int[]{0}, new boolean[]{true}, 100);

        assertEquals(1, merged.getRowCount());
    }

    public void testMergeNoSortColumns() {
        WorkerQueryResponse r1 = makeResponse(List.of("val"), List.of("Integer"), new Object[][]{{3, 1}}, 2);
        WorkerQueryResponse r2 = makeResponse(List.of("val"), List.of("Integer"), new Object[][]{{4, 2}}, 2);

        WorkerQueryResponse merged = TopKMerger.merge(List.of(r1, r2), null, null, 3);

        assertEquals(3, merged.getRowCount());
    }

    public void testMergeEmptySortColumns() {
        WorkerQueryResponse r1 = makeResponse(List.of("val"), List.of("Integer"), new Object[][]{{3, 1}}, 2);

        WorkerQueryResponse merged = TopKMerger.merge(List.of(r1), new int[]{}, new boolean[]{}, 2);

        assertEquals(2, merged.getRowCount());
    }

    public void testMergeZeroLimitReturnsAll() {
        WorkerQueryResponse r1 = makeResponse(List.of("val"), List.of("Integer"), new Object[][]{{1, 2}}, 2);

        WorkerQueryResponse merged = TopKMerger.merge(List.of(r1), new int[]{0}, new boolean[]{true}, 0);

        assertEquals(2, merged.getRowCount());
    }

    public void testMergeMultipleColumns() {
        WorkerQueryResponse r1 = makeResponse(
            List.of("a", "b"), List.of("Integer", "String"),
            new Object[][]{{1, 2}, {"x", "y"}}, 2
        );
        WorkerQueryResponse r2 = makeResponse(
            List.of("a", "b"), List.of("Integer", "String"),
            new Object[][]{{0}, {"z"}}, 1
        );

        WorkerQueryResponse merged = TopKMerger.merge(List.of(r1, r2), new int[]{0}, new boolean[]{true}, 2);

        assertEquals(2, merged.getRowCount());
        assertEquals(0, merged.getColumnData()[0][0]);
        assertEquals(1, merged.getColumnData()[0][1]);
        assertEquals("z", merged.getColumnData()[1][0]);
        assertEquals("x", merged.getColumnData()[1][1]);
    }

    public void testMergePreservesColumnMetadata() {
        WorkerQueryResponse r1 = makeResponse(List.of("name", "age"), List.of("String", "Integer"), new Object[][]{{"alice"}, {30}}, 1);

        WorkerQueryResponse merged = TopKMerger.merge(List.of(r1), new int[]{1}, new boolean[]{true}, 10);

        assertEquals(List.of("name", "age"), merged.getColumnNames());
        assertEquals(List.of("String", "Integer"), merged.getColumnTypes());
    }

    // --- compareValues tests ---

    public void testCompareValuesBothNull() {
        assertEquals(0, TopKMerger.compareValues(null, null));
    }

    public void testCompareValuesFirstNull() {
        assertEquals(1, TopKMerger.compareValues(null, "a"));
    }

    public void testCompareValuesSecondNull() {
        assertEquals(-1, TopKMerger.compareValues("a", null));
    }

    public void testCompareValuesComparableLessThan() {
        assertTrue(TopKMerger.compareValues(1, 2) < 0);
    }

    public void testCompareValuesComparableGreaterThan() {
        assertTrue(TopKMerger.compareValues(2, 1) > 0);
    }

    public void testCompareValuesComparableEqual() {
        assertEquals(0, TopKMerger.compareValues(5, 5));
    }

    public void testCompareValuesStrings() {
        assertTrue(TopKMerger.compareValues("apple", "banana") < 0);
        assertTrue(TopKMerger.compareValues("banana", "apple") > 0);
        assertEquals(0, TopKMerger.compareValues("same", "same"));
    }

    public void testCompareValuesNonComparableUsesToString() {
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
        assertTrue(TopKMerger.compareValues(a, b) < 0);
    }

    // --- buildComparator tests ---

    public void testBuildComparatorAscending() {
        Comparator<Object[]> cmp = TopKMerger.buildComparator(new int[]{0}, new boolean[]{true});
        Object[] row1 = {1};
        Object[] row2 = {2};
        assertTrue(cmp.compare(row1, row2) < 0);
    }

    public void testBuildComparatorDescending() {
        Comparator<Object[]> cmp = TopKMerger.buildComparator(new int[]{0}, new boolean[]{false});
        Object[] row1 = {1};
        Object[] row2 = {2};
        assertTrue(cmp.compare(row1, row2) > 0);
    }

    public void testBuildComparatorMultiColumn() {
        Comparator<Object[]> cmp = TopKMerger.buildComparator(new int[]{0, 1}, new boolean[]{true, false});
        Object[] row1 = {1, "b"};
        Object[] row2 = {1, "a"};
        // First column equal, second column descending: "b" > "a" but reversed -> row1 before row2
        assertTrue(cmp.compare(row1, row2) < 0);
    }

    public void testBuildComparatorNullSortAscDefaultsToAscending() {
        Comparator<Object[]> cmp = TopKMerger.buildComparator(new int[]{0}, null);
        Object[] row1 = {1};
        Object[] row2 = {2};
        assertTrue(cmp.compare(row1, row2) < 0);
    }

    public void testBuildComparatorEqualRows() {
        Comparator<Object[]> cmp = TopKMerger.buildComparator(new int[]{0, 1}, new boolean[]{true, true});
        Object[] row1 = {1, "a"};
        Object[] row2 = {1, "a"};
        assertEquals(0, cmp.compare(row1, row2));
    }

    public void testBuildComparatorColumnBeyondRowLength() {
        // Sort column index beyond row length — treats missing values as null
        Comparator<Object[]> cmp = TopKMerger.buildComparator(new int[]{5}, new boolean[]{true});
        Object[] row1 = {1};
        Object[] row2 = {2};
        // Both col 5 values are null → equal
        assertEquals(0, cmp.compare(row1, row2));
    }

    public void testBuildComparatorSortAscShorterThanSortColumns() {
        // sortAsc array shorter than sortColumns — extra columns default to ascending
        Comparator<Object[]> cmp = TopKMerger.buildComparator(new int[]{0, 1}, new boolean[]{false});
        Object[] row1 = {1, "a"};
        Object[] row2 = {1, "b"};
        // col 0 equal (desc doesn't matter), col 1 defaults to asc: "a" < "b"
        assertTrue(cmp.compare(row1, row2) < 0);
    }

    // --- Helper ---

    private WorkerQueryResponse makeResponse(List<String> names, List<String> types, Object[][] columnData, int rowCount) {
        return new WorkerQueryResponse(names, types, rowCount, columnData);
    }
}

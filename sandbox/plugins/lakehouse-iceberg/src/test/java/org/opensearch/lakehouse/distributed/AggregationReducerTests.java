/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class AggregationReducerTests extends OpenSearchTestCase {

    // --- sumColumn tests ---

    public void testSumColumnLong() {
        WorkerQueryResponse r1 = makeResponse(new Object[][]{{50L}}, 1);
        WorkerQueryResponse r2 = makeResponse(new Object[][]{{30L}}, 1);

        Object result = AggregationReducer.sumColumn(List.of(r1, r2), 0);
        assertEquals(80L, result);
    }

    public void testSumColumnInteger() {
        WorkerQueryResponse r1 = makeResponse(new Object[][]{{10}}, 1);
        WorkerQueryResponse r2 = makeResponse(new Object[][]{{20}}, 1);

        Object result = AggregationReducer.sumColumn(List.of(r1, r2), 0);
        // Integer sums promoted to Long
        assertEquals(30L, result);
    }

    public void testSumColumnDouble() {
        WorkerQueryResponse r1 = makeResponse(new Object[][]{{1.5}}, 1);
        WorkerQueryResponse r2 = makeResponse(new Object[][]{{2.5}}, 1);

        Object result = AggregationReducer.sumColumn(List.of(r1, r2), 0);
        assertEquals(4.0, (double) result, 0.001);
    }

    public void testSumColumnFloat() {
        WorkerQueryResponse r1 = makeResponse(new Object[][]{{1.0f}}, 1);
        WorkerQueryResponse r2 = makeResponse(new Object[][]{{2.0f}}, 1);

        Object result = AggregationReducer.sumColumn(List.of(r1, r2), 0);
        assertEquals(3.0f, (float) result, 0.001f);
    }

    public void testSumColumnWithNulls() {
        WorkerQueryResponse r1 = makeResponse(new Object[][]{{null}}, 1);
        WorkerQueryResponse r2 = makeResponse(new Object[][]{{50L}}, 1);

        Object result = AggregationReducer.sumColumn(List.of(r1, r2), 0);
        assertEquals(50L, result);
    }

    public void testSumColumnAllNullsReturnsNull() {
        WorkerQueryResponse r1 = makeResponse(new Object[][]{{null}}, 1);
        WorkerQueryResponse r2 = makeResponse(new Object[][]{{null}}, 1);

        Object result = AggregationReducer.sumColumn(List.of(r1, r2), 0);
        assertNull(result);
    }

    public void testSumColumnNonNumericReturnsFirstNonNull() {
        WorkerQueryResponse r1 = makeResponse(new Object[][]{{"hello"}}, 1);
        WorkerQueryResponse r2 = makeResponse(new Object[][]{{"world"}}, 1);

        Object result = AggregationReducer.sumColumn(List.of(r1, r2), 0);
        assertEquals("hello", result);
    }

    public void testSumColumnSkipsEmptyResponses() {
        WorkerQueryResponse empty = makeResponse(new Object[][]{{}}, 0);
        WorkerQueryResponse r2 = makeResponse(new Object[][]{{100L}}, 1);

        Object result = AggregationReducer.sumColumn(List.of(empty, r2), 0);
        assertEquals(100L, result);
    }

    public void testSumColumnLongWithNullInMiddle() {
        WorkerQueryResponse r1 = makeResponse(new Object[][]{{10L}}, 1);
        WorkerQueryResponse r2 = makeResponse(new Object[][]{{null}}, 1);
        WorkerQueryResponse r3 = makeResponse(new Object[][]{{30L}}, 1);

        Object result = AggregationReducer.sumColumn(List.of(r1, r2, r3), 0);
        assertEquals(40L, result);
    }

    public void testSumColumnIntegerWithNullInMiddle() {
        WorkerQueryResponse r1 = makeResponse(new Object[][]{{10}}, 1);
        WorkerQueryResponse r2 = makeResponse(new Object[][]{{null}}, 1);
        WorkerQueryResponse r3 = makeResponse(new Object[][]{{20}}, 1);

        Object result = AggregationReducer.sumColumn(List.of(r1, r2, r3), 0);
        assertEquals(30L, result);
    }

    public void testSumColumnDoubleWithNullInMiddle() {
        WorkerQueryResponse r1 = makeResponse(new Object[][]{{1.0}}, 1);
        WorkerQueryResponse r2 = makeResponse(new Object[][]{{null}}, 1);
        WorkerQueryResponse r3 = makeResponse(new Object[][]{{2.0}}, 1);

        Object result = AggregationReducer.sumColumn(List.of(r1, r2, r3), 0);
        assertEquals(3.0, (double) result, 0.001);
    }

    public void testSumColumnFloatWithNullInMiddle() {
        WorkerQueryResponse r1 = makeResponse(new Object[][]{{1.0f}}, 1);
        WorkerQueryResponse r2 = makeResponse(new Object[][]{{null}}, 1);
        WorkerQueryResponse r3 = makeResponse(new Object[][]{{2.0f}}, 1);

        Object result = AggregationReducer.sumColumn(List.of(r1, r2, r3), 0);
        assertEquals(3.0f, (float) result, 0.001f);
    }

    // --- minColumn tests ---

    public void testMinColumnIntegers() {
        WorkerQueryResponse r1 = makeResponse(new Object[][]{{30}}, 1);
        WorkerQueryResponse r2 = makeResponse(new Object[][]{{10}}, 1);
        WorkerQueryResponse r3 = makeResponse(new Object[][]{{20}}, 1);

        Object result = AggregationReducer.minColumn(List.of(r1, r2, r3), 0);
        assertEquals(10, result);
    }

    public void testMinColumnLongs() {
        WorkerQueryResponse r1 = makeResponse(new Object[][]{{100L}}, 1);
        WorkerQueryResponse r2 = makeResponse(new Object[][]{{50L}}, 1);

        Object result = AggregationReducer.minColumn(List.of(r1, r2), 0);
        assertEquals(50L, result);
    }

    public void testMinColumnStrings() {
        WorkerQueryResponse r1 = makeResponse(new Object[][]{{"2013-07-05"}}, 1);
        WorkerQueryResponse r2 = makeResponse(new Object[][]{{"2013-07-01"}}, 1);
        WorkerQueryResponse r3 = makeResponse(new Object[][]{{"2013-07-10"}}, 1);

        Object result = AggregationReducer.minColumn(List.of(r1, r2, r3), 0);
        assertEquals("2013-07-01", result);
    }

    public void testMinColumnWithNulls() {
        WorkerQueryResponse r1 = makeResponse(new Object[][]{{null}}, 1);
        WorkerQueryResponse r2 = makeResponse(new Object[][]{{50L}}, 1);

        Object result = AggregationReducer.minColumn(List.of(r1, r2), 0);
        assertEquals(50L, result);
    }

    public void testMinColumnAllNullsReturnsNull() {
        WorkerQueryResponse r1 = makeResponse(new Object[][]{{null}}, 1);
        WorkerQueryResponse r2 = makeResponse(new Object[][]{{null}}, 1);

        Object result = AggregationReducer.minColumn(List.of(r1, r2), 0);
        assertNull(result);
    }

    public void testMinColumnSkipsEmptyResponses() {
        WorkerQueryResponse empty = makeResponse(new Object[][]{{}}, 0);
        WorkerQueryResponse r2 = makeResponse(new Object[][]{{42}}, 1);

        Object result = AggregationReducer.minColumn(List.of(empty, r2), 0);
        assertEquals(42, result);
    }

    // --- maxColumn tests ---

    public void testMaxColumnIntegers() {
        WorkerQueryResponse r1 = makeResponse(new Object[][]{{30}}, 1);
        WorkerQueryResponse r2 = makeResponse(new Object[][]{{10}}, 1);

        Object result = AggregationReducer.maxColumn(List.of(r1, r2), 0);
        assertEquals(30, result);
    }

    public void testMaxColumnDoubles() {
        WorkerQueryResponse r1 = makeResponse(new Object[][]{{1.5}}, 1);
        WorkerQueryResponse r2 = makeResponse(new Object[][]{{3.5}}, 1);

        Object result = AggregationReducer.maxColumn(List.of(r1, r2), 0);
        assertEquals(3.5, result);
    }

    public void testMaxColumnStrings() {
        WorkerQueryResponse r1 = makeResponse(new Object[][]{{"2013-07-20"}}, 1);
        WorkerQueryResponse r2 = makeResponse(new Object[][]{{"2013-07-30"}}, 1);
        WorkerQueryResponse r3 = makeResponse(new Object[][]{{"2013-07-15"}}, 1);

        Object result = AggregationReducer.maxColumn(List.of(r1, r2, r3), 0);
        assertEquals("2013-07-30", result);
    }

    public void testMaxColumnWithNulls() {
        WorkerQueryResponse r1 = makeResponse(new Object[][]{{null}}, 1);
        WorkerQueryResponse r2 = makeResponse(new Object[][]{{50L}}, 1);

        Object result = AggregationReducer.maxColumn(List.of(r1, r2), 0);
        assertEquals(50L, result);
    }

    public void testMaxColumnAllNullsReturnsNull() {
        WorkerQueryResponse r1 = makeResponse(new Object[][]{{null}}, 1);
        WorkerQueryResponse r2 = makeResponse(new Object[][]{{null}}, 1);

        Object result = AggregationReducer.maxColumn(List.of(r1, r2), 0);
        assertNull(result);
    }

    public void testMaxColumnSkipsEmptyResponses() {
        WorkerQueryResponse empty = makeResponse(new Object[][]{{}}, 0);
        WorkerQueryResponse r2 = makeResponse(new Object[][]{{42}}, 1);

        Object result = AggregationReducer.maxColumn(List.of(empty, r2), 0);
        assertEquals(42, result);
    }

    public void testMinColumnMixedNumericTypes() {
        // Integer vs Long — previously threw ClassCastException
        WorkerQueryResponse r1 = makeResponse(new Object[][]{{Integer.valueOf(10)}}, 1);
        WorkerQueryResponse r2 = makeResponse(new Object[][]{{Long.valueOf(5)}}, 1);
        Object result = AggregationReducer.minColumn(List.of(r1, r2), 0);
        assertEquals(Long.valueOf(5), result);
    }

    public void testMaxColumnMixedNumericTypes() {
        WorkerQueryResponse r1 = makeResponse(new Object[][]{{Integer.valueOf(10)}}, 1);
        WorkerQueryResponse r2 = makeResponse(new Object[][]{{Long.valueOf(5)}}, 1);
        Object result = AggregationReducer.maxColumn(List.of(r1, r2), 0);
        assertEquals(Integer.valueOf(10), result);
    }

    // --- Helper ---

    private WorkerQueryResponse makeResponse(Object[][] columnData, int rowCount) {
        return new WorkerQueryResponse(List.of("col"), List.of("type"), rowCount, columnData);
    }
}

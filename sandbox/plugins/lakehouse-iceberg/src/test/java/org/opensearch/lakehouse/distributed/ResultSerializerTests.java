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
import java.util.List;

public class ResultSerializerTests extends OpenSearchTestCase {

    public void testToRowsConvertsColumnMajorToRowMajor() {
        List<String> names = List.of("id", "name");
        List<String> types = List.of("Integer", "String");
        Object[][] columnData = new Object[][]{
            {1, 2, 3},
            {"alice", "bob", "charlie"}
        };
        WorkerQueryResponse response = new WorkerQueryResponse(names, types, 3, columnData);

        List<Object[]> rows = ResultSerializer.toRows(response);

        assertEquals(3, rows.size());
        assertEquals(1, rows.get(0)[0]);
        assertEquals("alice", rows.get(0)[1]);
        assertEquals(2, rows.get(1)[0]);
        assertEquals("bob", rows.get(1)[1]);
        assertEquals(3, rows.get(2)[0]);
        assertEquals("charlie", rows.get(2)[1]);
    }

    public void testToRowsWithEmptyResponse() {
        WorkerQueryResponse response = new WorkerQueryResponse(List.of(), List.of(), 0, new Object[0][]);

        List<Object[]> rows = ResultSerializer.toRows(response);

        assertEquals(0, rows.size());
    }

    public void testToRowsWithSingleRow() {
        Object[][] columnData = new Object[][]{{42}, {"hello"}};
        WorkerQueryResponse response = new WorkerQueryResponse(
            List.of("a", "b"), List.of("Integer", "String"), 1, columnData
        );

        List<Object[]> rows = ResultSerializer.toRows(response);

        assertEquals(1, rows.size());
        assertEquals(42, rows.get(0)[0]);
        assertEquals("hello", rows.get(0)[1]);
    }

    public void testToRowsWithNullValues() {
        Object[][] columnData = new Object[][]{{null, "val"}, {"x", null}};
        WorkerQueryResponse response = new WorkerQueryResponse(
            List.of("c1", "c2"), List.of("String", "String"), 2, columnData
        );

        List<Object[]> rows = ResultSerializer.toRows(response);

        assertEquals(2, rows.size());
        assertNull(rows.get(0)[0]);
        assertEquals("x", rows.get(0)[1]);
        assertEquals("val", rows.get(1)[0]);
        assertNull(rows.get(1)[1]);
    }

    public void testToColumnResponseConvertsRowMajorToColumnMajor() {
        List<Object[]> rows = List.of(
            new Object[]{1, "alice"},
            new Object[]{2, "bob"},
            new Object[]{3, "charlie"}
        );
        List<String> names = List.of("id", "name");
        List<String> types = List.of("Integer", "String");

        WorkerQueryResponse response = ResultSerializer.toColumnResponse(rows, names, types);

        assertEquals(3, response.getRowCount());
        assertEquals(names, response.getColumnNames());
        assertEquals(types, response.getColumnTypes());
        assertEquals(2, response.getColumnData().length);
        assertEquals(1, response.getColumnData()[0][0]);
        assertEquals(2, response.getColumnData()[0][1]);
        assertEquals(3, response.getColumnData()[0][2]);
        assertEquals("alice", response.getColumnData()[1][0]);
        assertEquals("bob", response.getColumnData()[1][1]);
        assertEquals("charlie", response.getColumnData()[1][2]);
    }

    public void testToColumnResponseWithEmptyRows() {
        List<Object[]> rows = List.of();
        List<String> names = List.of("a", "b");
        List<String> types = List.of("Int", "Str");

        WorkerQueryResponse response = ResultSerializer.toColumnResponse(rows, names, types);

        assertEquals(0, response.getRowCount());
        assertEquals(names, response.getColumnNames());
        assertEquals(types, response.getColumnTypes());
        assertEquals(0, response.getColumnData().length);
    }

    public void testToColumnResponseWithSingleColumn() {
        List<Object[]> rows = List.of(
            new Object[]{10},
            new Object[]{20}
        );

        WorkerQueryResponse response = ResultSerializer.toColumnResponse(
            rows, List.of("val"), List.of("Integer")
        );

        assertEquals(2, response.getRowCount());
        assertEquals(1, response.getColumnData().length);
        assertEquals(10, response.getColumnData()[0][0]);
        assertEquals(20, response.getColumnData()[0][1]);
    }

    public void testRoundtripConversion() {
        // Row -> Column -> Row roundtrip
        List<Object[]> originalRows = new ArrayList<>();
        originalRows.add(new Object[]{1, "a", 1.0});
        originalRows.add(new Object[]{2, "b", 2.0});

        List<String> names = List.of("x", "y", "z");
        List<String> types = List.of("Integer", "String", "Double");

        WorkerQueryResponse columnResponse = ResultSerializer.toColumnResponse(originalRows, names, types);
        List<Object[]> reconstructedRows = ResultSerializer.toRows(columnResponse);

        assertEquals(originalRows.size(), reconstructedRows.size());
        for (int i = 0; i < originalRows.size(); i++) {
            Object[] orig = originalRows.get(i);
            Object[] recon = reconstructedRows.get(i);
            assertEquals(orig.length, recon.length);
            for (int j = 0; j < orig.length; j++) {
                assertEquals(orig[j], recon[j]);
            }
        }
    }

    public void testToColumnResponseWithShortRow() {
        // Row with fewer columns than expected (edge case)
        List<Object[]> rows = List.of(
            new Object[]{1, 2, 3},
            new Object[]{4}  // only 1 element instead of 3
        );
        List<String> names = List.of("a", "b", "c");
        List<String> types = List.of("Integer", "Integer", "Integer");

        WorkerQueryResponse response = ResultSerializer.toColumnResponse(rows, names, types);

        assertEquals(2, response.getRowCount());
        assertEquals(3, response.getColumnData().length);
        assertEquals(1, response.getColumnData()[0][0]);
        assertEquals(4, response.getColumnData()[0][1]);
        assertEquals(2, response.getColumnData()[1][0]);
        assertNull(response.getColumnData()[1][1]); // padded with null
        assertEquals(3, response.getColumnData()[2][0]);
        assertNull(response.getColumnData()[2][1]); // padded with null
    }
}

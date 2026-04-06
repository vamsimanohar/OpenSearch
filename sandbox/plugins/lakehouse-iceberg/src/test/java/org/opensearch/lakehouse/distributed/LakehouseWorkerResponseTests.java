/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class LakehouseWorkerResponseTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws IOException {
        String[] columnNames = new String[] { "id", "name", "amount" };
        Object[][] rows = new Object[][] {
            { 1, "Alice", 100.5 },
            { 2, "Bob", 200.75 },
            { 3, "Charlie", null }
        };

        LakehouseWorkerResponse original = new LakehouseWorkerResponse(rows, columnNames);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        LakehouseWorkerResponse deserialized = new LakehouseWorkerResponse(in);

        assertArrayEquals(columnNames, deserialized.getColumnNames());
        assertEquals(rows.length, deserialized.getRows().length);
        for (int i = 0; i < rows.length; i++) {
            assertArrayEquals(rows[i], deserialized.getRows()[i]);
        }
    }

    public void testEmptyRows() throws IOException {
        String[] columnNames = new String[] { "id", "name" };
        Object[][] rows = new Object[0][];

        LakehouseWorkerResponse original = new LakehouseWorkerResponse(rows, columnNames);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        LakehouseWorkerResponse deserialized = new LakehouseWorkerResponse(in);

        assertArrayEquals(columnNames, deserialized.getColumnNames());
        assertEquals(0, deserialized.getRows().length);
    }

    public void testSingleRowSingleColumn() throws IOException {
        String[] columnNames = new String[] { "count" };
        Object[][] rows = new Object[][] { { 42L } };

        LakehouseWorkerResponse original = new LakehouseWorkerResponse(rows, columnNames);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        LakehouseWorkerResponse deserialized = new LakehouseWorkerResponse(in);

        assertArrayEquals(columnNames, deserialized.getColumnNames());
        assertEquals(1, deserialized.getRows().length);
        assertArrayEquals(rows[0], deserialized.getRows()[0]);
    }

    public void testNullCellValues() throws IOException {
        String[] columnNames = new String[] { "a", "b" };
        Object[][] rows = new Object[][] {
            { null, null },
            { "value", null }
        };

        LakehouseWorkerResponse original = new LakehouseWorkerResponse(rows, columnNames);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        LakehouseWorkerResponse deserialized = new LakehouseWorkerResponse(in);

        assertArrayEquals(columnNames, deserialized.getColumnNames());
        assertEquals(rows.length, deserialized.getRows().length);
        for (int i = 0; i < rows.length; i++) {
            assertArrayEquals(rows[i], deserialized.getRows()[i]);
        }
    }
}

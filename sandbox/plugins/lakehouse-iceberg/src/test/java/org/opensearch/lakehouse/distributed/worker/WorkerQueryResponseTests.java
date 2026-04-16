/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed.worker;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

public class WorkerQueryResponseTests extends OpenSearchTestCase {

    public void testGetters() {
        List<String> names = List.of("id", "name", "score");
        List<String> types = List.of("Long", "String", "Double");
        Object[][] data = new Object[][]{
            {1L, 2L, 3L},
            {"alice", "bob", "charlie"},
            {9.5, 8.0, 7.5}
        };
        WorkerQueryResponse response = new WorkerQueryResponse(names, types, 3, data);

        assertEquals(names, response.getColumnNames());
        assertEquals(types, response.getColumnTypes());
        assertEquals(3, response.getRowCount());
        assertEquals(3, response.getColumnData().length);
        assertEquals(1L, response.getColumnData()[0][0]);
        assertEquals("bob", response.getColumnData()[1][1]);
        assertEquals(7.5, response.getColumnData()[2][2]);
    }

    public void testSerializationRoundtrip() throws IOException {
        List<String> names = List.of("col1", "col2");
        List<String> types = List.of("Integer", "String");
        Object[][] data = new Object[][]{
            {1, 2, 3},
            {"a", "b", "c"}
        };
        WorkerQueryResponse original = new WorkerQueryResponse(names, types, 3, data);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        WorkerQueryResponse deserialized = new WorkerQueryResponse(in);

        assertEquals(original.getColumnNames(), deserialized.getColumnNames());
        assertEquals(original.getColumnTypes(), deserialized.getColumnTypes());
        assertEquals(original.getRowCount(), deserialized.getRowCount());
        assertEquals(original.getColumnData().length, deserialized.getColumnData().length);
        for (int col = 0; col < original.getColumnData().length; col++) {
            for (int row = 0; row < original.getRowCount(); row++) {
                assertEquals(original.getColumnData()[col][row], deserialized.getColumnData()[col][row]);
            }
        }
    }

    public void testSerializationWithEmptyData() throws IOException {
        WorkerQueryResponse original = new WorkerQueryResponse(List.of(), List.of(), 0, new Object[0][]);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        WorkerQueryResponse deserialized = new WorkerQueryResponse(in);

        assertEquals(0, deserialized.getRowCount());
        assertEquals(0, deserialized.getColumnData().length);
        assertTrue(deserialized.getColumnNames().isEmpty());
        assertTrue(deserialized.getColumnTypes().isEmpty());
    }

    public void testSerializationWithNullValues() throws IOException {
        List<String> names = List.of("col1");
        List<String> types = List.of("String");
        Object[][] data = new Object[][]{{null, "value", null}};
        WorkerQueryResponse original = new WorkerQueryResponse(names, types, 3, data);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        WorkerQueryResponse deserialized = new WorkerQueryResponse(in);

        assertEquals(3, deserialized.getRowCount());
        assertNull(deserialized.getColumnData()[0][0]);
        assertEquals("value", deserialized.getColumnData()[0][1]);
        assertNull(deserialized.getColumnData()[0][2]);
    }

    public void testSerializationWithMixedTypes() throws IOException {
        List<String> names = List.of("int_col", "str_col", "long_col", "double_col");
        List<String> types = List.of("Integer", "String", "Long", "Double");
        Object[][] data = new Object[][]{
            {42},
            {"hello"},
            {100L},
            {3.14}
        };
        WorkerQueryResponse original = new WorkerQueryResponse(names, types, 1, data);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        WorkerQueryResponse deserialized = new WorkerQueryResponse(in);

        assertEquals(42, deserialized.getColumnData()[0][0]);
        assertEquals("hello", deserialized.getColumnData()[1][0]);
        assertEquals(100L, deserialized.getColumnData()[2][0]);
        assertEquals(3.14, deserialized.getColumnData()[3][0]);
    }

    public void testToXContent() throws IOException {
        List<String> names = List.of("id", "name");
        List<String> types = List.of("Integer", "String");
        Object[][] data = new Object[][]{{1}, {"test"}};
        WorkerQueryResponse response = new WorkerQueryResponse(names, types, 1, data);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, null);
        builder.close();

        String json = builder.toString();
        assertTrue(json.contains("\"rowCount\":1"));
        assertTrue(json.contains("\"name\":\"id\""));
        assertTrue(json.contains("\"type\":\"Integer\""));
        assertTrue(json.contains("\"name\":\"name\""));
        assertTrue(json.contains("\"type\":\"String\""));
    }

    public void testToXContentEmptyResponse() throws IOException {
        WorkerQueryResponse response = new WorkerQueryResponse(List.of(), List.of(), 0, new Object[0][]);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, null);
        builder.close();

        String json = builder.toString();
        assertTrue(json.contains("\"rowCount\":0"));
        assertTrue(json.contains("\"columns\":[]"));
    }

    public void testArrowIpcConstructor() {
        byte[] ipcBytes = new byte[] { 0x41, 0x52, 0x52, 0x4F, 0x57, 0x31 };
        WorkerQueryResponse response = new WorkerQueryResponse(ipcBytes);

        assertNotNull(response.getArrowIpcData());
        assertArrayEquals(ipcBytes, response.getArrowIpcData());
        assertTrue(response.isArrowIpc());
    }

    public void testArrowIpcSerializationRoundtrip() throws IOException {
        byte[] ipcBytes = new byte[256];
        for (int i = 0; i < ipcBytes.length; i++) {
            ipcBytes[i] = (byte) (i & 0xFF);
        }
        WorkerQueryResponse original = new WorkerQueryResponse(ipcBytes);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        WorkerQueryResponse deserialized = new WorkerQueryResponse(in);

        assertTrue(deserialized.isArrowIpc());
        assertArrayEquals(original.getArrowIpcData(), deserialized.getArrowIpcData());
        assertEquals(List.of(), deserialized.getColumnNames());
        assertEquals(List.of(), deserialized.getColumnTypes());
        assertEquals(-1, deserialized.getRowCount());
        assertNull(deserialized.getColumnData());
    }

    public void testLegacySerializationRoundtrip() throws IOException {
        List<String> names = List.of("a", "b");
        List<String> types = List.of("Integer", "String");
        Object[][] data = new Object[][] { { 10, 20 }, { "x", "y" } };
        WorkerQueryResponse original = new WorkerQueryResponse(names, types, 2, data);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        WorkerQueryResponse deserialized = new WorkerQueryResponse(in);

        assertFalse(deserialized.isArrowIpc());
        assertNull(deserialized.getArrowIpcData());
        assertEquals(names, deserialized.getColumnNames());
        assertEquals(types, deserialized.getColumnTypes());
        assertEquals(2, deserialized.getRowCount());
        assertEquals(10, deserialized.getColumnData()[0][0]);
        assertEquals("y", deserialized.getColumnData()[1][1]);
    }

    public void testArrowIpcGetters() {
        byte[] ipcBytes = new byte[] { 1, 2, 3, 4, 5 };
        WorkerQueryResponse response = new WorkerQueryResponse(ipcBytes);

        assertArrayEquals(ipcBytes, response.getArrowIpcData());
        assertEquals(List.of(), response.getColumnNames());
        assertEquals(List.of(), response.getColumnTypes());
        assertEquals(-1, response.getRowCount());
        assertNull(response.getColumnData());
    }

    public void testIsArrowIpc() {
        WorkerQueryResponse arrowResponse = new WorkerQueryResponse(new byte[] { 0x01 });
        assertTrue(arrowResponse.isArrowIpc());

        WorkerQueryResponse legacyResponse = new WorkerQueryResponse(List.of("col"), List.of("String"), 0, new Object[1][]);
        assertFalse(legacyResponse.isArrowIpc());
    }

    public void testArrowIpcToXContent() throws IOException {
        byte[] ipcBytes = new byte[1024];
        WorkerQueryResponse response = new WorkerQueryResponse(ipcBytes);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, null);
        builder.close();

        String json = builder.toString();
        assertTrue(json.contains("\"format\":\"arrow_ipc\""));
        assertTrue(json.contains("\"arrowIpcBytes\":1024"));
        assertFalse(json.contains("\"rowCount\""));
        assertFalse(json.contains("\"columns\""));
    }

    public void testMixedLegacyNotArrowIpc() {
        List<String> names = List.of("id", "value");
        List<String> types = List.of("Long", "Double");
        Object[][] data = new Object[][] { { 1L, 2L }, { 3.14, 2.71 } };
        WorkerQueryResponse response = new WorkerQueryResponse(names, types, 2, data);

        assertFalse(response.isArrowIpc());
        assertNull(response.getArrowIpcData());
        assertEquals(names, response.getColumnNames());
        assertEquals(types, response.getColumnTypes());
        assertEquals(2, response.getRowCount());
        assertNotNull(response.getColumnData());
    }
}

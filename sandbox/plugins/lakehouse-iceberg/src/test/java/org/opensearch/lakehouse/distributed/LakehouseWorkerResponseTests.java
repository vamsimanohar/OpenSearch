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

/**
 * Tests for {@link LakehouseWorkerResponse} serialization round-trip.
 */
public class LakehouseWorkerResponseTests extends OpenSearchTestCase {

    public void testSerializationRoundTrip() throws IOException {
        String[] columnNames = new String[] { "city", "count", "total_price" };
        Object[][] rows = new Object[][] {
            { "New York", 100L, 5000.5 },
            { "San Francisco", 50L, 2500.0 },
            { "Chicago", 75L, 3750.25 }
        };

        LakehouseWorkerResponse original = new LakehouseWorkerResponse(rows, columnNames);

        // Serialize
        BytesStreamOutput output = new BytesStreamOutput();
        original.writeTo(output);

        // Deserialize
        StreamInput input = output.bytes().streamInput();
        LakehouseWorkerResponse deserialized = new LakehouseWorkerResponse(input);

        // Verify column names
        assertArrayEquals("Column names should match", columnNames, deserialized.getColumnNames());

        // Verify row count
        assertEquals("Row count should match", rows.length, deserialized.getRows().length);

        // Verify row contents
        for (int i = 0; i < rows.length; i++) {
            Object[] originalRow = rows[i];
            Object[] deserializedRow = deserialized.getRows()[i];
            assertEquals("Row " + i + " length should match", originalRow.length, deserializedRow.length);
            for (int j = 0; j < originalRow.length; j++) {
                assertEquals("Row " + i + " col " + j + " should match", originalRow[j], deserializedRow[j]);
            }
        }
    }

    public void testSerializationEmptyResponse() throws IOException {
        LakehouseWorkerResponse original = new LakehouseWorkerResponse(new Object[0][], new String[0]);

        BytesStreamOutput output = new BytesStreamOutput();
        original.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        LakehouseWorkerResponse deserialized = new LakehouseWorkerResponse(input);

        assertEquals("Should have 0 column names", 0, deserialized.getColumnNames().length);
        assertEquals("Should have 0 rows", 0, deserialized.getRows().length);
    }

    public void testSerializationWithNullValues() throws IOException {
        String[] columnNames = new String[] { "name", "value" };
        Object[][] rows = new Object[][] {
            { "test", null },
            { null, 42L }
        };

        LakehouseWorkerResponse original = new LakehouseWorkerResponse(rows, columnNames);

        BytesStreamOutput output = new BytesStreamOutput();
        original.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        LakehouseWorkerResponse deserialized = new LakehouseWorkerResponse(input);

        assertNull("Null value should be preserved in row 0", deserialized.getRows()[0][1]);
        assertNull("Null value should be preserved in row 1", deserialized.getRows()[1][0]);
        // readGenericValue may deserialize int as long, so compare as Number
        assertEquals("Non-null value should be preserved", 42L, ((Number) deserialized.getRows()[1][1]).longValue());
    }

    public void testSerializationWithMixedTypes() throws IOException {
        String[] columnNames = new String[] { "str", "int_val", "long_val", "double_val", "bool_val" };
        Object[][] rows = new Object[][] {
            { "hello", 42, 1000000000L, 3.14, true }
        };

        LakehouseWorkerResponse original = new LakehouseWorkerResponse(rows, columnNames);

        BytesStreamOutput output = new BytesStreamOutput();
        original.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        LakehouseWorkerResponse deserialized = new LakehouseWorkerResponse(input);

        Object[] row = deserialized.getRows()[0];
        assertEquals("String value should match", "hello", row[0]);
        assertEquals("Int value should match", 42, row[1]);
        assertEquals("Long value should match", 1000000000L, row[2]);
        assertEquals("Double value should match", 3.14, (double) row[3], 0.001);
        assertEquals("Boolean value should match", true, row[4]);
    }
}

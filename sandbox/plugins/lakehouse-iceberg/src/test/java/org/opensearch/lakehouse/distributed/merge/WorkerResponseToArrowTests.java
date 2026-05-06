/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed.merge;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.opensearch.lakehouse.distributed.worker.WorkerQueryResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class WorkerResponseToArrowTests extends OpenSearchTestCase {

    private BufferAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    // --- Type mapping tests ---

    public void testBigIntColumn() {
        WorkerQueryResponse response = makeResponse(
            List.of("id"), List.of("BIGINT"), new Object[][]{{100L, 200L, 300L}}, 3
        );

        try (VectorSchemaRoot root = WorkerResponseToArrow.convert(response, allocator)) {
            assertEquals(3, root.getRowCount());
            assertEquals(1, root.getFieldVectors().size());
            BigIntVector vec = (BigIntVector) root.getVector("id");
            assertEquals(100L, vec.get(0));
            assertEquals(200L, vec.get(1));
            assertEquals(300L, vec.get(2));
        }
    }

    public void testLongColumn() {
        WorkerQueryResponse response = makeResponse(
            List.of("cnt"), List.of("LONG"), new Object[][]{{42L, 99L}}, 2
        );

        try (VectorSchemaRoot root = WorkerResponseToArrow.convert(response, allocator)) {
            assertEquals(2, root.getRowCount());
            BigIntVector vec = (BigIntVector) root.getVector("cnt");
            assertEquals(42L, vec.get(0));
            assertEquals(99L, vec.get(1));
        }
    }

    public void testIntegerColumn() {
        WorkerQueryResponse response = makeResponse(
            List.of("val"), List.of("INTEGER"), new Object[][]{{10, 20}}, 2
        );

        try (VectorSchemaRoot root = WorkerResponseToArrow.convert(response, allocator)) {
            assertEquals(2, root.getRowCount());
            IntVector vec = (IntVector) root.getVector("val");
            assertEquals(10, vec.get(0));
            assertEquals(20, vec.get(1));
        }
    }

    public void testIntColumn() {
        WorkerQueryResponse response = makeResponse(
            List.of("x"), List.of("INT"), new Object[][]{{5}}, 1
        );

        try (VectorSchemaRoot root = WorkerResponseToArrow.convert(response, allocator)) {
            assertEquals(1, root.getRowCount());
            IntVector vec = (IntVector) root.getVector("x");
            assertEquals(5, vec.get(0));
        }
    }

    public void testDoubleColumn() {
        WorkerQueryResponse response = makeResponse(
            List.of("price"), List.of("DOUBLE"), new Object[][]{{1.5, 2.7}}, 2
        );

        try (VectorSchemaRoot root = WorkerResponseToArrow.convert(response, allocator)) {
            assertEquals(2, root.getRowCount());
            Float8Vector vec = (Float8Vector) root.getVector("price");
            assertEquals(1.5, vec.get(0), 0.001);
            assertEquals(2.7, vec.get(1), 0.001);
        }
    }

    public void testFloatColumn() {
        // FLOAT maps to Float4Vector (single precision)
        WorkerQueryResponse response = makeResponse(
            List.of("f"), List.of("FLOAT"), new Object[][]{{3.14f}}, 1
        );

        try (VectorSchemaRoot root = WorkerResponseToArrow.convert(response, allocator)) {
            assertEquals(1, root.getRowCount());
            Float4Vector vec = (Float4Vector) root.getVector("f");
            assertEquals(3.14f, vec.get(0), 0.01f);
        }
    }

    public void testVarCharColumn() {
        WorkerQueryResponse response = makeResponse(
            List.of("name"), List.of("VARCHAR"), new Object[][]{{"hello", "world"}}, 2
        );

        try (VectorSchemaRoot root = WorkerResponseToArrow.convert(response, allocator)) {
            assertEquals(2, root.getRowCount());
            VarCharVector vec = (VarCharVector) root.getVector("name");
            assertEquals("hello", new String(vec.get(0)));
            assertEquals("world", new String(vec.get(1)));
        }
    }

    public void testStringColumn() {
        WorkerQueryResponse response = makeResponse(
            List.of("s"), List.of("STRING"), new Object[][]{{"abc"}}, 1
        );

        try (VectorSchemaRoot root = WorkerResponseToArrow.convert(response, allocator)) {
            assertEquals(1, root.getRowCount());
            VarCharVector vec = (VarCharVector) root.getVector("s");
            assertEquals("abc", new String(vec.get(0)));
        }
    }

    public void testUtf8Column() {
        WorkerQueryResponse response = makeResponse(
            List.of("text"), List.of("Utf8"), new Object[][]{{"utf8-value"}}, 1
        );

        try (VectorSchemaRoot root = WorkerResponseToArrow.convert(response, allocator)) {
            assertEquals(1, root.getRowCount());
            VarCharVector vec = (VarCharVector) root.getVector("text");
            assertEquals("utf8-value", new String(vec.get(0)));
        }
    }

    public void testBooleanColumn() {
        WorkerQueryResponse response = makeResponse(
            List.of("flag"), List.of("BOOLEAN"), new Object[][]{{true, false, true}}, 3
        );

        try (VectorSchemaRoot root = WorkerResponseToArrow.convert(response, allocator)) {
            assertEquals(3, root.getRowCount());
            BitVector vec = (BitVector) root.getVector("flag");
            assertEquals(1, vec.get(0));
            assertEquals(0, vec.get(1));
            assertEquals(1, vec.get(2));
        }
    }

    public void testTimestampColumn() {
        long ts1 = 1625000000000L;
        long ts2 = 1625100000000L;
        WorkerQueryResponse response = makeResponse(
            List.of("ts"), List.of("TIMESTAMP"), new Object[][]{{ts1, ts2}}, 2
        );

        try (VectorSchemaRoot root = WorkerResponseToArrow.convert(response, allocator)) {
            assertEquals(2, root.getRowCount());
            TimeStampMilliVector vec = (TimeStampMilliVector) root.getVector("ts");
            assertEquals(ts1, vec.get(0));
            assertEquals(ts2, vec.get(1));
        }
    }

    public void testDateColumn() {
        // DateDayVector stores days since epoch as int
        WorkerQueryResponse response = makeResponse(
            List.of("d"), List.of("DATE"), new Object[][]{{18800, 18801}}, 2
        );

        try (VectorSchemaRoot root = WorkerResponseToArrow.convert(response, allocator)) {
            assertEquals(2, root.getRowCount());
            DateDayVector vec = (DateDayVector) root.getVector("d");
            assertEquals(18800, vec.get(0));
            assertEquals(18801, vec.get(1));
        }
    }

    public void testUnknownTypeFallsBackToVarChar() {
        WorkerQueryResponse response = makeResponse(
            List.of("x"), List.of("UNKNOWN_TYPE"), new Object[][]{{42}}, 1
        );

        try (VectorSchemaRoot root = WorkerResponseToArrow.convert(response, allocator)) {
            assertEquals(1, root.getRowCount());
            VarCharVector vec = (VarCharVector) root.getVector("x");
            assertEquals("42", new String(vec.get(0)));
        }
    }

    // --- Null value tests ---

    public void testNullValues() {
        WorkerQueryResponse response = makeResponse(
            List.of("a", "b"), List.of("BIGINT", "VARCHAR"),
            new Object[][]{{100L, null, 300L}, {"hello", null, "world"}}, 3
        );

        try (VectorSchemaRoot root = WorkerResponseToArrow.convert(response, allocator)) {
            assertEquals(3, root.getRowCount());

            BigIntVector intVec = (BigIntVector) root.getVector("a");
            assertEquals(100L, intVec.get(0));
            assertTrue(intVec.isNull(1));
            assertEquals(300L, intVec.get(2));

            VarCharVector strVec = (VarCharVector) root.getVector("b");
            assertEquals("hello", new String(strVec.get(0)));
            assertTrue(strVec.isNull(1));
            assertEquals("world", new String(strVec.get(2)));
        }
    }

    public void testAllNullValues() {
        WorkerQueryResponse response = makeResponse(
            List.of("val"), List.of("INTEGER"), new Object[][]{{null, null}}, 2
        );

        try (VectorSchemaRoot root = WorkerResponseToArrow.convert(response, allocator)) {
            assertEquals(2, root.getRowCount());
            IntVector vec = (IntVector) root.getVector("val");
            assertTrue(vec.isNull(0));
            assertTrue(vec.isNull(1));
        }
    }

    // --- Empty response test ---

    public void testEmptyResponse() {
        WorkerQueryResponse response = makeResponse(
            List.of("col"), List.of("BIGINT"), new Object[0][], 0
        );

        try (VectorSchemaRoot root = WorkerResponseToArrow.convert(response, allocator)) {
            assertEquals(0, root.getRowCount());
            assertEquals(1, root.getFieldVectors().size());
            assertEquals("col", root.getSchema().getFields().get(0).getName());
        }
    }

    // --- Multiple columns test ---

    public void testMultipleColumns() {
        WorkerQueryResponse response = makeResponse(
            List.of("id", "name", "active", "score"),
            List.of("BIGINT", "VARCHAR", "BOOLEAN", "DOUBLE"),
            new Object[][]{{1L, 2L}, {"alice", "bob"}, {true, false}, {9.5, 8.3}},
            2
        );

        try (VectorSchemaRoot root = WorkerResponseToArrow.convert(response, allocator)) {
            assertEquals(2, root.getRowCount());
            assertEquals(4, root.getFieldVectors().size());

            BigIntVector idVec = (BigIntVector) root.getVector("id");
            assertEquals(1L, idVec.get(0));
            assertEquals(2L, idVec.get(1));

            VarCharVector nameVec = (VarCharVector) root.getVector("name");
            assertEquals("alice", new String(nameVec.get(0)));
            assertEquals("bob", new String(nameVec.get(1)));

            BitVector activeVec = (BitVector) root.getVector("active");
            assertEquals(1, activeVec.get(0));
            assertEquals(0, activeVec.get(1));

            Float8Vector scoreVec = (Float8Vector) root.getVector("score");
            assertEquals(9.5, scoreVec.get(0), 0.001);
            assertEquals(8.3, scoreVec.get(1), 0.001);
        }
    }

    // --- mapToArrowType coverage ---

    public void testMapToArrowTypeAllKnownTypes() {
        // Verify all known type strings map to correct Arrow types
        assertTrue(WorkerResponseToArrow.mapToArrowType("BIGINT") instanceof ArrowType.Int);
        assertTrue(WorkerResponseToArrow.mapToArrowType("LONG") instanceof ArrowType.Int);
        assertTrue(WorkerResponseToArrow.mapToArrowType("INTEGER") instanceof ArrowType.Int);
        assertTrue(WorkerResponseToArrow.mapToArrowType("INT") instanceof ArrowType.Int);
        assertTrue(WorkerResponseToArrow.mapToArrowType("DOUBLE") instanceof ArrowType.FloatingPoint);
        assertTrue(WorkerResponseToArrow.mapToArrowType("FLOAT") instanceof ArrowType.FloatingPoint);
        assertTrue(WorkerResponseToArrow.mapToArrowType("VARCHAR") instanceof ArrowType.Utf8);
        assertTrue(WorkerResponseToArrow.mapToArrowType("STRING") instanceof ArrowType.Utf8);
        assertTrue(WorkerResponseToArrow.mapToArrowType("UTF8") instanceof ArrowType.Utf8);
        assertTrue(WorkerResponseToArrow.mapToArrowType("BOOLEAN") instanceof ArrowType.Bool);
        assertTrue(WorkerResponseToArrow.mapToArrowType("BOOL") instanceof ArrowType.Bool);
        assertTrue(WorkerResponseToArrow.mapToArrowType("TIMESTAMP") instanceof ArrowType.Timestamp);
        assertTrue(WorkerResponseToArrow.mapToArrowType("DATE") instanceof ArrowType.Date);
    }

    public void testMapToArrowTypeNull() {
        // null type name falls back to Utf8
        assertTrue(WorkerResponseToArrow.mapToArrowType(null) instanceof ArrowType.Utf8);
    }

    public void testMapToArrowTypeCaseInsensitive() {
        // Should be case-insensitive
        assertTrue(WorkerResponseToArrow.mapToArrowType("bigint") instanceof ArrowType.Int);
        assertTrue(WorkerResponseToArrow.mapToArrowType("Varchar") instanceof ArrowType.Utf8);
        assertTrue(WorkerResponseToArrow.mapToArrowType("boolean") instanceof ArrowType.Bool);
    }

    // --- Validation tests ---

    public void testNullResponseThrows() {
        expectThrows(IllegalArgumentException.class, () -> WorkerResponseToArrow.convert(null, allocator));
    }

    public void testNullAllocatorThrows() {
        WorkerQueryResponse response = makeResponse(List.of("col"), List.of("BIGINT"), new Object[][]{{1L}}, 1);
        expectThrows(IllegalArgumentException.class, () -> WorkerResponseToArrow.convert(response, null));
    }

    // --- Boolean conversion edge cases ---

    public void testBooleanFromNumber() {
        WorkerQueryResponse response = makeResponse(
            List.of("flag"), List.of("BOOLEAN"), new Object[][]{{1, 0}}, 2
        );

        try (VectorSchemaRoot root = WorkerResponseToArrow.convert(response, allocator)) {
            BitVector vec = (BitVector) root.getVector("flag");
            assertEquals(1, vec.get(0));
            assertEquals(0, vec.get(1));
        }
    }

    public void testBooleanFromString() {
        WorkerQueryResponse response = makeResponse(
            List.of("flag"), List.of("BOOLEAN"), new Object[][]{{"true", "false"}}, 2
        );

        try (VectorSchemaRoot root = WorkerResponseToArrow.convert(response, allocator)) {
            BitVector vec = (BitVector) root.getVector("flag");
            assertEquals(1, vec.get(0));
            assertEquals(0, vec.get(1));
        }
    }

    // --- Integer/Long from string conversion ---

    public void testIntegerFromStringValue() {
        WorkerQueryResponse response = makeResponse(
            List.of("x"), List.of("INTEGER"), new Object[][]{{"42"}}, 1
        );

        try (VectorSchemaRoot root = WorkerResponseToArrow.convert(response, allocator)) {
            IntVector vec = (IntVector) root.getVector("x");
            assertEquals(42, vec.get(0));
        }
    }

    public void testBigIntFromStringValue() {
        WorkerQueryResponse response = makeResponse(
            List.of("x"), List.of("BIGINT"), new Object[][]{{"9999999999"}}, 1
        );

        try (VectorSchemaRoot root = WorkerResponseToArrow.convert(response, allocator)) {
            BigIntVector vec = (BigIntVector) root.getVector("x");
            assertEquals(9999999999L, vec.get(0));
        }
    }

    public void testDoubleFromStringValue() {
        WorkerQueryResponse response = makeResponse(
            List.of("x"), List.of("DOUBLE"), new Object[][]{{"3.14"}}, 1
        );

        try (VectorSchemaRoot root = WorkerResponseToArrow.convert(response, allocator)) {
            Float8Vector vec = (Float8Vector) root.getVector("x");
            assertEquals(3.14, vec.get(0), 0.001);
        }
    }

    // --- Helper ---

    private WorkerQueryResponse makeResponse(List<String> names, List<String> types, Object[][] columnData, int rowCount) {
        return new WorkerQueryResponse(names, types, rowCount, columnData);
    }
}

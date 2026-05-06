/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed.merge;

import org.apache.arrow.memory.BufferAllocator;
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
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.lakehouse.distributed.worker.WorkerQueryResponse;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Converts a {@link WorkerQueryResponse} (column-major {@code Object[][]}) to an
 * Arrow {@link VectorSchemaRoot} suitable for feeding into an
 * {@link org.opensearch.analytics.spi.ExchangeSink} or serializing to Arrow IPC.
 * <p>
 * Type mapping from {@link WorkerQueryResponse#getColumnTypes()} strings:
 * <ul>
 *   <li>{@code BIGINT / LONG} &rarr; {@link BigIntVector}</li>
 *   <li>{@code INTEGER / INT} &rarr; {@link IntVector}</li>
 *   <li>{@code DOUBLE / FLOAT} &rarr; {@link Float8Vector}</li>
 *   <li>{@code VARCHAR / STRING / Utf8} &rarr; {@link VarCharVector}</li>
 *   <li>{@code BOOLEAN} &rarr; {@link BitVector}</li>
 *   <li>{@code TIMESTAMP} &rarr; {@link TimeStampMilliVector}</li>
 *   <li>{@code DATE} &rarr; {@link DateDayVector}</li>
 *   <li>Default &rarr; {@link VarCharVector} ({@code toString()} fallback)</li>
 * </ul>
 * <p>
 * The caller owns the returned {@link VectorSchemaRoot} and must close it.
 *
 * @opensearch.internal
 */
public final class WorkerResponseToArrow {

    private WorkerResponseToArrow() {}

    /**
     * Converts a {@link WorkerQueryResponse} to an Arrow {@link VectorSchemaRoot}.
     *
     * @param response  the worker response with column-major data and type metadata
     * @param allocator the Arrow buffer allocator for creating vectors
     * @return a new {@link VectorSchemaRoot} containing the response data; caller must close
     * @throws IllegalArgumentException if response or allocator is null
     */
    public static VectorSchemaRoot convert(WorkerQueryResponse response, BufferAllocator allocator) {
        if (response == null) {
            throw new IllegalArgumentException("response must not be null");
        }
        if (allocator == null) {
            throw new IllegalArgumentException("allocator must not be null");
        }

        List<String> columnNames = response.getColumnNames();
        List<String> columnTypes = response.getColumnTypes();
        int rowCount = response.getRowCount();
        Object[][] columnData = response.getColumnData();

        // Build Arrow schema
        List<Field> fields = new ArrayList<>(columnNames.size());
        for (int i = 0; i < columnNames.size(); i++) {
            ArrowType arrowType = mapToArrowType(columnTypes.get(i));
            fields.add(new Field(columnNames.get(i), FieldType.nullable(arrowType), null));
        }
        Schema schema = new Schema(fields);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);

        try {
            root.allocateNew();
            // Populate each column vector
            for (int col = 0; col < columnNames.size(); col++) {
                Object[] colData = col < columnData.length ? columnData[col] : new Object[0];
                populateVector(root, col, colData, rowCount, columnTypes.get(col));
            }
            root.setRowCount(rowCount);
        } catch (Exception e) {
            root.close();
            throw e;
        }
        return root;
    }

    /**
     * Maps a column type string to an Arrow type.
     *
     * @param typeName the type name from {@link WorkerQueryResponse#getColumnTypes()}
     * @return the corresponding {@link ArrowType}
     */
    static ArrowType mapToArrowType(String typeName) {
        if (typeName == null) {
            return ArrowType.Utf8.INSTANCE;
        }
        return switch (typeName.toUpperCase(Locale.ROOT)) {
            case "BIGINT", "LONG", "INT64" -> new ArrowType.Int(64, true);
            case "INTEGER", "INT", "INT32" -> new ArrowType.Int(32, true);
            case "SMALLINT", "INT16" -> new ArrowType.Int(16, true);
            case "TINYINT", "INT8" -> new ArrowType.Int(8, true);
            case "DOUBLE", "FLOAT64", "FLOAT8" -> new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
            case "FLOAT", "FLOAT32", "FLOAT4" -> new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE);
            case "VARCHAR", "STRING", "UTF8" -> ArrowType.Utf8.INSTANCE;
            case "BOOLEAN", "BOOL" -> ArrowType.Bool.INSTANCE;
            case "TIMESTAMP" -> new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, null);
            case "DATE" -> new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY);
            default -> ArrowType.Utf8.INSTANCE;
        };
    }

    /**
     * Populates a single column vector in the root.
     */
    private static void populateVector(VectorSchemaRoot root, int colIndex, Object[] data, int rowCount, String typeName) {
        var vector = root.getVector(colIndex);
        String upperType = typeName != null ? typeName.toUpperCase(Locale.ROOT) : "";

        for (int row = 0; row < rowCount; row++) {
            Object value = row < data.length ? data[row] : null;
            if (value == null) {
                // All vector types support setNull via index; allocateNew already zeroes validity.
                // Just skip — null is the default after allocateNew.
                continue;
            }
            switch (vector) {
                case BigIntVector bigIntVec -> bigIntVec.setSafe(row, toLong(value));
                case IntVector intVec -> intVec.setSafe(row, toInt(value));
                case Float8Vector float8Vec -> float8Vec.setSafe(row, toDouble(value));
                case Float4Vector float4Vec -> float4Vec.setSafe(row, toFloat(value));
                case VarCharVector varCharVec -> varCharVec.setSafe(row, toBytes(value));
                case BitVector bitVec -> bitVec.setSafe(row, toBit(value));
                case TimeStampMilliVector tsVec -> tsVec.setSafe(row, toLong(value));
                case DateDayVector dateVec -> dateVec.setSafe(row, toInt(value));
                default -> {
                    // Fallback: treat as VarChar-like with toString
                    if (vector instanceof VarCharVector vc) {
                        vc.setSafe(row, toBytes(value));
                    }
                }
            }
        }
        vector.setValueCount(rowCount);
    }

    private static long toLong(Object value) {
        if (value instanceof Number n) {
            return n.longValue();
        }
        return Long.parseLong(value.toString());
    }

    private static int toInt(Object value) {
        if (value instanceof Number n) {
            return n.intValue();
        }
        return Integer.parseInt(value.toString());
    }

    private static double toDouble(Object value) {
        if (value instanceof Number n) {
            return n.doubleValue();
        }
        return Double.parseDouble(value.toString());
    }

    private static float toFloat(Object value) {
        if (value instanceof Number n) {
            return n.floatValue();
        }
        return Float.parseFloat(value.toString());
    }

    private static byte[] toBytes(Object value) {
        return value.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static int toBit(Object value) {
        if (value instanceof Boolean b) {
            return b ? 1 : 0;
        }
        if (value instanceof Number n) {
            return n.intValue() != 0 ? 1 : 0;
        }
        return Boolean.parseBoolean(value.toString()) ? 1 : 0;
    }
}

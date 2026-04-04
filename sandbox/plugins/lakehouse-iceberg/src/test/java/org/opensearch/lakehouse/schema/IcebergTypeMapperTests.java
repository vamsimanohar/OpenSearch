/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.schema;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.iceberg.types.Types;
import org.opensearch.test.OpenSearchTestCase;

public class IcebergTypeMapperTests extends OpenSearchTestCase {

    public void testBooleanType() {
        assertEquals(SqlTypeName.BOOLEAN, IcebergTypeMapper.toCalcite(Types.BooleanType.get()));
    }

    public void testIntegerType() {
        assertEquals(SqlTypeName.INTEGER, IcebergTypeMapper.toCalcite(Types.IntegerType.get()));
    }

    public void testLongType() {
        assertEquals(SqlTypeName.BIGINT, IcebergTypeMapper.toCalcite(Types.LongType.get()));
    }

    public void testFloatType() {
        assertEquals(SqlTypeName.FLOAT, IcebergTypeMapper.toCalcite(Types.FloatType.get()));
    }

    public void testDoubleType() {
        assertEquals(SqlTypeName.DOUBLE, IcebergTypeMapper.toCalcite(Types.DoubleType.get()));
    }

    public void testStringType() {
        assertEquals(SqlTypeName.VARCHAR, IcebergTypeMapper.toCalcite(Types.StringType.get()));
    }

    public void testBinaryType() {
        assertEquals(SqlTypeName.VARBINARY, IcebergTypeMapper.toCalcite(Types.BinaryType.get()));
    }

    public void testDateType() {
        assertEquals(SqlTypeName.DATE, IcebergTypeMapper.toCalcite(Types.DateType.get()));
    }

    public void testTimeType() {
        assertEquals(SqlTypeName.TIME, IcebergTypeMapper.toCalcite(Types.TimeType.get()));
    }

    public void testTimestampWithoutZone() {
        assertEquals(SqlTypeName.TIMESTAMP, IcebergTypeMapper.toCalcite(Types.TimestampType.withoutZone()));
    }

    public void testTimestampWithZone() {
        assertEquals(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, IcebergTypeMapper.toCalcite(Types.TimestampType.withZone()));
    }

    public void testDecimalType() {
        assertEquals(SqlTypeName.DECIMAL, IcebergTypeMapper.toCalcite(Types.DecimalType.of(38, 18)));
    }

    public void testUuidType() {
        assertEquals(SqlTypeName.VARCHAR, IcebergTypeMapper.toCalcite(Types.UUIDType.get()));
    }

    public void testFixedType() {
        assertEquals(SqlTypeName.VARBINARY, IcebergTypeMapper.toCalcite(Types.FixedType.ofLength(16)));
    }

    public void testListType() {
        assertEquals(SqlTypeName.ARRAY, IcebergTypeMapper.toCalcite(
            Types.ListType.ofRequired(1, Types.StringType.get())));
    }

    public void testMapType() {
        assertEquals(SqlTypeName.MAP, IcebergTypeMapper.toCalcite(
            Types.MapType.ofRequired(1, 2, Types.StringType.get(), Types.LongType.get())));
    }

    public void testStructType() {
        assertEquals(SqlTypeName.ROW, IcebergTypeMapper.toCalcite(
            Types.StructType.of(
                Types.NestedField.required(1, "name", Types.StringType.get()),
                Types.NestedField.required(2, "age", Types.IntegerType.get())
            )));
    }
}

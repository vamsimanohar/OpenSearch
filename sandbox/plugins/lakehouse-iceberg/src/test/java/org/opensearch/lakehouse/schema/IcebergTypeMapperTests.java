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
        assertEquals(SqlTypeName.BOOLEAN, IcebergTypeMapper.toSqlTypeName(Types.BooleanType.get()));
    }

    public void testIntegerType() {
        assertEquals(SqlTypeName.INTEGER, IcebergTypeMapper.toSqlTypeName(Types.IntegerType.get()));
    }

    public void testLongType() {
        assertEquals(SqlTypeName.BIGINT, IcebergTypeMapper.toSqlTypeName(Types.LongType.get()));
    }

    public void testFloatType() {
        assertEquals(SqlTypeName.FLOAT, IcebergTypeMapper.toSqlTypeName(Types.FloatType.get()));
    }

    public void testDoubleType() {
        assertEquals(SqlTypeName.DOUBLE, IcebergTypeMapper.toSqlTypeName(Types.DoubleType.get()));
    }

    public void testDecimalType() {
        assertEquals(SqlTypeName.DECIMAL, IcebergTypeMapper.toSqlTypeName(Types.DecimalType.of(10, 2)));
    }

    public void testDateType() {
        assertEquals(SqlTypeName.DATE, IcebergTypeMapper.toSqlTypeName(Types.DateType.get()));
    }

    public void testTimeType() {
        assertEquals(SqlTypeName.TIME, IcebergTypeMapper.toSqlTypeName(Types.TimeType.get()));
    }

    public void testTimestampType() {
        assertEquals(SqlTypeName.TIMESTAMP, IcebergTypeMapper.toSqlTypeName(Types.TimestampType.withoutZone()));
    }

    public void testTimestampTzType() {
        assertEquals(SqlTypeName.TIMESTAMP, IcebergTypeMapper.toSqlTypeName(Types.TimestampType.withZone()));
    }

    public void testStringType() {
        assertEquals(SqlTypeName.VARCHAR, IcebergTypeMapper.toSqlTypeName(Types.StringType.get()));
    }

    public void testUuidType() {
        assertEquals(SqlTypeName.VARCHAR, IcebergTypeMapper.toSqlTypeName(Types.UUIDType.get()));
    }

    public void testBinaryType() {
        assertEquals(SqlTypeName.VARBINARY, IcebergTypeMapper.toSqlTypeName(Types.BinaryType.get()));
    }

    public void testFixedType() {
        assertEquals(SqlTypeName.VARBINARY, IcebergTypeMapper.toSqlTypeName(Types.FixedType.ofLength(16)));
    }

    public void testUnsupportedTypeFallsBackToVarchar() {
        // LIST, MAP, STRUCT are not explicitly handled — they hit the default branch
        assertEquals(SqlTypeName.VARCHAR, IcebergTypeMapper.toSqlTypeName(Types.ListType.ofRequired(1, Types.StringType.get())));
        assertEquals(
            SqlTypeName.VARCHAR,
            IcebergTypeMapper.toSqlTypeName(Types.MapType.ofRequired(1, 2, Types.StringType.get(), Types.StringType.get()))
        );
        assertEquals(
            SqlTypeName.VARCHAR,
            IcebergTypeMapper.toSqlTypeName(Types.StructType.of(Types.NestedField.required(1, "field", Types.StringType.get())))
        );
    }
}

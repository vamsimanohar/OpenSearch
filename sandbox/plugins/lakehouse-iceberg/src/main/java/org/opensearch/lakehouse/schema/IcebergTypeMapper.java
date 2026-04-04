/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.schema;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Maps Apache Iceberg types to Apache Calcite SqlTypeName.
 */
public final class IcebergTypeMapper {

    private IcebergTypeMapper() {}

    /**
     * Converts an Iceberg {@link Type} to a Calcite {@link SqlTypeName}.
     */
    public static SqlTypeName toCalcite(Type icebergType) {
        switch (icebergType.typeId()) {
            case BOOLEAN:
                return SqlTypeName.BOOLEAN;
            case INTEGER:
                return SqlTypeName.INTEGER;
            case LONG:
                return SqlTypeName.BIGINT;
            case FLOAT:
                return SqlTypeName.FLOAT;
            case DOUBLE:
                return SqlTypeName.DOUBLE;
            case STRING:
                return SqlTypeName.VARCHAR;
            case BINARY:
            case FIXED:
                return SqlTypeName.VARBINARY;
            case DATE:
                return SqlTypeName.DATE;
            case TIME:
                return SqlTypeName.TIME;
            case TIMESTAMP:
                Types.TimestampType ts = (Types.TimestampType) icebergType;
                return ts.shouldAdjustToUTC()
                    ? SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
                    : SqlTypeName.TIMESTAMP;
            case DECIMAL:
                return SqlTypeName.DECIMAL;
            case UUID:
                return SqlTypeName.VARCHAR;
            case LIST:
                return SqlTypeName.ARRAY;
            case MAP:
                return SqlTypeName.MAP;
            case STRUCT:
                return SqlTypeName.ROW;
            default:
                return SqlTypeName.ANY;
        }
    }
}

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

/**
 * Maps Iceberg {@link Type} to Calcite {@link SqlTypeName}.
 */
public final class IcebergTypeMapper {

    private IcebergTypeMapper() {}

    /**
     * Converts an Iceberg type to the corresponding Calcite SQL type.
     *
     * @param icebergType the Iceberg type
     * @return the Calcite SQL type name
     */
    public static SqlTypeName toSqlTypeName(Type icebergType) {
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
            case DECIMAL:
                return SqlTypeName.DECIMAL;
            case DATE:
                return SqlTypeName.DATE;
            case TIME:
                return SqlTypeName.TIME;
            case TIMESTAMP:
                return SqlTypeName.TIMESTAMP;
            case STRING:
                return SqlTypeName.VARCHAR;
            case UUID:
                return SqlTypeName.VARCHAR;
            case FIXED:
            case BINARY:
                return SqlTypeName.VARBINARY;
            default:
                return SqlTypeName.VARCHAR;
        }
    }
}

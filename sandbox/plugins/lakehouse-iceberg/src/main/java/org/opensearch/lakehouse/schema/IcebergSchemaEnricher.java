/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.schema;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.iceberg.Table;

import java.util.Map;

/**
 * Static utility that registers Iceberg tables into a Calcite
 * {@link SchemaPlus} alongside existing OpenSearch indices.
 */
public final class IcebergSchemaEnricher {

    private IcebergSchemaEnricher() {}

    /**
     * Adds every entry in {@code icebergTables} to the given Calcite schema.
     *
     * @param schema        the Calcite schema to enrich
     * @param icebergTables map of table name to Iceberg table
     * @throws IllegalArgumentException if a table name collides with an
     *         existing entry (e.g. an OpenSearch index with the same name).
     */
    public static void enrich(SchemaPlus schema, Map<String, Table> icebergTables) {
        for (Map.Entry<String, Table> entry : icebergTables.entrySet()) {
            String tableName = entry.getKey();
            if (schema.getTable(tableName) != null) {
                throw new IllegalArgumentException(
                    "Table name collision: '" + tableName + "' already exists as an OpenSearch index"
                );
            }
            schema.add(tableName, new IcebergCalciteTable(entry.getValue()));
        }
    }
}

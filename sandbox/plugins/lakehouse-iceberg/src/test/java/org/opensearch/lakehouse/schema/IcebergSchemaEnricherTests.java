/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.schema;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IcebergSchemaEnricherTests extends OpenSearchTestCase {

    public void testAddIcebergTablesRegistersInSchemaPlus() {
        SchemaPlus schema = CalciteSchema.createRootSchema(false).plus();
        Table mockTable = mock(Table.class);
        when(mockTable.currentSnapshot()).thenReturn(null);
        when(mockTable.schema()).thenReturn(new Schema());

        IcebergSchemaEnricher.enrich(schema, Map.of("logs_cold", mockTable));

        assertNotNull(schema.getTable("logs_cold"));
        assertTrue(schema.getTable("logs_cold") instanceof IcebergCalciteTable);
    }

    public void testNameCollisionThrowsException() {
        SchemaPlus schema = CalciteSchema.createRootSchema(false).plus();
        Table mockTable = mock(Table.class);
        when(mockTable.currentSnapshot()).thenReturn(null);
        when(mockTable.schema()).thenReturn(new Schema());

        // Add first table
        IcebergSchemaEnricher.enrich(schema, Map.of("logs_cold", mockTable));

        // Try to add duplicate
        expectThrows(IllegalArgumentException.class, () -> IcebergSchemaEnricher.enrich(schema, Map.of("logs_cold", mockTable)));
    }
}

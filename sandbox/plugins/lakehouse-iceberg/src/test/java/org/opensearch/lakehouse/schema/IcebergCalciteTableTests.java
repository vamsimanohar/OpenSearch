/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.schema;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IcebergCalciteTableTests extends OpenSearchTestCase {

    public void testGetRowTypeReturnsCalciteTypesFromIcebergSchema() {
        Table mockTable = mock(Table.class);
        Snapshot mockSnapshot = mock(Snapshot.class);
        when(mockSnapshot.snapshotId()).thenReturn(12345L);
        when(mockTable.currentSnapshot()).thenReturn(mockSnapshot);

        Schema schema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "service", Types.StringType.get()),
            Types.NestedField.optional(3, "ts", Types.TimestampType.withoutZone())
        );
        when(mockTable.schema()).thenReturn(schema);

        IcebergCalciteTable table = new IcebergCalciteTable(mockTable);
        RelDataType rowType = table.getRowType(new JavaTypeFactoryImpl());

        assertEquals(3, rowType.getFieldCount());
        assertEquals("id", rowType.getFieldList().get(0).getName());
        assertEquals("service", rowType.getFieldList().get(1).getName());
        assertEquals("ts", rowType.getFieldList().get(2).getName());
    }

    public void testPinnedSnapshotIdMatchesCurrentSnapshot() {
        Table mockTable = mock(Table.class);
        Snapshot mockSnapshot = mock(Snapshot.class);
        when(mockSnapshot.snapshotId()).thenReturn(12345L);
        when(mockTable.currentSnapshot()).thenReturn(mockSnapshot);
        when(mockTable.schema()).thenReturn(new Schema());

        IcebergCalciteTable table = new IcebergCalciteTable(mockTable);
        assertEquals(12345L, table.getPinnedSnapshotId());
    }

    public void testNullSnapshotReturnsNegativeOne() {
        Table mockTable = mock(Table.class);
        when(mockTable.currentSnapshot()).thenReturn(null);
        when(mockTable.schema()).thenReturn(new Schema());

        IcebergCalciteTable table = new IcebergCalciteTable(mockTable);
        assertEquals(-1L, table.getPinnedSnapshotId());
    }
}

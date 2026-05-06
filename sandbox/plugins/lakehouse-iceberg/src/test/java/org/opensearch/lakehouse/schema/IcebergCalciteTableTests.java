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
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.opensearch.lakehouse.catalog.CatalogConfig;
import org.opensearch.lakehouse.catalog.CatalogType;
import org.opensearch.lakehouse.catalog.IcebergCatalogConnector;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IcebergCalciteTableTests extends OpenSearchTestCase {

    private CatalogConfig config;
    private IcebergCatalogConnector connector;
    private Table mockTable;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        config = new CatalogConfig("test_idx", CatalogType.GLUE, "us-west-2", "s3://bucket", "db", "events", "default", null, null);
        connector = mock(IcebergCatalogConnector.class);
        mockTable = mock(Table.class);

        Schema schema = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "amount", Types.DecimalType.of(10, 2)),
            Types.NestedField.optional(4, "active", Types.BooleanType.get())
        );
        Snapshot snapshot = mock(Snapshot.class);
        when(snapshot.snapshotId()).thenReturn(42L);
        when(mockTable.schema()).thenReturn(schema);
        when(mockTable.currentSnapshot()).thenReturn(snapshot);
        when(connector.loadTable(config)).thenReturn(mockTable);
    }

    public void testQualifiedName() {
        IcebergCalciteTable table = new IcebergCalciteTable(config, connector);
        assertEquals("db.events", table.qualifiedName());
    }

    public void testFormat() {
        IcebergCalciteTable table = new IcebergCalciteTable(config, connector);
        assertEquals("iceberg", table.format());
    }

    public void testSnapshotIdBeforeLoad() {
        IcebergCalciteTable table = new IcebergCalciteTable(config, connector);
        assertEquals(-1L, table.snapshotId());
    }

    public void testGetRowTypeTriggersLazyLoad() {
        IcebergCalciteTable table = new IcebergCalciteTable(config, connector);

        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RelDataType rowType = table.getRowType(typeFactory);

        verify(connector, times(1)).loadTable(config);
        assertEquals(4, rowType.getFieldCount());
        assertEquals("id", rowType.getFieldList().get(0).getName());
        assertEquals(SqlTypeName.BIGINT, rowType.getFieldList().get(0).getType().getSqlTypeName());
        assertFalse(rowType.getFieldList().get(0).getType().isNullable());
        assertEquals("name", rowType.getFieldList().get(1).getName());
        assertEquals(SqlTypeName.VARCHAR, rowType.getFieldList().get(1).getType().getSqlTypeName());
        assertTrue(rowType.getFieldList().get(1).getType().isNullable());
        assertEquals("amount", rowType.getFieldList().get(2).getName());
        assertEquals(SqlTypeName.DECIMAL, rowType.getFieldList().get(2).getType().getSqlTypeName());
        assertEquals(10, rowType.getFieldList().get(2).getType().getPrecision());
        assertEquals(2, rowType.getFieldList().get(2).getType().getScale());
        assertEquals("active", rowType.getFieldList().get(3).getName());
        assertEquals(SqlTypeName.BOOLEAN, rowType.getFieldList().get(3).getType().getSqlTypeName());
    }

    public void testGetRowTypeCalledTwiceLoadsOnce() {
        IcebergCalciteTable table = new IcebergCalciteTable(config, connector);

        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        table.getRowType(typeFactory);
        table.getRowType(typeFactory);

        verify(connector, times(1)).loadTable(config);
    }

    public void testSnapshotIdAfterLoad() {
        IcebergCalciteTable table = new IcebergCalciteTable(config, connector);

        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        table.getRowType(typeFactory);

        assertEquals(42L, table.snapshotId());
    }

    public void testSnapshotIdNullSnapshot() {
        when(mockTable.currentSnapshot()).thenReturn(null);

        IcebergCalciteTable table = new IcebergCalciteTable(config, connector);
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        table.getRowType(typeFactory);

        assertEquals(-1L, table.snapshotId());
    }

    public void testCatalogConfig() {
        IcebergCalciteTable table = new IcebergCalciteTable(config, connector);
        assertSame(config, table.catalogConfig());
    }

    public void testConcurrentGetRowTypeLoadsOnce() throws Exception {
        IcebergCalciteTable table = new IcebergCalciteTable(config, connector);
        int threadCount = 4;
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        AtomicInteger errors = new AtomicInteger(0);

        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(() -> {
                try {
                    barrier.await();
                    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
                    RelDataType rowType = table.getRowType(typeFactory);
                    assertEquals(4, rowType.getFieldCount());
                } catch (Exception e) {
                    errors.incrementAndGet();
                }
            });
            threads[i].start();
        }
        for (Thread t : threads) {
            t.join(5000);
        }
        assertEquals(0, errors.get());
        verify(connector, times(1)).loadTable(config);
    }
}

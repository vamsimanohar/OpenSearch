/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.schema;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.opensearch.analytics.schema.ExternalTable;

/**
 * A Calcite {@link AbstractTable} backed by an Apache Iceberg table.
 * The constructor pins the current snapshot so that every query operator
 * sees a consistent view of the table.
 */
public class IcebergCalciteTable extends AbstractTable implements ExternalTable {
    private final Table icebergTable;
    private final long pinnedSnapshotId;

    /**
     * Creates a Calcite table backed by the given Iceberg table, pinning the current snapshot.
     *
     * @param icebergTable the Iceberg table to wrap
     */
    public IcebergCalciteTable(Table icebergTable) {
        this.icebergTable = icebergTable;
        this.pinnedSnapshotId = icebergTable.currentSnapshot() != null ? icebergTable.currentSnapshot().snapshotId() : -1L;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        Schema schema = icebergTable.schema();
        for (Types.NestedField field : schema.columns()) {
            builder.add(field.name(), IcebergTypeMapper.toCalcite(field.type()));
        }
        return builder.build();
    }

    /** Returns the underlying Iceberg table. */
    public Table getIcebergTable() {
        return icebergTable;
    }

    /** Returns the pinned snapshot ID, or {@code -1} if no snapshot exists. */
    public long getPinnedSnapshotId() {
        return pinnedSnapshotId;
    }
}

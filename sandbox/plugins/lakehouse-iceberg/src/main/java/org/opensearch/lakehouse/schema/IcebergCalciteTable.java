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

/**
 * A Calcite {@link AbstractTable} backed by an Apache Iceberg table.
 * The constructor pins the current snapshot so that every query operator
 * sees a consistent view of the table.
 */
public class IcebergCalciteTable extends AbstractTable {
    private final Table icebergTable;
    private final long pinnedSnapshotId;

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

    public Table getIcebergTable() {
        return icebergTable;
    }

    public long getPinnedSnapshotId() {
        return pinnedSnapshotId;
    }
}

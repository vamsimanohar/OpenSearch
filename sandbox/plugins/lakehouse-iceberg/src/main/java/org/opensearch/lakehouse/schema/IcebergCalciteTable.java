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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.opensearch.analytics.schema.ExternalTable;
import org.opensearch.lakehouse.catalog.CatalogConfig;
import org.opensearch.lakehouse.catalog.IcebergCatalogConnector;

/**
 * Calcite table backed by an Iceberg table.
 * <p>
 * Registered into the Calcite schema lazily — the expensive Iceberg SDK call
 * ({@code loadTable()}) is deferred until Calcite actually calls
 * {@link #getRowType(RelDataTypeFactory)}, which only happens for tables
 * referenced by the query. This avoids loading metadata for every lakehouse
 * index on every query.
 */
public class IcebergCalciteTable extends AbstractTable implements ExternalTable {

    private final CatalogConfig config;
    private final IcebergCatalogConnector catalogConnector;

    private volatile Table loadedTable;
    private volatile Schema icebergSchema;
    private volatile long snapshotId = -1L;

    /**
     * Creates a lazy IcebergCalciteTable. No Iceberg SDK calls are made until
     * {@link #getRowType(RelDataTypeFactory)} is called.
     *
     * @param config           the catalog configuration for this table
     * @param catalogConnector the connector used to load the table on demand
     */
    public IcebergCalciteTable(CatalogConfig config, IcebergCatalogConnector catalogConnector) {
        this.config = config;
        this.catalogConnector = catalogConnector;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        ensureLoaded();
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (Types.NestedField field : icebergSchema.columns()) {
            SqlTypeName sqlType = IcebergTypeMapper.toSqlTypeName(field.type());
            RelDataType type;
            if (sqlType == SqlTypeName.DECIMAL) {
                Types.DecimalType decimal = (Types.DecimalType) field.type();
                type = typeFactory.createSqlType(sqlType, decimal.precision(), decimal.scale());
            } else {
                type = typeFactory.createSqlType(sqlType);
            }
            builder.add(field.name(), typeFactory.createTypeWithNullability(type, field.isOptional()));
        }
        return builder.build();
    }

    private void ensureLoaded() {
        if (icebergSchema != null) {
            return;
        }
        synchronized (this) {
            if (icebergSchema != null) {
                return;
            }
            Table table = catalogConnector.loadTable(config);
            this.loadedTable = table;
            this.icebergSchema = table.schema();
            Snapshot current = table.currentSnapshot();
            this.snapshotId = current != null ? current.snapshotId() : -1L;
        }
    }

    @Override
    public String qualifiedName() {
        return config.namespace() + "." + config.tableName();
    }

    @Override
    public String format() {
        return "iceberg";
    }

    /** Returns the pinned snapshot ID, or -1 if not yet loaded or table has no snapshots. */
    public long snapshotId() {
        return snapshotId;
    }

    /** Returns the catalog configuration for this table. */
    public CatalogConfig catalogConfig() {
        return config;
    }

    /**
     * Returns the loaded Iceberg table. The table is loaded lazily on first access
     * (triggered by Calcite resolving the table during query planning).
     *
     * @throws IllegalStateException if the table has not been loaded yet
     */
    public Table icebergTable() {
        ensureLoaded();
        return loadedTable;
    }
}

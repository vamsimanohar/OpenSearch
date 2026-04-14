/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.exec;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.iceberg.expressions.Expression;
import org.opensearch.lakehouse.scan.CalciteToIcebergPredicateConverter;
import org.opensearch.lakehouse.schema.IcebergCalciteTable;

import java.util.List;

/**
 * Visitor that walks a Calcite {@link RelNode} tree in a single pass to extract:
 * <ul>
 *   <li>The {@link IcebergCalciteTable} from the first {@link TableScan}</li>
 *   <li>The Iceberg {@link Expression} from any {@link Filter} directly above a {@link TableScan}</li>
 *   <li>The table name from the first {@link TableScan}</li>
 * </ul>
 * <p>
 * Uses Calcite's {@link RelVisitor} infrastructure for idiomatic tree traversal,
 * replacing the three ad-hoc recursive methods that previously existed in
 * {@link LakehouseQueryExecutor}.
 *
 * @opensearch.internal
 */
public class IcebergPlanVisitor extends RelVisitor {

    private IcebergCalciteTable icebergTable;
    private Expression icebergFilter;
    private String tableName;

    @Override
    public void visit(RelNode node, int ordinal, RelNode parent) {
        if (node instanceof TableScan) {
            TableScan scan = (TableScan) node;
            if (icebergTable == null) {
                org.apache.calcite.schema.Table table = scan.getTable().unwrap(org.apache.calcite.schema.Table.class);
                if (table instanceof IcebergCalciteTable) {
                    icebergTable = (IcebergCalciteTable) table;
                }
            }
            if (tableName == null) {
                List<String> qn = scan.getTable().getQualifiedName();
                tableName = qn.get(qn.size() - 1);
            }
        }
        if (node instanceof Filter && icebergFilter == null) {
            Filter filter = (Filter) node;
            if (filter.getInput() instanceof TableScan) {
                RelDataType inputRowType = filter.getInput().getRowType();
                icebergFilter = CalciteToIcebergPredicateConverter.convert(filter.getCondition(), inputRowType);
            }
        }
        super.visit(node, ordinal, parent);
    }

    /**
     * Returns the {@link IcebergCalciteTable} found in the plan, or {@code null} if none.
     */
    public IcebergCalciteTable getIcebergTable() {
        return icebergTable;
    }

    /**
     * Returns the Iceberg filter expression extracted from a Filter node
     * directly above a TableScan, or {@code null} if none found.
     */
    public Expression getIcebergFilter() {
        return icebergFilter;
    }

    /**
     * Returns the table name from the first TableScan in the plan, or {@code null} if none found.
     */
    public String getTableName() {
        return tableName;
    }
}

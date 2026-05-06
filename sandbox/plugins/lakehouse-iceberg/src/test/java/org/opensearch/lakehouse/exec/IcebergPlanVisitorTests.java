/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.exec;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.iceberg.expressions.Expression;
import org.opensearch.lakehouse.schema.IcebergCalciteTable;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.List;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IcebergPlanVisitorTests extends OpenSearchTestCase {

    private static final RelDataTypeFactory TYPE_FACTORY = new JavaTypeFactoryImpl();
    private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

    // --- TableScan with IcebergCalciteTable ---

    public void testExtractsIcebergTableFromTableScan() {
        IcebergCalciteTable icebergTable = mock(IcebergCalciteTable.class);
        TableScan scan = mockTableScan(icebergTable, "my_table");

        IcebergPlanVisitor visitor = new IcebergPlanVisitor();
        visitor.go(scan);

        assertSame(icebergTable, visitor.getIcebergTable());
        assertEquals("my_table", visitor.getTableName());
        assertNull(visitor.getIcebergFilter());
    }

    public void testExtractsTableNameFromTableScan() {
        TableScan scan = mockTableScan(null, "orders");

        IcebergPlanVisitor visitor = new IcebergPlanVisitor();
        visitor.go(scan);

        assertNull(visitor.getIcebergTable());
        assertEquals("orders", visitor.getTableName());
    }

    // --- Filter above TableScan ---

    public void testExtractsFilterAboveTableScan() {
        IcebergCalciteTable icebergTable = mock(IcebergCalciteTable.class);
        TableScan scan = mockTableScan(icebergTable, "hits");

        // Build a row type for the scan — set via reflection because getRowType() is final
        RelDataType rowType = TYPE_FACTORY.builder()
            .add("id", SqlTypeName.BIGINT)
            .add("name", SqlTypeName.VARCHAR)
            .build();
        setRowType(scan, rowType);

        // Build a simple condition: id > 10
        RexNode condition = REX_BUILDER.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            REX_BUILDER.makeInputRef(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), 0),
            REX_BUILDER.makeBigintLiteral(BigDecimal.valueOf(10))
        );

        Filter filter = mockFilter(scan, condition);

        IcebergPlanVisitor visitor = new IcebergPlanVisitor();
        visitor.go(filter);

        assertSame(icebergTable, visitor.getIcebergTable());
        assertEquals("hits", visitor.getTableName());
        assertNotNull(visitor.getIcebergFilter());
        assertEquals(Expression.Operation.GT, visitor.getIcebergFilter().op());
    }

    // --- Filter NOT directly above TableScan ---

    public void testFilterNotAboveTableScanDoesNotExtractFilter() {
        // Filter -> Project -> TableScan  (filter.getInput() is not TableScan)
        IcebergCalciteTable icebergTable = mock(IcebergCalciteTable.class);
        TableScan scan = mockTableScan(icebergTable, "events");

        // Project node wrapping the scan
        RelNode project = mockNodeWithInput(scan);

        // Filter whose input is the project, not the scan
        RexNode condition = REX_BUILDER.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            REX_BUILDER.makeInputRef(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), 0),
            REX_BUILDER.makeBigintLiteral(BigDecimal.valueOf(5))
        );
        Filter filter = mockFilter(project, condition);

        IcebergPlanVisitor visitor = new IcebergPlanVisitor();
        visitor.go(filter);

        assertSame(icebergTable, visitor.getIcebergTable());
        assertEquals("events", visitor.getTableName());
        assertNull(visitor.getIcebergFilter());
    }

    // --- Nested plan: Project -> Filter -> TableScan ---

    public void testNestedPlanExtractsAllThreeProperties() {
        IcebergCalciteTable icebergTable = mock(IcebergCalciteTable.class);
        TableScan scan = mockTableScan(icebergTable, "metrics");

        RelDataType rowType = TYPE_FACTORY.builder()
            .add("value", SqlTypeName.DOUBLE)
            .build();
        setRowType(scan, rowType);

        RexNode condition = REX_BUILDER.makeCall(
            SqlStdOperatorTable.LESS_THAN,
            REX_BUILDER.makeInputRef(TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE), 0),
            REX_BUILDER.makeLiteral(100.0, TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE))
        );

        Filter filter = mockFilter(scan, condition);
        RelNode project = mockNodeWithInput(filter);

        IcebergPlanVisitor visitor = new IcebergPlanVisitor();
        visitor.go(project);

        assertSame(icebergTable, visitor.getIcebergTable());
        assertEquals("metrics", visitor.getTableName());
        assertNotNull(visitor.getIcebergFilter());
    }

    // --- Empty plan (no TableScan) ---

    public void testEmptyPlanReturnsNullForAll() {
        RelNode emptyNode = mockLeafNode();

        IcebergPlanVisitor visitor = new IcebergPlanVisitor();
        visitor.go(emptyNode);

        assertNull(visitor.getIcebergTable());
        assertNull(visitor.getTableName());
        assertNull(visitor.getIcebergFilter());
    }

    // --- Only first TableScan is captured ---

    public void testOnlyFirstTableScanIsCaptured() {
        IcebergCalciteTable firstTable = mock(IcebergCalciteTable.class);
        IcebergCalciteTable secondTable = mock(IcebergCalciteTable.class);

        TableScan scan1 = mockTableScan(firstTable, "table_a");
        TableScan scan2 = mockTableScan(secondTable, "table_b");

        // Parent node with two children
        RelNode parent = mock(RelNode.class);
        when(parent.getInputs()).thenReturn(List.of(scan1, scan2));
        doAnswer(invocation -> {
            RelVisitor v = invocation.getArgument(0);
            v.visit(scan1, 0, parent);
            v.visit(scan2, 1, parent);
            return null;
        }).when(parent).childrenAccept(any(RelVisitor.class));

        IcebergPlanVisitor visitor = new IcebergPlanVisitor();
        visitor.go(parent);

        assertSame(firstTable, visitor.getIcebergTable());
        assertEquals("table_a", visitor.getTableName());
    }

    // --- Only first filter is captured ---

    public void testOnlyFirstFilterIsCaptured() {
        IcebergCalciteTable icebergTable = mock(IcebergCalciteTable.class);
        TableScan scan = mockTableScan(icebergTable, "data");

        RelDataType rowType = TYPE_FACTORY.builder()
            .add("id", SqlTypeName.BIGINT)
            .build();
        setRowType(scan, rowType);

        RexNode condition1 = REX_BUILDER.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            REX_BUILDER.makeInputRef(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), 0),
            REX_BUILDER.makeBigintLiteral(BigDecimal.valueOf(1))
        );
        Filter filter1 = mockFilter(scan, condition1);

        RexNode condition2 = REX_BUILDER.makeCall(
            SqlStdOperatorTable.LESS_THAN,
            REX_BUILDER.makeInputRef(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), 0),
            REX_BUILDER.makeBigintLiteral(BigDecimal.valueOf(100))
        );
        Filter filter2 = mockFilter(filter1, condition2);

        IcebergPlanVisitor visitor = new IcebergPlanVisitor();
        visitor.go(filter2);

        // filter2 is visited first — but its input is filter1, not a TableScan, so it's skipped.
        // filter1 is visited second — its input is a TableScan, so it's extracted.
        assertNotNull(visitor.getIcebergFilter());
        assertEquals(Expression.Operation.GT, visitor.getIcebergFilter().op());
    }

    // --- TableScan with non-Iceberg table ---

    public void testTableScanWithNonIcebergTableReturnsNullIcebergTable() {
        // Table that is NOT an IcebergCalciteTable
        org.apache.calcite.schema.Table regularTable = mock(org.apache.calcite.schema.Table.class);
        TableScan scan = mockTableScanWithTable(regularTable, "regular_table");

        IcebergPlanVisitor visitor = new IcebergPlanVisitor();
        visitor.go(scan);

        assertNull(visitor.getIcebergTable());
        assertEquals("regular_table", visitor.getTableName());
    }

    // --- Helper methods ---

    private TableScan mockTableScan(IcebergCalciteTable icebergTable, String tableName) {
        TableScan scan = mock(TableScan.class);
        RelOptTable relOptTable = mock(RelOptTable.class);
        when(scan.getTable()).thenReturn(relOptTable);
        when(relOptTable.unwrap(org.apache.calcite.schema.Table.class)).thenReturn(icebergTable);
        when(relOptTable.getQualifiedName()).thenReturn(List.of("catalog", "schema", tableName));
        when(scan.getInputs()).thenReturn(List.of());
        doAnswer(invocation -> null).when(scan).childrenAccept(any(RelVisitor.class));
        return scan;
    }

    private TableScan mockTableScanWithTable(org.apache.calcite.schema.Table table, String tableName) {
        TableScan scan = mock(TableScan.class);
        RelOptTable relOptTable = mock(RelOptTable.class);
        when(scan.getTable()).thenReturn(relOptTable);
        when(relOptTable.unwrap(org.apache.calcite.schema.Table.class)).thenReturn(table);
        when(relOptTable.getQualifiedName()).thenReturn(List.of("catalog", "schema", tableName));
        when(scan.getInputs()).thenReturn(List.of());
        doAnswer(invocation -> null).when(scan).childrenAccept(any(RelVisitor.class));
        return scan;
    }

    private Filter mockFilter(RelNode input, RexNode condition) {
        Filter filter = mock(Filter.class);
        when(filter.getInput()).thenReturn(input);
        when(filter.getCondition()).thenReturn(condition);
        when(filter.getInputs()).thenReturn(List.of(input));
        doAnswer(invocation -> {
            RelVisitor v = invocation.getArgument(0);
            v.visit(input, 0, filter);
            return null;
        }).when(filter).childrenAccept(any(RelVisitor.class));
        return filter;
    }

    private RelNode mockNodeWithInput(RelNode input) {
        RelNode node = mock(RelNode.class);
        when(node.getInputs()).thenReturn(List.of(input));
        doAnswer(invocation -> {
            RelVisitor v = invocation.getArgument(0);
            v.visit(input, 0, node);
            return null;
        }).when(node).childrenAccept(any(RelVisitor.class));
        return node;
    }

    private RelNode mockLeafNode() {
        RelNode node = mock(RelNode.class);
        when(node.getInputs()).thenReturn(List.of());
        doAnswer(invocation -> null).when(node).childrenAccept(any(RelVisitor.class));
        return node;
    }

    /**
     * Sets the protected {@code rowType} field on an {@link org.apache.calcite.rel.AbstractRelNode}
     * mock via reflection. Needed because {@code getRowType()} is final and cannot be mocked.
     */
    private static void setRowType(RelNode node, RelDataType rowType) {
        try {
            Field field = org.apache.calcite.rel.AbstractRelNode.class.getDeclaredField("rowType");
            field.setAccessible(true);
            field.set(node, rowType);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set rowType via reflection", e);
        }
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link DefaultPlanExecutor}'s static helper methods.
 */
@SuppressWarnings("deprecation")
public class DefaultPlanExecutorTests extends OpenSearchTestCase {

    private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    private final RelOptCluster cluster = RelOptCluster.create(
        new HepPlanner(new HepProgramBuilder().build()),
        new RexBuilder(typeFactory)
    );

    public void testExtractTableNameFromTableScan() {
        RelOptTable table = mock(RelOptTable.class);
        when(table.getQualifiedName()).thenReturn(List.of("my_index"));
        when(table.getRowType()).thenReturn(buildRowType(1));

        TableScan scan = new StubTableScan(cluster, cluster.traitSet(), table);
        assertEquals("my_index", DefaultPlanExecutor.extractTableName(scan));
    }

    public void testExtractTableNameThrowsForNoTableScan() {
        expectThrows(IllegalArgumentException.class, () -> {
            DefaultPlanExecutor.extractTableName(
                new org.apache.calcite.rel.AbstractRelNode(cluster, cluster.traitSet()) {}
            );
        });
    }

    private RelDataType buildRowType(int fieldCount) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (int i = 0; i < fieldCount; i++) {
            builder.add("field_" + i, SqlTypeName.VARCHAR);
        }
        return builder.build();
    }

    private static class StubTableScan extends TableScan {
        StubTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
            super(cluster, traitSet, List.of(), table);
        }
    }
}

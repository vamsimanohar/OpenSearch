/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

/**
 * Tests for {@link PhysicalPlanSplitter}.
 *
 * <p>Uses Calcite's RelBuilder to construct logical plans programmatically,
 * then verifies the generated worker and coordinator SQL strings.
 */
public class PhysicalPlanSplitterTests extends OpenSearchTestCase {

    private static final String TABLE_NAME = "test_table";

    private RelBuilder createRelBuilder() {
        return RelBuilder.create(
            Frameworks.newConfigBuilder()
                .defaultSchema(
                    Frameworks.createRootSchema(true)
                )
                .build()
        );
    }

    /**
     * Builds a RelNode representing: SELECT * FROM test_table
     */
    private RelNode buildScanOnly(RelBuilder builder) {
        return builder
            .values(
                new String[] { "city", "price", "quantity" },
                "NYC", 100, 10
            )
            .build();
    }

    // --- Test: Scan-only query (no aggregation) ---

    public void testScanOnlyQueryCanDistribute() {
        RelBuilder builder = createRelBuilder();
        // Build a simple values-based plan (scan equivalent)
        RelNode plan = builder
            .values(new String[] { "city", "price" }, "NYC", 100)
            .build();

        PhysicalPlanSplitter.SplitPlan splitPlan = PhysicalPlanSplitter.split(plan, TABLE_NAME);

        assertTrue("Scan-only queries should be distributable", splitPlan.canDistribute());
        assertNotNull("Worker SQL should not be null", splitPlan.getWorkerSql());
        assertNotNull("Coordinator SQL should not be null", splitPlan.getCoordinatorSql());
        assertTrue("Coordinator SQL should reference __partial",
            splitPlan.getCoordinatorSql().contains(PhysicalPlanSplitter.PARTIAL_TABLE));
    }

    // --- Test: Scan with ORDER BY + LIMIT ---

    public void testScanOnlyWithSortGeneratesOrderByInCoordinator() {
        RelBuilder builder = createRelBuilder();
        RelNode plan = builder
            .values(new String[] { "city", "price" }, "NYC", 100)
            .sort(
                builder.field("city")
            )
            .limit(0, 10)
            .build();

        PhysicalPlanSplitter.SplitPlan splitPlan = PhysicalPlanSplitter.split(plan, TABLE_NAME);

        assertTrue("Should be distributable", splitPlan.canDistribute());
        // Worker SQL should NOT contain ORDER BY or LIMIT
        String workerSql = splitPlan.getWorkerSql().toUpperCase();
        assertFalse("Worker SQL should not contain ORDER BY", workerSql.contains("ORDER BY"));
        assertFalse("Worker SQL should not contain LIMIT", workerSql.contains("LIMIT"));
        // Coordinator SQL should contain ORDER BY and LIMIT
        String coordSql = splitPlan.getCoordinatorSql().toUpperCase();
        assertTrue("Coordinator SQL should contain ORDER BY", coordSql.contains("ORDER BY"));
        assertTrue("Coordinator SQL should contain LIMIT", coordSql.contains("LIMIT"));
    }

    // --- Test: GROUP BY with COUNT ---

    public void testGroupByCountDecomposition() {
        RelBuilder builder = createRelBuilder();
        RelNode plan = builder
            .values(new String[] { "city", "price" }, "NYC", 100)
            .aggregate(
                builder.groupKey("city"),
                builder.count(false, "cnt")
            )
            .build();

        PhysicalPlanSplitter.SplitPlan splitPlan = PhysicalPlanSplitter.split(plan, TABLE_NAME);

        assertTrue("GROUP BY COUNT should be distributable", splitPlan.canDistribute());
        // Worker SQL should contain COUNT(*)
        String workerSql = splitPlan.getWorkerSql().toUpperCase();
        assertTrue("Worker SQL should contain COUNT", workerSql.contains("COUNT"));
        assertTrue("Worker SQL should contain GROUP BY", workerSql.contains("GROUP BY"));
        // Coordinator SQL should contain SUM (to merge COUNT partial results)
        String coordSql = splitPlan.getCoordinatorSql().toUpperCase();
        assertTrue("Coordinator SQL should contain SUM for COUNT merge", coordSql.contains("SUM"));
        assertTrue("Coordinator SQL should contain GROUP BY", coordSql.contains("GROUP BY"));
    }

    // --- Test: GROUP BY with SUM ---

    public void testGroupBySumDecomposition() {
        RelBuilder builder = createRelBuilder();
        RelNode plan = builder
            .values(new String[] { "city", "price" }, "NYC", 100)
            .aggregate(
                builder.groupKey("city"),
                builder.sum(false, "total_price", builder.field("price"))
            )
            .build();

        PhysicalPlanSplitter.SplitPlan splitPlan = PhysicalPlanSplitter.split(plan, TABLE_NAME);

        assertTrue("GROUP BY SUM should be distributable", splitPlan.canDistribute());
        // Worker should have SUM
        String workerSql = splitPlan.getWorkerSql().toUpperCase();
        assertTrue("Worker SQL should contain SUM", workerSql.contains("SUM"));
        // Coordinator should also have SUM
        String coordSql = splitPlan.getCoordinatorSql().toUpperCase();
        assertTrue("Coordinator SQL should contain SUM", coordSql.contains("SUM"));
    }

    // --- Test: GROUP BY with MIN, MAX ---

    public void testGroupByMinMaxDecomposition() {
        RelBuilder builder = createRelBuilder();
        RelNode plan = builder
            .values(new String[] { "city", "price" }, "NYC", 100)
            .aggregate(
                builder.groupKey("city"),
                builder.min("min_price", builder.field("price")),
                builder.max("max_price", builder.field("price"))
            )
            .build();

        PhysicalPlanSplitter.SplitPlan splitPlan = PhysicalPlanSplitter.split(plan, TABLE_NAME);

        assertTrue("GROUP BY MIN/MAX should be distributable", splitPlan.canDistribute());
        String workerSql = splitPlan.getWorkerSql().toUpperCase();
        assertTrue("Worker SQL should contain MIN", workerSql.contains("MIN"));
        assertTrue("Worker SQL should contain MAX", workerSql.contains("MAX"));
        String coordSql = splitPlan.getCoordinatorSql().toUpperCase();
        assertTrue("Coordinator SQL should contain MIN", coordSql.contains("MIN"));
        assertTrue("Coordinator SQL should contain MAX", coordSql.contains("MAX"));
    }

    // --- Test: GROUP BY with AVG decomposition ---

    public void testGroupByAvgDecomposition() {
        RelBuilder builder = createRelBuilder();
        RelNode plan = builder
            .values(new String[] { "city", "price" }, "NYC", 100)
            .aggregate(
                builder.groupKey("city"),
                builder.avg(false, "avg_price", builder.field("price"))
            )
            .build();

        PhysicalPlanSplitter.SplitPlan splitPlan = PhysicalPlanSplitter.split(plan, TABLE_NAME);

        assertTrue("GROUP BY AVG should be distributable", splitPlan.canDistribute());
        // Worker should have SUM + COUNT for AVG decomposition
        String workerSql = splitPlan.getWorkerSql().toUpperCase();
        assertTrue("Worker SQL should contain SUM (for AVG decomposition)", workerSql.contains("SUM"));
        assertTrue("Worker SQL should contain COUNT (for AVG decomposition)", workerSql.contains("COUNT"));
        // Coordinator should compute CAST(SUM(...) AS DOUBLE) / SUM(...)
        String coordSql = splitPlan.getCoordinatorSql().toUpperCase();
        assertTrue("Coordinator SQL should contain CAST", coordSql.contains("CAST"));
        assertTrue("Coordinator SQL should contain DOUBLE", coordSql.contains("DOUBLE"));
    }

    // --- Test: Aggregate with ORDER BY + LIMIT ---

    public void testAggregateWithSortAndLimit() {
        RelBuilder builder = createRelBuilder();
        RelNode plan = builder
            .values(new String[] { "city", "price" }, "NYC", 100)
            .aggregate(
                builder.groupKey("city"),
                builder.count(false, "cnt")
            )
            .sort(
                builder.desc(builder.field("cnt"))
            )
            .limit(0, 10)
            .build();

        PhysicalPlanSplitter.SplitPlan splitPlan = PhysicalPlanSplitter.split(plan, TABLE_NAME);

        assertTrue("Should be distributable", splitPlan.canDistribute());
        // Worker SQL should NOT have ORDER BY or LIMIT
        String workerSql = splitPlan.getWorkerSql().toUpperCase();
        assertFalse("Worker SQL should not contain ORDER BY", workerSql.contains("ORDER BY"));
        assertFalse("Worker SQL should not contain LIMIT", workerSql.contains("LIMIT"));
        // Coordinator SQL should have ORDER BY and LIMIT
        String coordSql = splitPlan.getCoordinatorSql().toUpperCase();
        assertTrue("Coordinator SQL should contain ORDER BY", coordSql.contains("ORDER BY"));
        assertTrue("Coordinator SQL should contain LIMIT", coordSql.contains("LIMIT"));
    }

    // --- Test: SplitPlan toString ---

    public void testSplitPlanToString() {
        PhysicalPlanSplitter.SplitPlan plan = new PhysicalPlanSplitter.SplitPlan(
            "SELECT * FROM t", "SELECT * FROM __partial", true,
            PhysicalPlanSplitter.MergeType.PASS_THROUGH, 0, null, null, -1
        );

        String str = plan.toString();
        assertTrue("toString should include canDistribute", str.contains("canDistribute=true"));
        assertTrue("toString should include workerSql", str.contains("workerSql="));
        assertTrue("toString should include coordinatorSql", str.contains("coordinatorSql="));
    }

    public void testCannotDistributeReturnsNullSql() {
        PhysicalPlanSplitter.SplitPlan plan = new PhysicalPlanSplitter.SplitPlan(
            null, null, false, null, 0, null, null, -1
        );

        assertFalse("Should not be distributable", plan.canDistribute());
        assertNull("Worker SQL should be null", plan.getWorkerSql());
        assertNull("Coordinator SQL should be null", plan.getCoordinatorSql());
    }
}

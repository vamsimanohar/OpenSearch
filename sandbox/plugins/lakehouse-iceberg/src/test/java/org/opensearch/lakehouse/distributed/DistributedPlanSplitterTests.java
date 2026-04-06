/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link DistributedPlanSplitter}.
 *
 * <p>Each test builds a Calcite RelNode tree using RelBuilder and verifies
 * that the plan analyzer produces the correct {@link DistributionPlan}.</p>
 */
public class DistributedPlanSplitterTests extends OpenSearchTestCase {

    private RelBuilder relBuilder;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        // Create a root schema and add a test table "orders" with columns:
        //   id INTEGER, name VARCHAR, amount DOUBLE, region VARCHAR
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        rootSchema.add("orders", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                return typeFactory.builder()
                    .add("id", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .add("name", typeFactory.createSqlType(SqlTypeName.VARCHAR))
                    .add("amount", typeFactory.createSqlType(SqlTypeName.DOUBLE))
                    .add("region", typeFactory.createSqlType(SqlTypeName.VARCHAR))
                    .build();
            }
        });

        FrameworkConfig config = Frameworks.newConfigBuilder()
            .defaultSchema(rootSchema)
            .build();

        relBuilder = RelBuilder.create(config);
    }

    // ---- SCAN_ONLY tests ----

    public void testScanOnlyQuery() {
        // SELECT * FROM orders
        RelNode plan = relBuilder
            .scan("orders")
            .build();

        DistributionPlan result = DistributedPlanSplitter.analyze(plan);

        assertEquals(DistributionPlan.QueryType.SCAN_ONLY, result.getQueryType());
        assertEquals(0, result.getGroupKeyOutputColumns().length);
        assertTrue(result.getAggregateMerges().isEmpty());
    }

    public void testScanWithFilterIsScanOnly() {
        // SELECT * FROM orders WHERE id > 10
        RelNode plan = relBuilder
            .scan("orders")
            .filter(
                relBuilder.call(
                    SqlStdOperatorTable.GREATER_THAN,
                    relBuilder.field("id"),
                    relBuilder.literal(10)
                )
            )
            .build();

        DistributionPlan result = DistributedPlanSplitter.analyze(plan);
        assertEquals(DistributionPlan.QueryType.SCAN_ONLY, result.getQueryType());
    }

    public void testScanWithProjectIsScanOnly() {
        // SELECT id, name FROM orders
        RelNode plan = relBuilder
            .scan("orders")
            .project(
                relBuilder.field("id"),
                relBuilder.field("name")
            )
            .build();

        DistributionPlan result = DistributedPlanSplitter.analyze(plan);
        assertEquals(DistributionPlan.QueryType.SCAN_ONLY, result.getQueryType());
    }

    // ---- GLOBAL_AGGREGATE tests ----

    public void testGlobalCount() {
        // SELECT COUNT(*) FROM orders
        RelNode plan = relBuilder
            .scan("orders")
            .aggregate(
                relBuilder.groupKey(),
                relBuilder.count()
            )
            .build();

        DistributionPlan result = DistributedPlanSplitter.analyze(plan);

        assertEquals(DistributionPlan.QueryType.GLOBAL_AGGREGATE, result.getQueryType());
        assertEquals(0, result.getGroupKeyOutputColumns().length);
        assertEquals(1, result.getAggregateMerges().size());
        assertEquals(0, result.getAggregateMerges().get(0).getOutputColumnIndex());
        assertEquals(DistributionPlan.MergeOp.SUM, result.getAggregateMerges().get(0).getMergeOp());
    }

    public void testGlobalSumAndCount() {
        // SELECT COUNT(*), SUM(amount) FROM orders
        RelNode plan = relBuilder
            .scan("orders")
            .aggregate(
                relBuilder.groupKey(),
                relBuilder.count(),
                relBuilder.sum(relBuilder.field("amount"))
            )
            .build();

        DistributionPlan result = DistributedPlanSplitter.analyze(plan);

        assertEquals(DistributionPlan.QueryType.GLOBAL_AGGREGATE, result.getQueryType());
        assertEquals(2, result.getAggregateMerges().size());

        // COUNT -> SUM merge at position 0
        assertEquals(0, result.getAggregateMerges().get(0).getOutputColumnIndex());
        assertEquals(DistributionPlan.MergeOp.SUM, result.getAggregateMerges().get(0).getMergeOp());

        // SUM -> SUM merge at position 1
        assertEquals(1, result.getAggregateMerges().get(1).getOutputColumnIndex());
        assertEquals(DistributionPlan.MergeOp.SUM, result.getAggregateMerges().get(1).getMergeOp());
    }

    public void testGlobalMinMax() {
        // SELECT MIN(amount), MAX(amount) FROM orders
        RelNode plan = relBuilder
            .scan("orders")
            .aggregate(
                relBuilder.groupKey(),
                relBuilder.min(relBuilder.field("amount")),
                relBuilder.max(relBuilder.field("amount"))
            )
            .build();

        DistributionPlan result = DistributedPlanSplitter.analyze(plan);

        assertEquals(DistributionPlan.QueryType.GLOBAL_AGGREGATE, result.getQueryType());
        assertEquals(2, result.getAggregateMerges().size());
        assertEquals(DistributionPlan.MergeOp.MIN, result.getAggregateMerges().get(0).getMergeOp());
        assertEquals(DistributionPlan.MergeOp.MAX, result.getAggregateMerges().get(1).getMergeOp());
    }

    // ---- GROUPED_AGGREGATE tests ----

    public void testGroupedCountStar() {
        // SELECT region, COUNT(*) FROM orders GROUP BY region
        RelNode plan = relBuilder
            .scan("orders")
            .aggregate(
                relBuilder.groupKey(relBuilder.field("region")),
                relBuilder.count()
            )
            .build();

        DistributionPlan result = DistributedPlanSplitter.analyze(plan);

        assertEquals(DistributionPlan.QueryType.GROUPED_AGGREGATE, result.getQueryType());
        // Group key at position 0
        assertEquals(1, result.getGroupKeyOutputColumns().length);
        assertEquals(0, result.getGroupKeyOutputColumns()[0]);
        // COUNT at position 1, merged by SUM
        assertEquals(1, result.getAggregateMerges().size());
        assertEquals(1, result.getAggregateMerges().get(0).getOutputColumnIndex());
        assertEquals(DistributionPlan.MergeOp.SUM, result.getAggregateMerges().get(0).getMergeOp());
    }

    public void testGroupedCountAndSum() {
        // SELECT region, COUNT(*), SUM(amount) FROM orders GROUP BY region
        RelNode plan = relBuilder
            .scan("orders")
            .aggregate(
                relBuilder.groupKey(relBuilder.field("region")),
                relBuilder.count(),
                relBuilder.sum(relBuilder.field("amount"))
            )
            .build();

        DistributionPlan result = DistributedPlanSplitter.analyze(plan);

        assertEquals(DistributionPlan.QueryType.GROUPED_AGGREGATE, result.getQueryType());
        assertEquals(1, result.getGroupKeyOutputColumns().length);
        assertEquals(0, result.getGroupKeyOutputColumns()[0]);
        assertEquals(2, result.getAggregateMerges().size());

        // COUNT at position 1
        assertEquals(1, result.getAggregateMerges().get(0).getOutputColumnIndex());
        assertEquals(DistributionPlan.MergeOp.SUM, result.getAggregateMerges().get(0).getMergeOp());

        // SUM at position 2
        assertEquals(2, result.getAggregateMerges().get(1).getOutputColumnIndex());
        assertEquals(DistributionPlan.MergeOp.SUM, result.getAggregateMerges().get(1).getMergeOp());
    }

    public void testMultipleGroupKeys() {
        // SELECT region, name, COUNT(*) FROM orders GROUP BY region, name
        RelNode plan = relBuilder
            .scan("orders")
            .aggregate(
                relBuilder.groupKey(relBuilder.field("region"), relBuilder.field("name")),
                relBuilder.count()
            )
            .build();

        DistributionPlan result = DistributedPlanSplitter.analyze(plan);

        assertEquals(DistributionPlan.QueryType.GROUPED_AGGREGATE, result.getQueryType());
        assertEquals(2, result.getGroupKeyOutputColumns().length);
        assertEquals(0, result.getGroupKeyOutputColumns()[0]);
        assertEquals(1, result.getGroupKeyOutputColumns()[1]);
        // COUNT at position 2
        assertEquals(1, result.getAggregateMerges().size());
        assertEquals(2, result.getAggregateMerges().get(0).getOutputColumnIndex());
    }

    // ---- UNSUPPORTED tests ----

    public void testAvgIsUnsupported() {
        // SELECT AVG(amount) FROM orders
        RelNode plan = relBuilder
            .scan("orders")
            .aggregate(
                relBuilder.groupKey(),
                relBuilder.avg(relBuilder.field("amount"))
            )
            .build();

        DistributionPlan result = DistributedPlanSplitter.analyze(plan);
        assertEquals(DistributionPlan.QueryType.UNSUPPORTED, result.getQueryType());
    }

    public void testGroupedAvgIsUnsupported() {
        // SELECT region, AVG(amount) FROM orders GROUP BY region
        RelNode plan = relBuilder
            .scan("orders")
            .aggregate(
                relBuilder.groupKey(relBuilder.field("region")),
                relBuilder.avg(relBuilder.field("amount"))
            )
            .build();

        DistributionPlan result = DistributedPlanSplitter.analyze(plan);
        assertEquals(DistributionPlan.QueryType.UNSUPPORTED, result.getQueryType());
    }

    public void testGroupedAggregateWithProjectRemovingGroupKeyIsUnsupported() {
        // SELECT COUNT(*), SUM(amount) FROM orders GROUP BY region
        // (group key 'region' projected away)
        RelNode plan = relBuilder
            .scan("orders")
            .aggregate(
                relBuilder.groupKey(relBuilder.field("region")),
                relBuilder.count(),
                relBuilder.sum(relBuilder.field("amount"))
            )
            .project(
                relBuilder.field(1),  // COUNT (skip region at 0)
                relBuilder.field(2)   // SUM
            )
            .build();

        DistributionPlan result = DistributedPlanSplitter.analyze(plan);
        assertEquals(DistributionPlan.QueryType.UNSUPPORTED, result.getQueryType());
    }

    // ---- Sort wrapping tests ----

    public void testGroupedAggregateWithSort() {
        // SELECT region, COUNT(*) FROM orders GROUP BY region ORDER BY region
        RelNode plan = relBuilder
            .scan("orders")
            .aggregate(
                relBuilder.groupKey(relBuilder.field("region")),
                relBuilder.count()
            )
            .sort(relBuilder.field("region"))
            .build();

        DistributionPlan result = DistributedPlanSplitter.analyze(plan);

        // Should unwrap sort and find the grouped aggregate
        assertEquals(DistributionPlan.QueryType.GROUPED_AGGREGATE, result.getQueryType());
        assertEquals(1, result.getGroupKeyOutputColumns().length);
        assertEquals(0, result.getGroupKeyOutputColumns()[0]);
    }

    public void testGlobalAggregateWithSortLimit() {
        // SELECT COUNT(*) FROM orders LIMIT 1
        RelNode plan = relBuilder
            .scan("orders")
            .aggregate(
                relBuilder.groupKey(),
                relBuilder.count()
            )
            .sortLimit(0, 1)
            .build();

        DistributionPlan result = DistributedPlanSplitter.analyze(plan);
        assertEquals(DistributionPlan.QueryType.GLOBAL_AGGREGATE, result.getQueryType());
    }

    // ---- Project-on-aggregate that preserves group keys ----

    // ---- Sort+Limit detection tests ----

    public void testOrderByWithLimit() {
        // SELECT * FROM orders ORDER BY amount DESC LIMIT 3
        RelNode plan = relBuilder
            .scan("orders")
            .sortLimit(0, 3, relBuilder.desc(relBuilder.field("amount")))
            .build();

        DistributionPlan result = DistributedPlanSplitter.analyze(plan);

        assertEquals(DistributionPlan.QueryType.SCAN_ONLY, result.getQueryType());
        assertNotNull("Expected sort info", result.getSortInfo());
        assertEquals(1, result.getSortInfo().getSortColumns().length);
        assertEquals(2, result.getSortInfo().getSortColumns()[0]); // amount is column index 2
        assertFalse("Expected DESC", result.getSortInfo().getAscending()[0]);
        assertEquals(3, result.getSortInfo().getLimit());
    }

    public void testOrderByAscWithLimit() {
        // SELECT * FROM orders ORDER BY id ASC LIMIT 5
        RelNode plan = relBuilder
            .scan("orders")
            .sortLimit(0, 5, relBuilder.field("id"))
            .build();

        DistributionPlan result = DistributedPlanSplitter.analyze(plan);

        assertEquals(DistributionPlan.QueryType.SCAN_ONLY, result.getQueryType());
        assertNotNull("Expected sort info", result.getSortInfo());
        assertEquals(1, result.getSortInfo().getSortColumns().length);
        assertEquals(0, result.getSortInfo().getSortColumns()[0]); // id is column index 0
        assertTrue("Expected ASC", result.getSortInfo().getAscending()[0]);
        assertEquals(5, result.getSortInfo().getLimit());
    }

    public void testGroupedAggregateWithOrderByAndLimit() {
        // SELECT region, COUNT(*) FROM orders GROUP BY region ORDER BY COUNT(*) DESC LIMIT 2
        RelNode plan = relBuilder
            .scan("orders")
            .aggregate(
                relBuilder.groupKey(relBuilder.field("region")),
                relBuilder.count()
            )
            .sortLimit(0, 2, relBuilder.desc(relBuilder.field(1)))
            .build();

        DistributionPlan result = DistributedPlanSplitter.analyze(plan);

        assertEquals(DistributionPlan.QueryType.GROUPED_AGGREGATE, result.getQueryType());
        assertNotNull("Expected sort info on grouped aggregate", result.getSortInfo());
        assertEquals(1, result.getSortInfo().getSortColumns().length);
        assertEquals(1, result.getSortInfo().getSortColumns()[0]); // COUNT is at position 1
        assertFalse("Expected DESC", result.getSortInfo().getAscending()[0]);
        assertEquals(2, result.getSortInfo().getLimit());
    }

    public void testLimitOnlyNoOrderBy() {
        // SELECT * FROM orders LIMIT 10
        RelNode plan = relBuilder
            .scan("orders")
            .sortLimit(0, 10)
            .build();

        DistributionPlan result = DistributedPlanSplitter.analyze(plan);

        assertEquals(DistributionPlan.QueryType.SCAN_ONLY, result.getQueryType());
        assertNotNull("Expected sort info with limit only", result.getSortInfo());
        assertEquals(0, result.getSortInfo().getSortColumns().length);
        assertEquals(10, result.getSortInfo().getLimit());
    }

    public void testMultiColumnOrderByWithLimit() {
        // SELECT * FROM orders ORDER BY region ASC, amount DESC LIMIT 5
        RelNode plan = relBuilder
            .scan("orders")
            .sortLimit(0, 5,
                relBuilder.field("region"),
                relBuilder.desc(relBuilder.field("amount")))
            .build();

        DistributionPlan result = DistributedPlanSplitter.analyze(plan);

        assertEquals(DistributionPlan.QueryType.SCAN_ONLY, result.getQueryType());
        assertNotNull("Expected sort info", result.getSortInfo());
        assertEquals(2, result.getSortInfo().getSortColumns().length);
        // region is column index 3, amount is column index 2
        assertEquals(3, result.getSortInfo().getSortColumns()[0]);
        assertTrue("Expected ASC for region", result.getSortInfo().getAscending()[0]);
        assertEquals(2, result.getSortInfo().getSortColumns()[1]);
        assertFalse("Expected DESC for amount", result.getSortInfo().getAscending()[1]);
        assertEquals(5, result.getSortInfo().getLimit());
    }

    // ---- Existing project tests ----

    public void testGroupedAggregateWithProjectPreservingGroupKey() {
        // SELECT region, COUNT(*) FROM orders GROUP BY region
        // with explicit project that keeps all columns
        RelNode plan = relBuilder
            .scan("orders")
            .aggregate(
                relBuilder.groupKey(relBuilder.field("region")),
                relBuilder.count(),
                relBuilder.sum(relBuilder.field("amount"))
            )
            .project(
                relBuilder.field(0),  // region (group key)
                relBuilder.field(1)   // COUNT
            )
            .build();

        DistributionPlan result = DistributedPlanSplitter.analyze(plan);

        assertEquals(DistributionPlan.QueryType.GROUPED_AGGREGATE, result.getQueryType());
        assertEquals(1, result.getGroupKeyOutputColumns().length);
        assertEquals(0, result.getGroupKeyOutputColumns()[0]);
        // Only COUNT is in the project output (SUM projected away)
        assertEquals(1, result.getAggregateMerges().size());
        assertEquals(1, result.getAggregateMerges().get(0).getOutputColumnIndex());
        assertEquals(DistributionPlan.MergeOp.SUM, result.getAggregateMerges().get(0).getMergeOp());
    }
}

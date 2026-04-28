/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.engine;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigDecimal;
import java.util.List;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PlanFragmenterTests extends OpenSearchTestCase {

    private static final RelDataType BIGINT_TYPE = new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.BIGINT);

    // ==== fragment() — full plan tests ====

    public void testSimpleScanReturnsConcat() {
        RelNode scan = mockSimpleNode();
        SubPlan plan = PlanFragmenter.fragment(scan, "SELECT * FROM t");


        assertEquals(2, plan.getStageCount());

        PlanFragment leaf = plan.getLeafStage();
        assertTrue(leaf.isLeaf());
        assertEquals("SELECT * FROM t", leaf.getSql());
        assertEquals(ExchangeType.GATHER, leaf.getOutputExchange());

        PlanFragment fin = plan.getFinalStage();
        assertFalse(fin.isLeaf());
        assertEquals("SELECT * FROM __exchange_input__", fin.getSql());
        assertEquals(ExchangeType.NONE, fin.getOutputExchange());
    }

    public void testGlobalCountReturnsGatherMerge() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(makeAggCall(SqlStdOperatorTable.COUNT, false)));
        SubPlan plan = PlanFragmenter.fragment(agg, "SELECT COUNT(*) FROM t");


        assertEquals(2, plan.getStageCount());
        assertEquals(ExchangeType.GATHER, plan.getLeafStage().getOutputExchange());
        assertNull(plan.getLeafStage().getHashColumns());
    }

    public void testGlobalAvgDecomposesInLeafSql() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(makeAggCall(SqlStdOperatorTable.AVG, false)));
        SubPlan plan = PlanFragmenter.fragment(agg, "SELECT AVG(x) FROM t");


        assertEquals(2, plan.getStageCount());

        String leafSql = plan.getLeafStage().getSql();
        assertTrue(leafSql.contains("SUM(CAST("));
        assertTrue(leafSql.contains("COUNT("));
        assertFalse(leafSql.contains("AVG("));

        String coordSql = plan.getFinalStage().getSql();
        assertTrue(coordSql.contains("CAST(SUM("));
        assertTrue(coordSql.contains("/ SUM("));
    }

    public void testGlobalMixedSumAvgCount() {
        AggregateCall sumCall = makeAggCall(SqlStdOperatorTable.SUM, false);
        AggregateCall avgCall = makeAggCall(SqlStdOperatorTable.AVG, false);
        AggregateCall countCall = makeAggCall(SqlStdOperatorTable.COUNT, false);
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(sumCall, avgCall, countCall));
        SubPlan plan = PlanFragmenter.fragment(agg, "SELECT SUM(a), AVG(b), COUNT(*) FROM t");


        String leafSql = plan.getLeafStage().getSql();
        assertTrue(leafSql.contains("SUM(CAST("));
        assertTrue(leafSql.contains("COUNT("));
    }

    public void testGlobalMinMax() {
        AggregateCall minCall = makeAggCall(SqlStdOperatorTable.MIN, false);
        AggregateCall maxCall = makeAggCall(SqlStdOperatorTable.MAX, false);
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(minCall, maxCall));
        SubPlan plan = PlanFragmenter.fragment(agg, "SELECT MIN(x), MAX(y) FROM t");


        String coordSql = plan.getFinalStage().getSql();
        assertTrue(coordSql.contains("MIN("));
        assertTrue(coordSql.contains("MAX("));
    }

    public void testTopKMergeWithSortAndLimit() {
        Sort sort = makeSort(true, true);
        SubPlan plan = PlanFragmenter.fragment(sort, "SELECT * FROM t ORDER BY col0 ASC LIMIT 10");


        assertEquals(2, plan.getStageCount());
        assertEquals(ExchangeType.GATHER, plan.getLeafStage().getOutputExchange());

        String coordSql = plan.getFinalStage().getSql();
        assertTrue(coordSql.contains("ORDER BY"));
    }

    public void testSortWithoutLimitThrows() {
        Sort sort = makeSort(true, false);
        expectThrows(UnsupportedOperationException.class,
            () -> PlanFragmenter.fragment(sort, "SELECT * FROM t ORDER BY col0"));
    }

    public void testGroupByNoLimitReturnsTwoPhaseGather() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0), List.of(makeAggCall(SqlStdOperatorTable.COUNT, false)));
        SubPlan plan = PlanFragmenter.fragment(agg, "SELECT region, COUNT(*) FROM t GROUP BY region");


        assertEquals(2, plan.getStageCount());
        assertEquals(ExchangeType.GATHER, plan.getLeafStage().getOutputExchange());
        assertNull(plan.getLeafStage().getHashColumns());

        String coordSql = plan.getFinalStage().getSql();
        assertTrue(coordSql.contains("GROUP BY"));
        assertTrue(coordSql.contains("SUM("));
    }

    public void testGroupByWithLimitReturnsTwoPhaseGather() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0), List.of(makeAggCall(SqlStdOperatorTable.COUNT, false)));
        RelNode wrapper = mockNodeWithInput(agg);
        Sort sort = makeSortWithInput(wrapper, true);

        SubPlan plan = PlanFragmenter.fragment(sort,
            "SELECT region, COUNT(*) FROM t GROUP BY region ORDER BY COUNT(*) DESC LIMIT 10");

        assertEquals(2, plan.getStageCount());
        assertEquals(ExchangeType.GATHER, plan.getLeafStage().getOutputExchange());

        String leafSql = plan.getLeafStage().getSql();
        assertTrue("Workers keep ORDER BY with expanded LIMIT", leafSql.contains("LIMIT 1000"));
        assertFalse("Workers should not have OFFSET", leafSql.toUpperCase().contains("OFFSET"));

        String coordSql = plan.getFinalStage().getSql();
        assertTrue(coordSql.contains("GROUP BY"));
        assertTrue(coordSql.contains("LIMIT 10"));
    }

    public void testGroupByWithLimitAndAvgReturnsThreeStageHash() {
        AggregateCall avgCall = makeAggCall(SqlStdOperatorTable.AVG, false);
        AggregateCall countCall = makeAggCall(SqlStdOperatorTable.COUNT, false);
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0), List.of(avgCall, countCall));
        RelNode wrapper = mockNodeWithInput(agg);
        Sort sort = makeSortWithInput(wrapper, true, 5);

        SubPlan plan = PlanFragmenter.fragment(sort,
            "SELECT region, AVG(price), COUNT(*) FROM t GROUP BY region ORDER BY 2 DESC LIMIT 5");

        assertEquals(3, plan.getStageCount());
        assertEquals(ExchangeType.HASH, plan.getLeafStage().getOutputExchange());

        String leafSql = plan.getLeafStage().getSql();
        assertTrue("Worker should decompose AVG", leafSql.contains("SUM(CAST("));
        assertTrue("Worker should have bounded LIMIT", leafSql.contains("LIMIT 500"));

        String intermediateSql = plan.getStages().get(1).getSql();
        assertTrue(intermediateSql.contains("LIMIT 5"));
    }

    public void testGlobalCountDistinctDecomposes() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(makeAggCall(SqlStdOperatorTable.COUNT, true)));
        SubPlan plan = PlanFragmenter.fragment(agg, "SELECT COUNT(DISTINCT userid) FROM t");

        assertEquals(2, plan.getStageCount());

        String leafSql = plan.getLeafStage().getSql();
        assertTrue(leafSql.contains("DISTINCT"));
        assertTrue(leafSql.contains("userid"));
        assertFalse(leafSql.contains("COUNT(DISTINCT"));

        String coordSql = plan.getFinalStage().getSql();
        assertTrue(coordSql.contains("COUNT(DISTINCT"));
    }

    public void testGroupByDistinctSumThrows() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0), List.of(makeAggCall(SqlStdOperatorTable.SUM, true)));
        expectThrows(UnsupportedOperationException.class,
            () -> PlanFragmenter.fragment(agg, "SELECT region, SUM(DISTINCT x) FROM t GROUP BY region"));
    }

    public void testMixedCountDistinctWithOtherAggsThrows() {
        AggregateCall sumCall = makeAggCall(SqlStdOperatorTable.SUM, false);
        AggregateCall countDistinctCall = makeAggCall(SqlStdOperatorTable.COUNT, true);
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0), List.of(sumCall, countDistinctCall));
        expectThrows(UnsupportedOperationException.class,
            () -> PlanFragmenter.fragment(agg,
                "SELECT region, SUM(x), COUNT(DISTINCT userid) FROM t GROUP BY region"));
    }

    public void testGroupByWithNonPassthroughAnyValueDistributes() {
        AggregateCall anyValueCall = makeAggCallWithArgs(SqlStdOperatorTable.ANY_VALUE, false, List.of(0));
        AggregateCall countCall = makeAggCall(SqlStdOperatorTable.COUNT, false);
        Aggregate agg = mockAggregate(ImmutableBitSet.of(1), List.of(anyValueCall, countCall));
        SubPlan plan = PlanFragmenter.fragment(agg,
            "SELECT 1 AS one, url, COUNT(*) FROM t GROUP BY url");

        assertEquals(2, plan.getStageCount());
        String coordSql = plan.getFinalStage().getSql();
        assertTrue(coordSql.contains("GROUP BY"));
        assertTrue(coordSql.contains("MIN("));
    }

    public void testGroupByWithHavingDistributes() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0), List.of(makeAggCall(SqlStdOperatorTable.COUNT, false)));
        SubPlan plan = PlanFragmenter.fragment(agg,
            "SELECT region, COUNT(*) FROM t GROUP BY region HAVING COUNT(*) > 100");

        assertEquals(2, plan.getStageCount());
        String leafSql = plan.getLeafStage().getSql();
        assertFalse("Worker SQL should not have HAVING", leafSql.toUpperCase().contains("HAVING"));

        String coordSql = plan.getFinalStage().getSql();
        assertTrue("Coordinator HAVING should use re-aggregated SUM", coordSql.contains("HAVING SUM("));
    }

    public void testGroupByWithOffsetReturnsTwoPhaseGather() {
        AggregateCall countCall = makeAggCall(SqlStdOperatorTable.COUNT, false);
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0), List.of(countCall));
        RelNode wrapper = mockNodeWithInput(agg);
        Sort sort = makeSortWithInputAndOffset(wrapper, 10, 100);

        SubPlan plan = PlanFragmenter.fragment(sort,
            "SELECT region, COUNT(*) FROM t GROUP BY region ORDER BY 2 DESC LIMIT 10 OFFSET 100");

        assertEquals(2, plan.getStageCount());
        assertEquals(ExchangeType.GATHER, plan.getLeafStage().getOutputExchange());

        String leafSql = plan.getLeafStage().getSql();
        assertTrue("Workers expand LIMIT to max(LIMIT+OFFSET, LIMIT*100)", leafSql.contains("LIMIT 1000"));
        assertFalse("Workers should not have OFFSET", leafSql.toUpperCase().contains("OFFSET"));

        String coordSql = plan.getFinalStage().getSql();
        assertTrue("Coordinator LIMIT should be limit+offset", coordSql.contains("LIMIT 110"));
        assertFalse("Coordinator should not have OFFSET (applied in Java)", coordSql.toUpperCase().contains("OFFSET"));
        assertEquals("Global offset should be stored in SubPlan", 100, plan.getGlobalOffset());
    }

    public void testGroupByCountDistinctWithLimitReturnsThreeStageHash() {
        AggregateCall countDistinctCall = makeAggCall(SqlStdOperatorTable.COUNT, true);
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0), List.of(countDistinctCall));
        RelNode wrapper = mockNodeWithInput(agg);
        Sort sort = makeSortWithInput(wrapper, true);

        SubPlan plan = PlanFragmenter.fragment(sort,
            "SELECT region, COUNT(DISTINCT userid) FROM t GROUP BY region ORDER BY 2 DESC LIMIT 10");

        assertEquals(3, plan.getStageCount());
        assertEquals(ExchangeType.HASH, plan.getLeafStage().getOutputExchange());

        String leafSql = plan.getLeafStage().getSql();
        assertTrue("Worker should dedup", leafSql.contains("userid"));
        assertFalse("COUNT(DISTINCT) workers must not have bounded LIMIT", leafSql.toUpperCase().contains("LIMIT"));

        String intermediateSql = plan.getStages().get(1).getSql();
        assertTrue(intermediateSql.contains("COUNT(DISTINCT"));
        assertTrue(intermediateSql.contains("LIMIT 10"));
    }

    public void testGroupByWithHavingAndLimitReturnsThreeStageHash() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0), List.of(
            makeAggCall(SqlStdOperatorTable.AVG, false),
            makeAggCall(SqlStdOperatorTable.COUNT, false)
        ));
        RelNode wrapper = mockNodeWithInput(agg);
        Sort sort = makeSortWithInput(wrapper, true, 25);

        SubPlan plan = PlanFragmenter.fragment(sort,
            "SELECT counterid, AVG(CHAR_LENGTH(url)) AS l, COUNT(*) AS c FROM t GROUP BY counterid HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25");

        assertEquals(3, plan.getStageCount());
        assertEquals(ExchangeType.HASH, plan.getLeafStage().getOutputExchange());

        String leafSql = plan.getLeafStage().getSql();
        assertFalse("Worker should not have HAVING", leafSql.toUpperCase().contains("HAVING"));
        assertTrue("Worker should decompose AVG", leafSql.contains("SUM(CAST("));
        assertTrue("Worker should have bounded LIMIT", leafSql.contains("LIMIT 2500"));

        String intermediateSql = plan.getStages().get(1).getSql();
        assertTrue("Intermediate HAVING should use re-aggregated SUM", intermediateSql.contains("HAVING SUM("));
        assertTrue(intermediateSql.contains("LIMIT 25"));
    }

    public void testMixedCountDistinctWithLimitThrows() {
        AggregateCall sumCall = makeAggCall(SqlStdOperatorTable.SUM, false);
        AggregateCall countCall = makeAggCall(SqlStdOperatorTable.COUNT, false);
        AggregateCall avgCall = makeAggCall(SqlStdOperatorTable.AVG, false);
        AggregateCall countDistinctCall = makeAggCall(SqlStdOperatorTable.COUNT, true);
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0), List.of(sumCall, countCall, avgCall, countDistinctCall));
        RelNode wrapper = mockNodeWithInput(agg);
        Sort sort = makeSortWithInput(wrapper, true);

        expectThrows(UnsupportedOperationException.class,
            () -> PlanFragmenter.fragment(sort,
                "SELECT regionid, SUM(advengineid), COUNT(*) AS c, AVG(resolutionwidth), COUNT(DISTINCT userid) FROM t GROUP BY regionid ORDER BY c DESC LIMIT 10"));
    }

    public void testGroupByAvgNoLimitDecomposes() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0), List.of(makeAggCall(SqlStdOperatorTable.AVG, false)));
        SubPlan plan = PlanFragmenter.fragment(agg, "SELECT region, AVG(x) FROM t GROUP BY region");

        assertEquals(2, plan.getStageCount());

        String leafSql = plan.getLeafStage().getSql();
        assertTrue(leafSql.contains("SUM(CAST("));
        assertTrue(leafSql.contains("COUNT("));
        assertFalse(leafSql.contains("AVG("));

        String coordSql = plan.getFinalStage().getSql();
        assertTrue(coordSql.contains("CAST(SUM("));
        assertTrue(coordSql.contains("/ SUM("));
    }

    public void testNestedAggregateDetected() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(makeAggCall(SqlStdOperatorTable.SUM, false)));
        RelNode project = mockNodeWithInput(agg);
        SubPlan plan = PlanFragmenter.fragment(project, "SELECT SUM(x) FROM t");

        assertEquals(2, plan.getStageCount());
    }

    public void testNestedSortDetected() {
        Sort sort = makeSort(true, true);
        RelNode project = mockNodeWithInput(sort);
        SubPlan plan = PlanFragmenter.fragment(project, "SELECT * FROM t ORDER BY col0 LIMIT 10");

        assertEquals(2, plan.getStageCount());
    }

    // ==== SQL builder tests ====

    public void testBuildGlobalMergeCoordinatorSqlSumCount() {
        String sql = PlanFragmenter.buildGlobalMergeCoordinatorSql(
            new SqlKind[] { SqlKind.SUM, SqlKind.COUNT }, false, new boolean[] { false, false }
        );
        assertEquals("SELECT SUM(\"col_0\"), SUM(\"col_1\") FROM __exchange_input__", sql);
    }

    public void testBuildGlobalMergeCoordinatorSqlMinMax() {
        String sql = PlanFragmenter.buildGlobalMergeCoordinatorSql(
            new SqlKind[] { SqlKind.MIN, SqlKind.MAX }, false, new boolean[] { false, false }
        );
        assertEquals("SELECT MIN(\"col_0\"), MAX(\"col_1\") FROM __exchange_input__", sql);
    }

    public void testBuildGlobalMergeCoordinatorSqlWithAvg() {
        String sql = PlanFragmenter.buildGlobalMergeCoordinatorSql(
            new SqlKind[] { SqlKind.SUM, SqlKind.AVG }, true, new boolean[] { false, false }
        );
        // SUM takes col_0, AVG takes col_1 (SUM) and col_2 (COUNT)
        assertEquals(
            "SELECT SUM(\"col_0\"), CAST(SUM(\"col_1\") AS DOUBLE) / SUM(\"col_2\") FROM __exchange_input__",
            sql
        );
    }

    public void testBuildGlobalMergeCoordinatorSqlWithCountDistinct() {
        String sql = PlanFragmenter.buildGlobalMergeCoordinatorSql(
            new SqlKind[] { SqlKind.COUNT }, false, new boolean[] { true }
        );
        assertEquals("SELECT COUNT(DISTINCT \"col_0\") FROM __exchange_input__", sql);
    }

    public void testBuildGlobalMergeCoordinatorSqlNullKinds() {
        String sql = PlanFragmenter.buildGlobalMergeCoordinatorSql(null, false, null);
        assertEquals("SELECT * FROM __exchange_input__", sql);
    }

    public void testBuildGlobalMergeCoordinatorSqlEmptyKinds() {
        String sql = PlanFragmenter.buildGlobalMergeCoordinatorSql(new SqlKind[] {}, false, new boolean[] {});
        assertEquals("SELECT * FROM __exchange_input__", sql);
    }

    public void testBuildTwoPhaseGroupByCoordinatorSql() {
        String sql = PlanFragmenter.buildTwoPhaseGroupByCoordinatorSql(
            1, new SqlKind[] { SqlKind.COUNT, SqlKind.SUM }, null, false, new boolean[] { false, false }
        );
        assertEquals(
            "SELECT \"col_0\", SUM(\"col_1\"), SUM(\"col_2\") FROM __exchange_input__ GROUP BY \"col_0\"",
            sql
        );
    }

    public void testBuildTwoPhaseGroupByCoordinatorSqlWithSort() {
        Sort sort = makeSort(true, false);
        String sql = PlanFragmenter.buildTwoPhaseGroupByCoordinatorSql(
            1, new SqlKind[] { SqlKind.COUNT }, sort, false, new boolean[] { false }
        );
        assertTrue(sql.contains("ORDER BY 1 ASC"));
    }

    public void testBuildTwoPhaseGroupByCoordinatorSqlMinMax() {
        String sql = PlanFragmenter.buildTwoPhaseGroupByCoordinatorSql(
            2, new SqlKind[] { SqlKind.MIN, SqlKind.MAX }, null, false, new boolean[] { false, false }
        );
        assertEquals(
            "SELECT \"col_0\", \"col_1\", MIN(\"col_2\"), MAX(\"col_3\") FROM __exchange_input__ GROUP BY \"col_0\", \"col_1\"",
            sql
        );
    }

    public void testBuildTwoPhaseGroupByCoordinatorSqlWithCountDistinct() {
        String sql = PlanFragmenter.buildTwoPhaseGroupByCoordinatorSql(
            1, new SqlKind[] { SqlKind.COUNT, SqlKind.COUNT }, null, false, new boolean[] { false, true }
        );
        assertEquals(
            "SELECT \"col_0\", SUM(\"col_1\"), COUNT(DISTINCT \"col_2\") FROM __exchange_input__ GROUP BY \"col_0\"",
            sql
        );
    }

    public void testBuildTwoPhaseGroupByCoordinatorSqlWithAvgAndCountDistinct() {
        String sql = PlanFragmenter.buildTwoPhaseGroupByCoordinatorSql(
            1, new SqlKind[] { SqlKind.AVG, SqlKind.COUNT }, null, true, new boolean[] { false, true }
        );
        assertEquals(
            "SELECT \"col_0\", CAST(SUM(\"col_1\") AS DOUBLE) / SUM(\"col_2\"), COUNT(DISTINCT \"col_3\") FROM __exchange_input__ GROUP BY \"col_0\"",
            sql
        );
    }

    public void testBuildTwoPhaseGroupByCoordinatorSqlWithPassthrough() {
        // 3 group keys, aggs=[ANY_VALUE(passthrough), COUNT], only COUNT appears in coordinator
        String sql = PlanFragmenter.buildTwoPhaseGroupByCoordinatorSql(
            3, new SqlKind[] { SqlKind.ANY_VALUE, SqlKind.COUNT }, null, false,
            new boolean[] { false, false }, new boolean[] { true, false }
        );
        assertEquals(
            "SELECT \"col_0\", \"col_1\", \"col_2\", SUM(\"col_3\") FROM __exchange_input__ GROUP BY \"col_0\", \"col_1\", \"col_2\"",
            sql
        );
    }

    public void testBuildTwoPhaseGroupByCoordinatorSqlWithMultiplePassthrough() {
        // 4 group keys, aggs=[ANY_VALUE(pt) x3, COUNT], only COUNT appears
        String sql = PlanFragmenter.buildTwoPhaseGroupByCoordinatorSql(
            4, new SqlKind[] { SqlKind.ANY_VALUE, SqlKind.ANY_VALUE, SqlKind.ANY_VALUE, SqlKind.COUNT }, null, false,
            new boolean[] { false, false, false, false }, new boolean[] { true, true, true, false }
        );
        assertEquals(
            "SELECT \"col_0\", \"col_1\", \"col_2\", \"col_3\", SUM(\"col_4\") FROM __exchange_input__ GROUP BY \"col_0\", \"col_1\", \"col_2\", \"col_3\"",
            sql
        );
    }

    public void testBuildTwoPhaseGroupByCoordinatorSqlAvgWithOrderByWrapsSubquery() {
        Sort sort = makeSort(true, false);
        String sql = PlanFragmenter.buildTwoPhaseGroupByCoordinatorSql(
            1, new SqlKind[] { SqlKind.AVG }, sort, true, new boolean[] { false }
        );
        assertTrue("AVG + ORDER BY should wrap in subquery", sql.startsWith("SELECT * FROM (SELECT"));
        assertTrue(sql.contains("GROUP BY"));
        assertTrue(sql.contains("ORDER BY"));
    }

    public void testBuildIntermediateGroupBySql() {
        Sort sort = makeSortWithLimit(10);
        String sql = PlanFragmenter.buildIntermediateGroupBySql(
            1, new SqlKind[] { SqlKind.COUNT, SqlKind.SUM }, sort, false, new boolean[] { false, false }
        );
        assertTrue(sql.contains("\"col_0\""));
        assertTrue(sql.contains("SUM(\"col_1\")"));
        assertTrue(sql.contains("SUM(\"col_2\")"));
        assertTrue(sql.contains("GROUP BY \"col_0\""));
        assertTrue(sql.contains("ORDER BY 1 ASC"));
        assertTrue(sql.contains("LIMIT 10"));
    }

    public void testBuildIntermediateGroupBySqlWithAvg() {
        Sort sort = makeSortWithLimit(5);
        String sql = PlanFragmenter.buildIntermediateGroupBySql(
            1, new SqlKind[] { SqlKind.AVG, SqlKind.COUNT }, sort, true, new boolean[] { false, false }
        );
        // AVG takes 2 worker cols (SUM+COUNT)
        assertTrue(sql.contains("CAST(SUM(\"col_1\") AS DOUBLE) / SUM(\"col_2\")"));
        // COUNT is next col after the 2 AVG cols
        assertTrue(sql.contains("SUM(\"col_3\")"));
        assertTrue(sql.contains("LIMIT 5"));
    }

    public void testBuildIntermediateGroupBySqlWithCountDistinct() {
        Sort sort = makeSortWithLimit(10);
        String sql = PlanFragmenter.buildIntermediateGroupBySql(
            1, new SqlKind[] { SqlKind.COUNT, SqlKind.COUNT }, sort, false, new boolean[] { false, true }
        );
        assertTrue(sql.contains("SUM(\"col_1\")"));
        assertTrue(sql.contains("COUNT(DISTINCT \"col_2\")"));
        assertTrue(sql.contains("LIMIT 10"));
    }

    public void testBuildTopKCoordinatorSql() {
        String sql = PlanFragmenter.buildTopKCoordinatorSql(new int[] { 2 }, new boolean[] { false }, 20, 0);
        assertEquals("SELECT * FROM __exchange_input__ ORDER BY \"col_2\" DESC LIMIT 20", sql);
    }

    public void testBuildTopKCoordinatorSqlMultipleSortColumns() {
        String sql = PlanFragmenter.buildTopKCoordinatorSql(
            new int[] { 0, 3 }, new boolean[] { true, false }, 50, 0
        );
        assertEquals("SELECT * FROM __exchange_input__ ORDER BY \"col_0\" ASC, \"col_3\" DESC LIMIT 50", sql);
    }

    public void testBuildTopKCoordinatorSqlNoLimit() {
        String sql = PlanFragmenter.buildTopKCoordinatorSql(new int[] { 1 }, new boolean[] { true }, 0, 0);
        assertEquals("SELECT * FROM __exchange_input__ ORDER BY \"col_1\" ASC", sql);
    }

    public void testBuildTopKCoordinatorSqlWithExtraColumns() {
        // Sort column 1 >= outputColumnCount=1 → needs subquery stripping
        String sql = PlanFragmenter.buildTopKCoordinatorSql(
            new int[] { 1 }, new boolean[] { true }, 10, 1
        );
        assertEquals(
            "SELECT \"col_0\" FROM (SELECT * FROM __exchange_input__ ORDER BY \"col_1\" ASC LIMIT 10)",
            sql
        );
    }

    public void testBuildTopKCoordinatorSqlAllColumnsInOutput() {
        // Sort column 1 < outputColumnCount=3 → no stripping
        String sql = PlanFragmenter.buildTopKCoordinatorSql(
            new int[] { 1 }, new boolean[] { true }, 10, 3
        );
        assertEquals("SELECT * FROM __exchange_input__ ORDER BY \"col_1\" ASC LIMIT 10", sql);
    }

    public void testAddColumnsToSelectSingleColumn() {
        String sql = "SELECT \"a\" FROM \"t\" ORDER BY \"b\" LIMIT 10";
        String result = PlanFragmenter.addColumnsToSelect(sql, List.of("b"));
        assertEquals("SELECT \"a\", \"b\" FROM \"t\" ORDER BY \"b\" LIMIT 10", result);
    }

    public void testAddColumnsToSelectMultiLine() {
        String sql = "SELECT \"a\"\nFROM \"t\"\nORDER BY \"b\"\nLIMIT 10";
        String result = PlanFragmenter.addColumnsToSelect(sql, List.of("b"));
        assertEquals("SELECT \"a\", \"b\"\nFROM \"t\"\nORDER BY \"b\"\nLIMIT 10", result);
    }

    public void testAddColumnsToSelectNoFromReturnsOriginal() {
        String sql = "INVALID SQL WITHOUT FROM";
        assertEquals(sql, PlanFragmenter.addColumnsToSelect(sql, List.of("col")));
    }

    public void testBuildTwoPhaseGroupByCoordinatorSqlWithHaving() {
        String sql = PlanFragmenter.buildTwoPhaseGroupByCoordinatorSql(
            1, new SqlKind[] { SqlKind.COUNT }, null, false,
            new boolean[] { false }, null, "COUNT(*) > 100"
        );
        assertTrue("HAVING should use re-aggregated SUM", sql.contains("HAVING SUM(\"col_1\") > 100"));
        assertTrue(sql.contains("GROUP BY"));
    }

    public void testBuildIntermediateGroupBySqlWithPassthrough() {
        Sort sort = makeSortWithLimit(10);
        String sql = PlanFragmenter.buildIntermediateGroupBySql(
            3, new SqlKind[] { SqlKind.ANY_VALUE, SqlKind.COUNT }, sort, false,
            new boolean[] { false, false }, new boolean[] { true, false }, null
        );
        assertTrue(sql.contains("GROUP BY"));
        assertTrue(sql.contains("LIMIT 10"));
        assertFalse("Passthrough agg should be skipped", sql.contains("MIN("));
    }

    public void testBuildIntermediateGroupBySqlWithHaving() {
        Sort sort = makeSortWithLimit(25);
        String sql = PlanFragmenter.buildIntermediateGroupBySql(
            1, new SqlKind[] { SqlKind.COUNT }, sort, false,
            new boolean[] { false }, null, "COUNT(*) > 100000"
        );
        assertTrue("HAVING should use re-aggregated SUM", sql.contains("HAVING SUM(\"col_1\") > 100000"));
        assertTrue(sql.contains("LIMIT 25"));
    }

    public void testBuildIntermediateGroupBySqlWithAvgWrapsSubquery() {
        Sort sort = makeSortWithLimit(10);
        String sql = PlanFragmenter.buildIntermediateGroupBySql(
            1, new SqlKind[] { SqlKind.AVG, SqlKind.COUNT }, sort, true,
            new boolean[] { false, false }, null, null
        );
        assertTrue("AVG + ORDER BY should wrap in subquery", sql.startsWith("SELECT * FROM (SELECT"));
        assertTrue(sql.contains("CAST(SUM("));
    }

    // ==== HAVING extraction/stripping tests ====

    public void testExtractHavingClause() {
        String having = PlanFragmenter.extractHavingClause(
            "SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 100 ORDER BY 2 DESC LIMIT 25"
        );
        assertEquals("COUNT(*) > 100", having);
    }

    public void testExtractHavingClauseNoHaving() {
        assertNull(PlanFragmenter.extractHavingClause(
            "SELECT a, COUNT(*) FROM t GROUP BY a ORDER BY 2 DESC LIMIT 25"
        ));
    }

    public void testExtractHavingClauseAtEnd() {
        String having = PlanFragmenter.extractHavingClause(
            "SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 100"
        );
        assertEquals("COUNT(*) > 100", having);
    }

    public void testStripHavingRemovesClause() {
        String result = PlanFragmenter.stripHaving(
            "SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 100 ORDER BY 2 DESC LIMIT 25"
        );
        assertFalse(result.toUpperCase().contains("HAVING"));
        assertTrue(result.contains("ORDER BY"));
        assertTrue(result.contains("LIMIT 25"));
    }

    public void testStripHavingNoHavingUnchanged() {
        String sql = "SELECT a, COUNT(*) FROM t GROUP BY a ORDER BY 2 DESC";
        assertEquals(sql, PlanFragmenter.stripHaving(sql));
    }

    public void testRewriteHavingCountStar() {
        String result = PlanFragmenter.rewriteHavingForReAggregation(
            "COUNT(*) > 100000", 1, new SqlKind[] { SqlKind.COUNT }, false,
            new boolean[] { false }, null
        );
        assertEquals("SUM(\"col_1\") > 100000", result);
    }

    public void testRewriteHavingWithAvgAndCount() {
        // aggKinds=[AVG, COUNT], AVG takes 2 cols → COUNT is at col_3
        String result = PlanFragmenter.rewriteHavingForReAggregation(
            "COUNT(*) > 100000", 1, new SqlKind[] { SqlKind.AVG, SqlKind.COUNT }, true,
            new boolean[] { false, false }, null
        );
        assertEquals("SUM(\"col_3\") > 100000", result);
    }

    public void testStripHavingAtEnd() {
        String result = PlanFragmenter.stripHaving(
            "SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 100"
        );
        assertEquals("SELECT a, COUNT(*) FROM t GROUP BY a", result);
    }

    public void testStripOffsetRemovesOffset() {
        String result = PlanFragmenter.stripOffset(
            "SELECT a, COUNT(*) FROM t GROUP BY a ORDER BY 2 DESC LIMIT 10 OFFSET 1000"
        );
        assertEquals("SELECT a, COUNT(*) FROM t GROUP BY a ORDER BY 2 DESC LIMIT 10", result);
    }

    public void testStripOffsetNoOffset() {
        String sql = "SELECT a FROM t ORDER BY a LIMIT 10";
        assertEquals(sql, PlanFragmenter.stripOffset(sql));
    }

    public void testAdjustLimitChangesValue() {
        String result = PlanFragmenter.adjustLimit(
            "SELECT a FROM t GROUP BY a ORDER BY 2 DESC LIMIT 10", 110
        );
        assertEquals("SELECT a FROM t GROUP BY a ORDER BY 2 DESC LIMIT 110", result);
    }

    public void testAdjustLimitNoLimitUnchanged() {
        String sql = "SELECT a FROM t GROUP BY a";
        assertEquals(sql, PlanFragmenter.adjustLimit(sql, 100));
    }

    // ==== SQL rewriting tests ====

    public void testDecomposeAvgSimple() {
        String result = PlanFragmenter.decomposeAvg("SELECT AVG(x) FROM t");
        assertEquals("SELECT SUM(CAST(x AS DOUBLE)), COUNT(x) FROM t", result);
    }

    public void testDecomposeAvgMixed() {
        String result = PlanFragmenter.decomposeAvg("SELECT SUM(a), AVG(b), COUNT(*) FROM t");
        assertEquals("SELECT SUM(a), SUM(CAST(b AS DOUBLE)), COUNT(b), COUNT(*) FROM t", result);
    }

    public void testDecomposeAvgNoAvgUnchanged() {
        String sql = "SELECT SUM(x), COUNT(*) FROM t";
        assertEquals(sql, PlanFragmenter.decomposeAvg(sql));
    }

    public void testDecomposeAvgNestedParens() {
        String result = PlanFragmenter.decomposeAvg("SELECT AVG(CAST(x AS DOUBLE)) FROM t");
        assertEquals("SELECT SUM(CAST(CAST(x AS DOUBLE) AS DOUBLE)), COUNT(CAST(x AS DOUBLE)) FROM t", result);
    }

    public void testDecomposeAvgSkipsPartOfIdentifier() {
        String sql = "SELECT XAVG(x) FROM t";
        assertEquals(sql, PlanFragmenter.decomposeAvg(sql));
    }

    public void testStripOrderByLimitOffset() {
        assertEquals("SELECT a, COUNT(*) FROM t GROUP BY a",
            PlanFragmenter.stripOrderByLimitOffset("SELECT a, COUNT(*) FROM t GROUP BY a ORDER BY 2 DESC LIMIT 10"));
    }

    public void testStripLimitOnly() {
        assertEquals("SELECT a, COUNT(*) FROM t GROUP BY a",
            PlanFragmenter.stripOrderByLimitOffset("SELECT a, COUNT(*) FROM t GROUP BY a LIMIT 10"));
    }

    public void testStripNoClause() {
        String sql = "SELECT a, COUNT(*) FROM t GROUP BY a";
        assertEquals(sql, PlanFragmenter.stripOrderByLimitOffset(sql));
    }

    public void testDecomposeGlobalDistinctToRawValues() {
        String result = PlanFragmenter.decomposeGlobalDistinctToRawValues(
            "SELECT COUNT(DISTINCT userid) FROM t"
        );
        assertEquals("SELECT DISTINCT userid FROM t", result);
    }

    public void testDecomposeGlobalDistinctToRawValuesMixed() {
        String result = PlanFragmenter.decomposeGlobalDistinctToRawValues(
            "SELECT SUM(a), COUNT(DISTINCT b), COUNT(*) FROM t"
        );
        assertEquals("SELECT DISTINCT SUM(a), b, COUNT(*) FROM t", result);
    }

    public void testDecomposeGlobalDistinctSkipsPartOfIdentifier() {
        String sql = "SELECT XCOUNT(DISTINCT x) FROM t";
        assertEquals(sql, PlanFragmenter.decomposeGlobalDistinctToRawValues(sql));
    }

    public void testDecomposeDistinctToDedupGroupBy() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0), List.of(makeAggCall(SqlStdOperatorTable.COUNT, true)));
        String result = PlanFragmenter.decomposeDistinctToDedup(
            "SELECT region, COUNT(DISTINCT userid) FROM t GROUP BY region", agg
        );
        assertEquals("SELECT region, userid FROM t GROUP BY region, userid", result);
    }

    public void testExtractIsDistinct() {
        AggregateCall regular = makeAggCall(SqlStdOperatorTable.COUNT, false);
        AggregateCall distinct = makeAggCall(SqlStdOperatorTable.COUNT, true);
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(regular, distinct));
        boolean[] result = PlanFragmenter.extractIsDistinct(agg);
        assertEquals(2, result.length);
        assertFalse(result[0]);
        assertTrue(result[1]);
    }

    public void testExtractIsPassthroughAnyValueOnGroupKey() {
        AggregateCall anyValueCall = makeAggCallWithArgs(SqlStdOperatorTable.ANY_VALUE, false, List.of(1));
        AggregateCall countCall = makeAggCall(SqlStdOperatorTable.COUNT, false);
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0, 1, 2), List.of(anyValueCall, countCall));
        boolean[] result = PlanFragmenter.extractIsPassthrough(agg);
        assertEquals(2, result.length);
        assertTrue(result[0]);
        assertFalse(result[1]);
    }

    public void testExtractIsPassthroughMinOnGroupKey() {
        AggregateCall minCall = makeAggCallWithArgs(SqlStdOperatorTable.MIN, false, List.of(1));
        AggregateCall countCall = makeAggCall(SqlStdOperatorTable.COUNT, false);
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0, 1, 2), List.of(minCall, countCall));
        boolean[] result = PlanFragmenter.extractIsPassthrough(agg);
        assertEquals(2, result.length);
        assertTrue(result[0]);
        assertFalse(result[1]);
    }

    public void testExtractIsPassthroughMinNotOnGroupKey() {
        AggregateCall minCall = makeAggCallWithArgs(SqlStdOperatorTable.MIN, false, List.of(5));
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0, 1), List.of(minCall));
        boolean[] result = PlanFragmenter.extractIsPassthrough(agg);
        assertEquals(1, result.length);
        assertFalse(result[0]);
    }

    public void testExtractIsPassthroughMultipleAnyValue() {
        AggregateCall av1 = makeAggCallWithArgs(SqlStdOperatorTable.ANY_VALUE, false, List.of(1));
        AggregateCall av2 = makeAggCallWithArgs(SqlStdOperatorTable.ANY_VALUE, false, List.of(2));
        AggregateCall av3 = makeAggCallWithArgs(SqlStdOperatorTable.ANY_VALUE, false, List.of(3));
        AggregateCall countCall = makeAggCall(SqlStdOperatorTable.COUNT, false);
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0, 1, 2, 3), List.of(av1, av2, av3, countCall));
        boolean[] result = PlanFragmenter.extractIsPassthrough(agg);
        assertEquals(4, result.length);
        assertTrue(result[0]);
        assertTrue(result[1]);
        assertTrue(result[2]);
        assertFalse(result[3]);
    }

    public void testRemapSortPositionNoPassthrough() {
        assertEquals(3, PlanFragmenter.remapSortPosition(3, 2, new boolean[]{false, false}));
    }

    public void testRemapSortPositionGroupKeyUnchanged() {
        assertEquals(1, PlanFragmenter.remapSortPosition(1, 3, new boolean[]{true, false}));
    }

    public void testRemapSortPositionSkipsPassthrough() {
        // groupCount=3, aggs=[passthrough, COUNT], sort on COUNT (position 4 in Calcite)
        // After skipping passthrough, COUNT is at position 3
        assertEquals(3, PlanFragmenter.remapSortPosition(4, 3, new boolean[]{true, false}));
    }

    public void testRemapSortPositionMultiplePassthrough() {
        // groupCount=4, aggs=[pt, pt, pt, COUNT], sort on COUNT (position 7 in Calcite)
        // After skipping 3 passthroughs, COUNT is at position 4
        assertEquals(4, PlanFragmenter.remapSortPosition(7, 4, new boolean[]{true, true, true, false}));
    }

    public void testRemapSortPositionNullPassthrough() {
        assertEquals(5, PlanFragmenter.remapSortPosition(5, 2, null));
    }

    // ==== RelNode extraction tests ====

    public void testHasDistinctFalse() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(makeAggCall(SqlStdOperatorTable.COUNT, false)));
        assertFalse(PlanFragmenter.hasDistinct(agg));
    }

    public void testHasDistinctTrue() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(makeAggCall(SqlStdOperatorTable.COUNT, true)));
        assertTrue(PlanFragmenter.hasDistinct(agg));
    }

    public void testHasAvgFalse() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(makeAggCall(SqlStdOperatorTable.COUNT, false)));
        assertFalse(PlanFragmenter.hasAvg(agg));
    }

    public void testHasAvgTrue() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(makeAggCall(SqlStdOperatorTable.AVG, false)));
        assertTrue(PlanFragmenter.hasAvg(agg));
    }

    public void testExtractAggKinds() {
        AggregateCall sumCall = makeAggCall(SqlStdOperatorTable.SUM, false);
        AggregateCall minCall = makeAggCall(SqlStdOperatorTable.MIN, false);
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(sumCall, minCall));
        SqlKind[] kinds = PlanFragmenter.extractAggKinds(agg);
        assertEquals(2, kinds.length);
        assertEquals(SqlKind.SUM, kinds[0]);
        assertEquals(SqlKind.MIN, kinds[1]);
    }

    public void testExtractSortColumns() {
        Sort sort = makeSort(true, true);
        int[] cols = PlanFragmenter.extractSortColumns(sort);
        assertEquals(1, cols.length);
        assertEquals(0, cols[0]);
    }

    public void testExtractSortDirections() {
        Sort sort = makeSort(true, true);
        boolean[] dirs = PlanFragmenter.extractSortDirections(sort);
        assertEquals(1, dirs.length);
        assertTrue(dirs[0]);
    }

    public void testExtractLimitFromRexLiteral() {
        Sort sort = makeSortWithLimit(42);
        assertEquals(42, PlanFragmenter.extractLimit(sort));
    }

    public void testExtractLimitFromNonLiteralReturnsZero() {
        Sort sort = makeSort(true, true);
        assertEquals(0, PlanFragmenter.extractLimit(sort));
    }

    public void testExtractOffsetFromNull() {
        Sort sort = makeSort(true, true);
        assertEquals(0, PlanFragmenter.extractOffset(sort));
    }

    // ==== PlanVisitor tests ====

    public void testPlanVisitorFindsAggregate() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(makeAggCall(SqlStdOperatorTable.SUM, false)));
        PlanFragmenter.PlanVisitor visitor = new PlanFragmenter.PlanVisitor();
        visitor.go(agg);
        assertNotNull(visitor.aggregate);
        assertNull(visitor.sort);
    }

    public void testPlanVisitorFindsSort() {
        Sort sort = makeSort(true, true);
        PlanFragmenter.PlanVisitor visitor = new PlanFragmenter.PlanVisitor();
        visitor.go(sort);
        assertNotNull(visitor.sort);
        assertNull(visitor.aggregate);
    }

    public void testPlanVisitorFindsBoth() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0), List.of(makeAggCall(SqlStdOperatorTable.COUNT, false)));
        RelNode wrapper = mockNodeWithInput(agg);
        Sort sort = makeSortWithInput(wrapper, true);

        PlanFragmenter.PlanVisitor visitor = new PlanFragmenter.PlanVisitor();
        visitor.go(sort);
        assertNotNull(visitor.sort);
        assertNotNull(visitor.aggregate);
    }

    public void testPlanVisitorCapturesSortWithLimitOnly() {
        Sort sort = makeSort(false, true);
        PlanFragmenter.PlanVisitor visitor = new PlanFragmenter.PlanVisitor();
        visitor.go(sort);
        assertNotNull("Sort with LIMIT (no ORDER BY) should be captured", visitor.sort);
    }

    // Note: Sort with empty collation AND no fetch cannot be created (Calcite rejects "trivial sort")

    // ---- helpers (same pattern as QueryAnalyzerTests) ----

    private Aggregate mockAggregate(ImmutableBitSet groupSet, List<AggregateCall> aggCalls) {
        Aggregate agg = mock(Aggregate.class);
        when(agg.getGroupSet()).thenReturn(groupSet);
        when(agg.getAggCallList()).thenReturn(aggCalls);
        when(agg.getInputs()).thenReturn(List.of());
        return agg;
    }

    @SuppressWarnings("deprecation")
    private AggregateCall makeAggCall(SqlAggFunction aggFunction, boolean distinct) {
        return new AggregateCall(aggFunction, distinct, List.of(), BIGINT_TYPE, null);
    }

    @SuppressWarnings("deprecation")
    private AggregateCall makeAggCallWithArgs(SqlAggFunction aggFunction, boolean distinct, List<Integer> args) {
        return new AggregateCall(aggFunction, distinct, args, BIGINT_TYPE, null);
    }

    private Sort makeSort(boolean hasCollation, boolean hasFetch) {
        RelCollation collation;
        if (hasCollation) {
            RelFieldCollation fieldCollation = new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING);
            collation = RelCollations.of(fieldCollation);
        } else {
            collation = RelCollations.EMPTY;
        }
        RexNode fetchNode = hasFetch ? mock(RexNode.class) : null;
        RelOptCluster cluster = mock(RelOptCluster.class);
        RelTraitSet traitSet = RelTraitSet.createEmpty().plus(collation);
        RelNode input = mockSimpleNode();
        RelDataType rowType = mock(RelDataType.class);
        when(rowType.getFieldNames()).thenReturn(List.of("col0"));
        when(rowType.getFieldCount()).thenReturn(1);
        when(input.getRowType()).thenReturn(rowType);
        return new StubSort(cluster, traitSet, input, collation, null, fetchNode);
    }

    private Sort makeSortWithInput(RelNode input, boolean hasFetch) {
        return makeSortWithInput(input, hasFetch, 10);
    }

    private Sort makeSortWithInput(RelNode input, boolean hasFetch, int limitValue) {
        RelFieldCollation fieldCollation = new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING);
        RelCollation collation = RelCollations.of(fieldCollation);
        RexNode fetchNode = null;
        if (hasFetch) {
            RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
            org.apache.calcite.rex.RexBuilder rexBuilder = new org.apache.calcite.rex.RexBuilder(typeFactory);
            fetchNode = rexBuilder.makeExactLiteral(
                BigDecimal.valueOf(limitValue), typeFactory.createSqlType(SqlTypeName.INTEGER)
            );
        }
        RelOptCluster cluster = mock(RelOptCluster.class);
        RelTraitSet traitSet = RelTraitSet.createEmpty().plus(collation);
        RelDataType rowType = mock(RelDataType.class);
        when(rowType.getFieldNames()).thenReturn(List.of("col0", "col1"));
        when(rowType.getFieldCount()).thenReturn(2);
        when(input.getRowType()).thenReturn(rowType);
        when(input.getInputs()).thenReturn(List.of());
        return new StubSort(cluster, traitSet, input, collation, null, fetchNode);
    }

    private Sort makeSortWithInputAndOffset(RelNode input, int limitValue, int offsetValue) {
        RelFieldCollation fieldCollation = new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING);
        RelCollation collation = RelCollations.of(fieldCollation);
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        org.apache.calcite.rex.RexBuilder rexBuilder = new org.apache.calcite.rex.RexBuilder(typeFactory);
        RexNode fetchNode = rexBuilder.makeExactLiteral(
            BigDecimal.valueOf(limitValue), typeFactory.createSqlType(SqlTypeName.INTEGER)
        );
        RexNode offsetNode = rexBuilder.makeExactLiteral(
            BigDecimal.valueOf(offsetValue), typeFactory.createSqlType(SqlTypeName.INTEGER)
        );
        RelOptCluster cluster = mock(RelOptCluster.class);
        RelTraitSet traitSet = RelTraitSet.createEmpty().plus(collation);
        RelDataType rowType = mock(RelDataType.class);
        when(rowType.getFieldNames()).thenReturn(List.of("col0", "col1"));
        when(rowType.getFieldCount()).thenReturn(2);
        when(input.getRowType()).thenReturn(rowType);
        when(input.getInputs()).thenReturn(List.of());
        return new StubSort(cluster, traitSet, input, collation, offsetNode, fetchNode);
    }

    private Sort makeSortWithLimit(int limitValue) {
        RelFieldCollation fieldCollation = new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING);
        RelCollation collation = RelCollations.of(fieldCollation);
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        org.apache.calcite.rex.RexBuilder rexBuilder = new org.apache.calcite.rex.RexBuilder(typeFactory);
        RexLiteral fetchLiteral = rexBuilder.makeExactLiteral(
            BigDecimal.valueOf(limitValue),
            typeFactory.createSqlType(SqlTypeName.INTEGER)
        );
        RelOptCluster cluster = mock(RelOptCluster.class);
        RelTraitSet traitSet = RelTraitSet.createEmpty().plus(collation);
        RelNode input = mockSimpleNode();
        RelDataType rowType = mock(RelDataType.class);
        when(rowType.getFieldNames()).thenReturn(List.of("col0"));
        when(rowType.getFieldCount()).thenReturn(1);
        when(input.getRowType()).thenReturn(rowType);
        return new StubSort(cluster, traitSet, input, collation, null, fetchLiteral);
    }

    private RelNode mockSimpleNode() {
        RelNode node = mock(RelNode.class);
        when(node.getInputs()).thenReturn(List.of());
        return node;
    }

    private RelNode mockNodeWithInput(RelNode input) {
        RelNode node = mock(RelNode.class);
        when(node.getInputs()).thenReturn(List.of(input));
        RelDataType rowType = mock(RelDataType.class);
        when(rowType.getFieldNames()).thenReturn(List.of("col0"));
        when(rowType.getFieldCount()).thenReturn(1);
        when(node.getRowType()).thenReturn(rowType);
        doAnswer(invocation -> {
            org.apache.calcite.rel.RelVisitor visitor = invocation.getArgument(0);
            visitor.visit(input, 0, node);
            return null;
        }).when(node).childrenAccept(any(org.apache.calcite.rel.RelVisitor.class));
        return node;
    }

    private static class StubSort extends Sort {
        StubSort(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            RelCollation collation,
            RexNode offset,
            RexNode fetch
        ) {
            super(cluster, traitSet, List.of(), input, collation, offset, fetch);
        }

        @Override
        public Sort copy(RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
            return new StubSort(getCluster(), traitSet, input, collation, offset, fetch);
        }
    }
}

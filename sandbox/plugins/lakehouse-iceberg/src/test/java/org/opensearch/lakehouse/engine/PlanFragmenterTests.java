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
        FragmentedPlan plan = PlanFragmenter.fragment(scan, "SELECT * FROM t");

        assertFalse(plan.isSingleNode());
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
        FragmentedPlan plan = PlanFragmenter.fragment(agg, "SELECT COUNT(*) FROM t");

        assertFalse(plan.isSingleNode());
        assertEquals(2, plan.getStageCount());
        assertEquals(ExchangeType.GATHER, plan.getLeafStage().getOutputExchange());
        assertNull(plan.getLeafStage().getHashColumns());
    }

    public void testGlobalAvgDecomposesInLeafSql() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(makeAggCall(SqlStdOperatorTable.AVG, false)));
        FragmentedPlan plan = PlanFragmenter.fragment(agg, "SELECT AVG(x) FROM t");

        assertFalse(plan.isSingleNode());
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
        FragmentedPlan plan = PlanFragmenter.fragment(agg, "SELECT SUM(a), AVG(b), COUNT(*) FROM t");

        assertFalse(plan.isSingleNode());
        String leafSql = plan.getLeafStage().getSql();
        assertTrue(leafSql.contains("SUM(CAST("));
        assertTrue(leafSql.contains("COUNT("));
    }

    public void testGlobalMinMax() {
        AggregateCall minCall = makeAggCall(SqlStdOperatorTable.MIN, false);
        AggregateCall maxCall = makeAggCall(SqlStdOperatorTable.MAX, false);
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(minCall, maxCall));
        FragmentedPlan plan = PlanFragmenter.fragment(agg, "SELECT MIN(x), MAX(y) FROM t");

        assertFalse(plan.isSingleNode());
        String coordSql = plan.getFinalStage().getSql();
        assertTrue(coordSql.contains("MIN("));
        assertTrue(coordSql.contains("MAX("));
    }

    public void testTopKMergeWithSortAndLimit() {
        Sort sort = makeSort(true, true);
        FragmentedPlan plan = PlanFragmenter.fragment(sort, "SELECT * FROM t ORDER BY col0 ASC LIMIT 10");

        assertFalse(plan.isSingleNode());
        assertEquals(2, plan.getStageCount());
        assertEquals(ExchangeType.GATHER, plan.getLeafStage().getOutputExchange());

        String coordSql = plan.getFinalStage().getSql();
        assertTrue(coordSql.contains("ORDER BY"));
    }

    public void testSortWithoutLimitReturnsSingleNode() {
        Sort sort = makeSort(true, false);
        FragmentedPlan plan = PlanFragmenter.fragment(sort, "SELECT * FROM t ORDER BY col0");
        assertTrue(plan.isSingleNode());
    }

    public void testGroupByNoLimitReturnsTwoPhaseGather() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0), List.of(makeAggCall(SqlStdOperatorTable.COUNT, false)));
        FragmentedPlan plan = PlanFragmenter.fragment(agg, "SELECT region, COUNT(*) FROM t GROUP BY region");

        assertFalse(plan.isSingleNode());
        assertEquals(2, plan.getStageCount());
        assertEquals(ExchangeType.GATHER, plan.getLeafStage().getOutputExchange());
        assertNull(plan.getLeafStage().getHashColumns());

        String coordSql = plan.getFinalStage().getSql();
        assertTrue(coordSql.contains("GROUP BY"));
        assertTrue(coordSql.contains("SUM("));
    }

    public void testGroupByWithLimitReturnsThreeStageHash() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0), List.of(makeAggCall(SqlStdOperatorTable.COUNT, false)));
        RelNode wrapper = mockNodeWithInput(agg);
        Sort sort = makeSortWithInput(wrapper, true);

        FragmentedPlan plan = PlanFragmenter.fragment(sort,
            "SELECT region, COUNT(*) FROM t GROUP BY region ORDER BY COUNT(*) DESC LIMIT 10");

        assertFalse(plan.isSingleNode());
        assertEquals(3, plan.getStageCount());

        // Stage 0: leaf with HASH exchange on group keys
        PlanFragment leaf = plan.getStages().get(0);
        assertTrue(leaf.isLeaf());
        assertEquals(ExchangeType.HASH, leaf.getOutputExchange());
        assertNotNull(leaf.getHashColumns());
        assertArrayEquals(new int[] { 0 }, leaf.getHashColumns());
        // Worker SQL should have ORDER BY/LIMIT stripped
        assertFalse(leaf.getSql().toUpperCase().contains("ORDER BY"));
        assertFalse(leaf.getSql().toUpperCase().contains("LIMIT"));

        // Stage 1: intermediate with GATHER exchange
        PlanFragment mid = plan.getStages().get(1);
        assertFalse(mid.isLeaf());
        assertEquals(ExchangeType.GATHER, mid.getOutputExchange());
        assertTrue(mid.getSql().contains("GROUP BY"));
        assertTrue(mid.getSql().contains("LIMIT 10"));

        // Stage 2: final CONCAT
        PlanFragment fin = plan.getStages().get(2);
        assertEquals(ExchangeType.NONE, fin.getOutputExchange());
        assertEquals("SELECT * FROM __exchange_input__", fin.getSql());
    }

    public void testGroupByWithLimitAndAvgReturnsThreeStageHash() {
        AggregateCall avgCall = makeAggCall(SqlStdOperatorTable.AVG, false);
        AggregateCall countCall = makeAggCall(SqlStdOperatorTable.COUNT, false);
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0), List.of(avgCall, countCall));
        RelNode wrapper = mockNodeWithInput(agg);
        Sort sort = makeSortWithInput(wrapper, true);

        FragmentedPlan plan = PlanFragmenter.fragment(sort,
            "SELECT region, AVG(price), COUNT(*) FROM t GROUP BY region ORDER BY 2 DESC LIMIT 5");

        assertFalse(plan.isSingleNode());
        assertEquals(3, plan.getStageCount());

        // Leaf SQL should decompose AVG
        String leafSql = plan.getLeafStage().getSql();
        assertTrue(leafSql.contains("SUM(CAST("));
        assertFalse(leafSql.contains("AVG("));

        // Intermediate SQL should recombine AVG
        String midSql = plan.getStages().get(1).getSql();
        assertTrue(midSql.contains("CAST(SUM("));
        assertTrue(midSql.contains("/ SUM("));
    }

    public void testCountDistinctReturnsSingleNode() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(makeAggCall(SqlStdOperatorTable.COUNT, true)));
        FragmentedPlan plan = PlanFragmenter.fragment(agg, "SELECT COUNT(DISTINCT x) FROM t");
        assertTrue(plan.isSingleNode());
    }

    public void testGroupByDistinctReturnsSingleNode() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0), List.of(makeAggCall(SqlStdOperatorTable.SUM, true)));
        FragmentedPlan plan = PlanFragmenter.fragment(agg, "SELECT region, SUM(DISTINCT x) FROM t GROUP BY region");
        assertTrue(plan.isSingleNode());
    }

    public void testGroupByAvgNoLimitReturnsSingleNode() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0), List.of(makeAggCall(SqlStdOperatorTable.AVG, false)));
        FragmentedPlan plan = PlanFragmenter.fragment(agg, "SELECT region, AVG(x) FROM t GROUP BY region");
        assertTrue(plan.isSingleNode());
    }

    public void testNestedAggregateDetected() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(makeAggCall(SqlStdOperatorTable.SUM, false)));
        RelNode project = mockNodeWithInput(agg);
        FragmentedPlan plan = PlanFragmenter.fragment(project, "SELECT SUM(x) FROM t");
        assertFalse(plan.isSingleNode());
        assertEquals(2, plan.getStageCount());
    }

    public void testNestedSortDetected() {
        Sort sort = makeSort(true, true);
        RelNode project = mockNodeWithInput(sort);
        FragmentedPlan plan = PlanFragmenter.fragment(project, "SELECT * FROM t ORDER BY col0 LIMIT 10");
        assertFalse(plan.isSingleNode());
        assertEquals(2, plan.getStageCount());
    }

    // ==== SQL builder tests ====

    public void testBuildGlobalMergeCoordinatorSqlSumCount() {
        String sql = PlanFragmenter.buildGlobalMergeCoordinatorSql(
            new SqlKind[] { SqlKind.SUM, SqlKind.COUNT }, false
        );
        assertEquals("SELECT SUM(\"col_0\"), SUM(\"col_1\") FROM __exchange_input__", sql);
    }

    public void testBuildGlobalMergeCoordinatorSqlMinMax() {
        String sql = PlanFragmenter.buildGlobalMergeCoordinatorSql(
            new SqlKind[] { SqlKind.MIN, SqlKind.MAX }, false
        );
        assertEquals("SELECT MIN(\"col_0\"), MAX(\"col_1\") FROM __exchange_input__", sql);
    }

    public void testBuildGlobalMergeCoordinatorSqlWithAvg() {
        String sql = PlanFragmenter.buildGlobalMergeCoordinatorSql(
            new SqlKind[] { SqlKind.SUM, SqlKind.AVG }, true
        );
        // SUM takes col_0, AVG takes col_1 (SUM) and col_2 (COUNT)
        assertEquals(
            "SELECT SUM(\"col_0\"), CAST(SUM(\"col_1\") AS DOUBLE) / SUM(\"col_2\") FROM __exchange_input__",
            sql
        );
    }

    public void testBuildGlobalMergeCoordinatorSqlNullKinds() {
        String sql = PlanFragmenter.buildGlobalMergeCoordinatorSql(null, false);
        assertEquals("SELECT * FROM __exchange_input__", sql);
    }

    public void testBuildGlobalMergeCoordinatorSqlEmptyKinds() {
        String sql = PlanFragmenter.buildGlobalMergeCoordinatorSql(new SqlKind[] {}, false);
        assertEquals("SELECT * FROM __exchange_input__", sql);
    }

    public void testBuildTwoPhaseGroupByCoordinatorSql() {
        String sql = PlanFragmenter.buildTwoPhaseGroupByCoordinatorSql(
            1, new SqlKind[] { SqlKind.COUNT, SqlKind.SUM }, null
        );
        assertEquals(
            "SELECT \"col_0\", SUM(\"col_1\"), SUM(\"col_2\") FROM __exchange_input__ GROUP BY \"col_0\"",
            sql
        );
    }

    public void testBuildTwoPhaseGroupByCoordinatorSqlWithSort() {
        Sort sort = makeSort(true, false);
        String sql = PlanFragmenter.buildTwoPhaseGroupByCoordinatorSql(
            1, new SqlKind[] { SqlKind.COUNT }, sort
        );
        assertTrue(sql.contains("ORDER BY 1 ASC"));
    }

    public void testBuildTwoPhaseGroupByCoordinatorSqlMinMax() {
        String sql = PlanFragmenter.buildTwoPhaseGroupByCoordinatorSql(
            2, new SqlKind[] { SqlKind.MIN, SqlKind.MAX }, null
        );
        assertEquals(
            "SELECT \"col_0\", \"col_1\", MIN(\"col_2\"), MAX(\"col_3\") FROM __exchange_input__ GROUP BY \"col_0\", \"col_1\"",
            sql
        );
    }

    public void testBuildIntermediateGroupBySql() {
        Sort sort = makeSortWithLimit(10);
        String sql = PlanFragmenter.buildIntermediateGroupBySql(
            1, new SqlKind[] { SqlKind.COUNT, SqlKind.SUM }, sort, false
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
            1, new SqlKind[] { SqlKind.AVG, SqlKind.COUNT }, sort, true
        );
        // AVG takes 2 worker cols (SUM+COUNT)
        assertTrue(sql.contains("CAST(SUM(\"col_1\") AS DOUBLE) / SUM(\"col_2\")"));
        // COUNT is next col after the 2 AVG cols
        assertTrue(sql.contains("SUM(\"col_3\")"));
        assertTrue(sql.contains("LIMIT 5"));
    }

    public void testBuildTopKCoordinatorSql() {
        String sql = PlanFragmenter.buildTopKCoordinatorSql(new int[] { 2 }, new boolean[] { false }, 20);
        assertEquals("SELECT * FROM __exchange_input__ ORDER BY \"col_2\" DESC LIMIT 20", sql);
    }

    public void testBuildTopKCoordinatorSqlMultipleSortColumns() {
        String sql = PlanFragmenter.buildTopKCoordinatorSql(
            new int[] { 0, 3 }, new boolean[] { true, false }, 50
        );
        assertEquals("SELECT * FROM __exchange_input__ ORDER BY \"col_0\" ASC, \"col_3\" DESC LIMIT 50", sql);
    }

    public void testBuildTopKCoordinatorSqlNoLimit() {
        String sql = PlanFragmenter.buildTopKCoordinatorSql(new int[] { 1 }, new boolean[] { true }, 0);
        assertEquals("SELECT * FROM __exchange_input__ ORDER BY \"col_1\" ASC", sql);
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

    public void testPlanVisitorSkipsSortWithEmptyCollation() {
        Sort sort = makeSort(false, true);
        PlanFragmenter.PlanVisitor visitor = new PlanFragmenter.PlanVisitor();
        visitor.go(sort);
        assertNull(visitor.sort);
    }

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

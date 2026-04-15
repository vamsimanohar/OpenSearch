/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.lakehouse.distributed.merge.MergeStrategy;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigDecimal;
import java.util.List;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryAnalyzerTests extends OpenSearchTestCase {

    private static final RelDataType BIGINT_TYPE = new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.BIGINT);

    public void testGlobalAggregateWithCount() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(makeAggCall(SqlStdOperatorTable.COUNT, false)));
        assertEquals(MergeStrategy.GLOBAL_MERGE, QueryAnalyzer.analyze(agg));
    }

    public void testGlobalAggregateWithSum() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(makeAggCall(SqlStdOperatorTable.SUM, false)));
        assertEquals(MergeStrategy.GLOBAL_MERGE, QueryAnalyzer.analyze(agg));
    }

    public void testGlobalAggregateWithMin() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(makeAggCall(SqlStdOperatorTable.MIN, false)));
        assertEquals(MergeStrategy.GLOBAL_MERGE, QueryAnalyzer.analyze(agg));
    }

    public void testGlobalAggregateWithMax() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(makeAggCall(SqlStdOperatorTable.MAX, false)));
        assertEquals(MergeStrategy.GLOBAL_MERGE, QueryAnalyzer.analyze(agg));
    }

    public void testGroupByWithSimpleAggsReturnsTwoPhaseGroupBy() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0, 1), List.of(makeAggCall(SqlStdOperatorTable.COUNT, false)));
        assertEquals(MergeStrategy.TWO_PHASE_GROUP_BY, QueryAnalyzer.analyze(agg));
    }

    public void testGroupByWithAvgReturnsTwoPhaseGroupBy() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0), List.of(makeAggCall(SqlStdOperatorTable.AVG, false)));
        assertEquals(MergeStrategy.TWO_PHASE_GROUP_BY, QueryAnalyzer.analyze(agg));
    }

    public void testGroupByWithCountDistinctReturnsDistinctExpand() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0), List.of(makeAggCall(SqlStdOperatorTable.COUNT, true)));
        assertEquals(MergeStrategy.DISTINCT_EXPAND, QueryAnalyzer.analyze(agg));
    }

    public void testCountDistinctReturnsDistinctExpand() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(makeAggCall(SqlStdOperatorTable.COUNT, true)));
        assertEquals(MergeStrategy.DISTINCT_EXPAND, QueryAnalyzer.analyze(agg));
    }

    public void testGroupByWithMixedCountDistinctReturnsMixedDistinct() {
        AggregateCall countCall = makeAggCall(SqlStdOperatorTable.COUNT, false);
        AggregateCall countDistinctCall = makeAggCall(SqlStdOperatorTable.COUNT, true);
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0), List.of(countCall, countDistinctCall));
        assertEquals(MergeStrategy.MIXED_DISTINCT, QueryAnalyzer.analyze(agg));
    }

    public void testGlobalMixedCountDistinctReturnsMixedDistinct() {
        AggregateCall sumCall = makeAggCall(SqlStdOperatorTable.SUM, false);
        AggregateCall countDistinctCall = makeAggCall(SqlStdOperatorTable.COUNT, true);
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(sumCall, countDistinctCall));
        assertEquals(MergeStrategy.MIXED_DISTINCT, QueryAnalyzer.analyze(agg));
    }

    public void testGroupByWithDistinctSumReturnsSingleNode() {
        // SUM(DISTINCT x) is not COUNT(DISTINCT), so not supported
        AggregateCall distinctSum = makeAggCall(SqlStdOperatorTable.SUM, true);
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0), List.of(distinctSum));
        assertEquals(MergeStrategy.SINGLE_NODE, QueryAnalyzer.analyze(agg));
    }

    public void testAvgReturnsGlobalMerge() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(makeAggCall(SqlStdOperatorTable.AVG, false)));
        assertEquals(MergeStrategy.GLOBAL_MERGE, QueryAnalyzer.analyze(agg));
    }

    public void testSortWithLimitReturnsTopKMerge() {
        Sort sort = makeSort(true, true);
        assertEquals(MergeStrategy.TOPK_MERGE, QueryAnalyzer.analyze(sort));
    }

    public void testSortWithoutLimitReturnsSingleNode() {
        Sort sort = makeSort(true, false);
        assertEquals(MergeStrategy.SINGLE_NODE, QueryAnalyzer.analyze(sort));
    }

    public void testSortWithLimitButSortColumnProjectedAwayReturnsSingleNode() {
        // Simulates: SELECT SearchPhrase FROM hits ORDER BY EventTime LIMIT 10
        // Sort on field index 1 (EventTime) but output only has 1 field (SearchPhrase)
        Sort sort = makeSort(true, true, 1, 1);
        assertEquals(MergeStrategy.SINGLE_NODE, QueryAnalyzer.analyze(sort));
    }

    public void testSimpleScanReturnsConcat() {
        RelNode scan = mockSimpleNode();
        assertEquals(MergeStrategy.CONCAT, QueryAnalyzer.analyze(scan));
    }

    public void testNestedAggregateIsDetected() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(makeAggCall(SqlStdOperatorTable.SUM, false)));
        RelNode project = mockNodeWithInput(agg);
        assertEquals(MergeStrategy.GLOBAL_MERGE, QueryAnalyzer.analyze(project));
    }

    public void testNestedSortIsDetected() {
        Sort sort = makeSort(true, true);
        RelNode project = mockNodeWithInput(sort);
        assertEquals(MergeStrategy.TOPK_MERGE, QueryAnalyzer.analyze(project));
    }

    public void testHasDistinctOrAvgReturnsFalseForCount() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(makeAggCall(SqlStdOperatorTable.COUNT, false)));
        assertFalse(QueryAnalyzer.hasDistinctOrAvg(agg));
    }

    public void testHasDistinctOrAvgReturnsTrueForDistinct() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(makeAggCall(SqlStdOperatorTable.COUNT, true)));
        assertTrue(QueryAnalyzer.hasDistinctOrAvg(agg));
    }

    public void testHasDistinctOrAvgReturnsTrueForAvg() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(makeAggCall(SqlStdOperatorTable.AVG, false)));
        assertTrue(QueryAnalyzer.hasDistinctOrAvg(agg));
    }

    public void testDistinctSumWithNonDistinctCountReturnsSingleNode() {
        // SUM(DISTINCT x) + COUNT(*) → not COUNT(DISTINCT), so SINGLE_NODE
        AggregateCall countCall = makeAggCall(SqlStdOperatorTable.COUNT, false);
        AggregateCall distinctSumCall = makeAggCall(SqlStdOperatorTable.SUM, true);
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(countCall, distinctSumCall));
        assertEquals(MergeStrategy.SINGLE_NODE, QueryAnalyzer.analyze(agg));
    }

    public void testHasMixedCountDistinctReturnsTrueForCountAndCountDistinct() {
        AggregateCall countCall = makeAggCall(SqlStdOperatorTable.COUNT, false);
        AggregateCall countDistinctCall = makeAggCall(SqlStdOperatorTable.COUNT, true);
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(countCall, countDistinctCall));
        assertTrue(QueryAnalyzer.hasMixedCountDistinct(agg));
    }

    public void testHasMixedCountDistinctReturnsFalseForOnlyCountDistinct() {
        AggregateCall countDistinctCall = makeAggCall(SqlStdOperatorTable.COUNT, true);
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(countDistinctCall));
        assertFalse(QueryAnalyzer.hasMixedCountDistinct(agg));
    }

    public void testHasMixedCountDistinctReturnsFalseForSumDistinct() {
        // SUM(DISTINCT) is not COUNT(DISTINCT) — should return false
        AggregateCall countCall = makeAggCall(SqlStdOperatorTable.COUNT, false);
        AggregateCall sumDistinctCall = makeAggCall(SqlStdOperatorTable.SUM, true);
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(countCall, sumDistinctCall));
        assertFalse(QueryAnalyzer.hasMixedCountDistinct(agg));
    }

    public void testEmptyAggCallListWithGroupByReturnsTwoPhaseGroupBy() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0), List.of());
        assertEquals(MergeStrategy.TWO_PHASE_GROUP_BY, QueryAnalyzer.analyze(agg));
    }

    public void testEmptyAggCallListWithNoGroupByReturnsGlobalMerge() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of());
        assertEquals(MergeStrategy.GLOBAL_MERGE, QueryAnalyzer.analyze(agg));
    }

    // --- analyzeDetailed tests ---

    public void testAnalyzeDetailedGlobalMergeIncludesAggKinds() {
        AggregateCall minCall = makeAggCall(SqlStdOperatorTable.MIN, false);
        AggregateCall maxCall = makeAggCall(SqlStdOperatorTable.MAX, false);
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(minCall, maxCall));

        QueryAnalyzer.AnalysisResult result = QueryAnalyzer.analyzeDetailed(agg);

        assertEquals(MergeStrategy.GLOBAL_MERGE, result.strategy);
        assertNotNull(result.aggKinds);
        assertEquals(2, result.aggKinds.length);
        assertEquals(SqlKind.MIN, result.aggKinds[0]);
        assertEquals(SqlKind.MAX, result.aggKinds[1]);
    }

    public void testAnalyzeDetailedTopKMergeIncludesSortAndLimit() {
        Sort sort = makeSort(true, true);

        QueryAnalyzer.AnalysisResult result = QueryAnalyzer.analyzeDetailed(sort);

        assertEquals(MergeStrategy.TOPK_MERGE, result.strategy);
        assertNotNull(result.sortColumns);
        assertEquals(1, result.sortColumns.length);
        assertEquals(0, result.sortColumns[0]);
        assertNotNull(result.sortAsc);
        assertTrue(result.sortAsc[0]);
    }

    public void testAnalyzeDetailedConcatHasNoMetadata() {
        RelNode scan = mockSimpleNode();

        QueryAnalyzer.AnalysisResult result = QueryAnalyzer.analyzeDetailed(scan);

        assertEquals(MergeStrategy.CONCAT, result.strategy);
        assertNull(result.aggKinds);
        assertNull(result.sortColumns);
    }

    public void testAnalyzeDetailedTwoPhaseGroupByHasMetadata() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0), List.of(makeAggCall(SqlStdOperatorTable.COUNT, false)));

        QueryAnalyzer.AnalysisResult result = QueryAnalyzer.analyzeDetailed(agg);

        assertEquals(MergeStrategy.TWO_PHASE_GROUP_BY, result.strategy);
        assertNotNull(result.isGroupKey);
        assertEquals(2, result.isGroupKey.length);
        assertTrue(result.isGroupKey[0]);  // GROUP BY key
        assertFalse(result.isGroupKey[1]); // COUNT aggregate
        assertNotNull(result.aggKinds);
        assertEquals(SqlKind.COUNT, result.aggKinds[1]);
    }

    public void testAnalyzeDetailedSingleNodeHasNoMetadata() {
        // SUM(DISTINCT) forces SINGLE_NODE (not COUNT DISTINCT, so can't use DISTINCT_EXPAND)
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0), List.of(makeAggCall(SqlStdOperatorTable.SUM, true)));

        QueryAnalyzer.AnalysisResult result = QueryAnalyzer.analyzeDetailed(agg);

        assertEquals(MergeStrategy.SINGLE_NODE, result.strategy);
        assertNull(result.aggKinds);
    }

    public void testAnalyzeDetailedCountDistinctReturnsDistinctExpand() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0), List.of(makeAggCall(SqlStdOperatorTable.COUNT, true)));

        QueryAnalyzer.AnalysisResult result = QueryAnalyzer.analyzeDetailed(agg);

        assertEquals(MergeStrategy.DISTINCT_EXPAND, result.strategy);
    }

    public void testGroupByWithAvgHasAvgInAggKinds() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0), List.of(makeAggCall(SqlStdOperatorTable.AVG, false)));

        QueryAnalyzer.AnalysisResult result = QueryAnalyzer.analyzeDetailed(agg);

        assertEquals(MergeStrategy.TWO_PHASE_GROUP_BY, result.strategy);
        assertNotNull(result.aggKinds);
        assertEquals(SqlKind.AVG, result.aggKinds[1]); // index 0 is group key (null), index 1 is AVG
    }

    public void testGlobalAvgHasAvgInAggKinds() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(makeAggCall(SqlStdOperatorTable.AVG, false)));

        QueryAnalyzer.AnalysisResult result = QueryAnalyzer.analyzeDetailed(agg);

        assertEquals(MergeStrategy.GLOBAL_MERGE, result.strategy);
        assertNotNull(result.aggKinds);
        assertEquals(1, result.aggKinds.length);
        assertEquals(SqlKind.AVG, result.aggKinds[0]);
    }

    // --- hasDistinct / hasAvg tests ---

    public void testHasDistinctReturnsFalseForCount() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(makeAggCall(SqlStdOperatorTable.COUNT, false)));
        assertFalse(QueryAnalyzer.hasDistinct(agg));
    }

    public void testHasDistinctReturnsTrueForDistinctCount() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(makeAggCall(SqlStdOperatorTable.COUNT, true)));
        assertTrue(QueryAnalyzer.hasDistinct(agg));
    }

    public void testHasDistinctReturnsFalseForAvg() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(makeAggCall(SqlStdOperatorTable.AVG, false)));
        assertFalse(QueryAnalyzer.hasDistinct(agg));
    }

    public void testHasAvgReturnsTrueForAvg() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(makeAggCall(SqlStdOperatorTable.AVG, false)));
        assertTrue(QueryAnalyzer.hasAvg(agg));
    }

    public void testHasAvgReturnsFalseForCount() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(makeAggCall(SqlStdOperatorTable.COUNT, false)));
        assertFalse(QueryAnalyzer.hasAvg(agg));
    }

    // --- hasOnlyCountDistinct tests ---

    public void testHasOnlyCountDistinctReturnsTrueForSingleCountDistinct() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(makeAggCall(SqlStdOperatorTable.COUNT, true)));
        assertTrue(QueryAnalyzer.hasOnlyCountDistinct(agg));
    }

    public void testHasOnlyCountDistinctReturnsFalseForNonDistinctCount() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(makeAggCall(SqlStdOperatorTable.COUNT, false)));
        assertFalse(QueryAnalyzer.hasOnlyCountDistinct(agg));
    }

    public void testHasOnlyCountDistinctReturnsFalseForDistinctSum() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(makeAggCall(SqlStdOperatorTable.SUM, true)));
        assertFalse(QueryAnalyzer.hasOnlyCountDistinct(agg));
    }

    public void testHasOnlyCountDistinctReturnsFalseForMixed() {
        AggregateCall countCall = makeAggCall(SqlStdOperatorTable.COUNT, false);
        AggregateCall countDistinctCall = makeAggCall(SqlStdOperatorTable.COUNT, true);
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(countCall, countDistinctCall));
        assertFalse(QueryAnalyzer.hasOnlyCountDistinct(agg));
    }

    public void testHasOnlyCountDistinctReturnsFalseForEmptyAggList() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of());
        assertFalse(QueryAnalyzer.hasOnlyCountDistinct(agg));
    }

    public void testExtractAggKinds() {
        AggregateCall countCall = makeAggCall(SqlStdOperatorTable.COUNT, false);
        AggregateCall sumCall = makeAggCall(SqlStdOperatorTable.SUM, false);
        AggregateCall minCall = makeAggCall(SqlStdOperatorTable.MIN, false);
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(countCall, sumCall, minCall));

        SqlKind[] kinds = QueryAnalyzer.extractAggKinds(agg);

        assertEquals(3, kinds.length);
        assertEquals(SqlKind.COUNT, kinds[0]);
        assertEquals(SqlKind.SUM, kinds[1]);
        assertEquals(SqlKind.MIN, kinds[2]);
    }

    public void testExtractSortColumns() {
        Sort sort = makeSort(true, true);
        int[] cols = QueryAnalyzer.extractSortColumns(sort);
        assertEquals(1, cols.length);
        assertEquals(0, cols[0]);
    }

    public void testExtractSortDirections() {
        Sort sort = makeSort(true, true);
        boolean[] dirs = QueryAnalyzer.extractSortDirections(sort);
        assertEquals(1, dirs.length);
        assertTrue(dirs[0]);
    }

    public void testExtractLimitFromRexLiteral() {
        Sort sort = makeSortWithLimit(10);
        assertEquals(10, QueryAnalyzer.extractLimit(sort));
    }

    public void testExtractLimitFromNonLiteralReturnsZero() {
        Sort sort = makeSort(true, true);
        // makeSort uses mock(RexNode.class) which is not RexLiteral
        assertEquals(0, QueryAnalyzer.extractLimit(sort));
    }

    // --- HAVING clause tests ---

    public void testFilterAboveAggregateReturnsTwoPhaseGroupBy() {
        // Plan: Filter(HAVING) → Aggregate(GROUP BY + COUNT) — no Sort
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0), List.of(makeAggCall(SqlStdOperatorTable.COUNT, false)));
        Filter havingFilter = mockHavingFilter(agg, 1, SqlKind.GREATER_THAN, 100000);

        QueryAnalyzer.AnalysisResult result = QueryAnalyzer.analyzeDetailed(havingFilter);

        assertEquals(MergeStrategy.TWO_PHASE_GROUP_BY, result.strategy);
        assertNotNull(result.having);
        assertEquals(1, result.having.columnIndex);
        assertEquals(SqlKind.GREATER_THAN, result.having.operator);
        assertEquals(100000, result.having.value);
    }

    public void testFilterAboveAggregateWithAvgReturnsTwoPhaseGroupBy() {
        // Plan: Filter(HAVING) → Aggregate(GROUP BY + AVG + COUNT) — no Sort
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0), List.of(
            makeAggCall(SqlStdOperatorTable.AVG, false),
            makeAggCall(SqlStdOperatorTable.COUNT, false)
        ));
        Filter havingFilter = mockHavingFilter(agg, 2, SqlKind.GREATER_THAN, 100000);

        QueryAnalyzer.AnalysisResult result = QueryAnalyzer.analyzeDetailed(havingFilter);

        assertEquals(MergeStrategy.TWO_PHASE_GROUP_BY, result.strategy);
        assertNotNull(result.having);
        assertEquals(2, result.having.columnIndex);
        assertEquals(100000, result.having.value);
    }

    public void testHavingConditionOperatorSql() {
        assertEquals(">", new QueryAnalyzer.HavingCondition(0, SqlKind.GREATER_THAN, 0).operatorSql());
        assertEquals(">=", new QueryAnalyzer.HavingCondition(0, SqlKind.GREATER_THAN_OR_EQUAL, 0).operatorSql());
        assertEquals("<", new QueryAnalyzer.HavingCondition(0, SqlKind.LESS_THAN, 0).operatorSql());
        assertEquals("<=", new QueryAnalyzer.HavingCondition(0, SqlKind.LESS_THAN_OR_EQUAL, 0).operatorSql());
        assertEquals("=", new QueryAnalyzer.HavingCondition(0, SqlKind.EQUALS, 0).operatorSql());
        assertEquals("!=", new QueryAnalyzer.HavingCondition(0, SqlKind.NOT_EQUALS, 0).operatorSql());
    }

    public void testExtractHavingConditionWithComparison() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0), List.of(makeAggCall(SqlStdOperatorTable.COUNT, false)));
        Filter filter = mockHavingFilter(agg, 1, SqlKind.GREATER_THAN, 100000);

        QueryAnalyzer.HavingCondition having = QueryAnalyzer.extractHavingCondition(filter, 1);

        assertNotNull(having);
        assertEquals(1, having.columnIndex);
        assertEquals(SqlKind.GREATER_THAN, having.operator);
        assertEquals(100000, having.value);
    }

    public void testExtractHavingConditionWithNonCallReturnsNull() {
        Filter filter = mock(Filter.class);
        RexNode nonCall = mock(RexNode.class);
        when(filter.getCondition()).thenReturn(nonCall);

        assertNull(QueryAnalyzer.extractHavingCondition(filter, 1));
    }

    public void testGroupByWithoutHavingHasNullHaving() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0), List.of(makeAggCall(SqlStdOperatorTable.COUNT, false)));

        QueryAnalyzer.AnalysisResult result = QueryAnalyzer.analyzeDetailed(agg);

        assertEquals(MergeStrategy.TWO_PHASE_GROUP_BY, result.strategy);
        assertNull(result.having);
    }

    // --- Helper methods ---

    private Aggregate mockAggregate(ImmutableBitSet groupSet, List<AggregateCall> aggCalls) {
        Aggregate agg = mock(Aggregate.class);
        when(agg.getGroupSet()).thenReturn(groupSet);
        when(agg.getAggCallList()).thenReturn(aggCalls);
        when(agg.getInputs()).thenReturn(List.of());
        return agg;
    }

    /**
     * Creates a real AggregateCall using the public constructor.
     * AggregateCall has final methods (isDistinct, getAggregation) so it cannot be mocked.
     */
    @SuppressWarnings("deprecation")
    private AggregateCall makeAggCall(SqlAggFunction aggFunction, boolean distinct) {
        return new AggregateCall(aggFunction, distinct, List.of(), BIGINT_TYPE, null);
    }

    private Sort makeSort(boolean hasCollation, boolean hasFetch) {
        return makeSort(hasCollation, hasFetch, 0, 2);
    }

    private Sort makeSort(boolean hasCollation, boolean hasFetch, int sortFieldIndex, int outputFieldCount) {
        RelCollation collation;
        if (hasCollation) {
            RelFieldCollation fieldCollation = new RelFieldCollation(sortFieldIndex, RelFieldCollation.Direction.ASCENDING);
            collation = RelCollations.of(fieldCollation);
        } else {
            collation = RelCollations.EMPTY;
        }

        RexNode fetchNode = hasFetch ? mock(RexNode.class) : null;

        RelOptCluster cluster = mock(RelOptCluster.class);
        RelTraitSet traitSet = RelTraitSet.createEmpty().plus(collation);
        RelNode input = mockSimpleNode();
        RelDataType rowType = mock(RelDataType.class);
        when(rowType.getFieldCount()).thenReturn(outputFieldCount);
        when(input.getRowType()).thenReturn(rowType);

        return new StubSort(cluster, traitSet, input, collation, fetchNode);
    }

    /**
     * Concrete Sort subclass for testing. Allows setting collation and fetch
     * via the constructor without needing a full Calcite environment.
     */
    private static class StubSort extends Sort {
        StubSort(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode fetch) {
            super(cluster, traitSet, List.of(), input, collation, null, fetch);
        }

        @Override
        public Sort copy(RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
            return new StubSort(getCluster(), traitSet, input, collation, fetch);
        }
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
        when(rowType.getFieldCount()).thenReturn(2);
        when(node.getRowType()).thenReturn(rowType);
        // RelVisitor uses childrenAccept(), not getInputs(), for traversal
        doAnswer(invocation -> {
            org.apache.calcite.rel.RelVisitor visitor = invocation.getArgument(0);
            visitor.visit(input, 0, node);
            return null;
        }).when(node).childrenAccept(any(org.apache.calcite.rel.RelVisitor.class));
        return node;
    }

    @SuppressWarnings("unchecked")
    private Filter mockHavingFilter(Aggregate input, int columnIndex, SqlKind operator, long value) {
        Filter filter = mock(Filter.class);
        when(filter.getInput()).thenReturn(input);
        when(filter.getInputs()).thenReturn(List.of(input));
        doAnswer(invocation -> {
            org.apache.calcite.rel.RelVisitor visitor = invocation.getArgument(0);
            visitor.visit(input, 0, filter);
            return null;
        }).when(filter).childrenAccept(any(org.apache.calcite.rel.RelVisitor.class));

        // Build RexCall: column op value
        RexInputRef colRef = mock(RexInputRef.class);
        when(colRef.getIndex()).thenReturn(columnIndex);

        RexLiteral valueLiteral = mock(RexLiteral.class);
        when(valueLiteral.getValueAs(Number.class)).thenReturn(value);

        RexCall call = mock(RexCall.class);
        when(call.getKind()).thenReturn(operator);
        when(call.getOperands()).thenReturn(List.of(colRef, valueLiteral));

        when(filter.getCondition()).thenReturn(call);
        return filter;
    }

    private Sort makeSortWithLimit(int limitValue) {
        RelFieldCollation fieldCollation = new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING);
        RelCollation collation = RelCollations.of(fieldCollation);

        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        org.apache.calcite.rex.RexBuilder rexBuilder = new org.apache.calcite.rex.RexBuilder(typeFactory);
        RexLiteral fetchLiteral = rexBuilder.makeExactLiteral(BigDecimal.valueOf(limitValue),
            typeFactory.createSqlType(SqlTypeName.INTEGER));

        RelOptCluster cluster = mock(RelOptCluster.class);
        RelTraitSet traitSet = RelTraitSet.createEmpty().plus(collation);
        RelNode input = mockSimpleNode();
        RelDataType rowType = mock(RelDataType.class);
        when(rowType.getFieldCount()).thenReturn(2);
        when(input.getRowType()).thenReturn(rowType);

        return new StubSort(cluster, traitSet, input, collation, fetchLiteral);
    }
}

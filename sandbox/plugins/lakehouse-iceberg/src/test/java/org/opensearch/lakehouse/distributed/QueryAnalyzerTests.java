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

    public void testGroupByAggregateReturnsSingleNode() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0, 1), List.of(makeAggCall(SqlStdOperatorTable.COUNT, false)));
        assertEquals(MergeStrategy.SINGLE_NODE, QueryAnalyzer.analyze(agg));
    }

    public void testCountDistinctReturnsSingleNode() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(makeAggCall(SqlStdOperatorTable.COUNT, true)));
        assertEquals(MergeStrategy.SINGLE_NODE, QueryAnalyzer.analyze(agg));
    }

    public void testAvgReturnsSingleNode() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(makeAggCall(SqlStdOperatorTable.AVG, false)));
        assertEquals(MergeStrategy.SINGLE_NODE, QueryAnalyzer.analyze(agg));
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

    public void testMultipleAggCallsMixedDistinct() {
        AggregateCall countCall = makeAggCall(SqlStdOperatorTable.COUNT, false);
        AggregateCall distinctSumCall = makeAggCall(SqlStdOperatorTable.SUM, true);
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of(countCall, distinctSumCall));
        assertEquals(MergeStrategy.SINGLE_NODE, QueryAnalyzer.analyze(agg));
    }

    public void testEmptyAggCallListWithGroupByReturnsSingleNode() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0), List.of());
        assertEquals(MergeStrategy.SINGLE_NODE, QueryAnalyzer.analyze(agg));
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

    public void testAnalyzeDetailedSingleNodeHasNoMetadata() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0), List.of(makeAggCall(SqlStdOperatorTable.COUNT, false)));

        QueryAnalyzer.AnalysisResult result = QueryAnalyzer.analyzeDetailed(agg);

        assertEquals(MergeStrategy.SINGLE_NODE, result.strategy);
        assertNull(result.aggKinds);
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

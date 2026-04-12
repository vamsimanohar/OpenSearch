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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

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

    public void testSortWithoutLimitReturnsConcat() {
        Sort sort = makeSort(true, false);
        assertEquals(MergeStrategy.CONCAT, QueryAnalyzer.analyze(sort));
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

    public void testFindAggregateReturnsNullForNoAggregate() {
        RelNode simple = mockSimpleNode();
        assertNull(QueryAnalyzer.findAggregate(simple));
    }

    public void testFindSortReturnsNullForNoSort() {
        RelNode simple = mockSimpleNode();
        assertNull(QueryAnalyzer.findSort(simple));
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

    public void testAggregateInfoHoldsReference() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of());
        QueryAnalyzer.AggregateInfo info = new QueryAnalyzer.AggregateInfo(agg);
        assertSame(agg, info.aggregate);
    }

    public void testSortInfoHoldsReference() {
        Sort sort = makeSort(true, true);
        QueryAnalyzer.SortInfo info = new QueryAnalyzer.SortInfo(sort);
        assertSame(sort, info.sort);
    }

    public void testEmptyAggCallListWithGroupByReturnsSingleNode() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(0), List.of());
        assertEquals(MergeStrategy.SINGLE_NODE, QueryAnalyzer.analyze(agg));
    }

    public void testEmptyAggCallListWithNoGroupByReturnsGlobalMerge() {
        Aggregate agg = mockAggregate(ImmutableBitSet.of(), List.of());
        assertEquals(MergeStrategy.GLOBAL_MERGE, QueryAnalyzer.analyze(agg));
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
        return node;
    }
}

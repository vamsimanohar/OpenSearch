/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Tests for {@link DistributionPlan} — factory methods, withSortInfo,
 * toString, and edge cases.
 */
public class DistributionPlanTests extends OpenSearchTestCase {

    // ---- Factory method tests ----

    public void testScanOnlyFactory() {
        DistributionPlan plan = DistributionPlan.scanOnly();
        assertEquals(DistributionPlan.QueryType.SCAN_ONLY, plan.getQueryType());
        assertEquals(0, plan.getGroupKeyOutputColumns().length);
        assertTrue(plan.getAggregateMerges().isEmpty());
        assertNull(plan.getSortInfo());
    }

    public void testGlobalAggregateFactory() {
        List<DistributionPlan.AggMergeInfo> merges = List.of(
            new DistributionPlan.AggMergeInfo(0, DistributionPlan.MergeOp.SUM),
            new DistributionPlan.AggMergeInfo(1, DistributionPlan.MergeOp.MIN)
        );
        DistributionPlan plan = DistributionPlan.globalAggregate(merges);

        assertEquals(DistributionPlan.QueryType.GLOBAL_AGGREGATE, plan.getQueryType());
        assertEquals(0, plan.getGroupKeyOutputColumns().length);
        assertEquals(2, plan.getAggregateMerges().size());
        assertEquals(DistributionPlan.MergeOp.SUM, plan.getAggregateMerges().get(0).getMergeOp());
        assertEquals(DistributionPlan.MergeOp.MIN, plan.getAggregateMerges().get(1).getMergeOp());
        assertNull(plan.getSortInfo());
    }

    public void testGroupedAggregateFactory() {
        int[] groupKeys = {0, 1};
        List<DistributionPlan.AggMergeInfo> merges = List.of(
            new DistributionPlan.AggMergeInfo(2, DistributionPlan.MergeOp.SUM),
            new DistributionPlan.AggMergeInfo(3, DistributionPlan.MergeOp.MAX)
        );
        DistributionPlan plan = DistributionPlan.groupedAggregate(groupKeys, merges);

        assertEquals(DistributionPlan.QueryType.GROUPED_AGGREGATE, plan.getQueryType());
        assertArrayEquals(new int[]{0, 1}, plan.getGroupKeyOutputColumns());
        assertEquals(2, plan.getAggregateMerges().size());
        assertEquals(2, plan.getAggregateMerges().get(0).getOutputColumnIndex());
        assertEquals(3, plan.getAggregateMerges().get(1).getOutputColumnIndex());
        assertNull(plan.getSortInfo());
    }

    public void testUnsupportedFactory() {
        DistributionPlan plan = DistributionPlan.unsupported();
        assertEquals(DistributionPlan.QueryType.UNSUPPORTED, plan.getQueryType());
        assertEquals(0, plan.getGroupKeyOutputColumns().length);
        assertTrue(plan.getAggregateMerges().isEmpty());
        assertNull(plan.getSortInfo());
    }

    // ---- withSortInfo tests ----

    public void testWithSortInfoCreatesNewPlan() {
        DistributionPlan original = DistributionPlan.scanOnly();
        DistributionPlan.SortInfo sortInfo = new DistributionPlan.SortInfo(
            new int[]{0}, new boolean[]{true}, new boolean[]{false}, 10
        );
        DistributionPlan withSort = original.withSortInfo(sortInfo);

        // Original should be unmodified
        assertNull(original.getSortInfo());
        // New plan should have sort info
        assertNotNull(withSort.getSortInfo());
        assertEquals(10, withSort.getSortInfo().getLimit());
        // Type preserved
        assertEquals(DistributionPlan.QueryType.SCAN_ONLY, withSort.getQueryType());
    }

    public void testWithSortInfoOnGroupedAggregate() {
        int[] groupKeys = {0};
        List<DistributionPlan.AggMergeInfo> merges = List.of(
            new DistributionPlan.AggMergeInfo(1, DistributionPlan.MergeOp.SUM)
        );
        DistributionPlan original = DistributionPlan.groupedAggregate(groupKeys, merges);
        DistributionPlan.SortInfo sortInfo = new DistributionPlan.SortInfo(
            new int[]{1}, new boolean[]{false}, new boolean[]{true}, 5
        );
        DistributionPlan withSort = original.withSortInfo(sortInfo);

        assertEquals(DistributionPlan.QueryType.GROUPED_AGGREGATE, withSort.getQueryType());
        assertArrayEquals(new int[]{0}, withSort.getGroupKeyOutputColumns());
        assertEquals(1, withSort.getAggregateMerges().size());
        assertNotNull(withSort.getSortInfo());
        assertEquals(5, withSort.getSortInfo().getLimit());
        assertFalse(withSort.getSortInfo().getAscending()[0]);
    }

    // ---- SortInfo tests ----

    public void testSortInfoGetters() {
        DistributionPlan.SortInfo sortInfo = new DistributionPlan.SortInfo(
            new int[]{2, 3}, new boolean[]{true, false}, new boolean[]{false, true}, 100
        );

        assertArrayEquals(new int[]{2, 3}, sortInfo.getSortColumns());
        assertTrue(sortInfo.getAscending()[0]);
        assertFalse(sortInfo.getAscending()[1]);
        assertFalse(sortInfo.getNullsFirst()[0]);
        assertTrue(sortInfo.getNullsFirst()[1]);
        assertEquals(100, sortInfo.getLimit());
    }

    public void testSortInfoNoLimit() {
        DistributionPlan.SortInfo sortInfo = new DistributionPlan.SortInfo(
            new int[]{0}, new boolean[]{true}, new boolean[]{false}, -1
        );
        assertEquals(-1, sortInfo.getLimit());
    }

    public void testSortInfoEmptyColumns() {
        DistributionPlan.SortInfo sortInfo = new DistributionPlan.SortInfo(
            new int[0], new boolean[0], new boolean[0], 10
        );
        assertEquals(0, sortInfo.getSortColumns().length);
        assertEquals(10, sortInfo.getLimit());
    }

    public void testSortInfoToString() {
        DistributionPlan.SortInfo sortInfo = new DistributionPlan.SortInfo(
            new int[]{0}, new boolean[]{true}, new boolean[]{false}, 5
        );
        String str = sortInfo.toString();
        assertTrue(str.contains("sortColumns=[0]"));
        assertTrue(str.contains("ascending=[true]"));
        assertTrue(str.contains("limit=5"));
    }

    // ---- AggMergeInfo tests ----

    public void testAggMergeInfoGetters() {
        DistributionPlan.AggMergeInfo info = new DistributionPlan.AggMergeInfo(3, DistributionPlan.MergeOp.MAX);
        assertEquals(3, info.getOutputColumnIndex());
        assertEquals(DistributionPlan.MergeOp.MAX, info.getMergeOp());
    }

    public void testAggMergeInfoToString() {
        DistributionPlan.AggMergeInfo info = new DistributionPlan.AggMergeInfo(0, DistributionPlan.MergeOp.SUM);
        String str = info.toString();
        assertTrue(str.contains("col=0"));
        assertTrue(str.contains("op=SUM"));
    }

    // ---- toString tests ----

    public void testDistributionPlanToString() {
        DistributionPlan plan = DistributionPlan.scanOnly();
        String str = plan.toString();
        assertTrue(str.contains("SCAN_ONLY"));
        assertTrue(str.contains("groupKeys=[]"));
    }

    public void testDistributionPlanToStringWithSort() {
        DistributionPlan.SortInfo sortInfo = new DistributionPlan.SortInfo(
            new int[]{0}, new boolean[]{true}, new boolean[]{false}, 10
        );
        DistributionPlan plan = DistributionPlan.scanOnly().withSortInfo(sortInfo);
        String str = plan.toString();
        assertTrue(str.contains("SCAN_ONLY"));
        assertTrue(str.contains("SortInfo"));
        assertTrue(str.contains("limit=10"));
    }

    // ---- QueryType enum coverage ----

    public void testQueryTypeValues() {
        DistributionPlan.QueryType[] values = DistributionPlan.QueryType.values();
        assertEquals(4, values.length);
        assertEquals(DistributionPlan.QueryType.SCAN_ONLY, DistributionPlan.QueryType.valueOf("SCAN_ONLY"));
        assertEquals(DistributionPlan.QueryType.GLOBAL_AGGREGATE, DistributionPlan.QueryType.valueOf("GLOBAL_AGGREGATE"));
        assertEquals(DistributionPlan.QueryType.GROUPED_AGGREGATE, DistributionPlan.QueryType.valueOf("GROUPED_AGGREGATE"));
        assertEquals(DistributionPlan.QueryType.UNSUPPORTED, DistributionPlan.QueryType.valueOf("UNSUPPORTED"));
    }

    public void testMergeOpValues() {
        DistributionPlan.MergeOp[] values = DistributionPlan.MergeOp.values();
        assertEquals(3, values.length);
        assertEquals(DistributionPlan.MergeOp.SUM, DistributionPlan.MergeOp.valueOf("SUM"));
        assertEquals(DistributionPlan.MergeOp.MIN, DistributionPlan.MergeOp.valueOf("MIN"));
        assertEquals(DistributionPlan.MergeOp.MAX, DistributionPlan.MergeOp.valueOf("MAX"));
    }
}

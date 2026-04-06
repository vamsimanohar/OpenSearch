/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import java.util.Collections;
import java.util.List;

/**
 * Describes how to merge distributed worker results for a given query.
 *
 * <p>Created by {@link DistributedPlanSplitter} from a Calcite logical plan.
 * Used by {@link DistributedResultMerger} to correctly combine partial results
 * from multiple worker nodes.
 */
public class DistributionPlan {

    /** Classification of how the query's results should be merged. */
    public enum QueryType {
        /** No aggregation — worker results are simply concatenated. */
        SCAN_ONLY,
        /** Global aggregate (no GROUP BY) — each worker returns one row, merge into one row. */
        GLOBAL_AGGREGATE,
        /** Grouped aggregate — workers may return overlapping groups that need re-aggregation. */
        GROUPED_AGGREGATE,
        /** Query cannot be safely distributed (e.g., contains AVG). */
        UNSUPPORTED
    }

    /** How to combine partial aggregate values across workers. */
    public enum MergeOp {
        /** Sum partial values (used for COUNT and SUM). */
        SUM,
        /** Take the minimum across workers. */
        MIN,
        /** Take the maximum across workers. */
        MAX
    }

    /**
     * Describes sort columns and limit for merge-sort at the coordinator.
     *
     * <p>When a query has ORDER BY + LIMIT, each worker returns pre-sorted,
     * pre-limited rows. The coordinator must merge-sort and apply the final LIMIT.
     */
    public static class SortInfo {
        private final int[] sortColumns;
        private final boolean[] ascending;
        private final boolean[] nullsFirst;
        private final long limit;

        /**
         * Creates sort info for coordinator merge-sort.
         *
         * @param sortColumns column positions to sort by
         * @param ascending   sort direction per column (true = ASC, false = DESC)
         * @param nullsFirst  null handling per column (true = nulls first)
         * @param limit       LIMIT value (-1 if no limit)
         */
        public SortInfo(int[] sortColumns, boolean[] ascending, boolean[] nullsFirst, long limit) {
            this.sortColumns = sortColumns;
            this.ascending = ascending;
            this.nullsFirst = nullsFirst;
            this.limit = limit;
        }

        /** Column positions to sort by. */
        public int[] getSortColumns() {
            return sortColumns;
        }

        /** Sort direction per column (true = ASC, false = DESC). */
        public boolean[] getAscending() {
            return ascending;
        }

        /** Null handling per column (true = nulls first, false = nulls last). */
        public boolean[] getNullsFirst() {
            return nullsFirst;
        }

        /** LIMIT value, or -1 if no limit. */
        public long getLimit() {
            return limit;
        }

        @Override
        public String toString() {
            return "SortInfo{sortColumns=" + java.util.Arrays.toString(sortColumns)
                + ", ascending=" + java.util.Arrays.toString(ascending)
                + ", nullsFirst=" + java.util.Arrays.toString(nullsFirst)
                + ", limit=" + limit + "}";
        }
    }

    /** Describes how to merge a single aggregate column across workers. */
    public static class AggMergeInfo {
        private final int outputColumnIndex;
        private final MergeOp mergeOp;

        /**
         * Creates a new aggregate merge descriptor.
         *
         * @param outputColumnIndex position of this column in the worker's output row
         * @param mergeOp           how to combine this column's values across workers
         */
        public AggMergeInfo(int outputColumnIndex, MergeOp mergeOp) {
            this.outputColumnIndex = outputColumnIndex;
            this.mergeOp = mergeOp;
        }

        /** Position of this aggregate column in the worker's output row. */
        public int getOutputColumnIndex() {
            return outputColumnIndex;
        }

        /** How to combine this column's values across workers. */
        public MergeOp getMergeOp() {
            return mergeOp;
        }

        @Override
        public String toString() {
            return "AggMergeInfo{col=" + outputColumnIndex + ", op=" + mergeOp + "}";
        }
    }

    private final QueryType queryType;
    private final int[] groupKeyOutputColumns;
    private final List<AggMergeInfo> aggregateMerges;
    private final SortInfo sortInfo;

    private DistributionPlan(QueryType queryType, int[] groupKeyOutputColumns,
                             List<AggMergeInfo> aggregateMerges, SortInfo sortInfo) {
        this.queryType = queryType;
        this.groupKeyOutputColumns = groupKeyOutputColumns;
        this.aggregateMerges = aggregateMerges;
        this.sortInfo = sortInfo;
    }

    /** Creates a plan for scan-only queries (no aggregation). */
    public static DistributionPlan scanOnly() {
        return new DistributionPlan(QueryType.SCAN_ONLY, new int[0], Collections.emptyList(), null);
    }

    /**
     * Creates a plan for global aggregate queries (no GROUP BY).
     *
     * @param merges merge info for each aggregate column
     */
    public static DistributionPlan globalAggregate(List<AggMergeInfo> merges) {
        return new DistributionPlan(QueryType.GLOBAL_AGGREGATE, new int[0], merges, null);
    }

    /**
     * Creates a plan for grouped aggregate queries.
     *
     * @param groupKeyOutputColumns positions of group key columns in worker output
     * @param merges                merge info for each aggregate column
     */
    public static DistributionPlan groupedAggregate(int[] groupKeyOutputColumns, List<AggMergeInfo> merges) {
        return new DistributionPlan(QueryType.GROUPED_AGGREGATE, groupKeyOutputColumns, merges, null);
    }

    /** Creates a plan indicating the query cannot be safely distributed. */
    public static DistributionPlan unsupported() {
        return new DistributionPlan(QueryType.UNSUPPORTED, new int[0], Collections.emptyList(), null);
    }

    /**
     * Returns a copy of this plan with sort info attached.
     *
     * @param sortInfo the sort/limit info to apply after merging
     * @return a new plan with sort info set
     */
    public DistributionPlan withSortInfo(SortInfo sortInfo) {
        return new DistributionPlan(this.queryType, this.groupKeyOutputColumns,
            this.aggregateMerges, sortInfo);
    }

    /** Returns the query type classification. */
    public QueryType getQueryType() {
        return queryType;
    }

    /** Returns the positions of group key columns in worker output rows. */
    public int[] getGroupKeyOutputColumns() {
        return groupKeyOutputColumns;
    }

    /** Returns the merge instructions for aggregate columns. */
    public List<AggMergeInfo> getAggregateMerges() {
        return aggregateMerges;
    }

    /** Returns the sort/limit info, or {@code null} if no sort is needed at merge time. */
    public SortInfo getSortInfo() {
        return sortInfo;
    }

    @Override
    public String toString() {
        return "DistributionPlan{type=" + queryType
            + ", groupKeys=" + java.util.Arrays.toString(groupKeyOutputColumns)
            + ", merges=" + aggregateMerges
            + ", sortInfo=" + sortInfo + "}";
    }
}

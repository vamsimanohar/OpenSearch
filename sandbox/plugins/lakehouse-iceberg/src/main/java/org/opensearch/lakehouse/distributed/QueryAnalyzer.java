/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlKind;

import java.util.List;

/**
 * Inspects a Calcite {@link RelNode} tree to determine the appropriate {@link MergeStrategy}
 * for distributed query execution.
 * <p>
 * Phase 1 classification rules:
 * <ul>
 *   <li>{@link MergeStrategy#GLOBAL_MERGE} — global aggregation (no GROUP BY), with only
 *       SUM/COUNT/MIN/MAX aggregate functions (no AVG, no DISTINCT)</li>
 *   <li>{@link MergeStrategy#TOPK_MERGE} — ORDER BY with LIMIT, no aggregation</li>
 *   <li>{@link MergeStrategy#SINGLE_NODE} — GROUP BY, COUNT DISTINCT, AVG, or any other
 *       non-trivially distributable pattern</li>
 *   <li>{@link MergeStrategy#CONCAT} — simple scan/filter/project with no agg and no sort</li>
 * </ul>
 *
 * @opensearch.internal
 */
public final class QueryAnalyzer {

    private QueryAnalyzer() {}

    /**
     * Analyzes the RelNode tree and returns the appropriate merge strategy.
     *
     * @param relNode the root of the Calcite logical plan
     * @return the merge strategy to use for distributed execution
     */
    public static MergeStrategy analyze(RelNode relNode) {
        return analyzeDetailed(relNode).strategy;
    }

    /**
     * Analyzes the RelNode tree and returns a detailed result including merge strategy,
     * aggregate function kinds (for GLOBAL_MERGE), and sort/limit info (for TOPK_MERGE).
     *
     * @param relNode the root of the Calcite logical plan
     * @return the detailed analysis result
     */
    public static AnalysisResult analyzeDetailed(RelNode relNode) {
        AggregateInfo aggInfo = findAggregate(relNode);
        SortInfo sortInfo = findSort(relNode);

        if (aggInfo != null) {
            if (!aggInfo.aggregate.getGroupSet().isEmpty()) {
                return new AnalysisResult(MergeStrategy.SINGLE_NODE);
            }
            if (hasDistinctOrAvg(aggInfo.aggregate)) {
                return new AnalysisResult(MergeStrategy.SINGLE_NODE);
            }
            SqlKind[] aggKinds = extractAggKinds(aggInfo.aggregate);
            return new AnalysisResult(MergeStrategy.GLOBAL_MERGE, aggKinds, null, null, 0);
        }

        if (sortInfo != null && sortInfo.sort.fetch != null) {
            int[] sortColumns = extractSortColumns(sortInfo.sort);
            boolean[] sortAsc = extractSortDirections(sortInfo.sort);
            int limit = extractLimit(sortInfo.sort);
            return new AnalysisResult(MergeStrategy.TOPK_MERGE, null, sortColumns, sortAsc, limit);
        }

        return new AnalysisResult(MergeStrategy.CONCAT);
    }

    /**
     * Recursively searches for a {@link Sort} node in the plan tree.
     *
     * @param node the node to search from
     * @return the SortInfo if found, null otherwise
     */
    static SortInfo findSort(RelNode node) {
        if (node instanceof Sort) {
            Sort sort = (Sort) node;
            if (!sort.getCollation().getFieldCollations().isEmpty()) {
                return new SortInfo(sort);
            }
        }
        for (RelNode input : node.getInputs()) {
            SortInfo result = findSort(input);
            if (result != null) {
                return result;
            }
        }
        return null;
    }

    /**
     * Recursively searches for an {@link Aggregate} node in the plan tree.
     *
     * @param node the node to search from
     * @return the AggregateInfo if found, null otherwise
     */
    static AggregateInfo findAggregate(RelNode node) {
        if (node instanceof Aggregate) {
            return new AggregateInfo((Aggregate) node);
        }
        for (RelNode input : node.getInputs()) {
            AggregateInfo result = findAggregate(input);
            if (result != null) {
                return result;
            }
        }
        return null;
    }

    /**
     * Checks if the aggregate has any DISTINCT aggregate calls or AVG functions.
     *
     * @param aggregate the aggregate node
     * @return true if any call is DISTINCT or AVG
     */
    static boolean hasDistinctOrAvg(Aggregate aggregate) {
        for (AggregateCall call : aggregate.getAggCallList()) {
            if (call.isDistinct()) {
                return true;
            }
            if (call.getAggregation().getKind() == SqlKind.AVG) {
                return true;
            }
        }
        return false;
    }

    /**
     * Extracts aggregate function kinds from the Aggregate node.
     */
    static SqlKind[] extractAggKinds(Aggregate aggregate) {
        List<AggregateCall> calls = aggregate.getAggCallList();
        SqlKind[] kinds = new SqlKind[calls.size()];
        for (int i = 0; i < calls.size(); i++) {
            kinds[i] = calls.get(i).getAggregation().getKind();
        }
        return kinds;
    }

    /**
     * Extracts sort column indices from the Sort node's collation.
     */
    static int[] extractSortColumns(Sort sort) {
        List<RelFieldCollation> collations = sort.getCollation().getFieldCollations();
        int[] cols = new int[collations.size()];
        for (int i = 0; i < collations.size(); i++) {
            cols[i] = collations.get(i).getFieldIndex();
        }
        return cols;
    }

    /**
     * Extracts sort directions (true=ascending) from the Sort node's collation.
     */
    static boolean[] extractSortDirections(Sort sort) {
        List<RelFieldCollation> collations = sort.getCollation().getFieldCollations();
        boolean[] asc = new boolean[collations.size()];
        for (int i = 0; i < collations.size(); i++) {
            asc[i] = collations.get(i).getDirection() == RelFieldCollation.Direction.ASCENDING;
        }
        return asc;
    }

    /**
     * Extracts the LIMIT value from the Sort node's fetch expression.
     * Returns 0 if fetch is null or not a literal.
     */
    static int extractLimit(Sort sort) {
        if (sort.fetch instanceof RexLiteral) {
            return ((RexLiteral) sort.fetch).getValueAs(Integer.class);
        }
        return 0;
    }

    /**
     * Holds a reference to a discovered Aggregate node.
     */
    static final class AggregateInfo {
        final Aggregate aggregate;

        AggregateInfo(Aggregate aggregate) {
            this.aggregate = aggregate;
        }
    }

    /**
     * Holds a reference to a discovered Sort node.
     */
    static final class SortInfo {
        final Sort sort;

        SortInfo(Sort sort) {
            this.sort = sort;
        }
    }

    /**
     * Result of plan analysis containing the merge strategy and associated metadata.
     */
    static final class AnalysisResult {
        final MergeStrategy strategy;
        final SqlKind[] aggKinds;
        final int[] sortColumns;
        final boolean[] sortAsc;
        final int limit;

        AnalysisResult(MergeStrategy strategy) {
            this(strategy, null, null, null, 0);
        }

        AnalysisResult(MergeStrategy strategy, SqlKind[] aggKinds, int[] sortColumns, boolean[] sortAsc, int limit) {
            this.strategy = strategy;
            this.aggKinds = aggKinds;
            this.sortColumns = sortColumns;
            this.sortAsc = sortAsc;
            this.limit = limit;
        }
    }
}

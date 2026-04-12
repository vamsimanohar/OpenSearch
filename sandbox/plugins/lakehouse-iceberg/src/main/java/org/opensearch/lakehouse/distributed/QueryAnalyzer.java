/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.sql.SqlKind;

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
        AggregateInfo aggInfo = findAggregate(relNode);
        SortInfo sortInfo = findSort(relNode);

        if (aggInfo != null) {
            // Has GROUP BY → SINGLE_NODE (can't trivially merge grouped aggregates)
            if (!aggInfo.aggregate.getGroupSet().isEmpty()) {
                return MergeStrategy.SINGLE_NODE;
            }
            // Has DISTINCT or AVG → SINGLE_NODE
            if (hasDistinctOrAvg(aggInfo.aggregate)) {
                return MergeStrategy.SINGLE_NODE;
            }
            // Global aggregation with only SUM/COUNT/MIN/MAX → GLOBAL_MERGE
            return MergeStrategy.GLOBAL_MERGE;
        }

        if (sortInfo != null && sortInfo.sort.fetch != null) {
            // ORDER BY with LIMIT → TOPK_MERGE
            return MergeStrategy.TOPK_MERGE;
        }

        return MergeStrategy.CONCAT;
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
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Analyzes a Calcite logical plan to determine how distributed worker results
 * should be merged.
 *
 * <p>Walks the {@link RelNode} tree to find aggregation nodes and produces a
 * {@link DistributionPlan} describing the merge strategy. Supports COUNT, SUM,
 * MIN, and MAX aggregates. Queries containing AVG or other non-decomposable
 * aggregates are marked as {@link DistributionPlan.QueryType#UNSUPPORTED}.
 */
public final class DistributedPlanSplitter {

    private static final Logger logger = LogManager.getLogger(DistributedPlanSplitter.class);

    private DistributedPlanSplitter() {}

    /**
     * Analyzes a Calcite logical plan and returns a distribution plan.
     *
     * @param plan the root of the Calcite logical plan
     * @return a {@link DistributionPlan} describing how to merge worker results
     */
    public static DistributionPlan analyze(RelNode plan) {
        // Unwrap LogicalSort on top (ORDER BY / LIMIT) — sort is applied after merge
        RelNode current = plan;
        if (current instanceof LogicalSort) {
            current = ((LogicalSort) current).getInput();
        }

        // Check for LogicalProject on top of LogicalAggregate
        LogicalProject topProject = null;
        if (current instanceof LogicalProject) {
            topProject = (LogicalProject) current;
            current = topProject.getInput();
        }

        // Find LogicalAggregate
        LogicalAggregate aggregate = findAggregate(current);
        if (aggregate == null) {
            logger.debug("[DistributedPlanSplitter] No aggregate found — SCAN_ONLY");
            return DistributionPlan.scanOnly();
        }

        // Check all aggregate functions are supported (COUNT, SUM, MIN, MAX)
        List<AggregateCall> aggCalls = aggregate.getAggCallList();
        for (AggregateCall call : aggCalls) {
            DistributionPlan.MergeOp op = toMergeOp(call);
            if (op == null) {
                logger.debug("[DistributedPlanSplitter] Unsupported aggregate function: {}",
                    call.getAggregation().getName());
                return DistributionPlan.unsupported();
            }
        }

        int groupCount = aggregate.getGroupSet().cardinality();
        boolean isGlobal = groupCount == 0;

        if (isGlobal) {
            // Global aggregate: no group keys, just aggregate columns
            List<DistributionPlan.AggMergeInfo> merges = new ArrayList<>();
            for (int i = 0; i < aggCalls.size(); i++) {
                int outputCol = i; // Global agg output: [agg0, agg1, ...]
                DistributionPlan.MergeOp op = toMergeOp(aggCalls.get(i));
                merges.add(new DistributionPlan.AggMergeInfo(outputCol, op));
            }

            // If there's a project on top, remap output column indices
            if (topProject != null) {
                merges = remapMergesForProject(topProject, merges);
                if (merges == null) {
                    return DistributionPlan.unsupported();
                }
            }

            logger.debug("[DistributedPlanSplitter] GLOBAL_AGGREGATE with {} merges", merges.size());
            return DistributionPlan.globalAggregate(merges);
        }

        // Grouped aggregate: output is [groupKey0, groupKey1, ..., agg0, agg1, ...]
        // Group key columns occupy positions 0..groupCount-1
        // Aggregate columns occupy positions groupCount..groupCount+aggCalls.size()-1
        int[] groupKeyOutputColumns = new int[groupCount];
        for (int i = 0; i < groupCount; i++) {
            groupKeyOutputColumns[i] = i;
        }

        List<DistributionPlan.AggMergeInfo> merges = new ArrayList<>();
        for (int i = 0; i < aggCalls.size(); i++) {
            int outputCol = groupCount + i;
            DistributionPlan.MergeOp op = toMergeOp(aggCalls.get(i));
            merges.add(new DistributionPlan.AggMergeInfo(outputCol, op));
        }

        // If there's a project on top, check that all group keys are present
        if (topProject != null) {
            int[] remappedGroupKeys = remapGroupKeysForProject(topProject, groupKeyOutputColumns);
            if (remappedGroupKeys == null) {
                logger.debug("[DistributedPlanSplitter] Project removes group keys — UNSUPPORTED");
                return DistributionPlan.unsupported();
            }
            List<DistributionPlan.AggMergeInfo> remappedMerges = remapMergesForProject(topProject, merges);
            if (remappedMerges == null) {
                return DistributionPlan.unsupported();
            }
            logger.debug("[DistributedPlanSplitter] GROUPED_AGGREGATE with {} group keys, {} merges (remapped through project)",
                remappedGroupKeys.length, remappedMerges.size());
            return DistributionPlan.groupedAggregate(remappedGroupKeys, remappedMerges);
        }

        logger.debug("[DistributedPlanSplitter] GROUPED_AGGREGATE with {} group keys, {} merges",
            groupCount, merges.size());
        return DistributionPlan.groupedAggregate(groupKeyOutputColumns, merges);
    }

    /**
     * Maps an {@link AggregateCall} to the corresponding {@link DistributionPlan.MergeOp}.
     *
     * @return the merge operation, or {@code null} if the aggregate is not supported
     */
    static DistributionPlan.MergeOp toMergeOp(AggregateCall call) {
        SqlKind kind = call.getAggregation().getKind();
        switch (kind) {
            case COUNT:
                // COUNT partial results are summed across workers
                return DistributionPlan.MergeOp.SUM;
            case SUM:
            case SUM0:
                return DistributionPlan.MergeOp.SUM;
            case MIN:
                return DistributionPlan.MergeOp.MIN;
            case MAX:
                return DistributionPlan.MergeOp.MAX;
            default:
                // AVG, MEDIAN, etc. cannot be correctly merged from partial results
                return null;
        }
    }

    /**
     * Finds a {@link LogicalAggregate} node. Checks the given node directly.
     */
    private static LogicalAggregate findAggregate(RelNode node) {
        if (node instanceof LogicalAggregate) {
            return (LogicalAggregate) node;
        }
        return null;
    }

    /**
     * Remaps group key output column positions through a LogicalProject.
     *
     * <p>Checks that every group key position from the aggregate appears in the
     * project's output. Returns the new positions of group keys in the project's
     * output, or {@code null} if any group key is missing.
     *
     * @param project              the project on top of the aggregate
     * @param groupKeyAggPositions positions of group keys in the aggregate's output
     * @return remapped positions in the project's output, or {@code null} if a group key is missing
     */
    private static int[] remapGroupKeysForProject(LogicalProject project, int[] groupKeyAggPositions) {
        List<RexNode> projects = project.getProjects();
        int[] remapped = new int[groupKeyAggPositions.length];

        for (int g = 0; g < groupKeyAggPositions.length; g++) {
            int aggPos = groupKeyAggPositions[g];
            int newPos = -1;
            for (int p = 0; p < projects.size(); p++) {
                RexNode expr = projects.get(p);
                if (expr instanceof RexInputRef && ((RexInputRef) expr).getIndex() == aggPos) {
                    newPos = p;
                    break;
                }
            }
            if (newPos == -1) {
                // Group key not present in project output
                return null;
            }
            remapped[g] = newPos;
        }
        return remapped;
    }

    /**
     * Remaps aggregate merge info through a LogicalProject.
     *
     * <p>Each aggregate merge's output column index is translated from the
     * aggregate's output space to the project's output space.
     *
     * @param project the project on top of the aggregate
     * @param merges  merge info with aggregate output positions
     * @return remapped merge info with project output positions, or {@code null} if a column is missing
     */
    private static List<DistributionPlan.AggMergeInfo> remapMergesForProject(
        LogicalProject project, List<DistributionPlan.AggMergeInfo> merges
    ) {
        List<RexNode> projects = project.getProjects();
        List<DistributionPlan.AggMergeInfo> remapped = new ArrayList<>();

        for (DistributionPlan.AggMergeInfo merge : merges) {
            int aggPos = merge.getOutputColumnIndex();
            int newPos = -1;
            for (int p = 0; p < projects.size(); p++) {
                RexNode expr = projects.get(p);
                if (expr instanceof RexInputRef && ((RexInputRef) expr).getIndex() == aggPos) {
                    newPos = p;
                    break;
                }
            }
            if (newPos == -1) {
                // Aggregate column projected away — this is fine, skip it
                continue;
            }
            remapped.add(new DistributionPlan.AggMergeInfo(newPos, merge.getMergeOp()));
        }

        return remapped;
    }
}

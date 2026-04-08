/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed.fragmenter;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.sql.SqlKind;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.lakehouse.distributed.PhysicalPlanSplitter;
import org.opensearch.lakehouse.distributed.stage.InputSpec;
import org.opensearch.lakehouse.distributed.stage.PartitioningScheme;
import org.opensearch.lakehouse.distributed.stage.Stage;
import org.opensearch.lakehouse.distributed.stage.StageDAG;
import org.opensearch.lakehouse.distributed.stage.StageId;

import java.util.List;
import java.util.Map;

/**
 * Fragments a Calcite RelNode into a StageDAG for multi-stage distributed execution.
 *
 * <p>Walks the plan tree, identifies exchange boundaries, and produces stages
 * connected by partitioning schemes. Each stage contains a SQL string that
 * DataFusion can execute independently.
 *
 * <p>Currently supports single-table queries (aggregation, sort+limit, scan-only).
 * JOIN support is added in Task 7.
 */
public final class CalcitePlanFragmenter {

    private static final Logger logger = LogManager.getLogger(CalcitePlanFragmenter.class);
    private int nextStageId = 0;

    /** Creates a new CalcitePlanFragmenter. */
    public CalcitePlanFragmenter() {}

    /**
     * Fragments a Calcite plan into a StageDAG.
     *
     * @param plan       the Calcite logical plan
     * @param tableName  the table name as registered in DataFusion
     * @param numWorkers number of available worker nodes
     * @return the stage DAG, or null if fragmentation is not possible
     */
    public StageDAG fragment(RelNode plan, String tableName, int numWorkers) {
        nextStageId = 0;

        // Peel off layers: Sort? -> Project? -> Aggregate? -> Filter? -> Scan
        Sort sort = null;
        Aggregate aggregate = null;
        RelNode current = plan;

        if (current instanceof Sort s) {
            sort = s;
            current = s.getInput();
        }
        if (current instanceof Project p) {
            current = p.getInput();
        }
        if (current instanceof Aggregate a) {
            aggregate = a;
            current = a.getInput();
        }

        // Check if we have an aggregate to distribute
        if (aggregate != null && canDistributeAggregate(aggregate)) {
            return fragmentAggregate(plan, tableName, numWorkers);
        }

        // Sort + Limit without aggregate: distributed top-K
        if (sort != null && sort.fetch != null) {
            return fragmentSortLimit(plan, tableName, numWorkers);
        }

        // Scan-only: single stage, no fragmentation benefit for multi-stage
        logger.debug("[PlanFragmenter] Plan not suitable for multi-stage fragmentation");
        return null;
    }

    private StageDAG fragmentAggregate(RelNode plan, String tableName, int numWorkers) {
        PhysicalPlanSplitter.SplitPlan splitPlan = PhysicalPlanSplitter.split(plan, tableName);
        if (!splitPlan.canDistribute()) {
            logger.debug("[PlanFragmenter] PhysicalPlanSplitter cannot distribute this aggregate");
            return null;
        }

        // Stage 0: Scan + partial aggregate (runs on all workers)
        StageId scanId = newStageId();
        Stage scanStage = new Stage(
            scanId,
            splitPlan.getWorkerSql(),
            tableName,
            new InputSpec.ScanInput(List.of(), Map.of()),
            PartitioningScheme.gather(),
            Stage.StageType.SCAN,
            List.of()
        );

        // Stage 1: Final aggregate (runs on coordinator)
        StageId finalId = newStageId();
        Stage finalStage = new Stage(
            finalId,
            splitPlan.getCoordinatorSql(),
            PhysicalPlanSplitter.PARTIAL_TABLE,
            new InputSpec.ExchangeInput(Map.of(scanId, PhysicalPlanSplitter.PARTIAL_TABLE)),
            PartitioningScheme.gather(),
            Stage.StageType.FINAL,
            List.of(scanId)
        );

        logger.info("[PlanFragmenter] Fragmented aggregate query into 2 stages: scan=[{}], final=[{}]",
            splitPlan.getWorkerSql(), splitPlan.getCoordinatorSql());

        return new StageDAG(List.of(scanStage, finalStage), finalId);
    }

    private StageDAG fragmentSortLimit(RelNode plan, String tableName, int numWorkers) {
        PhysicalPlanSplitter.SplitPlan splitPlan = PhysicalPlanSplitter.split(plan, tableName);
        if (!splitPlan.canDistribute()) {
            return null;
        }

        StageId scanId = newStageId();
        Stage scanStage = new Stage(scanId,
            splitPlan.getWorkerSql(), tableName,
            new InputSpec.ScanInput(List.of(), Map.of()),
            PartitioningScheme.gather(),
            Stage.StageType.SCAN, List.of());

        StageId finalId = newStageId();
        Stage finalStage = new Stage(finalId,
            splitPlan.getCoordinatorSql(), PhysicalPlanSplitter.PARTIAL_TABLE,
            new InputSpec.ExchangeInput(Map.of(scanId, PhysicalPlanSplitter.PARTIAL_TABLE)),
            PartitioningScheme.gather(),
            Stage.StageType.FINAL, List.of(scanId));

        logger.info("[PlanFragmenter] Fragmented sort+limit query into 2 stages");
        return new StageDAG(List.of(scanStage, finalStage), finalId);
    }

    private boolean canDistributeAggregate(Aggregate aggregate) {
        for (AggregateCall call : aggregate.getAggCallList()) {
            SqlKind kind = call.getAggregation().getKind();
            if (kind != SqlKind.COUNT && kind != SqlKind.SUM && kind != SqlKind.SUM0
                && kind != SqlKind.MIN && kind != SqlKind.MAX && kind != SqlKind.AVG) {
                return false;
            }
        }
        return true;
    }

    private StageId newStageId() {
        return new StageId(nextStageId++);
    }
}

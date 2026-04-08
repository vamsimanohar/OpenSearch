/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.lakehouse.distributed.fragmenter.CalcitePlanFragmenter;
import org.opensearch.lakehouse.distributed.scheduler.StageScheduler;
import org.opensearch.lakehouse.distributed.stage.Stage;
import org.opensearch.lakehouse.distributed.stage.StageDAG;
import org.opensearch.lakehouse.scan.IcebergScanPlan;
import org.opensearch.transport.TransportService;

import java.util.List;
import java.util.Map;

/**
 * Top-level orchestrator for multi-stage distributed query execution.
 *
 * <p>Tries to fragment the query into a multi-stage DAG. If successful,
 * uses StageScheduler for phased execution. Falls back to the existing
 * single-stage DistributedQueryCoordinator if fragmentation is not possible.
 */
public final class MultiStageCoordinator {

    private static final Logger logger = LogManager.getLogger(MultiStageCoordinator.class);

    private final ClusterService clusterService;
    private final TransportService transportService;
    private final DistributedQueryCoordinator fallbackCoordinator;

    /**
     * Creates a new MultiStageCoordinator.
     *
     * @param clusterService      the cluster service for node discovery
     * @param transportService    the transport service for worker communication
     * @param fallbackCoordinator the single-stage coordinator to fall back to
     */
    public MultiStageCoordinator(ClusterService clusterService,
                                  TransportService transportService,
                                  DistributedQueryCoordinator fallbackCoordinator) {
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.fallbackCoordinator = fallbackCoordinator;
    }

    /**
     * Attempts multi-stage execution. Falls back to single-stage scatter-gather
     * if the query cannot be fragmented into multiple stages.
     *
     * @param logicalPlan   the Calcite logical plan
     * @param tableName     the table name
     * @param files         the Iceberg scan plan files
     * @param storageConfig storage configuration
     * @param splitPlan     the existing single-stage split plan (for fallback)
     * @return the query result rows
     */
    public Iterable<Object[]> execute(RelNode logicalPlan,
                                       String tableName,
                                       List<IcebergScanPlan.FileInfo> files,
                                       Map<String, String> storageConfig,
                                       PhysicalPlanSplitter.SplitPlan splitPlan) {
        int numWorkers = getDataNodeCount();

        // Try multi-stage fragmentation
        CalcitePlanFragmenter fragmenter = new CalcitePlanFragmenter();
        StageDAG dag = fragmenter.fragment(logicalPlan, tableName, numWorkers);

        if (dag != null && dag.stageCount() > 1) {
            logger.info("[MultiStageCoordinator] Using multi-stage execution: {} stages", dag.stageCount());
            logger.info("[MultiStageCoordinator] Execution plan:\n{}", formatPlan(dag));

            StageScheduler scheduler = new StageScheduler(clusterService, transportService);
            return scheduler.execute(dag, files, storageConfig);
        }

        // Fall back to existing single-stage scatter-gather
        logger.info("[MultiStageCoordinator] Falling back to single-stage scatter-gather");
        return fallbackCoordinator.execute(splitPlan, files, storageConfig, tableName);
    }

    /**
     * Returns the StageDAG for explain API without executing.
     *
     * @param logicalPlan the Calcite logical plan
     * @param tableName   the table name
     * @return the stage DAG, or null if fragmentation is not possible
     */
    public StageDAG explain(RelNode logicalPlan, String tableName) {
        CalcitePlanFragmenter fragmenter = new CalcitePlanFragmenter();
        return fragmenter.fragment(logicalPlan, tableName, getDataNodeCount());
    }

    private int getDataNodeCount() {
        return clusterService.state().nodes().getDataNodes().size();
    }

    private String formatPlan(StageDAG dag) {
        StringBuilder sb = new StringBuilder();
        for (Stage stage : dag.topologicalOrder()) {
            sb.append(String.format("  %s [%s]: %s\n    input: %s\n    output: %s\n",
                stage.getId(), stage.getType(), stage.getSql(),
                stage.getInputSpec(), stage.getOutputPartitioning()));
        }
        return sb.toString();
    }
}

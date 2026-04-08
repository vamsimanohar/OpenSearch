/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed.scheduler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.exec.ExternalScanContext;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.lakehouse.distributed.FilePartitioner;
import org.opensearch.lakehouse.distributed.LakehouseWorkerAction;
import org.opensearch.lakehouse.distributed.LakehouseWorkerRequest;
import org.opensearch.lakehouse.distributed.LakehouseWorkerResponse;
import org.opensearch.lakehouse.distributed.exchange.ExchangeService;
import org.opensearch.lakehouse.distributed.stage.InputSpec;
import org.opensearch.lakehouse.distributed.stage.Stage;
import org.opensearch.lakehouse.distributed.stage.StageDAG;
import org.opensearch.lakehouse.distributed.stage.StageId;
import org.opensearch.lakehouse.scan.IcebergScanPlan;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Executes a StageDAG with phased dependency tracking.
 *
 * <p>Stages execute in topological order. Leaf stages (scans) run first
 * in parallel. Once a stage completes, its output is registered in the
 * ExchangeService. Downstream stages start only when all their dependencies
 * have completed.
 *
 * <p>FINAL stages execute on the coordinator node using the DataFusion
 * native merge (same as current IPC merge path).
 */
public final class StageScheduler {

    private static final Logger logger = LogManager.getLogger(StageScheduler.class);
    private static final long STAGE_TIMEOUT_MINUTES = 5;

    private final ClusterService clusterService;
    private final TransportService transportService;
    private final ExchangeService exchangeService;

    /**
     * Creates a new StageScheduler.
     *
     * @param clusterService   the cluster service for node discovery
     * @param transportService the transport service for sending requests
     */
    public StageScheduler(ClusterService clusterService, TransportService transportService) {
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.exchangeService = new ExchangeService();
    }

    /**
     * Executes a StageDAG and returns the final results.
     *
     * @param dag           the stage DAG to execute
     * @param files         data files from the Iceberg scan plan
     * @param storageConfig S3/storage configuration
     * @return final result rows
     */
    public Iterable<Object[]> execute(StageDAG dag,
                                       List<IcebergScanPlan.FileInfo> files,
                                       Map<String, String> storageConfig) {
        long t0 = System.nanoTime();
        logger.info("[StageScheduler] ====== MULTI-STAGE EXECUTION START ======");
        logger.info("[StageScheduler] DAG: {} stages, root={}", dag.stageCount(), dag.getRootStageId());

        List<DiscoveryNode> dataNodes = getDataNodes();
        logger.info("[StageScheduler] Available workers: {}", dataNodes.size());

        // Track execution state per stage
        Map<StageId, StageExecution> executions = new LinkedHashMap<>();
        for (Stage stage : dag.getStages()) {
            executions.put(stage.getId(), new StageExecution(stage.getId()));
            exchangeService.registerOutputScheme(stage.getId(), stage.getOutputPartitioning());
        }

        Iterable<Object[]> finalResult = null;

        try {
            // Execute stages in topological order
            for (Stage stage : dag.topologicalOrder()) {
                StageExecution exec = executions.get(stage.getId());
                logger.info("[StageScheduler] ------ Stage {} ({}) ------", stage.getId(), stage.getType());

                // Verify all dependencies completed successfully
                for (StageId dep : stage.getSourceStages()) {
                    StageExecution depExec = executions.get(dep);
                    if (!depExec.isTerminal()) {
                        throw new IllegalStateException("Dependency " + dep + " not completed before " + stage.getId());
                    }
                    if (depExec.getState() == StageExecution.State.FAILED) {
                        throw new RuntimeException("Dependency " + dep + " failed", depExec.getFailureCause());
                    }
                }

                exec.transitionTo(StageExecution.State.RUNNING);

                if (stage.getType() == Stage.StageType.FINAL) {
                    finalResult = executeFinalStage(stage, exec);
                } else {
                    executeWorkerStage(stage, exec, files, storageConfig, dataNodes);
                }

                logger.info("[StageScheduler] Stage {} completed in {} ms",
                    stage.getId(), exec.getElapsedMs());
            }
        } catch (Exception e) {
            logger.error("[StageScheduler] Multi-stage execution failed", e);
            throw new RuntimeException("Multi-stage execution failed", e);
        } finally {
            exchangeService.clear();
        }

        long t1 = System.nanoTime();
        logger.info("[StageScheduler] ====== MULTI-STAGE EXECUTION END ({} ms) ======",
            (t1 - t0) / 1_000_000);

        if (finalResult == null) {
            throw new IllegalStateException("No final results from root stage " + dag.getRootStageId());
        }
        return finalResult;
    }

    private void executeWorkerStage(Stage stage, StageExecution exec,
                                     List<IcebergScanPlan.FileInfo> files,
                                     Map<String, String> storageConfig,
                                     List<DiscoveryNode> dataNodes) {
        List<List<IcebergScanPlan.FileInfo>> partitions = FilePartitioner.partition(files, dataNodes.size());

        CountDownLatch latch = new CountDownLatch(partitions.size());
        AtomicReference<Exception> firstError = new AtomicReference<>();

        for (int i = 0; i < partitions.size(); i++) {
            DiscoveryNode targetNode = dataNodes.get(i);
            List<IcebergScanPlan.FileInfo> partition = partitions.get(i);

            String[] filePaths = partition.stream()
                .map(IcebergScanPlan.FileInfo::getPath)
                .toArray(String[]::new);

            LakehouseWorkerRequest request = new LakehouseWorkerRequest(
                filePaths, stage.getSql(), storageConfig, stage.getTableName()
            );

            logger.info("[StageScheduler] {} task[{}] -> {}: {} files, sql={}",
                stage.getId(), i, targetNode.getName(), filePaths.length, stage.getSql());

            final int taskIdx = i;
            transportService.sendRequest(targetNode, LakehouseWorkerAction.NAME, request,
                new TransportResponseHandler<LakehouseWorkerResponse>() {
                    @Override
                    public LakehouseWorkerResponse read(StreamInput in) throws IOException {
                        return new LakehouseWorkerResponse(in);
                    }

                    @Override
                    public void handleResponse(LakehouseWorkerResponse response) {
                        if (response.hasIpcBytes()) {
                            exchangeService.addStageOutput(stage.getId(), response.getIpcBytes());
                            logger.info("[StageScheduler] {} task[{}] returned {} IPC bytes from {}",
                                stage.getId(), taskIdx, response.getIpcBytes().length, targetNode.getName());
                        }
                        latch.countDown();
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        logger.error("[StageScheduler] {} worker failed on {}",
                            stage.getId(), targetNode.getName(), exp);
                        firstError.compareAndSet(null, exp);
                        latch.countDown();
                    }

                    @Override
                    public String executor() { return ThreadPool.Names.GENERIC; }
                }
            );
        }

        try {
            boolean done = latch.await(STAGE_TIMEOUT_MINUTES, TimeUnit.MINUTES);
            if (!done) throw new RuntimeException(stage.getId() + " timed out");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(stage.getId() + " interrupted", e);
        }

        Exception error = firstError.get();
        if (error != null) {
            exec.fail(error);
            throw new RuntimeException(stage.getId() + " failed", error);
        }

        exec.transitionTo(StageExecution.State.FINISHED);
        logger.info("[StageScheduler] {} finished: {} IPC outputs collected",
            stage.getId(), exchangeService.getOutputCount(stage.getId()));
    }

    private Iterable<Object[]> executeFinalStage(Stage stage, StageExecution exec) {
        InputSpec input = stage.getInputSpec();
        if (!(input instanceof InputSpec.ExchangeInput exchangeInput)) {
            throw new IllegalStateException("Final stage must have exchange input");
        }

        // Collect all upstream IPC batches
        List<byte[]> allBatches = new ArrayList<>();
        for (Map.Entry<StageId, String> entry : exchangeInput.getSourceTableNames().entrySet()) {
            byte[][] outputs = exchangeService.getAllOutputs(entry.getKey());
            logger.info("[StageScheduler] Final stage collecting from {}: {} IPC batches",
                entry.getKey(), outputs.length);
            Collections.addAll(allBatches, outputs);
        }

        // Use the global backend executor for DataFusion merge
        Function<ExternalScanContext, Iterable<Object[]>> executor =
            ExternalScanContext.getGlobalBackendExecutor();
        if (executor == null) {
            throw new IllegalStateException("No global backend executor for merge");
        }

        byte[][] ipcBatches = allBatches.toArray(new byte[0][]);
        ExternalScanContext mergeContext = new ExternalScanContext(
            stage.getTableName(), List.of(), stage.getSql(), Map.of()
        );
        mergeContext.setIpcBatches(ipcBatches);

        logger.info("[StageScheduler] Final stage merge: {} IPC batches, sql={}",
            ipcBatches.length, stage.getSql());

        Iterable<Object[]> result = executor.apply(mergeContext);

        exec.transitionTo(StageExecution.State.FINISHED);
        return result;
    }

    private List<DiscoveryNode> getDataNodes() {
        DiscoveryNodes nodes = clusterService.state().nodes();
        return new ArrayList<>(nodes.getDataNodes().values());
    }

    /** Returns the exchange service used for inter-stage data routing. */
    public ExchangeService getExchangeService() { return exchangeService; }
}

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
import org.opensearch.lakehouse.distributed.exchange.ExchangePullAction;
import org.opensearch.lakehouse.distributed.exchange.ExchangePullRequest;
import org.opensearch.lakehouse.distributed.exchange.ExchangePullResponse;
import org.opensearch.lakehouse.distributed.exchange.ExchangeService;
import org.opensearch.lakehouse.distributed.exchange.WorkerOutputManager;
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
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Executes a StageDAG with phased dependency tracking and pull-based exchange.
 *
 * <p>Stages execute in topological order. Leaf stages (scans) run first
 * in parallel on worker nodes. Each worker stores its IPC output in
 * {@link WorkerOutputManager} on its local node.
 *
 * <p>FINAL stages execute on the coordinator node. The coordinator pulls
 * IPC bytes from each worker via {@link ExchangePullAction} transport,
 * then merges using DataFusion native merge.
 *
 * <p>The push-based {@link ExchangeService} is still used as a fallback
 * (workers also send IPC bytes in their transport response).
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
        String queryId = UUID.randomUUID().toString();

        logger.info("[StageScheduler] ====== MULTI-STAGE EXECUTION START (queryId={}) ======", queryId);
        logger.info("[StageScheduler] DAG: {} stages, root={}", dag.stageCount(), dag.getRootStageId());

        List<DiscoveryNode> dataNodes = getDataNodes();
        logger.info("[StageScheduler] Available workers: {}", dataNodes.size());

        // Track execution state per stage
        Map<StageId, StageExecution> executions = new LinkedHashMap<>();
        for (Stage stage : dag.getStages()) {
            executions.put(stage.getId(), new StageExecution(stage.getId()));
            exchangeService.registerOutputScheme(stage.getId(), stage.getOutputPartitioning());
        }

        // Track which nodes ran each stage (for pull-based exchange)
        Map<StageId, List<NodeAssignment>> stageNodeAssignments = new LinkedHashMap<>();

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

                switch (stage.getType()) {
                    case FINAL:
                        finalResult = executeFinalStage(stage, exec, queryId, stageNodeAssignments);
                        break;
                    case INTERMEDIATE:
                        executeIntermediateStage(stage, exec, dataNodes, queryId, stageNodeAssignments);
                        break;
                    case SCAN:
                    default:
                        executeWorkerStage(stage, exec, files, storageConfig, dataNodes, queryId, stageNodeAssignments);
                        break;
                }

                logger.info("[StageScheduler] Stage {} completed in {} ms",
                    stage.getId(), exec.getElapsedMs());
            }
        } catch (Exception e) {
            logger.error("[StageScheduler] Multi-stage execution failed", e);
            throw new RuntimeException("Multi-stage execution failed", e);
        } finally {
            exchangeService.clear();
            cleanupWorkerOutputs(queryId);
        }

        long t1 = System.nanoTime();
        logger.info("[StageScheduler] ====== MULTI-STAGE EXECUTION END (queryId={}, {} ms) ======",
            queryId, (t1 - t0) / 1_000_000);

        if (finalResult == null) {
            throw new IllegalStateException("No final results from root stage " + dag.getRootStageId());
        }
        return finalResult;
    }

    private void executeWorkerStage(Stage stage, StageExecution exec,
                                     List<IcebergScanPlan.FileInfo> files,
                                     Map<String, String> storageConfig,
                                     List<DiscoveryNode> dataNodes,
                                     String queryId,
                                     Map<StageId, List<NodeAssignment>> stageNodeAssignments) {
        List<List<IcebergScanPlan.FileInfo>> partitions = FilePartitioner.partition(files, dataNodes.size());

        CountDownLatch latch = new CountDownLatch(partitions.size());
        AtomicReference<Exception> firstError = new AtomicReference<>();
        List<NodeAssignment> assignments = Collections.synchronizedList(new ArrayList<>());

        String stageIdStr = stage.getId().toString();

        for (int i = 0; i < partitions.size(); i++) {
            DiscoveryNode targetNode = dataNodes.get(i);
            List<IcebergScanPlan.FileInfo> partition = partitions.get(i);

            String[] filePaths = partition.stream()
                .map(IcebergScanPlan.FileInfo::getPath)
                .toArray(String[]::new);

            // Include queryId and stageId so worker stores output in WorkerOutputManager
            LakehouseWorkerRequest request = new LakehouseWorkerRequest(
                filePaths, stage.getSql(), storageConfig, stage.getTableName(),
                queryId, stageIdStr
            );

            logger.info("[StageScheduler] {} task[{}] -> {} ({}): {} files, queryId={}, stageId={}, sql={}",
                stage.getId(), i, targetNode.getName(), targetNode.getId(), filePaths.length,
                queryId, stageIdStr, stage.getSql());

            final int taskIdx = i;
            transportService.sendRequest(targetNode, LakehouseWorkerAction.NAME, request,
                new TransportResponseHandler<LakehouseWorkerResponse>() {
                    @Override
                    public LakehouseWorkerResponse read(StreamInput in) throws IOException {
                        return new LakehouseWorkerResponse(in);
                    }

                    @Override
                    public void handleResponse(LakehouseWorkerResponse response) {
                        // Record which node ran this stage task
                        assignments.add(new NodeAssignment(targetNode.getId(), targetNode));

                        // Also store in ExchangeService as fallback
                        if (response.hasIpcBytes()) {
                            exchangeService.addStageOutput(stage.getId(), response.getIpcBytes());
                            logger.info("[StageScheduler] {} task[{}] returned {} IPC bytes from {} (also stored on worker for pull)",
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

        // Record node assignments for this stage (used by downstream stages to pull)
        stageNodeAssignments.put(stage.getId(), new ArrayList<>(assignments));

        exec.transitionTo(StageExecution.State.FINISHED);
        logger.info("[StageScheduler] {} finished: {} workers assigned, {} IPC outputs in ExchangeService (fallback)",
            stage.getId(), assignments.size(), exchangeService.getOutputCount(stage.getId()));
    }

    private void executeIntermediateStage(Stage stage, StageExecution exec,
                                             List<DiscoveryNode> dataNodes,
                                             String queryId,
                                             Map<StageId, List<NodeAssignment>> stageNodeAssignments) {
        InputSpec input = stage.getInputSpec();
        if (!(input instanceof InputSpec.ExchangeInput exchangeInput)) {
            throw new IllegalStateException("INTERMEDIATE stage must have exchange input");
        }

        String stageIdStr = stage.getId().toString();

        // Build exchange inputs: for each source stage, include which nodes ran it
        List<LakehouseWorkerRequest.ExchangeInput> workerExchangeInputs = new ArrayList<>();
        for (Map.Entry<StageId, String> entry : exchangeInput.getSourceTableNames().entrySet()) {
            StageId sourceStageId = entry.getKey();
            String memTableName = entry.getValue();
            List<NodeAssignment> sourceNodes = stageNodeAssignments.getOrDefault(sourceStageId, List.of());
            List<String> nodeIds = sourceNodes.stream().map(NodeAssignment::nodeId).toList();

            workerExchangeInputs.add(new LakehouseWorkerRequest.ExchangeInput(
                sourceStageId.toString(), memTableName, nodeIds
            ));
            logger.info("[StageScheduler] INTERMEDIATE {} pulls from {} ({} nodes) as table '{}'",
                stage.getId(), sourceStageId, nodeIds.size(), memTableName);
        }

        // Dispatch to all data nodes (each worker pulls from all upstream nodes)
        CountDownLatch latch = new CountDownLatch(dataNodes.size());
        AtomicReference<Exception> firstError = new AtomicReference<>();
        List<NodeAssignment> assignments = Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < dataNodes.size(); i++) {
            DiscoveryNode targetNode = dataNodes.get(i);

            LakehouseWorkerRequest request = new LakehouseWorkerRequest(
                new String[0], stage.getSql(), Map.of(), stage.getTableName(),
                queryId, stageIdStr, workerExchangeInputs
            );

            logger.info("[StageScheduler] INTERMEDIATE {} task[{}] -> {}: sql={}",
                stage.getId(), i, targetNode.getName(), stage.getSql());

            final int taskIdx = i;
            transportService.sendRequest(targetNode, LakehouseWorkerAction.NAME, request,
                new TransportResponseHandler<LakehouseWorkerResponse>() {
                    @Override
                    public LakehouseWorkerResponse read(StreamInput in) throws IOException {
                        return new LakehouseWorkerResponse(in);
                    }

                    @Override
                    public void handleResponse(LakehouseWorkerResponse response) {
                        assignments.add(new NodeAssignment(targetNode.getId(), targetNode));
                        if (response.hasIpcBytes()) {
                            exchangeService.addStageOutput(stage.getId(), response.getIpcBytes());
                            logger.info("[StageScheduler] INTERMEDIATE {} task[{}] returned {} IPC bytes from {}",
                                stage.getId(), taskIdx, response.getIpcBytes().length, targetNode.getName());
                        }
                        latch.countDown();
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        logger.error("[StageScheduler] INTERMEDIATE {} worker failed on {}",
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

        stageNodeAssignments.put(stage.getId(), new ArrayList<>(assignments));
        exec.transitionTo(StageExecution.State.FINISHED);
        logger.info("[StageScheduler] INTERMEDIATE {} finished: {} workers", stage.getId(), assignments.size());
    }

    private Iterable<Object[]> executeFinalStage(Stage stage, StageExecution exec,
                                                   String queryId,
                                                   Map<StageId, List<NodeAssignment>> stageNodeAssignments) {
        InputSpec input = stage.getInputSpec();
        if (!(input instanceof InputSpec.ExchangeInput exchangeInput)) {
            throw new IllegalStateException("Final stage must have exchange input");
        }

        // Try pull-based exchange: pull IPC bytes from workers
        List<byte[]> allBatches = new ArrayList<>();
        boolean pullSucceeded = false;

        for (Map.Entry<StageId, String> entry : exchangeInput.getSourceTableNames().entrySet()) {
            StageId sourceStageId = entry.getKey();
            List<NodeAssignment> workerNodes = stageNodeAssignments.getOrDefault(sourceStageId, List.of());

            if (!workerNodes.isEmpty()) {
                logger.info("[StageScheduler] Final stage pulling from {} workers for {}",
                    workerNodes.size(), sourceStageId);

                List<byte[]> pulled = pullFromWorkers(queryId, sourceStageId.toString(), workerNodes);
                if (!pulled.isEmpty()) {
                    allBatches.addAll(pulled);
                    pullSucceeded = true;
                    logger.info("[StageScheduler] Pulled {} IPC batches from workers for {}",
                        pulled.size(), sourceStageId);
                }
            }
        }

        // Fallback to ExchangeService if pull didn't work
        if (!pullSucceeded) {
            logger.info("[StageScheduler] Pull-based exchange returned no data, falling back to ExchangeService");
            for (Map.Entry<StageId, String> entry : exchangeInput.getSourceTableNames().entrySet()) {
                byte[][] outputs = exchangeService.getAllOutputs(entry.getKey());
                logger.info("[StageScheduler] Final stage collecting from {} (ExchangeService fallback): {} IPC batches",
                    entry.getKey(), outputs.length);
                Collections.addAll(allBatches, outputs);
            }
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

        logger.info("[StageScheduler] Final stage merge: {} IPC batches (pull={}), sql={}",
            ipcBatches.length, pullSucceeded, stage.getSql());

        Iterable<Object[]> result = executor.apply(mergeContext);

        exec.transitionTo(StageExecution.State.FINISHED);
        return result;
    }

    /**
     * Pulls IPC output from worker nodes via the ExchangePull transport action.
     */
    private List<byte[]> pullFromWorkers(String queryId, String stageId, List<NodeAssignment> workers) {
        List<byte[]> results = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(workers.size());
        AtomicReference<Exception> firstError = new AtomicReference<>();

        for (NodeAssignment worker : workers) {
            ExchangePullRequest pullReq = new ExchangePullRequest(queryId, stageId);

            logger.info("[StageScheduler] Pulling from worker {} for queryId={}, stageId={}",
                worker.node().getName(), queryId, stageId);

            transportService.sendRequest(worker.node(), ExchangePullAction.NAME, pullReq,
                new TransportResponseHandler<ExchangePullResponse>() {
                    @Override
                    public ExchangePullResponse read(StreamInput in) throws IOException {
                        return new ExchangePullResponse(in);
                    }

                    @Override
                    public void handleResponse(ExchangePullResponse response) {
                        if (response.hasData()) {
                            results.add(response.getIpcBytes());
                            logger.info("[StageScheduler] Pulled {} bytes from {}",
                                response.getIpcBytes().length, worker.node().getName());
                        } else {
                            logger.warn("[StageScheduler] No data pulled from {} for queryId={}, stageId={}",
                                worker.node().getName(), queryId, stageId);
                        }
                        latch.countDown();
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        logger.error("[StageScheduler] Pull failed from {}", worker.node().getName(), exp);
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
            if (!done) {
                logger.warn("[StageScheduler] Pull timed out for queryId={}, stageId={}", queryId, stageId);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("[StageScheduler] Pull interrupted for queryId={}, stageId={}", queryId, stageId);
        }

        if (firstError.get() != null) {
            logger.warn("[StageScheduler] Pull had errors, will fall back to ExchangeService", firstError.get());
            return List.of();
        }

        return results;
    }

    /**
     * Cleans up WorkerOutputManager on the local node after query completes.
     */
    private void cleanupWorkerOutputs(String queryId) {
        WorkerOutputManager.instance().cleanup(queryId);
    }

    private List<DiscoveryNode> getDataNodes() {
        DiscoveryNodes nodes = clusterService.state().nodes();
        return new ArrayList<>(nodes.getDataNodes().values());
    }

    /** Returns the exchange service used for inter-stage data routing. */
    public ExchangeService getExchangeService() { return exchangeService; }

    /** Records which node ran a stage task. */
    record NodeAssignment(String nodeId, DiscoveryNode node) {}
}

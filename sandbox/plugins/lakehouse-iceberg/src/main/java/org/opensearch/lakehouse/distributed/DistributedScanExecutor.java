/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.exec.DataWarehouseQueryEngine;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.datafusion.DataFusionService;
import org.opensearch.be.datafusion.DatafusionResultStream;
import org.opensearch.be.datafusion.NativeRuntimeHandle;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.StreamHandle;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.FutureUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.lakehouse.LakehousePlugin;
import org.opensearch.lakehouse.distributed.merge.ResultSerializer;
import org.opensearch.lakehouse.distributed.merge.WorkerResponseToArrow;
import org.opensearch.lakehouse.distributed.worker.WorkerQueryAction;
import org.opensearch.lakehouse.distributed.worker.WorkerQueryExecutor;
import org.opensearch.lakehouse.distributed.worker.WorkerQueryRequest;
import org.opensearch.lakehouse.distributed.worker.WorkerQueryResponse;
import org.opensearch.lakehouse.engine.ExchangeType;
import org.opensearch.lakehouse.engine.PlanFragment;
import org.opensearch.lakehouse.engine.PlanFragmenter;
import org.opensearch.lakehouse.engine.SubPlan;
import org.opensearch.lakehouse.execution.QueryState;
import org.opensearch.lakehouse.execution.QueryStateMachine;
import org.opensearch.lakehouse.execution.StageState;
import org.opensearch.lakehouse.execution.StageStateMachine;
import org.opensearch.lakehouse.execution.TaskState;
import org.opensearch.lakehouse.execution.TaskStateMachine;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unified scan executor that handles both single-node and distributed query execution.
 * <p>
 * Fully asynchronous: dispatches worker queries via transport or local thread pool,
 * collects responses via per-task state machines, and delivers merged results
 * through an {@link ActionListener} callback. No thread ever blocks waiting for results.
 * <p>
 * When multiple eligible worker nodes are available and the query is distributable,
 * splits files across workers, dispatches in parallel, and merges results.
 * Otherwise, delegates to single-node execution via {@link WorkerQueryExecutor}.
 *
 * @opensearch.internal
 */
public class DistributedScanExecutor {

    private static final Logger logger = LogManager.getLogger(DistributedScanExecutor.class);

    /** Timeout for native coordinator SQL execution. */
    static final long NATIVE_TIMEOUT_MINUTES = 15L;

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final NodeDiscovery nodeDiscovery;
    private final DataWarehouseQueryEngine queryEngine;
    private final NativeIpcExecutor nativeIpcExecutor;
    private final java.util.concurrent.Executor stateExecutor;

    /**
     * Creates a new DistributedScanExecutor.
     *
     * @param transportService the transport service for sending requests to remote nodes
     * @param clusterService   the cluster service for node discovery
     * @param queryEngine     the external query backend for executing queries
     */
    public DistributedScanExecutor(TransportService transportService, ClusterService clusterService, DataWarehouseQueryEngine queryEngine) {
        this(transportService, clusterService, new NodeDiscovery(clusterService), queryEngine, NativeBridge::executeFromIpcAsync);
    }

    /**
     * Constructor that accepts a pre-built NodeDiscovery (for testing).
     *
     * @param transportService the transport service
     * @param clusterService   the cluster service
     * @param nodeDiscovery    the node discovery instance
     * @param queryEngine     the external query backend for executing queries
     */
    DistributedScanExecutor(TransportService transportService, ClusterService clusterService, NodeDiscovery nodeDiscovery, DataWarehouseQueryEngine queryEngine) {
        this(transportService, clusterService, nodeDiscovery, queryEngine, NativeBridge::executeFromIpcAsync, Runnable::run);
    }

    /**
     * Full constructor exposing test seams for native IPC execution.
     *
     * @param transportService   the transport service
     * @param clusterService     the cluster service
     * @param nodeDiscovery      the node discovery instance
     * @param queryEngine        the external query backend for executing queries
     * @param nativeIpcExecutor  seam for the native IPC execution call
     */
    DistributedScanExecutor(
        TransportService transportService,
        ClusterService clusterService,
        NodeDiscovery nodeDiscovery,
        DataWarehouseQueryEngine queryEngine,
        NativeIpcExecutor nativeIpcExecutor
    ) {
        this(transportService, clusterService, nodeDiscovery, queryEngine, nativeIpcExecutor, Runnable::run);
    }

    /**
     * Full constructor exposing all test seams.
     */
    DistributedScanExecutor(
        TransportService transportService,
        ClusterService clusterService,
        NodeDiscovery nodeDiscovery,
        DataWarehouseQueryEngine queryEngine,
        NativeIpcExecutor nativeIpcExecutor,
        java.util.concurrent.Executor stateExecutor
    ) {
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.nodeDiscovery = nodeDiscovery;
        this.queryEngine = queryEngine;
        this.nativeIpcExecutor = nativeIpcExecutor;
        this.stateExecutor = stateExecutor;
    }

    /**
     * Executes the query asynchronously, choosing between distributed and single-node paths.
     * <p>
     * Uses {@link PlanFragmenter} to decompose the query into a {@link SubPlan} of stages.
     * Distributed execution is used when multiple eligible workers are available and the
     * plan is distributable. Otherwise, falls back to single-node execution.
     * <p>
     * For 2-stage plans (GATHER exchange): dispatches leaf SQL to workers, merges with
     * coordinator SQL via DataFusion. For 3-stage plans (HASH exchange): dispatches leaf
     * SQL to workers, runs intermediate SQL on coordinator (P1 simplification), then
     * concatenates results.
     *
     * @param relNode       the Calcite logical plan (for query analysis)
     * @param sqlQuery      the SQL query string to send to workers
     * @param filePaths     the data file paths to distribute
     * @param fileSizes     file sizes in bytes, parallel to filePaths
     * @param storageConfig storage configuration (S3 region, bucket, credentials)
     * @param tableName     the table name for the query
     * @param listener      callback for the merged result rows
     */
    public void executeAsync(
        RelNode relNode,
        String sqlQuery,
        List<String> filePaths,
        long[] fileSizes,
        Map<String, String> storageConfig,
        String tableName,
        ActionListener<Iterable<Object[]>> listener
    ) {
        List<DiscoveryNode> workers = nodeDiscovery.getEligibleNodes();

        if (workers.size() <= 1) {
            logger.debug("[ScanExecutor] Single node, executing locally");
            executeSingleNodeAsync(sqlQuery, filePaths, fileSizes, storageConfig, tableName, listener);
            return;
        }

        SubPlan subPlan;
        try {
            subPlan = PlanFragmenter.fragment(relNode, sqlQuery);
        } catch (UnsupportedOperationException e) {
            logger.info("[ScanExecutor] Query not distributable ({}), executing on single node", e.getMessage());
            executeSingleNodeAsync(sqlQuery, filePaths, fileSizes, storageConfig, tableName, listener);
            return;
        }

        // Initialize query state machine
        String queryId = UUID.randomUUID().toString().substring(0, 8);
        QueryStateMachine queryStateMachine = new QueryStateMachine(queryId, stateExecutor);

        // Create stage state machines and register with query
        List<StageStateMachine> stageMachines = new ArrayList<>();
        for (int i = 0; i < subPlan.getStageCount(); i++) {
            StageStateMachine stageSm = new StageStateMachine(i, stateExecutor);
            queryStateMachine.addStage(stageSm);
            stageMachines.add(stageSm);
        }

        // Wire query failure to logging
        queryStateMachine.addStateChangeListener(state -> {
            if (state == QueryState.FAILED) {
                logger.error("[ScanExecutor] Query {} failed: {}", queryId, queryStateMachine.getFailureCause().getMessage());
            } else {
                logger.info("[ScanExecutor] Query {} → {}", queryId, state);
            }
        });

        queryStateMachine.transitionToStarting();

        PlanFragment leafStage = subPlan.getLeafStage();
        PlanFragment finalStage = subPlan.getFinalStage();

        logger.info(
            "[ScanExecutor] Distributing query {} across {} workers, stages={}, exchange={}, files={}",
            queryId,
            workers.size(),
            subPlan.getStageCount(),
            leafStage.getOutputExchange(),
            filePaths.size()
        );

        List<FilePartitioner.FileAssignment> assignments = FilePartitioner.partition(filePaths, fileSizes, workers.size());

        queryStateMachine.transitionToRunning();
        StageStateMachine leafStageSm = stageMachines.get(0);

        if (subPlan.getStageCount() == 3 && leafStage.getOutputExchange() == ExchangeType.HASH) {
            StageStateMachine intermediateStageSm = stageMachines.get(1);
            StageStateMachine finalStageSm = stageMachines.get(2);

            executeThreeStageAsync(subPlan, workers, assignments, storageConfig, tableName,
                leafStageSm, intermediateStageSm, finalStageSm, ActionListener.wrap(
                result -> {
                    queryStateMachine.transitionToFinishing();
                    queryStateMachine.transitionToFinished();
                    listener.onResponse(result);
                },
                e -> {
                    queryStateMachine.transitionToFailed(e);
                    listener.onFailure(e);
                }
            ));
        } else {
            StageStateMachine finalStageSm = stageMachines.get(stageMachines.size() - 1);
            String workerSql = leafStage.getSql();
            String coordinatorSql = finalStage.getSql();

            dispatchAndCollect(queryId, workers, assignments, workerSql, storageConfig, tableName,
                leafStageSm, ActionListener.wrap(
                responses -> {
                    // Leaf stage done, start final stage (coordinator merge)
                    finalStageSm.transitionToScheduling();
                    finalStageSm.transitionToRunning();
                    try {
                        mergeViaDataFusion(responses, coordinatorSql, subPlan, ActionListener.wrap(
                            result -> {
                                finalStageSm.transitionToFinished();
                                queryStateMachine.transitionToFinishing();
                                queryStateMachine.transitionToFinished();
                                listener.onResponse(result);
                            },
                            e -> {
                                finalStageSm.transitionToFailed(e);
                                queryStateMachine.transitionToFailed(e);
                                listener.onFailure(e);
                            }
                        ));
                    } catch (Exception e) {
                        finalStageSm.transitionToFailed(e);
                        queryStateMachine.transitionToFailed(e);
                        listener.onFailure(e);
                    }
                },
                e -> {
                    queryStateMachine.transitionToFailed(e);
                    listener.onFailure(e);
                }
            ));
        }
    }

    /**
     * Executes a 3-stage plan: leaf(HASH) → intermediate(re-aggregate) → final(CONCAT).
     * <p>
     * P1 simplification: the intermediate stage runs entirely on the coordinator. Workers
     * pre-aggregate and return partial results; the coordinator re-aggregates all groups
     * with ORDER BY + LIMIT. This is still a win because workers reduce data volume before
     * sending to the coordinator.
     */
    private void executeThreeStageAsync(
        SubPlan subPlan,
        List<DiscoveryNode> workers,
        List<FilePartitioner.FileAssignment> assignments,
        Map<String, String> storageConfig,
        String tableName,
        StageStateMachine leafStageSm,
        StageStateMachine intermediateStageSm,
        StageStateMachine finalStageSm,
        ActionListener<Iterable<Object[]>> listener
    ) {
        PlanFragment leafStage = subPlan.getLeafStage();
        PlanFragment intermediateStage = subPlan.getStages().get(1);

        logger.info("[ScanExecutor] 3-stage HASH plan: running intermediate on coordinator");

        dispatchAndCollect("3stage", workers, assignments, leafStage.getSql(), storageConfig, tableName,
            leafStageSm, ActionListener.wrap(
            responses -> {
                // Leaf done → start intermediate stage on coordinator
                intermediateStageSm.transitionToScheduling();
                intermediateStageSm.transitionToRunning();
                try {
                    mergeViaDataFusion(responses, intermediateStage.getSql(), subPlan, ActionListener.wrap(
                        result -> {
                            intermediateStageSm.transitionToFinished();
                            finalStageSm.transitionToScheduling();
                            finalStageSm.transitionToRunning();
                            finalStageSm.transitionToFinished();
                            listener.onResponse(result);
                        },
                        e -> {
                            intermediateStageSm.transitionToFailed(e);
                            listener.onFailure(e);
                        }
                    ));
                } catch (Exception e) {
                    intermediateStageSm.transitionToFailed(e);
                    listener.onFailure(e);
                }
            },
            listener::onFailure
        ));
    }

    /**
     * Executes the query on the local node asynchronously via {@link WorkerQueryExecutor}
     * on the {@code lakehouse_worker} thread pool.
     */
    private void executeSingleNodeAsync(
        String sqlQuery,
        List<String> filePaths,
        long[] fileSizes,
        Map<String, String> storageConfig,
        String tableName,
        ActionListener<Iterable<Object[]>> listener
    ) {
        WorkerQueryRequest request = new WorkerQueryRequest(sqlQuery, filePaths, fileSizes, storageConfig, tableName);
        transportService.getThreadPool().executor(LakehousePlugin.LAKEHOUSE_WORKER_THREAD_POOL).execute(() -> {
            try {
                WorkerQueryResponse response = WorkerQueryExecutor.execute(request, clusterService, queryEngine);
                listener.onResponse(ResultSerializer.toRows(response));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    /**
     * Dispatches worker requests and collects responses asynchronously.
     * <p>
     * Creates a {@link TaskStateMachine} per worker task. Task completion cascades
     * to the stage: when all tasks finish, the stage transitions to FINISHED.
     * If any task fails, the stage transitions to FAILED.
     *
     * @param queryId       query identifier for logging
     * @param workers       eligible worker nodes
     * @param assignments   file assignments (one per worker)
     * @param sqlQuery      the SQL query
     * @param storageConfig storage configuration
     * @param tableName     the table name
     * @param stageSm       stage state machine for this dispatch
     * @param listener      callback for collected responses
     */
    void dispatchAndCollect(
        String queryId,
        List<DiscoveryNode> workers,
        List<FilePartitioner.FileAssignment> assignments,
        String sqlQuery,
        Map<String, String> storageConfig,
        String tableName,
        StageStateMachine stageSm,
        ActionListener<List<WorkerQueryResponse>> listener
    ) {
        int assignmentCount = assignments.size();
        stageSm.transitionToScheduling();

        // Per-task state machines
        AtomicInteger pendingTasks = new AtomicInteger(assignmentCount);
        AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        List<WorkerQueryResponse> responses = java.util.Collections.synchronizedList(new ArrayList<>(assignmentCount));

        stageSm.transitionToRunning();

        String localNodeId = clusterService.state().nodes().getLocalNodeId();

        for (int i = 0; i < assignmentCount; i++) {
            FilePartitioner.FileAssignment assignment = assignments.get(i);
            DiscoveryNode targetNode = workers.get(i % workers.size());
            String taskId = queryId + "-s" + stageSm.getStageId() + "-t" + i;
            TaskStateMachine taskSm = new TaskStateMachine(taskId, stateExecutor);

            if (assignment.getFilePaths().isEmpty()) {
                logger.warn("[ScanExecutor] Task {} has no files assigned", taskId);
                responses.add(new WorkerQueryResponse(List.of(), List.of(), 0, new Object[0][]));
                taskSm.finished();
                if (pendingTasks.decrementAndGet() == 0) {
                    stageSm.transitionToFinished();
                    listener.onResponse(List.copyOf(responses));
                }
                continue;
            }

            WorkerQueryRequest request = new WorkerQueryRequest(
                sqlQuery,
                assignment.getFilePaths(),
                assignment.getFileSizes(),
                storageConfig,
                tableName
            );

            logger.debug("[ScanExecutor] Task {} dispatching to node {}: {} files", taskId, targetNode.getId(), request.getFilePaths().size());

            // Task-level listener: on response → task FINISHED; on failure → task FAILED → stage FAILED
            ActionListener<WorkerQueryResponse> taskListener = new ActionListener<>() {
                @Override
                public void onResponse(WorkerQueryResponse response) {
                    taskSm.finished();
                    logger.debug("[ScanExecutor] Task {} finished: {} rows", taskId, response.getRowCount());
                    responses.add(response);
                    if (pendingTasks.decrementAndGet() == 0) {
                        if (firstFailure.get() != null) {
                            stageSm.transitionToFailed(firstFailure.get());
                            listener.onFailure(new RuntimeException("Stage " + stageSm.getStageId() + " failed", firstFailure.get()));
                        } else {
                            stageSm.transitionToFinished();
                            listener.onResponse(List.copyOf(responses));
                        }
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    taskSm.fail(e);
                    taskSm.terminationComplete();
                    logger.error("[ScanExecutor] Task {} failed: {}", taskId, e.getMessage());
                    firstFailure.compareAndSet(null, e);
                    if (pendingTasks.decrementAndGet() == 0) {
                        stageSm.transitionToFailed(firstFailure.get());
                        listener.onFailure(new RuntimeException("Stage " + stageSm.getStageId() + " failed", firstFailure.get()));
                    }
                }
            };

            boolean isLocal = targetNode.getId().equals(localNodeId);
            if (isLocal) {
                dispatchLocal(request, taskListener);
            } else {
                dispatchRemote(targetNode, request, taskListener);
            }
        }
    }

    /**
     * Dispatches a request to a remote worker node via the transport service.
     */
    void dispatchRemote(DiscoveryNode node, WorkerQueryRequest request, ActionListener<WorkerQueryResponse> listener) {
        logger.debug("[ScanExecutor] Dispatching to remote node {}: {} files", node.getId(), request.getFilePaths().size());
        transportService.sendRequest(
            node,
            WorkerQueryAction.NAME,
            request,
            new TransportResponseHandler<WorkerQueryResponse>() {
                @Override
                public WorkerQueryResponse read(StreamInput in) throws IOException {
                    return new WorkerQueryResponse(in);
                }

                @Override
                public void handleResponse(WorkerQueryResponse response) {
                    listener.onResponse(response);
                }

                @Override
                public void handleException(TransportException exp) {
                    logger.error("[ScanExecutor] Remote node {} failed: {}", node.getId(), exp.getMessage(), exp);
                    listener.onFailure(exp);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            }
        );
    }

    /**
     * Dispatches a request to the local node by executing the worker query directly
     * on a {@code lakehouse_worker} thread pool thread, bypassing transport serialization.
     * This is the coordinator-as-worker optimization: avoids the serialize → send to
     * localhost → deserialize round-trip overhead.
     */
    void dispatchLocal(WorkerQueryRequest request, ActionListener<WorkerQueryResponse> listener) {
        logger.debug("[ScanExecutor] Executing locally: {} files", request.getFilePaths().size());
        transportService.getThreadPool().executor(LakehousePlugin.LAKEHOUSE_WORKER_THREAD_POOL).execute(() -> {
            try {
                WorkerQueryResponse response = WorkerQueryExecutor.execute(request, clusterService, queryEngine);
                listener.onResponse(response);
            } catch (Exception e) {
                logger.error("[ScanExecutor] Local execution failed", e);
                listener.onFailure(e);
            }
        });
    }

    /**
     * Merges worker responses via the native DataFusion runtime.
     * <p>
     * Converts all non-empty worker responses to Arrow VectorSchemaRoots, serializes them
     * as a single Arrow IPC stream, sends the stream to the native DataFusion runtime with
     * the given coordinator SQL, and drains the result stream back into row-major
     * {@code Iterable<Object[]>} for the listener.
     * <p>
     * Uses the DataFusion plugin's classloader on the thread-context classloader (TCCL)
     * during the stream-drain phase because Arrow C-Data imports require flatbuffers
     * classes that live in the DataFusion plugin's classloader, not the lakehouse plugin's.
     *
     * @param responses      worker responses to merge
     * @param coordinatorSql SQL to run over the accumulated input (e.g. {@code SELECT * FROM __exchange_input__})
     * @param subPlan        the execution plan (for logging)
     * @param listener       callback for the merged row-major result
     */
    void mergeViaDataFusion(
        List<WorkerQueryResponse> responses,
        String coordinatorSql,
        SubPlan subPlan,
        ActionListener<Iterable<Object[]>> listener
    ) {
        // Filter out empty responses
        List<WorkerQueryResponse> nonEmpty = new ArrayList<>();
        for (WorkerQueryResponse r : responses) {
            if (r.getRowCount() > 0) {
                nonEmpty.add(r);
            }
        }
        if (nonEmpty.isEmpty()) {
            listener.onResponse(List.of());
            return;
        }

        DataFusionService dfService = DataFusionPlugin.ensureSharedService();

        try {
            // 1. Convert responses to Arrow IPC bytes
            byte[] ipc = serializeResponsesAsIpc(nonEmpty, dfService);

            logger.info(
                "[ScanExecutor] {} merge via DataFusion: {} responses, {} bytes IPC, sql={}",
                subPlan, nonEmpty.size(), ipc.length, coordinatorSql
            );

            // 2. Call NativeBridge.executeFromIpcAsync to get a stream pointer
            NativeRuntimeHandle runtimeHandle = dfService.getNativeRuntime();
            long runtimePtr = runtimeHandle.get();

            CompletableFuture<Long> future = new CompletableFuture<>();
            nativeIpcExecutor.executeFromIpc(ipc, coordinatorSql, runtimePtr, new ActionListener<>() {
                @Override
                public void onResponse(Long streamPtr) {
                    future.complete(streamPtr);
                }

                @Override
                public void onFailure(Exception e) {
                    future.completeExceptionally(e);
                }
            });

            long streamPtr;
            try {
                streamPtr = future.get(NATIVE_TIMEOUT_MINUTES, TimeUnit.MINUTES);
            } catch (TimeoutException e) {
                FutureUtils.cancel(future);
                listener.onFailure(new RuntimeException(
                    "DataFusion " + subPlan + " merge timed out after " + NATIVE_TIMEOUT_MINUTES + " minutes", e));
                return;
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                listener.onFailure(new RuntimeException(
                    "DataFusion " + subPlan + " merge failed", cause != null ? cause : e));
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                listener.onFailure(new RuntimeException("DataFusion " + subPlan + " merge interrupted", e));
                return;
            }

            // 3. Drain the result stream — swap TCCL for Arrow C-Data imports
            List<Object[]> rows = drainStreamToRows(streamPtr, runtimeHandle, dfService);

            // 4. Apply global offset in Java (DataFusion's OFFSET corrupts VarChar Arrow buffers)
            int globalOffset = subPlan.getGlobalOffset();
            if (globalOffset > 0 && globalOffset < rows.size()) {
                rows = rows.subList(globalOffset, rows.size());
            } else if (globalOffset >= rows.size()) {
                rows = List.of();
            }

            listener.onResponse(rows);

        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Serializes non-empty worker responses as a single Arrow IPC stream (schema + batches + EOS).
     * Each response becomes one record batch in the stream.
     *
     * @param responses non-empty worker responses (all must share the same column schema)
     * @param dfService provides the child allocator for Arrow buffer allocation
     * @return full Arrow IPC stream-format bytes
     */
    static byte[] serializeResponsesAsIpc(List<WorkerQueryResponse> responses, DataFusionService dfService) throws Exception {
        BufferAllocator allocator = dfService.newChildAllocator();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            // Convert first response to get the schema template
            VectorSchemaRoot template = WorkerResponseToArrow.convert(responses.get(0), allocator);
            try (VectorSchemaRoot scratch = VectorSchemaRoot.create(template.getSchema(), allocator);
                 ArrowStreamWriter writer = new ArrowStreamWriter(scratch, null, Channels.newChannel(baos))) {

                writer.start();

                // Write the first response (already converted as template)
                copyInto(scratch, template);
                writer.writeBatch();
                template.close();

                // Write remaining responses
                for (int i = 1; i < responses.size(); i++) {
                    VectorSchemaRoot batch = WorkerResponseToArrow.convert(responses.get(i), allocator);
                    copyInto(scratch, batch);
                    writer.writeBatch();
                    batch.close();
                }

                writer.end();
            }
        } finally {
            allocator.close();
        }
        return baos.toByteArray();
    }

    /**
     * Transfers vectors from {@code src} into {@code dst}. Both must share the same schema.
     * After this call, {@code src} is emptied (its buffers have moved to {@code dst}).
     */
    private static void copyInto(VectorSchemaRoot dst, VectorSchemaRoot src) {
        for (var v : dst.getFieldVectors()) {
            v.clear();
        }
        for (int i = 0; i < src.getFieldVectors().size(); i++) {
            var srcVec = src.getFieldVectors().get(i);
            var dstVec = dst.getFieldVectors().get(i);
            var tp = srcVec.makeTransferPair(dstVec);
            tp.transfer();
        }
        dst.setRowCount(src.getRowCount());
    }

    /**
     * Drains a native DataFusion result stream into a list of row-major {@code Object[]} arrays.
     * <p>
     * Swaps the thread-context classloader to the DataFusion plugin's classloader for the
     * duration of the drain, because Arrow C-Data imports require flatbuffers classes that
     * live in the DataFusion plugin classloader.
     *
     * @param streamPtr     the native stream pointer from {@link NativeBridge#executeFromIpcAsync}
     * @param runtimeHandle the native runtime handle
     * @param dfService     provides the child allocator for result deserialization
     * @return list of row arrays
     */
    static List<Object[]> drainStreamToRows(long streamPtr, NativeRuntimeHandle runtimeHandle, DataFusionService dfService) {
        // Swap TCCL for Arrow C-Data imports (flatbuffers live in DataFusion plugin classloader)
        Thread currentThread = Thread.currentThread();
        ClassLoader originalCl = currentThread.getContextClassLoader();
        currentThread.setContextClassLoader(DataFusionPlugin.class.getClassLoader());
        try {
            StreamHandle streamHandle = new StreamHandle(streamPtr, runtimeHandle);
            BufferAllocator allocator = dfService.newChildAllocator();
            DatafusionResultStream resultStream = new DatafusionResultStream(streamHandle, allocator);

            List<Object[]> rows = new ArrayList<>();
            try {
                var batchIterator = resultStream.iterator();
                while (batchIterator.hasNext()) {
                    var batch = batchIterator.next();
                    List<String> fieldNames = batch.getFieldNames();
                    for (int row = 0; row < batch.getRowCount(); row++) {
                        Object[] rowValues = new Object[fieldNames.size()];
                        for (int col = 0; col < fieldNames.size(); col++) {
                            Object val = batch.getFieldValue(fieldNames.get(col), row);
                            if (val instanceof org.apache.arrow.vector.util.Text) {
                                val = val.toString();
                            }
                            rowValues[col] = val;
                        }
                        rows.add(rowValues);
                    }
                }
            } finally {
                resultStream.close();
            }

            logger.info("[ScanExecutor] DataFusion merge drained {} rows", rows.size());
            return rows;
        } finally {
            currentThread.setContextClassLoader(originalCl);
        }
    }

    /**
     * Seam for the native IPC execution call. Production uses
     * {@link NativeBridge#executeFromIpcAsync}; tests inject a mock to avoid
     * loading the native library.
     */
    @FunctionalInterface
    interface NativeIpcExecutor {
        /**
         * Executes a SQL query over Arrow IPC bytes via the native DataFusion runtime.
         *
         * @param ipc        full Arrow IPC stream-format bytes
         * @param sql        coordinator SQL to evaluate over the IPC input
         * @param runtimePtr native DataFusion runtime pointer
         * @param listener   receives the resulting stream pointer or any failure
         */
        void executeFromIpc(byte[] ipc, String sql, long runtimePtr, ActionListener<Long> listener);
    }
}

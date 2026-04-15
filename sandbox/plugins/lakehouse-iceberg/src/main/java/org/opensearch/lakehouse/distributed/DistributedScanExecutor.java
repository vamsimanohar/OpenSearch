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
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.analytics.exec.DataWarehouseQueryEngine;
import org.opensearch.analytics.exec.DataWarehouseScanContext;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.lakehouse.LakehousePlugin;
import org.opensearch.lakehouse.distributed.merge.AvgDecomposer;
import org.opensearch.lakehouse.distributed.merge.DistinctExpander;
import org.opensearch.lakehouse.distributed.merge.MergeStrategy;
import org.opensearch.lakehouse.distributed.merge.MergeSqlGenerator;
import org.opensearch.lakehouse.distributed.merge.MixedDistinctExpander;
import org.opensearch.lakehouse.distributed.merge.ResultMerger;
import org.opensearch.lakehouse.distributed.merge.ResultSerializer;
import org.opensearch.lakehouse.distributed.worker.WorkerCredentialResolver;
import org.opensearch.lakehouse.distributed.worker.WorkerQueryAction;
import org.opensearch.lakehouse.distributed.worker.WorkerQueryExecutor;
import org.opensearch.lakehouse.distributed.worker.WorkerQueryRequest;
import org.opensearch.lakehouse.distributed.worker.WorkerQueryResponse;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Unified scan executor that handles both single-node and distributed query execution.
 * <p>
 * Fully asynchronous: dispatches worker queries via transport or local thread pool,
 * collects responses via {@link GroupedActionListener}, and delivers merged results
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

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final NodeDiscovery nodeDiscovery;
    private final DataWarehouseQueryEngine queryEngine;

    /**
     * Creates a new DistributedScanExecutor.
     *
     * @param transportService the transport service for sending requests to remote nodes
     * @param clusterService   the cluster service for node discovery
     * @param queryEngine     the external query backend for executing queries
     */
    public DistributedScanExecutor(TransportService transportService, ClusterService clusterService, DataWarehouseQueryEngine queryEngine) {
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.nodeDiscovery = new NodeDiscovery(clusterService);
        this.queryEngine = queryEngine;
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
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.nodeDiscovery = nodeDiscovery;
        this.queryEngine = queryEngine;
    }

    /**
     * Executes the query asynchronously, choosing between distributed and single-node paths.
     * <p>
     * Distributed execution is used when:
     * <ul>
     *   <li>Multiple eligible worker nodes are available</li>
     *   <li>The query's merge strategy is not {@link MergeStrategy#SINGLE_NODE}</li>
     * </ul>
     * Otherwise, falls back to single-node execution via {@link WorkerQueryExecutor}.
     * <p>
     * Results are delivered through the listener callback. No thread blocks waiting.
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

        // Single-node: execute directly without distribution overhead
        if (workers.size() <= 1) {
            logger.debug("[ScanExecutor] Single node, executing locally");
            executeSingleNodeAsync(sqlQuery, filePaths, fileSizes, storageConfig, tableName, listener);
            return;
        }

        QueryAnalyzer.AnalysisResult analysis = QueryAnalyzer.analyzeDetailed(relNode);
        if (analysis.strategy == MergeStrategy.SINGLE_NODE) {
            logger.debug("[ScanExecutor] Query requires SINGLE_NODE execution");
            executeSingleNodeAsync(sqlQuery, filePaths, fileSizes, storageConfig, tableName, listener);
            return;
        }

        // Filter to connected remote workers (exclude coordinator and disconnected nodes)
        String localNodeId = clusterService.state().nodes().getLocalNodeId();
        List<DiscoveryNode> remoteWorkers = workers.stream()
            .filter(n -> !n.getId().equals(localNodeId))
            .filter(n -> transportService.nodeConnected(n))
            .toList();
        if (remoteWorkers.isEmpty()) {
            logger.debug("[ScanExecutor] No connected remote workers, executing locally");
            executeSingleNodeAsync(sqlQuery, filePaths, fileSizes, storageConfig, tableName, listener);
            return;
        }

        // Build worker SQL based on strategy
        String workerSql = sqlQuery;
        if (analysis.strategy == MergeStrategy.DISTINCT_EXPAND) {
            workerSql = DistinctExpander.rewriteWorkerSql(workerSql);
        } else if (analysis.strategy == MergeStrategy.MIXED_DISTINCT) {
            workerSql = MixedDistinctExpander.rewriteWorkerSql(workerSql);
        } else {
            if (analysis.strategy == MergeStrategy.TWO_PHASE_GROUP_BY || analysis.strategy == MergeStrategy.GLOBAL_MERGE) {
                if (AvgDecomposer.hasAvg(analysis)) {
                    workerSql = AvgDecomposer.decomposeWorkerSql(workerSql);
                }
            }
            if (analysis.strategy == MergeStrategy.TWO_PHASE_GROUP_BY) {
                workerSql = stripHavingClause(workerSql);
                workerSql = stripOrderByAndLimit(workerSql);
            }
        }

        // Phase 2.5: partition files across coordinator + remote workers (N+1 total)
        int totalWorkers = remoteWorkers.size() + 1;
        List<FilePartitioner.FileAssignment> allAssignments = FilePartitioner.partition(filePaths, fileSizes, totalWorkers);

        // First assignment → coordinator (local batch handle, no IPC serialization)
        FilePartitioner.FileAssignment localAssignment = allAssignments.get(0);
        List<FilePartitioner.FileAssignment> remoteAssignments = allAssignments.subList(1, allAssignments.size());

        logger.info(
            "[ScanExecutor] Distributing query: coordinator={} files + {} remote workers, strategy={}, totalFiles={}",
            localAssignment.getFilePaths().size(),
            remoteWorkers.size(),
            analysis.strategy,
            filePaths.size()
        );

        // Start local worker: returns batch handle (RecordBatches stay in Rust memory)
        CompletableFuture<Long> localBatchFuture = dispatchLocalBatches(
            localAssignment, workerSql, storageConfig, tableName
        );

        // Dispatch remote workers and merge when all complete
        final String finalWorkerSql = workerSql;
        dispatchAndCollect(remoteWorkers, remoteAssignments, workerSql, storageConfig, tableName, ActionListener.wrap(
            responses -> {
                long localBatchHandle = 0;
                try {
                    localBatchHandle = localBatchFuture.get(15, TimeUnit.MINUTES);
                } catch (Exception e) {
                    logger.warn("[ScanExecutor] Local worker failed, merging with remote data only: {}", e.getMessage());
                }

                try {
                    // Collect Arrow IPC bytes from remote workers
                    List<byte[]> remoteIpcData = new ArrayList<>();
                    for (WorkerQueryResponse r : responses) {
                        if (r.isArrowIpc() && r.getArrowIpcData().length > 0) {
                            remoteIpcData.add(r.getArrowIpcData());
                        }
                    }

                    if (remoteIpcData.isEmpty() && localBatchHandle == 0) {
                        listener.onResponse(List.of());
                        return;
                    }

                    // Generate merge SQL from remote IPC column names
                    String mergeSql;
                    if (!remoteIpcData.isEmpty()) {
                        List<String> columnNames = queryEngine.readArrowIpcColumnNames(remoteIpcData.get(0));
                        if (analysis.strategy == MergeStrategy.DISTINCT_EXPAND) {
                            mergeSql = DistinctExpander.generateMergeSql(columnNames, sqlQuery);
                        } else if (analysis.strategy == MergeStrategy.MIXED_DISTINCT) {
                            mergeSql = MixedDistinctExpander.generateMergeSql(columnNames, sqlQuery);
                        } else {
                            mergeSql = MergeSqlGenerator.generate(analysis, columnNames);
                        }
                    } else {
                        // Only local data — pass through without re-aggregation
                        mergeSql = "SELECT * FROM input";
                    }

                    logger.info(
                        "[ScanExecutor] Streaming merge: local={}, remote={}, strategy={}, sql={}",
                        localBatchHandle != 0, remoteIpcData.size(), analysis.strategy, mergeSql
                    );

                    // Phase 2.5: streaming merge (local batch handle + remote IPC, no IPC round-trip)
                    Iterable<Object[]> rows = queryEngine.executeMergeStreaming(
                        localBatchHandle, remoteIpcData, mergeSql
                    );
                    listener.onResponse(rows);
                } catch (Exception e) {
                    if (localBatchHandle != 0) {
                        try { queryEngine.freeBatchHandle(localBatchHandle); } catch (Exception ignored) { }
                    }
                    listener.onFailure(e);
                }
            },
            distributedFailure -> {
                // Free local batch handle on failure, then fall back to single-node
                localBatchFuture.thenAccept(handle -> {
                    if (handle != 0) {
                        try { queryEngine.freeBatchHandle(handle); } catch (Exception ignored) { }
                    }
                });
                logger.warn(
                    "[ScanExecutor] Distributed execution failed (strategy={}), falling back to single-node: {}",
                    analysis.strategy, distributedFailure.getMessage()
                );
                executeSingleNodeAsync(sqlQuery, filePaths, fileSizes, storageConfig, tableName, listener);
            }
        ));
    }

    /**
     * Executes the query on the local node asynchronously using the streaming query engine.
     * Uses {@link DataWarehouseQueryEngine#executeQuery} directly instead of the IPC path
     * to avoid materializing the entire result as a single byte[] — critical for high-cardinality
     * GROUP BY queries where the IPC buffer can exceed Java heap capacity.
     */
    @SuppressWarnings("removal")
    private void executeSingleNodeAsync(
        String sqlQuery,
        List<String> filePaths,
        long[] fileSizes,
        Map<String, String> storageConfig,
        String tableName,
        ActionListener<Iterable<Object[]>> listener
    ) {
        Map<String, String> resolvedConfig = WorkerCredentialResolver.resolve(storageConfig, clusterService);
        DataWarehouseScanContext scanContext = new DataWarehouseScanContext(
            tableName, filePaths, fileSizes, sqlQuery, resolvedConfig
        );
        transportService.getThreadPool().executor(LakehousePlugin.LAKEHOUSE_WORKER_THREAD_POOL).execute(() -> {
            try {
                Iterable<Object[]> rows = AccessController.doPrivileged(
                    (PrivilegedAction<Iterable<Object[]>>) () -> queryEngine.executeQuery(scanContext)
                );
                listener.onResponse(rows);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    /**
     * Dispatches worker requests and collects responses asynchronously.
     * <p>
     * Uses {@link GroupedActionListener} to collect all worker responses.
     * When all responses arrive, the listener is called with the collected results.
     * No thread blocks waiting — the callback fires on the thread that delivers
     * the last response.
     *
     * @param workers       eligible worker nodes
     * @param assignments   file assignments (one per worker)
     * @param sqlQuery      the SQL query
     * @param storageConfig storage configuration
     * @param tableName     the table name
     * @param listener      callback for collected responses
     */
    void dispatchAndCollect(
        List<DiscoveryNode> workers,
        List<FilePartitioner.FileAssignment> assignments,
        String sqlQuery,
        Map<String, String> storageConfig,
        String tableName,
        ActionListener<List<WorkerQueryResponse>> listener
    ) {
        int assignmentCount = assignments.size();

        GroupedActionListener<WorkerQueryResponse> groupListener = new GroupedActionListener<>(
            ActionListener.wrap(
                collected -> listener.onResponse(List.copyOf(collected)),
                listener::onFailure
            ),
            assignmentCount
        );

        String localNodeId = clusterService.state().nodes().getLocalNodeId();

        for (int i = 0; i < assignmentCount; i++) {
            FilePartitioner.FileAssignment assignment = assignments.get(i);
            DiscoveryNode targetNode = workers.get(i % workers.size());

            if (assignment.getFilePaths().isEmpty()) {
                logger.warn("[ScanExecutor] Worker {} has no files assigned (more workers than files)", i);
                groupListener.onResponse(
                    new WorkerQueryResponse(List.of(), List.of(), 0, new Object[0][])
                );
                continue;
            }

            WorkerQueryRequest request = new WorkerQueryRequest(
                sqlQuery,
                assignment.getFilePaths(),
                assignment.getFileSizes(),
                storageConfig,
                tableName
            );

            boolean isLocal = targetNode.getId().equals(localNodeId);
            if (isLocal) {
                dispatchLocal(request, groupListener);
            } else {
                dispatchRemote(targetNode, request, groupListener);
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
     * Strips the HAVING clause from SQL for two-phase GROUP BY workers.
     * Workers produce partial aggregates — HAVING must be applied on the coordinator
     * after re-aggregation, not on individual workers with partial data.
     */
    static String stripHavingClause(String sql) {
        return sql.replaceAll("(?is)\\s+HAVING\\s+.*?(?=\\s+ORDER\\s+BY|\\s+LIMIT\\s+|$)", "");
    }

    /**
     * Strips ORDER BY and LIMIT clauses from the SQL for two-phase GROUP BY workers.
     * Workers run partial GROUP BY without ordering or limiting — the coordinator
     * applies ORDER BY and LIMIT on the re-aggregated results.
     */
    static String stripOrderByAndLimit(String sql) {
        // Strip ORDER BY ... (and everything after it, including LIMIT and OFFSET)
        String stripped = sql.replaceAll("(?is)\\s+ORDER\\s+BY\\s+.+$", "");
        if (stripped.equals(sql)) {
            // No ORDER BY found — strip standalone LIMIT [OFFSET]
            stripped = sql.replaceAll("(?is)\\s+LIMIT\\s+\\d+(\\s+OFFSET\\s+\\d+)?\\s*$", "");
        }
        return stripped.trim();
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
     * Dispatches the coordinator's local file assignment using batch handles instead of IPC.
     * RecordBatches stay in Rust native memory — no serialization overhead.
     * The returned batch handle is passed to {@code executeMergeStreaming} for zero-copy merge.
     */
    @SuppressWarnings("removal")
    CompletableFuture<Long> dispatchLocalBatches(
        FilePartitioner.FileAssignment assignment,
        String workerSql,
        Map<String, String> storageConfig,
        String tableName
    ) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        if (assignment.getFilePaths().isEmpty()) {
            future.complete(0L);
            return future;
        }

        Map<String, String> resolvedConfig = WorkerCredentialResolver.resolve(storageConfig, clusterService);
        DataWarehouseScanContext scanContext = new DataWarehouseScanContext(
            tableName, assignment.getFilePaths(), assignment.getFileSizes(), workerSql, resolvedConfig
        );

        logger.debug("[ScanExecutor] Local batch execution: {} files", assignment.getFilePaths().size());
        transportService.getThreadPool().executor(LakehousePlugin.LAKEHOUSE_WORKER_THREAD_POOL).execute(() -> {
            try {
                long batchHandle = AccessController.doPrivileged(
                    (PrivilegedAction<Long>) () -> queryEngine.executeQueryToBatches(scanContext)
                );
                future.complete(batchHandle);
            } catch (Exception e) {
                logger.error("[ScanExecutor] Local batch execution failed", e);
                future.completeExceptionally(e);
            }
        });
        return future;
    }
}

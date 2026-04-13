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
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unified scan executor that handles both single-node and distributed query execution.
 * <p>
 * When multiple eligible worker nodes are available and the query is distributable,
 * splits files across workers, dispatches in parallel, and merges results.
 * Otherwise, delegates to single-node execution via {@link WorkerQueryExecutor}.
 * <p>
 * Note: The Calcite query pipeline above this class is synchronous
 * ({@code prepareScan()} returns {@code ExternalScanContext}), so distributed dispatch
 * uses {@code CompletableFuture.get()} to bridge async transport to the sync caller.
 * Fully async execution would require rewriting the Calcite pipeline to use
 * {@code ActionListener} callbacks throughout — a future improvement.
 *
 * @opensearch.internal
 */
public class DistributedScanExecutor {

    private static final Logger logger = LogManager.getLogger(DistributedScanExecutor.class);

    /** Default timeout for waiting for worker responses, in seconds. */
    static final long DEFAULT_TIMEOUT_SECONDS = 120;

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
     * Executes the query, automatically choosing between distributed and single-node paths.
     * <p>
     * Distributed execution is used when:
     * <ul>
     *   <li>Multiple eligible worker nodes are available</li>
     *   <li>The query's merge strategy is not {@link MergeStrategy#SINGLE_NODE}</li>
     * </ul>
     * Otherwise, falls back to single-node execution via {@link WorkerQueryExecutor}.
     *
     * @param relNode       the Calcite logical plan (for query analysis)
     * @param sqlQuery      the SQL query string to send to workers
     * @param filePaths     the data file paths to distribute
     * @param fileSizes     file sizes in bytes, parallel to filePaths
     * @param storageConfig storage configuration (S3 region, bucket, credentials)
     * @param tableName     the table name for the query
     * @return merged rows as Iterable&lt;Object[]&gt;
     */
    public Iterable<Object[]> execute(
        RelNode relNode,
        String sqlQuery,
        List<String> filePaths,
        long[] fileSizes,
        Map<String, String> storageConfig,
        String tableName
    ) {
        List<DiscoveryNode> workers = nodeDiscovery.getEligibleNodes();

        // Single-node: execute directly without distribution overhead
        if (workers.size() <= 1) {
            logger.debug("[ScanExecutor] Single node, executing locally");
            return executeSingleNode(sqlQuery, filePaths, fileSizes, storageConfig, tableName);
        }

        QueryAnalyzer.AnalysisResult analysis = QueryAnalyzer.analyzeDetailed(relNode);
        if (analysis.strategy == MergeStrategy.SINGLE_NODE) {
            logger.debug("[ScanExecutor] Query requires SINGLE_NODE execution");
            return executeSingleNode(sqlQuery, filePaths, fileSizes, storageConfig, tableName);
        }

        logger.info(
            "[ScanExecutor] Distributing query across {} workers, strategy={}, files={}",
            workers.size(),
            analysis.strategy,
            filePaths.size()
        );

        // Partition files across workers
        List<FilePartitioner.FileAssignment> assignments = FilePartitioner.partition(filePaths, fileSizes, workers.size());

        // Dispatch requests and collect responses
        List<WorkerQueryResponse> responses = dispatchAndCollect(workers, assignments, sqlQuery, storageConfig, tableName);

        // Merge results using analysis metadata
        WorkerQueryResponse merged = ResultMerger.merge(
            responses, analysis.strategy, analysis.sortColumns, analysis.sortAsc, analysis.limit, analysis.aggKinds
        );

        // Convert to row-oriented
        return ResultSerializer.toRows(merged);
    }

    /**
     * Executes the query on the local node only, using {@link WorkerQueryExecutor}.
     */
    private Iterable<Object[]> executeSingleNode(
        String sqlQuery,
        List<String> filePaths,
        long[] fileSizes,
        Map<String, String> storageConfig,
        String tableName
    ) {
        WorkerQueryRequest request = new WorkerQueryRequest(sqlQuery, filePaths, fileSizes, storageConfig, tableName);
        WorkerQueryResponse response = WorkerQueryExecutor.execute(request, clusterService, queryEngine);
        return ResultSerializer.toRows(response);
    }

    /**
     * Dispatches worker requests and collects responses synchronously.
     * <p>
     * Uses {@link GroupedActionListener} to collect all worker responses, bridged to
     * a {@link CompletableFuture} for synchronous consumption. This blocking pattern
     * is necessary because the Calcite pipeline above is synchronous.
     *
     * @param workers       eligible worker nodes
     * @param assignments   file assignments (one per worker)
     * @param sqlQuery      the SQL query
     * @param storageConfig storage configuration
     * @param tableName     the table name
     * @return list of worker responses
     */
    List<WorkerQueryResponse> dispatchAndCollect(
        List<DiscoveryNode> workers,
        List<FilePartitioner.FileAssignment> assignments,
        String sqlQuery,
        Map<String, String> storageConfig,
        String tableName
    ) {
        int assignmentCount = assignments.size();
        long dispatchStartTime = System.currentTimeMillis();
        CompletableFuture<Collection<WorkerQueryResponse>> future = new CompletableFuture<>();
        AtomicInteger completedCount = new AtomicInteger(0);

        GroupedActionListener<WorkerQueryResponse> groupListener = new GroupedActionListener<>(
            ActionListener.wrap(responses -> {
                logger.info(
                    "[ScanExecutor] All {} workers responded in {}ms",
                    assignmentCount,
                    System.currentTimeMillis() - dispatchStartTime
                );
                future.complete(responses);
            }, ex -> {
                logger.error(
                    "[ScanExecutor] Dispatch failed after {}/{} workers responded in {}ms",
                    completedCount.get(),
                    assignmentCount,
                    System.currentTimeMillis() - dispatchStartTime,
                    ex
                );
                future.completeExceptionally(ex);
            }),
            assignmentCount
        );

        String localNodeId = clusterService.state().nodes().getLocalNodeId();

        for (int i = 0; i < assignmentCount; i++) {
            FilePartitioner.FileAssignment assignment = assignments.get(i);
            DiscoveryNode targetNode = workers.get(i % workers.size());

            if (assignment.getFilePaths().isEmpty()) {
                logger.warn("[ScanExecutor] Worker {} has no files assigned (more workers than files)", i);
                completedCount.incrementAndGet();
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
            int workerIdx = i;
            ActionListener<WorkerQueryResponse> trackingListener = ActionListener.wrap(
                response -> {
                    int done = completedCount.incrementAndGet();
                    logger.info(
                        "[ScanExecutor] Worker {} ({}{}) responded: {} rows, {}ms elapsed ({}/{})",
                        workerIdx,
                        targetNode.getId(),
                        isLocal ? "/local" : "",
                        response.getRowCount(),
                        System.currentTimeMillis() - dispatchStartTime,
                        done,
                        assignmentCount
                    );
                    groupListener.onResponse(response);
                },
                ex -> {
                    int done = completedCount.incrementAndGet();
                    logger.error(
                        "[ScanExecutor] Worker {} ({}{}) FAILED after {}ms ({}/{}): {}",
                        workerIdx,
                        targetNode.getId(),
                        isLocal ? "/local" : "",
                        System.currentTimeMillis() - dispatchStartTime,
                        done,
                        assignmentCount,
                        ex.getMessage(),
                        ex
                    );
                    groupListener.onFailure(ex);
                }
            );

            if (isLocal) {
                dispatchLocal(request, trackingListener);
            } else {
                dispatchRemote(targetNode, request, trackingListener);
            }
        }

        try {
            Collection<WorkerQueryResponse> collected = future.get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return new ArrayList<>(collected);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Distributed query execution interrupted", e);
        } catch (java.util.concurrent.TimeoutException e) {
            throw new RuntimeException(
                "Distributed query execution timed out after " + DEFAULT_TIMEOUT_SECONDS
                    + " seconds. Workers responded: " + completedCount.get() + "/" + assignmentCount, e
            );
        } catch (Exception e) {
            throw new RuntimeException("Distributed query execution failed", e);
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
     * on a GENERIC thread pool thread, bypassing transport serialization entirely.
     * This is the coordinator-as-worker optimization: saves ~0.1s per query by
     * avoiding serialize → send to localhost → deserialize round-trip.
     */
    void dispatchLocal(WorkerQueryRequest request, ActionListener<WorkerQueryResponse> listener) {
        logger.debug("[ScanExecutor] Executing locally (direct, no transport): {} files", request.getFilePaths().size());
        transportService.getThreadPool().executor(ThreadPool.Names.GENERIC).execute(() -> {
            try {
                WorkerQueryResponse response = WorkerQueryExecutor.execute(request, clusterService, queryEngine);
                listener.onResponse(response);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }
}

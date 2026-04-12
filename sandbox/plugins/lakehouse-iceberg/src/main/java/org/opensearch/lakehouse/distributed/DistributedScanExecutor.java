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
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Orchestrates distributed query execution across multiple cluster nodes.
 * <p>
 * Flow:
 * <ol>
 *   <li>NodeDiscovery: get eligible worker nodes</li>
 *   <li>QueryAnalyzer: determine merge strategy from the Calcite plan</li>
 *   <li>If SINGLE_NODE: return null (caller uses single-node path)</li>
 *   <li>FilePartitioner: split files across workers</li>
 *   <li>Dispatch requests to workers via TransportService</li>
 *   <li>Collect responses via GroupedActionListener</li>
 *   <li>ResultMerger: merge worker responses</li>
 *   <li>Convert to Iterable&lt;Object[]&gt; for the pipeline</li>
 * </ol>
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

    /**
     * Creates a new DistributedScanExecutor.
     *
     * @param transportService the transport service for sending requests to remote nodes
     * @param clusterService   the cluster service for node discovery
     */
    public DistributedScanExecutor(TransportService transportService, ClusterService clusterService) {
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.nodeDiscovery = new NodeDiscovery(clusterService);
    }

    /**
     * Constructor that accepts a pre-built NodeDiscovery (for testing).
     *
     * @param transportService the transport service
     * @param clusterService   the cluster service
     * @param nodeDiscovery    the node discovery instance
     */
    DistributedScanExecutor(TransportService transportService, ClusterService clusterService, NodeDiscovery nodeDiscovery) {
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.nodeDiscovery = nodeDiscovery;
    }

    /**
     * Attempts to execute the query in a distributed fashion.
     * <p>
     * Returns null if the query should fall back to single-node execution
     * (either because there's only one eligible node, or the query requires SINGLE_NODE strategy).
     *
     * @param relNode       the Calcite logical plan (for query analysis)
     * @param sqlQuery      the SQL query string to send to workers
     * @param filePaths     the data file paths to distribute
     * @param fileSizes     file sizes in bytes, parallel to filePaths
     * @param storageConfig storage configuration (S3 region, bucket, credentials)
     * @param tableName     the table name for the query
     * @return merged rows as Iterable&lt;Object[]&gt;, or null if single-node fallback is needed
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
        if (workers.size() <= 1) {
            logger.debug("[DistributedScan] Only {} eligible node(s), falling back to single-node", workers.size());
            return null;
        }

        QueryAnalyzer.AnalysisResult analysis = QueryAnalyzer.analyzeDetailed(relNode);
        if (analysis.strategy == MergeStrategy.SINGLE_NODE) {
            logger.debug("[DistributedScan] Query requires SINGLE_NODE execution, falling back");
            return null;
        }

        logger.info(
            "[DistributedScan] Distributing query across {} workers, strategy={}, files={}",
            workers.size(),
            analysis.strategy,
            filePaths.size()
        );

        // Partition files across workers
        List<FilePartitioner.FileAssignment> assignments = FilePartitioner.partition(filePaths, fileSizes, workers.size());

        // Dispatch requests and collect responses
        List<WorkerQueryResponse> responses = dispatchAndCollect(workers, assignments, sqlQuery, storageConfig, tableName);

        // Merge results using analysis metadata (agg kinds for GLOBAL_MERGE, sort/limit for TOPK_MERGE)
        WorkerQueryResponse merged = ResultMerger.merge(
            responses, analysis.strategy, analysis.sortColumns, analysis.sortAsc, analysis.limit, analysis.aggKinds
        );

        // Convert to row-oriented
        return ResultSerializer.toRows(merged);
    }

    /**
     * Dispatches worker requests and collects responses synchronously using a CompletableFuture.
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
        CompletableFuture<Collection<WorkerQueryResponse>> future = new CompletableFuture<>();

        GroupedActionListener<WorkerQueryResponse> groupListener = new GroupedActionListener<>(
            ActionListener.wrap(future::complete, future::completeExceptionally),
            assignmentCount
        );

        String localNodeId = clusterService.state().nodes().getLocalNodeId();

        for (int i = 0; i < assignmentCount; i++) {
            FilePartitioner.FileAssignment assignment = assignments.get(i);
            DiscoveryNode targetNode = workers.get(i % workers.size());

            if (assignment.getFilePaths().isEmpty()) {
                // No files for this worker — return empty response
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

            if (targetNode.getId().equals(localNodeId)) {
                dispatchLocal(request, groupListener);
            } else {
                dispatchRemote(targetNode, request, groupListener);
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
                "Distributed query execution timed out after " + DEFAULT_TIMEOUT_SECONDS + " seconds", e
            );
        } catch (Exception e) {
            throw new RuntimeException("Distributed query execution failed", e);
        }
    }

    /**
     * Dispatches a request to a remote worker node via the transport service.
     */
    void dispatchRemote(DiscoveryNode node, WorkerQueryRequest request, ActionListener<WorkerQueryResponse> listener) {
        logger.debug("[DistributedScan] Dispatching to remote node {}: {} files", node.getId(), request.getFilePaths().size());
        transportService.sendRequest(
            node,
            WorkerQueryAction.NAME,
            request,
            new TransportResponseHandler<WorkerQueryResponse>() {
                @Override
                public WorkerQueryResponse read(org.opensearch.core.common.io.stream.StreamInput in) throws java.io.IOException {
                    return new WorkerQueryResponse(in);
                }

                @Override
                public void handleResponse(WorkerQueryResponse response) {
                    listener.onResponse(response);
                }

                @Override
                public void handleException(org.opensearch.transport.TransportException exp) {
                    listener.onFailure(exp);
                }

                @Override
                public String executor() {
                    return org.opensearch.threadpool.ThreadPool.Names.SAME;
                }
            }
        );
    }

    /**
     * Dispatches a request to the local node by executing the worker query directly
     * on a GENERIC thread pool thread, bypassing transport serialization entirely.
     */
    void dispatchLocal(WorkerQueryRequest request, ActionListener<WorkerQueryResponse> listener) {
        logger.debug("[DistributedScan] Executing locally (direct, no transport): {} files", request.getFilePaths().size());
        transportService.getThreadPool().executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC).execute(() -> {
            try {
                WorkerQueryResponse response = WorkerQueryTransportAction.executeLocally(request, clusterService);
                listener.onResponse(response);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }
}

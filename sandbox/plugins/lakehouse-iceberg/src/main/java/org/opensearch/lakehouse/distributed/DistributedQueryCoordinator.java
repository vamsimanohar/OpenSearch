/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.exec.ExternalScanContext;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.lakehouse.scan.IcebergScanPlan;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Orchestrates distributed Iceberg query execution across cluster nodes using
 * a proper two-phase approach: physical plan splitting + scatter-gather.
 *
 * <p>Supports two transport modes:
 * <ul>
 *   <li><b>Arrow Flight streaming</b> (preferred): batched streaming via {@link StreamTransportService}</li>
 *   <li><b>Standard transport</b> (fallback): single-response via {@link TransportService}</li>
 * </ul>
 */
public class DistributedQueryCoordinator {

    private static final Logger logger = LogManager.getLogger(DistributedQueryCoordinator.class);

    private static final long WORKER_TIMEOUT_MINUTES = 5;
    private static final int MIN_FILES_FOR_DISTRIBUTION = 2;

    private final ClusterService clusterService;
    private final TransportService transportService;
    private final StreamTransportService streamTransportService;

    /**
     * Creates a new distributed query coordinator.
     *
     * @param clusterService         cluster service for discovering data nodes
     * @param transportService       standard transport for single-response requests
     * @param streamTransportService Arrow Flight streaming transport (may be null)
     */
    public DistributedQueryCoordinator(
        ClusterService clusterService,
        TransportService transportService,
        StreamTransportService streamTransportService
    ) {
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.streamTransportService = streamTransportService;
    }

    /**
     * Determines whether this query should use distributed execution.
     *
     * @param fileInfos the data files from the scan plan
     * @return {@code true} if the query should be distributed across multiple nodes
     */
    public boolean shouldDistribute(List<IcebergScanPlan.FileInfo> fileInfos) {
        if (transportService == null && streamTransportService == null) {
            logger.debug("[DistributedQueryCoordinator] No transport available, skipping distribution");
            return false;
        }
        if (fileInfos == null || fileInfos.size() < MIN_FILES_FOR_DISTRIBUTION) {
            return false;
        }
        List<DiscoveryNode> dataNodes = getDataNodes();
        return dataNodes.size() > 1;
    }

    /**
     * Executes a distributed query using the split plan approach.
     *
     * @param splitPlan     the split plan from {@link PhysicalPlanSplitter}
     * @param fileInfos     file metadata from the Iceberg scan plan
     * @param storageConfig S3/storage configuration for workers
     * @param tableName     the table name for DataFusion registration
     * @return final result rows after coordinator merge
     */
    public Iterable<Object[]> execute(
        PhysicalPlanSplitter.SplitPlan splitPlan,
        List<IcebergScanPlan.FileInfo> fileInfos,
        Map<String, String> storageConfig,
        String tableName
    ) {
        long tExec0 = System.nanoTime();
        List<DiscoveryNode> dataNodes = getDataNodes();
        long totalFileSize = fileInfos.stream().mapToLong(IcebergScanPlan.FileInfo::getFileSizeInBytes).sum();

        logger.info("[Coordinator] ====== SCATTER-GATHER START ======");
        logger.info("[Coordinator] table={}, files={}, totalSize={} bytes, nodes={}, transport={}",
            tableName, fileInfos.size(), totalFileSize,
            dataNodes.size(), streamTransportService != null ? "ArrowFlight" : "Standard");
        for (int i = 0; i < dataNodes.size(); i++) {
            logger.info("[Coordinator] dataNode[{}]: {}", i, dataNodes.get(i).getName());
        }

        List<List<IcebergScanPlan.FileInfo>> partitions = FilePartitioner.partition(fileInfos, dataNodes.size());
        for (int i = 0; i < partitions.size(); i++) {
            List<IcebergScanPlan.FileInfo> partition = partitions.get(i);
            long partSize = partition.stream().mapToLong(IcebergScanPlan.FileInfo::getFileSizeInBytes).sum();
            logger.info("[Coordinator] partition[{}] -> [{}]: {} files, {} bytes", i, dataNodes.get(i).getName(), partition.size(), partSize);
        }

        logger.info("[Coordinator] ------ SCATTER: sending to {} workers ------", partitions.size());
        long tScatter0 = System.nanoTime();
        List<LakehouseWorkerResponse> responses;
        if (streamTransportService != null) {
            responses = fanOutViaStreaming(partitions, dataNodes, splitPlan, storageConfig, tableName);
        } else {
            responses = fanOutViaStandard(partitions, dataNodes, splitPlan, storageConfig, tableName);
        }
        long tScatter1 = System.nanoTime();
        logger.info("[Coordinator] ------ GATHER: {} responses in {} ms ------", responses.size(), (tScatter1 - tScatter0) / 1_000_000);
        for (int i = 0; i < responses.size(); i++) {
            LakehouseWorkerResponse r = responses.get(i);
            logger.info("[Coordinator]   response[{}]: hasIPC={}, ipcBytes={}, rows={}",
                i, r.hasIpcBytes(), r.hasIpcBytes() ? r.getIpcBytes().length : 0, r.getRows().length);
        }

        // Try native DataFusion merge via Arrow IPC
        long tMerge0 = System.nanoTime();
        Iterable<Object[]> ipcResult = tryIpcMerge(responses, splitPlan, tableName);
        if (ipcResult != null) {
            long tMerge1 = System.nanoTime();
            logger.info("[Coordinator] ------ MERGE (IPC/DataFusion): {} ms ------", (tMerge1 - tMerge0) / 1_000_000);
            logger.info("[Coordinator] [TIMING] Total: {} ms (scatter={} ms, merge={} ms)",
                (tMerge1 - tExec0) / 1_000_000, (tScatter1 - tScatter0) / 1_000_000, (tMerge1 - tMerge0) / 1_000_000);
            logger.info("[Coordinator] ====== SCATTER-GATHER END ======");
            return ipcResult;
        }

        // Fallback: Java merge
        logger.info("[Coordinator] ------ MERGE (Java fallback) ------");
        List<Object[]> partialResults = new ArrayList<>();
        for (LakehouseWorkerResponse response : responses) {
            for (Object[] row : response.getRows()) {
                partialResults.add(row);
            }
        }
        List<Object[]> merged = CoordinatorMerger.merge(partialResults, splitPlan);
        long tEnd = System.nanoTime();
        logger.info("[Coordinator] Java merge: {} partial -> {} result rows, {} ms",
            partialResults.size(), merged.size(), (tEnd - tMerge0) / 1_000_000);
        logger.info("[Coordinator] [TIMING] Total: {} ms", (tEnd - tExec0) / 1_000_000);
        logger.info("[Coordinator] ====== SCATTER-GATHER END ======");
        return merged;
    }

    /**
     * Attempts to merge worker results using native DataFusion via Arrow IPC.
     * Returns merged results if all workers returned IPC bytes and the global
     * backend executor is available; returns {@code null} to fall back to Java merge.
     *
     * @param responses  worker responses
     * @param splitPlan  the split plan with coordinator SQL
     * @param tableName  the original table name
     * @return merged results, or {@code null} if IPC merge is not possible
     */
    private Iterable<Object[]> tryIpcMerge(
        List<LakehouseWorkerResponse> responses,
        PhysicalPlanSplitter.SplitPlan splitPlan,
        String tableName
    ) {
        if (responses.isEmpty()) {
            return null;
        }

        // Check if ALL responses carry IPC bytes
        for (LakehouseWorkerResponse response : responses) {
            if (!response.hasIpcBytes()) {
                logger.debug("[DistributedQueryCoordinator] Response missing IPC bytes, falling back to Java merge");
                return null;
            }
        }

        // Check if the global backend executor is available
        Function<ExternalScanContext, Iterable<Object[]>> executor = ExternalScanContext.getGlobalBackendExecutor();
        if (executor == null) {
            logger.debug("[DistributedQueryCoordinator] No global backend executor, falling back to Java merge");
            return null;
        }

        // Collect IPC byte arrays from all workers
        byte[][] ipcBatches = new byte[responses.size()][];
        long totalBytes = 0;
        for (int i = 0; i < responses.size(); i++) {
            ipcBatches[i] = responses.get(i).getIpcBytes();
            totalBytes += ipcBatches[i].length;
        }

        String coordinatorSql = splitPlan.getCoordinatorSql();
        logger.info("[DistributedQueryCoordinator] IPC merge: {} workers, {} total bytes, coordinatorSql={}",
            ipcBatches.length, totalBytes, coordinatorSql);

        // Create a merge context: the backend executor detects ipcBatches != null
        // and routes to DataFusionPlugin.executeMergeQuery() via NativeBridge.mergeIpcBatches()
        ExternalScanContext mergeContext = new ExternalScanContext(
            PhysicalPlanSplitter.PARTIAL_TABLE,
            List.of(),
            coordinatorSql,
            Map.of()
        );
        mergeContext.setIpcBatches(ipcBatches);

        try {
            Iterable<Object[]> result = executor.apply(mergeContext);
            logger.info("[DistributedQueryCoordinator] IPC merge completed successfully");
            return result;
        } catch (Exception e) {
            logger.warn("[DistributedQueryCoordinator] IPC merge failed, falling back to Java merge", e);
            return null;
        }
    }

    /**
     * Fan out via Arrow Flight streaming transport (batched responses).
     */
    private List<LakehouseWorkerResponse> fanOutViaStreaming(
        List<List<IcebergScanPlan.FileInfo>> partitions,
        List<DiscoveryNode> dataNodes,
        PhysicalPlanSplitter.SplitPlan splitPlan,
        Map<String, String> storageConfig,
        String tableName
    ) {
        for (DiscoveryNode node : dataNodes) {
            streamTransportService.connectToNode(node);
        }

        CountDownLatch latch = new CountDownLatch(partitions.size());
        List<LakehouseWorkerResponse> responses = Collections.synchronizedList(new ArrayList<>());
        AtomicReference<Exception> firstError = new AtomicReference<>();

        for (int i = 0; i < partitions.size(); i++) {
            DiscoveryNode targetNode = dataNodes.get(i);
            LakehouseWorkerRequest request = buildRequest(partitions.get(i), splitPlan, storageConfig, tableName);

            streamTransportService.sendRequest(
                targetNode,
                LakehouseWorkerAction.NAME,
                request,
                TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build(),
                new TransportResponseHandler<LakehouseWorkerResponse>() {
                    @Override
                    public LakehouseWorkerResponse read(StreamInput in) throws IOException {
                        return new LakehouseWorkerResponse(in);
                    }

                    @Override
                    public void handleResponse(LakehouseWorkerResponse response) {
                        responses.add(response);
                        latch.countDown();
                    }

                    @Override
                    public void handleStreamResponse(StreamTransportResponse<LakehouseWorkerResponse> streamResponse) {
                        try {
                            LakehouseWorkerResponse batch;
                            while ((batch = streamResponse.nextResponse()) != null) {
                                responses.add(batch);
                            }
                        } catch (Exception e) {
                            streamResponse.cancel("Worker processing error", e);
                            firstError.compareAndSet(null, e);
                        } finally {
                            try { streamResponse.close(); } catch (IOException ignored) {}
                            latch.countDown();
                        }
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        logger.error("[DistributedQueryCoordinator] Worker [{}] failed", targetNode.getName(), exp);
                        firstError.compareAndSet(null, exp);
                        latch.countDown();
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.GENERIC;
                    }
                }
            );
        }

        waitForWorkers(latch, firstError, partitions.size(), responses.size());
        return responses;
    }

    /**
     * Fan out via standard transport (single response per worker).
     */
    private List<LakehouseWorkerResponse> fanOutViaStandard(
        List<List<IcebergScanPlan.FileInfo>> partitions,
        List<DiscoveryNode> dataNodes,
        PhysicalPlanSplitter.SplitPlan splitPlan,
        Map<String, String> storageConfig,
        String tableName
    ) {
        CountDownLatch latch = new CountDownLatch(partitions.size());
        List<LakehouseWorkerResponse> responses = Collections.synchronizedList(new ArrayList<>());
        AtomicReference<Exception> firstError = new AtomicReference<>();

        for (int i = 0; i < partitions.size(); i++) {
            DiscoveryNode targetNode = dataNodes.get(i);
            LakehouseWorkerRequest request = buildRequest(partitions.get(i), splitPlan, storageConfig, tableName);

            logger.info("[DistributedQueryCoordinator] Sending standard request to node [{}]: {} files",
                targetNode.getName(), partitions.get(i).size());

            transportService.sendRequest(
                targetNode,
                LakehouseWorkerAction.NAME,
                request,
                new TransportResponseHandler<LakehouseWorkerResponse>() {
                    @Override
                    public LakehouseWorkerResponse read(StreamInput in) throws IOException {
                        return new LakehouseWorkerResponse(in);
                    }

                    @Override
                    public void handleResponse(LakehouseWorkerResponse response) {
                        logger.info("[DistributedQueryCoordinator] Received response from [{}]: {} rows",
                            targetNode.getName(), response.getRows().length);
                        responses.add(response);
                        latch.countDown();
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        logger.error("[DistributedQueryCoordinator] Worker [{}] failed", targetNode.getName(), exp);
                        firstError.compareAndSet(null, exp);
                        latch.countDown();
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.GENERIC;
                    }
                }
            );
        }

        waitForWorkers(latch, firstError, partitions.size(), responses.size());
        return responses;
    }

    private LakehouseWorkerRequest buildRequest(
        List<IcebergScanPlan.FileInfo> partition,
        PhysicalPlanSplitter.SplitPlan splitPlan,
        Map<String, String> storageConfig,
        String tableName
    ) {
        String[] filePaths = partition.stream()
            .map(IcebergScanPlan.FileInfo::getPath)
            .toArray(String[]::new);
        return new LakehouseWorkerRequest(filePaths, splitPlan.getWorkerSql(), storageConfig, tableName);
    }

    private void waitForWorkers(CountDownLatch latch, AtomicReference<Exception> firstError, int workerCount, int responseCount) {
        try {
            boolean completed = latch.await(WORKER_TIMEOUT_MINUTES, TimeUnit.MINUTES);
            if (!completed) {
                throw new RuntimeException(
                    "Distributed query timed out after " + WORKER_TIMEOUT_MINUTES + " minutes. "
                        + "Received " + responseCount + " responses from " + workerCount + " workers."
                );
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Distributed query interrupted", e);
        }

        Exception error = firstError.get();
        if (error != null) {
            throw new RuntimeException("Distributed query failed on one or more worker nodes", error);
        }
    }

    private List<DiscoveryNode> getDataNodes() {
        DiscoveryNodes nodes = clusterService.state().nodes();
        return new ArrayList<>(nodes.getDataNodes().values());
    }
}

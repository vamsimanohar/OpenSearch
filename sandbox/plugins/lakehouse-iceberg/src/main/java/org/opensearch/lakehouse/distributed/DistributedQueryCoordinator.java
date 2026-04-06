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
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Orchestrates distributed Iceberg query execution across cluster nodes using
 * a scatter-gather pattern.
 *
 * <p>When a query targets an Iceberg table and the cluster has multiple data nodes,
 * this coordinator:
 * <ol>
 *   <li>Partitions the data files across available data nodes using {@link FilePartitioner}</li>
 *   <li>Sends a {@link LakehouseWorkerRequest} to each node via the transport layer</li>
 *   <li>Collects {@link LakehouseWorkerResponse} results from all nodes</li>
 *   <li>Merges the partial results into a single result set</li>
 * </ol>
 *
 * <p>Falls back to single-node execution when:
 * <ul>
 *   <li>The cluster has only one data node</li>
 *   <li>The scan has only one data file</li>
 *   <li>The backend executor is not available</li>
 * </ul>
 */
public class DistributedQueryCoordinator {

    private static final Logger logger = LogManager.getLogger(DistributedQueryCoordinator.class);

    /** Maximum time to wait for all worker responses. */
    private static final long WORKER_TIMEOUT_MINUTES = 5;

    /** Minimum number of files required before distributing (avoid overhead for trivial scans). */
    private static final int MIN_FILES_FOR_DISTRIBUTION = 2;

    private final ClusterService clusterService;
    private final StreamTransportService streamTransportService;

    /**
     * Creates a new distributed query coordinator.
     *
     * @param clusterService         cluster service for discovering data nodes
     * @param streamTransportService Arrow Flight streaming transport for sending requests to worker nodes
     */
    public DistributedQueryCoordinator(ClusterService clusterService, StreamTransportService streamTransportService) {
        this.clusterService = clusterService;
        this.streamTransportService = streamTransportService;
    }

    /**
     * Determines whether this query should use distributed execution.
     *
     * @param fileInfos the data files from the scan plan
     * @return {@code true} if the query should be distributed across multiple nodes
     */
    public boolean shouldDistribute(List<IcebergScanPlan.FileInfo> fileInfos) {
        if (streamTransportService == null) {
            logger.debug("[DistributedQueryCoordinator] Arrow Flight streaming transport not available, skipping distribution");
            return false;
        }
        if (fileInfos == null || fileInfos.size() < MIN_FILES_FOR_DISTRIBUTION) {
            return false;
        }
        List<DiscoveryNode> dataNodes = getDataNodes();
        return dataNodes.size() > 1;
    }

    /**
     * Executes a distributed query by splitting files across cluster data nodes
     * and merging the results.
     *
     * <p>The caller must have already verified that distributed execution is appropriate
     * via {@link #shouldDistribute(List)}.
     *
     * @param scanContext the scan context with file paths, Substrait plan, and storage config
     * @param fileInfos   the file metadata from the Iceberg scan plan (with sizes for balanced partitioning)
     * @param plan        the distribution plan describing how to merge worker results
     * @return merged result rows from all worker nodes
     * @throws RuntimeException if any worker fails or the operation times out
     */
    public Iterable<Object[]> execute(ExternalScanContext scanContext, List<IcebergScanPlan.FileInfo> fileInfos, DistributionPlan plan) {
        List<DiscoveryNode> dataNodes = getDataNodes();

        logger.info("[DistributedQueryCoordinator] Distributing query via Arrow Flight: table={}, files={}, nodes={}",
            scanContext.getTableName(), fileInfos.size(), dataNodes.size());

        // Partition files across nodes using size-balanced greedy assignment
        List<List<IcebergScanPlan.FileInfo>> partitions = FilePartitioner.partition(fileInfos, dataNodes.size());

        logger.info("[DistributedQueryCoordinator] Created {} partitions across {} data nodes",
            partitions.size(), dataNodes.size());
        if (logger.isDebugEnabled()) {
            for (int i = 0; i < partitions.size(); i++) {
                List<IcebergScanPlan.FileInfo> partition = partitions.get(i);
                long totalSize = partition.stream().mapToLong(IcebergScanPlan.FileInfo::getFileSizeInBytes).sum();
                logger.debug("[DistributedQueryCoordinator] Partition {} -> node [{}]: {} files, {} bytes",
                    i, dataNodes.get(i).getName(), partition.size(), totalSize);
            }
        }

        // Connect to all target nodes via Arrow Flight streaming transport
        for (DiscoveryNode node : dataNodes) {
            streamTransportService.connectToNode(node);
        }

        // Fan out streaming requests to worker nodes
        CountDownLatch latch = new CountDownLatch(partitions.size());
        List<LakehouseWorkerResponse> responses = Collections.synchronizedList(new ArrayList<>());
        AtomicReference<Exception> firstError = new AtomicReference<>();

        for (int i = 0; i < partitions.size(); i++) {
            List<IcebergScanPlan.FileInfo> partition = partitions.get(i);
            DiscoveryNode targetNode = dataNodes.get(i);

            String[] filePaths = partition.stream()
                .map(IcebergScanPlan.FileInfo::getPath)
                .toArray(String[]::new);

            LakehouseWorkerRequest request = new LakehouseWorkerRequest(
                filePaths,
                scanContext.getSubstraitPlan(),
                scanContext.getStorageConfig(),
                scanContext.getTableName()
            );

            logger.debug("[DistributedQueryCoordinator] Sending streaming request to node [{}]: {} files",
                targetNode.getName(), filePaths.length);

            // Use Arrow Flight streaming transport — worker sends batched responses
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
                        // Single-response path (should not be called for streaming)
                        logger.debug("[DistributedQueryCoordinator] Received single response from node [{}]: {} rows",
                            targetNode.getName(), response.getRows().length);
                        responses.add(response);
                        latch.countDown();
                    }

                    @Override
                    public void handleStreamResponse(StreamTransportResponse<LakehouseWorkerResponse> streamResponse) {
                        try {
                            int batchCount = 0;
                            int totalRows = 0;
                            LakehouseWorkerResponse batch;
                            while ((batch = streamResponse.nextResponse()) != null) {
                                batchCount++;
                                totalRows += batch.getRows().length;
                                responses.add(batch);
                            }
                            logger.debug("[DistributedQueryCoordinator] Received {} batches ({} rows) from node [{}]",
                                batchCount, totalRows, targetNode.getName());
                        } catch (Exception e) {
                            streamResponse.cancel("Worker processing error", e);
                            logger.error("[DistributedQueryCoordinator] Stream error from node [{}]", targetNode.getName(), e);
                            firstError.compareAndSet(null, e);
                        } finally {
                            try {
                                streamResponse.close();
                            } catch (IOException ioe) {
                                logger.warn("[DistributedQueryCoordinator] Error closing stream from node [{}]",
                                    targetNode.getName(), ioe);
                            }
                            latch.countDown();
                        }
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        logger.error("[DistributedQueryCoordinator] Worker node [{}] failed", targetNode.getName(), exp);
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

        // Wait for all workers to complete
        try {
            boolean completed = latch.await(WORKER_TIMEOUT_MINUTES, TimeUnit.MINUTES);
            if (!completed) {
                throw new RuntimeException(
                    "Distributed query timed out after " + WORKER_TIMEOUT_MINUTES + " minutes. "
                        + "Received " + responses.size() + " batches from " + partitions.size() + " workers."
                );
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Distributed query interrupted", e);
        }

        // Check for errors
        Exception error = firstError.get();
        if (error != null) {
            throw new RuntimeException(
                "Distributed query failed on one or more worker nodes", error
            );
        }

        // Merge all batches from all workers using the distribution plan
        return DistributedResultMerger.merge(responses, plan);
    }

    /**
     * Returns the current data nodes in the cluster.
     *
     * @return list of data nodes (may include the local node)
     */
    private List<DiscoveryNode> getDataNodes() {
        DiscoveryNodes nodes = clusterService.state().nodes();
        return new ArrayList<>(nodes.getDataNodes().values());
    }
}

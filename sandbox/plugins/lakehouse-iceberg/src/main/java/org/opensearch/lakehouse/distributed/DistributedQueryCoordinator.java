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
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

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
    private final TransportService transportService;

    /**
     * Creates a new distributed query coordinator.
     *
     * @param clusterService   cluster service for discovering data nodes
     * @param transportService transport service for sending requests to worker nodes
     */
    public DistributedQueryCoordinator(ClusterService clusterService, TransportService transportService) {
        this.clusterService = clusterService;
        this.transportService = transportService;
    }

    /**
     * Determines whether this query should use distributed execution.
     *
     * @param fileInfos the data files from the scan plan
     * @return {@code true} if the query should be distributed across multiple nodes
     */
    public boolean shouldDistribute(List<IcebergScanPlan.FileInfo> fileInfos) {
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

        logger.info("[DistributedQueryCoordinator] Distributing query: table={}, files={}, nodes={}",
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

        // Fan out requests to worker nodes
        CountDownLatch latch = new CountDownLatch(partitions.size());
        List<LakehouseWorkerResponse> responses = Collections.synchronizedList(new ArrayList<>(partitions.size()));
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

            logger.debug("[DistributedQueryCoordinator] Sending request to node [{}]: {} files",
                targetNode.getName(), filePaths.length);

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
                        logger.debug("[DistributedQueryCoordinator] Received response from node [{}]: {} rows",
                            targetNode.getName(), response.getRows().length);
                        responses.add(response);
                        latch.countDown();
                    }

                    @Override
                    public void handleException(org.opensearch.transport.TransportException exp) {
                        logger.error("[DistributedQueryCoordinator] Worker node [{}] failed", targetNode.getName(), exp);
                        firstError.compareAndSet(null, exp);
                        latch.countDown();
                    }

                    @Override
                    public String executor() {
                        // Must use GENERIC, not SAME, to avoid transport thread deadlock.
                        // SAME runs on the transport I/O thread, which can deadlock when
                        // the coordinator is blocking on latch.await() while responses
                        // need the same thread pool to be delivered.
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
                        + "Received " + responses.size() + " of " + partitions.size() + " responses."
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

        // Merge responses from all workers using the distribution plan
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

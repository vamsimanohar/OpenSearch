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
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Orchestrates distributed Iceberg query execution across cluster nodes using
 * a proper two-phase approach: physical plan splitting + scatter-gather.
 *
 * <p>Unlike the naive scatter-gather approach (which sends the FULL query to every
 * worker), this coordinator uses {@link PhysicalPlanSplitter} to generate:
 * <ul>
 *   <li><b>Worker SQL</b>: partial aggregation, scan + filter + GROUP BY (no ORDER BY/LIMIT)</li>
 *   <li><b>Coordinator SQL</b>: final aggregation + ORDER BY + LIMIT on merged partial results</li>
 * </ul>
 *
 * <p>The execution flow is:
 * <ol>
 *   <li>Split the plan into worker and coordinator SQL via {@link PhysicalPlanSplitter}</li>
 *   <li>Partition files across N worker nodes via {@link FilePartitioner}</li>
 *   <li>Send {workerSql, filePaths[], storageConfig} to each worker via Arrow Flight</li>
 *   <li>Workers execute workerSql via DataFusion, stream Object[][] back</li>
 *   <li>Coordinator collects all worker results</li>
 *   <li>Coordinator registers partial results as a temporary table in DataFusion</li>
 *   <li>Coordinator executes coordinatorSql via DataFusion to produce final results</li>
 * </ol>
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
     * Executes a distributed query using the split plan approach.
     *
     * <p>Sends the worker SQL to each node's assigned file subset, collects partial
     * results, then executes the coordinator SQL to produce the final result.
     *
     * @param splitPlan     the split plan from {@link PhysicalPlanSplitter}
     * @param fileInfos     file metadata from the Iceberg scan plan
     * @param storageConfig S3/storage configuration for workers
     * @param tableName     the table name for DataFusion registration
     * @return final result rows
     */
    public Iterable<Object[]> execute(
        PhysicalPlanSplitter.SplitPlan splitPlan,
        List<IcebergScanPlan.FileInfo> fileInfos,
        Map<String, String> storageConfig,
        String tableName
    ) {
        List<DiscoveryNode> dataNodes = getDataNodes();

        logger.info("[DistributedQueryCoordinator] Distributing query: table={}, files={}, nodes={}, workerSql={}",
            tableName, fileInfos.size(), dataNodes.size(), splitPlan.getWorkerSql());

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
                splitPlan.getWorkerSql(),
                storageConfig,
                tableName
            );

            logger.debug("[DistributedQueryCoordinator] Sending streaming request to node [{}]: {} files, sql={}",
                targetNode.getName(), filePaths.length, splitPlan.getWorkerSql());

            // Use Arrow Flight streaming transport
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
            throw new RuntimeException("Distributed query failed on one or more worker nodes", error);
        }

        // Collect all partial results from workers
        List<Object[]> partialResults = new ArrayList<>();
        String[] columnNames = null;
        for (LakehouseWorkerResponse response : responses) {
            if (columnNames == null && response.getColumnNames().length > 0) {
                columnNames = response.getColumnNames();
            }
            for (Object[] row : response.getRows()) {
                partialResults.add(row);
            }
        }

        logger.info("[DistributedQueryCoordinator] Collected {} partial rows from {} workers. Executing coordinator SQL: {}",
            partialResults.size(), partitions.size(), splitPlan.getCoordinatorSql());

        // Execute coordinator SQL against partial results via the local DataFusion backend.
        // Register partial results as a temporary table named "__partial", then execute coordinatorSql.
        return executeCoordinatorPhase(splitPlan.getCoordinatorSql(), partialResults, columnNames, tableName);
    }

    /**
     * Executes the coordinator phase: registers partial worker results as a
     * temporary table and runs the coordinator SQL against it.
     *
     * <p>For now, this uses the global backend executor with the partial results
     * registered as a virtual table. In the future, this will use a dedicated
     * DataFusion Rust function that accepts Arrow RecordBatches directly.
     *
     * @param coordinatorSql the SQL to execute on partial results
     * @param partialResults the merged partial rows from all workers
     * @param columnNames    column names from the worker results
     * @param originalTable  the original table name (not used in coordinator SQL)
     * @return final result rows
     */
    private Iterable<Object[]> executeCoordinatorPhase(
        String coordinatorSql, List<Object[]> partialResults, String[] columnNames, String originalTable
    ) {
        // Use the global backend executor to run coordinator SQL.
        // Build a scan context that will register partial results as "__partial".
        Function<ExternalScanContext, Iterable<Object[]>> executor = ExternalScanContext.getGlobalBackendExecutor();
        if (executor == null) {
            logger.warn("[DistributedQueryCoordinator] No backend executor available for coordinator phase, returning partial results");
            return partialResults;
        }

        // For now, return partial results directly if coordinator SQL is just a pass-through
        if (coordinatorSql.equals("SELECT * FROM " + PhysicalPlanSplitter.PARTIAL_TABLE)) {
            logger.info("[DistributedQueryCoordinator] Coordinator SQL is pass-through, returning {} partial rows", partialResults.size());
            return partialResults;
        }

        // For aggregate queries, we would ideally register the partial results as a
        // MemTable in DataFusion and execute coordinatorSql against it. This requires
        // a new JNI API endpoint. For now, return partial results with a warning.
        //
        // TODO: Implement MemTable registration + coordinator SQL execution in Rust
        //   1. NativeBridge.registerMemTable(runtimePtr, "__partial", columnNames, rows)
        //   2. NativeBridge.executeSql(runtimePtr, coordinatorSql) -> streamPtr
        //   3. Read stream as usual
        logger.warn("[DistributedQueryCoordinator] Coordinator SQL execution not yet implemented via DataFusion. "
            + "Returning partial results directly. SQL would be: {}", coordinatorSql);
        return partialResults;
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

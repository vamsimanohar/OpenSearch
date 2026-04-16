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
import org.opensearch.lakehouse.LakehousePlugin;
import org.opensearch.lakehouse.distributed.merge.MergeStrategy;
import org.opensearch.lakehouse.distributed.merge.ResultMerger;
import org.opensearch.lakehouse.distributed.merge.ResultSerializer;
import org.opensearch.lakehouse.distributed.worker.WorkerQueryAction;
import org.opensearch.lakehouse.distributed.worker.WorkerQueryExecutor;
import org.opensearch.lakehouse.distributed.worker.WorkerQueryRequest;
import org.opensearch.lakehouse.distributed.worker.WorkerQueryResponse;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

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

        logger.info(
            "[ScanExecutor] Distributing query across {} workers, strategy={}, files={}",
            workers.size(),
            analysis.strategy,
            filePaths.size()
        );

        // For TOPK_MERGE, sort columns may not be in the SELECT output.
        // Detect missing sort columns, add them to worker SQL, and strip after merge.
        String workerSql = sqlQuery;
        int[] mergeSortColumns = analysis.sortColumns;
        int outputColumnCount = analysis.outputColumnCount;
        boolean needsColumnStripping = false;

        if (analysis.strategy == MergeStrategy.TOPK_MERGE && analysis.sortColumnNames != null) {
            List<String> outputNames = relNode.getRowType().getFieldNames();

            List<String> missingColumns = new ArrayList<>();
            for (String sortColName : analysis.sortColumnNames) {
                if (sortColName != null && !outputNames.contains(sortColName)) {
                    missingColumns.add(sortColName);
                }
            }

            List<String> workerOutputNames = new ArrayList<>(outputNames);
            if (!missingColumns.isEmpty()) {
                workerSql = addColumnsToSelect(sqlQuery, missingColumns);
                workerOutputNames.addAll(missingColumns);
                needsColumnStripping = true;
                logger.info(
                    "[ScanExecutor] TOPK_MERGE: added {} sort columns to worker SQL: {}",
                    missingColumns.size(),
                    missingColumns
                );
            }

            // Remap sort indices to worker output column positions (name-based)
            mergeSortColumns = new int[analysis.sortColumnNames.length];
            for (int i = 0; i < analysis.sortColumnNames.length; i++) {
                mergeSortColumns[i] = workerOutputNames.indexOf(analysis.sortColumnNames[i]);
            }
        }

        // Capture effectively-final locals for lambda
        final String finalWorkerSql = workerSql;
        final int[] finalMergeSortColumns = mergeSortColumns;
        final boolean finalNeedsStripping = needsColumnStripping;
        final int finalOutputColumnCount = outputColumnCount;

        // Partition files across workers
        List<FilePartitioner.FileAssignment> assignments = FilePartitioner.partition(filePaths, fileSizes, workers.size());

        // Dispatch requests and collect responses asynchronously
        dispatchAndCollect(workers, assignments, finalWorkerSql, storageConfig, tableName, ActionListener.wrap(
            responses -> {
                try {
                    WorkerQueryResponse merged = ResultMerger.merge(
                        responses, analysis.strategy, finalMergeSortColumns, analysis.sortAsc, analysis.limit, analysis.aggKinds
                    );
                    if (finalNeedsStripping) {
                        merged = stripExtraColumns(merged, finalOutputColumnCount);
                    }
                    listener.onResponse(ResultSerializer.toRows(merged));
                } catch (Exception e) {
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
     * Inserts additional columns into the SELECT clause of a SQL query.
     * Finds the first {@code FROM} keyword and inserts the quoted column names before it.
     * Used for TOPK_MERGE when ORDER BY columns are not in the original SELECT.
     *
     * @param sql     the original SQL query
     * @param columns column names to add to the SELECT clause
     * @return the rewritten SQL with additional columns
     */
    static String addColumnsToSelect(String sql, List<String> columns) {
        // Find the first FROM keyword preceded by whitespace (space or newline).
        // Calcite generates multi-line SQL where FROM starts on a new line.
        int fromKeywordIdx = -1;
        String upper = sql.toUpperCase();
        int searchFrom = 0;
        while (searchFrom < upper.length()) {
            int idx = upper.indexOf("FROM ", searchFrom);
            if (idx < 0) break;
            if (idx > 0 && Character.isWhitespace(sql.charAt(idx - 1))) {
                fromKeywordIdx = idx;
                break;
            }
            searchFrom = idx + 4;
        }
        if (fromKeywordIdx < 0) {
            return sql;
        }
        // Insert columns right before the whitespace that precedes FROM
        int insertPos = fromKeywordIdx;
        while (insertPos > 0 && Character.isWhitespace(sql.charAt(insertPos - 1))) {
            insertPos--;
        }
        StringBuilder sb = new StringBuilder(sql.substring(0, insertPos));
        for (String col : columns) {
            sb.append(", \"").append(col).append("\"");
        }
        sb.append(sql.substring(insertPos));
        return sb.toString();
    }

    /**
     * Removes columns beyond the original output count from a worker response.
     * Used after TOPK_MERGE to strip sort-only columns that were added to the worker SQL.
     *
     * @param response          the merged response with extra columns
     * @param outputColumnCount the number of columns in the original query output
     * @return a response with only the original output columns
     */
    static WorkerQueryResponse stripExtraColumns(WorkerQueryResponse response, int outputColumnCount) {
        if (outputColumnCount >= response.getColumnNames().size()) {
            return response;
        }
        List<String> names = new ArrayList<>(response.getColumnNames().subList(0, outputColumnCount));
        List<String> types = new ArrayList<>(response.getColumnTypes().subList(0, outputColumnCount));
        Object[][] data = response.getColumnData();
        Object[][] trimmed = new Object[outputColumnCount][];
        System.arraycopy(data, 0, trimmed, 0, outputColumnCount);
        return new WorkerQueryResponse(names, types, response.getRowCount(), trimmed);
    }
}

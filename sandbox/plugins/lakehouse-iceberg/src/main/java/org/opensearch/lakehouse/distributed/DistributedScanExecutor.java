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
import org.apache.calcite.sql.SqlKind;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.GroupedActionListener;
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
import org.opensearch.lakehouse.distributed.merge.MergeStrategy;
import org.opensearch.lakehouse.distributed.merge.ResultSerializer;
import org.opensearch.lakehouse.distributed.merge.WorkerResponseToArrow;
import org.opensearch.lakehouse.distributed.worker.WorkerQueryAction;
import org.opensearch.lakehouse.distributed.worker.WorkerQueryExecutor;
import org.opensearch.lakehouse.distributed.worker.WorkerQueryRequest;
import org.opensearch.lakehouse.distributed.worker.WorkerQueryResponse;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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

    /** Coordinator SQL for CONCAT merge: passes all input rows through unchanged. */
    static final String CONCAT_COORDINATOR_SQL = "SELECT * FROM __exchange_input__";

    /** Timeout for native coordinator SQL execution. */
    static final long NATIVE_TIMEOUT_MINUTES = 15L;

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final NodeDiscovery nodeDiscovery;
    private final DataWarehouseQueryEngine queryEngine;
    private final NativeIpcExecutor nativeIpcExecutor;

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
        this(transportService, clusterService, nodeDiscovery, queryEngine, NativeBridge::executeFromIpcAsync);
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
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.nodeDiscovery = nodeDiscovery;
        this.queryEngine = queryEngine;
        this.nativeIpcExecutor = nativeIpcExecutor;
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

        // For TWO_PHASE_GROUP_BY: strip ORDER BY/LIMIT/OFFSET from worker SQL.
        // Workers must return ALL groups for their partition — coordinator re-aggregates and applies ordering.
        String workerSql = sqlQuery;
        if (analysis.strategy == MergeStrategy.TWO_PHASE_GROUP_BY && analysis.sortColumns != null) {
            workerSql = stripOrderByLimitOffset(sqlQuery);
            logger.info("[ScanExecutor] TWO_PHASE_GROUP_BY: stripped ORDER BY/LIMIT/OFFSET from worker SQL");
        }

        // For TOPK_MERGE, sort columns may not be in the SELECT output.
        // Detect missing sort columns and add them to worker SQL so they appear in Arrow IPC.
        // The coordinator SQL handles stripping them from the final output.
        // Workers use generic col_N naming, so we track sort column indices for coordinator SQL.
        int outputColumnCount = analysis.outputColumnCount;
        int[] sortColumnIndices = analysis.sortColumns;

        if (analysis.strategy == MergeStrategy.TOPK_MERGE && analysis.sortColumnNames != null) {
            List<String> outputNames = relNode.getRowType().getFieldNames();

            List<String> missingColumns = new ArrayList<>();
            for (String sortColName : analysis.sortColumnNames) {
                if (sortColName != null && !outputNames.contains(sortColName)) {
                    missingColumns.add(sortColName);
                }
            }

            if (!missingColumns.isEmpty()) {
                workerSql = addColumnsToSelect(sqlQuery, missingColumns);
                logger.info(
                    "[ScanExecutor] TOPK_MERGE: added {} sort columns to worker SQL: {}",
                    missingColumns.size(),
                    missingColumns
                );
            }

            // Remap sort indices to worker output positions (name-based lookup)
            List<String> workerOutputNames = new ArrayList<>(outputNames);
            workerOutputNames.addAll(missingColumns);
            sortColumnIndices = new int[analysis.sortColumnNames.length];
            for (int i = 0; i < analysis.sortColumnNames.length; i++) {
                sortColumnIndices[i] = workerOutputNames.indexOf(analysis.sortColumnNames[i]);
            }
        }

        // Capture effectively-final locals for lambda
        final String finalWorkerSql = workerSql;
        final int finalOutputColumnCount = outputColumnCount;
        final int[] finalSortColumnIndices = sortColumnIndices;

        // Partition files across workers
        List<FilePartitioner.FileAssignment> assignments = FilePartitioner.partition(filePaths, fileSizes, workers.size());

        // Dispatch requests and collect responses asynchronously
        dispatchAndCollect(workers, assignments, finalWorkerSql, storageConfig, tableName, ActionListener.wrap(
            responses -> {
                try {
                    String coordinatorSql = buildCoordinatorSql(
                        analysis, responses, finalOutputColumnCount, finalSortColumnIndices
                    );
                    mergeViaDataFusion(responses, coordinatorSql, analysis.strategy, listener);
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
     * Strips ORDER BY, LIMIT, and OFFSET clauses from a SQL query.
     * Used for TWO_PHASE_GROUP_BY workers that must return all groups, not a truncated top-K.
     * The coordinator SQL re-applies ordering after re-aggregation.
     *
     * @param sql the original SQL query with ORDER BY/LIMIT/OFFSET
     * @return the SQL with those clauses removed
     */
    static String stripOrderByLimitOffset(String sql) {
        String upper = sql.toUpperCase();
        int orderByIdx = findKeyword(upper, "ORDER BY");
        if (orderByIdx >= 0) {
            return sql.substring(0, orderByIdx).stripTrailing();
        }
        int limitIdx = findKeyword(upper, "LIMIT");
        if (limitIdx >= 0) {
            return sql.substring(0, limitIdx).stripTrailing();
        }
        return sql;
    }

    /**
     * Finds a SQL keyword preceded by whitespace in the uppercase query string.
     */
    private static int findKeyword(String upper, String keyword) {
        int searchFrom = 0;
        while (searchFrom < upper.length()) {
            int idx = upper.indexOf(keyword, searchFrom);
            if (idx < 0) return -1;
            if (idx == 0 || Character.isWhitespace(upper.charAt(idx - 1))) {
                return idx;
            }
            searchFrom = idx + keyword.length();
        }
        return -1;
    }

    /**
     * Builds the coordinator SQL for the given merge strategy and worker responses.
     * <p>
     * For CONCAT: {@code SELECT * FROM __exchange_input__}<br>
     * For GLOBAL_MERGE: re-aggregate partial results (SUM/MIN/MAX per column)<br>
     * For TOPK_MERGE: merge-sort and limit ({@code ORDER BY ... LIMIT N})<br>
     * For TWO_PHASE_GROUP_BY: re-aggregate grouped partial results
     */
    static String buildCoordinatorSql(
        QueryAnalyzer.AnalysisResult analysis,
        List<WorkerQueryResponse> responses,
        int outputColumnCount,
        int[] sortColumnIndices
    ) {
        return switch (analysis.strategy) {
            case CONCAT -> CONCAT_COORDINATOR_SQL;
            case GLOBAL_MERGE -> buildGlobalMergeCoordinatorSql(responses, analysis.aggKinds);
            case TOPK_MERGE -> buildTopKMergeCoordinatorSql(
                sortColumnIndices, analysis.sortAsc, analysis.limit, outputColumnCount
            );
            case TWO_PHASE_GROUP_BY -> buildTwoPhaseGroupByCoordinatorSql(
                responses, analysis.groupCount, analysis.aggKinds,
                analysis.sortColumns, analysis.sortAsc, analysis.limit, analysis.offset
            );
            case SINGLE_NODE -> throw new IllegalStateException("SINGLE_NODE should not reach coordinator SQL");
        };
    }

    /**
     * Builds coordinator SQL for GLOBAL_MERGE: re-aggregates single-row partial results.
     * <p>
     * Each worker returns one row of partial aggregates. The coordinator re-aggregates
     * by applying the correct function per column: SUM for SUM/COUNT, MIN for MIN, MAX for MAX.
     * <p>
     * Example: worker SQL {@code SELECT COUNT(*), SUM(x), MIN(y) FROM t} produces columns
     * {@code ["count(*)","sum(x)","min(y)"]}. Coordinator SQL becomes:
     * {@code SELECT SUM("count(*)"), SUM("sum(x)"), MIN("min(y)") FROM __exchange_input__}
     */
    static String buildGlobalMergeCoordinatorSql(List<WorkerQueryResponse> responses, SqlKind[] aggKinds) {
        WorkerQueryResponse first = responses.stream().filter(r -> r.getRowCount() > 0).findFirst().orElse(responses.get(0));
        List<String> columnNames = first.getColumnNames();

        StringBuilder sb = new StringBuilder("SELECT ");
        for (int i = 0; i < columnNames.size(); i++) {
            if (i > 0) sb.append(", ");
            SqlKind kind = (aggKinds != null && i < aggKinds.length) ? aggKinds[i] : SqlKind.SUM;
            String quotedCol = "\"" + columnNames.get(i) + "\"";
            String func = switch (kind) {
                case MIN -> "MIN";
                case MAX -> "MAX";
                default -> "SUM";
            };
            sb.append(func).append("(").append(quotedCol).append(")");
        }
        sb.append(" FROM __exchange_input__");
        return sb.toString();
    }

    /**
     * Builds coordinator SQL for TWO_PHASE_GROUP_BY: re-aggregates partial grouped results.
     * <p>
     * Each worker runs the full GROUP BY query on its partition, producing partial grouped
     * results. The coordinator re-groups by the same keys and applies re-aggregation:
     * SUM for SUM/COUNT (sum of partial counts = total count), MIN for MIN, MAX for MAX.
     * <p>
     * Worker columns are {@code col_0, col_1, ...} where the first {@code groupCount}
     * columns are group keys and the rest are aggregate values.
     * <p>
     * Example: worker SQL {@code SELECT region, COUNT(*), SUM(x) FROM t GROUP BY region}
     * produces {@code [col_0=region, col_1=count, col_2=sum]}. Coordinator SQL:
     * {@code SELECT "col_0", SUM("col_1"), SUM("col_2") FROM __exchange_input__ GROUP BY "col_0"}
     */
    static String buildTwoPhaseGroupByCoordinatorSql(
        List<WorkerQueryResponse> responses,
        int groupCount,
        SqlKind[] aggKinds,
        int[] sortColumns,
        boolean[] sortAsc,
        int limit,
        int offset
    ) {
        WorkerQueryResponse first = responses.stream().filter(r -> r.getRowCount() > 0).findFirst().orElse(responses.get(0));
        int totalCols = first.getColumnNames().size();

        StringBuilder sb = new StringBuilder("SELECT ");
        for (int i = 0; i < totalCols; i++) {
            if (i > 0) sb.append(", ");
            String quotedCol = "\"col_" + i + "\"";
            if (i < groupCount) {
                sb.append(quotedCol);
            } else {
                int aggIdx = i - groupCount;
                SqlKind kind = (aggKinds != null && aggIdx < aggKinds.length) ? aggKinds[aggIdx] : SqlKind.SUM;
                String func = switch (kind) {
                    case MIN -> "MIN";
                    case MAX -> "MAX";
                    default -> "SUM";
                };
                sb.append(func).append("(").append(quotedCol).append(")");
            }
        }
        sb.append(" FROM __exchange_input__ GROUP BY ");
        for (int i = 0; i < groupCount; i++) {
            if (i > 0) sb.append(", ");
            sb.append("\"col_").append(i).append("\"");
        }

        if (sortColumns != null && sortColumns.length > 0) {
            sb.append(" ORDER BY ");
            for (int i = 0; i < sortColumns.length; i++) {
                if (i > 0) sb.append(", ");
                // Use 1-based column position — safe after GROUP BY because aggregate
                // columns can't be referenced by name when wrapped in SUM/MIN/MAX
                sb.append(sortColumns[i] + 1);
                sb.append(sortAsc[i] ? " ASC" : " DESC");
            }
        }
        if (limit > 0) {
            sb.append(" LIMIT ").append(limit);
        }
        if (offset > 0) {
            sb.append(" OFFSET ").append(offset);
        }
        return sb.toString();
    }

    /**
     * Builds coordinator SQL for TOPK_MERGE: merge-sorts pre-sorted worker results and limits.
     * <p>
     * Workers each return their local top-K rows with generic {@code col_N} naming.
     * The coordinator merge-sorts all results and takes the global top-K.
     * When extra sort columns were appended to the worker SQL (not in the original SELECT),
     * a subquery strips them by projecting only the first {@code outputColumnCount} columns.
     * <p>
     * Example without extra columns:
     * {@code SELECT * FROM __exchange_input__ ORDER BY "col_2" ASC LIMIT 10}
     * <p>
     * Example with extra sort columns stripped:
     * {@code SELECT "col_0", "col_1" FROM (SELECT * FROM __exchange_input__ ORDER BY "col_3" ASC LIMIT 10)}
     *
     * @param sortColumnIndices position-based indices into the worker output (col_N)
     * @param sortAsc           sort direction per sort column (true=ASC)
     * @param limit             LIMIT value (0 = no limit)
     * @param outputColumnCount number of original output columns (before sort column appending)
     */
    static String buildTopKMergeCoordinatorSql(
        int[] sortColumnIndices,
        boolean[] sortAsc,
        int limit,
        int outputColumnCount
    ) {
        StringBuilder orderBy = new StringBuilder(" ORDER BY ");
        for (int i = 0; i < sortColumnIndices.length; i++) {
            if (i > 0) orderBy.append(", ");
            orderBy.append("\"col_").append(sortColumnIndices[i]).append("\"");
            orderBy.append(sortAsc[i] ? " ASC" : " DESC");
        }
        if (limit > 0) {
            orderBy.append(" LIMIT ").append(limit);
        }

        // Check if any sort column index falls outside the original output range
        boolean needsStripping = false;
        for (int idx : sortColumnIndices) {
            if (idx >= outputColumnCount) {
                needsStripping = true;
                break;
            }
        }

        if (needsStripping && outputColumnCount > 0) {
            StringBuilder sb = new StringBuilder("SELECT ");
            for (int i = 0; i < outputColumnCount; i++) {
                if (i > 0) sb.append(", ");
                sb.append("\"col_").append(i).append("\"");
            }
            sb.append(" FROM (SELECT * FROM __exchange_input__").append(orderBy).append(")");
            return sb.toString();
        }

        return "SELECT * FROM __exchange_input__" + orderBy;
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
     * @param strategy       the merge strategy (for logging)
     * @param listener       callback for the merged row-major result
     */
    void mergeViaDataFusion(
        List<WorkerQueryResponse> responses,
        String coordinatorSql,
        MergeStrategy strategy,
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
                strategy, nonEmpty.size(), ipc.length, coordinatorSql
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
                    "DataFusion " + strategy + " merge timed out after " + NATIVE_TIMEOUT_MINUTES + " minutes", e));
                return;
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                listener.onFailure(new RuntimeException(
                    "DataFusion " + strategy + " merge failed", cause != null ? cause : e));
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                listener.onFailure(new RuntimeException("DataFusion " + strategy + " merge interrupted", e));
                return;
            }

            // 3. Drain the result stream — swap TCCL for Arrow C-Data imports
            List<Object[]> rows = drainStreamToRows(streamPtr, runtimeHandle, dfService);
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

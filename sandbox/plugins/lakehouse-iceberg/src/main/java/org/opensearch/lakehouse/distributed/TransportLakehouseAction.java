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
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.analytics.exec.ExternalScanContext;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.lakehouse.LakehouseState;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.stream.StreamErrorCode;
import org.opensearch.transport.stream.StreamException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * Transport action handler for distributed Iceberg query execution on worker nodes.
 *
 * <p>Supports two execution paths:
 * <ul>
 *   <li><b>Arrow Flight streaming</b> (primary): When {@link StreamTransportService} is available
 *       (feature flag enabled), registers a streaming handler that sends results in batches
 *       via {@code channel.sendResponseBatch()} + {@code channel.completeStream()}.</li>
 *   <li><b>Standard transport</b> (fallback for client().execute()): The {@code doExecute()}
 *       path handles requests from {@code client().execute()} (used by integration tests
 *       and validation).</li>
 * </ul>
 *
 * <p>This SQL-based variant receives a SQL query string (not Substrait bytes) from the
 * coordinator. The worker executes the SQL directly via the DataFusion backend.
 */
public class TransportLakehouseAction extends HandledTransportAction<LakehouseWorkerRequest, LakehouseWorkerResponse> {

    private static final Logger logger = LogManager.getLogger(TransportLakehouseAction.class);

    /** Number of rows per batch when streaming results back to the coordinator. */
    private static final int STREAM_BATCH_SIZE = 1000;

    /**
     * Guice-injected constructor. Registers this handler with both the standard transport
     * service (for {@code client().execute()} validation) and the Arrow Flight streaming
     * transport (for distributed query execution).
     *
     * @param transportService       standard transport service for handler registration
     * @param actionFilters          action filters
     * @param clusterService         cluster service for discovering data nodes
     * @param streamTransportService Arrow Flight streaming transport (null if feature flag is off)
     */
    @Inject
    public TransportLakehouseAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        @Nullable StreamTransportService streamTransportService
    ) {
        super(LakehouseWorkerAction.NAME, transportService, actionFilters, LakehouseWorkerRequest::new);

        // Register streaming handler on Arrow Flight transport if available
        if (streamTransportService != null) {
            streamTransportService.registerRequestHandler(
                LakehouseWorkerAction.NAME,
                ThreadPool.Names.GENERIC,
                LakehouseWorkerRequest::new,
                this::handleStreamRequest
            );
            logger.info("[TransportLakehouseAction] Registered Arrow Flight streaming handler for distributed queries");
        } else {
            logger.info("[TransportLakehouseAction] Arrow Flight streaming not available (feature flag off)");
        }

        // Initialize the distributed query coordinator with both transport types
        DistributedQueryCoordinator coordinator = new DistributedQueryCoordinator(
            clusterService, transportService, streamTransportService
        );
        LakehouseState.instance().setDistributedCoordinator(coordinator);
        logger.info("[TransportLakehouseAction] Initialized distributed query coordinator (streaming={})",
            streamTransportService != null);

        // Initialize the multi-stage coordinator (Mini-Trino engine) with fallback
        MultiStageCoordinator multiStage = new MultiStageCoordinator(
            clusterService, transportService, coordinator
        );
        LakehouseState.instance().setMultiStageCoordinator(multiStage);
        logger.info("[TransportLakehouseAction] Initialized multi-stage coordinator (Mini-Trino engine)");
    }

    /**
     * Standard transport handler for {@code client().execute()} requests.
     * Used by integration tests and request validation.
     *
     * @param task     the task for this request
     * @param request  the worker request
     * @param listener the action listener for the response
     */
    @Override
    protected void doExecute(Task task, LakehouseWorkerRequest request, ActionListener<LakehouseWorkerResponse> listener) {
        try {
            LakehouseWorkerResponse response = executeWorkerQuery(request);
            listener.onResponse(response);
        } catch (Exception e) {
            logger.error("[TransportLakehouseAction] Worker execution failed", e);
            listener.onFailure(e);
        }
    }

    /**
     * Arrow Flight streaming handler for distributed query execution.
     * Streams results back to the coordinator in batches of {@link #STREAM_BATCH_SIZE} rows.
     *
     * @param request the worker request with file paths, SQL query, and storage config
     * @param channel the streaming transport channel for sending batched responses
     * @param task    the task associated with this request
     */
    private void handleStreamRequest(LakehouseWorkerRequest request, TransportChannel channel, Task task) throws IOException {
        long tWorker0 = System.nanoTime();
        try {
            String[] filePaths = request.getFilePaths();
            String sqlQuery = request.getSqlQuery();
            String tableName = request.getTableName();

            logger.info("[Worker] ====== WORKER EXECUTION START (streaming) ======");
            logger.info("[Worker] table={}, files={}, sql={}", tableName, filePaths.length, sqlQuery);
            for (int i = 0; i < Math.min(filePaths.length, 5); i++) {
                logger.info("[Worker]   file[{}]: {}", i, filePaths[i]);
            }
            if (filePaths.length > 5) {
                logger.info("[Worker]   ... and {} more files", filePaths.length - 5);
            }

            ExternalScanContext scanContext = new ExternalScanContext(
                tableName,
                Arrays.asList(filePaths),
                sqlQuery,
                request.getStorageConfig()
            );

            // Prefer IPC format — single response with all data
            Function<ExternalScanContext, byte[]> ipcExecutor = ExternalScanContext.getGlobalIpcExecutor();
            if (ipcExecutor != null) {
                logger.info("[Worker] Using IPC executor path");
                long tIpc0 = System.nanoTime();
                byte[] ipcBytes = ipcExecutor.apply(scanContext);
                long tIpc1 = System.nanoTime();
                if (ipcBytes != null && ipcBytes.length > 0) {
                    channel.sendResponseBatch(new LakehouseWorkerResponse(ipcBytes));
                    channel.completeStream();
                    long tWorker1 = System.nanoTime();
                    logger.info("[Worker] [TIMING] IPC execution: {} ms, transport: {} ms, total: {} ms — {} bytes",
                        (tIpc1 - tIpc0) / 1_000_000, (tWorker1 - tIpc1) / 1_000_000,
                        (tWorker1 - tWorker0) / 1_000_000, ipcBytes.length);
                    logger.info("[Worker] ====== WORKER EXECUTION END (IPC) ======");
                    return;
                }
            }

            // Fallback to legacy row-by-row streaming
            Function<ExternalScanContext, Iterable<Object[]>> executor = ExternalScanContext.getGlobalBackendExecutor();
            if (executor == null) {
                throw new IllegalStateException(
                    "Backend executor not initialized. The analytics backend must have processed at least one query "
                        + "before distributed worker execution is available."
                );
            }

            Iterable<Object[]> result = executor.apply(scanContext);

            List<Object[]> batch = new ArrayList<>(STREAM_BATCH_SIZE);
            String[] columnNames = null;
            int totalRows = 0;
            int batchCount = 0;

            for (Object[] row : result) {
                if (columnNames == null && row.length > 0) {
                    columnNames = new String[row.length];
                    for (int i = 0; i < row.length; i++) {
                        columnNames[i] = "col_" + i;
                    }
                }
                batch.add(row);

                if (batch.size() >= STREAM_BATCH_SIZE) {
                    channel.sendResponseBatch(new LakehouseWorkerResponse(
                        batch.toArray(new Object[0][]), columnNames
                    ));
                    totalRows += batch.size();
                    batchCount++;
                    batch.clear();
                }
            }

            if (!batch.isEmpty()) {
                if (columnNames == null) {
                    columnNames = new String[0];
                }
                channel.sendResponseBatch(new LakehouseWorkerResponse(
                    batch.toArray(new Object[0][]), columnNames
                ));
                totalRows += batch.size();
                batchCount++;
            }

            channel.completeStream();
            long tWorker1 = System.nanoTime();
            logger.info("[Worker] [TIMING] Row streaming total: {} ms — {} rows in {} batches",
                (tWorker1 - tWorker0) / 1_000_000, totalRows, batchCount);
            logger.info("[Worker] ====== WORKER EXECUTION END (rows) ======");

        } catch (StreamException e) {
            if (e.getErrorCode() == StreamErrorCode.CANCELLED) {
                logger.info("[TransportLakehouseAction] Client cancelled stream: {}", e.getMessage());
            } else {
                channel.sendResponse(e);
            }
        } catch (Exception e) {
            logger.error("[TransportLakehouseAction] Streaming worker execution failed", e);
            channel.sendResponse(e);
        }
    }

    /**
     * Executes the worker query, preferring IPC format for efficient transport.
     * Falls back to legacy Object[][] format if IPC executor is not available.
     */
    private LakehouseWorkerResponse executeWorkerQuery(LakehouseWorkerRequest request) {
        long tWorker0 = System.nanoTime();
        String[] filePaths = request.getFilePaths();
        String sqlQuery = request.getSqlQuery();
        String tableName = request.getTableName();

        logger.info("[Worker] ====== WORKER EXECUTION START (standard) ======");
        logger.info("[Worker] table={}, files={}, sql={}", tableName, filePaths.length, sqlQuery);
        for (int i = 0; i < Math.min(filePaths.length, 5); i++) {
            logger.info("[Worker]   file[{}]: {}", i, filePaths[i]);
        }
        if (filePaths.length > 5) {
            logger.info("[Worker]   ... and {} more files", filePaths.length - 5);
        }

        ExternalScanContext scanContext = new ExternalScanContext(
            tableName,
            Arrays.asList(filePaths),
            sqlQuery,
            request.getStorageConfig()
        );

        // Prefer IPC format for efficient transport
        Function<ExternalScanContext, byte[]> ipcExecutor = ExternalScanContext.getGlobalIpcExecutor();
        if (ipcExecutor != null) {
            logger.info("[Worker] Using IPC executor path");
            long tIpc0 = System.nanoTime();
            byte[] ipcBytes = ipcExecutor.apply(scanContext);
            long tIpc1 = System.nanoTime();
            if (ipcBytes != null && ipcBytes.length > 0) {
                logger.info("[Worker] [TIMING] IPC execution: {} ms, total: {} ms — {} bytes",
                    (tIpc1 - tIpc0) / 1_000_000, (tIpc1 - tWorker0) / 1_000_000, ipcBytes.length);
                logger.info("[Worker] ====== WORKER EXECUTION END (IPC) ======");
                return new LakehouseWorkerResponse(ipcBytes);
            }
        }

        // Fallback to legacy Object[][] format
        logger.info("[Worker] Falling back to legacy Object[][] path");
        Function<ExternalScanContext, Iterable<Object[]>> executor = ExternalScanContext.getGlobalBackendExecutor();
        if (executor == null) {
            throw new IllegalStateException(
                "Backend executor not initialized. The analytics backend must have processed at least one query "
                    + "before distributed worker execution is available."
            );
        }

        long tExec0 = System.nanoTime();
        Iterable<Object[]> result = executor.apply(scanContext);

        List<Object[]> rowList = new ArrayList<>();
        String[] columnNames = null;
        for (Object[] row : result) {
            if (columnNames == null && row.length > 0) {
                columnNames = new String[row.length];
                for (int i = 0; i < row.length; i++) {
                    columnNames[i] = "col_" + i;
                }
            }
            rowList.add(row);
        }

        if (columnNames == null) {
            columnNames = new String[0];
        }

        Object[][] rows = rowList.toArray(new Object[0][]);
        long tWorker1 = System.nanoTime();
        logger.info("[Worker] [TIMING] Row execution: {} ms, total: {} ms — {} rows, {} columns",
            (tWorker1 - tExec0) / 1_000_000, (tWorker1 - tWorker0) / 1_000_000, rows.length, columnNames.length);
        logger.info("[Worker] ====== WORKER EXECUTION END (rows) ======");
        return new LakehouseWorkerResponse(rows, columnNames);
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed.worker;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.exec.DataWarehouseScanContext;
import org.opensearch.analytics.exec.DataWarehouseQueryEngine;
import org.opensearch.cluster.service.ClusterService;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Executes a distributed query fragment on a worker node.
 * <p>
 * Responsible for:
 * <ul>
 *   <li>Resolving the DataFusion queryEngine</li>
 *   <li>Delegating credential resolution to {@link WorkerCredentialResolver}</li>
 *   <li>Delegating to DataFusion and building the response</li>
 * </ul>
 * <p>
 * Extracted from {@link WorkerQueryTransportAction} for single-responsibility.
 * Shared by both the transport action (remote dispatch) and direct local execution.
 *
 * @opensearch.internal
 */
public final class WorkerQueryExecutor {

    private static final Logger logger = LogManager.getLogger(WorkerQueryExecutor.class);

    private WorkerQueryExecutor() {}

    /**
     * Executes a worker query request and returns the response.
     * Can be called from transport action (remote) or directly (local).
     *
     * @param request        the worker query request
     * @param clusterService the cluster service for credential resolution
     * @param queryEngine    the data warehouse query engine for executing queries
     * @return the worker query response
     */
    @SuppressWarnings("removal")
    public static WorkerQueryResponse execute(WorkerQueryRequest request, ClusterService clusterService, DataWarehouseQueryEngine queryEngine) {
        if (queryEngine == null) {
            throw new IllegalStateException("No DataWarehouseQueryEngine registered for worker query execution");
        }

        Map<String, String> storageConfig = WorkerCredentialResolver.resolve(request.getStorageConfig(), clusterService);

        DataWarehouseScanContext scanContext = new DataWarehouseScanContext(
            request.getTableName(),
            request.getFilePaths(),
            request.getFileSizes(),
            request.getSqlQuery(),
            storageConfig
        );

        logger.info(
            "[WorkerQuery] Executing: table={}, files={}, sql={}",
            request.getTableName(),
            request.getFilePaths().size(),
            request.getSqlQuery()
        );

        long t0 = System.currentTimeMillis();
        Iterable<Object[]> rows = AccessController.doPrivileged(
            (PrivilegedAction<Iterable<Object[]>>) () -> queryEngine.executeQuery(scanContext)
        );
        long t1 = System.currentTimeMillis();

        WorkerQueryResponse response = buildResponse(rows);
        logger.info("[PERF] Worker query: {}ms ({} rows)", t1 - t0, response.getRowCount());
        return response;
    }

    /**
     * Converts the row-oriented result from the queryEngine into a column-oriented response.
     *
     * @param rows iterable of row arrays from executeRemoteQuery
     * @return column-oriented WorkerQueryResponse
     */
    static WorkerQueryResponse buildResponse(Iterable<Object[]> rows) {
        List<Object[]> rowList = new ArrayList<>();
        for (Object[] row : rows) {
            rowList.add(sanitizeRow(row));
        }

        if (rowList.isEmpty()) {
            return new WorkerQueryResponse(List.of(), List.of(), 0, new Object[0][]);
        }

        int numCols = rowList.get(0).length;
        int numRows = rowList.size();

        List<String> columnNames = new ArrayList<>(numCols);
        List<String> columnTypes = new ArrayList<>(numCols);
        Object[][] columnData = new Object[numCols][numRows];

        for (int col = 0; col < numCols; col++) {
            columnNames.add("col_" + col);
            for (int row = 0; row < numRows; row++) {
                columnData[col][row] = rowList.get(row)[col];
            }
            // Infer type from first non-null value
            String typeName = "UNKNOWN";
            for (int row = 0; row < numRows; row++) {
                if (columnData[col][row] != null) {
                    typeName = columnData[col][row].getClass().getSimpleName();
                    break;
                }
            }
            columnTypes.add(typeName);
        }

        return new WorkerQueryResponse(columnNames, columnTypes, numRows, columnData);
    }

    /**
     * Converts non-serializable types to types supported by StreamOutput.writeGenericValue().
     * LocalDateTime and LocalDate from DataFusion are converted to their ISO-8601 string form.
     * Returns a defensive copy to avoid corrupting upstream iterator state.
     */
    static Object[] sanitizeRow(Object[] row) {
        Object[] copy = row.clone();
        for (int i = 0; i < copy.length; i++) {
            if (copy[i] instanceof LocalDateTime) {
                copy[i] = copy[i].toString();
            } else if (copy[i] instanceof LocalDate) {
                copy[i] = copy[i].toString();
            }
        }
        return copy;
    }
}

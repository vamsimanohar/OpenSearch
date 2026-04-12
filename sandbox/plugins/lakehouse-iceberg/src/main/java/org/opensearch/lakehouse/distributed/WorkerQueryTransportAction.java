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
import org.opensearch.analytics.exec.RemoteQueryBackendHolder;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.lakehouse.LakehouseState;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Transport action that executes a distributed query fragment on a worker node.
 * <p>
 * Receives a {@link WorkerQueryRequest} containing SQL, file paths, and storage config,
 * builds an {@link ExternalScanContext}, and delegates to the DataFusion backend
 * via {@link AnalyticsSearchBackendPlugin#executeRemoteQuery(ExternalScanContext)}.
 * <p>
 * The backend provider is discovered via {@link RemoteQueryBackendHolder}, which is
 * set by the analytics-engine plugin during initialization.
 *
 * @opensearch.internal
 */
public class WorkerQueryTransportAction extends HandledTransportAction<WorkerQueryRequest, WorkerQueryResponse> {

    private static final Logger logger = LogManager.getLogger(WorkerQueryTransportAction.class);

    private static volatile AnalyticsSearchBackendPlugin backendProvider;

    /**
     * Sets the backend provider used to execute queries on worker nodes.
     * Visible for testing.
     *
     * @param provider the analytics search backend plugin (e.g., DataFusion)
     */
    public static void setBackendProvider(AnalyticsSearchBackendPlugin provider) {
        backendProvider = provider;
    }

    /** Returns the current backend provider, or null if not set. Visible for testing. */
    static AnalyticsSearchBackendPlugin getBackendProvider() {
        return backendProvider;
    }

    /**
     * Creates the transport action via Guice injection.
     * Initializes the distributed scan executor on {@link LakehouseState}
     * since this is the earliest point where TransportService and ClusterService
     * are available via Guice.
     *
     * @param transportService the transport service
     * @param actionFilters    the action filters
     * @param clusterService   the cluster service
     */
    @Inject
    public WorkerQueryTransportAction(TransportService transportService, ActionFilters actionFilters, ClusterService clusterService) {
        super(WorkerQueryAction.NAME, transportService, actionFilters, WorkerQueryRequest::new);
        LakehouseState.instance().initDistributedExecutor(transportService, clusterService);
    }

    @Override
    @SuppressWarnings("removal")
    protected void doExecute(Task task, WorkerQueryRequest request, ActionListener<WorkerQueryResponse> listener) {
        try {
            AnalyticsSearchBackendPlugin provider = resolveBackend();

            ExternalScanContext scanContext = new ExternalScanContext(
                request.getTableName(),
                request.getFilePaths(),
                request.getFileSizes(),
                request.getSqlQuery(),
                request.getStorageConfig()
            );

            logger.info(
                "[WorkerQuery] Executing on worker: table={}, files={}, sql={}",
                request.getTableName(),
                request.getFilePaths().size(),
                request.getSqlQuery()
            );

            long t0 = System.currentTimeMillis();
            Iterable<Object[]> rows = AccessController.doPrivileged(
                (PrivilegedAction<Iterable<Object[]>>) () -> provider.executeRemoteQuery(scanContext)
            );
            long t1 = System.currentTimeMillis();

            WorkerQueryResponse response = buildResponse(rows);
            logger.info("[PERF] Worker query: {}ms ({} rows)", t1 - t0, response.getRowCount());
            listener.onResponse(response);
        } catch (Exception e) {
            logger.error("[WorkerQuery] Execution failed", e);
            listener.onFailure(e);
        }
    }

    /**
     * Resolves the backend provider from static field or RemoteQueryBackendHolder.
     */
    private static AnalyticsSearchBackendPlugin resolveBackend() {
        AnalyticsSearchBackendPlugin provider = backendProvider;
        if (provider == null) {
            provider = RemoteQueryBackendHolder.getProvider();
            if (provider != null) {
                backendProvider = provider;
            }
        }
        if (provider == null) {
            throw new IllegalStateException("No analytics backend registered for worker query execution");
        }
        return provider;
    }

    /**
     * Converts the row-oriented result from the backend into a column-oriented response.
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
     */
    static Object[] sanitizeRow(Object[] row) {
        for (int i = 0; i < row.length; i++) {
            if (row[i] instanceof LocalDateTime) {
                row[i] = row[i].toString();
            } else if (row[i] instanceof LocalDate) {
                row[i] = row[i].toString();
            }
        }
        return row;
    }
}

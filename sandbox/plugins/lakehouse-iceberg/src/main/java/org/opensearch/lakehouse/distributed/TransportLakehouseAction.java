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
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.lakehouse.LakehouseState;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;


/**
 * Transport action handler for distributed Iceberg query execution on worker nodes.
 *
 * <p>The coordinator splits an Iceberg file scan across cluster nodes and sends each
 * worker a {@link LakehouseWorkerRequest} containing the assigned file paths, serialized
 * Substrait plan, storage configuration, and table name. This handler:
 * <ol>
 *   <li>Reconstructs an {@link ExternalScanContext} from the request</li>
 *   <li>Delegates to the DataFusion backend via the executor stored in {@link LakehouseState}</li>
 *   <li>Converts the result rows into a {@link LakehouseWorkerResponse}</li>
 * </ol>
 */
public class TransportLakehouseAction extends HandledTransportAction<LakehouseWorkerRequest, LakehouseWorkerResponse> {

    private static final Logger logger = LogManager.getLogger(TransportLakehouseAction.class);

    /**
     * Guice-injected constructor. Registers this handler with the transport service
     * under the {@link LakehouseWorkerAction#NAME} action name.
     *
     * <p>Also initializes the {@link DistributedQueryCoordinator} and registers it
     * in {@link LakehouseState}, since this is the earliest point where both
     * {@code TransportService} and {@code ClusterService} are available via Guice.
     *
     * @param transportService transport service for handler registration and distributed dispatch
     * @param actionFilters    action filters
     * @param clusterService   cluster service for discovering data nodes
     */
    @Inject
    public TransportLakehouseAction(TransportService transportService, ActionFilters actionFilters, ClusterService clusterService) {
        super(LakehouseWorkerAction.NAME, transportService, actionFilters, LakehouseWorkerRequest::new);

        // Initialize the distributed query coordinator now that both services are available
        DistributedQueryCoordinator coordinator = new DistributedQueryCoordinator(clusterService, transportService);
        LakehouseState.instance().setDistributedCoordinator(coordinator);
        logger.info("[TransportLakehouseAction] Initialized distributed query coordinator");
    }

    @Override
    protected void doExecute(Task task, LakehouseWorkerRequest request, ActionListener<LakehouseWorkerResponse> listener) {
        try {
            String[] filePaths = request.getFilePaths();
            byte[] substraitPlan = request.getSubstraitPlan();
            String tableName = request.getTableName();

            logger.info("[TransportLakehouseAction] Worker received request: table={}, files={}, plan={} bytes",
                tableName, filePaths.length, substraitPlan != null ? substraitPlan.length : 0);

            // Build the ExternalScanContext from the request data
            ExternalScanContext scanContext = new ExternalScanContext(
                tableName,
                Arrays.asList(filePaths),
                substraitPlan,
                request.getStorageConfig()
            );

            // Retrieve the backend executor from the global registry (set by DefaultPlanExecutor)
            Function<ExternalScanContext, Iterable<Object[]>> executor = ExternalScanContext.getGlobalBackendExecutor();
            if (executor == null) {
                throw new IllegalStateException(
                    "Backend executor not initialized. The analytics backend must have processed at least one query "
                    + "before distributed worker execution is available."
                );
            }

            // Execute the query via the backend (DataFusion native engine)
            Iterable<Object[]> result = executor.apply(scanContext);

            // Convert Iterable<Object[]> to arrays for the response
            List<Object[]> rowList = new ArrayList<>();
            String[] columnNames = null;
            for (Object[] row : result) {
                if (columnNames == null && row.length > 0) {
                    // Column names are not available from Iterable<Object[]> alone.
                    // They will be inferred from the first batch by the coordinator.
                    // For now, generate positional column names.
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
            logger.info("[TransportLakehouseAction] Worker completed: {} rows, {} columns", rows.length, columnNames.length);

            listener.onResponse(new LakehouseWorkerResponse(rows, columnNames));
        } catch (Exception e) {
            logger.error("[TransportLakehouseAction] Worker execution failed", e);
            listener.onFailure(e);
        }
    }
}

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
import org.opensearch.analytics.exec.DataWarehouseQueryEngine;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.lakehouse.LakehousePlugin;
import org.opensearch.transport.TransportService;

/**
 * Transport action that executes a distributed query fragment on a worker node.
 * <p>
 * This is a thin transport action that delegates all execution logic to
 * {@link WorkerQueryExecutor}. The executor handles credential resolution,
 * DataFusion invocation, and response building.
 * <p>
 * Uses a dedicated {@code lakehouse_worker} thread pool to avoid blocking GENERIC threads
 * with DataFusion JNI calls. This is critical: long-running queries on GENERIC threads
 * would block cluster health checks and cause node disconnections.
 *
 * @opensearch.internal
 */
public class WorkerQueryTransportAction extends HandledTransportAction<WorkerQueryRequest, WorkerQueryResponse> {

    private static final Logger logger = LogManager.getLogger(WorkerQueryTransportAction.class);

    private final ClusterService clusterService;
    private final DataWarehouseQueryEngine queryEngine;

    @Inject
    public WorkerQueryTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        DataWarehouseQueryEngine queryEngine
    ) {
        super(WorkerQueryAction.NAME, transportService, actionFilters, WorkerQueryRequest::new, LakehousePlugin.LAKEHOUSE_WORKER_THREAD_POOL);
        this.clusterService = clusterService;
        this.queryEngine = queryEngine;
    }

    @Override
    protected void doExecute(Task task, WorkerQueryRequest request, ActionListener<WorkerQueryResponse> listener) {
        try {
            WorkerQueryResponse response = WorkerQueryExecutor.execute(request, clusterService, queryEngine);
            long beforeSend = System.currentTimeMillis();
            logger.info("[WorkerQuery] Calling listener.onResponse: {} rows at t={}", response.getRowCount(), beforeSend);
            listener.onResponse(response);
            long afterSend = System.currentTimeMillis();
            logger.info("[WorkerQuery] listener.onResponse returned: took {}ms", afterSend - beforeSend);
        } catch (Exception e) {
            logger.error("[WorkerQuery] Execution failed", e);
            listener.onFailure(e);
        }
    }
}

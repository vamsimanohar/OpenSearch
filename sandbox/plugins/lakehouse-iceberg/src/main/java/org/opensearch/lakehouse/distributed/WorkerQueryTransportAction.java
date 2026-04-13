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
import org.opensearch.analytics.exec.ExternalQueryBackend;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.lakehouse.LakehouseState;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Transport action that executes a distributed query fragment on a worker node.
 * <p>
 * This is a thin transport action that delegates all execution logic to
 * {@link WorkerQueryExecutor}. The executor handles credential resolution,
 * DataFusion invocation, and response building.
 * <p>
 * Uses {@link ThreadPool.Names#GENERIC} executor to avoid blocking Netty I/O threads
 * with DataFusion JNI calls. This is critical: without it, long-running queries
 * block all transport, causing cluster-wide timeouts.
 *
 * @opensearch.internal
 */
public class WorkerQueryTransportAction extends HandledTransportAction<WorkerQueryRequest, WorkerQueryResponse> {

    private static final Logger logger = LogManager.getLogger(WorkerQueryTransportAction.class);

    private final ClusterService clusterService;
    private final ExternalQueryBackend queryBackend;

    /**
     * Creates the transport action via Guice injection.
     * Initializes the distributed scan executor on {@link LakehouseState}
     * since this is the earliest point where TransportService and ClusterService
     * are available via Guice.
     *
     * @param transportService the transport service
     * @param actionFilters    the action filters
     * @param clusterService   the cluster service
     * @param queryBackend     the external query backend for executing queries
     */
    @Inject
    public WorkerQueryTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        ExternalQueryBackend queryBackend
    ) {
        super(WorkerQueryAction.NAME, transportService, actionFilters, WorkerQueryRequest::new, ThreadPool.Names.GENERIC);
        this.clusterService = clusterService;
        this.queryBackend = queryBackend;
        LakehouseState.instance().initDistributedExecutor(transportService, clusterService, queryBackend);
    }

    @Override
    protected void doExecute(Task task, WorkerQueryRequest request, ActionListener<WorkerQueryResponse> listener) {
        try {
            WorkerQueryResponse response = WorkerQueryExecutor.execute(request, clusterService, queryBackend);
            listener.onResponse(response);
        } catch (Exception e) {
            logger.error("[WorkerQuery] Execution failed", e);
            listener.onFailure(e);
        }
    }
}

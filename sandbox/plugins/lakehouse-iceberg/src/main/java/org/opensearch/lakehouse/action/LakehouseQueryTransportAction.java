/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.analytics.EngineContext;
import org.opensearch.analytics.exec.DataWarehouseQueryEngine;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.lakehouse.distributed.DistributedScanExecutor;
import org.opensearch.lakehouse.exec.LakehouseQueryExecutor;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action for lakehouse SQL and PPL query execution.
 * <p>
 * Fully asynchronous: delegates to {@link LakehouseQueryExecutor} which delivers
 * results via {@link ActionListener} callbacks. No thread blocks waiting for results,
 * similar to how OpenSearch search dispatches shard queries.
 * <p>
 * The Netty event loop thread that calls {@code doExecute()} returns immediately after
 * kicking off the async pipeline. Actual DataFusion execution happens on the
 * {@code lakehouse_worker} thread pool via {@link DistributedScanExecutor}.
 *
 * @opensearch.internal
 */
public class LakehouseQueryTransportAction extends HandledTransportAction<LakehouseQueryRequest, PPLResponse> {

    private static final Logger logger = LogManager.getLogger(LakehouseQueryTransportAction.class);
    private final LakehouseQueryExecutor queryExecutor;

    @Inject
    public LakehouseQueryTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        EngineContext engineContext,
        ClusterService clusterService,
        DataWarehouseQueryEngine queryEngine
    ) {
        super(LakehouseQueryAction.NAME, transportService, actionFilters, LakehouseQueryRequest::new);
        DistributedScanExecutor scanExecutor = new DistributedScanExecutor(transportService, clusterService, queryEngine);
        this.queryExecutor = new LakehouseQueryExecutor(engineContext, scanExecutor);
    }

    @Override
    protected void doExecute(Task task, LakehouseQueryRequest request, ActionListener<PPLResponse> listener) {
        if (request.isSql()) {
            logger.info("[Lakehouse] Executing SQL: {}", request.getQueryText());
            queryExecutor.executeSql(request.getQueryText(), listener);
        } else {
            logger.info("[Lakehouse] Executing PPL: {}", request.getQueryText());
            queryExecutor.executePpl(request.getQueryText(), listener);
        }
    }
}

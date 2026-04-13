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
import org.opensearch.analytics.exec.ExternalQueryBackend;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.lakehouse.exec.LakehouseQueryExecutor;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Transport action for lakehouse SQL and PPL query execution.
 * Uses {@link LakehouseQueryExecutor} directly, bypassing the analytics-engine
 * PushDownPlanner/DefaultPlanExecutor pipeline.
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
        ExternalQueryBackend queryBackend
    ) {
        super(LakehouseQueryAction.NAME, transportService, actionFilters, LakehouseQueryRequest::new, ThreadPool.Names.GENERIC);
        this.queryExecutor = new LakehouseQueryExecutor(engineContext, queryBackend);
    }

    @Override
    protected void doExecute(Task task, LakehouseQueryRequest request, ActionListener<PPLResponse> listener) {
        try {
            PPLResponse response;
            if (request.isSql()) {
                logger.info("[Lakehouse] Executing SQL: {}", request.getQueryText());
                response = queryExecutor.executeSql(request.getQueryText());
            } else {
                logger.info("[Lakehouse] Executing PPL: {}", request.getQueryText());
                response = queryExecutor.executePpl(request.getQueryText());
            }
            listener.onResponse(response);
        } catch (Exception e) {
            logger.error("[Lakehouse] Query execution failed", e);
            listener.onFailure(e);
        }
    }
}

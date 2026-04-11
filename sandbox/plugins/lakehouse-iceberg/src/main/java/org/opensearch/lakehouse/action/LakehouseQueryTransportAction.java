/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.action;

import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.analytics.EngineContext;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.ppl.action.UnifiedQueryService;
import org.opensearch.ppl.planner.PushDownPlanner;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action for lakehouse SQL and PPL query execution.
 * Receives {@link EngineContext} and {@link QueryPlanExecutor} via Guice injection
 * from the analytics-engine plugin.
 *
 * @opensearch.internal
 */
public class LakehouseQueryTransportAction extends HandledTransportAction<LakehouseQueryRequest, PPLResponse> {

    private static final Logger logger = LogManager.getLogger(LakehouseQueryTransportAction.class);

    private final UnifiedQueryService queryService;

    /**
     * Creates the transport action via Guice injection.
     *
     * @param transportService the transport service
     * @param actionFilters the action filters
     * @param engineContext the engine context from analytics-engine
     * @param executor the query plan executor from analytics-engine
     */
    @Inject
    public LakehouseQueryTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        EngineContext engineContext,
        QueryPlanExecutor<RelNode, Iterable<Object[]>> executor
    ) {
        super(LakehouseQueryAction.NAME, transportService, actionFilters, LakehouseQueryRequest::new);
        PushDownPlanner pushDownPlanner = new PushDownPlanner(engineContext.operatorTable(), executor);
        this.queryService = new UnifiedQueryService(pushDownPlanner, engineContext);
    }

    @Override
    protected void doExecute(Task task, LakehouseQueryRequest request, ActionListener<PPLResponse> listener) {
        try {
            PPLResponse response;
            if (request.isSql()) {
                logger.info("[Lakehouse] Executing SQL: {}", request.getQueryText());
                response = queryService.executeSql(request.getQueryText());
            } else {
                logger.info("[Lakehouse] Executing PPL: {}", request.getQueryText());
                response = queryService.execute(request.getQueryText());
            }
            listener.onResponse(response);
        } catch (Exception e) {
            logger.error("[Lakehouse] Query execution failed", e);
            listener.onFailure(e);
        }
    }
}

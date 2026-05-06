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
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action for lakehouse SQL and PPL query execution.
 * <p>
 * Delegates to the lakehouse query executor for async query execution.
 *
 * @opensearch.internal
 */
public class LakehouseQueryTransportAction extends HandledTransportAction<LakehouseQueryRequest, PPLResponse> {

    private static final Logger logger = LogManager.getLogger(LakehouseQueryTransportAction.class);

    @Inject
    public LakehouseQueryTransportAction(
        TransportService transportService,
        ActionFilters actionFilters
    ) {
        super(LakehouseQueryAction.NAME, transportService, actionFilters, LakehouseQueryRequest::new);
    }

    @Override
    protected void doExecute(Task task, LakehouseQueryRequest request, ActionListener<PPLResponse> listener) {
        if (request.isSql()) {
            logger.info("[Lakehouse] Executing SQL: {}", request.getQueryText());
        } else {
            logger.info("[Lakehouse] Executing PPL: {}", request.getQueryText());
        }
        listener.onFailure(new UnsupportedOperationException("Query executor not yet wired"));
    }
}

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
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * REST handler for {@code POST _lakehouse/ppl}.
 *
 * @opensearch.internal
 */
public class LakehousePplRestAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(LakehousePplRestAction.class);

    /** Creates the PPL REST handler. */
    public LakehousePplRestAction() {}

    @Override
    public String getName() {
        return "lakehouse_ppl_query";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "_lakehouse/ppl"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String query = LakehouseSqlRestAction.parseQuery(request);
        return channel -> {
            logger.info("[LakehousePpl] Executing PPL: {}", query);
            client.execute(
                LakehouseQueryAction.INSTANCE,
                new LakehouseQueryRequest(query, false),
                new ActionListener<PPLResponse>() {
                    @Override
                    public void onResponse(PPLResponse response) {
                        try {
                            XContentBuilder builder = LakehouseSqlRestAction.buildResponseJson(channel, query, response);
                            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                        } catch (Exception e) {
                            sendError(e);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error("[LakehousePpl] PPL execution failed", e);
                        sendError(e);
                    }

                    private void sendError(Exception e) {
                        try {
                            channel.sendResponse(new BytesRestResponse(channel, RestStatus.BAD_REQUEST, e));
                        } catch (IOException ioe) {
                            logger.error("[LakehousePpl] Failed to send error response", ioe);
                        }
                    }
                }
            );
        };
    }
}

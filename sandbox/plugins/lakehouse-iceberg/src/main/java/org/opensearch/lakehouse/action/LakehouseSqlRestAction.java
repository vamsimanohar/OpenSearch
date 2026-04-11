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
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * REST handler for {@code POST _lakehouse/sql}.
 *
 * @opensearch.internal
 */
public class LakehouseSqlRestAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(LakehouseSqlRestAction.class);

    /** Creates the SQL REST handler. */
    public LakehouseSqlRestAction() {}

    @Override
    public String getName() {
        return "lakehouse_sql_query";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "_lakehouse/sql"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String query = parseQuery(request);
        return channel -> {
            logger.info("[LakehouseSql] Executing SQL: {}", query);
            client.execute(
                LakehouseQueryAction.INSTANCE,
                new LakehouseQueryRequest(query, true),
                new ActionListener<PPLResponse>() {
                    @Override
                    public void onResponse(PPLResponse response) {
                        try {
                            channel.sendResponse(new BytesRestResponse(RestStatus.OK, buildResponseJson(channel, query, response)));
                        } catch (Exception e) {
                            sendError(e);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.error("[LakehouseSql] SQL execution failed", e);
                        sendError(e);
                    }

                    private void sendError(Exception e) {
                        try {
                            channel.sendResponse(new BytesRestResponse(channel, RestStatus.BAD_REQUEST, e));
                        } catch (IOException ioe) {
                            logger.error("[LakehouseSql] Failed to send error response", ioe);
                        }
                    }
                }
            );
        };
    }

    /**
     * Parses the query text from the request body.
     *
     * @param request the REST request
     * @return the query text
     * @throws IOException if parsing fails
     */
    static String parseQuery(RestRequest request) throws IOException {
        String query = null;
        try (XContentParser parser = request.contentParser()) {
            XContentParser.Token token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("Expected JSON object");
            }
            String fieldName = null;
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                    fieldName = parser.currentName();
                } else if ("query".equals(fieldName)) {
                    query = parser.text();
                }
            }
        }
        if (query == null || query.isBlank()) {
            throw new IllegalArgumentException("Missing 'query' field in request body");
        }
        return query;
    }

    /**
     * Builds the JSON response from a PPLResponse.
     *
     * @param channel the REST channel for creating the builder
     * @param query the original query text
     * @param response the query response
     * @return the XContentBuilder with the response JSON
     * @throws IOException if building fails
     */
    static XContentBuilder buildResponseJson(
        org.opensearch.rest.RestChannel channel,
        String query,
        PPLResponse response
    ) throws IOException {
        XContentBuilder builder = channel.newBuilder();
        builder.startObject();
        builder.field("query", query);

        builder.startArray("schema");
        for (String col : response.getColumns()) {
            builder.startObject();
            builder.field("name", col);
            builder.endObject();
        }
        builder.endArray();

        builder.startArray("rows");
        int rowCount = 0;
        for (Object[] row : response.getRows()) {
            builder.startArray();
            for (Object val : row) {
                builder.value(val);
            }
            builder.endArray();
            rowCount++;
        }
        builder.endArray();

        builder.field("total", rowCount);
        builder.endObject();
        return builder;
    }
}

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
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.lakehouse.cluster.LakehouseMetadata;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.opensearch.rest.RestRequest.Method.PUT;

/**
 * REST handler for {@code PUT _lakehouse/table/{name}}.
 * Validates that the referenced catalog exists, then writes the table binding to cluster state.
 */
public class RegisterTableAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(RegisterTableAction.class);
    private static final String SOURCE = "register-lakehouse-table";
    private static final Pattern VALID_NAME = Pattern.compile("[a-zA-Z0-9_-]+");

    private final ClusterService clusterService;

    /**
     * Creates a table registration handler.
     *
     * @param clusterService the cluster service for state updates
     */
    public RegisterTableAction(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public String getName() {
        return "lakehouse_register_table";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "_lakehouse/table/{name}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String tableName = request.param("name");
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("Table name is required");
        }
        if (!VALID_NAME.matcher(tableName).matches()) {
            throw new IllegalArgumentException(
                "Table name must match [a-zA-Z0-9_-]+, got: " + tableName
            );
        }

        Map<String, String> binding = new HashMap<>();
        try (XContentParser parser = request.contentParser()) {
            XContentParser.Token token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("Expected a JSON object as request body");
            }
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                String fieldName = parser.currentName();
                parser.nextToken();
                binding.put(fieldName, parser.text());
            }
        }

        String catalogName = binding.get("catalog");
        if (catalogName == null || catalogName.isBlank()) {
            throw new IllegalArgumentException("Table binding must include a 'catalog' field");
        }

        return channel -> clusterService.submitStateUpdateTask(SOURCE, new ClusterStateUpdateTask(Priority.NORMAL) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                LakehouseMetadata existing = currentState.metadata().custom(LakehouseMetadata.TYPE);
                if (existing == null) {
                    existing = LakehouseMetadata.EMPTY;
                }

                // Validate that the referenced catalog exists
                if (!existing.catalogs().containsKey(catalogName)) {
                    throw new IllegalArgumentException(
                        "Catalog [" + catalogName + "] does not exist. Register the catalog first."
                    );
                }

                Map<String, Map<String, String>> newTables = new HashMap<>(existing.tables());
                newTables.put(tableName, binding);

                LakehouseMetadata updated = new LakehouseMetadata(existing.catalogs(), newTables);

                Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata()).putCustom(LakehouseMetadata.TYPE, updated);
                return ClusterState.builder(currentState).metadata(mdBuilder).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.error("Failed to register table [{}]: {}", tableName, e.getMessage());
                RestStatus status = RestStatus.INTERNAL_SERVER_ERROR;
                if (e instanceof IllegalArgumentException) {
                    status = RestStatus.BAD_REQUEST;
                }
                try {
                    channel.sendResponse(new BytesRestResponse(channel, status, e));
                } catch (Exception inner) {
                    logger.error("Failed to send error response", inner);
                }
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                try {
                    channel.sendResponse(
                        new BytesRestResponse(RestStatus.OK, channel.newBuilder().startObject().field("acknowledged", true)
                            .field("table", tableName).endObject())
                    );
                } catch (Exception e) {
                    logger.error("Failed to send response for table registration [{}]", tableName, e);
                }
            }
        });
    }
}

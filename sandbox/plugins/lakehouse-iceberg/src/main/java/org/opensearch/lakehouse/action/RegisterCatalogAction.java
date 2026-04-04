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

import static org.opensearch.rest.RestRequest.Method.PUT;

/**
 * REST handler for {@code PUT _lakehouse/catalog/{name}}.
 * Parses the request body, validates, and writes catalog configuration to cluster state.
 */
public class RegisterCatalogAction extends BaseRestHandler {

    private static final Logger logger = LogManager.getLogger(RegisterCatalogAction.class);
    private static final String SOURCE = "register-lakehouse-catalog";

    private final ClusterService clusterService;

    public RegisterCatalogAction(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public String getName() {
        return "lakehouse_register_catalog";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(PUT, "_lakehouse/catalog/{name}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String catalogName = request.param("name");
        if (catalogName == null || catalogName.isBlank()) {
            throw new IllegalArgumentException("Catalog name is required");
        }

        Map<String, String> config = new HashMap<>();
        try (XContentParser parser = request.contentParser()) {
            XContentParser.Token token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("Expected a JSON object as request body");
            }
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                String fieldName = parser.currentName();
                parser.nextToken();
                config.put(fieldName, parser.text());
            }
        }

        if (config.isEmpty()) {
            throw new IllegalArgumentException("Catalog configuration must not be empty");
        }

        return channel -> clusterService.submitStateUpdateTask(SOURCE, new ClusterStateUpdateTask(Priority.NORMAL) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                LakehouseMetadata existing = currentState.metadata().custom(LakehouseMetadata.TYPE);
                if (existing == null) {
                    existing = LakehouseMetadata.EMPTY;
                }

                Map<String, Map<String, String>> newCatalogs = new HashMap<>(existing.catalogs());
                newCatalogs.put(catalogName, config);

                LakehouseMetadata updated = new LakehouseMetadata(newCatalogs, existing.tables());

                Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata()).putCustom(LakehouseMetadata.TYPE, updated);
                return ClusterState.builder(currentState).metadata(mdBuilder).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.error("Failed to register catalog [{}]: {}", catalogName, e.getMessage());
                try {
                    channel.sendResponse(new BytesRestResponse(channel, RestStatus.INTERNAL_SERVER_ERROR, e));
                } catch (Exception inner) {
                    logger.error("Failed to send error response", inner);
                }
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                try {
                    channel.sendResponse(
                        new BytesRestResponse(RestStatus.OK, channel.newBuilder().startObject().field("acknowledged", true)
                            .field("catalog", catalogName).endObject())
                    );
                } catch (Exception e) {
                    logger.error("Failed to send response for catalog registration [{}]", catalogName, e);
                }
            }
        });
    }
}

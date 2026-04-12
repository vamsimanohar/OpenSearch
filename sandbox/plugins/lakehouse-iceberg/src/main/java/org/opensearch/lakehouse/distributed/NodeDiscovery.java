/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Discovers eligible worker nodes in the cluster for distributed query execution.
 * <p>
 * A node is eligible if it has the node attribute {@code lakehouse.worker=true},
 * which is registered by {@link org.opensearch.lakehouse.LakehousePlugin}.
 * If no nodes have the attribute (e.g., single-node mode), the local node is returned.
 *
 * @opensearch.internal
 */
public class NodeDiscovery {

    /** Node attribute key indicating this node can execute lakehouse queries. */
    public static final String LAKEHOUSE_WORKER_ATTR = "lakehouse.worker";

    private final ClusterService clusterService;

    /**
     * Creates a new NodeDiscovery instance.
     *
     * @param clusterService the cluster service for accessing cluster state
     */
    public NodeDiscovery(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    /**
     * Returns all nodes in the cluster that have the lakehouse worker attribute.
     * If no nodes have the attribute, returns the local node as a fallback.
     *
     * @return list of eligible worker nodes, never empty
     */
    public List<DiscoveryNode> getEligibleNodes() {
        DiscoveryNodes nodes = clusterService.state().nodes();
        List<DiscoveryNode> eligible = new ArrayList<>();
        for (DiscoveryNode node : nodes) {
            if ("true".equals(node.getAttributes().get(LAKEHOUSE_WORKER_ATTR))) {
                eligible.add(node);
            }
        }
        if (eligible.isEmpty()) {
            DiscoveryNode localNode = nodes.getLocalNode();
            if (localNode != null) {
                return Collections.singletonList(localNode);
            }
            return Collections.emptyList();
        }
        // Shuffle to avoid always assigning the largest files to the same node
        Collections.shuffle(eligible);
        return Collections.unmodifiableList(eligible);
    }
}

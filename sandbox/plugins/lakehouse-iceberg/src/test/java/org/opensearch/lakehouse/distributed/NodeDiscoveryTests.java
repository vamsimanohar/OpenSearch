/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NodeDiscoveryTests extends OpenSearchTestCase {

    public void testReturnsNodesWithLakehouseAttribute() {
        DiscoveryNode workerNode1 = newNode("node1", Map.of(NodeDiscovery.LAKEHOUSE_WORKER_ATTR, "true"));
        DiscoveryNode workerNode2 = newNode("node2", Map.of(NodeDiscovery.LAKEHOUSE_WORKER_ATTR, "true"));
        DiscoveryNode otherNode = newNode("node3", Map.of());

        ClusterService clusterService = mockClusterService(
            List.of(workerNode1, workerNode2, otherNode),
            "node1"
        );
        NodeDiscovery discovery = new NodeDiscovery(clusterService);
        List<DiscoveryNode> eligible = discovery.getEligibleNodes();

        assertEquals(2, eligible.size());
        assertTrue(eligible.contains(workerNode1));
        assertTrue(eligible.contains(workerNode2));
        assertFalse(eligible.contains(otherNode));
    }

    public void testFallsBackToLocalNodeWhenNoAttributePresent() {
        DiscoveryNode localNode = newNode("local", Map.of());
        DiscoveryNode otherNode = newNode("other", Map.of());

        ClusterService clusterService = mockClusterService(List.of(localNode, otherNode), "local");
        NodeDiscovery discovery = new NodeDiscovery(clusterService);
        List<DiscoveryNode> eligible = discovery.getEligibleNodes();

        assertEquals(1, eligible.size());
        assertEquals(localNode, eligible.get(0));
    }

    public void testSingleNodeCluster() {
        DiscoveryNode singleNode = newNode("single", Map.of(NodeDiscovery.LAKEHOUSE_WORKER_ATTR, "true"));

        ClusterService clusterService = mockClusterService(List.of(singleNode), "single");
        NodeDiscovery discovery = new NodeDiscovery(clusterService);
        List<DiscoveryNode> eligible = discovery.getEligibleNodes();

        assertEquals(1, eligible.size());
        assertEquals(singleNode, eligible.get(0));
    }

    public void testSingleNodeClusterWithoutAttribute() {
        DiscoveryNode singleNode = newNode("single", Map.of());

        ClusterService clusterService = mockClusterService(List.of(singleNode), "single");
        NodeDiscovery discovery = new NodeDiscovery(clusterService);
        List<DiscoveryNode> eligible = discovery.getEligibleNodes();

        assertEquals(1, eligible.size());
        assertEquals(singleNode, eligible.get(0));
    }

    public void testAttributeValueMustBeTrue() {
        DiscoveryNode falseAttr = newNode("node1", Map.of(NodeDiscovery.LAKEHOUSE_WORKER_ATTR, "false"));
        DiscoveryNode localNode = newNode("local", Map.of());

        ClusterService clusterService = mockClusterService(List.of(falseAttr, localNode), "local");
        NodeDiscovery discovery = new NodeDiscovery(clusterService);
        List<DiscoveryNode> eligible = discovery.getEligibleNodes();

        // "false" attribute value should not match — falls back to local node
        assertEquals(1, eligible.size());
        assertEquals(localNode, eligible.get(0));
    }

    public void testEmptyClusterReturnsEmptyList() {
        ClusterService clusterService = mockClusterService(List.of(), null);
        NodeDiscovery discovery = new NodeDiscovery(clusterService);
        List<DiscoveryNode> eligible = discovery.getEligibleNodes();

        assertTrue(eligible.isEmpty());
    }

    public void testResultListIsUnmodifiable() {
        DiscoveryNode node = newNode("n1", Map.of(NodeDiscovery.LAKEHOUSE_WORKER_ATTR, "true"));
        ClusterService clusterService = mockClusterService(List.of(node), "n1");
        NodeDiscovery discovery = new NodeDiscovery(clusterService);
        List<DiscoveryNode> eligible = discovery.getEligibleNodes();

        expectThrows(UnsupportedOperationException.class, () -> eligible.add(newNode("x", Map.of())));
    }

    public void testLakehouseWorkerAttrConstant() {
        assertEquals("lakehouse.worker", NodeDiscovery.LAKEHOUSE_WORKER_ATTR);
    }

    private static DiscoveryNode newNode(String nodeId, Map<String, String> attributes) {
        return new DiscoveryNode(
            nodeId,
            nodeId,
            buildNewFakeTransportAddress(),
            attributes,
            Set.of(),
            Version.CURRENT
        );
    }

    private static ClusterService mockClusterService(List<DiscoveryNode> nodes, String localNodeId) {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        for (DiscoveryNode node : nodes) {
            builder.add(node);
        }
        if (localNodeId != null) {
            builder.localNodeId(localNodeId);
        }
        DiscoveryNodes discoveryNodes = builder.build();

        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.nodes()).thenReturn(discoveryNodes);

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        return clusterService;
    }
}

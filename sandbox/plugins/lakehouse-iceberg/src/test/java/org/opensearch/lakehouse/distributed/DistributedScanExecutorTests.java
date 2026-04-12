/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.apache.calcite.rel.RelNode;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DistributedScanExecutorTests extends OpenSearchTestCase {

    public void testSingleNodeFallbackReturnsNull() {
        // Only 1 eligible node → returns null (fall back to single-node)
        DiscoveryNode localNode = newNode("local", Map.of(NodeDiscovery.LAKEHOUSE_WORKER_ATTR, "true"));
        ClusterService clusterService = mockClusterService(List.of(localNode), "local");
        TransportService transportService = mock(TransportService.class);

        DistributedScanExecutor executor = new DistributedScanExecutor(transportService, clusterService);
        RelNode relNode = mockSimpleRelNode();

        Iterable<Object[]> result = executor.execute(
            relNode,
            "SELECT * FROM t",
            List.of("f1", "f2"),
            new long[]{100, 200},
            Map.of(),
            "t"
        );

        assertNull(result);
    }

    public void testSingleNodeStrategyFallbackReturnsNull() {
        // 2 eligible nodes but query requires SINGLE_NODE → returns null
        DiscoveryNode node1 = newNode("n1", Map.of(NodeDiscovery.LAKEHOUSE_WORKER_ATTR, "true"));
        DiscoveryNode node2 = newNode("n2", Map.of(NodeDiscovery.LAKEHOUSE_WORKER_ATTR, "true"));
        ClusterService clusterService = mockClusterService(List.of(node1, node2), "n1");
        TransportService transportService = mock(TransportService.class);

        DistributedScanExecutor executor = new DistributedScanExecutor(transportService, clusterService);

        // Mock a GroupBy aggregate → SINGLE_NODE
        RelNode relNode = mockGroupByRelNode();

        Iterable<Object[]> result = executor.execute(
            relNode,
            "SELECT col, COUNT(*) FROM t GROUP BY col",
            List.of("f1", "f2"),
            new long[]{100, 200},
            Map.of(),
            "t"
        );

        assertNull(result);
    }

    public void testNoEligibleNodesReturnsNull() {
        // NodeDiscovery falls back to local node (1 node) → returns null
        DiscoveryNode localNode = newNode("local", Map.of());
        ClusterService clusterService = mockClusterService(List.of(localNode), "local");
        TransportService transportService = mock(TransportService.class);

        DistributedScanExecutor executor = new DistributedScanExecutor(transportService, clusterService);
        RelNode relNode = mockSimpleRelNode();

        Iterable<Object[]> result = executor.execute(
            relNode,
            "SELECT * FROM t",
            List.of("f1"),
            new long[]{100},
            Map.of(),
            "t"
        );

        assertNull(result);
    }

    public void testConstructorWithNodeDiscovery() {
        TransportService transportService = mock(TransportService.class);
        ClusterService clusterService = mock(ClusterService.class);
        NodeDiscovery nodeDiscovery = mock(NodeDiscovery.class);

        // Use the package-private constructor
        DistributedScanExecutor executor = new DistributedScanExecutor(transportService, clusterService, nodeDiscovery);
        assertNotNull(executor);
    }

    public void testDefaultTimeoutConstant() {
        assertEquals(120L, DistributedScanExecutor.DEFAULT_TIMEOUT_SECONDS);
    }

    @SuppressWarnings("unchecked")
    public void testDispatchAndCollectWithEmptyAssignment() {
        DiscoveryNode node1 = newNode("n1", Map.of(NodeDiscovery.LAKEHOUSE_WORKER_ATTR, "true"));
        DiscoveryNode node2 = newNode("n2", Map.of(NodeDiscovery.LAKEHOUSE_WORKER_ATTR, "true"));
        ClusterService clusterService = mockClusterService(List.of(node1, node2), "n1");
        TransportService transportService = mock(TransportService.class);

        DistributedScanExecutor executor = new DistributedScanExecutor(transportService, clusterService);

        // Create an empty assignment (no files)
        FilePartitioner.FileAssignment emptyAssignment = new FilePartitioner.FileAssignment(List.of(), new long[]{}, 0);

        // Mock transport to provide response for the non-empty assignment
        FilePartitioner.FileAssignment realAssignment = new FilePartitioner.FileAssignment(List.of("f1"), new long[]{100}, 100);

        // For the local node dispatch, the transport service will be called
        // We need to simulate response via the handler
        org.mockito.stubbing.Answer<Void> answerWithResponse = invocation -> {
            @SuppressWarnings("unchecked")
            org.opensearch.transport.TransportResponseHandler<WorkerQueryResponse> handler =
                (org.opensearch.transport.TransportResponseHandler<WorkerQueryResponse>) invocation.getArguments()[3];
            handler.handleResponse(new WorkerQueryResponse(List.of("col"), List.of("Integer"), 1, new Object[][]{{42}}));
            return null;
        };

        org.mockito.Mockito.doAnswer(answerWithResponse).when(transportService).sendRequest(
            any(DiscoveryNode.class),
            any(String.class),
            any(org.opensearch.transport.TransportRequest.class),
            any(org.opensearch.transport.TransportResponseHandler.class)
        );

        List<WorkerQueryResponse> responses = executor.dispatchAndCollect(
            List.of(node1, node2),
            List.of(emptyAssignment, realAssignment),
            "SELECT * FROM t",
            Map.of(),
            "t"
        );

        assertEquals(2, responses.size());
    }

    @SuppressWarnings("unchecked")
    public void testDispatchRemoteCallsTransportService() {
        DiscoveryNode node1 = newNode("n1", Map.of());
        DiscoveryNode node2 = newNode("n2", Map.of());
        ClusterService clusterService = mockClusterService(List.of(node1, node2), "n1");
        TransportService transportService = mock(TransportService.class);

        DistributedScanExecutor executor = new DistributedScanExecutor(transportService, clusterService);

        WorkerQueryRequest request = new WorkerQueryRequest("SELECT 1", List.of("f1"), new long[]{100}, Map.of(), "t");

        @SuppressWarnings("unchecked")
        ActionListener<WorkerQueryResponse> listener = mock(ActionListener.class);

        executor.dispatchRemote(node2, request, listener);

        // Verify transportService.sendRequest was called
        org.mockito.Mockito.verify(transportService).sendRequest(
            org.mockito.Mockito.eq(node2),
            org.mockito.Mockito.eq(WorkerQueryAction.NAME),
            org.mockito.Mockito.eq(request),
            any(org.opensearch.transport.TransportResponseHandler.class)
        );
    }

    @SuppressWarnings("unchecked")
    public void testDispatchLocalCallsTransportService() {
        DiscoveryNode localNode = newNode("local", Map.of());
        ClusterService clusterService = mockClusterService(List.of(localNode), "local");
        TransportService transportService = mock(TransportService.class);

        DistributedScanExecutor executor = new DistributedScanExecutor(transportService, clusterService);

        WorkerQueryRequest request = new WorkerQueryRequest("SELECT 1", List.of("f1"), new long[]{100}, Map.of(), "t");

        @SuppressWarnings("unchecked")
        ActionListener<WorkerQueryResponse> listener = mock(ActionListener.class);

        executor.dispatchLocal(request, listener);

        // Verify sendRequest was called with local node
        org.mockito.Mockito.verify(transportService).sendRequest(
            org.mockito.Mockito.eq(localNode),
            org.mockito.Mockito.eq(WorkerQueryAction.NAME),
            org.mockito.Mockito.eq(request),
            any(org.opensearch.transport.TransportResponseHandler.class)
        );
    }

    // --- Helper methods ---

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

    private RelNode mockSimpleRelNode() {
        RelNode node = mock(RelNode.class);
        when(node.getInputs()).thenReturn(List.of());
        return node;
    }

    private RelNode mockGroupByRelNode() {
        org.apache.calcite.rel.core.Aggregate agg = mock(org.apache.calcite.rel.core.Aggregate.class);
        when(agg.getGroupSet()).thenReturn(org.apache.calcite.util.ImmutableBitSet.of(0));
        when(agg.getAggCallList()).thenReturn(List.of());
        when(agg.getInputs()).thenReturn(List.of());
        return agg;
    }
}

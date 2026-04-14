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
import org.opensearch.analytics.exec.DataWarehouseScanContext;
import org.opensearch.analytics.exec.DataWarehouseQueryEngine;
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

    private DataWarehouseQueryEngine setupMockBackend(Object[]... rows) {
        DataWarehouseQueryEngine mockBackend = mock(DataWarehouseQueryEngine.class);
        when(mockBackend.executeQuery(any(DataWarehouseScanContext.class)))
            .thenReturn(List.of(rows));
        return mockBackend;
    }

    public void testSingleNodeFallbackExecutesLocally() {
        // Only 1 eligible node → executes locally via WorkerQueryExecutor
        DataWarehouseQueryEngine mockBackend = setupMockBackend(new Object[]{1, "hello"}, new Object[]{2, "world"});
        DiscoveryNode localNode = newNode("local", Map.of(NodeDiscovery.LAKEHOUSE_WORKER_ATTR, "true"));
        ClusterService clusterService = mockClusterService(List.of(localNode), "local");
        TransportService transportService = mock(TransportService.class);

        DistributedScanExecutor executor = new DistributedScanExecutor(transportService, clusterService, mockBackend);
        RelNode relNode = mockSimpleRelNode();

        Iterable<Object[]> result = executor.execute(
            relNode,
            "SELECT * FROM t",
            List.of("f1", "f2"),
            new long[]{100, 200},
            Map.of("localMode", "true"),
            "t"
        );

        assertNotNull(result);
        List<Object[]> rows = new ArrayList<>();
        result.forEach(rows::add);
        assertEquals(2, rows.size());
    }

    public void testSingleNodeStrategyFallbackExecutesLocally() {
        // 2 eligible nodes but query requires SINGLE_NODE → executes locally
        DataWarehouseQueryEngine mockBackend = setupMockBackend(new Object[]{42});
        DiscoveryNode node1 = newNode("n1", Map.of(NodeDiscovery.LAKEHOUSE_WORKER_ATTR, "true"));
        DiscoveryNode node2 = newNode("n2", Map.of(NodeDiscovery.LAKEHOUSE_WORKER_ATTR, "true"));
        ClusterService clusterService = mockClusterService(List.of(node1, node2), "n1");
        TransportService transportService = mock(TransportService.class);

        DistributedScanExecutor executor = new DistributedScanExecutor(transportService, clusterService, mockBackend);

        // Mock a GroupBy aggregate → SINGLE_NODE
        RelNode relNode = mockGroupByRelNode();

        Iterable<Object[]> result = executor.execute(
            relNode,
            "SELECT col, COUNT(*) FROM t GROUP BY col",
            List.of("f1", "f2"),
            new long[]{100, 200},
            Map.of("localMode", "true"),
            "t"
        );

        assertNotNull(result);
        List<Object[]> rows = new ArrayList<>();
        result.forEach(rows::add);
        assertEquals(1, rows.size());
    }

    public void testNoEligibleNodesExecutesLocally() {
        // NodeDiscovery falls back to local node (1 node) → executes locally
        DataWarehouseQueryEngine mockBackend = setupMockBackend(new Object[]{"value"});
        DiscoveryNode localNode = newNode("local", Map.of());
        ClusterService clusterService = mockClusterService(List.of(localNode), "local");
        TransportService transportService = mock(TransportService.class);

        DistributedScanExecutor executor = new DistributedScanExecutor(transportService, clusterService, mockBackend);
        RelNode relNode = mockSimpleRelNode();

        Iterable<Object[]> result = executor.execute(
            relNode,
            "SELECT * FROM t",
            List.of("f1"),
            new long[]{100},
            Map.of("localMode", "true"),
            "t"
        );

        assertNotNull(result);
        List<Object[]> rows = new ArrayList<>();
        result.forEach(rows::add);
        assertEquals(1, rows.size());
    }

    public void testConstructorWithNodeDiscovery() {
        TransportService transportService = mock(TransportService.class);
        ClusterService clusterService = mock(ClusterService.class);
        NodeDiscovery nodeDiscovery = mock(NodeDiscovery.class);

        // Use the package-private constructor
        DistributedScanExecutor executor = new DistributedScanExecutor(transportService, clusterService, nodeDiscovery, mock(DataWarehouseQueryEngine.class));
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

        // Mock the thread pool scheduler for health check logging
        org.opensearch.threadpool.ThreadPool threadPool = mock(org.opensearch.threadpool.ThreadPool.class);
        java.util.concurrent.ScheduledExecutorService scheduler = mock(java.util.concurrent.ScheduledExecutorService.class);
        when(transportService.getThreadPool()).thenReturn(threadPool);
        when(threadPool.scheduler()).thenReturn(scheduler);
        org.mockito.Mockito.doReturn(mock(java.util.concurrent.ScheduledFuture.class))
            .when(scheduler).scheduleAtFixedRate(any(Runnable.class), any(Long.class), any(Long.class), any(java.util.concurrent.TimeUnit.class));

        DistributedScanExecutor executor = new DistributedScanExecutor(transportService, clusterService, mock(DataWarehouseQueryEngine.class));

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

        DistributedScanExecutor executor = new DistributedScanExecutor(transportService, clusterService, mock(DataWarehouseQueryEngine.class));

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
    public void testDispatchLocalUsesThreadPoolNotTransport() {
        DiscoveryNode localNode = newNode("local", Map.of());
        ClusterService clusterService = mockClusterService(List.of(localNode), "local");
        TransportService transportService = mock(TransportService.class);

        // Mock the thread pool chain: transportService → threadPool → executor
        org.opensearch.threadpool.ThreadPool threadPool = mock(org.opensearch.threadpool.ThreadPool.class);
        java.util.concurrent.ExecutorService executorService = mock(java.util.concurrent.ExecutorService.class);
        when(transportService.getThreadPool()).thenReturn(threadPool);
        when(threadPool.executor(org.opensearch.lakehouse.LakehousePlugin.LAKEHOUSE_WORKER_THREAD_POOL)).thenReturn(executorService);

        DistributedScanExecutor executor = new DistributedScanExecutor(transportService, clusterService, mock(DataWarehouseQueryEngine.class));

        WorkerQueryRequest request = new WorkerQueryRequest(
            "SELECT 1", List.of("f1"), new long[]{100}, Map.of("localMode", "true"), "t"
        );

        @SuppressWarnings("unchecked")
        ActionListener<WorkerQueryResponse> listener = mock(ActionListener.class);

        executor.dispatchLocal(request, listener);

        // Verify thread pool was used (NOT transport sendRequest)
        org.mockito.Mockito.verify(threadPool).executor(org.opensearch.lakehouse.LakehousePlugin.LAKEHOUSE_WORKER_THREAD_POOL);
        org.mockito.Mockito.verify(executorService).execute(any(Runnable.class));
        // Verify sendRequest was NOT called (local dispatch bypasses transport)
        org.mockito.Mockito.verify(transportService, org.mockito.Mockito.never()).sendRequest(
            any(DiscoveryNode.class),
            any(String.class),
            any(org.opensearch.transport.TransportRequest.class),
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

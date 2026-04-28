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
import org.opensearch.lakehouse.distributed.worker.WorkerQueryAction;
import org.opensearch.lakehouse.distributed.worker.WorkerQueryRequest;
import org.opensearch.lakehouse.distributed.worker.WorkerQueryResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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

    private List<Object[]> executeAndWait(
        DistributedScanExecutor executor,
        RelNode relNode,
        String sql,
        List<String> filePaths,
        long[] fileSizes,
        Map<String, String> storageConfig,
        String tableName
    ) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Iterable<Object[]>> resultRef = new AtomicReference<>();
        AtomicReference<Exception> errorRef = new AtomicReference<>();

        executor.executeAsync(relNode, sql, filePaths, fileSizes, storageConfig, tableName, new ActionListener<>() {
            @Override
            public void onResponse(Iterable<Object[]> result) {
                resultRef.set(result);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                errorRef.set(e);
                latch.countDown();
            }
        });

        assertTrue("Timed out waiting for async result", latch.await(10, TimeUnit.SECONDS));
        if (errorRef.get() != null) throw errorRef.get();

        List<Object[]> rows = new ArrayList<>();
        resultRef.get().forEach(rows::add);
        return rows;
    }

    public void testSingleNodeFallbackExecutesLocally() throws Exception {
        DataWarehouseQueryEngine mockBackend = setupMockBackend(new Object[]{1, "hello"}, new Object[]{2, "world"});
        DiscoveryNode localNode = newNode("local", Map.of(NodeDiscovery.LAKEHOUSE_WORKER_ATTR, "true"));
        ClusterService clusterService = mockClusterService(List.of(localNode), "local");
        TransportService transportService = mockTransportServiceWithThreadPool();

        DistributedScanExecutor executor = new DistributedScanExecutor(transportService, clusterService, mockBackend);
        RelNode relNode = mockSimpleRelNode();

        List<Object[]> rows = executeAndWait(
            executor, relNode, "SELECT * FROM t",
            List.of("f1", "f2"), new long[]{100, 200},
            Map.of("localMode", "true"), "t"
        );

        assertEquals(2, rows.size());
    }

    @SuppressWarnings("unchecked")
    public void testUnsupportedDistinctSumThrowsException() {
        DataWarehouseQueryEngine mockBackend = setupMockBackend(new Object[]{42});
        DiscoveryNode node1 = newNode("n1", Map.of(NodeDiscovery.LAKEHOUSE_WORKER_ATTR, "true"));
        DiscoveryNode node2 = newNode("n2", Map.of(NodeDiscovery.LAKEHOUSE_WORKER_ATTR, "true"));
        ClusterService clusterService = mockClusterService(List.of(node1, node2), "n1");
        TransportService transportService = mockTransportServiceWithThreadPool();

        DistributedScanExecutor executor = new DistributedScanExecutor(transportService, clusterService, mockBackend);
        RelNode relNode = mockGroupByWithDistinctSumRelNode();
        ActionListener<Iterable<Object[]>> listener = mock(ActionListener.class);

        expectThrows(UnsupportedOperationException.class,
            () -> executor.executeAsync(relNode, "SELECT col, SUM(DISTINCT col) FROM t GROUP BY col",
                List.of("f1", "f2"), new long[]{100, 200},
                Map.of("localMode", "true"), "t", listener));
    }

    public void testNoEligibleNodesExecutesLocally() throws Exception {
        DataWarehouseQueryEngine mockBackend = setupMockBackend(new Object[]{"value"});
        DiscoveryNode localNode = newNode("local", Map.of());
        ClusterService clusterService = mockClusterService(List.of(localNode), "local");
        TransportService transportService = mockTransportServiceWithThreadPool();

        DistributedScanExecutor executor = new DistributedScanExecutor(transportService, clusterService, mockBackend);
        RelNode relNode = mockSimpleRelNode();

        List<Object[]> rows = executeAndWait(
            executor, relNode, "SELECT * FROM t",
            List.of("f1"), new long[]{100},
            Map.of("localMode", "true"), "t"
        );

        assertEquals(1, rows.size());
    }

    public void testConstructorWithNodeDiscovery() {
        TransportService transportService = mock(TransportService.class);
        ClusterService clusterService = mock(ClusterService.class);
        NodeDiscovery nodeDiscovery = mock(NodeDiscovery.class);

        DistributedScanExecutor executor = new DistributedScanExecutor(transportService, clusterService, nodeDiscovery, mock(DataWarehouseQueryEngine.class));
        assertNotNull(executor);
    }

    @SuppressWarnings("unchecked")
    public void testDispatchAndCollectWithEmptyAssignment() throws Exception {
        DiscoveryNode node1 = newNode("n1", Map.of(NodeDiscovery.LAKEHOUSE_WORKER_ATTR, "true"));
        DiscoveryNode node2 = newNode("n2", Map.of(NodeDiscovery.LAKEHOUSE_WORKER_ATTR, "true"));
        ClusterService clusterService = mockClusterService(List.of(node1, node2), "n1");
        TransportService transportService = mock(TransportService.class);

        DistributedScanExecutor executor = new DistributedScanExecutor(transportService, clusterService, mock(DataWarehouseQueryEngine.class));

        FilePartitioner.FileAssignment emptyAssignment = new FilePartitioner.FileAssignment(List.of(), new long[]{}, 0);
        FilePartitioner.FileAssignment realAssignment = new FilePartitioner.FileAssignment(List.of("f1"), new long[]{100}, 100);

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

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<List<WorkerQueryResponse>> resultRef = new AtomicReference<>();

        executor.dispatchAndCollect(
            List.of(node1, node2),
            List.of(emptyAssignment, realAssignment),
            "SELECT * FROM t",
            Map.of(),
            "t",
            new ActionListener<>() {
                @Override
                public void onResponse(List<WorkerQueryResponse> responses) {
                    resultRef.set(responses);
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    latch.countDown();
                }
            }
        );

        assertTrue("Timed out", latch.await(10, TimeUnit.SECONDS));
        assertEquals(2, resultRef.get().size());
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

        org.mockito.Mockito.verify(threadPool).executor(org.opensearch.lakehouse.LakehousePlugin.LAKEHOUSE_WORKER_THREAD_POOL);
        org.mockito.Mockito.verify(executorService).execute(any(Runnable.class));
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

    private static TransportService mockTransportServiceWithThreadPool() {
        TransportService transportService = mock(TransportService.class);
        org.opensearch.threadpool.ThreadPool threadPool = mock(org.opensearch.threadpool.ThreadPool.class);
        java.util.concurrent.ExecutorService directExecutor = new java.util.concurrent.AbstractExecutorService() {
            private volatile boolean shutdown = false;

            @Override public void execute(Runnable command) { command.run(); }
            @Override public void shutdown() { shutdown = true; }
            @Override public List<Runnable> shutdownNow() { shutdown = true; return List.of(); }
            @Override public boolean isShutdown() { return shutdown; }
            @Override public boolean isTerminated() { return shutdown; }
            @Override public boolean awaitTermination(long timeout, TimeUnit unit) { return true; }
        };
        when(transportService.getThreadPool()).thenReturn(threadPool);
        when(threadPool.executor(org.opensearch.lakehouse.LakehousePlugin.LAKEHOUSE_WORKER_THREAD_POOL)).thenReturn(directExecutor);
        return transportService;
    }

    private RelNode mockSimpleRelNode() {
        RelNode node = mock(RelNode.class);
        when(node.getInputs()).thenReturn(List.of());
        return node;
    }

    @SuppressWarnings("deprecation")
    private RelNode mockGroupByWithDistinctSumRelNode() {
        org.apache.calcite.rel.core.Aggregate agg = mock(org.apache.calcite.rel.core.Aggregate.class);
        when(agg.getGroupSet()).thenReturn(org.apache.calcite.util.ImmutableBitSet.of(0));
        org.apache.calcite.rel.type.RelDataType bigintType = new org.apache.calcite.sql.type.BasicSqlType(
            org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT, org.apache.calcite.sql.type.SqlTypeName.BIGINT
        );
        org.apache.calcite.rel.core.AggregateCall distinctCall = new org.apache.calcite.rel.core.AggregateCall(
            org.apache.calcite.sql.fun.SqlStdOperatorTable.SUM, true, List.of(), bigintType, null
        );
        when(agg.getAggCallList()).thenReturn(List.of(distinctCall));
        when(agg.getInputs()).thenReturn(List.of());
        return agg;
    }
}

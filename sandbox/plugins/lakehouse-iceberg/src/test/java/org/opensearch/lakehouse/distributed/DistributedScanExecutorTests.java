/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlKind;
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
import java.util.Collections;
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

    /**
     * Helper to execute async and block for result in tests.
     */
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
        // Only 1 eligible node → executes locally via WorkerQueryExecutor
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

    public void testSingleNodeStrategyFallbackExecutesLocally() throws Exception {
        // 2 eligible nodes but query requires SINGLE_NODE → executes locally
        DataWarehouseQueryEngine mockBackend = setupMockBackend(new Object[]{42});
        DiscoveryNode node1 = newNode("n1", Map.of(NodeDiscovery.LAKEHOUSE_WORKER_ATTR, "true"));
        DiscoveryNode node2 = newNode("n2", Map.of(NodeDiscovery.LAKEHOUSE_WORKER_ATTR, "true"));
        ClusterService clusterService = mockClusterService(List.of(node1, node2), "n1");
        TransportService transportService = mockTransportServiceWithThreadPool();

        DistributedScanExecutor executor = new DistributedScanExecutor(transportService, clusterService, mockBackend);

        // Mock a GroupBy aggregate with DISTINCT → SINGLE_NODE
        RelNode relNode = mockGroupByWithDistinctRelNode();

        List<Object[]> rows = executeAndWait(
            executor, relNode, "SELECT col, COUNT(*) FROM t GROUP BY col",
            List.of("f1", "f2"), new long[]{100, 200},
            Map.of("localMode", "true"), "t"
        );

        assertEquals(1, rows.size());
    }

    public void testNoEligibleNodesExecutesLocally() throws Exception {
        // NodeDiscovery falls back to local node (1 node) → executes locally
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

        // Use the package-private constructor
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

        // Create an empty assignment (no files)
        FilePartitioner.FileAssignment emptyAssignment = new FilePartitioner.FileAssignment(List.of(), new long[]{}, 0);

        // Mock transport to provide response for the non-empty assignment
        FilePartitioner.FileAssignment realAssignment = new FilePartitioner.FileAssignment(List.of("f1"), new long[]{100}, 100);

        // For the local node dispatch, the transport service will be called
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

    // --- TOPK_MERGE SQL rewriting tests ---

    public void testAddColumnsToSelectSingleColumn() {
        String sql = "SELECT \"searchphrase\" FROM \"hits\" WHERE \"searchphrase\" <> '' ORDER BY \"eventtime\" LIMIT 10";
        String result = DistributedScanExecutor.addColumnsToSelect(sql, List.of("eventtime"));
        assertEquals(
            "SELECT \"searchphrase\", \"eventtime\" FROM \"hits\" WHERE \"searchphrase\" <> '' ORDER BY \"eventtime\" LIMIT 10",
            result
        );
    }

    public void testAddColumnsToSelectMultiLineFromOnNewLine() {
        // Calcite generates multi-line SQL with FROM on a new line
        String sql = "SELECT \"searchphrase\"\nFROM \"hits_s3\"\nWHERE \"searchphrase\" <> ''\nORDER BY \"eventtime\"\nLIMIT 10";
        String result = DistributedScanExecutor.addColumnsToSelect(sql, List.of("eventtime"));
        assertEquals(
            "SELECT \"searchphrase\", \"eventtime\"\nFROM \"hits_s3\"\nWHERE \"searchphrase\" <> ''\nORDER BY \"eventtime\"\nLIMIT 10",
            result
        );
    }

    public void testAddColumnsToSelectMultipleColumns() {
        String sql = "SELECT \"a\" FROM \"t\" ORDER BY \"b\", \"c\" LIMIT 5";
        String result = DistributedScanExecutor.addColumnsToSelect(sql, List.of("b", "c"));
        assertEquals("SELECT \"a\", \"b\", \"c\" FROM \"t\" ORDER BY \"b\", \"c\" LIMIT 5", result);
    }

    public void testAddColumnsToSelectNoFromReturnsOriginal() {
        String sql = "INVALID SQL WITHOUT FROM";
        String result = DistributedScanExecutor.addColumnsToSelect(sql, List.of("col"));
        assertEquals(sql, result);
    }

    // --- Coordinator SQL builder tests ---

    public void testBuildGlobalMergeCoordinatorSqlSumCountMinMax() {
        WorkerQueryResponse r = new WorkerQueryResponse(
            List.of("count(*)", "sum(x)", "min(y)", "max(z)"),
            List.of("Long", "Double", "String", "String"),
            1,
            new Object[][]{{100L}, {1000.0}, {"2013-07-01"}, {"2013-07-30"}}
        );
        SqlKind[] aggKinds = {SqlKind.COUNT, SqlKind.SUM, SqlKind.MIN, SqlKind.MAX};
        String sql = DistributedScanExecutor.buildGlobalMergeCoordinatorSql(List.of(r), aggKinds);
        assertEquals(
            "SELECT SUM(\"count(*)\"), SUM(\"sum(x)\"), MIN(\"min(y)\"), MAX(\"max(z)\") FROM __exchange_input__",
            sql
        );
    }

    public void testBuildGlobalMergeCoordinatorSqlDefaultsToSum() {
        WorkerQueryResponse r = new WorkerQueryResponse(
            List.of("col0", "col1"), List.of("Long", "Long"), 1, new Object[][]{{10L}, {20L}}
        );
        String sql = DistributedScanExecutor.buildGlobalMergeCoordinatorSql(List.of(r), null);
        assertEquals("SELECT SUM(\"col0\"), SUM(\"col1\") FROM __exchange_input__", sql);
    }

    public void testBuildTopKMergeCoordinatorSqlSortColumnsInOutput() {
        // Sort column index 1 is within outputColumnCount=2 → no wrapping needed
        String sql = DistributedScanExecutor.buildTopKMergeCoordinatorSql(
            new int[]{1}, new boolean[]{true}, 10, 2
        );
        assertEquals("SELECT * FROM __exchange_input__ ORDER BY \"col_1\" ASC LIMIT 10", sql);
    }

    public void testBuildTopKMergeCoordinatorSqlMultiSortAllInOutput() {
        // All sort column indices (0,1) within outputColumnCount=3 → no wrapping
        String sql = DistributedScanExecutor.buildTopKMergeCoordinatorSql(
            new int[]{0, 1}, new boolean[]{true, false}, 5, 3
        );
        assertEquals("SELECT * FROM __exchange_input__ ORDER BY \"col_0\" ASC, \"col_1\" DESC LIMIT 5", sql);
    }

    public void testBuildTopKMergeCoordinatorSqlWithExtraColumns() {
        // Sort column index 1 >= outputColumnCount=1 → needs stripping via subquery
        String sql = DistributedScanExecutor.buildTopKMergeCoordinatorSql(
            new int[]{1}, new boolean[]{true}, 10, 1
        );
        assertEquals(
            "SELECT \"col_0\" FROM (SELECT * FROM __exchange_input__ ORDER BY \"col_1\" ASC LIMIT 10)",
            sql
        );
    }

    public void testBuildTopKMergeCoordinatorSqlNoLimitOmitsLimit() {
        String sql = DistributedScanExecutor.buildTopKMergeCoordinatorSql(
            new int[]{0}, new boolean[]{true}, 0, 1
        );
        assertEquals("SELECT * FROM __exchange_input__ ORDER BY \"col_0\" ASC", sql);
    }

    // --- TWO_PHASE_GROUP_BY coordinator SQL tests ---

    public void testBuildTwoPhaseGroupBySingleGroupSingleAgg() {
        WorkerQueryResponse r = new WorkerQueryResponse(
            List.of("col_0", "col_1"), List.of("String", "Long"), 2,
            new Object[][]{{"a", "b"}, {10L, 20L}}
        );
        String sql = DistributedScanExecutor.buildTwoPhaseGroupByCoordinatorSql(
            List.of(r), 1, new SqlKind[]{SqlKind.COUNT}, null, null, 0, 0
        );
        assertEquals(
            "SELECT \"col_0\", SUM(\"col_1\") FROM __exchange_input__ GROUP BY \"col_0\"",
            sql
        );
    }

    public void testBuildTwoPhaseGroupByMultiGroupMultiAgg() {
        WorkerQueryResponse r = new WorkerQueryResponse(
            List.of("col_0", "col_1", "col_2", "col_3", "col_4"),
            List.of("String", "Integer", "Long", "Long", "String"),
            1,
            new Object[][]{{"x"}, {1}, {50L}, {100L}, {"2013-07-01"}}
        );
        String sql = DistributedScanExecutor.buildTwoPhaseGroupByCoordinatorSql(
            List.of(r), 2, new SqlKind[]{SqlKind.COUNT, SqlKind.SUM, SqlKind.MIN}, null, null, 0, 0
        );
        assertEquals(
            "SELECT \"col_0\", \"col_1\", SUM(\"col_2\"), SUM(\"col_3\"), MIN(\"col_4\") FROM __exchange_input__ GROUP BY \"col_0\", \"col_1\"",
            sql
        );
    }

    public void testBuildTwoPhaseGroupByMinMax() {
        WorkerQueryResponse r = new WorkerQueryResponse(
            List.of("col_0", "col_1", "col_2"),
            List.of("String", "String", "String"),
            1,
            new Object[][]{{"grp"}, {"2013-01-01"}, {"2013-12-31"}}
        );
        String sql = DistributedScanExecutor.buildTwoPhaseGroupByCoordinatorSql(
            List.of(r), 1, new SqlKind[]{SqlKind.MIN, SqlKind.MAX}, null, null, 0, 0
        );
        assertEquals(
            "SELECT \"col_0\", MIN(\"col_1\"), MAX(\"col_2\") FROM __exchange_input__ GROUP BY \"col_0\"",
            sql
        );
    }

    public void testBuildTwoPhaseGroupByDefaultsToSum() {
        WorkerQueryResponse r = new WorkerQueryResponse(
            List.of("col_0", "col_1"), List.of("Integer", "Long"), 1,
            new Object[][]{{1}, {100L}}
        );
        String sql = DistributedScanExecutor.buildTwoPhaseGroupByCoordinatorSql(
            List.of(r), 1, null, null, null, 0, 0
        );
        assertEquals(
            "SELECT \"col_0\", SUM(\"col_1\") FROM __exchange_input__ GROUP BY \"col_0\"",
            sql
        );
    }

    public void testBuildTwoPhaseGroupByWithOrderByLimitOffset() {
        WorkerQueryResponse r = new WorkerQueryResponse(
            List.of("col_0", "col_1"), List.of("String", "Long"), 2,
            new Object[][]{{"a", "b"}, {10L, 20L}}
        );
        String sql = DistributedScanExecutor.buildTwoPhaseGroupByCoordinatorSql(
            List.of(r), 1, new SqlKind[]{SqlKind.COUNT},
            new int[]{1}, new boolean[]{false}, 10, 5
        );
        assertEquals(
            "SELECT \"col_0\", SUM(\"col_1\") FROM __exchange_input__ GROUP BY \"col_0\" ORDER BY 2 DESC LIMIT 10 OFFSET 5",
            sql
        );
    }

    public void testBuildTwoPhaseGroupBySkipsEmptyResponses() {
        WorkerQueryResponse empty = new WorkerQueryResponse(
            List.of("col_0", "col_1"), List.of("String", "Long"), 0, new Object[0][]
        );
        WorkerQueryResponse nonEmpty = new WorkerQueryResponse(
            List.of("col_0", "col_1"), List.of("String", "Long"), 1,
            new Object[][]{{"a"}, {5L}}
        );
        String sql = DistributedScanExecutor.buildTwoPhaseGroupByCoordinatorSql(
            List.of(empty, nonEmpty), 1, new SqlKind[]{SqlKind.COUNT}, null, null, 0, 0
        );
        assertEquals(
            "SELECT \"col_0\", SUM(\"col_1\") FROM __exchange_input__ GROUP BY \"col_0\"",
            sql
        );
    }

    // --- AVG decomposition tests ---

    public void testHasAvgKindWithAvg() {
        assertTrue(DistributedScanExecutor.hasAvgKind(new SqlKind[]{SqlKind.SUM, SqlKind.AVG}));
    }

    public void testHasAvgKindWithoutAvg() {
        assertFalse(DistributedScanExecutor.hasAvgKind(new SqlKind[]{SqlKind.SUM, SqlKind.COUNT}));
    }

    public void testHasAvgKindNull() {
        assertFalse(DistributedScanExecutor.hasAvgKind(null));
    }

    public void testDecomposeAvgSimple() {
        assertEquals(
            "SELECT SUM(CAST(userid AS DOUBLE)), COUNT(userid) FROM hits",
            DistributedScanExecutor.decomposeAvgInSql("SELECT AVG(userid) FROM hits")
        );
    }

    public void testDecomposeAvgMixed() {
        assertEquals(
            "SELECT SUM(advengineid), COUNT(*), SUM(CAST(resolutionwidth AS DOUBLE)), COUNT(resolutionwidth) FROM hits",
            DistributedScanExecutor.decomposeAvgInSql("SELECT SUM(advengineid), COUNT(*), AVG(resolutionwidth) FROM hits")
        );
    }

    public void testDecomposeAvgNoAvgUnchanged() {
        String sql = "SELECT SUM(x), COUNT(*), MIN(y) FROM t";
        assertEquals(sql, DistributedScanExecutor.decomposeAvgInSql(sql));
    }

    public void testDecomposeAvgWithNestedParens() {
        assertEquals(
            "SELECT SUM(CAST(CHAR_LENGTH(url) AS DOUBLE)), COUNT(CHAR_LENGTH(url)) FROM hits",
            DistributedScanExecutor.decomposeAvgInSql("SELECT AVG(CHAR_LENGTH(url)) FROM hits")
        );
    }

    public void testDecomposeAvgSkipsPartOfIdentifier() {
        String sql = "SELECT XAVG(col) FROM t";
        assertEquals(sql, DistributedScanExecutor.decomposeAvgInSql(sql));
    }

    // --- GLOBAL_MERGE with AVG coordinator SQL tests ---

    public void testBuildGlobalMergeCoordinatorSqlWithAvg() {
        // q4: SELECT AVG(userid) FROM hits
        // Workers compute SUM(userid), COUNT(userid) → col_0, col_1
        WorkerQueryResponse r = new WorkerQueryResponse(
            List.of("col_0", "col_1"), List.of("Long", "Long"), 1,
            new Object[][]{{50000L}, {1000L}}
        );
        SqlKind[] aggKinds = {SqlKind.AVG};
        String sql = DistributedScanExecutor.buildGlobalMergeCoordinatorSql(List.of(r), aggKinds);
        assertEquals(
            "SELECT CAST(SUM(\"col_0\") AS DOUBLE) / SUM(\"col_1\") FROM __exchange_input__",
            sql
        );
    }

    public void testBuildGlobalMergeCoordinatorSqlWithMixedSumCountAvg() {
        // q3: SELECT SUM(advengineid), COUNT(*), AVG(resolutionwidth) FROM hits
        // Workers return: col_0=SUM(adveng), col_1=COUNT(*), col_2=SUM(reswidth), col_3=COUNT(reswidth)
        WorkerQueryResponse r = new WorkerQueryResponse(
            List.of("col_0", "col_1", "col_2", "col_3"),
            List.of("Long", "Long", "Long", "Long"),
            1,
            new Object[][]{{500L}, {100L}, {1280000L}, {100L}}
        );
        SqlKind[] aggKinds = {SqlKind.SUM, SqlKind.COUNT, SqlKind.AVG};
        String sql = DistributedScanExecutor.buildGlobalMergeCoordinatorSql(List.of(r), aggKinds);
        assertEquals(
            "SELECT SUM(\"col_0\"), SUM(\"col_1\"), CAST(SUM(\"col_2\") AS DOUBLE) / SUM(\"col_3\") FROM __exchange_input__",
            sql
        );
    }

    // --- stripOrderByLimitOffset tests ---

    public void testStripOrderByLimitOffset() {
        assertEquals(
            "SELECT a, COUNT(*) FROM t GROUP BY a",
            DistributedScanExecutor.stripOrderByLimitOffset(
                "SELECT a, COUNT(*) FROM t GROUP BY a ORDER BY COUNT(*) DESC LIMIT 10"
            )
        );
    }

    public void testStripOrderByLimitOffsetWithOffset() {
        assertEquals(
            "SELECT a, COUNT(*) FROM t GROUP BY a",
            DistributedScanExecutor.stripOrderByLimitOffset(
                "SELECT a, COUNT(*) FROM t GROUP BY a ORDER BY COUNT(*) DESC LIMIT 10 OFFSET 1000"
            )
        );
    }

    public void testStripOrderByLimitOffsetNoOrderBy() {
        String sql = "SELECT a, COUNT(*) FROM t GROUP BY a";
        assertEquals(sql, DistributedScanExecutor.stripOrderByLimitOffset(sql));
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

    /**
     * Creates a mock TransportService with a real thread pool for async tests.
     * The thread pool executor runs tasks immediately on the calling thread.
     */
    private static TransportService mockTransportServiceWithThreadPool() {
        TransportService transportService = mock(TransportService.class);
        org.opensearch.threadpool.ThreadPool threadPool = mock(org.opensearch.threadpool.ThreadPool.class);
        // Use a direct executor that runs tasks immediately (for deterministic tests)
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
    private RelNode mockGroupByWithDistinctRelNode() {
        org.apache.calcite.rel.core.Aggregate agg = mock(org.apache.calcite.rel.core.Aggregate.class);
        when(agg.getGroupSet()).thenReturn(org.apache.calcite.util.ImmutableBitSet.of(0));
        org.apache.calcite.rel.type.RelDataType bigintType = new org.apache.calcite.sql.type.BasicSqlType(
            org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT, org.apache.calcite.sql.type.SqlTypeName.BIGINT
        );
        org.apache.calcite.rel.core.AggregateCall distinctCall = new org.apache.calcite.rel.core.AggregateCall(
            org.apache.calcite.sql.fun.SqlStdOperatorTable.COUNT, true, List.of(), bigintType, null
        );
        when(agg.getAggCallList()).thenReturn(List.of(distinctCall));
        when(agg.getInputs()).thenReturn(List.of());
        return agg;
    }
}

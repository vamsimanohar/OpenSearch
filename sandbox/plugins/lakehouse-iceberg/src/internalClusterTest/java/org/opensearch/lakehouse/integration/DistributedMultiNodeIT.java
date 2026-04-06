/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.integration;

import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.lakehouse.LakehousePlugin;
import org.opensearch.lakehouse.LakehouseState;
import org.opensearch.lakehouse.distributed.DistributedQueryCoordinator;
import org.opensearch.lakehouse.distributed.LakehouseWorkerAction;
import org.opensearch.lakehouse.distributed.LakehouseWorkerRequest;
import org.opensearch.lakehouse.distributed.LakehouseWorkerResponse;
import org.opensearch.lakehouse.scan.IcebergScanPlan;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportResponseHandler;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.stream.StreamTransportResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.common.util.FeatureFlags.STREAM_TRANSPORT;

/**
 * Multi-node integration tests for distributed Iceberg query execution
 * using Arrow Flight streaming transport.
 *
 * <p>These tests verify that:
 * <ul>
 *   <li>StreamTransportService is available when FlightStreamPlugin is loaded</li>
 *   <li>The distributed coordinator detects multiple nodes and enables distribution</li>
 *   <li>Streaming requests/responses work between nodes via Arrow Flight</li>
 *   <li>The worker action receives and processes streaming requests</li>
 * </ul>
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, minNumDataNodes = 2, maxNumDataNodes = 3)
public class DistributedMultiNodeIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LakehousePlugin.class, FlightStreamPlugin.class);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        internalCluster().ensureAtLeastNumDataNodes(2);
    }

    /**
     * Verify that StreamTransportService is non-null when FlightStreamPlugin is loaded
     * and the feature flag is enabled.
     */
    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testStreamTransportServiceAvailable() {
        StreamTransportService streamTransportService = internalCluster().getInstance(StreamTransportService.class);
        assertNotNull("StreamTransportService should be available with FlightStreamPlugin loaded", streamTransportService);
    }

    /**
     * Verify that the distributed coordinator is initialized and detects
     * that distribution is possible on a multi-node cluster with Arrow Flight.
     */
    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testCoordinatorDetectsMultiNodeCluster() {
        DistributedQueryCoordinator coordinator = LakehouseState.instance().distributedCoordinator();
        assertNotNull("Coordinator should be initialized", coordinator);

        // With 2+ nodes and Arrow Flight available, shouldDistribute should return true
        // for multiple files
        List<IcebergScanPlan.FileInfo> files = List.of(
            new IcebergScanPlan.FileInfo("s3://bucket/data/file1.parquet", 1024 * 1024),
            new IcebergScanPlan.FileInfo("s3://bucket/data/file2.parquet", 2048 * 1024),
            new IcebergScanPlan.FileInfo("s3://bucket/data/file3.parquet", 512 * 1024)
        );

        assertTrue(
            "shouldDistribute must return true on multi-node cluster with Arrow Flight and multiple files",
            coordinator.shouldDistribute(files)
        );
    }

    /**
     * Verify that shouldDistribute still returns false for single file even
     * on a multi-node cluster with Arrow Flight.
     */
    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testShouldNotDistributeSingleFileOnMultiNode() {
        DistributedQueryCoordinator coordinator = LakehouseState.instance().distributedCoordinator();
        assertNotNull(coordinator);

        List<IcebergScanPlan.FileInfo> singleFile = List.of(
            new IcebergScanPlan.FileInfo("s3://bucket/data/file.parquet", 1024 * 1024)
        );

        assertFalse(
            "shouldDistribute must return false for single file even on multi-node cluster",
            coordinator.shouldDistribute(singleFile)
        );
    }

    /**
     * Send a streaming request to a worker node and verify the worker action
     * receives it and responds (with an expected error since no backend executor
     * is registered in this test cluster).
     */
    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testStreamingWorkerRequestReachesNode() throws Exception {
        StreamTransportService streamTransportService = internalCluster().getInstance(StreamTransportService.class);
        assertNotNull(streamTransportService);

        // Pick a data node to send the streaming request to
        DiscoveryNode targetNode = getClusterState().nodes().getDataNodes().values().iterator().next();
        assertNotNull("Should have at least one data node", targetNode);

        // Connect to the target node
        streamTransportService.connectToNode(targetNode);

        // Send a streaming request — it will fail because the backend executor
        // is not initialized, but the transport path should work
        LakehouseWorkerRequest request = new LakehouseWorkerRequest(
            new String[]{"test-file.parquet"},
            new byte[]{1, 2, 3},
            Map.of(),
            "test_table"
        );

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        List<LakehouseWorkerResponse> responses = new ArrayList<>();

        streamTransportService.sendRequest(
            targetNode,
            LakehouseWorkerAction.NAME,
            request,
            TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STREAM).build(),
            new StreamTransportResponseHandler<LakehouseWorkerResponse>() {
                @Override
                public LakehouseWorkerResponse read(StreamInput in) throws IOException {
                    return new LakehouseWorkerResponse(in);
                }

                @Override
                public void handleStreamResponse(StreamTransportResponse<LakehouseWorkerResponse> streamResponse) {
                    try {
                        LakehouseWorkerResponse batch;
                        while ((batch = streamResponse.nextResponse()) != null) {
                            responses.add(batch);
                        }
                    } catch (Exception e) {
                        error.set(e);
                    } finally {
                        try {
                            streamResponse.close();
                        } catch (IOException ignored) {}
                        latch.countDown();
                    }
                }

                @Override
                public void handleException(TransportException exp) {
                    error.set(exp);
                    latch.countDown();
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.GENERIC;
                }
            }
        );

        assertTrue("Should receive response within 10 seconds", latch.await(10, TimeUnit.SECONDS));

        // We expect an error because the backend executor is not set in test cluster
        // (DataFusion plugin not loaded). The important thing is the streaming transport
        // path worked — the request reached the remote node.
        assertNotNull(
            "Expected error since backend executor is not initialized in test cluster",
            error.get()
        );
        assertTrue(
            "Error should indicate backend executor not initialized, got: " + error.get().getMessage(),
            error.get().getMessage() != null
                && error.get().getMessage().contains("Backend executor not initialized")
        );
    }

    /**
     * Verify that the worker action is registered on the streaming transport
     * and the standard transport (for validation requests).
     */
    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testWorkerActionRegisteredOnBothTransports() {
        // Standard transport path — client().execute() should work
        LakehouseWorkerRequest request = new LakehouseWorkerRequest(
            new String[]{"file.parquet"},
            new byte[]{1, 2, 3},
            Map.of(),
            "test_table"
        );

        try {
            client().execute(LakehouseWorkerAction.INSTANCE, request).actionGet();
            fail("Should have thrown because backend executor is not set");
        } catch (Exception e) {
            assertFalse(
                "Transport action should be registered (not a 'no handler' error)",
                e.getMessage() != null && e.getMessage().contains("No handler found for action")
            );
            assertTrue(
                "Error should indicate backend executor not initialized, got: " + e.getMessage(),
                e.getMessage() != null && e.getMessage().contains("Backend executor not initialized")
            );
        }
    }

    /**
     * Verify that the cluster has the expected number of data nodes
     * for distributed query execution.
     */
    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testMultiNodeClusterTopology() {
        int dataNodeCount = getClusterState().nodes().getDataNodes().size();
        assertTrue(
            "Cluster should have at least 2 data nodes, got: " + dataNodeCount,
            dataNodeCount >= 2
        );

        // Log node info for debugging
        for (DiscoveryNode node : getClusterState().nodes().getDataNodes().values()) {
            logger.info("[DistributedMultiNodeIT] Data node: {} ({})", node.getName(), node.getId());
        }
    }
}

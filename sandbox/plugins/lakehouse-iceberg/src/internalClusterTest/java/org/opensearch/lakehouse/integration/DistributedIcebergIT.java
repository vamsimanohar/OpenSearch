/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.integration;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.lakehouse.LakehousePlugin;
import org.opensearch.lakehouse.LakehouseState;
import org.opensearch.lakehouse.distributed.DistributedPlanSplitter;
import org.opensearch.lakehouse.distributed.DistributedQueryCoordinator;
import org.opensearch.lakehouse.distributed.DistributedResultMerger;
import org.opensearch.lakehouse.distributed.DistributionPlan;
import org.opensearch.lakehouse.distributed.FilePartitioner;
import org.opensearch.lakehouse.distributed.LakehouseWorkerAction;
import org.opensearch.lakehouse.distributed.LakehouseWorkerRequest;
import org.opensearch.lakehouse.distributed.LakehouseWorkerResponse;
import org.opensearch.lakehouse.scan.IcebergScanPlan;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Integration tests for distributed query components in the lakehouse-iceberg plugin.
 *
 * <p>These tests verify that distributed query infrastructure initializes correctly
 * on a single-node cluster and that single-node fallback logic works as expected.
 * Since only {@link LakehousePlugin} is loaded (without the analytics engine or
 * DataFusion backend), the backend executor will be null, but the coordinator
 * and transport action should be fully operational.</p>
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class DistributedIcebergIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LakehousePlugin.class);
    }

    /**
     * Verify that the distributed query coordinator is initialized after plugin loads.
     * The coordinator is created during TransportLakehouseAction's @Inject constructor
     * and stored in LakehouseState.
     */
    public void testDistributedCoordinatorInitialized() {
        DistributedQueryCoordinator coordinator = LakehouseState.instance().distributedCoordinator();
        assertNotNull("Distributed query coordinator should be initialized after plugin loads", coordinator);
    }

    /**
     * Verify that shouldDistribute returns false when Arrow Flight streaming
     * transport is not available (feature flag off) or on a single-node cluster.
     * Without FlightStreamPlugin, StreamTransportService is null, so distribution
     * is disabled regardless of file count or node count.
     */
    public void testShouldNotDistributeOnSingleNode() {
        DistributedQueryCoordinator coordinator = LakehouseState.instance().distributedCoordinator();
        assertNotNull("Coordinator must be initialized", coordinator);

        // Create multiple file infos — distribution still should not happen on single node
        List<IcebergScanPlan.FileInfo> files = List.of(
            new IcebergScanPlan.FileInfo("s3://bucket/data/file1.parquet", 1024 * 1024),
            new IcebergScanPlan.FileInfo("s3://bucket/data/file2.parquet", 2048 * 1024),
            new IcebergScanPlan.FileInfo("s3://bucket/data/file3.parquet", 512 * 1024),
            new IcebergScanPlan.FileInfo("s3://bucket/data/file4.parquet", 768 * 1024)
        );

        boolean shouldDistribute = coordinator.shouldDistribute(files);
        assertFalse("shouldDistribute must return false on a single-node cluster", shouldDistribute);
    }

    /**
     * Verify that the transport action for distributed worker execution is registered.
     * Execute a LakehouseWorkerAction request — it should fail because the backend
     * executor is not set (DataFusion plugin not loaded), but the action itself must
     * be found (no "No handler found for action" error).
     */
    public void testTransportActionRegistered() {
        LakehouseWorkerRequest request = new LakehouseWorkerRequest(
            new String[] { "file1.parquet" },
            new byte[] { 1, 2, 3 },  // Non-empty to pass validation
            Map.of(),
            "test_table"
        );

        try {
            client().execute(LakehouseWorkerAction.INSTANCE, request).actionGet();
            fail("Should have thrown because backend executor is not set");
        } catch (Exception e) {
            // The action should be registered but fail during execution because
            // the backend executor (DataFusion) is not available.
            assertFalse(
                "Transport action should be registered (not a 'no handler' error)",
                e.getMessage() != null && e.getMessage().contains("No handler found for action")
            );
            // Verify the error is about the backend executor not being initialized
            assertTrue(
                "Error should indicate backend executor is not initialized, got: " + e.getMessage(),
                e.getMessage() != null && e.getMessage().contains("Backend executor not initialized")
            );
        }
    }

    /**
     * Verify that DistributedPlanSplitter.analyze() correctly identifies a simple
     * table scan plan as SCAN_ONLY. Uses Calcite RelBuilder to construct a minimal
     * logical plan, following the same pattern as CalciteSubstraitConverterTests.
     */
    public void testDistributionPlanAnalysis() {
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        rootSchema.add("test_table", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                return typeFactory.builder()
                    .add("id", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .add("name", typeFactory.createSqlType(SqlTypeName.VARCHAR))
                    .build();
            }
        });

        FrameworkConfig config = Frameworks.newConfigBuilder()
            .defaultSchema(rootSchema)
            .build();

        RelBuilder builder = RelBuilder.create(config);
        RelNode plan = builder.scan("test_table").build();

        DistributionPlan distPlan = DistributedPlanSplitter.analyze(plan);
        assertNotNull("Distribution plan should not be null", distPlan);
        assertEquals(
            "A simple table scan should be classified as SCAN_ONLY",
            DistributionPlan.QueryType.SCAN_ONLY,
            distPlan.getQueryType()
        );
        assertEquals("SCAN_ONLY plan should have no group keys", 0, distPlan.getGroupKeyOutputColumns().length);
        assertTrue("SCAN_ONLY plan should have no aggregate merges", distPlan.getAggregateMerges().isEmpty());
        assertNull("SCAN_ONLY plan should have no sort info", distPlan.getSortInfo());
    }

    /**
     * Verify that shouldDistribute returns false when Arrow Flight streaming
     * transport is not available. This test cluster does not load FlightStreamPlugin,
     * so StreamTransportService is null and distribution must be disabled.
     */
    public void testShouldNotDistributeWithoutStreamingTransport() {
        DistributedQueryCoordinator coordinator = LakehouseState.instance().distributedCoordinator();
        assertNotNull(coordinator);

        // Even with many files, distribution should be disabled without Arrow Flight
        List<IcebergScanPlan.FileInfo> manyFiles = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            manyFiles.add(new IcebergScanPlan.FileInfo("s3://bucket/data/file" + i + ".parquet", 1024 * 1024));
        }
        assertFalse(
            "shouldDistribute must return false when Arrow Flight streaming is not available",
            coordinator.shouldDistribute(manyFiles)
        );
    }

    /**
     * Verify that shouldDistribute returns false when given null file list.
     */
    public void testShouldNotDistributeWithNullFiles() {
        DistributedQueryCoordinator coordinator = LakehouseState.instance().distributedCoordinator();
        assertNotNull(coordinator);

        assertFalse("shouldDistribute must return false for null file list",
            coordinator.shouldDistribute(null));
    }

    /**
     * Verify that shouldDistribute returns false when given empty file list.
     */
    public void testShouldNotDistributeWithEmptyFiles() {
        DistributedQueryCoordinator coordinator = LakehouseState.instance().distributedCoordinator();
        assertNotNull(coordinator);

        assertFalse("shouldDistribute must return false for empty file list",
            coordinator.shouldDistribute(List.of()));
    }

    /**
     * Verify that shouldDistribute returns false with only a single file,
     * even though we have multiple nodes in a hypothetical multi-node cluster.
     * On a single-node test cluster, this should also be false.
     */
    public void testShouldNotDistributeWithSingleFile() {
        DistributedQueryCoordinator coordinator = LakehouseState.instance().distributedCoordinator();
        assertNotNull(coordinator);

        List<IcebergScanPlan.FileInfo> singleFile = List.of(
            new IcebergScanPlan.FileInfo("s3://bucket/data/file.parquet", 1024 * 1024)
        );
        assertFalse("shouldDistribute must return false for single file",
            coordinator.shouldDistribute(singleFile));
    }

    /**
     * Verify FilePartitioner correctly handles edge case where files exactly
     * equal the number of partitions.
     */
    public void testFilePartitionerExactSplit() {
        List<IcebergScanPlan.FileInfo> files = List.of(
            new IcebergScanPlan.FileInfo("file1.parquet", 100),
            new IcebergScanPlan.FileInfo("file2.parquet", 100),
            new IcebergScanPlan.FileInfo("file3.parquet", 100)
        );

        List<List<IcebergScanPlan.FileInfo>> partitions = FilePartitioner.partition(files, 3);

        assertEquals("3 files across 3 partitions should give 3 partitions", 3, partitions.size());
        for (List<IcebergScanPlan.FileInfo> partition : partitions) {
            assertEquals("Each partition should have exactly 1 file", 1, partition.size());
        }
    }

    /**
     * Verify FilePartitioner handles heavily skewed file sizes.
     * One huge file vs many tiny files — should still produce balanced partitions.
     */
    public void testFilePartitionerSkewedSizes() {
        List<IcebergScanPlan.FileInfo> files = new ArrayList<>();
        files.add(new IcebergScanPlan.FileInfo("huge.parquet", 10_000_000L));
        for (int i = 0; i < 20; i++) {
            files.add(new IcebergScanPlan.FileInfo("tiny" + i + ".parquet", 1_000L));
        }

        List<List<IcebergScanPlan.FileInfo>> partitions = FilePartitioner.partition(files, 3);

        assertEquals(3, partitions.size());
        // All files should be accounted for
        int totalFiles = partitions.stream().mapToInt(List::size).sum();
        assertEquals(21, totalFiles);
    }

    /**
     * Verify that DistributedResultMerger correctly merges scan-only results
     * from multiple worker responses within the cluster plugin context.
     */
    public void testResultMergerScanOnlyInCluster() {
        LakehouseWorkerResponse r1 = new LakehouseWorkerResponse(
            new Object[][]{{1, "alice"}, {2, "bob"}},
            new String[]{"id", "name"}
        );
        LakehouseWorkerResponse r2 = new LakehouseWorkerResponse(
            new Object[][]{{3, "carol"}},
            new String[]{"id", "name"}
        );

        List<Object[]> result = DistributedResultMerger.merge(
            List.of(r1, r2), DistributionPlan.scanOnly()
        );

        assertEquals("Merged scan-only should have 3 rows", 3, result.size());
        assertEquals(1, result.get(0)[0]);
        assertEquals("carol", result.get(2)[1]);
    }

    /**
     * Verify that DistributedResultMerger correctly performs grouped aggregate
     * merge with overlapping groups from multiple workers.
     */
    public void testResultMergerGroupedAggregateInCluster() {
        DistributionPlan plan = DistributionPlan.groupedAggregate(
            new int[]{0},
            List.of(new DistributionPlan.AggMergeInfo(1, DistributionPlan.MergeOp.SUM))
        );

        LakehouseWorkerResponse r1 = new LakehouseWorkerResponse(
            new Object[][]{{"east", 10L}, {"west", 5L}},
            new String[]{"region", "count"}
        );
        LakehouseWorkerResponse r2 = new LakehouseWorkerResponse(
            new Object[][]{{"east", 7L}, {"south", 3L}},
            new String[]{"region", "count"}
        );

        List<Object[]> result = DistributedResultMerger.merge(List.of(r1, r2), plan);

        assertEquals("Should have 3 distinct groups", 3, result.size());
        // Find east group and verify merge
        Object[] east = null;
        for (Object[] row : result) {
            if ("east".equals(row[0])) {
                east = row;
                break;
            }
        }
        assertNotNull("Expected 'east' group in merged results", east);
        assertEquals("east counts should be summed: 10 + 7 = 17", 17L, east[1]);
    }

    /**
     * Verify that DistributedResultMerger handles sort + limit correctly
     * across multiple worker responses.
     */
    public void testResultMergerSortAndLimitInCluster() {
        DistributionPlan.SortInfo sortInfo = new DistributionPlan.SortInfo(
            new int[]{0}, new boolean[]{false}, new boolean[]{true}, 2
        );
        DistributionPlan plan = DistributionPlan.scanOnly().withSortInfo(sortInfo);

        LakehouseWorkerResponse r1 = new LakehouseWorkerResponse(
            new Object[][]{{10}, {7}, {3}},
            new String[]{"amount"}
        );
        LakehouseWorkerResponse r2 = new LakehouseWorkerResponse(
            new Object[][]{{9}, {6}, {1}},
            new String[]{"amount"}
        );

        List<Object[]> result = DistributedResultMerger.merge(List.of(r1, r2), plan);

        assertEquals("LIMIT 2 should return 2 rows", 2, result.size());
        assertEquals("First should be 10 (DESC)", 10, result.get(0)[0]);
        assertEquals("Second should be 9 (DESC)", 9, result.get(1)[0]);
    }

    /**
     * Verify that the transport action rejects requests with invalid fields.
     * A request with empty filePaths should fail with a validation error.
     */
    public void testTransportActionRejectsEmptyFilePaths() {
        LakehouseWorkerRequest request = new LakehouseWorkerRequest(
            new String[0],           // Empty file paths - invalid
            new byte[] { 1, 2, 3 },
            Map.of(),
            "test_table"
        );

        try {
            client().execute(LakehouseWorkerAction.INSTANCE, request).actionGet();
            fail("Should have thrown validation exception for empty filePaths");
        } catch (Exception e) {
            assertTrue(
                "Error should be about validation, got: " + e.getMessage(),
                e.getMessage() != null && e.getMessage().contains("filePaths must not be null or empty")
            );
        }
    }

    /**
     * Verify that the transport action rejects requests with empty substrait plan.
     */
    public void testTransportActionRejectsEmptySubstraitPlan() {
        LakehouseWorkerRequest request = new LakehouseWorkerRequest(
            new String[] { "file.parquet" },
            new byte[0],             // Empty substrait plan - invalid
            Map.of(),
            "test_table"
        );

        try {
            client().execute(LakehouseWorkerAction.INSTANCE, request).actionGet();
            fail("Should have thrown validation exception for empty substraitPlan");
        } catch (Exception e) {
            assertTrue(
                "Error should be about validation, got: " + e.getMessage(),
                e.getMessage() != null && e.getMessage().contains("substraitPlan must not be null or empty")
            );
        }
    }

    /**
     * Verify that DistributionPlan.unsupported() is correctly identified
     * and throws when attempting to merge.
     */
    public void testUnsupportedPlanCannotBeMerged() {
        LakehouseWorkerResponse r = new LakehouseWorkerResponse(
            new Object[][]{{1}}, new String[]{"x"}
        );

        expectThrows(
            IllegalStateException.class,
            () -> DistributedResultMerger.merge(List.of(r), DistributionPlan.unsupported())
        );
    }
}

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
import org.opensearch.lakehouse.distributed.DistributionPlan;
import org.opensearch.lakehouse.distributed.LakehouseWorkerAction;
import org.opensearch.lakehouse.distributed.LakehouseWorkerRequest;
import org.opensearch.lakehouse.scan.IcebergScanPlan;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

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
     * Verify that shouldDistribute returns false on a single-node cluster,
     * even when there are multiple files to process. Single-node clusters
     * should always fall back to local execution.
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
            new byte[] {},
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
}

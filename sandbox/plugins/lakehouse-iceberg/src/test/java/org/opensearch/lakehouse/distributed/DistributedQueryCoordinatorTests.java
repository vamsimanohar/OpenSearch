/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.opensearch.lakehouse.scan.IcebergScanPlan;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link DistributedQueryCoordinator}.
 * Tests the shouldDistribute() decision logic without requiring a real cluster.
 */
public class DistributedQueryCoordinatorTests extends OpenSearchTestCase {

    /**
     * When StreamTransportService is null (Arrow Flight not available),
     * shouldDistribute must always return false regardless of file count.
     */
    public void testShouldNotDistributeWithNullStreamTransport() {
        DistributedQueryCoordinator coordinator = new DistributedQueryCoordinator(null, null);

        List<IcebergScanPlan.FileInfo> files = List.of(
            new IcebergScanPlan.FileInfo("file1.parquet", 1024),
            new IcebergScanPlan.FileInfo("file2.parquet", 1024),
            new IcebergScanPlan.FileInfo("file3.parquet", 1024)
        );

        assertFalse("shouldDistribute must return false when StreamTransportService is null",
            coordinator.shouldDistribute(files));
    }

    /**
     * shouldDistribute returns false for null file list even with null transport.
     */
    public void testShouldNotDistributeWithNullFiles() {
        DistributedQueryCoordinator coordinator = new DistributedQueryCoordinator(null, null);
        assertFalse(coordinator.shouldDistribute(null));
    }

    /**
     * shouldDistribute returns false for empty file list even with null transport.
     */
    public void testShouldNotDistributeWithEmptyFiles() {
        DistributedQueryCoordinator coordinator = new DistributedQueryCoordinator(null, null);
        assertFalse(coordinator.shouldDistribute(List.of()));
    }

    /**
     * shouldDistribute returns false for single file even with null transport.
     */
    public void testShouldNotDistributeWithSingleFile() {
        DistributedQueryCoordinator coordinator = new DistributedQueryCoordinator(null, null);
        assertFalse(coordinator.shouldDistribute(
            List.of(new IcebergScanPlan.FileInfo("file.parquet", 1024))
        ));
    }
}

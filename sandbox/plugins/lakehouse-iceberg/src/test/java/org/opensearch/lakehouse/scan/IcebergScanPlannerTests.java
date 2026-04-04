/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.scan;

import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Tests for {@link IcebergScanPlan} data class and {@link IcebergScanPlanner}.
 *
 * <p>Full integration testing of {@link IcebergScanPlanner#planScan} with a real Iceberg table
 * requires either a HadoopCatalog or mocking the complex Iceberg scan chain and is deferred
 * to integration test tasks.</p>
 */
public class IcebergScanPlannerTests extends OpenSearchTestCase {

    public void testScanPlanWithNoFilterReturnsAllFiles() {
        var files = List.of(
            new IcebergScanPlan.FileInfo("s3://bucket/file1.parquet", 1000),
            new IcebergScanPlan.FileInfo("s3://bucket/file2.parquet", 2000),
            new IcebergScanPlan.FileInfo("s3://bucket/file3.parquet", 3000)
        );
        var plan = new IcebergScanPlan(files, List.of("id", "name"));

        assertEquals(3, plan.fileCount());
        assertEquals(6000L, plan.getTotalFileSize());
        assertEquals(3, plan.getDataFilePaths().size());
        assertEquals("s3://bucket/file1.parquet", plan.getDataFilePaths().get(0));
    }

    public void testScanPlanEmptyFiles() {
        var plan = new IcebergScanPlan(List.of(), List.of());
        assertEquals(0, plan.fileCount());
        assertEquals(0L, plan.getTotalFileSize());
    }

    public void testScanPlanProjectedColumns() {
        var files = List.of(
            new IcebergScanPlan.FileInfo("s3://bucket/data.parquet", 500)
        );
        var plan = new IcebergScanPlan(files, List.of("col1", "col2", "col3"));

        assertEquals(List.of("col1", "col2", "col3"), plan.getProjectedColumns());
    }

    public void testFileInfoAccessors() {
        var fileInfo = new IcebergScanPlan.FileInfo("s3://bucket/test.parquet", 4096);

        assertEquals("s3://bucket/test.parquet", fileInfo.getPath());
        assertEquals(4096L, fileInfo.getFileSizeInBytes());
    }

    public void testGetDataFilePathsPreservesOrder() {
        var files = List.of(
            new IcebergScanPlan.FileInfo("s3://bucket/a.parquet", 100),
            new IcebergScanPlan.FileInfo("s3://bucket/b.parquet", 200),
            new IcebergScanPlan.FileInfo("s3://bucket/c.parquet", 300)
        );
        var plan = new IcebergScanPlan(files, List.of());

        assertEquals(List.of("s3://bucket/a.parquet", "s3://bucket/b.parquet", "s3://bucket/c.parquet"), plan.getDataFilePaths());
    }
}

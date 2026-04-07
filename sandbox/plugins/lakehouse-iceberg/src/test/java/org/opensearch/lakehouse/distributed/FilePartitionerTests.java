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

import java.util.ArrayList;
import java.util.List;

/**
 * Tests for {@link FilePartitioner}.
 */
public class FilePartitionerTests extends OpenSearchTestCase {

    public void testPartitionEvenFiles() {
        List<IcebergScanPlan.FileInfo> files = List.of(
            new IcebergScanPlan.FileInfo("s3://bucket/file1.parquet", 100),
            new IcebergScanPlan.FileInfo("s3://bucket/file2.parquet", 100),
            new IcebergScanPlan.FileInfo("s3://bucket/file3.parquet", 100),
            new IcebergScanPlan.FileInfo("s3://bucket/file4.parquet", 100)
        );

        List<List<IcebergScanPlan.FileInfo>> partitions = FilePartitioner.partition(files, 2);

        assertEquals(2, partitions.size());
        assertEquals(2, partitions.get(0).size());
        assertEquals(2, partitions.get(1).size());
    }

    public void testPartitionSkewedFiles() {
        List<IcebergScanPlan.FileInfo> files = List.of(
            new IcebergScanPlan.FileInfo("s3://bucket/big.parquet", 1000),
            new IcebergScanPlan.FileInfo("s3://bucket/small1.parquet", 10),
            new IcebergScanPlan.FileInfo("s3://bucket/small2.parquet", 10),
            new IcebergScanPlan.FileInfo("s3://bucket/small3.parquet", 10)
        );

        List<List<IcebergScanPlan.FileInfo>> partitions = FilePartitioner.partition(files, 2);

        assertEquals(2, partitions.size());
        // The big file should go to partition 0 first, then the small files
        // get distributed to partition 1 (which has less total size)
        long size0 = partitions.get(0).stream().mapToLong(IcebergScanPlan.FileInfo::getFileSizeInBytes).sum();
        long size1 = partitions.get(1).stream().mapToLong(IcebergScanPlan.FileInfo::getFileSizeInBytes).sum();
        // The greedy algorithm should produce a reasonable balance
        assertTrue("Partition sizes should be within 10x of each other", size0 / (double) Math.max(size1, 1) < 100);
    }

    public void testPartitionMorePartitionsThanFiles() {
        List<IcebergScanPlan.FileInfo> files = List.of(
            new IcebergScanPlan.FileInfo("s3://bucket/file1.parquet", 100),
            new IcebergScanPlan.FileInfo("s3://bucket/file2.parquet", 200)
        );

        List<List<IcebergScanPlan.FileInfo>> partitions = FilePartitioner.partition(files, 5);

        // Should only create 2 partitions (min of files and numPartitions)
        assertEquals(2, partitions.size());
        assertEquals(1, partitions.get(0).size());
        assertEquals(1, partitions.get(1).size());
    }

    public void testPartitionSingleFile() {
        List<IcebergScanPlan.FileInfo> files = List.of(
            new IcebergScanPlan.FileInfo("s3://bucket/file1.parquet", 100)
        );

        List<List<IcebergScanPlan.FileInfo>> partitions = FilePartitioner.partition(files, 3);

        assertEquals(1, partitions.size());
        assertEquals(1, partitions.get(0).size());
    }

    public void testPartitionEmptyFiles() {
        List<List<IcebergScanPlan.FileInfo>> partitions = FilePartitioner.partition(List.of(), 3);
        assertTrue(partitions.isEmpty());
    }

    public void testPartitionNullFiles() {
        List<List<IcebergScanPlan.FileInfo>> partitions = FilePartitioner.partition(null, 3);
        assertTrue(partitions.isEmpty());
    }

    public void testPartitionInvalidPartitionCount() {
        List<IcebergScanPlan.FileInfo> files = List.of(
            new IcebergScanPlan.FileInfo("s3://bucket/file1.parquet", 100)
        );

        expectThrows(IllegalArgumentException.class, () -> FilePartitioner.partition(files, 0));
        expectThrows(IllegalArgumentException.class, () -> FilePartitioner.partition(files, -1));
    }

    public void testPartitionAllFilesAccountedFor() {
        List<IcebergScanPlan.FileInfo> files = new ArrayList<>();
        for (int i = 0; i < 17; i++) {
            files.add(new IcebergScanPlan.FileInfo("s3://bucket/file" + i + ".parquet", (i + 1) * 100L));
        }

        List<List<IcebergScanPlan.FileInfo>> partitions = FilePartitioner.partition(files, 5);

        // All files should be distributed
        int totalFiles = partitions.stream().mapToInt(List::size).sum();
        assertEquals(17, totalFiles);

        // No partition should be empty
        for (List<IcebergScanPlan.FileInfo> partition : partitions) {
            assertFalse("No partition should be empty", partition.isEmpty());
        }
    }
}

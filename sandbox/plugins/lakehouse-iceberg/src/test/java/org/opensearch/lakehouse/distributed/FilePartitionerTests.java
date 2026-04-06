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

    public void testSingleFileSinglePartition() {
        var files = List.of(fi("file1.parquet", 1000));
        var result = FilePartitioner.partition(files, 1);

        assertEquals(1, result.size());
        assertEquals(1, result.get(0).size());
        assertEquals("file1.parquet", result.get(0).get(0).getPath());
    }

    public void testMultipleFilesSinglePartition() {
        var files = List.of(
            fi("a.parquet", 100),
            fi("b.parquet", 200),
            fi("c.parquet", 300)
        );
        var result = FilePartitioner.partition(files, 1);

        assertEquals(1, result.size());
        assertEquals(3, result.get(0).size());
    }

    public void testEvenSplitEqualSizes() {
        var files = List.of(
            fi("f1.parquet", 500),
            fi("f2.parquet", 500),
            fi("f3.parquet", 500),
            fi("f4.parquet", 500)
        );
        var result = FilePartitioner.partition(files, 2);

        assertEquals(2, result.size());
        assertEquals(2, result.get(0).size());
        assertEquals(2, result.get(1).size());
    }

    public void testSkewedFiles() {
        // 1 large file (1 GB) + 10 small files (100 MB each)
        var files = new ArrayList<IcebergScanPlan.FileInfo>();
        files.add(fi("big.parquet", 1_000_000_000L));
        for (int i = 0; i < 10; i++) {
            files.add(fi("small" + i + ".parquet", 100_000_000L));
        }

        var result = FilePartitioner.partition(files, 3);
        assertEquals(3, result.size());

        // Verify all files are assigned
        int totalFiles = result.stream().mapToInt(List::size).sum();
        assertEquals(11, totalFiles);

        // Verify size balance: no partition should have more than twice the average
        long totalSize = 2_000_000_000L; // 1GB + 10*100MB
        long avgSize = totalSize / 3;
        for (var partition : result) {
            long partitionSize = partition.stream().mapToLong(IcebergScanPlan.FileInfo::getFileSizeInBytes).sum();
            assertTrue(
                "Partition size " + partitionSize + " exceeds 2x average " + avgSize,
                partitionSize <= avgSize * 2
            );
        }
    }

    public void testMorePartitionsThanFiles() {
        var files = List.of(
            fi("a.parquet", 100),
            fi("b.parquet", 200),
            fi("c.parquet", 300)
        );
        var result = FilePartitioner.partition(files, 5);

        // Should produce only 3 partitions (no empty ones)
        assertEquals(3, result.size());
        int totalFiles = result.stream().mapToInt(List::size).sum();
        assertEquals(3, totalFiles);
    }

    public void testZeroFiles() {
        var result = FilePartitioner.partition(List.of(), 3);
        assertTrue(result.isEmpty());
    }

    public void testNullFiles() {
        var result = FilePartitioner.partition(null, 3);
        assertTrue(result.isEmpty());
    }

    public void testInvalidPartitionCountZero() {
        expectThrows(IllegalArgumentException.class, () -> FilePartitioner.partition(List.of(), 0));
    }

    public void testInvalidPartitionCountNegative() {
        expectThrows(IllegalArgumentException.class, () -> FilePartitioner.partition(List.of(), -1));
    }

    public void testAllFilesSameSize() {
        var files = List.of(
            fi("f1.parquet", 1000),
            fi("f2.parquet", 1000),
            fi("f3.parquet", 1000),
            fi("f4.parquet", 1000),
            fi("f5.parquet", 1000),
            fi("f6.parquet", 1000)
        );
        var result = FilePartitioner.partition(files, 3);

        assertEquals(3, result.size());
        // With equal sizes, greedy assignment distributes round-robin: 2 files per partition
        for (var partition : result) {
            assertEquals(2, partition.size());
        }
    }

    public void testAllFilesPresent() {
        var files = List.of(
            fi("a.parquet", 300),
            fi("b.parquet", 100),
            fi("c.parquet", 200),
            fi("d.parquet", 400),
            fi("e.parquet", 150)
        );
        var result = FilePartitioner.partition(files, 2);

        // Collect all paths from the result
        var allPaths = result.stream()
            .flatMap(List::stream)
            .map(IcebergScanPlan.FileInfo::getPath)
            .sorted()
            .toList();

        assertEquals(List.of("a.parquet", "b.parquet", "c.parquet", "d.parquet", "e.parquet"), allPaths);
    }

    private static IcebergScanPlan.FileInfo fi(String path, long size) {
        return new IcebergScanPlan.FileInfo(path, size);
    }
}

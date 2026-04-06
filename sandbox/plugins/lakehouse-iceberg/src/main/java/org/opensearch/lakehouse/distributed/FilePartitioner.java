/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.opensearch.lakehouse.scan.IcebergScanPlan;

import java.util.ArrayList;
import java.util.List;

/**
 * Partitions a list of Iceberg data files across N worker nodes using
 * size-balanced greedy assignment. Each file is assigned to the partition
 * with the smallest current total size, producing a balanced distribution.
 */
public class FilePartitioner {

    private FilePartitioner() {
        // utility class
    }

    /**
     * Partitions files across the given number of partitions using greedy
     * size-balanced assignment.
     *
     * @param files         the list of files to partition
     * @param numPartitions the desired number of partitions (must be positive)
     * @return a list of partitions, each containing a subset of files
     * @throws IllegalArgumentException if numPartitions is not positive
     */
    public static List<List<IcebergScanPlan.FileInfo>> partition(List<IcebergScanPlan.FileInfo> files, int numPartitions) {
        if (numPartitions <= 0) {
            throw new IllegalArgumentException("numPartitions must be positive, got: " + numPartitions);
        }
        if (files == null || files.isEmpty()) {
            return List.of();
        }

        int actualPartitions = Math.min(numPartitions, files.size());

        List<List<IcebergScanPlan.FileInfo>> partitions = new ArrayList<>(actualPartitions);
        long[] partitionSizes = new long[actualPartitions];
        for (int i = 0; i < actualPartitions; i++) {
            partitions.add(new ArrayList<>());
        }

        for (IcebergScanPlan.FileInfo file : files) {
            int smallest = indexOfMin(partitionSizes);
            partitions.get(smallest).add(file);
            partitionSizes[smallest] += file.getFileSizeInBytes();
        }

        return partitions;
    }

    /** Returns the index of the minimum value in the array. */
    private static int indexOfMin(long[] values) {
        int minIndex = 0;
        for (int i = 1; i < values.length; i++) {
            if (values[i] < values[minIndex]) {
                minIndex = i;
            }
        }
        return minIndex;
    }
}

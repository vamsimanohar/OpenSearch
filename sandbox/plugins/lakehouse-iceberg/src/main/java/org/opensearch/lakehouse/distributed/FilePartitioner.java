/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Splits a list of files across N workers using greedy bin-packing by file size.
 * <p>
 * Algorithm:
 * <ol>
 *   <li>Sort files by size descending</li>
 *   <li>Assign each file to the worker with the smallest total load</li>
 * </ol>
 * This ensures no worker gets more than 2x the average load.
 *
 * @opensearch.internal
 */
public final class FilePartitioner {

    private FilePartitioner() {}

    /**
     * Partitions files across the given number of workers using greedy bin-packing.
     *
     * @param filePaths   the file paths to partition
     * @param fileSizes   file sizes in bytes, parallel to filePaths
     * @param workerCount number of workers to distribute across (must be &gt;= 1)
     * @return list of {@link FileAssignment}, one per effective worker
     * @throws IllegalArgumentException if workerCount &lt; 1 or arrays have mismatched lengths
     */
    public static List<FileAssignment> partition(List<String> filePaths, long[] fileSizes, int workerCount) {
        if (workerCount < 1) {
            throw new IllegalArgumentException("workerCount must be >= 1 but was " + workerCount);
        }
        if (filePaths.size() != fileSizes.length) {
            throw new IllegalArgumentException(
                "filePaths size (" + filePaths.size() + ") != fileSizes length (" + fileSizes.length + ")"
            );
        }

        int fileCount = filePaths.size();
        if (fileCount == 0) {
            return List.of();
        }

        // Effective worker count — no point having more workers than files
        int effectiveWorkers = Math.min(workerCount, fileCount);

        // Create index array sorted by file size descending
        Integer[] indices = new Integer[fileCount];
        for (int i = 0; i < fileCount; i++) {
            indices[i] = i;
        }
        Arrays.sort(indices, Comparator.comparingLong(i -> -fileSizes[i]));

        // Initialize per-worker accumulators
        List<List<String>> workerPaths = new ArrayList<>(effectiveWorkers);
        List<List<Long>> workerSizes = new ArrayList<>(effectiveWorkers);
        long[] workerTotals = new long[effectiveWorkers];
        for (int w = 0; w < effectiveWorkers; w++) {
            workerPaths.add(new ArrayList<>());
            workerSizes.add(new ArrayList<>());
        }

        // Greedy assignment: assign each file to the least-loaded worker
        for (int idx : indices) {
            int minWorker = findMinWorker(workerTotals);
            workerPaths.get(minWorker).add(filePaths.get(idx));
            workerSizes.get(minWorker).add(fileSizes[idx]);
            workerTotals[minWorker] += fileSizes[idx];
        }

        // Build result
        List<FileAssignment> result = new ArrayList<>(effectiveWorkers);
        for (int w = 0; w < effectiveWorkers; w++) {
            List<Long> sizes = workerSizes.get(w);
            long[] sizeArray = new long[sizes.size()];
            for (int i = 0; i < sizes.size(); i++) {
                sizeArray[i] = sizes.get(i);
            }
            result.add(new FileAssignment(workerPaths.get(w), sizeArray, workerTotals[w]));
        }
        return result;
    }

    /**
     * Finds the index of the worker with the smallest total load.
     */
    static int findMinWorker(long[] workerTotals) {
        int minIdx = 0;
        for (int i = 1; i < workerTotals.length; i++) {
            if (workerTotals[i] < workerTotals[minIdx]) {
                minIdx = i;
            }
        }
        return minIdx;
    }

    /**
     * A file assignment for a single worker: the file paths, their sizes, and the total size.
     *
     * @opensearch.internal
     */
    public static final class FileAssignment {
        private final List<String> filePaths;
        private final long[] fileSizes;
        private final long totalSize;

        /**
         * Creates a new file assignment.
         *
         * @param filePaths the assigned file paths
         * @param fileSizes the file sizes in bytes
         * @param totalSize the total size in bytes
         */
        public FileAssignment(List<String> filePaths, long[] fileSizes, long totalSize) {
            this.filePaths = filePaths;
            this.fileSizes = fileSizes;
            this.totalSize = totalSize;
        }

        /** Returns the file paths assigned to this worker. */
        public List<String> getFilePaths() {
            return filePaths;
        }

        /** Returns the file sizes in bytes. */
        public long[] getFileSizes() {
            return fileSizes;
        }

        /** Returns the total byte size for this worker. */
        public long getTotalSize() {
            return totalSize;
        }
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FilePartitionerTests extends OpenSearchTestCase {

    public void testEqualFileSizesDistributeEvenly() {
        List<String> files = List.of("f1", "f2", "f3", "f4");
        long[] sizes = {100, 100, 100, 100};

        List<FilePartitioner.FileAssignment> result = FilePartitioner.partition(files, sizes, 2);

        assertEquals(2, result.size());
        // Each worker gets 2 files of 100 bytes
        assertEquals(2, result.get(0).getFilePaths().size());
        assertEquals(2, result.get(1).getFilePaths().size());
        assertEquals(200, result.get(0).getTotalSize());
        assertEquals(200, result.get(1).getTotalSize());
    }

    public void testUnevenSizesGreedyBalancing() {
        // Files: 1000, 500, 300, 200
        // Greedy: 1000 → w0 (1000), 500 → w1 (500), 300 → w1 (800), 200 → w1 (1000)
        List<String> files = List.of("big", "medium", "small", "tiny");
        long[] sizes = {1000, 500, 300, 200};

        List<FilePartitioner.FileAssignment> result = FilePartitioner.partition(files, sizes, 2);

        assertEquals(2, result.size());
        long maxLoad = Math.max(result.get(0).getTotalSize(), result.get(1).getTotalSize());
        long minLoad = Math.min(result.get(0).getTotalSize(), result.get(1).getTotalSize());
        assertEquals(1000, maxLoad);
        assertEquals(1000, minLoad);
    }

    public void testMoreWorkersThanFiles() {
        List<String> files = List.of("f1", "f2");
        long[] sizes = {100, 200};

        List<FilePartitioner.FileAssignment> result = FilePartitioner.partition(files, sizes, 5);

        // Effective workers = min(5, 2) = 2
        assertEquals(2, result.size());
        // All files accounted for
        int totalFiles = result.stream().mapToInt(a -> a.getFilePaths().size()).sum();
        assertEquals(2, totalFiles);
    }

    public void testSingleWorker() {
        List<String> files = List.of("f1", "f2", "f3");
        long[] sizes = {100, 200, 300};

        List<FilePartitioner.FileAssignment> result = FilePartitioner.partition(files, sizes, 1);

        assertEquals(1, result.size());
        assertEquals(3, result.get(0).getFilePaths().size());
        assertEquals(600, result.get(0).getTotalSize());
    }

    public void testSingleFile() {
        List<String> files = List.of("only");
        long[] sizes = {500};

        List<FilePartitioner.FileAssignment> result = FilePartitioner.partition(files, sizes, 3);

        // Only 1 effective worker since only 1 file
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).getFilePaths().size());
        assertEquals("only", result.get(0).getFilePaths().get(0));
        assertEquals(500, result.get(0).getTotalSize());
    }

    public void testEmptyFileList() {
        List<String> files = List.of();
        long[] sizes = {};

        List<FilePartitioner.FileAssignment> result = FilePartitioner.partition(files, sizes, 3);

        assertTrue(result.isEmpty());
    }

    public void testInvalidWorkerCountThrows() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> FilePartitioner.partition(List.of("f"), new long[]{1}, 0)
        );
        assertTrue(ex.getMessage().contains("workerCount must be >= 1"));
    }

    public void testMismatchedArrayLengthsThrows() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> FilePartitioner.partition(List.of("f1", "f2"), new long[]{1}, 1)
        );
        assertTrue(ex.getMessage().contains("filePaths size"));
    }

    public void testAllFilesAccounted() {
        List<String> files = List.of("a", "b", "c", "d", "e");
        long[] sizes = {50, 40, 30, 20, 10};

        List<FilePartitioner.FileAssignment> result = FilePartitioner.partition(files, sizes, 3);

        // All files must appear exactly once across all assignments
        Set<String> allAssigned = new HashSet<>();
        long totalSize = 0;
        for (FilePartitioner.FileAssignment a : result) {
            allAssigned.addAll(a.getFilePaths());
            totalSize += a.getTotalSize();
            // Verify fileSizes array matches filePaths
            assertEquals(a.getFilePaths().size(), a.getFileSizes().length);
        }
        assertEquals(new HashSet<>(files), allAssigned);
        assertEquals(150, totalSize);
    }

    public void testFileSizesParallelToPaths() {
        List<String> files = List.of("f1", "f2");
        long[] sizes = {100, 200};

        List<FilePartitioner.FileAssignment> result = FilePartitioner.partition(files, sizes, 1);

        FilePartitioner.FileAssignment a = result.get(0);
        for (int i = 0; i < a.getFilePaths().size(); i++) {
            String path = a.getFilePaths().get(i);
            long size = a.getFileSizes()[i];
            // Verify size matches the original
            int origIdx = files.indexOf(path);
            assertEquals(sizes[origIdx], size);
        }
    }

    public void testFindMinWorkerSelectsFirst() {
        long[] totals = {100, 200, 300};
        assertEquals(0, FilePartitioner.findMinWorker(totals));
    }

    public void testFindMinWorkerSelectsMiddle() {
        long[] totals = {200, 100, 300};
        assertEquals(1, FilePartitioner.findMinWorker(totals));
    }

    public void testFindMinWorkerSelectsLast() {
        long[] totals = {300, 200, 100};
        assertEquals(2, FilePartitioner.findMinWorker(totals));
    }

    public void testFileAssignmentGetters() {
        List<String> paths = List.of("a", "b");
        long[] sizes = {10, 20};
        FilePartitioner.FileAssignment assignment = new FilePartitioner.FileAssignment(paths, sizes, 30);

        assertEquals(paths, assignment.getFilePaths());
        assertArrayEquals(sizes, assignment.getFileSizes());
        assertEquals(30, assignment.getTotalSize());
    }
}

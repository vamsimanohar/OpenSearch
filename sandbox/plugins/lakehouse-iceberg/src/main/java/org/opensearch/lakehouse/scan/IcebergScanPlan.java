/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.scan;

import java.util.List;

/**
 * Result of Iceberg scan planning: pruned file paths with metadata.
 */
public class IcebergScanPlan {

    /** Metadata about a single data file in the scan plan. */
    public static class FileInfo {
        private final String path;
        private final long fileSizeInBytes;

        /**
         * Creates file info with the given path and size.
         *
         * @param path            the data file path
         * @param fileSizeInBytes the file size in bytes
         */
        public FileInfo(String path, long fileSizeInBytes) {
            this.path = path;
            this.fileSizeInBytes = fileSizeInBytes;
        }

        /** Returns the data file path. */
        public String getPath() {
            return path;
        }

        /** Returns the file size in bytes. */
        public long getFileSizeInBytes() {
            return fileSizeInBytes;
        }
    }

    private final List<FileInfo> files;
    private final List<String> projectedColumns;

    /**
     * Creates a scan plan with the given files and projected columns.
     *
     * @param files            the pruned list of data files
     * @param projectedColumns the columns to project
     */
    public IcebergScanPlan(List<FileInfo> files, List<String> projectedColumns) {
        this.files = files;
        this.projectedColumns = projectedColumns;
    }

    /** Returns the list of data files in the scan plan. */
    public List<FileInfo> getFiles() {
        return files;
    }

    /** Returns the projected column names. */
    public List<String> getProjectedColumns() {
        return projectedColumns;
    }

    /** Returns just the file paths from the scan plan. */
    public List<String> getDataFilePaths() {
        return files.stream().map(FileInfo::getPath).toList();
    }

    /** Returns the total size of all files in bytes. */
    public long getTotalFileSize() {
        return files.stream().mapToLong(FileInfo::getFileSizeInBytes).sum();
    }

    /** Returns the number of data files in the scan plan. */
    public int fileCount() {
        return files.size();
    }
}

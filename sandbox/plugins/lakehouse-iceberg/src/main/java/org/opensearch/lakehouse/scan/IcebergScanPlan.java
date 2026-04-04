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

    public static class FileInfo {
        private final String path;
        private final long fileSizeInBytes;

        public FileInfo(String path, long fileSizeInBytes) {
            this.path = path;
            this.fileSizeInBytes = fileSizeInBytes;
        }

        public String getPath() {
            return path;
        }

        public long getFileSizeInBytes() {
            return fileSizeInBytes;
        }
    }

    private final List<FileInfo> files;
    private final List<String> projectedColumns;

    public IcebergScanPlan(List<FileInfo> files, List<String> projectedColumns) {
        this.files = files;
        this.projectedColumns = projectedColumns;
    }

    public List<FileInfo> getFiles() {
        return files;
    }

    public List<String> getProjectedColumns() {
        return projectedColumns;
    }

    public List<String> getDataFilePaths() {
        return files.stream().map(FileInfo::getPath).toList();
    }

    public long getTotalFileSize() {
        return files.stream().mapToLong(FileInfo::getFileSizeInBytes).sum();
    }

    public int fileCount() {
        return files.size();
    }
}

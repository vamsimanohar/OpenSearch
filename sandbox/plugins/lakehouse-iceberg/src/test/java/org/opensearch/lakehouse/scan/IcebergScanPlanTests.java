/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.scan;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.List;

public class IcebergScanPlanTests extends OpenSearchTestCase {

    // ── FileInfo tests ──────────────────────────────────────────────────

    public void testFileInfoConstructorAndGetters() {
        IcebergScanPlan.FileInfo info = new IcebergScanPlan.FileInfo("s3://bucket/data/file1.parquet", 1024L);
        assertEquals("s3://bucket/data/file1.parquet", info.getPath());
        assertEquals(1024L, info.getFileSizeInBytes());
    }

    public void testFileInfoZeroSize() {
        IcebergScanPlan.FileInfo info = new IcebergScanPlan.FileInfo("/tmp/empty.parquet", 0L);
        assertEquals("/tmp/empty.parquet", info.getPath());
        assertEquals(0L, info.getFileSizeInBytes());
    }

    // ── IcebergScanPlan constructor / getters ───────────────────────────

    public void testConstructorAndGetters() {
        List<IcebergScanPlan.FileInfo> files = List.of(
            new IcebergScanPlan.FileInfo("file1.parquet", 100L),
            new IcebergScanPlan.FileInfo("file2.parquet", 200L)
        );
        List<String> columns = List.of("id", "name");

        IcebergScanPlan plan = new IcebergScanPlan(files, columns);

        assertEquals(files, plan.getFiles());
        assertEquals(columns, plan.getProjectedColumns());
    }

    // ── getDataFilePaths ────────────────────────────────────────────────

    public void testGetDataFilePaths() {
        List<IcebergScanPlan.FileInfo> files = List.of(
            new IcebergScanPlan.FileInfo("a.parquet", 10L),
            new IcebergScanPlan.FileInfo("b.parquet", 20L),
            new IcebergScanPlan.FileInfo("c.parquet", 30L)
        );
        IcebergScanPlan plan = new IcebergScanPlan(files, List.of());

        List<String> paths = plan.getDataFilePaths();

        assertEquals(3, paths.size());
        assertEquals("a.parquet", paths.get(0));
        assertEquals("b.parquet", paths.get(1));
        assertEquals("c.parquet", paths.get(2));
    }

    // ── getTotalFileSize ────────────────────────────────────────────────

    public void testGetTotalFileSize() {
        List<IcebergScanPlan.FileInfo> files = List.of(
            new IcebergScanPlan.FileInfo("f1.parquet", 100L),
            new IcebergScanPlan.FileInfo("f2.parquet", 250L),
            new IcebergScanPlan.FileInfo("f3.parquet", 50L)
        );
        IcebergScanPlan plan = new IcebergScanPlan(files, List.of("col1"));

        assertEquals(400L, plan.getTotalFileSize());
    }

    // ── fileCount ───────────────────────────────────────────────────────

    public void testFileCount() {
        List<IcebergScanPlan.FileInfo> files = List.of(
            new IcebergScanPlan.FileInfo("x.parquet", 10L),
            new IcebergScanPlan.FileInfo("y.parquet", 20L)
        );
        IcebergScanPlan plan = new IcebergScanPlan(files, List.of());

        assertEquals(2, plan.fileCount());
    }

    // ── Empty plan ──────────────────────────────────────────────────────

    public void testEmptyPlan() {
        IcebergScanPlan plan = new IcebergScanPlan(Collections.emptyList(), Collections.emptyList());

        assertEquals(0, plan.fileCount());
        assertEquals(0L, plan.getTotalFileSize());
        assertTrue(plan.getDataFilePaths().isEmpty());
        assertTrue(plan.getFiles().isEmpty());
        assertTrue(plan.getProjectedColumns().isEmpty());
    }
}

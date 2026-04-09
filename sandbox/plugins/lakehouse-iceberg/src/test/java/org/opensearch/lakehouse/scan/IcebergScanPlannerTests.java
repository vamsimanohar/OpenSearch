/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.scan;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IcebergScanPlannerTests extends OpenSearchTestCase {

    private ExecutorService executor;
    private Table table;
    private TableScan scan;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        executor = Executors.newSingleThreadExecutor();
        table = mock(Table.class);
        scan = mock(TableScan.class);

        // Default chaining: newScan -> filter -> planWith -> planFiles
        when(table.newScan()).thenReturn(scan);
        when(scan.filter(any(Expression.class))).thenReturn(scan);
        when(scan.planWith(any(ExecutorService.class))).thenReturn(scan);
    }

    @Override
    public void tearDown() throws Exception {
        executor.shutdownNow();
        super.tearDown();
    }

    // -- Basic scan with no predicates --

    public void testBasicScanNoPredicates() throws Exception {
        FileScanTask task1 = mockTask("s3://bucket/data/part-001.parquet", 1000L);
        FileScanTask task2 = mockTask("s3://bucket/data/part-002.parquet", 2000L);
        when(scan.planFiles()).thenReturn(closeable(List.of(task1, task2)));

        IcebergScanPlanner planner = new IcebergScanPlanner(executor);
        IcebergScanPlan plan = planner.planScan(table, -1, Collections.emptyList(), null);

        assertEquals(2, plan.fileCount());
        assertEquals(3000L, plan.getTotalFileSize());
        assertEquals("s3://bucket/data/part-001.parquet", plan.getDataFilePaths().get(0));
        assertEquals("s3://bucket/data/part-002.parquet", plan.getDataFilePaths().get(1));
        // null projectedColumns should produce empty list
        assertTrue(plan.getProjectedColumns().isEmpty());

        // Should not call useSnapshot when snapshotId <= 0
        verify(scan, never()).useSnapshot(any(Long.class));
    }

    // -- Scan with snapshot ID --

    public void testScanWithSnapshotId() throws Exception {
        FileScanTask task = mockTask("file.parquet", 500L);
        when(scan.useSnapshot(42L)).thenReturn(scan);
        when(scan.planFiles()).thenReturn(closeable(List.of(task)));

        IcebergScanPlanner planner = new IcebergScanPlanner(executor);
        IcebergScanPlan plan = planner.planScan(table, 42L, Collections.emptyList(), null);

        verify(scan).useSnapshot(42L);
        assertEquals(1, plan.fileCount());
    }

    // -- Scan with predicates --

    public void testScanWithPredicates() throws Exception {
        Expression pred1 = Expressions.greaterThan("id", 100);
        Expression pred2 = Expressions.lessThan("id", 200);
        FileScanTask task = mockTask("filtered.parquet", 750L);
        when(scan.planFiles()).thenReturn(closeable(List.of(task)));

        IcebergScanPlanner planner = new IcebergScanPlanner(executor);
        IcebergScanPlan plan = planner.planScan(table, -1, List.of(pred1, pred2), null);

        // Should have called filter with the AND-combined predicates
        verify(scan).filter(any(Expression.class));
        assertEquals(1, plan.fileCount());
        assertEquals(750L, plan.getTotalFileSize());
    }

    // -- Scan with projected columns --

    @SuppressWarnings("unchecked")
    public void testScanWithProjectedColumns() throws Exception {
        FileScanTask task = mockTask("projected.parquet", 300L);
        when(scan.select(any(java.util.Collection.class))).thenReturn(scan);
        when(scan.planFiles()).thenReturn(closeable(List.of(task)));

        List<String> columns = List.of("id", "name");
        IcebergScanPlanner planner = new IcebergScanPlanner(executor);
        IcebergScanPlan plan = planner.planScan(table, -1, Collections.emptyList(), columns);

        verify(scan).select(columns);
        assertEquals(columns, plan.getProjectedColumns());
        assertEquals(1, plan.fileCount());
    }

    // -- Scan with empty projected columns list (should not call select) --

    @SuppressWarnings("unchecked")
    public void testScanWithEmptyProjectedColumns() throws Exception {
        FileScanTask task = mockTask("full.parquet", 100L);
        when(scan.planFiles()).thenReturn(closeable(List.of(task)));

        IcebergScanPlanner planner = new IcebergScanPlanner(executor);
        IcebergScanPlan plan = planner.planScan(table, -1, Collections.emptyList(), Collections.emptyList());

        verify(scan, never()).select(any(java.util.Collection.class));
        assertTrue(plan.getProjectedColumns().isEmpty());
    }

    // -- IOException wrapping --

    public void testIOExceptionThrowsUncheckedIOException() throws Exception {
        // Simulate IOException on close() via try-with-resources
        CloseableIterable<FileScanTask> closeFailIterable = new CloseableIterable<FileScanTask>() {
            @Override
            public void close() throws IOException {
                throw new IOException("Failed to close scan tasks");
            }

            @Override
            public CloseableIterator<FileScanTask> iterator() {
                return wrapIterator(Collections.<FileScanTask>emptyList().iterator());
            }
        };
        when(scan.planFiles()).thenReturn(closeFailIterable);

        IcebergScanPlanner planner = new IcebergScanPlanner(executor);
        UncheckedIOException ex = expectThrows(
            UncheckedIOException.class,
            () -> planner.planScan(table, -1, Collections.emptyList(), null)
        );
        assertEquals("Failed to plan Iceberg scan", ex.getMessage());
        assertTrue(ex.getCause() instanceof IOException);
    }

    // -- Empty scan results --

    public void testEmptyScanResults() throws Exception {
        when(scan.planFiles()).thenReturn(closeable(Collections.emptyList()));

        IcebergScanPlanner planner = new IcebergScanPlanner(executor);
        IcebergScanPlan plan = planner.planScan(table, -1, Collections.emptyList(), null);

        assertEquals(0, plan.fileCount());
        assertEquals(0L, plan.getTotalFileSize());
        assertTrue(plan.getDataFilePaths().isEmpty());
    }

    // -- Helpers --

    private static FileScanTask mockTask(String path, long size) {
        FileScanTask task = mock(FileScanTask.class);
        DataFile dataFile = mock(DataFile.class);
        when(dataFile.path()).thenReturn(path);
        when(dataFile.fileSizeInBytes()).thenReturn(size);
        when(task.file()).thenReturn(dataFile);
        return task;
    }

    private static <T> CloseableIterable<T> closeable(List<T> items) {
        return new CloseableIterable<T>() {
            @Override
            public void close() {}

            @Override
            public CloseableIterator<T> iterator() {
                return wrapIterator(items.iterator());
            }
        };
    }

    private static <T> CloseableIterator<T> wrapIterator(Iterator<T> delegate) {
        return new CloseableIterator<T>() {
            @Override
            public void close() {}

            @Override
            public boolean hasNext() {
                return delegate.hasNext();
            }

            @Override
            public T next() {
                return delegate.next();
            }
        };
    }
}

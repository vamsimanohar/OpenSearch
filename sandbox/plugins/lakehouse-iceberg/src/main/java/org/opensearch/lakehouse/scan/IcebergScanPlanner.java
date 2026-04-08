/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.scan;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Plans Iceberg table scans with predicate pushdown and parallel manifest fetching.
 */
public class IcebergScanPlanner {

    private final ExecutorService executorService;

    /**
     * Creates a scan planner with the given executor for parallel manifest fetching.
     *
     * @param executorService executor for parallel operations
     */
    public IcebergScanPlanner(ExecutorService executorService) {
        this.executorService = executorService;
    }

    /**
     * Plans a scan against an Iceberg table with predicate pushdown.
     * Uses parallel manifest fetching via planWith(executor).
     *
     * @param table the Iceberg table to scan
     * @param snapshotId the snapshot ID to use, or a value &lt;= 0 to use the current snapshot
     * @param predicates Iceberg filter expressions to push down
     * @param projectedColumns columns to project, or null/empty for all columns
     * @return an {@link IcebergScanPlan} containing the pruned file list
     */
    public IcebergScanPlan planScan(Table table, long snapshotId, List<Expression> predicates, List<String> projectedColumns) {
        Expression combined = predicates.stream()
            .reduce(Expressions::and)
            .orElse(Expressions.alwaysTrue());

        TableScan scan = table.newScan();
        if (snapshotId > 0) {
            scan = scan.useSnapshot(snapshotId);
        }
        scan = scan.filter(combined);

        if (projectedColumns != null && !projectedColumns.isEmpty()) {
            scan = scan.select(projectedColumns);
        }

        // Use parallel manifest fetching
        List<IcebergScanPlan.FileInfo> files = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = scan.planWith(executorService).planFiles()) {
            for (FileScanTask task : tasks) {
                files.add(new IcebergScanPlan.FileInfo(
                    task.file().path().toString(),
                    task.file().fileSizeInBytes()
                ));
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to plan Iceberg scan", e);
        }

        return new IcebergScanPlan(files, projectedColumns != null ? projectedColumns : List.of());
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed.merge;

import org.apache.calcite.sql.SqlKind;
import org.opensearch.lakehouse.distributed.worker.WorkerQueryResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Combines partial {@link WorkerQueryResponse} results from distributed workers into a single
 * merged response according to the given {@link MergeStrategy}.
 * <p>
 * This is a thin dispatcher that routes to the appropriate merge implementation:
 * <ul>
 *   <li><b>CONCAT</b> — concatenate all rows ({@link #mergeConcat})</li>
 *   <li><b>GLOBAL_MERGE</b> — re-aggregate via {@link AggregationReducer}</li>
 *   <li><b>TOPK_MERGE</b> — merge-sort via {@link TopKMerger}</li>
 *   <li><b>SINGLE_NODE</b> — pass through the single worker's response</li>
 * </ul>
 *
 * @opensearch.internal
 */
public final class ResultMerger {

    private ResultMerger() {}

    /**
     * Merges multiple worker responses according to the given strategy.
     *
     * @param responses    the worker responses to merge
     * @param strategy     the merge strategy
     * @param sortColumns  column indices to sort by (for TOPK_MERGE), may be null
     * @param sortAsc      ascending flag for each sort column (for TOPK_MERGE), may be null
     * @param limit        row limit (for TOPK_MERGE), ignored for other strategies
     * @return the merged response
     */
    public static WorkerQueryResponse merge(
        List<WorkerQueryResponse> responses,
        MergeStrategy strategy,
        int[] sortColumns,
        boolean[] sortAsc,
        int limit
    ) {
        return merge(responses, strategy, sortColumns, sortAsc, limit, null);
    }

    /**
     * Merges multiple worker responses with aggregate function metadata.
     *
     * @param responses    the worker responses to merge
     * @param strategy     the merge strategy
     * @param sortColumns  column indices to sort by (for TOPK_MERGE), may be null
     * @param sortAsc      ascending flag for each sort column (for TOPK_MERGE), may be null
     * @param limit        row limit (for TOPK_MERGE), ignored for other strategies
     * @param aggKinds     aggregate function kinds per column (for GLOBAL_MERGE), may be null
     * @return the merged response
     */
    public static WorkerQueryResponse merge(
        List<WorkerQueryResponse> responses,
        MergeStrategy strategy,
        int[] sortColumns,
        boolean[] sortAsc,
        int limit,
        SqlKind[] aggKinds
    ) {
        List<WorkerQueryResponse> nonEmpty = filterNonEmpty(responses);
        if (nonEmpty.isEmpty()) {
            return emptyResponse(responses);
        }

        return switch (strategy) {
            case CONCAT -> mergeConcat(nonEmpty);
            case GLOBAL_MERGE -> mergeGlobal(nonEmpty, aggKinds);
            case TOPK_MERGE -> TopKMerger.merge(nonEmpty, sortColumns, sortAsc, limit);
            case SINGLE_NODE -> nonEmpty.get(0);
        };
    }

    /**
     * Concatenates all rows from all worker responses.
     */
    static WorkerQueryResponse mergeConcat(List<WorkerQueryResponse> responses) {
        WorkerQueryResponse first = responses.get(0);
        List<String> columnNames = first.getColumnNames();
        List<String> columnTypes = first.getColumnTypes();
        int numCols = columnNames.size();

        int totalRows = 0;
        for (WorkerQueryResponse r : responses) {
            totalRows += r.getRowCount();
        }

        Object[][] merged = new Object[numCols][totalRows];
        int offset = 0;
        for (WorkerQueryResponse r : responses) {
            Object[][] data = r.getColumnData();
            for (int col = 0; col < numCols; col++) {
                System.arraycopy(data[col], 0, merged[col], offset, r.getRowCount());
            }
            offset += r.getRowCount();
        }

        return new WorkerQueryResponse(columnNames, columnTypes, totalRows, merged);
    }

    /**
     * Re-aggregates single-row global results. Assumes each worker returns exactly one row.
     * <p>
     * Uses aggregate function kinds to determine merge operation per column:
     * SUM/COUNT -> sum, MIN -> min, MAX -> max. Falls back to sum if aggKinds is null.
     * Delegates column-level operations to {@link AggregationReducer}.
     *
     * @param responses the worker responses (one row each)
     * @param aggKinds  aggregate function kinds per column, may be null (defaults to SUM)
     */
    static WorkerQueryResponse mergeGlobal(List<WorkerQueryResponse> responses, SqlKind[] aggKinds) {
        WorkerQueryResponse first = responses.get(0);
        List<String> columnNames = first.getColumnNames();
        List<String> columnTypes = first.getColumnTypes();
        int numCols = columnNames.size();

        Object[][] merged = new Object[numCols][1];
        for (int col = 0; col < numCols; col++) {
            SqlKind kind = (aggKinds != null && col < aggKinds.length) ? aggKinds[col] : SqlKind.SUM;
            if (kind == SqlKind.MIN) {
                merged[col][0] = AggregationReducer.minColumn(responses, col);
            } else if (kind == SqlKind.MAX) {
                merged[col][0] = AggregationReducer.maxColumn(responses, col);
            } else {
                merged[col][0] = AggregationReducer.sumColumn(responses, col);
            }
        }

        return new WorkerQueryResponse(columnNames, columnTypes, 1, merged);
    }

    /**
     * Filters out responses with zero rows.
     */
    static List<WorkerQueryResponse> filterNonEmpty(List<WorkerQueryResponse> responses) {
        List<WorkerQueryResponse> result = new ArrayList<>();
        for (WorkerQueryResponse r : responses) {
            if (r.getRowCount() > 0) {
                result.add(r);
            }
        }
        return result;
    }

    /**
     * Creates an empty response preserving column metadata from the first response if available.
     */
    static WorkerQueryResponse emptyResponse(List<WorkerQueryResponse> responses) {
        if (!responses.isEmpty()) {
            WorkerQueryResponse first = responses.get(0);
            return new WorkerQueryResponse(first.getColumnNames(), first.getColumnTypes(), 0, new Object[0][]);
        }
        return new WorkerQueryResponse(Collections.emptyList(), Collections.emptyList(), 0, new Object[0][]);
    }

}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Merge-sorts pre-sorted worker results and takes the top K rows.
 * <p>
 * Each worker has already returned its local top-K sorted by the same sort keys.
 * This class performs the global merge-sort across all workers and truncates
 * to the requested limit.
 *
 * @opensearch.internal
 */
public final class TopKMerger {

    private TopKMerger() {}

    /**
     * Merges pre-sorted worker responses and takes the top K rows.
     *
     * @param responses   the non-empty worker responses to merge
     * @param sortColumns column indices to sort by, may be null
     * @param sortAsc     ascending flag for each sort column, may be null
     * @param limit       row limit; 0 means no limit
     * @return the merged response with at most {@code limit} rows
     */
    static WorkerQueryResponse merge(
        List<WorkerQueryResponse> responses,
        int[] sortColumns,
        boolean[] sortAsc,
        int limit
    ) {
        WorkerQueryResponse first = responses.get(0);
        List<String> columnNames = first.getColumnNames();
        List<String> columnTypes = first.getColumnTypes();

        // Convert all responses to rows for sorting
        List<Object[]> allRows = new ArrayList<>();
        for (WorkerQueryResponse r : responses) {
            allRows.addAll(ResultSerializer.toRows(r));
        }

        // Sort by the given sort columns
        if (sortColumns != null && sortColumns.length > 0) {
            allRows.sort(buildComparator(sortColumns, sortAsc));
        }

        // Take top K
        int effectiveLimit = limit > 0 ? Math.min(limit, allRows.size()) : allRows.size();
        List<Object[]> topK = allRows.subList(0, effectiveLimit);

        return ResultSerializer.toColumnResponse(topK, columnNames, columnTypes);
    }

    /**
     * Builds a comparator for rows based on sort columns and directions.
     *
     * @param sortColumns column indices to sort by
     * @param sortAsc     ascending flag per column; null defaults to ascending
     * @return a comparator for row arrays
     */
    @SuppressWarnings("unchecked")
    static Comparator<Object[]> buildComparator(int[] sortColumns, boolean[] sortAsc) {
        return (row1, row2) -> {
            for (int i = 0; i < sortColumns.length; i++) {
                int col = sortColumns[i];
                boolean asc = sortAsc != null && i < sortAsc.length ? sortAsc[i] : true;

                Object v1 = col < row1.length ? row1[col] : null;
                Object v2 = col < row2.length ? row2[col] : null;

                int cmp = compareValues(v1, v2);
                if (!asc) {
                    cmp = -cmp;
                }
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        };
    }

    /**
     * Compares two values, supporting nulls (nulls sort last).
     * Falls back to {@code toString()} comparison for non-Comparable types.
     *
     * @param v1 first value
     * @param v2 second value
     * @return negative if v1 &lt; v2, positive if v1 &gt; v2, zero if equal
     */
    static int compareValues(Object v1, Object v2) {
        if (v1 == null && v2 == null) return 0;
        if (v1 == null) return 1;
        if (v2 == null) return -1;
        // Normalize numeric types to avoid ClassCastException when comparing
        // different widths (e.g., Integer vs Long from different workers)
        if (v1 instanceof Number && v2 instanceof Number) {
            return Double.compare(((Number) v1).doubleValue(), ((Number) v2).doubleValue());
        }
        if (v1 instanceof Comparable && v2 instanceof Comparable && v1.getClass().equals(v2.getClass())) {
            @SuppressWarnings("unchecked")
            int cmp = ((Comparable<Object>) v1).compareTo(v2);
            return cmp;
        }
        return v1.toString().compareTo(v2.toString());
    }
}

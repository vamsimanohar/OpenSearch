/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Combines partial {@link WorkerQueryResponse} results from distributed workers into a single
 * merged response according to the given {@link MergeStrategy}.
 * <p>
 * Merge strategies:
 * <ul>
 *   <li><b>CONCAT</b> — concatenate all rows from all workers</li>
 *   <li><b>GLOBAL_MERGE</b> — re-aggregate single-row global results (SUM of counts, MIN of mins, etc.)</li>
 *   <li><b>TOPK_MERGE</b> — merge-sort pre-sorted worker results and take top K</li>
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
            case TOPK_MERGE -> mergeTopK(nonEmpty, sortColumns, sortAsc, limit);
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
     * SUM/COUNT → sum, MIN → min, MAX → max. Falls back to sum if aggKinds is null.
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
                merged[col][0] = minColumn(responses, col);
            } else if (kind == SqlKind.MAX) {
                merged[col][0] = maxColumn(responses, col);
            } else {
                merged[col][0] = sumColumn(responses, col);
            }
        }

        return new WorkerQueryResponse(columnNames, columnTypes, 1, merged);
    }

    /**
     * Sums a single column across all worker responses (row 0 from each).
     * Handles Long, Integer, Double, and Float numeric types.
     */
    static Object sumColumn(List<WorkerQueryResponse> responses, int colIdx) {
        // Determine type from first non-null value
        Object sample = null;
        for (WorkerQueryResponse r : responses) {
            if (r.getRowCount() > 0 && r.getColumnData()[colIdx][0] != null) {
                sample = r.getColumnData()[colIdx][0];
                break;
            }
        }
        if (sample == null) {
            return null;
        }

        if (sample instanceof Long) {
            long sum = 0;
            for (WorkerQueryResponse r : responses) {
                if (r.getRowCount() > 0 && r.getColumnData()[colIdx][0] != null) {
                    sum += ((Number) r.getColumnData()[colIdx][0]).longValue();
                }
            }
            return sum;
        } else if (sample instanceof Integer) {
            long sum = 0;
            for (WorkerQueryResponse r : responses) {
                if (r.getRowCount() > 0 && r.getColumnData()[colIdx][0] != null) {
                    sum += ((Number) r.getColumnData()[colIdx][0]).intValue();
                }
            }
            return sum;
        } else if (sample instanceof Double) {
            double sum = 0.0;
            for (WorkerQueryResponse r : responses) {
                if (r.getRowCount() > 0 && r.getColumnData()[colIdx][0] != null) {
                    sum += ((Number) r.getColumnData()[colIdx][0]).doubleValue();
                }
            }
            return sum;
        } else if (sample instanceof Float) {
            double sum = 0.0;
            for (WorkerQueryResponse r : responses) {
                if (r.getRowCount() > 0 && r.getColumnData()[colIdx][0] != null) {
                    sum += ((Number) r.getColumnData()[colIdx][0]).floatValue();
                }
            }
            return (float) sum;
        }
        // Non-numeric — return first non-null (for MIN/MAX of strings, etc.)
        return sample;
    }

    /**
     * Finds the minimum value of a column across all worker responses (row 0 from each).
     * Supports Comparable types (Long, Integer, Double, String, etc.).
     */
    @SuppressWarnings("unchecked")
    static Object minColumn(List<WorkerQueryResponse> responses, int colIdx) {
        Comparable<Object> min = null;
        for (WorkerQueryResponse r : responses) {
            if (r.getRowCount() > 0 && r.getColumnData()[colIdx][0] != null) {
                Comparable<Object> val = (Comparable<Object>) r.getColumnData()[colIdx][0];
                if (min == null || val.compareTo((Object) min) < 0) {
                    min = val;
                }
            }
        }
        return min;
    }

    /**
     * Finds the maximum value of a column across all worker responses (row 0 from each).
     * Supports Comparable types (Long, Integer, Double, String, etc.).
     */
    @SuppressWarnings("unchecked")
    static Object maxColumn(List<WorkerQueryResponse> responses, int colIdx) {
        Comparable<Object> max = null;
        for (WorkerQueryResponse r : responses) {
            if (r.getRowCount() > 0 && r.getColumnData()[colIdx][0] != null) {
                Comparable<Object> val = (Comparable<Object>) r.getColumnData()[colIdx][0];
                if (max == null || val.compareTo((Object) max) > 0) {
                    max = val;
                }
            }
        }
        return max;
    }

    /**
     * Merge-sorts pre-sorted worker results and takes the top K rows.
     * Each worker has already returned its local top-K sorted by the same sort keys.
     */
    static WorkerQueryResponse mergeTopK(
        List<WorkerQueryResponse> responses,
        int[] sortColumns,
        boolean[] sortAsc,
        int limit
    ) {
        WorkerQueryResponse first = responses.get(0);
        List<String> columnNames = first.getColumnNames();
        List<String> columnTypes = first.getColumnTypes();
        int numCols = columnNames.size();

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
     */
    @SuppressWarnings("unchecked")
    static int compareValues(Object v1, Object v2) {
        if (v1 == null && v2 == null) return 0;
        if (v1 == null) return 1;
        if (v2 == null) return -1;
        if (v1 instanceof Comparable && v2 instanceof Comparable) {
            return ((Comparable<Object>) v1).compareTo(v2);
        }
        return v1.toString().compareTo(v2.toString());
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

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import java.util.List;

/**
 * Handles the reduce step of map-reduce for global aggregates.
 * <p>
 * Each worker returns a single-row partial aggregate (e.g., partial COUNT, partial SUM).
 * This class combines those partial results into a final global result by dispatching
 * on numeric type: Long, Integer, Double, Float. Non-numeric types fall through to
 * a first-non-null default.
 *
 * @opensearch.internal
 */
public final class AggregationReducer {

    private AggregationReducer() {}

    /**
     * Sums a single column across all worker responses (row 0 from each).
     * Handles Long, Integer, Double, and Float numeric types.
     * Non-numeric values return the first non-null sample.
     *
     * @param responses the worker responses (one row each)
     * @param colIdx    the column index to sum
     * @return the summed value, or null if all values are null
     */
    static Object sumColumn(List<WorkerQueryResponse> responses, int colIdx) {
        Object sample = findFirstNonNull(responses, colIdx);
        if (sample == null) {
            return null;
        }

        if (sample instanceof Long) {
            long sum = 0;
            for (WorkerQueryResponse r : responses) {
                if (hasValue(r, colIdx)) {
                    sum += ((Number) r.getColumnData()[colIdx][0]).longValue();
                }
            }
            return sum;
        } else if (sample instanceof Integer) {
            long sum = 0;
            for (WorkerQueryResponse r : responses) {
                if (hasValue(r, colIdx)) {
                    sum += ((Number) r.getColumnData()[colIdx][0]).intValue();
                }
            }
            return sum;
        } else if (sample instanceof Double) {
            double sum = 0.0;
            for (WorkerQueryResponse r : responses) {
                if (hasValue(r, colIdx)) {
                    sum += ((Number) r.getColumnData()[colIdx][0]).doubleValue();
                }
            }
            return sum;
        } else if (sample instanceof Float) {
            double sum = 0.0;
            for (WorkerQueryResponse r : responses) {
                if (hasValue(r, colIdx)) {
                    sum += ((Number) r.getColumnData()[colIdx][0]).floatValue();
                }
            }
            return (float) sum;
        }
        // Non-numeric — return first non-null
        return sample;
    }

    /**
     * Finds the minimum value of a column across all worker responses (row 0 from each).
     * Supports Comparable types (Long, Integer, Double, String, etc.).
     *
     * @param responses the worker responses (one row each)
     * @param colIdx    the column index to minimize
     * @return the minimum value, or null if all values are null
     */
    @SuppressWarnings("unchecked")
    static Object minColumn(List<WorkerQueryResponse> responses, int colIdx) {
        Comparable<Object> min = null;
        for (WorkerQueryResponse r : responses) {
            if (hasValue(r, colIdx)) {
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
     *
     * @param responses the worker responses (one row each)
     * @param colIdx    the column index to maximize
     * @return the maximum value, or null if all values are null
     */
    @SuppressWarnings("unchecked")
    static Object maxColumn(List<WorkerQueryResponse> responses, int colIdx) {
        Comparable<Object> max = null;
        for (WorkerQueryResponse r : responses) {
            if (hasValue(r, colIdx)) {
                Comparable<Object> val = (Comparable<Object>) r.getColumnData()[colIdx][0];
                if (max == null || val.compareTo((Object) max) > 0) {
                    max = val;
                }
            }
        }
        return max;
    }

    /**
     * Finds the first non-null value in a column across all worker responses.
     */
    private static Object findFirstNonNull(List<WorkerQueryResponse> responses, int colIdx) {
        for (WorkerQueryResponse r : responses) {
            if (hasValue(r, colIdx)) {
                return r.getColumnData()[colIdx][0];
            }
        }
        return null;
    }

    /**
     * Returns true if the response has at least one row and the column value at row 0 is non-null.
     */
    private static boolean hasValue(WorkerQueryResponse r, int colIdx) {
        return r.getRowCount() > 0 && r.getColumnData()[colIdx][0] != null;
    }
}

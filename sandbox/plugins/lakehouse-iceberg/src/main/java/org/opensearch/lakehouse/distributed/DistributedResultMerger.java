/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Merges partial results from distributed worker nodes based on a {@link DistributionPlan}.
 *
 * <p>For scan-only queries, results are simply concatenated. For aggregate queries,
 * partial aggregates from each worker are combined using the merge operations
 * specified in the distribution plan.
 */
public final class DistributedResultMerger {

    private static final Logger logger = LogManager.getLogger(DistributedResultMerger.class);

    private DistributedResultMerger() {}

    /**
     * Merges worker responses according to the distribution plan.
     *
     * @param responses the partial results from worker nodes
     * @param plan      the distribution plan describing how to merge
     * @return merged result rows
     * @throws IllegalStateException if the plan type is UNSUPPORTED
     */
    public static List<Object[]> merge(List<LakehouseWorkerResponse> responses, DistributionPlan plan) {
        if (responses == null || responses.isEmpty()) {
            return List.of();
        }

        switch (plan.getQueryType()) {
            case SCAN_ONLY:
                return mergeConcat(responses);
            case GLOBAL_AGGREGATE:
                return mergeGlobalAggregate(responses, plan);
            case GROUPED_AGGREGATE:
                return mergeGroupedAggregate(responses, plan);
            default:
                throw new IllegalStateException("Cannot merge results for UNSUPPORTED distribution plan");
        }
    }

    /**
     * Concatenates all worker rows into a single list (for scan-only queries).
     */
    private static List<Object[]> mergeConcat(List<LakehouseWorkerResponse> responses) {
        int totalRows = 0;
        for (LakehouseWorkerResponse response : responses) {
            totalRows += response.getRows().length;
        }

        logger.debug("[DistributedResultMerger] SCAN_ONLY merge: {} responses, {} total rows",
            responses.size(), totalRows);

        List<Object[]> merged = new ArrayList<>(totalRows);
        for (LakehouseWorkerResponse response : responses) {
            for (Object[] row : response.getRows()) {
                merged.add(row);
            }
        }
        return merged;
    }

    /**
     * Merges global aggregate results. Each worker returns a single row;
     * all rows are combined into one result row using the merge operations.
     */
    private static List<Object[]> mergeGlobalAggregate(List<LakehouseWorkerResponse> responses, DistributionPlan plan) {
        logger.debug("[DistributedResultMerger] GLOBAL_AGGREGATE merge: {} responses", responses.size());

        // Determine output width from the first non-empty response
        int numCols = 0;
        for (LakehouseWorkerResponse response : responses) {
            if (response.getRows().length > 0) {
                numCols = response.getRows()[0].length;
                break;
            }
        }
        if (numCols == 0) {
            return List.of();
        }

        Object[] result = new Object[numCols];
        boolean initialized = false;

        for (LakehouseWorkerResponse response : responses) {
            for (Object[] row : response.getRows()) {
                if (!initialized) {
                    System.arraycopy(row, 0, result, 0, numCols);
                    initialized = true;
                } else {
                    // Apply merge ops to aggregate columns
                    for (DistributionPlan.AggMergeInfo merge : plan.getAggregateMerges()) {
                        int col = merge.getOutputColumnIndex();
                        result[col] = applyMergeOp(merge.getMergeOp(), result[col], row[col]);
                    }
                }
            }
        }

        if (!initialized) {
            return List.of();
        }
        List<Object[]> singleRow = new ArrayList<>(1);
        singleRow.add(result);
        return singleRow;
    }

    /**
     * Merges grouped aggregate results. Workers may return overlapping groups;
     * rows with matching group keys are re-aggregated using the merge operations.
     */
    private static List<Object[]> mergeGroupedAggregate(List<LakehouseWorkerResponse> responses, DistributionPlan plan) {
        logger.debug("[DistributedResultMerger] GROUPED_AGGREGATE merge: {} responses", responses.size());

        int[] groupKeyCols = plan.getGroupKeyOutputColumns();

        // LinkedHashMap preserves insertion order for deterministic output
        Map<List<Object>, Object[]> groupMap = new LinkedHashMap<>();

        for (LakehouseWorkerResponse response : responses) {
            for (Object[] row : response.getRows()) {
                List<Object> key = extractGroupKey(row, groupKeyCols);
                Object[] existing = groupMap.get(key);

                if (existing == null) {
                    // First time seeing this group — store a copy
                    Object[] copy = new Object[row.length];
                    System.arraycopy(row, 0, copy, 0, row.length);
                    groupMap.put(key, copy);
                } else {
                    // Merge aggregate columns into existing row
                    for (DistributionPlan.AggMergeInfo merge : plan.getAggregateMerges()) {
                        int col = merge.getOutputColumnIndex();
                        existing[col] = applyMergeOp(merge.getMergeOp(), existing[col], row[col]);
                    }
                }
            }
        }

        logger.debug("[DistributedResultMerger] GROUPED_AGGREGATE merge produced {} groups", groupMap.size());
        return new ArrayList<>(groupMap.values());
    }

    /**
     * Extracts group key values from a row at the specified column positions.
     */
    private static List<Object> extractGroupKey(Object[] row, int[] groupKeyCols) {
        Object[] key = new Object[groupKeyCols.length];
        for (int i = 0; i < groupKeyCols.length; i++) {
            key[i] = row[groupKeyCols[i]];
        }
        return Arrays.asList(key);
    }

    /**
     * Applies a merge operation to combine two values.
     *
     * <p>Handles null values (treating them as identity elements) and
     * numeric type coercion between Integer, Long, and Double.
     */
    @SuppressWarnings("unchecked")
    static Object applyMergeOp(DistributionPlan.MergeOp op, Object accumulated, Object incoming) {
        if (incoming == null) {
            return accumulated;
        }
        if (accumulated == null) {
            return incoming;
        }

        switch (op) {
            case SUM:
                return addNumbers(accumulated, incoming);
            case MIN:
                return compareAndSelect(accumulated, incoming, true);
            case MAX:
                return compareAndSelect(accumulated, incoming, false);
            default:
                throw new IllegalArgumentException("Unknown merge operation: " + op);
        }
    }

    /**
     * Adds two numeric values, handling type promotion.
     * If either value is Double, the result is Double.
     * Otherwise, the result is Long.
     */
    private static Object addNumbers(Object a, Object b) {
        if (a instanceof Double || b instanceof Double) {
            return toDouble(a) + toDouble(b);
        }
        return toLong(a) + toLong(b);
    }

    /**
     * Selects the min or max of two comparable values.
     *
     * @param selectSmaller if true, selects the smaller value (MIN); if false, the larger (MAX)
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static Object compareAndSelect(Object a, Object b, boolean selectSmaller) {
        // For mixed numeric types, promote to a common type for comparison
        if (a instanceof Number && b instanceof Number) {
            if (a instanceof Double || b instanceof Double) {
                double da = toDouble(a);
                double db = toDouble(b);
                return selectSmaller ? (da <= db ? a : b) : (da >= db ? a : b);
            }
            long la = toLong(a);
            long lb = toLong(b);
            return selectSmaller ? (la <= lb ? a : b) : (la >= lb ? a : b);
        }
        // Fall back to Comparable
        if (a instanceof Comparable && b instanceof Comparable) {
            int cmp = ((Comparable) a).compareTo(b);
            return selectSmaller ? (cmp <= 0 ? a : b) : (cmp >= 0 ? a : b);
        }
        // If not comparable, return accumulated value
        return a;
    }

    private static long toLong(Object value) {
        return ((Number) value).longValue();
    }

    private static double toDouble(Object value) {
        return ((Number) value).doubleValue();
    }
}

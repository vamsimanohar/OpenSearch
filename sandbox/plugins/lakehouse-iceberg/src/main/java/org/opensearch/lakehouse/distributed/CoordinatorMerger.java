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
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Merges partial worker results on the coordinator node using Java-side
 * aggregation, sorting, and limiting.
 *
 * <p>Uses the structured merge metadata from {@link PhysicalPlanSplitter.SplitPlan}
 * rather than parsing the coordinator SQL string.
 */
public final class CoordinatorMerger {

    private static final Logger logger = LogManager.getLogger(CoordinatorMerger.class);

    private CoordinatorMerger() {}

    /**
     * Merges partial results from all workers according to the split plan's merge metadata.
     *
     * @param partialResults concatenated rows from all workers
     * @param splitPlan      the split plan with merge metadata
     * @return final merged result rows
     */
    public static List<Object[]> merge(List<Object[]> partialResults, PhysicalPlanSplitter.SplitPlan splitPlan) {
        if (partialResults == null || partialResults.isEmpty()) {
            return List.of();
        }

        PhysicalPlanSplitter.MergeType mergeType = splitPlan.getMergeType();
        if (mergeType == null) {
            mergeType = PhysicalPlanSplitter.MergeType.PASS_THROUGH;
        }

        logger.info("[CoordinatorMerger] Merging {} partial rows, mergeType={}", partialResults.size(), mergeType);

        List<Object[]> result;
        switch (mergeType) {
            case PASS_THROUGH:
                result = partialResults;
                break;
            case SCAN_WITH_SORT:
                result = applySortAndLimit(partialResults, splitPlan.getSortColumns(), splitPlan.getLimit());
                break;
            case AGGREGATE:
                result = mergeAggregate(partialResults, splitPlan);
                break;
            default:
                result = partialResults;
        }

        logger.info("[CoordinatorMerger] Merge complete: {} output rows", result.size());
        return result;
    }

    /**
     * Merges aggregate partial results using HashMap-based re-aggregation.
     */
    private static List<Object[]> mergeAggregate(
        List<Object[]> partialResults, PhysicalPlanSplitter.SplitPlan splitPlan
    ) {
        int groupKeyCount = splitPlan.getGroupKeyCount();
        List<PhysicalPlanSplitter.MergeColumn> mergeColumns = splitPlan.getMergeColumns();

        if (groupKeyCount == 0) {
            // Global aggregate (no GROUP BY) — merge all rows into one
            return mergeGlobalAggregate(partialResults, mergeColumns);
        }

        // Grouped aggregate — HashMap merge by group keys
        // Key: group key values as a List, Value: accumulator row
        Map<List<Object>, double[]> accumulators = new LinkedHashMap<>();

        for (Object[] row : partialResults) {
            List<Object> groupKey = new ArrayList<>(groupKeyCount);
            for (int i = 0; i < groupKeyCount; i++) {
                groupKey.add(row[i]);
            }

            double[] acc = accumulators.get(groupKey);
            if (acc == null) {
                // Initialize accumulator: for each merge column, track values
                // For AVG: track sum and count separately
                int accSize = 0;
                for (PhysicalPlanSplitter.MergeColumn mc : mergeColumns) {
                    accSize += mc.isAvg ? 2 : 1;
                }
                acc = new double[accSize];
                Arrays.fill(acc, Double.NaN); // NaN means uninitialized
                accumulators.put(groupKey, acc);
            }

            // Accumulate values
            int accIdx = 0;
            for (PhysicalPlanSplitter.MergeColumn mc : mergeColumns) {
                if (mc.isAvg) {
                    double sumVal = toDouble(row[mc.sourceIndex]);
                    double countVal = toDouble(row[mc.sourceIndex2]);
                    if (!Double.isNaN(sumVal)) {
                        acc[accIdx] = Double.isNaN(acc[accIdx]) ? sumVal : acc[accIdx] + sumVal;
                    }
                    if (!Double.isNaN(countVal)) {
                        acc[accIdx + 1] = Double.isNaN(acc[accIdx + 1]) ? countVal : acc[accIdx + 1] + countVal;
                    }
                    accIdx += 2;
                } else {
                    double val = toDouble(row[mc.sourceIndex]);
                    if (Double.isNaN(val)) {
                        accIdx++;
                        continue;
                    }
                    switch (mc.op) {
                        case SUM:
                            acc[accIdx] = Double.isNaN(acc[accIdx]) ? val : acc[accIdx] + val;
                            break;
                        case MIN:
                            acc[accIdx] = Double.isNaN(acc[accIdx]) ? val : Math.min(acc[accIdx], val);
                            break;
                        case MAX:
                            acc[accIdx] = Double.isNaN(acc[accIdx]) ? val : Math.max(acc[accIdx], val);
                            break;
                        default:
                            acc[accIdx] = val;
                    }
                    accIdx++;
                }
            }
        }

        // Build output rows: [group keys..., merged agg values...]
        int outputCols = groupKeyCount + mergeColumns.size();
        List<Object[]> result = new ArrayList<>(accumulators.size());
        for (Map.Entry<List<Object>, double[]> entry : accumulators.entrySet()) {
            Object[] outRow = new Object[outputCols];
            List<Object> groupKey = entry.getKey();
            for (int i = 0; i < groupKeyCount; i++) {
                outRow[i] = groupKey.get(i);
            }
            double[] acc = entry.getValue();
            int accIdx = 0;
            for (int i = 0; i < mergeColumns.size(); i++) {
                PhysicalPlanSplitter.MergeColumn mc = mergeColumns.get(i);
                if (mc.isAvg) {
                    double sum = acc[accIdx];
                    double count = acc[accIdx + 1];
                    outRow[groupKeyCount + i] = (Double.isNaN(sum) || Double.isNaN(count) || count == 0)
                        ? null : sum / count;
                    accIdx += 2;
                } else {
                    outRow[groupKeyCount + i] = Double.isNaN(acc[accIdx]) ? null : acc[accIdx];
                    accIdx++;
                }
            }
            result.add(outRow);
        }

        // Apply sort + limit if specified
        if (!splitPlan.getSortColumns().isEmpty() || splitPlan.getLimit() >= 0) {
            result = applySortAndLimit(result, splitPlan.getSortColumns(), splitPlan.getLimit());
        }

        return result;
    }

    /**
     * Merges a global aggregate (no GROUP BY). All partial rows merge into one output row.
     */
    private static List<Object[]> mergeGlobalAggregate(
        List<Object[]> partialResults, List<PhysicalPlanSplitter.MergeColumn> mergeColumns
    ) {
        int accSize = 0;
        for (PhysicalPlanSplitter.MergeColumn mc : mergeColumns) {
            accSize += mc.isAvg ? 2 : 1;
        }
        double[] acc = new double[accSize];
        Arrays.fill(acc, Double.NaN);

        for (Object[] row : partialResults) {
            int accIdx = 0;
            for (PhysicalPlanSplitter.MergeColumn mc : mergeColumns) {
                if (mc.isAvg) {
                    double sumVal = toDouble(row[mc.sourceIndex]);
                    double countVal = toDouble(row[mc.sourceIndex2]);
                    if (!Double.isNaN(sumVal)) {
                        acc[accIdx] = Double.isNaN(acc[accIdx]) ? sumVal : acc[accIdx] + sumVal;
                    }
                    if (!Double.isNaN(countVal)) {
                        acc[accIdx + 1] = Double.isNaN(acc[accIdx + 1]) ? countVal : acc[accIdx + 1] + countVal;
                    }
                    accIdx += 2;
                } else {
                    double val = toDouble(row[mc.sourceIndex]);
                    if (Double.isNaN(val)) { accIdx++; continue; }
                    switch (mc.op) {
                        case SUM: acc[accIdx] = Double.isNaN(acc[accIdx]) ? val : acc[accIdx] + val; break;
                        case MIN: acc[accIdx] = Double.isNaN(acc[accIdx]) ? val : Math.min(acc[accIdx], val); break;
                        case MAX: acc[accIdx] = Double.isNaN(acc[accIdx]) ? val : Math.max(acc[accIdx], val); break;
                        default: acc[accIdx] = val;
                    }
                    accIdx++;
                }
            }
        }

        // Build single output row
        Object[] outRow = new Object[mergeColumns.size()];
        int accIdx = 0;
        for (int i = 0; i < mergeColumns.size(); i++) {
            PhysicalPlanSplitter.MergeColumn mc = mergeColumns.get(i);
            if (mc.isAvg) {
                double sum = acc[accIdx];
                double count = acc[accIdx + 1];
                outRow[i] = (Double.isNaN(sum) || Double.isNaN(count) || count == 0) ? null : sum / count;
                accIdx += 2;
            } else {
                outRow[i] = Double.isNaN(acc[accIdx]) ? null : acc[accIdx];
                accIdx++;
            }
        }

        List<Object[]> result = new ArrayList<>(1);
        result.add(outRow);
        return result;
    }

    /**
     * Sorts rows and applies a limit.
     */
    private static List<Object[]> applySortAndLimit(
        List<Object[]> rows, List<PhysicalPlanSplitter.SortColumn> sortColumns, long limit
    ) {
        if (sortColumns != null && !sortColumns.isEmpty()) {
            rows.sort(buildComparator(sortColumns));
        }
        if (limit >= 0 && rows.size() > limit) {
            rows = new ArrayList<>(rows.subList(0, (int) limit));
        }
        return rows;
    }

    /**
     * Builds a comparator from sort column specifications.
     */
    private static Comparator<Object[]> buildComparator(List<PhysicalPlanSplitter.SortColumn> sortColumns) {
        return (a, b) -> {
            for (PhysicalPlanSplitter.SortColumn sc : sortColumns) {
                int idx = sc.outputIndex;
                if (idx >= a.length || idx >= b.length) continue;

                Object va = a[idx];
                Object vb = b[idx];

                // Handle nulls
                if (va == null && vb == null) continue;
                if (va == null) return sc.nullsFirst ? -1 : 1;
                if (vb == null) return sc.nullsFirst ? 1 : -1;

                int cmp = compareValues(va, vb);
                if (cmp != 0) {
                    return sc.descending ? -cmp : cmp;
                }
            }
            return 0;
        };
    }

    @SuppressWarnings("unchecked")
    private static int compareValues(Object a, Object b) {
        if (a instanceof Number && b instanceof Number) {
            return Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue());
        }
        if (a instanceof Comparable && b instanceof Comparable) {
            try {
                return ((Comparable<Object>) a).compareTo(b);
            } catch (ClassCastException e) {
                return String.valueOf(a).compareTo(String.valueOf(b));
            }
        }
        return String.valueOf(a).compareTo(String.valueOf(b));
    }

    private static double toDouble(Object value) {
        if (value == null) return Double.NaN;
        if (value instanceof Number) return ((Number) value).doubleValue();
        try {
            return Double.parseDouble(String.valueOf(value));
        } catch (NumberFormatException e) {
            return Double.NaN;
        }
    }
}

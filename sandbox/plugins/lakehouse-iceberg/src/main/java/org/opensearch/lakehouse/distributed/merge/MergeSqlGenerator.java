/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed.merge;

import org.apache.calcite.sql.SqlKind;
import org.opensearch.lakehouse.distributed.QueryAnalyzer;

import java.util.List;
import java.util.StringJoiner;

/**
 * Generates merge SQL for the coordinator's DataFusion StreamingTable.
 * The SQL runs against a table named "input" which contains concatenated
 * worker results as Arrow IPC data.
 *
 * @opensearch.internal
 */
public final class MergeSqlGenerator {

    private MergeSqlGenerator() {}

    /**
     * Generates merge SQL based on the query analysis result and column names.
     *
     * @param analysis    the query analysis result from QueryAnalyzer
     * @param columnNames the column names from the Arrow IPC schema
     * @return the merge SQL to execute against the "input" StreamingTable
     */
    public static String generate(QueryAnalyzer.AnalysisResult analysis, List<String> columnNames) {
        boolean hasAvg = AvgDecomposer.hasAvg(analysis);
        return switch (analysis.strategy) {
            case CONCAT -> generateConcat();
            case GLOBAL_MERGE -> hasAvg
                ? generateAvgMerge(analysis, columnNames)
                : generateGlobalMerge(analysis.aggKinds, columnNames);
            case TOPK_MERGE -> generateTopKMerge(analysis.sortColumns, analysis.sortAsc, analysis.limit, columnNames);
            case TWO_PHASE_GROUP_BY -> hasAvg
                ? generateAvgMerge(analysis, columnNames)
                : generateTwoPhaseGroupBy(analysis, columnNames);
            case DISTINCT_EXPAND -> throw new IllegalArgumentException("DISTINCT_EXPAND uses DistinctExpander.generateMergeSql()");
            case SINGLE_NODE -> throw new IllegalArgumentException("SINGLE_NODE should not reach merge SQL generation");
        };
    }

    // CONCAT: just pass through all rows
    static String generateConcat() {
        return "SELECT * FROM input";
    }

    // GLOBAL_MERGE: re-aggregate single-row partial results
    static String generateGlobalMerge(SqlKind[] aggKinds, List<String> columnNames) {
        StringJoiner cols = new StringJoiner(", ");
        for (int i = 0; i < columnNames.size(); i++) {
            String colName = quoteIdentifier(columnNames.get(i));
            SqlKind kind = (aggKinds != null && i < aggKinds.length) ? aggKinds[i] : SqlKind.SUM;
            String aggFunc = switch (kind) {
                case MIN -> "MIN";
                case MAX -> "MAX";
                default -> "SUM"; // SUM, COUNT both sum their partial results
            };
            cols.add(aggFunc + "(" + colName + ") AS " + colName);
        }
        return "SELECT " + cols + " FROM input";
    }

    // TOPK_MERGE: merge-sort worker results and apply limit
    static String generateTopKMerge(int[] sortColumns, boolean[] sortAsc, int limit, List<String> columnNames) {
        StringBuilder sb = new StringBuilder("SELECT * FROM input ORDER BY ");
        for (int i = 0; i < sortColumns.length; i++) {
            if (i > 0) sb.append(", ");
            String colName = quoteIdentifier(columnNames.get(sortColumns[i]));
            sb.append(colName);
            sb.append(sortAsc[i] ? " ASC" : " DESC");
        }
        if (limit > 0) {
            sb.append(" LIMIT ").append(limit);
        }
        return sb.toString();
    }

    /**
     * TWO_PHASE_GROUP_BY: re-aggregate partial GROUP BY results from workers.
     * GROUP BY key columns pass through. Aggregate columns are re-aggregated:
     * COUNT→SUM, SUM→SUM, MIN→MIN, MAX→MAX.
     * Then apply ORDER BY + LIMIT from the original query.
     */
    static String generateTwoPhaseGroupBy(QueryAnalyzer.AnalysisResult analysis, List<String> columnNames) {
        boolean[] isGroupKey = analysis.isGroupKey;
        SqlKind[] aggKinds = analysis.aggKinds;

        StringJoiner selectCols = new StringJoiner(", ");
        StringJoiner groupByCols = new StringJoiner(", ");

        for (int i = 0; i < columnNames.size(); i++) {
            String col = quoteIdentifier(columnNames.get(i));
            boolean isKey = (isGroupKey != null && i < isGroupKey.length) ? isGroupKey[i] : (i == 0);

            if (isKey) {
                selectCols.add(col);
                groupByCols.add(col);
            } else {
                // Aggregate column — re-aggregate
                SqlKind kind = (aggKinds != null && i < aggKinds.length) ? aggKinds[i] : SqlKind.SUM;
                String aggFunc = switch (kind) {
                    case MIN -> "MIN";
                    case MAX -> "MAX";
                    default -> "SUM"; // COUNT partial→SUM, SUM partial→SUM
                };
                selectCols.add(aggFunc + "(" + col + ") AS " + col);
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ").append(selectCols).append(" FROM input");
        sb.append(" GROUP BY ").append(groupByCols);

        // Apply ORDER BY from original query
        if (analysis.sortColumns != null && analysis.sortColumns.length > 0) {
            sb.append(" ORDER BY ");
            for (int i = 0; i < analysis.sortColumns.length; i++) {
                if (i > 0) sb.append(", ");
                sb.append(quoteIdentifier(columnNames.get(analysis.sortColumns[i])));
                sb.append(analysis.sortAsc[i] ? " ASC" : " DESC");
            }
        }

        // Apply LIMIT from original query
        if (analysis.limit > 0) {
            sb.append(" LIMIT ").append(analysis.limit);
        }

        return sb.toString();
    }

    /**
     * Generates merge SQL for queries containing AVG aggregates (both GLOBAL_MERGE and TWO_PHASE_GROUP_BY).
     * AVG columns are decomposed: workers return __avg_sum_N and __avg_count_N,
     * coordinator computes CAST(SUM(__avg_sum_N) AS DOUBLE) / SUM(__avg_count_N).
     */
    static String generateAvgMerge(QueryAnalyzer.AnalysisResult analysis, List<String> columnNames) {
        AvgDecomposer.MergeColumnInfo info = AvgDecomposer.buildMergeColumns(analysis, columnNames);

        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ").append(String.join(", ", info.selectExprs));
        sb.append(" FROM input");

        if (!info.groupByExprs.isEmpty()) {
            sb.append(" GROUP BY ").append(String.join(", ", info.groupByExprs));
        }

        // Apply ORDER BY — need to map original sort column indices to output column names
        if (analysis.sortColumns != null && analysis.sortColumns.length > 0) {
            sb.append(" ORDER BY ");
            for (int i = 0; i < analysis.sortColumns.length; i++) {
                if (i > 0) sb.append(", ");
                int sortIdx = analysis.sortColumns[i];
                String colName = (sortIdx < info.outputColumnNames.size())
                    ? info.outputColumnNames.get(sortIdx)
                    : info.outputColumnNames.get(info.outputColumnNames.size() - 1);
                sb.append(quoteIdentifier(colName));
                sb.append(analysis.sortAsc[i] ? " ASC" : " DESC");
            }
        }

        if (analysis.limit > 0) {
            sb.append(" LIMIT ").append(analysis.limit);
        }

        return sb.toString();
    }

    // Quote identifier with double quotes for DataFusion (case-sensitive)
    private static String quoteIdentifier(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }
}

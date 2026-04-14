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
     * @param columnNames the column names from the Calcite plan (relNode.getRowType().getFieldNames())
     * @return the merge SQL to execute against the "input" StreamingTable
     */
    public static String generate(QueryAnalyzer.AnalysisResult analysis, List<String> columnNames) {
        return switch (analysis.strategy) {
            case CONCAT -> generateConcat();
            case GLOBAL_MERGE -> generateGlobalMerge(analysis.aggKinds, columnNames);
            case TOPK_MERGE -> generateTopKMerge(analysis.sortColumns, analysis.sortAsc, analysis.limit, columnNames);
            case SINGLE_NODE -> throw new IllegalArgumentException("SINGLE_NODE should not reach merge SQL generation");
        };
    }

    // CONCAT: just pass through all rows
    static String generateConcat() {
        return "SELECT * FROM input";
    }

    // GLOBAL_MERGE: re-aggregate single-row partial results
    // Each worker returned one row with partial aggregates. The merge re-aggregates.
    // e.g., workers returned SUM(x), COUNT(y), MIN(z), MAX(w)
    //        merge SQL: SELECT SUM(col0), SUM(col1), MIN(col2), MAX(col3) FROM input
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
    // Workers each returned their top-K, coordinator does final sort + limit
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

    // Quote identifier with double quotes for DataFusion (case-sensitive)
    private static String quoteIdentifier(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }
}

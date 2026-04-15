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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Decomposes AVG aggregates for two-phase distributed execution.
 * <p>
 * Workers cannot compute a correct global AVG — partial averages from different
 * shards with different row counts cannot be simply averaged. Instead:
 * <ul>
 *   <li><b>Worker SQL</b>: AVG(x) → SUM(x) AS __avg_sum_N, COUNT(x) AS __avg_count_N</li>
 *   <li><b>Merge SQL</b>: CAST(SUM(__avg_sum_N) AS DOUBLE) / SUM(__avg_count_N) AS original_alias</li>
 * </ul>
 *
 * @opensearch.internal
 */
public final class AvgDecomposer {

    private AvgDecomposer() {}

    // Matches AVG(...) with optional AS "alias" — handles nested parens like AVG(length(URL))
    private static final Pattern AVG_PATTERN = Pattern.compile(
        "AVG\\(([^()]*(?:\\([^()]*\\)[^()]*)*)\\)",
        Pattern.CASE_INSENSITIVE
    );

    /**
     * Checks if the analysis result contains any AVG aggregates.
     */
    public static boolean hasAvg(QueryAnalyzer.AnalysisResult analysis) {
        if (analysis.aggKinds == null) return false;
        for (SqlKind kind : analysis.aggKinds) {
            if (kind == SqlKind.AVG) return true;
        }
        return false;
    }

    /**
     * Rewrites worker SQL to decompose AVG into SUM + COUNT.
     * Each AVG(expr) becomes SUM(expr) AS "__avg_sum_N", COUNT(expr) AS "__avg_count_N".
     *
     * @param sql the original SQL query
     * @return the rewritten SQL with AVG decomposed
     */
    public static String decomposeWorkerSql(String sql) {
        Matcher matcher = AVG_PATTERN.matcher(sql);
        StringBuilder sb = new StringBuilder();
        int avgIndex = 0;
        while (matcher.find()) {
            String innerExpr = matcher.group(1);
            String replacement = "SUM(" + innerExpr + ") AS \"__avg_sum_" + avgIndex
                + "\", COUNT(" + innerExpr + ") AS \"__avg_count_" + avgIndex + "\"";
            matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
            avgIndex++;
        }
        matcher.appendTail(sb);

        // Strip the original alias after our replacement (e.g., AS "resolutionwidth")
        // Pattern: our __avg_count_N" followed by AS "something"
        String result = sb.toString();
        result = result.replaceAll(
            "(\"__avg_count_\\d+\")\\s+AS\\s+\"[^\"]+\"",
            "$1"
        );
        return result;
    }

    /**
     * Builds the column mapping from worker output columns to merge SQL expressions.
     * Used by MergeSqlGenerator to produce the correct merge SQL for AVG columns.
     *
     * @param analysis    the query analysis result
     * @param columnNames the column names from the worker Arrow IPC output
     * @return the merge SQL column expressions
     */
    public static MergeColumnInfo buildMergeColumns(QueryAnalyzer.AnalysisResult analysis, List<String> columnNames) {
        SqlKind[] aggKinds = analysis.aggKinds;
        boolean[] isGroupKey = analysis.isGroupKey;

        List<String> selectExprs = new ArrayList<>();
        List<String> groupByExprs = new ArrayList<>();
        // Track original output column names for ORDER BY reference
        List<String> outputColumnNames = new ArrayList<>();

        int colIdx = 0;
        // Walk the original analysis columns (not the expanded worker columns)
        // We need to reconstruct which worker columns map to which original columns
        int origColIdx = 0;
        while (colIdx < columnNames.size()) {
            String colName = columnNames.get(colIdx);

            if (colName.startsWith("__avg_sum_")) {
                // This is a decomposed AVG — consume both __avg_sum_N and __avg_count_N
                String sumCol = quoteIdentifier(colName);
                String countCol = quoteIdentifier(columnNames.get(colIdx + 1));

                // Find the original alias — look at what aggKinds[origColIdx] says
                // For now, produce the AVG merge expression
                String alias = "\"avg_" + colName.substring("__avg_sum_".length()) + "\"";

                selectExprs.add("CAST(SUM(" + sumCol + ") AS DOUBLE) / SUM(" + countCol + ") AS " + alias);
                outputColumnNames.add("avg_" + colName.substring("__avg_sum_".length()));
                colIdx += 2; // skip sum and count columns
                origColIdx++;
            } else {
                boolean isKey = false;
                if (isGroupKey != null && origColIdx < isGroupKey.length) {
                    isKey = isGroupKey[origColIdx];
                }

                String quoted = quoteIdentifier(colName);
                if (isKey) {
                    selectExprs.add(quoted);
                    groupByExprs.add(quoted);
                } else {
                    // Non-AVG aggregate column
                    SqlKind kind = (aggKinds != null && origColIdx < aggKinds.length && aggKinds[origColIdx] != null)
                        ? aggKinds[origColIdx] : SqlKind.SUM;
                    String aggFunc = switch (kind) {
                        case MIN -> "MIN";
                        case MAX -> "MAX";
                        default -> "SUM";
                    };
                    selectExprs.add(aggFunc + "(" + quoted + ") AS " + quoted);
                }
                outputColumnNames.add(colName);
                colIdx++;
                origColIdx++;
            }
        }

        return new MergeColumnInfo(selectExprs, groupByExprs, outputColumnNames);
    }

    private static String quoteIdentifier(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }

    /**
     * Holds the computed merge column info for AVG-decomposed queries.
     */
    public static final class MergeColumnInfo {
        public final List<String> selectExprs;
        public final List<String> groupByExprs;
        public final List<String> outputColumnNames;

        MergeColumnInfo(List<String> selectExprs, List<String> groupByExprs, List<String> outputColumnNames) {
            this.selectExprs = selectExprs;
            this.groupByExprs = groupByExprs;
            this.outputColumnNames = outputColumnNames;
        }
    }
}

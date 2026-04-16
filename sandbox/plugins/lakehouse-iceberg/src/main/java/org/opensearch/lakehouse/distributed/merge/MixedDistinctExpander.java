/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed.merge;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Rewrites SQL for queries with mixed COUNT(DISTINCT) and regular aggregates.
 * <p>
 * Workers GROUP BY (original_keys + distinct_cols) with partial aggregates for non-distinct columns.
 * The coordinator re-aggregates regular aggregates and computes COUNT(DISTINCT) on concatenated results.
 * <p>
 * Example:
 * <ul>
 *   <li>Original: SELECT "regionid", SUM("x"), COUNT(*) AS c, COUNT(DISTINCT "userid") FROM "hits" GROUP BY "regionid"</li>
 *   <li>Worker:   SELECT "regionid", SUM("x"), COUNT(*) AS c, "userid" FROM "hits" GROUP BY "regionid", "userid"</li>
 *   <li>Merge:    SELECT "regionid", SUM("SUM(hits.x)") AS ..., SUM("c") AS "c", COUNT(DISTINCT "userid") FROM input GROUP BY "regionid"</li>
 * </ul>
 *
 * @opensearch.internal
 */
public final class MixedDistinctExpander {

    private MixedDistinctExpander() {}

    // Matches COUNT(DISTINCT "col") or COUNT(DISTINCT col) — captures the inner expression
    private static final Pattern COUNT_DISTINCT_PATTERN = Pattern.compile(
        "COUNT\\(DISTINCT\\s+([^)]+)\\)",
        Pattern.CASE_INSENSITIVE
    );

    // Matches GROUP BY clause
    private static final Pattern GROUP_BY_PATTERN = Pattern.compile(
        "\\bGROUP\\s+BY\\s+(.*?)(?=\\s+HAVING\\s+|\\s+ORDER\\s+BY|\\s+LIMIT\\s+|$)",
        Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );

    // Matches ORDER BY clause
    private static final Pattern ORDER_BY_PATTERN = Pattern.compile(
        "\\bORDER\\s+BY\\s+(.*?)(?=\\s+LIMIT\\s+|$)",
        Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );

    // Matches LIMIT clause
    private static final Pattern LIMIT_PATTERN = Pattern.compile(
        "\\bLIMIT\\s+(\\d+)",
        Pattern.CASE_INSENSITIVE
    );

    /**
     * Rewrites worker SQL for mixed COUNT(DISTINCT) queries.
     * Removes COUNT(DISTINCT) from SELECT, adds distinct cols to SELECT and GROUP BY,
     * decomposes AVG, and strips HAVING/ORDER BY/LIMIT.
     *
     * @param sql the original SQL query
     * @return the rewritten worker SQL
     */
    public static String rewriteWorkerSql(String sql) {
        // 1. Extract distinct column expressions
        Set<String> distinctColSet = new LinkedHashSet<>();
        Matcher cdMatcher = COUNT_DISTINCT_PATTERN.matcher(sql);
        while (cdMatcher.find()) {
            distinctColSet.add(cdMatcher.group(1).trim());
        }
        if (distinctColSet.isEmpty()) return sql;
        List<String> distinctCols = new ArrayList<>(distinctColSet);

        // 2. Remove COUNT(DISTINCT ...) with optional alias from SELECT
        String modified = sql;
        modified = modified.replaceAll(
            "(?i)COUNT\\(DISTINCT\\s+[^)]+\\)\\s*(?:AS\\s+(?:\"[^\"]+\"|\\w+))?",
            ""
        );

        // 3. Clean up commas left by removal
        modified = modified.replaceAll(",\\s*,", ",");
        modified = modified.replaceAll("(?i)(SELECT\\s+)\\s*,\\s*", "$1");
        modified = modified.replaceAll(",\\s*(?=\\s*FROM\\b)", "");

        // 4. Decompose AVG if present
        modified = AvgDecomposer.decomposeWorkerSql(modified);

        // 5. Add distinct columns to SELECT (before FROM)
        String distinctColsStr = String.join(", ", distinctCols);
        modified = modified.replaceFirst(
            "(?i)\\s+FROM\\s+",
            ", " + Matcher.quoteReplacement(distinctColsStr) + " FROM "
        );

        // 6. Add distinct columns to GROUP BY
        Matcher groupByMatcher = GROUP_BY_PATTERN.matcher(modified);
        if (groupByMatcher.find()) {
            String groupByStr = groupByMatcher.group(1).trim();
            modified = modified.substring(0, groupByMatcher.start())
                + "GROUP BY " + groupByStr + ", " + distinctColsStr
                + modified.substring(groupByMatcher.end());
        } else {
            // No GROUP BY in original — add one with just the distinct columns
            modified = appendBeforeTrailingClauses(modified, "GROUP BY " + distinctColsStr);
        }

        // 7. Strip HAVING, ORDER BY, LIMIT
        modified = modified.replaceAll("(?is)\\s+HAVING\\s+.*?(?=\\s+ORDER\\s+BY|\\s+LIMIT\\s+|$)", "");
        modified = modified.replaceAll("(?is)\\s+ORDER\\s+BY\\s+.+$", "");
        modified = modified.replaceAll("(?is)\\s+LIMIT\\s+\\d+\\s*$", "");

        return modified.replaceAll("\\s+", " ").trim();
    }

    /**
     * Generates merge SQL for the coordinator. Re-aggregates regular aggregates
     * and computes COUNT(DISTINCT) on concatenated worker results.
     *
     * @param workerColumnNames column names from worker Arrow IPC output
     * @param originalSql       the original SQL query
     * @return the merge SQL
     */
    public static String generateMergeSql(List<String> workerColumnNames, String originalSql) {
        // Extract group keys from original SQL
        Set<String> groupKeySet = new LinkedHashSet<>();
        Matcher groupByMatcher = GROUP_BY_PATTERN.matcher(originalSql);
        if (groupByMatcher.find()) {
            for (String col : groupByMatcher.group(1).trim().split(",")) {
                groupKeySet.add(unquote(col.trim()));
            }
        }

        // Extract distinct column names from original SQL
        Set<String> distinctColSet = new LinkedHashSet<>();
        Matcher cdMatcher = COUNT_DISTINCT_PATTERN.matcher(originalSql);
        while (cdMatcher.find()) {
            distinctColSet.add(unquote(cdMatcher.group(1).trim()));
        }

        // Build merge SQL components
        List<String> selectParts = new ArrayList<>();
        List<String> groupByParts = new ArrayList<>();

        int colIdx = 0;
        while (colIdx < workerColumnNames.size()) {
            String colName = workerColumnNames.get(colIdx);
            String colNameLower = colName.toLowerCase();
            String quoted = quoteIdentifier(colName);

            if (groupKeySet.contains(colNameLower)) {
                // Group key — pass through
                selectParts.add(quoted);
                groupByParts.add(quoted);
                colIdx++;
            } else if (distinctColSet.contains(colNameLower)) {
                // Distinct column — COUNT(DISTINCT)
                selectParts.add("COUNT(DISTINCT " + quoted + ")");
                colIdx++;
            } else if (colName.startsWith("__avg_sum_")
                && colIdx + 1 < workerColumnNames.size()
                && workerColumnNames.get(colIdx + 1).startsWith("__avg_count_")) {
                // AVG decomposition pair
                String sumCol = quoted;
                String countCol = quoteIdentifier(workerColumnNames.get(colIdx + 1));
                String alias = "\"avg_" + colName.substring("__avg_sum_".length()) + "\"";
                selectParts.add("CAST(SUM(" + sumCol + ") AS DOUBLE) / SUM(" + countCol + ") AS " + alias);
                colIdx += 2;
            } else {
                // Regular aggregate — re-aggregate
                String aggFunc = inferReAggFunction(colNameLower);
                selectParts.add(aggFunc + "(" + quoted + ") AS " + quoted);
                colIdx++;
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ").append(String.join(", ", selectParts));
        sb.append(" FROM input");

        if (!groupByParts.isEmpty()) {
            sb.append(" GROUP BY ").append(String.join(", ", groupByParts));
        }

        // Extract and append ORDER BY from original SQL
        Matcher orderByMatcher = ORDER_BY_PATTERN.matcher(originalSql);
        if (orderByMatcher.find()) {
            sb.append(" ORDER BY ").append(orderByMatcher.group(1).trim());
        }

        // Extract and append LIMIT from original SQL
        Matcher limitMatcher = LIMIT_PATTERN.matcher(originalSql);
        if (limitMatcher.find()) {
            sb.append(" LIMIT ").append(limitMatcher.group(1));
        }

        return sb.toString();
    }

    /**
     * Infers the re-aggregation function from the worker column name.
     * MIN(...) → MIN, MAX(...) → MAX, everything else → SUM.
     */
    static String inferReAggFunction(String colNameLower) {
        if (colNameLower.startsWith("min(")) return "MIN";
        if (colNameLower.startsWith("max(")) return "MAX";
        return "SUM";
    }

    /**
     * Removes quotes from an identifier: "foo" → foo.
     */
    private static String unquote(String identifier) {
        String s = identifier.trim();
        if (s.startsWith("\"") && s.endsWith("\"")) {
            s = s.substring(1, s.length() - 1);
        }
        return s.toLowerCase();
    }

    private static String quoteIdentifier(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }

    /**
     * Appends a clause before ORDER BY, LIMIT, or at the end of the SQL.
     */
    private static String appendBeforeTrailingClauses(String sql, String clause) {
        String orderByRegex = "(?i)\\s+ORDER\\s+BY\\s+";
        String limitRegex = "(?i)\\s+LIMIT\\s+";
        if (sql.matches("(?is).*\\bORDER\\s+BY\\b.*")) {
            return sql.replaceFirst(orderByRegex, " " + Matcher.quoteReplacement(clause) + " ORDER BY ");
        } else if (sql.matches("(?is).*\\bLIMIT\\b.*")) {
            return sql.replaceFirst(limitRegex, " " + Matcher.quoteReplacement(clause) + " LIMIT ");
        } else {
            return sql + " " + clause;
        }
    }
}

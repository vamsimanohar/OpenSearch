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
 * Rewrites SQL for COUNT(DISTINCT) expansion in distributed queries.
 * <p>
 * Workers return distinct raw values instead of aggregated counts.
 * The coordinator then runs COUNT(DISTINCT ...) on the concatenated results.
 * <p>
 * Example:
 * <ul>
 *   <li>Original: SELECT "regionid", COUNT(DISTINCT "userid") FROM "hits" GROUP BY "regionid" ORDER BY ... LIMIT ...</li>
 *   <li>Worker:   SELECT DISTINCT "regionid", "userid" FROM "hits"</li>
 *   <li>Merge:    SELECT "regionid", COUNT(DISTINCT "userid") FROM input GROUP BY "regionid" ORDER BY ... LIMIT ...</li>
 * </ul>
 *
 * @opensearch.internal
 */
public final class DistinctExpander {

    private DistinctExpander() {}

    // Matches COUNT(DISTINCT "col") or COUNT(DISTINCT col) — captures the inner expression
    private static final Pattern COUNT_DISTINCT_PATTERN = Pattern.compile(
        "COUNT\\(DISTINCT\\s+([^)]+)\\)",
        Pattern.CASE_INSENSITIVE
    );

    // Matches FROM "tablename" or FROM tablename
    private static final Pattern FROM_PATTERN = Pattern.compile(
        "\\bFROM\\s+(\"[^\"]+\"|\\w+)",
        Pattern.CASE_INSENSITIVE
    );

    // Matches WHERE clause (everything between WHERE and GROUP BY / ORDER BY / LIMIT / end)
    private static final Pattern WHERE_PATTERN = Pattern.compile(
        "\\bWHERE\\s+(.*?)(?=\\s+GROUP\\s+BY|\\s+ORDER\\s+BY|\\s+LIMIT\\s+|$)",
        Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );

    // Matches GROUP BY clause columns
    private static final Pattern GROUP_BY_PATTERN = Pattern.compile(
        "\\bGROUP\\s+BY\\s+(.*?)(?=\\s+ORDER\\s+BY|\\s+LIMIT\\s+|\\s+HAVING\\s+|$)",
        Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );

    /**
     * Rewrites the SQL for workers to return distinct raw values.
     * Replaces the aggregate query with SELECT DISTINCT of group keys + distinct columns.
     *
     * @param sql the original SQL query
     * @return the rewritten worker SQL
     */
    public static String rewriteWorkerSql(String sql) {
        // Extract distinct column expressions (deduplicate — same expr may appear in SELECT and ORDER BY)
        Set<String> distinctColSet = new LinkedHashSet<>();
        Matcher cdMatcher = COUNT_DISTINCT_PATTERN.matcher(sql);
        while (cdMatcher.find()) {
            distinctColSet.add(cdMatcher.group(1).trim());
        }
        if (distinctColSet.isEmpty()) return sql;
        List<String> distinctCols = new ArrayList<>(distinctColSet);

        // Extract table name
        Matcher fromMatcher = FROM_PATTERN.matcher(sql);
        if (!fromMatcher.find()) return sql;
        String tableName = fromMatcher.group(1);

        // Extract WHERE clause if present
        String whereClause = "";
        Matcher whereMatcher = WHERE_PATTERN.matcher(sql);
        if (whereMatcher.find()) {
            whereClause = " WHERE " + whereMatcher.group(1).trim();
        }

        // Extract GROUP BY columns
        List<String> groupByCols = new ArrayList<>();
        Matcher groupByMatcher = GROUP_BY_PATTERN.matcher(sql);
        if (groupByMatcher.find()) {
            String groupByStr = groupByMatcher.group(1).trim();
            for (String col : groupByStr.split(",")) {
                groupByCols.add(col.trim());
            }
        }

        // Build SELECT DISTINCT: group_keys + distinct_columns
        List<String> selectCols = new ArrayList<>(groupByCols);
        selectCols.addAll(distinctCols);

        return "SELECT DISTINCT " + String.join(", ", selectCols) + " FROM " + tableName + whereClause;
    }

    /**
     * Generates merge SQL for the coordinator.
     * The coordinator runs COUNT(DISTINCT ...) on the concatenated worker results.
     *
     * @param workerColumnNames column names from the worker Arrow IPC output
     * @param originalSql       the original SQL query (for extracting ORDER BY/LIMIT/GROUP BY structure)
     * @return the merge SQL
     */
    public static String generateMergeSql(List<String> workerColumnNames, String originalSql) {
        // Extract GROUP BY columns from original SQL
        List<String> groupByCols = new ArrayList<>();
        Matcher groupByMatcher = GROUP_BY_PATTERN.matcher(originalSql);
        if (groupByMatcher.find()) {
            String groupByStr = groupByMatcher.group(1).trim();
            for (String col : groupByStr.split(",")) {
                groupByCols.add(col.trim());
            }
        }

        // Distinct columns are those NOT in GROUP BY
        List<String> distinctCols = new ArrayList<>();
        for (String col : workerColumnNames) {
            String quoted = quoteIdentifier(col);
            boolean isGroupKey = false;
            for (String gk : groupByCols) {
                // Compare unquoted
                if (gk.replace("\"", "").equalsIgnoreCase(col)) {
                    isGroupKey = true;
                    break;
                }
            }
            if (!isGroupKey) {
                distinctCols.add(quoted);
            }
        }

        // Build SELECT: group keys + COUNT(DISTINCT distinct_cols)
        StringBuilder sb = new StringBuilder("SELECT ");
        List<String> selectParts = new ArrayList<>();

        for (String gk : groupByCols) {
            selectParts.add(gk);
        }
        for (String dc : distinctCols) {
            selectParts.add("COUNT(DISTINCT " + dc + ")");
        }

        sb.append(String.join(", ", selectParts));
        sb.append(" FROM input");

        if (!groupByCols.isEmpty()) {
            sb.append(" GROUP BY ").append(String.join(", ", groupByCols));
        }

        // Extract ORDER BY from original
        Pattern orderByPattern = Pattern.compile(
            "\\bORDER\\s+BY\\s+(.*?)(?=\\s+LIMIT\\s+|$)",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL
        );
        Matcher orderByMatcher = orderByPattern.matcher(originalSql);
        if (orderByMatcher.find()) {
            sb.append(" ORDER BY ").append(orderByMatcher.group(1).trim());
        }

        // Extract LIMIT from original
        Pattern limitPattern = Pattern.compile("\\bLIMIT\\s+(\\d+)", Pattern.CASE_INSENSITIVE);
        Matcher limitMatcher = limitPattern.matcher(originalSql);
        if (limitMatcher.find()) {
            sb.append(" LIMIT ").append(limitMatcher.group(1));
        }

        return sb.toString();
    }

    private static String quoteIdentifier(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }
}

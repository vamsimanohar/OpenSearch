/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.engine;

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.List;

/**
 * Decomposes a Calcite logical plan into a multi-stage {@link SubPlan}.
 * <p>
 * Walks the RelNode tree to classify the query and produces an ordered list of
 * {@link PlanFragment} stages connected by {@link ExchangeType} exchanges.
 * <p>
 * Classification rules and resulting plans:
 * <ul>
 *   <li><b>Simple scan/filter/project</b> — 2 stages: leaf(GATHER) → coordinator(CONCAT)</li>
 *   <li><b>Global aggregation</b> — 2 stages: leaf(GATHER) → coordinator(re-aggregate).
 *       AVG is decomposed into SUM+COUNT on workers, recombined on coordinator.</li>
 *   <li><b>ORDER BY + LIMIT</b> — 2 stages: leaf(GATHER) → coordinator(merge-sort + limit)</li>
 *   <li><b>GROUP BY (no LIMIT)</b> — 2 stages: leaf(GATHER) → coordinator(re-aggregate + re-group)</li>
 *   <li><b>GROUP BY + LIMIT</b> — 3 stages: leaf(HASH on group keys) → intermediate(GROUP BY + ORDER BY + LIMIT)
 *       → coordinator(GATHER + CONCAT). Each intermediate task owns a disjoint subset of groups.</li>
 *   <li><b>Global COUNT DISTINCT</b> — 2 stages: leaf(GATHER, dedup) → coordinator(COUNT DISTINCT).
 *       Workers deduplicate locally via GROUP BY, coordinator finalizes with COUNT(DISTINCT).</li>
 *   <li><b>GROUP BY + COUNT DISTINCT + LIMIT</b> — 3 stages: leaf(HASH, dedup) → intermediate(COUNT DISTINCT + ORDER BY + LIMIT)
 *       → coordinator(GATHER + CONCAT). Workers dedup by (group keys + distinct cols), intermediate has
 *       complete data per group (via HASH) and computes exact COUNT DISTINCT.</li>
 * </ul>
 *
 * @opensearch.internal
 */
public final class PlanFragmenter {

    private PlanFragmenter() {}

    /**
     * Analyzes the RelNode tree and SQL, producing a multi-stage execution plan.
     *
     * @param relNode the Calcite logical plan root
     * @param sql     the SQL query string (used for SQL rewriting on workers)
     * @return the fragmented plan
     */
    public static SubPlan fragment(RelNode relNode, String sql) {
        PlanVisitor visitor = new PlanVisitor();
        visitor.go(relNode);

        if (visitor.aggregate != null) {
            return fragmentAggregate(visitor, sql, relNode);
        }

        if (visitor.sort != null) {
            return fragmentSort(visitor.sort, sql, relNode);
        }

        return fragmentScan(sql);
    }

    private static SubPlan fragmentAggregate(PlanVisitor visitor, String sql, RelNode relNode) {
        Aggregate aggregate = visitor.aggregate;

        boolean hasNonCountDistinct = false;
        boolean hasCountDistinct = false;
        boolean hasNonDistinctAgg = false;
        for (AggregateCall call : aggregate.getAggCallList()) {
            if (call.isDistinct()) {
                if (call.getAggregation().getKind() != SqlKind.COUNT) {
                    hasNonCountDistinct = true;
                } else {
                    hasCountDistinct = true;
                }
            } else {
                hasNonDistinctAgg = true;
            }
        }
        if (hasNonCountDistinct) {
            throw new UnsupportedOperationException(
                "DISTINCT aggregates other than COUNT are not distributable — only COUNT(DISTINCT) is supported"
            );
        }
        if (hasCountDistinct && hasNonDistinctAgg) {
            throw new UnsupportedOperationException(
                "Mixed COUNT(DISTINCT) with other aggregates requires dedup expansion that exceeds memory limits"
            );
        }

        boolean hasGroupBy = !aggregate.getGroupSet().isEmpty();
        boolean hasAvg = hasAvg(aggregate);
        boolean hasDistinctAgg = hasDistinct(aggregate);
        SqlKind[] aggKinds = extractAggKinds(aggregate);
        boolean[] isDistinct = extractIsDistinct(aggregate);
        boolean[] isPassthrough = extractIsPassthrough(aggregate);

        String havingClause = null;
        String workingSql = sql;
        if (hasGroupBy && findKeyword(sql.toUpperCase(), "HAVING") >= 0) {
            havingClause = extractHavingClause(sql);
            workingSql = stripHaving(sql);
        }

        if (hasGroupBy) {
            return fragmentGroupBy(aggregate, visitor.sort, workingSql, aggKinds, hasAvg, hasDistinctAgg, isDistinct, isPassthrough, havingClause);
        }

        // Global aggregation (no GROUP BY)
        return fragmentGlobalAggregate(workingSql, aggKinds, hasAvg, hasDistinctAgg, isDistinct);
    }

    /**
     * Unified GROUP BY handler for all cases: with/without LIMIT, HAVING, OFFSET,
     * mixed COUNT DISTINCT, AVG, constant expressions.
     * <p>
     * Uses 3-stage HASH plan when LIMIT is present (to avoid sending all partial groups
     * to coordinator, which OOMs on high-cardinality GROUP BY). Workers pre-aggregate,
     * intermediate stage (on coordinator) re-aggregates disjoint group subsets with
     * ORDER BY + LIMIT. Uses 2-stage GATHER plan otherwise.
     * <p>
     * HAVING is stripped from workers and applied on the coordinator/intermediate stage
     * after full re-aggregation. OFFSET is applied globally on coordinator.
     */
    private static SubPlan fragmentGroupBy(
        Aggregate aggregate,
        Sort sort,
        String sql,
        SqlKind[] aggKinds,
        boolean hasAvg,
        boolean hasDistinct,
        boolean[] isDistinct,
        boolean[] isPassthrough,
        String havingClause
    ) {
        int groupCount = aggregate.getGroupSet().cardinality();
        boolean hasLimit = sort != null && sort.fetch != null;
        boolean needsDecomposition = hasAvg || hasDistinct;

        // Bounded local top-K: keep ORDER BY with expanded LIMIT to prevent OOM
        // from high-cardinality GROUP BY. Applied when LIMIT is present AND either:
        // (a) no decomposition needed, or
        // (b) AVG-only decomposition (no DISTINCT) — decomposition preserves
        //     non-AVG ORDER BY aliases (e.g. ORDER BY c where c=COUNT(*)).
        // COUNT(DISTINCT) can't use bounded top-K (dedup expands GROUP BY,
        // changing the query structure entirely).
        boolean useBoundedTopK = hasLimit && (!needsDecomposition || (hasAvg && !hasDistinct));

        String workerSql;
        if (useBoundedTopK) {
            int limit = extractLimit(sort);
            int offset = sort != null ? extractOffset(sort) : 0;
            int workerLimit = Math.max(limit + offset, limit * 100);
            workerSql = stripHaving(stripOffset(sql));
            workerSql = adjustLimit(workerSql, workerLimit);
        } else {
            workerSql = stripOrderByLimitOffset(sql);
        }
        if (hasDistinct) {
            workerSql = decomposeDistinctToDedup(workerSql, aggregate);
        }
        if (hasAvg) {
            workerSql = decomposeAvg(workerSql);
        }

        if (hasLimit && needsDecomposition) {
            int[] groupKeyIndices = groupKeyIndices(groupCount);
            PlanFragment leafStage = PlanFragment.leaf(0, workerSql, ExchangeType.HASH, groupKeyIndices);

            String intermediateSql = buildIntermediateGroupBySql(groupCount, aggKinds, sort, hasAvg, isDistinct, isPassthrough, havingClause);
            PlanFragment intermediateStage = PlanFragment.intermediate(1, intermediateSql, ExchangeType.GATHER, null);

            PlanFragment finalStage = PlanFragment.intermediate(2, "SELECT * FROM __exchange_input__", ExchangeType.NONE, null);

            return SubPlan.distributed(List.of(leafStage, intermediateStage, finalStage));
        }

        PlanFragment leafStage = PlanFragment.leaf(0, workerSql, ExchangeType.GATHER, null);

        String coordinatorSql = buildTwoPhaseGroupByCoordinatorSql(groupCount, aggKinds, sort, hasAvg, isDistinct, isPassthrough, havingClause);
        PlanFragment finalStage = PlanFragment.intermediate(1, coordinatorSql, ExchangeType.NONE, null);

        return SubPlan.distributed(List.of(leafStage, finalStage));
    }

    /**
     * Global aggregation: 2-stage plan with GATHER exchange.
     * Workers compute partial aggregates; coordinator re-aggregates.
     * COUNT DISTINCT is decomposed: workers emit raw distinct values,
     * coordinator finalizes with COUNT(DISTINCT).
     */
    private static SubPlan fragmentGlobalAggregate(
        String sql,
        SqlKind[] aggKinds,
        boolean hasAvg,
        boolean hasDistinct,
        boolean[] isDistinct
    ) {
        String workerSql = sql;
        if (hasDistinct) {
            workerSql = decomposeGlobalDistinctToRawValues(workerSql);
        }
        if (hasAvg) {
            workerSql = decomposeAvg(workerSql);
        }
        PlanFragment leafStage = PlanFragment.leaf(0, workerSql, ExchangeType.GATHER, null);

        String coordinatorSql = buildGlobalMergeCoordinatorSql(aggKinds, hasAvg, isDistinct);
        PlanFragment finalStage = PlanFragment.intermediate(1, coordinatorSql, ExchangeType.NONE, null);

        return SubPlan.distributed(List.of(leafStage, finalStage));
    }

    /**
     * ORDER BY + LIMIT: 2-stage plan with GATHER exchange (top-K merge).
     * ORDER BY without LIMIT: single-node (can't merge unsorted partitions correctly).
     * <p>
     * When the ORDER BY columns are not in the SELECT list, they are appended to the
     * worker SQL so they appear in the Arrow IPC stream. The coordinator SQL uses a
     * subquery to strip those extra columns from the final output.
     */
    private static SubPlan fragmentSort(Sort sort, String sql, RelNode relNode) {
        boolean hasCollation = sort.getCollation() != null && !sort.getCollation().getFieldCollations().isEmpty();
        if (!hasCollation) {
            return fragmentScan(sql);
        }
        if (sort.fetch == null) {
            throw new UnsupportedOperationException(
                "ORDER BY without LIMIT is not distributable — unbounded sort requires all data on one node"
            );
        }

        int limit = extractLimit(sort);
        int[] sortCols = extractSortColumns(sort);
        boolean[] sortAsc = extractSortDirections(sort);
        int outputColumnCount = relNode.getRowType().getFieldCount();
        String[] sortColumnNames = extractSortColumnNames(sort);

        // Detect missing sort columns and add them to worker SQL
        String workerSql = sql;
        int[] adjustedSortCols = sortCols;
        if (sortColumnNames != null) {
            List<String> outputNames = relNode.getRowType().getFieldNames();
            List<String> missingColumns = new ArrayList<>();
            for (String name : sortColumnNames) {
                if (name != null && !outputNames.contains(name)) {
                    missingColumns.add(name);
                }
            }
            if (!missingColumns.isEmpty()) {
                workerSql = addColumnsToSelect(sql, missingColumns);
                List<String> workerOutputNames = new ArrayList<>(outputNames);
                workerOutputNames.addAll(missingColumns);
                adjustedSortCols = new int[sortColumnNames.length];
                for (int i = 0; i < sortColumnNames.length; i++) {
                    adjustedSortCols[i] = workerOutputNames.indexOf(sortColumnNames[i]);
                }
            }
        }

        PlanFragment leafStage = PlanFragment.leaf(0, workerSql, ExchangeType.GATHER, null);

        String coordinatorSql = buildTopKCoordinatorSql(adjustedSortCols, sortAsc, limit, outputColumnCount);
        PlanFragment finalStage = PlanFragment.intermediate(1, coordinatorSql, ExchangeType.NONE, null);

        return SubPlan.distributed(List.of(leafStage, finalStage));
    }

    /**
     * Extracts sort column names from the Sort node's input row type.
     */
    static String[] extractSortColumnNames(Sort sort) {
        List<RelFieldCollation> collations = sort.getCollation().getFieldCollations();
        List<String> fieldNames = sort.getInput().getRowType().getFieldNames();
        String[] names = new String[collations.size()];
        for (int i = 0; i < collations.size(); i++) {
            int fieldIndex = collations.get(i).getFieldIndex();
            names[i] = fieldIndex < fieldNames.size() ? fieldNames.get(fieldIndex) : null;
        }
        return names;
    }

    /**
     * Inserts additional columns into the SELECT clause of a SQL query.
     * Finds the first {@code FROM} keyword and inserts the quoted column names before it.
     */
    static String addColumnsToSelect(String sql, List<String> columns) {
        int fromKeywordIdx = -1;
        String upper = sql.toUpperCase();
        int searchFrom = 0;
        while (searchFrom < upper.length()) {
            int idx = upper.indexOf("FROM ", searchFrom);
            if (idx < 0) break;
            if (idx > 0 && Character.isWhitespace(sql.charAt(idx - 1))) {
                fromKeywordIdx = idx;
                break;
            }
            searchFrom = idx + 4;
        }
        if (fromKeywordIdx < 0) {
            return sql;
        }
        int insertPos = fromKeywordIdx;
        while (insertPos > 0 && Character.isWhitespace(sql.charAt(insertPos - 1))) {
            insertPos--;
        }
        StringBuilder sb = new StringBuilder(sql.substring(0, insertPos));
        for (String col : columns) {
            sb.append(", \"").append(col).append("\"");
        }
        sb.append(sql.substring(insertPos));
        return sb.toString();
    }

    /**
     * Simple scan/filter/project: 2-stage plan with GATHER + CONCAT.
     */
    private static SubPlan fragmentScan(String sql) {
        PlanFragment leafStage = PlanFragment.leaf(0, sql, ExchangeType.GATHER, null);
        PlanFragment finalStage = PlanFragment.intermediate(
            1, "SELECT * FROM __exchange_input__", ExchangeType.NONE, null
        );
        return SubPlan.distributed(List.of(leafStage, finalStage));
    }

    // ---- SQL Builders ----

    /**
     * Builds coordinator SQL for global aggregation re-merge.
     * Maps each aggregate kind to the correct re-aggregation function.
     * AVG columns occupy 2 worker columns (SUM + COUNT) and are recombined as SUM/SUM.
     * COUNT DISTINCT columns use COUNT(DISTINCT col_N) since workers emit raw values.
     */
    static String buildGlobalMergeCoordinatorSql(SqlKind[] aggKinds, boolean hasAvg, boolean[] isDistinct) {
        if (aggKinds == null || aggKinds.length == 0) {
            return "SELECT * FROM __exchange_input__";
        }
        StringBuilder sb = new StringBuilder("SELECT ");
        int workerCol = 0;
        for (int i = 0; i < aggKinds.length; i++) {
            if (i > 0) sb.append(", ");
            SqlKind kind = aggKinds[i];
            if (kind == SqlKind.AVG) {
                String sumCol = "\"col_" + workerCol + "\"";
                String countCol = "\"col_" + (workerCol + 1) + "\"";
                sb.append("CAST(SUM(").append(sumCol).append(") AS DOUBLE) / SUM(").append(countCol).append(")");
                workerCol += 2;
            } else if (isDistinct != null && isDistinct[i]) {
                String col = "\"col_" + workerCol + "\"";
                sb.append("COUNT(DISTINCT ").append(col).append(")");
                workerCol++;
            } else {
                String col = "\"col_" + workerCol + "\"";
                String func = reAggFunction(kind);
                sb.append(func).append("(").append(col).append(")");
                workerCol++;
            }
        }
        sb.append(" FROM __exchange_input__");
        return sb.toString();
    }

    /**
     * Builds coordinator SQL for two-phase GROUP BY (with or without LIMIT).
     * Handles AVG decomposition (SUM+COUNT) and COUNT DISTINCT (workers emit deduped
     * rows; coordinator counts distinct values per group).
     */
    static String buildTwoPhaseGroupByCoordinatorSql(
        int groupCount,
        SqlKind[] aggKinds,
        Sort sort,
        boolean hasAvg,
        boolean[] isDistinct
    ) {
        return buildTwoPhaseGroupByCoordinatorSql(groupCount, aggKinds, sort, hasAvg, isDistinct, null, null);
    }

    static String buildTwoPhaseGroupByCoordinatorSql(
        int groupCount,
        SqlKind[] aggKinds,
        Sort sort,
        boolean hasAvg,
        boolean[] isDistinct,
        boolean[] isPassthrough
    ) {
        return buildTwoPhaseGroupByCoordinatorSql(groupCount, aggKinds, sort, hasAvg, isDistinct, isPassthrough, null);
    }

    static String buildTwoPhaseGroupByCoordinatorSql(
        int groupCount,
        SqlKind[] aggKinds,
        Sort sort,
        boolean hasAvg,
        boolean[] isDistinct,
        boolean[] isPassthrough,
        String havingClause
    ) {
        StringBuilder sb = new StringBuilder("SELECT ");
        int totalAggs = aggKinds != null ? aggKinds.length : 0;

        int workerCol = 0;
        for (int i = 0; i < groupCount; i++) {
            if (i > 0) sb.append(", ");
            sb.append("\"col_").append(workerCol).append("\"");
            workerCol++;
        }

        for (int i = 0; i < totalAggs; i++) {
            if (isPassthrough != null && isPassthrough[i]) {
                continue;
            }
            sb.append(", ");
            SqlKind kind = aggKinds[i];
            if (kind == SqlKind.AVG && hasAvg) {
                String sumCol = "\"col_" + workerCol + "\"";
                String countCol = "\"col_" + (workerCol + 1) + "\"";
                sb.append("CAST(SUM(").append(sumCol).append(") AS DOUBLE) / SUM(").append(countCol).append(")");
                workerCol += 2;
            } else if (isDistinct != null && isDistinct[i]) {
                String col = "\"col_" + workerCol + "\"";
                sb.append("COUNT(DISTINCT ").append(col).append(")");
                workerCol++;
            } else {
                String col = "\"col_" + workerCol + "\"";
                sb.append(reAggFunction(kind)).append("(").append(col).append(")");
                workerCol++;
            }
        }

        sb.append(" FROM __exchange_input__ GROUP BY ");
        for (int i = 0; i < groupCount; i++) {
            if (i > 0) sb.append(", ");
            sb.append("\"col_").append(i).append("\"");
        }

        if (havingClause != null) {
            String rewritten = rewriteHavingForReAggregation(havingClause, groupCount, aggKinds, hasAvg, isDistinct, isPassthrough);
            sb.append(" HAVING ").append(rewritten);
        }

        boolean hasOrderBy = sort != null && sort.getCollation() != null && !sort.getCollation().getFieldCollations().isEmpty();
        boolean wrapInSubquery = hasAvg && hasOrderBy;

        if (wrapInSubquery) {
            sb.insert(0, "SELECT * FROM (");
            sb.append(")");
        }

        if (hasOrderBy) {
            int[] sortCols = extractSortColumns(sort);
            boolean[] sortAsc = extractSortDirections(sort);
            sb.append(" ORDER BY ");
            for (int i = 0; i < sortCols.length; i++) {
                if (i > 0) sb.append(", ");
                int remapped = remapSortPosition(sortCols[i], groupCount, isPassthrough);
                sb.append(remapped + 1);
                sb.append(sortAsc[i] ? " ASC" : " DESC");
            }
        }

        if (sort != null) {
            int limit = extractLimit(sort);
            if (limit > 0) {
                sb.append(" LIMIT ").append(limit);
            }
            int offset = extractOffset(sort);
            if (offset > 0) {
                sb.append(" OFFSET ").append(offset);
            }
        }

        return sb.toString();
    }

    static String buildIntermediateGroupBySql(
        int groupCount,
        SqlKind[] aggKinds,
        Sort sort,
        boolean hasAvg,
        boolean[] isDistinct
    ) {
        return buildIntermediateGroupBySql(groupCount, aggKinds, sort, hasAvg, isDistinct, null, null);
    }

    /**
     * Builds intermediate stage SQL for GROUP BY + LIMIT with HASH exchange.
     * The intermediate task owns a disjoint set of groups (from HASH partitioning),
     * so it can safely apply GROUP BY + ORDER BY + LIMIT.
     * COUNT DISTINCT columns use COUNT(DISTINCT col_N) since workers emitted deduped raw values.
     */
    static String buildIntermediateGroupBySql(
        int groupCount,
        SqlKind[] aggKinds,
        Sort sort,
        boolean hasAvg,
        boolean[] isDistinct,
        boolean[] isPassthrough,
        String havingClause
    ) {
        StringBuilder sb = new StringBuilder("SELECT ");
        int totalAggs = aggKinds != null ? aggKinds.length : 0;

        int workerCol = 0;
        for (int i = 0; i < groupCount; i++) {
            if (i > 0) sb.append(", ");
            sb.append("\"col_").append(workerCol).append("\"");
            workerCol++;
        }

        for (int i = 0; i < totalAggs; i++) {
            if (isPassthrough != null && isPassthrough[i]) {
                continue;
            }
            sb.append(", ");
            SqlKind kind = aggKinds[i];
            if (kind == SqlKind.AVG && hasAvg) {
                String sumCol = "\"col_" + workerCol + "\"";
                String countCol = "\"col_" + (workerCol + 1) + "\"";
                sb.append("CAST(SUM(").append(sumCol).append(") AS DOUBLE) / SUM(").append(countCol).append(")");
                workerCol += 2;
            } else if (isDistinct != null && isDistinct[i]) {
                String col = "\"col_" + workerCol + "\"";
                sb.append("COUNT(DISTINCT ").append(col).append(")");
                workerCol++;
            } else {
                String col = "\"col_" + workerCol + "\"";
                sb.append(reAggFunction(kind)).append("(").append(col).append(")");
                workerCol++;
            }
        }

        sb.append(" FROM __exchange_input__ GROUP BY ");
        for (int i = 0; i < groupCount; i++) {
            if (i > 0) sb.append(", ");
            sb.append("\"col_").append(i).append("\"");
        }

        if (havingClause != null) {
            String rewritten = rewriteHavingForReAggregation(havingClause, groupCount, aggKinds, hasAvg, isDistinct, isPassthrough);
            sb.append(" HAVING ").append(rewritten);
        }

        boolean hasOrderBy = sort.getCollation() != null && !sort.getCollation().getFieldCollations().isEmpty();
        boolean wrapInSubquery = hasAvg && hasOrderBy;

        if (wrapInSubquery) {
            sb.insert(0, "SELECT * FROM (");
            sb.append(")");
        }

        if (hasOrderBy) {
            int[] sortCols = extractSortColumns(sort);
            boolean[] sortAsc = extractSortDirections(sort);
            sb.append(" ORDER BY ");
            for (int i = 0; i < sortCols.length; i++) {
                if (i > 0) sb.append(", ");
                int remapped = remapSortPosition(sortCols[i], groupCount, isPassthrough);
                sb.append(remapped + 1);
                sb.append(sortAsc[i] ? " ASC" : " DESC");
            }
        }

        int limit = extractLimit(sort);
        if (limit > 0) {
            sb.append(" LIMIT ").append(limit);
        }

        int offset = extractOffset(sort);
        if (offset > 0) {
            sb.append(" OFFSET ").append(offset);
        }

        return sb.toString();
    }

    /**
     * Builds top-K merge coordinator SQL with ORDER BY and LIMIT.
     * When extra sort columns were appended to the worker SQL (not in the original SELECT),
     * wraps with a subquery to project only the first {@code outputColumnCount} columns.
     *
     * @param sortCols          position-based indices into the worker output (col_N)
     * @param sortAsc           sort direction per sort column (true=ASC)
     * @param limit             LIMIT value (0 = no limit)
     * @param outputColumnCount number of original output columns; 0 means no stripping needed
     */
    static String buildTopKCoordinatorSql(int[] sortCols, boolean[] sortAsc, int limit, int outputColumnCount) {
        StringBuilder orderBy = new StringBuilder(" ORDER BY ");
        for (int i = 0; i < sortCols.length; i++) {
            if (i > 0) orderBy.append(", ");
            orderBy.append("\"col_").append(sortCols[i]).append("\"");
            orderBy.append(sortAsc[i] ? " ASC" : " DESC");
        }
        if (limit > 0) {
            orderBy.append(" LIMIT ").append(limit);
        }

        boolean needsStripping = false;
        if (outputColumnCount > 0) {
            for (int idx : sortCols) {
                if (idx >= outputColumnCount) {
                    needsStripping = true;
                    break;
                }
            }
        }

        if (needsStripping) {
            StringBuilder sb = new StringBuilder("SELECT ");
            for (int i = 0; i < outputColumnCount; i++) {
                if (i > 0) sb.append(", ");
                sb.append("\"col_").append(i).append("\"");
            }
            sb.append(" FROM (SELECT * FROM __exchange_input__").append(orderBy).append(")");
            return sb.toString();
        }

        return "SELECT * FROM __exchange_input__" + orderBy;
    }

    // ---- SQL Rewriting Helpers ----

    static String decomposeAvg(String sql) {
        String upper = sql.toUpperCase();
        StringBuilder result = new StringBuilder();
        int pos = 0;

        while (pos < sql.length()) {
            int avgIdx = upper.indexOf("AVG(", pos);
            if (avgIdx < 0) {
                result.append(sql, pos, sql.length());
                break;
            }
            if (avgIdx > 0 && Character.isLetterOrDigit(sql.charAt(avgIdx - 1))) {
                result.append(sql, pos, avgIdx + 4);
                pos = avgIdx + 4;
                continue;
            }
            int parenDepth = 0;
            int closeIdx = -1;
            for (int i = avgIdx + 3; i < sql.length(); i++) {
                if (sql.charAt(i) == '(') parenDepth++;
                else if (sql.charAt(i) == ')') {
                    parenDepth--;
                    if (parenDepth == 0) { closeIdx = i; break; }
                }
            }
            if (closeIdx < 0) {
                result.append(sql, pos, sql.length());
                break;
            }
            String innerExpr = sql.substring(avgIdx + 4, closeIdx);
            result.append(sql, pos, avgIdx);
            result.append("SUM(CAST(").append(innerExpr).append(" AS DOUBLE)), COUNT(").append(innerExpr).append(")");
            pos = closeIdx + 1;
        }
        return result.toString();
    }

    /**
     * Rewrites a global COUNT(DISTINCT col) query so workers emit locally-unique values.
     * Replaces {@code COUNT(DISTINCT col)} with just {@code col} and inserts the
     * {@code DISTINCT} keyword after {@code SELECT} to deduplicate on each worker.
     * The coordinator then runs {@code COUNT(DISTINCT col_N)} over the pre-deduped results.
     */
    static String decomposeGlobalDistinctToRawValues(String sql) {
        List<String> found = new ArrayList<>();
        String stripped = stripCountDistinct(sql, found);
        if (found.isEmpty()) {
            return stripped;
        }
        String upperStripped = stripped.toUpperCase();
        int selectIdx = upperStripped.indexOf("SELECT ");
        if (selectIdx >= 0) {
            stripped = stripped.substring(0, selectIdx + 7) + "DISTINCT " + stripped.substring(selectIdx + 7);
        }
        return stripped;
    }

    /**
     * Rewrites a GROUP BY query's COUNT(DISTINCT col) to plain col and appends
     * the distinct column names to the GROUP BY clause. This makes workers dedup
     * by (group keys + distinct cols) so the intermediate/coordinator stage can
     * compute exact COUNT(DISTINCT) per group.
     */
    static String decomposeDistinctToDedup(String sql, Aggregate aggregate) {
        List<String> distinctCols = new ArrayList<>();
        String stripped = stripCountDistinct(sql, distinctCols);
        if (!distinctCols.isEmpty()) {
            stripped = appendToGroupBy(stripped, distinctCols);
        }
        return stripped;
    }

    private static String stripCountDistinct(String sql) {
        return stripCountDistinct(sql, null);
    }

    private static String stripCountDistinct(String sql, List<String> extractedCols) {
        String upper = sql.toUpperCase();
        StringBuilder result = new StringBuilder();
        int pos = 0;

        while (pos < sql.length()) {
            int cdIdx = upper.indexOf("COUNT(DISTINCT ", pos);
            if (cdIdx < 0) {
                result.append(sql, pos, sql.length());
                break;
            }
            if (cdIdx > 0 && Character.isLetterOrDigit(sql.charAt(cdIdx - 1))) {
                result.append(sql, pos, cdIdx + 15);
                pos = cdIdx + 15;
                continue;
            }
            int parenDepth = 0;
            int closeIdx = -1;
            for (int i = cdIdx + 5; i < sql.length(); i++) {
                if (sql.charAt(i) == '(') parenDepth++;
                else if (sql.charAt(i) == ')') {
                    parenDepth--;
                    if (parenDepth == 0) { closeIdx = i; break; }
                }
            }
            if (closeIdx < 0) {
                result.append(sql, pos, sql.length());
                break;
            }
            String innerExpr = sql.substring(cdIdx + 15, closeIdx);
            if (extractedCols != null) {
                extractedCols.add(innerExpr.trim());
            }
            result.append(sql, pos, cdIdx);
            result.append(innerExpr);
            pos = closeIdx + 1;
        }
        return result.toString();
    }

    /**
     * Rewrites a HAVING clause so aggregate references use re-aggregation expressions
     * over worker output columns. For example, {@code COUNT(*) > 100000} becomes
     * {@code SUM("col_N") > 100000} because workers already computed partial COUNTs
     * and the coordinator re-aggregates with SUM.
     */
    static String rewriteHavingForReAggregation(
        String havingClause,
        int groupCount,
        SqlKind[] aggKinds,
        boolean hasAvg,
        boolean[] isDistinct,
        boolean[] isPassthrough
    ) {
        String result = havingClause;
        int totalAggs = aggKinds != null ? aggKinds.length : 0;
        int workerCol = groupCount;
        for (int i = 0; i < totalAggs; i++) {
            if (isPassthrough != null && isPassthrough[i]) {
                continue;
            }
            SqlKind kind = aggKinds[i];
            if (kind == SqlKind.AVG && hasAvg) {
                workerCol += 2;
            } else if (kind == SqlKind.COUNT) {
                String reAgg = "SUM(\"col_" + workerCol + "\")";
                if (isDistinct != null && isDistinct[i]) {
                    result = replaceAggInHaving(result, "COUNT(DISTINCT", reAgg);
                } else {
                    result = replaceAggInHaving(result, "COUNT(", reAgg);
                }
                workerCol++;
            } else {
                workerCol++;
            }
        }
        return result;
    }

    private static String replaceAggInHaving(String having, String aggPrefix, String replacement) {
        String upper = having.toUpperCase();
        int idx = upper.indexOf(aggPrefix);
        if (idx < 0) return having;
        if (aggPrefix.equals("COUNT(") && idx > 0 && upper.charAt(idx - 1) == '(') {
            return having;
        }
        int parenStart = having.indexOf('(', idx + aggPrefix.length() - 1);
        if (parenStart < 0) parenStart = idx + aggPrefix.length() - 1;
        int depth = 0;
        int closeIdx = -1;
        for (int i = parenStart; i < having.length(); i++) {
            if (having.charAt(i) == '(') depth++;
            else if (having.charAt(i) == ')') {
                depth--;
                if (depth == 0) { closeIdx = i; break; }
            }
        }
        if (closeIdx < 0) return having;
        return having.substring(0, idx) + replacement + having.substring(closeIdx + 1);
    }

    private static String appendToGroupBy(String sql, List<String> columns) {
        String upper = sql.toUpperCase();
        int groupByIdx = findKeyword(upper, "GROUP BY");
        if (groupByIdx < 0) {
            return sql;
        }
        int afterGroupBy = groupByIdx + 8;
        int endIdx = sql.length();
        int havingIdx = findKeyword(upper, "HAVING");
        int orderIdx = findKeyword(upper, "ORDER BY");
        int limitIdx = findKeyword(upper, "LIMIT");
        if (havingIdx >= 0) endIdx = Math.min(endIdx, havingIdx);
        if (orderIdx >= 0) endIdx = Math.min(endIdx, orderIdx);
        if (limitIdx >= 0) endIdx = Math.min(endIdx, limitIdx);

        StringBuilder sb = new StringBuilder(sql.substring(0, endIdx).stripTrailing());
        for (String col : columns) {
            sb.append(", ").append(col);
        }
        if (endIdx < sql.length()) {
            sb.append(" ").append(sql.substring(endIdx));
        }
        return sb.toString();
    }

    static String extractHavingClause(String sql) {
        String upper = sql.toUpperCase();
        int havingIdx = findKeyword(upper, "HAVING");
        if (havingIdx < 0) return null;
        int afterHaving = havingIdx + 7;
        int endIdx = sql.length();
        int orderIdx = findKeyword(upper, "ORDER BY");
        int limitIdx = findKeyword(upper, "LIMIT");
        if (orderIdx >= 0 && orderIdx > havingIdx) endIdx = Math.min(endIdx, orderIdx);
        if (limitIdx >= 0 && limitIdx > havingIdx) endIdx = Math.min(endIdx, limitIdx);
        return sql.substring(afterHaving, endIdx).strip();
    }

    static String stripHaving(String sql) {
        String upper = sql.toUpperCase();
        int havingIdx = findKeyword(upper, "HAVING");
        if (havingIdx < 0) return sql;
        int endIdx = sql.length();
        int orderIdx = findKeyword(upper, "ORDER BY");
        int limitIdx = findKeyword(upper, "LIMIT");
        if (orderIdx >= 0 && orderIdx > havingIdx) endIdx = orderIdx;
        else if (limitIdx >= 0 && limitIdx > havingIdx) endIdx = limitIdx;
        else endIdx = sql.length();
        String before = sql.substring(0, havingIdx).stripTrailing();
        String after = endIdx < sql.length() ? " " + sql.substring(endIdx) : "";
        return before + after;
    }

    static String adjustLimit(String sql, int newLimit) {
        String upper = sql.toUpperCase();
        int limitIdx = findKeyword(upper, "LIMIT");
        if (limitIdx < 0) return sql;
        int afterLimit = limitIdx + 5;
        while (afterLimit < sql.length() && Character.isWhitespace(sql.charAt(afterLimit))) {
            afterLimit++;
        }
        int endNum = afterLimit;
        while (endNum < sql.length() && Character.isDigit(sql.charAt(endNum))) {
            endNum++;
        }
        return sql.substring(0, afterLimit) + newLimit + sql.substring(endNum);
    }

    static String stripOffset(String sql) {
        String upper = sql.toUpperCase();
        int offsetIdx = findKeyword(upper, "OFFSET");
        if (offsetIdx < 0) return sql;
        int endIdx = offsetIdx + 6;
        while (endIdx < sql.length() && (Character.isWhitespace(sql.charAt(endIdx)) || Character.isDigit(sql.charAt(endIdx)))) {
            endIdx++;
        }
        return (sql.substring(0, offsetIdx).stripTrailing() + " " + sql.substring(endIdx)).stripTrailing();
    }

    static String stripOrderByLimitOffset(String sql) {
        String upper = sql.toUpperCase();
        int orderByIdx = findKeyword(upper, "ORDER BY");
        if (orderByIdx >= 0) {
            return sql.substring(0, orderByIdx).stripTrailing();
        }
        int limitIdx = findKeyword(upper, "LIMIT");
        if (limitIdx >= 0) {
            return sql.substring(0, limitIdx).stripTrailing();
        }
        return sql;
    }

    private static int findKeyword(String upper, String keyword) {
        int searchFrom = 0;
        while (searchFrom < upper.length()) {
            int idx = upper.indexOf(keyword, searchFrom);
            if (idx < 0) return -1;
            if (idx == 0 || Character.isWhitespace(upper.charAt(idx - 1))) {
                return idx;
            }
            searchFrom = idx + keyword.length();
        }
        return -1;
    }

    // ---- RelNode Extraction Helpers ----

    static boolean hasDistinct(Aggregate aggregate) {
        for (AggregateCall call : aggregate.getAggCallList()) {
            if (call.isDistinct()) return true;
        }
        return false;
    }

    static boolean hasAvg(Aggregate aggregate) {
        for (AggregateCall call : aggregate.getAggCallList()) {
            if (call.getAggregation().getKind() == SqlKind.AVG) return true;
        }
        return false;
    }

    static SqlKind[] extractAggKinds(Aggregate aggregate) {
        List<AggregateCall> calls = aggregate.getAggCallList();
        SqlKind[] kinds = new SqlKind[calls.size()];
        for (int i = 0; i < calls.size(); i++) {
            kinds[i] = calls.get(i).getAggregation().getKind();
        }
        return kinds;
    }

    static boolean[] extractIsDistinct(Aggregate aggregate) {
        List<AggregateCall> calls = aggregate.getAggCallList();
        boolean[] result = new boolean[calls.size()];
        for (int i = 0; i < calls.size(); i++) {
            result[i] = calls.get(i).isDistinct();
        }
        return result;
    }

    static boolean hasNonPassthroughAnyValue(Aggregate aggregate, boolean[] isPassthrough) {
        List<AggregateCall> calls = aggregate.getAggCallList();
        for (int i = 0; i < calls.size(); i++) {
            if (calls.get(i).getAggregation().getKind() == SqlKind.ANY_VALUE
                && (isPassthrough == null || !isPassthrough[i])) {
                return true;
            }
        }
        return false;
    }

    static boolean[] extractIsPassthrough(Aggregate aggregate) {
        ImmutableBitSet groupSet = aggregate.getGroupSet();
        List<AggregateCall> calls = aggregate.getAggCallList();
        boolean[] result = new boolean[calls.size()];
        for (int i = 0; i < calls.size(); i++) {
            AggregateCall call = calls.get(i);
            SqlKind kind = call.getAggregation().getKind();
            if ((kind == SqlKind.MIN || kind == SqlKind.MAX || kind == SqlKind.ANY_VALUE)
                && call.getArgList().size() == 1
                && groupSet.get(call.getArgList().get(0))) {
                result[i] = true;
            }
        }
        return result;
    }

    static int[] extractSortColumns(Sort sort) {
        List<RelFieldCollation> collations = sort.getCollation().getFieldCollations();
        int[] cols = new int[collations.size()];
        for (int i = 0; i < collations.size(); i++) {
            cols[i] = collations.get(i).getFieldIndex();
        }
        return cols;
    }

    static boolean[] extractSortDirections(Sort sort) {
        List<RelFieldCollation> collations = sort.getCollation().getFieldCollations();
        boolean[] asc = new boolean[collations.size()];
        for (int i = 0; i < collations.size(); i++) {
            asc[i] = collations.get(i).getDirection() == RelFieldCollation.Direction.ASCENDING;
        }
        return asc;
    }

    static int extractLimit(Sort sort) {
        if (sort.fetch instanceof RexLiteral) {
            return ((RexLiteral) sort.fetch).getValueAs(Integer.class);
        }
        return 0;
    }

    static int extractOffset(Sort sort) {
        if (sort.offset instanceof RexLiteral) {
            return ((RexLiteral) sort.offset).getValueAs(Integer.class);
        }
        return 0;
    }

    static int remapSortPosition(int pos, int groupCount, boolean[] isPassthrough) {
        if (isPassthrough == null || pos < groupCount) {
            return pos;
        }
        int aggIndex = pos - groupCount;
        int skipped = 0;
        for (int i = 0; i < aggIndex && i < isPassthrough.length; i++) {
            if (isPassthrough[i]) {
                skipped++;
            }
        }
        return pos - skipped;
    }

    private static String reAggFunction(SqlKind kind) {
        return switch (kind) {
            case MIN, ANY_VALUE -> "MIN";
            case MAX -> "MAX";
            default -> "SUM";
        };
    }

    private static int[] groupKeyIndices(int groupCount) {
        int[] indices = new int[groupCount];
        for (int i = 0; i < groupCount; i++) {
            indices[i] = i;
        }
        return indices;
    }

    /**
     * Visitor that walks the RelNode tree to find Aggregate and Sort nodes.
     */
    static class PlanVisitor extends RelVisitor {
        Aggregate aggregate;
        Sort sort;

        @Override
        public void visit(RelNode node, int ordinal, RelNode parent) {
            if (node instanceof Aggregate && aggregate == null) {
                aggregate = (Aggregate) node;
            } else if (node instanceof Sort && sort == null) {
                Sort s = (Sort) node;
                if (!s.getCollation().getFieldCollations().isEmpty() || s.fetch != null) {
                    sort = s;
                }
            }
            super.visit(node, ordinal, parent);
        }
    }
}

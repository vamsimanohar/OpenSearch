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
 *   <li><b>COUNT DISTINCT, unsupported patterns</b> — single-node fallback</li>
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

        if (hasDistinct(aggregate)) {
            return SubPlan.singleNode();
        }

        boolean hasGroupBy = !aggregate.getGroupSet().isEmpty();
        boolean hasAvg = hasAvg(aggregate);
        boolean hasLimit = visitor.sort != null && visitor.sort.fetch != null;
        SqlKind[] aggKinds = extractAggKinds(aggregate);

        if (hasGroupBy) {
            if (hasAvg && !hasLimit) {
                // GROUP BY + AVG without LIMIT: could decompose AVG but adds complexity.
                // Keep as single-node for now — can be optimized later.
                return SubPlan.singleNode();
            }

            if (hasLimit) {
                return fragmentGroupByWithLimit(aggregate, visitor.sort, sql, aggKinds, hasAvg);
            }

            return fragmentGroupByNoLimit(aggregate, visitor.sort, sql, aggKinds);
        }

        // Global aggregation (no GROUP BY) — AVG is distributable via SUM+COUNT
        return fragmentGlobalAggregate(sql, aggKinds, hasAvg);
    }

    /**
     * GROUP BY + LIMIT: 3-stage plan with HASH exchange.
     * <p>
     * Stage 0 (leaf): Execute partial GROUP BY on workers, HASH-partition output by group keys.
     * Stage 1 (intermediate): Each task receives a disjoint set of group keys, runs full
     *     GROUP BY + ORDER BY + LIMIT. Because groups are disjoint, LIMIT is correct.
     * Stage 2 (final): GATHER all intermediate results, CONCAT (each intermediate already applied LIMIT).
     */
    private static SubPlan fragmentGroupByWithLimit(
        Aggregate aggregate,
        Sort sort,
        String sql,
        SqlKind[] aggKinds,
        boolean hasAvg
    ) {
        int groupCount = aggregate.getGroupSet().cardinality();
        int[] groupKeyIndices = groupKeyIndices(groupCount);

        // Stage 0: workers run GROUP BY without ORDER BY/LIMIT, output HASH-partitioned by group keys
        String workerSql = stripOrderByLimitOffset(sql);
        if (hasAvg) {
            workerSql = decomposeAvg(workerSql);
        }
        PlanFragment leafStage = PlanFragment.leaf(0, workerSql, ExchangeType.HASH, groupKeyIndices);

        // Stage 1: intermediate tasks re-aggregate their partition with full ORDER BY + LIMIT
        String intermediateSql = buildIntermediateGroupBySql(groupCount, aggKinds, sort, hasAvg);
        PlanFragment intermediateStage = PlanFragment.intermediate(1, intermediateSql, ExchangeType.GATHER, null);

        // Stage 2: coordinator just concatenates — each intermediate already applied correct LIMIT
        PlanFragment finalStage = PlanFragment.intermediate(2, "SELECT * FROM __exchange_input__", ExchangeType.NONE, null);

        return SubPlan.distributed(List.of(leafStage, intermediateStage, finalStage));
    }

    /**
     * GROUP BY without LIMIT: 2-stage plan with GATHER exchange.
     * Workers run full GROUP BY; coordinator re-aggregates.
     */
    private static SubPlan fragmentGroupByNoLimit(Aggregate aggregate, Sort sort, String sql, SqlKind[] aggKinds) {
        int groupCount = aggregate.getGroupSet().cardinality();

        String workerSql = sql;
        if (sort != null && sort.getCollation() != null && !sort.getCollation().getFieldCollations().isEmpty()) {
            workerSql = stripOrderByLimitOffset(sql);
        }

        PlanFragment leafStage = PlanFragment.leaf(0, workerSql, ExchangeType.GATHER, null);

        String coordinatorSql = buildTwoPhaseGroupByCoordinatorSql(groupCount, aggKinds, sort);
        PlanFragment finalStage = PlanFragment.intermediate(1, coordinatorSql, ExchangeType.NONE, null);

        return SubPlan.distributed(List.of(leafStage, finalStage));
    }

    /**
     * Global aggregation: 2-stage plan with GATHER exchange.
     * Workers compute partial aggregates; coordinator re-aggregates.
     */
    private static SubPlan fragmentGlobalAggregate(String sql, SqlKind[] aggKinds, boolean hasAvg) {
        String workerSql = hasAvg ? decomposeAvg(sql) : sql;
        PlanFragment leafStage = PlanFragment.leaf(0, workerSql, ExchangeType.GATHER, null);

        String coordinatorSql = buildGlobalMergeCoordinatorSql(aggKinds, hasAvg);
        PlanFragment finalStage = PlanFragment.intermediate(1, coordinatorSql, ExchangeType.NONE, null);

        return SubPlan.distributed(List.of(leafStage, finalStage));
    }

    /**
     * ORDER BY + LIMIT: 2-stage plan with GATHER exchange (top-K merge).
     * ORDER BY without LIMIT: single-node (can't merge unsorted partitions correctly).
     */
    private static SubPlan fragmentSort(Sort sort, String sql, RelNode relNode) {
        if (sort.fetch == null) {
            return SubPlan.singleNode();
        }

        int limit = extractLimit(sort);
        int[] sortCols = extractSortColumns(sort);
        boolean[] sortAsc = extractSortDirections(sort);

        PlanFragment leafStage = PlanFragment.leaf(0, sql, ExchangeType.GATHER, null);

        String coordinatorSql = buildTopKCoordinatorSql(sortCols, sortAsc, limit);
        PlanFragment finalStage = PlanFragment.intermediate(1, coordinatorSql, ExchangeType.NONE, null);

        return SubPlan.distributed(List.of(leafStage, finalStage));
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
     */
    static String buildGlobalMergeCoordinatorSql(SqlKind[] aggKinds, boolean hasAvg) {
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
     * Builds coordinator SQL for two-phase GROUP BY without LIMIT.
     */
    static String buildTwoPhaseGroupByCoordinatorSql(int groupCount, SqlKind[] aggKinds, Sort sort) {
        StringBuilder sb = new StringBuilder("SELECT ");
        int totalAggs = aggKinds != null ? aggKinds.length : 0;
        int totalCols = groupCount + totalAggs;

        for (int i = 0; i < totalCols; i++) {
            if (i > 0) sb.append(", ");
            String col = "\"col_" + i + "\"";
            if (i < groupCount) {
                sb.append(col);
            } else {
                SqlKind kind = aggKinds[i - groupCount];
                sb.append(reAggFunction(kind)).append("(").append(col).append(")");
            }
        }

        sb.append(" FROM __exchange_input__ GROUP BY ");
        for (int i = 0; i < groupCount; i++) {
            if (i > 0) sb.append(", ");
            sb.append("\"col_").append(i).append("\"");
        }

        if (sort != null && sort.getCollation() != null && !sort.getCollation().getFieldCollations().isEmpty()) {
            int[] sortCols = extractSortColumns(sort);
            boolean[] sortAsc = extractSortDirections(sort);
            sb.append(" ORDER BY ");
            for (int i = 0; i < sortCols.length; i++) {
                if (i > 0) sb.append(", ");
                sb.append(sortCols[i] + 1);
                sb.append(sortAsc[i] ? " ASC" : " DESC");
            }
        }

        return sb.toString();
    }

    /**
     * Builds intermediate stage SQL for GROUP BY + LIMIT with HASH exchange.
     * The intermediate task owns a disjoint set of groups (from HASH partitioning),
     * so it can safely apply GROUP BY + ORDER BY + LIMIT.
     */
    static String buildIntermediateGroupBySql(int groupCount, SqlKind[] aggKinds, Sort sort, boolean hasAvg) {
        StringBuilder sb = new StringBuilder("SELECT ");
        int totalAggs = aggKinds != null ? aggKinds.length : 0;

        int workerCol = 0;
        for (int i = 0; i < groupCount; i++) {
            if (i > 0) sb.append(", ");
            sb.append("\"col_").append(workerCol).append("\"");
            workerCol++;
        }

        for (int i = 0; i < totalAggs; i++) {
            sb.append(", ");
            SqlKind kind = aggKinds[i];
            if (kind == SqlKind.AVG && hasAvg) {
                // AVG was decomposed into SUM+COUNT on workers; recombine
                String sumCol = "\"col_" + workerCol + "\"";
                String countCol = "\"col_" + (workerCol + 1) + "\"";
                sb.append("CAST(SUM(").append(sumCol).append(") AS DOUBLE) / SUM(").append(countCol).append(")");
                workerCol += 2;
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

        if (sort.getCollation() != null && !sort.getCollation().getFieldCollations().isEmpty()) {
            int[] sortCols = extractSortColumns(sort);
            boolean[] sortAsc = extractSortDirections(sort);
            sb.append(" ORDER BY ");
            for (int i = 0; i < sortCols.length; i++) {
                if (i > 0) sb.append(", ");
                sb.append(sortCols[i] + 1);
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
     */
    static String buildTopKCoordinatorSql(int[] sortCols, boolean[] sortAsc, int limit) {
        StringBuilder sb = new StringBuilder("SELECT * FROM __exchange_input__ ORDER BY ");
        for (int i = 0; i < sortCols.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append("\"col_").append(sortCols[i]).append("\"");
            sb.append(sortAsc[i] ? " ASC" : " DESC");
        }
        if (limit > 0) {
            sb.append(" LIMIT ").append(limit);
        }
        return sb.toString();
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

    private static String reAggFunction(SqlKind kind) {
        return switch (kind) {
            case MIN -> "MIN";
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
                if (!s.getCollation().getFieldCollations().isEmpty()) {
                    sort = s;
                }
            }
            super.visit(node, ordinal, parent);
        }
    }
}

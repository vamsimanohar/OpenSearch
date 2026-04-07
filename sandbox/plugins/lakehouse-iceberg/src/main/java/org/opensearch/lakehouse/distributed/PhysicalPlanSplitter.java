/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.lakehouse.exec.DataFusionSqlDialect;

import java.util.ArrayList;
import java.util.List;

/**
 * Splits a Calcite logical plan into two SQL strings for distributed execution:
 * <ul>
 *   <li><b>workerSql</b>: Executed on each worker node against a subset of files.
 *       Contains scan + filter + partial aggregation (no ORDER BY, no LIMIT).</li>
 *   <li><b>coordinatorSql</b>: Executed on the coordinator against the merged
 *       partial results. Contains final aggregation + ORDER BY + LIMIT.</li>
 * </ul>
 *
 * <p>The key insight is that aggregates must be decomposed for distributed execution:
 * <ul>
 *   <li>{@code COUNT(*)} on worker becomes {@code COUNT(*) AS __c0};
 *       coordinator merges with {@code SUM(__c0)}</li>
 *   <li>{@code SUM(col)} on worker becomes {@code SUM(col) AS __c1};
 *       coordinator merges with {@code SUM(__c1)}</li>
 *   <li>{@code MIN(col)} / {@code MAX(col)} are pass-through</li>
 *   <li>{@code AVG(col)} is decomposed into {@code SUM(col)} and {@code COUNT(col)};
 *       coordinator computes {@code CAST(SUM(__s) AS DOUBLE) / SUM(__n)}</li>
 * </ul>
 */
public final class PhysicalPlanSplitter {

    private static final Logger logger = LogManager.getLogger(PhysicalPlanSplitter.class);

    /** Name of the virtual table in coordinator SQL that holds partial worker results. */
    static final String PARTIAL_TABLE = "__partial";

    private PhysicalPlanSplitter() {}

    /** How the coordinator should merge partial worker results. */
    public enum MergeType {
        /** No aggregation — just return concatenated partial results. */
        PASS_THROUGH,
        /** Scan-only query with ORDER BY + LIMIT — sort and limit in Java. */
        SCAN_WITH_SORT,
        /** Aggregate query — re-aggregate using HashMap merge, then sort + limit. */
        AGGREGATE
    }

    /** Merge operation for a single output column. */
    public enum MergeOp {
        /** Pass through (group key or identity column). */
        IDENTITY,
        /** SUM the partial values. */
        SUM,
        /** MIN of partial values. */
        MIN,
        /** MAX of partial values. */
        MAX
    }

    /** Describes one output column of the coordinator merge. */
    public static class MergeColumn {
        final MergeOp op;
        final int sourceIndex;       // index in the partial result row
        final int sourceIndex2;      // -1 unless AVG (second source for count)
        final boolean isAvg;         // if true, output = SUM(sourceIndex) / SUM(sourceIndex2)

        MergeColumn(MergeOp op, int sourceIndex) {
            this(op, sourceIndex, -1, false);
        }

        MergeColumn(MergeOp op, int sourceIndex, int sourceIndex2, boolean isAvg) {
            this.op = op;
            this.sourceIndex = sourceIndex;
            this.sourceIndex2 = sourceIndex2;
            this.isAvg = isAvg;
        }
    }

    /** Sort direction for coordinator ORDER BY. */
    public static class SortColumn {
        final int outputIndex;       // index in the merged output row
        final boolean descending;
        final boolean nullsFirst;

        SortColumn(int outputIndex, boolean descending, boolean nullsFirst) {
            this.outputIndex = outputIndex;
            this.descending = descending;
            this.nullsFirst = nullsFirst;
        }
    }

    /**
     * Result of splitting a query plan into worker and coordinator SQL.
     */
    public static class SplitPlan {
        private final String workerSql;
        private final String coordinatorSql;
        private final boolean canDistribute;
        private final MergeType mergeType;
        private final int groupKeyCount;
        private final List<MergeColumn> mergeColumns;
        private final List<SortColumn> sortColumns;
        private final long limit;

        SplitPlan(String workerSql, String coordinatorSql, boolean canDistribute,
                  MergeType mergeType, int groupKeyCount, List<MergeColumn> mergeColumns,
                  List<SortColumn> sortColumns, long limit) {
            this.workerSql = workerSql;
            this.coordinatorSql = coordinatorSql;
            this.canDistribute = canDistribute;
            this.mergeType = mergeType;
            this.groupKeyCount = groupKeyCount;
            this.mergeColumns = mergeColumns != null ? mergeColumns : List.of();
            this.sortColumns = sortColumns != null ? sortColumns : List.of();
            this.limit = limit;
        }

        /** SQL to execute on each worker (partial aggregation, no sort/limit). */
        public String getWorkerSql() { return workerSql; }

        /** SQL to execute on coordinator against merged partial results. */
        public String getCoordinatorSql() { return coordinatorSql; }

        /** Whether the query can be distributed. False for unsupported aggregates. */
        public boolean canDistribute() { return canDistribute; }

        /** How the coordinator should merge partial results. */
        public MergeType getMergeType() { return mergeType; }

        /** Number of group key columns (first N columns in partial results). */
        public int getGroupKeyCount() { return groupKeyCount; }

        /** Merge operations for each output column (after group keys). */
        public List<MergeColumn> getMergeColumns() { return mergeColumns; }

        /** Sort specification for the final output. */
        public List<SortColumn> getSortColumns() { return sortColumns; }

        /** Maximum rows in the final output (-1 = unlimited). */
        public long getLimit() { return limit; }

        @Override
        public String toString() {
            return "SplitPlan{canDistribute=" + canDistribute
                + ", mergeType=" + mergeType
                + ", workerSql=" + workerSql
                + ", coordinatorSql=" + coordinatorSql + "}";
        }
    }

    /**
     * Splits a Calcite RelNode into worker and coordinator SQL strings.
     *
     * @param plan      the Calcite logical plan (root node)
     * @param tableName the table name as registered in DataFusion
     * @return the split plan with worker and coordinator SQL
     */
    public static SplitPlan split(RelNode plan, String tableName) {
        // Walk the plan to extract components
        LogicalSort sort = null;
        LogicalProject topProject = null;
        LogicalAggregate aggregate = null;

        RelNode current = plan;

        // Peel off the top-level sort (ORDER BY + LIMIT)
        if (current instanceof LogicalSort) {
            sort = (LogicalSort) current;
            current = sort.getInput();
        }

        // Peel off a top-level project (column aliases, expressions)
        if (current instanceof LogicalProject) {
            topProject = (LogicalProject) current;
            current = topProject.getInput();
        }

        // Check for aggregate
        if (current instanceof LogicalAggregate) {
            aggregate = (LogicalAggregate) current;
        }

        if (aggregate == null) {
            // SCAN-ONLY query: worker does everything except ORDER BY + LIMIT
            return splitScanOnly(plan, tableName, sort);
        }

        // AGGREGATE query: analyze aggregate calls for decomposition
        return splitAggregate(plan, tableName, aggregate, topProject, sort);
    }

    /**
     * Splits a scan-only (no aggregation) query.
     * Worker: original SQL without ORDER BY and LIMIT
     * Coordinator: SELECT * FROM __partial ORDER BY ... LIMIT ...
     */
    private static SplitPlan splitScanOnly(RelNode plan, String tableName, LogicalSort sort) {
        String fullSql = relToSql(plan, tableName);

        if (sort == null) {
            // No sort/limit: worker does everything, coordinator is pass-through
            return new SplitPlan(fullSql, "SELECT * FROM " + PARTIAL_TABLE, true,
                MergeType.PASS_THROUGH, 0, null, null, -1);
        }

        // Generate worker SQL from the sort's input (everything below the sort)
        String workerSql = relToSql(sort.getInput(), tableName);

        // Build coordinator SQL with ORDER BY + LIMIT from the sort
        StringBuilder coordSql = new StringBuilder("SELECT * FROM ").append(PARTIAL_TABLE);
        appendOrderByAndLimit(coordSql, sort);

        // Extract sort metadata
        List<SortColumn> sortCols = extractSortColumns(sort);
        long limit = extractLimit(sort);

        return new SplitPlan(workerSql, coordSql.toString(), true,
            MergeType.SCAN_WITH_SORT, 0, null, sortCols, limit);
    }

    /**
     * Splits an aggregate query into partial (worker) and final (coordinator) phases.
     */
    private static SplitPlan splitAggregate(
        RelNode plan, String tableName,
        LogicalAggregate aggregate, LogicalProject topProject, LogicalSort sort
    ) {
        List<AggregateCall> aggCalls = aggregate.getAggCallList();
        int groupCount = aggregate.getGroupSet().cardinality();

        // Collect group key field names from the aggregate's input row type
        List<String> inputFieldNames = aggregate.getInput().getRowType().getFieldNames();
        List<String> groupColumns = new ArrayList<>();
        int[] groupIndices = aggregate.getGroupSet().toArray();
        for (int idx : groupIndices) {
            groupColumns.add(quoteIdentifier(inputFieldNames.get(idx)));
        }

        // Analyze aggregate calls and build partial/final expressions
        List<String> workerSelectExprs = new ArrayList<>(groupColumns);
        List<String> coordSelectExprs = new ArrayList<>(groupColumns);
        List<String> coordFinalExprs = new ArrayList<>(); // for the final SELECT aliases
        List<MergeColumn> mergeColumns = new ArrayList<>();

        int aliasCounter = 0;
        int workerColIndex = groupCount; // track column index in worker output
        boolean hasUnsupported = false;

        // Track original output names for the coordinator to alias correctly
        List<String> aggOutputFieldNames = aggregate.getRowType().getFieldNames();

        for (int i = 0; i < aggCalls.size(); i++) {
            AggregateCall call = aggCalls.get(i);
            SqlKind kind = call.getAggregation().getKind();
            String outputName = aggOutputFieldNames.get(groupCount + i);

            switch (kind) {
                case COUNT: {
                    String alias = "__c" + aliasCounter++;
                    if (call.getArgList().isEmpty()) {
                        workerSelectExprs.add("COUNT(*) AS " + quoteIdentifier(alias));
                    } else {
                        String colName = inputFieldNames.get(call.getArgList().get(0));
                        workerSelectExprs.add("COUNT(" + quoteIdentifier(colName) + ") AS " + quoteIdentifier(alias));
                    }
                    coordSelectExprs.add("SUM(" + quoteIdentifier(alias) + ") AS " + quoteIdentifier(outputName));
                    mergeColumns.add(new MergeColumn(MergeOp.SUM, workerColIndex));
                    workerColIndex++;
                    break;
                }
                case SUM:
                case SUM0: {
                    String alias = "__c" + aliasCounter++;
                    String colName = inputFieldNames.get(call.getArgList().get(0));
                    workerSelectExprs.add("SUM(" + quoteIdentifier(colName) + ") AS " + quoteIdentifier(alias));
                    coordSelectExprs.add("SUM(" + quoteIdentifier(alias) + ") AS " + quoteIdentifier(outputName));
                    mergeColumns.add(new MergeColumn(MergeOp.SUM, workerColIndex));
                    workerColIndex++;
                    break;
                }
                case MIN: {
                    String alias = "__c" + aliasCounter++;
                    String colName = inputFieldNames.get(call.getArgList().get(0));
                    workerSelectExprs.add("MIN(" + quoteIdentifier(colName) + ") AS " + quoteIdentifier(alias));
                    coordSelectExprs.add("MIN(" + quoteIdentifier(alias) + ") AS " + quoteIdentifier(outputName));
                    mergeColumns.add(new MergeColumn(MergeOp.MIN, workerColIndex));
                    workerColIndex++;
                    break;
                }
                case MAX: {
                    String alias = "__c" + aliasCounter++;
                    String colName = inputFieldNames.get(call.getArgList().get(0));
                    workerSelectExprs.add("MAX(" + quoteIdentifier(colName) + ") AS " + quoteIdentifier(alias));
                    coordSelectExprs.add("MAX(" + quoteIdentifier(alias) + ") AS " + quoteIdentifier(outputName));
                    mergeColumns.add(new MergeColumn(MergeOp.MAX, workerColIndex));
                    workerColIndex++;
                    break;
                }
                case AVG: {
                    String sumAlias = "__c" + aliasCounter++;
                    String countAlias = "__c" + aliasCounter++;
                    String colName = inputFieldNames.get(call.getArgList().get(0));
                    workerSelectExprs.add("SUM(" + quoteIdentifier(colName) + ") AS " + quoteIdentifier(sumAlias));
                    workerSelectExprs.add("COUNT(" + quoteIdentifier(colName) + ") AS " + quoteIdentifier(countAlias));
                    coordSelectExprs.add(
                        "CAST(SUM(" + quoteIdentifier(sumAlias) + ") AS DOUBLE) / SUM("
                            + quoteIdentifier(countAlias) + ") AS " + quoteIdentifier(outputName)
                    );
                    // AVG merge: sum column at workerColIndex, count column at workerColIndex+1
                    mergeColumns.add(new MergeColumn(MergeOp.SUM, workerColIndex, workerColIndex + 1, true));
                    workerColIndex += 2;
                    break;
                }
                default:
                    // Unsupported aggregate (MEDIAN, PERCENTILE, etc.)
                    logger.warn("[PhysicalPlanSplitter] Unsupported aggregate for distribution: {}", kind);
                    hasUnsupported = true;
                    break;
            }
        }

        if (hasUnsupported) {
            return new SplitPlan(null, null, false, null, 0, null, null, -1);
        }

        // Build worker SQL: SELECT partial_aggs FROM table WHERE ... GROUP BY group_cols
        // We need the filter part from below the aggregate
        String filterClause = extractFilterClause(aggregate.getInput(), tableName);
        StringBuilder workerSql = new StringBuilder("SELECT ");
        workerSql.append(String.join(", ", workerSelectExprs));
        workerSql.append(" FROM ").append(quoteIdentifier(tableName));
        if (filterClause != null && !filterClause.isEmpty()) {
            workerSql.append(" WHERE ").append(filterClause);
        }
        if (!groupColumns.isEmpty()) {
            workerSql.append(" GROUP BY ").append(String.join(", ", groupColumns));
        }

        // Build coordinator SQL: SELECT final_aggs FROM __partial GROUP BY group_cols ORDER BY ... LIMIT ...
        StringBuilder coordSql = new StringBuilder("SELECT ");

        // If there's a topProject, we need to handle column rewriting
        if (topProject != null) {
            coordSql.append(buildCoordinatorSelectWithProject(topProject, coordSelectExprs, groupColumns, aggregate));
        } else {
            coordSql.append(String.join(", ", coordSelectExprs));
        }
        coordSql.append(" FROM ").append(PARTIAL_TABLE);
        if (!groupColumns.isEmpty()) {
            coordSql.append(" GROUP BY ").append(String.join(", ", groupColumns));
        }
        if (sort != null) {
            appendOrderByAndLimit(coordSql, sort);
        }

        // Extract sort and limit metadata for the coordinator merge
        List<SortColumn> sortCols = sort != null ? extractSortColumns(sort) : null;
        long limitVal = sort != null ? extractLimit(sort) : -1;

        logger.debug("[PhysicalPlanSplitter] Worker SQL: {}", workerSql);
        logger.debug("[PhysicalPlanSplitter] Coordinator SQL: {}", coordSql);

        return new SplitPlan(workerSql.toString(), coordSql.toString(), true,
            MergeType.AGGREGATE, groupCount, mergeColumns, sortCols, limitVal);
    }

    /**
     * Builds the coordinator SELECT list when there's a top project above the aggregate.
     * The project may rename or reorder columns.
     */
    private static String buildCoordinatorSelectWithProject(
        LogicalProject project,
        List<String> coordSelectExprs,
        List<String> groupColumns,
        LogicalAggregate aggregate
    ) {
        // The project's expressions reference the aggregate's output by index.
        // We map each project output to the corresponding coordinator expression.
        List<RexNode> projectExprs = project.getProjects();
        List<String> projectFieldNames = project.getRowType().getFieldNames();
        int groupCount = groupColumns.size();

        List<String> result = new ArrayList<>();
        for (int i = 0; i < projectExprs.size(); i++) {
            RexNode expr = projectExprs.get(i);
            if (expr instanceof RexInputRef) {
                int refIdx = ((RexInputRef) expr).getIndex();
                if (refIdx < groupCount) {
                    // Reference to a group key
                    result.add(groupColumns.get(refIdx) + " AS " + quoteIdentifier(projectFieldNames.get(i)));
                } else {
                    // Reference to an aggregate output
                    int aggIdx = refIdx - groupCount;
                    if (aggIdx < coordSelectExprs.size() - groupCount) {
                        result.add(coordSelectExprs.get(groupCount + aggIdx));
                    } else {
                        result.add(coordSelectExprs.get(refIdx));
                    }
                }
            } else {
                // Complex expression — fall back to simple mapping
                if (i < coordSelectExprs.size()) {
                    result.add(coordSelectExprs.get(i));
                }
            }
        }

        return String.join(", ", result);
    }

    /**
     * Extracts the WHERE clause from a filter node below an aggregate.
     * Returns the filter condition as a SQL string, or null if no filter.
     */
    private static String extractFilterClause(RelNode node, String tableName) {
        if (node instanceof LogicalFilter) {
            LogicalFilter filter = (LogicalFilter) node;
            // Use RelToSqlConverter to convert the filter's condition subtree
            // We generate SQL for the entire filter+scan, then extract the WHERE clause
            String fullSql = relToSql(filter, tableName);
            // RelToSqlConverter may produce multi-line SQL with \n before WHERE
            String normalized = fullSql.replaceAll("\\s+", " ").trim();
            int whereIdx = normalized.toUpperCase().indexOf(" WHERE ");
            if (whereIdx >= 0) {
                return normalized.substring(whereIdx + 7);
            }
            logger.warn("[PhysicalPlanSplitter] extractFilterClause: no WHERE found in SQL: {}", fullSql);
        }
        // Walk down to find filter
        for (RelNode input : node.getInputs()) {
            String result = extractFilterClause(input, tableName);
            if (result != null) return result;
        }
        return null;
    }

    /**
     * Converts a RelNode to a SQL string using the DataFusion dialect.
     * Strips schema qualifiers from the table name.
     */
    static String relToSql(RelNode node, String tableName) {
        SqlDialect dialect = DataFusionSqlDialect.DEFAULT;
        RelToSqlConverter converter = new RelToSqlConverter(dialect);
        SqlNode sqlNode = converter.visitRoot(node).asStatement();
        String sql = sqlNode.toSqlString(dialect).getSql();

        // Strip schema qualifiers: "schema"."table" -> "table"
        String quotedTable = "\"" + tableName + "\"";
        sql = sql.replaceAll("\"\\w+\"\\." + java.util.regex.Pattern.quote(quotedTable), quotedTable);

        // Lowercase all double-quoted identifiers to match Iceberg/Parquet schema case.
        // DataFusion treats quoted identifiers as case-sensitive.
        sql = lowercaseQuotedIdentifiers(sql);
        return sql;
    }

    /**
     * Lowercases all double-quoted identifiers in a SQL string.
     * Handles escaped quotes ({@code ""}) inside identifiers.
     */
    private static String lowercaseQuotedIdentifiers(String sql) {
        StringBuilder sb = new StringBuilder(sql.length());
        int i = 0;
        while (i < sql.length()) {
            if (sql.charAt(i) == '"') {
                // Find the end of the quoted identifier
                int start = i;
                i++;
                while (i < sql.length()) {
                    if (sql.charAt(i) == '"') {
                        if (i + 1 < sql.length() && sql.charAt(i + 1) == '"') {
                            i += 2; // escaped quote
                        } else {
                            i++; // end of identifier
                            break;
                        }
                    } else {
                        i++;
                    }
                }
                sb.append(sql.substring(start, i).toLowerCase(java.util.Locale.ROOT));
            } else if (sql.charAt(i) == '\'') {
                // Skip string literals — don't lowercase them
                sb.append(sql.charAt(i));
                i++;
                while (i < sql.length()) {
                    sb.append(sql.charAt(i));
                    if (sql.charAt(i) == '\'') {
                        if (i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
                            sb.append(sql.charAt(i + 1));
                            i += 2;
                        } else {
                            i++;
                            break;
                        }
                    } else {
                        i++;
                    }
                }
            } else {
                sb.append(sql.charAt(i));
                i++;
            }
        }
        return sb.toString();
    }

    /**
     * Appends ORDER BY and LIMIT clauses from a LogicalSort to a SQL builder.
     */
    private static void appendOrderByAndLimit(StringBuilder sql, LogicalSort sort) {
        List<RelFieldCollation> collations = sort.getCollation().getFieldCollations();
        if (!collations.isEmpty()) {
            sql.append(" ORDER BY ");
            List<String> orderTerms = new ArrayList<>();
            List<String> fieldNames = sort.getInput().getRowType().getFieldNames();
            for (RelFieldCollation c : collations) {
                String colName = quoteIdentifier(fieldNames.get(c.getFieldIndex()));
                String dir = c.getDirection() == RelFieldCollation.Direction.ASCENDING ? "ASC" : "DESC";
                String nullDir;
                if (c.nullDirection == RelFieldCollation.NullDirection.FIRST) {
                    nullDir = " NULLS FIRST";
                } else if (c.nullDirection == RelFieldCollation.NullDirection.LAST) {
                    nullDir = " NULLS LAST";
                } else {
                    nullDir = "";
                }
                orderTerms.add(colName + " " + dir + nullDir);
            }
            sql.append(String.join(", ", orderTerms));
        }

        if (sort.fetch != null) {
            long limit = -1;
            if (sort.fetch instanceof RexLiteral) {
                limit = ((Number) ((RexLiteral) sort.fetch).getValue()).longValue();
            }
            if (limit >= 0) {
                sql.append(" LIMIT ").append(limit);
            }
        }

        if (sort.offset != null) {
            long offset = 0;
            if (sort.offset instanceof RexLiteral) {
                offset = ((Number) ((RexLiteral) sort.offset).getValue()).longValue();
            }
            if (offset > 0) {
                sql.append(" OFFSET ").append(offset);
            }
        }
    }

    /**
     * Extracts sort column metadata from a LogicalSort for the Java-side merge.
     */
    private static List<SortColumn> extractSortColumns(LogicalSort sort) {
        List<SortColumn> result = new ArrayList<>();
        List<RelFieldCollation> collations = sort.getCollation().getFieldCollations();
        for (RelFieldCollation c : collations) {
            boolean desc = c.getDirection() == RelFieldCollation.Direction.DESCENDING;
            boolean nullsFirst = c.nullDirection == RelFieldCollation.NullDirection.FIRST;
            result.add(new SortColumn(c.getFieldIndex(), desc, nullsFirst));
        }
        return result;
    }

    /**
     * Extracts the LIMIT value from a LogicalSort, or -1 if no limit.
     */
    private static long extractLimit(LogicalSort sort) {
        if (sort.fetch instanceof RexLiteral) {
            return ((Number) ((RexLiteral) sort.fetch).getValue()).longValue();
        }
        return -1;
    }

    /**
     * Double-quotes an identifier for DataFusion SQL, lowercasing to match
     * Iceberg/Parquet schema conventions. DataFusion treats quoted identifiers
     * as case-sensitive, so {@code "URL"} would fail against a Parquet column
     * named {@code url}.
     */
    private static String quoteIdentifier(String name) {
        return "\"" + name.toLowerCase(java.util.Locale.ROOT).replace("\"", "\"\"") + "\"";
    }
}

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
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.lakehouse.distributed.merge.MergeStrategy;

import java.util.List;

/**
 * Inspects a Calcite {@link RelNode} tree to determine the appropriate {@link MergeStrategy}
 * for distributed query execution.
 * <p>
 * Uses Calcite's {@link RelVisitor} pattern for idiomatic tree traversal.
 *
 * @opensearch.internal
 */
public final class QueryAnalyzer {

    private QueryAnalyzer() {}

    public static MergeStrategy analyze(RelNode relNode) {
        return analyzeDetailed(relNode).strategy;
    }

    public static AnalysisResult analyzeDetailed(RelNode relNode) {
        PlanClassifier classifier = new PlanClassifier();
        classifier.go(relNode);

        if (classifier.aggregate != null) {
            boolean hasGroupBy = !classifier.aggregate.getGroupSet().isEmpty();
            boolean hasDistinct = hasDistinct(classifier.aggregate);

            if (hasGroupBy) {
                if (hasDistinct) {
                    if (hasOnlyCountDistinct(classifier.aggregate)) {
                        return buildDistinctExpandResult(classifier, relNode);
                    }
                    return new AnalysisResult(MergeStrategy.SINGLE_NODE);
                }
                // AVG is allowed — decomposed into SUM/COUNT on workers
                return buildTwoPhaseResult(classifier, relNode);
            }

            if (hasDistinct) {
                if (hasOnlyCountDistinct(classifier.aggregate)) {
                    return buildDistinctExpandResult(classifier, relNode);
                }
                return new AnalysisResult(MergeStrategy.SINGLE_NODE);
            }
            // Global aggregates (including AVG) — AVG decomposed into SUM/COUNT on workers
            SqlKind[] aggKinds = extractAggKinds(classifier.aggregate);
            return new AnalysisResult(MergeStrategy.GLOBAL_MERGE, aggKinds, null, null, 0, null);
        }

        if (classifier.sort != null) {
            if (!classifier.sort.getCollation().getFieldCollations().isEmpty() && classifier.sort.fetch != null) {
                int[] sortColumns = extractSortColumns(classifier.sort);
                int outputFieldCount = relNode.getRowType().getFieldCount();
                for (int col : sortColumns) {
                    if (col >= outputFieldCount) {
                        return new AnalysisResult(MergeStrategy.SINGLE_NODE);
                    }
                }
                boolean[] sortAsc = extractSortDirections(classifier.sort);
                int limit = extractLimit(classifier.sort);
                return new AnalysisResult(MergeStrategy.TOPK_MERGE, null, sortColumns, sortAsc, limit, null);
            }
            return new AnalysisResult(MergeStrategy.SINGLE_NODE);
        }

        return new AnalysisResult(MergeStrategy.CONCAT);
    }

    /**
     * Builds a TWO_PHASE_GROUP_BY result. Computes output column count from
     * Aggregate/Project structure — never calls relNode.getRowType().
     */
    private static AnalysisResult buildTwoPhaseResult(PlanClassifier classifier, RelNode relNode) {
        Aggregate aggregate = classifier.aggregate;
        Sort sort = classifier.sort;

        int groupKeyCount = aggregate.getGroupSet().cardinality();
        SqlKind[] rawAggKinds = extractAggKinds(aggregate);

        // Find the node directly above the Aggregate (skip Sort)
        RelNode nodeAboveAggregate = (sort != null) ? sort.getInput() : relNode;

        // Look through Filter (HAVING clause) to find the Aggregate or Project
        HavingCondition having = null;
        if (nodeAboveAggregate instanceof Filter) {
            Filter havingFilter = (Filter) nodeAboveAggregate;
            having = extractHavingCondition(havingFilter, groupKeyCount);
            nodeAboveAggregate = havingFilter.getInput();
        }

        // Determine column roles from plan structure
        boolean[] isGroupKey;
        SqlKind[] outputAggKinds;
        int outputColCount;

        if (nodeAboveAggregate instanceof Aggregate) {
            // Simple: Sort → Aggregate (most common for GROUP BY queries)
            outputColCount = groupKeyCount + rawAggKinds.length;
            isGroupKey = new boolean[outputColCount];
            outputAggKinds = new SqlKind[outputColCount];
            for (int i = 0; i < outputColCount; i++) {
                if (i < groupKeyCount) {
                    isGroupKey[i] = true;
                } else {
                    int aggIdx = i - groupKeyCount;
                    outputAggKinds[i] = (aggIdx < rawAggKinds.length) ? rawAggKinds[aggIdx] : SqlKind.SUM;
                }
            }
        } else if (nodeAboveAggregate instanceof Project) {
            // Sort → Project → Aggregate (e.g., SELECT 1 AS "one", url, COUNT(*))
            Project project = (Project) nodeAboveAggregate;
            List<RexNode> projects = project.getProjects();
            outputColCount = projects.size();
            isGroupKey = new boolean[outputColCount];
            outputAggKinds = new SqlKind[outputColCount];
            for (int i = 0; i < outputColCount; i++) {
                RexNode expr = projects.get(i);
                if (expr instanceof RexInputRef) {
                    int inputIdx = ((RexInputRef) expr).getIndex();
                    if (inputIdx < groupKeyCount) {
                        isGroupKey[i] = true;
                    } else {
                        int aggIdx = inputIdx - groupKeyCount;
                        outputAggKinds[i] = (aggIdx < rawAggKinds.length) ? rawAggKinds[aggIdx] : SqlKind.SUM;
                    }
                } else {
                    // Literal or complex expression — treat as GROUP BY key in merge
                    isGroupKey[i] = true;
                }
            }
        } else {
            // Unknown structure (e.g., Filter for HAVING) — fall back to SINGLE_NODE
            return new AnalysisResult(MergeStrategy.SINGLE_NODE);
        }

        // Extract sort/limit info
        int[] sortColumns = null;
        boolean[] sortAsc = null;
        int limit = 0;

        if (sort != null) {
            if (!sort.getCollation().getFieldCollations().isEmpty()) {
                sortColumns = extractSortColumns(sort);
                sortAsc = extractSortDirections(sort);
                for (int col : sortColumns) {
                    if (col >= outputColCount) {
                        return new AnalysisResult(MergeStrategy.SINGLE_NODE);
                    }
                }
            }
            if (sort.fetch != null) {
                limit = extractLimit(sort);
            }
        }

        return new AnalysisResult(MergeStrategy.TWO_PHASE_GROUP_BY, outputAggKinds, sortColumns, sortAsc, limit, isGroupKey, having);
    }

    /**
     * Extracts a HAVING condition from a Filter node above an Aggregate.
     * Handles simple comparison conditions like {@code column > value}.
     *
     * @param filter the Filter node representing the HAVING clause
     * @param groupKeyCount number of GROUP BY keys in the Aggregate output
     * @return the extracted HavingCondition, or null if the condition is too complex
     */
    static HavingCondition extractHavingCondition(Filter filter, int groupKeyCount) {
        RexNode condition = filter.getCondition();
        if (!(condition instanceof RexCall)) return null;

        RexCall call = (RexCall) condition;
        SqlKind op = call.getKind();
        if (call.getOperands().size() != 2) return null;

        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);

        if (left instanceof RexInputRef && right instanceof RexLiteral) {
            int colIndex = ((RexInputRef) left).getIndex();
            Number value = ((RexLiteral) right).getValueAs(Number.class);
            if (value != null) {
                return new HavingCondition(colIndex, op, value.longValue());
            }
        }
        return null;
    }

    /**
     * Builds a DISTINCT_EXPAND result for pure COUNT(DISTINCT) queries.
     * The analysis captures sort/limit info so the coordinator can apply ORDER BY + LIMIT.
     */
    private static AnalysisResult buildDistinctExpandResult(PlanClassifier classifier, RelNode relNode) {
        Sort sort = classifier.sort;

        // Extract sort/limit from the plan
        int[] sortColumns = null;
        boolean[] sortAsc = null;
        int limit = 0;

        if (sort != null) {
            if (!sort.getCollation().getFieldCollations().isEmpty()) {
                sortColumns = extractSortColumns(sort);
                sortAsc = extractSortDirections(sort);
            }
            if (sort.fetch != null) {
                limit = extractLimit(sort);
            }
        }

        return new AnalysisResult(MergeStrategy.DISTINCT_EXPAND, null, sortColumns, sortAsc, limit, null);
    }

    static class PlanClassifier extends RelVisitor {
        Aggregate aggregate;
        Sort sort;

        @Override
        public void visit(RelNode node, int ordinal, RelNode parent) {
            if (node instanceof Aggregate && aggregate == null) {
                aggregate = (Aggregate) node;
            } else if (node instanceof Sort && sort == null) {
                sort = (Sort) node;
            }
            super.visit(node, ordinal, parent);
        }
    }

    static boolean hasDistinctOrAvg(Aggregate aggregate) {
        for (AggregateCall call : aggregate.getAggCallList()) {
            if (call.isDistinct()) {
                return true;
            }
            if (call.getAggregation().getKind() == SqlKind.AVG) {
                return true;
            }
        }
        return false;
    }

    static boolean hasDistinct(Aggregate aggregate) {
        for (AggregateCall call : aggregate.getAggCallList()) {
            if (call.isDistinct()) {
                return true;
            }
        }
        return false;
    }

    static boolean hasAvg(Aggregate aggregate) {
        for (AggregateCall call : aggregate.getAggCallList()) {
            if (call.getAggregation().getKind() == SqlKind.AVG) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns true if ALL aggregate calls are COUNT(DISTINCT ...) and nothing else.
     * Mixed queries (e.g., COUNT(*) + COUNT(DISTINCT x)) return false.
     */
    static boolean hasOnlyCountDistinct(Aggregate aggregate) {
        List<AggregateCall> calls = aggregate.getAggCallList();
        if (calls.isEmpty()) return false;
        for (AggregateCall call : calls) {
            if (!call.isDistinct() || call.getAggregation().getKind() != SqlKind.COUNT) {
                return false;
            }
        }
        return true;
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

    /**
     * Represents a simple HAVING condition: column op value (e.g., COUNT(*) > 100000).
     */
    public static final class HavingCondition {
        public final int columnIndex;
        public final SqlKind operator;
        public final long value;

        HavingCondition(int columnIndex, SqlKind operator, long value) {
            this.columnIndex = columnIndex;
            this.operator = operator;
            this.value = value;
        }

        public String operatorSql() {
            return switch (operator) {
                case GREATER_THAN -> ">";
                case GREATER_THAN_OR_EQUAL -> ">=";
                case LESS_THAN -> "<";
                case LESS_THAN_OR_EQUAL -> "<=";
                case EQUALS -> "=";
                case NOT_EQUALS -> "!=";
                default -> ">";
            };
        }
    }

    /**
     * Result of query plan analysis, containing the merge strategy and metadata for distributed execution.
     */
    public static final class AnalysisResult {
        public final MergeStrategy strategy;
        public final SqlKind[] aggKinds;
        public final int[] sortColumns;
        public final boolean[] sortAsc;
        public final int limit;
        /** Per-output-column: true = GROUP BY key/literal, false = aggregate. */
        public final boolean[] isGroupKey;
        /** HAVING condition extracted from Filter above Aggregate, or null. */
        public final HavingCondition having;

        AnalysisResult(MergeStrategy strategy) {
            this(strategy, null, null, null, 0, null, null);
        }

        AnalysisResult(MergeStrategy strategy, SqlKind[] aggKinds, int[] sortColumns, boolean[] sortAsc, int limit, boolean[] isGroupKey) {
            this(strategy, aggKinds, sortColumns, sortAsc, limit, isGroupKey, null);
        }

        AnalysisResult(MergeStrategy strategy, SqlKind[] aggKinds, int[] sortColumns, boolean[] sortAsc, int limit, boolean[] isGroupKey,
            HavingCondition having) {
            this.strategy = strategy;
            this.aggKinds = aggKinds;
            this.sortColumns = sortColumns;
            this.sortAsc = sortAsc;
            this.limit = limit;
            this.isGroupKey = isGroupKey;
            this.having = having;
        }
    }
}

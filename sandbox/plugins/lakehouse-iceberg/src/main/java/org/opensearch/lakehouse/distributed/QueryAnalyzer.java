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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
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
            boolean hasDistinctOrAvg = hasDistinctOrAvg(classifier.aggregate);

            if (hasGroupBy) {
                if (hasDistinctOrAvg) {
                    return new AnalysisResult(MergeStrategy.SINGLE_NODE);
                }
                return buildTwoPhaseResult(classifier, relNode);
            }

            if (hasDistinctOrAvg) {
                return new AnalysisResult(MergeStrategy.SINGLE_NODE);
            }
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

        return new AnalysisResult(MergeStrategy.TWO_PHASE_GROUP_BY, outputAggKinds, sortColumns, sortAsc, limit, isGroupKey);
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

    public static final class AnalysisResult {
        public final MergeStrategy strategy;
        public final SqlKind[] aggKinds;
        public final int[] sortColumns;
        public final boolean[] sortAsc;
        public final int limit;
        /** Per-output-column: true = GROUP BY key/literal, false = aggregate. */
        public final boolean[] isGroupKey;

        AnalysisResult(MergeStrategy strategy) {
            this(strategy, null, null, null, 0, null);
        }

        AnalysisResult(MergeStrategy strategy, SqlKind[] aggKinds, int[] sortColumns, boolean[] sortAsc, int limit, boolean[] isGroupKey) {
            this.strategy = strategy;
            this.aggKinds = aggKinds;
            this.sortColumns = sortColumns;
            this.sortAsc = sortAsc;
            this.limit = limit;
            this.isGroupKey = isGroupKey;
        }
    }
}

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
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.lakehouse.distributed.merge.MergeStrategy;

import java.util.List;

/**
 * Inspects a Calcite {@link RelNode} tree to determine the appropriate {@link MergeStrategy}
 * for distributed query execution.
 * <p>
 * Uses Calcite's {@link RelVisitor} pattern for idiomatic tree traversal.
 * <p>
 * Phase 1 classification rules:
 * <ul>
 *   <li>{@link MergeStrategy#GLOBAL_MERGE} — global aggregation (no GROUP BY), with only
 *       SUM/COUNT/MIN/MAX aggregate functions (no AVG, no DISTINCT)</li>
 *   <li>{@link MergeStrategy#TOPK_MERGE} — ORDER BY with LIMIT, no aggregation</li>
 *   <li>{@link MergeStrategy#SINGLE_NODE} — GROUP BY, COUNT DISTINCT, AVG, or any other
 *       non-trivially distributable pattern</li>
 *   <li>{@link MergeStrategy#CONCAT} — simple scan/filter/project with no agg and no sort</li>
 * </ul>
 *
 * @opensearch.internal
 */
public final class QueryAnalyzer {

    private QueryAnalyzer() {}

    /**
     * Analyzes the RelNode tree and returns the appropriate merge strategy.
     *
     * @param relNode the root of the Calcite logical plan
     * @return the merge strategy to use for distributed execution
     */
    public static MergeStrategy analyze(RelNode relNode) {
        return analyzeDetailed(relNode).strategy;
    }

    /**
     * Analyzes the RelNode tree and returns a detailed result including merge strategy,
     * aggregate function kinds (for GLOBAL_MERGE), and sort/limit info (for TOPK_MERGE).
     *
     * @param relNode the root of the Calcite logical plan
     * @return the detailed analysis result
     */
    public static AnalysisResult analyzeDetailed(RelNode relNode) {
        PlanClassifier classifier = new PlanClassifier();
        classifier.go(relNode);

        if (classifier.aggregate != null) {
            if (hasDistinctOrAvg(classifier.aggregate)) {
                return new AnalysisResult(MergeStrategy.SINGLE_NODE);
            }
            SqlKind[] aggKinds = extractAggKinds(classifier.aggregate);
            if (!classifier.aggregate.getGroupSet().isEmpty()) {
                // GROUP BY + LIMIT must stay single-node: stripping LIMIT from workers
                // OOMs on high-cardinality GROUP BY, and keeping LIMIT is lossy.
                // Only distribute GROUP BY queries without LIMIT.
                if (classifier.sort != null && classifier.sort.fetch != null) {
                    return new AnalysisResult(MergeStrategy.SINGLE_NODE);
                }
                int groupCount = classifier.aggregate.getGroupSet().cardinality();
                int[] sortColumns = classifier.sort != null ? extractSortColumns(classifier.sort) : null;
                boolean[] sortAsc = classifier.sort != null ? extractSortDirections(classifier.sort) : null;
                return new AnalysisResult(MergeStrategy.TWO_PHASE_GROUP_BY, aggKinds, sortColumns, sortAsc, 0, null, 0, groupCount, 0);
            }
            return new AnalysisResult(MergeStrategy.GLOBAL_MERGE, aggKinds, null, null, 0);
        }

        if (classifier.sort != null) {
            if (classifier.sort.fetch != null) {
                int[] sortColumns = extractSortColumns(classifier.sort);
                boolean[] sortAsc = extractSortDirections(classifier.sort);
                int limit = extractLimit(classifier.sort);
                String[] sortColumnNames = extractSortColumnNames(classifier.sort);
                int outputColumnCount = relNode.getRowType().getFieldCount();
                return new AnalysisResult(MergeStrategy.TOPK_MERGE, null, sortColumns, sortAsc, limit, sortColumnNames, outputColumnCount);
            }
            // ORDER BY without LIMIT cannot be distributed via CONCAT (results would be unsorted)
            return new AnalysisResult(MergeStrategy.SINGLE_NODE);
        }

        return new AnalysisResult(MergeStrategy.CONCAT);
    }

    /**
     * Visitor that walks the RelNode tree to find Aggregate and Sort nodes.
     */
    static class PlanClassifier extends RelVisitor {
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

    /**
     * Checks if the aggregate has any DISTINCT aggregate calls or AVG functions.
     *
     * @param aggregate the aggregate node
     * @return true if any call is DISTINCT or AVG
     */
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

    /**
     * Extracts aggregate function kinds from the Aggregate node.
     */
    static SqlKind[] extractAggKinds(Aggregate aggregate) {
        List<AggregateCall> calls = aggregate.getAggCallList();
        SqlKind[] kinds = new SqlKind[calls.size()];
        for (int i = 0; i < calls.size(); i++) {
            kinds[i] = calls.get(i).getAggregation().getKind();
        }
        return kinds;
    }

    /**
     * Extracts sort column indices from the Sort node's collation.
     */
    static int[] extractSortColumns(Sort sort) {
        List<RelFieldCollation> collations = sort.getCollation().getFieldCollations();
        int[] cols = new int[collations.size()];
        for (int i = 0; i < collations.size(); i++) {
            cols[i] = collations.get(i).getFieldIndex();
        }
        return cols;
    }

    /**
     * Extracts sort directions (true=ascending) from the Sort node's collation.
     */
    static boolean[] extractSortDirections(Sort sort) {
        List<RelFieldCollation> collations = sort.getCollation().getFieldCollations();
        boolean[] asc = new boolean[collations.size()];
        for (int i = 0; i < collations.size(); i++) {
            asc[i] = collations.get(i).getDirection() == RelFieldCollation.Direction.ASCENDING;
        }
        return asc;
    }

    /**
     * Extracts the LIMIT value from the Sort node's fetch expression.
     * Returns 0 if fetch is null or not a literal.
     */
    static int extractLimit(Sort sort) {
        if (sort.fetch instanceof RexLiteral) {
            return ((RexLiteral) sort.fetch).getValueAs(Integer.class);
        }
        return 0;
    }

    /**
     * Extracts the OFFSET value from the Sort node's offset expression.
     * Returns 0 if offset is null or not a literal.
     */
    static int extractOffset(Sort sort) {
        if (sort.offset instanceof RexLiteral) {
            return ((RexLiteral) sort.offset).getValueAs(Integer.class);
        }
        return 0;
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
     * Result of plan analysis containing the merge strategy and associated metadata.
     */
    static final class AnalysisResult {
        final MergeStrategy strategy;
        final SqlKind[] aggKinds;
        final int[] sortColumns;
        final boolean[] sortAsc;
        final int limit;
        final String[] sortColumnNames;
        final int outputColumnCount;
        final int groupCount;
        final int offset;

        AnalysisResult(MergeStrategy strategy) {
            this(strategy, null, null, null, 0, null, 0, 0, 0);
        }

        AnalysisResult(MergeStrategy strategy, SqlKind[] aggKinds, int[] sortColumns, boolean[] sortAsc, int limit) {
            this(strategy, aggKinds, sortColumns, sortAsc, limit, null, 0, 0, 0);
        }

        AnalysisResult(
            MergeStrategy strategy,
            SqlKind[] aggKinds,
            int[] sortColumns,
            boolean[] sortAsc,
            int limit,
            String[] sortColumnNames,
            int outputColumnCount
        ) {
            this(strategy, aggKinds, sortColumns, sortAsc, limit, sortColumnNames, outputColumnCount, 0, 0);
        }

        AnalysisResult(
            MergeStrategy strategy,
            SqlKind[] aggKinds,
            int[] sortColumns,
            boolean[] sortAsc,
            int limit,
            String[] sortColumnNames,
            int outputColumnCount,
            int groupCount,
            int offset
        ) {
            this.strategy = strategy;
            this.aggKinds = aggKinds;
            this.sortColumns = sortColumns;
            this.sortAsc = sortAsc;
            this.limit = limit;
            this.sortColumnNames = sortColumnNames;
            this.outputColumnCount = outputColumnCount;
            this.groupCount = groupCount;
            this.offset = offset;
        }
    }
}

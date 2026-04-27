/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.engine;

/**
 * A single stage in a distributed query plan.
 * <p>
 * Each fragment represents a unit of work that executes as a DataFusion SQL query
 * on one or more nodes. Fragments are connected by exchanges: a fragment's output
 * feeds into the next stage via the specified {@link ExchangeType}.
 * <p>
 * Leaf fragments read from data files (Parquet on S3). Intermediate and final
 * fragments read from {@code __exchange_input__} — a virtual table populated
 * by Arrow Flight streams from the previous stage's output.
 *
 * @opensearch.internal
 */
public final class PlanFragment {

    private final int stageId;
    private final String sql;
    private final ExchangeType outputExchange;
    private final int[] hashColumns;
    private final boolean leaf;

    private PlanFragment(int stageId, String sql, ExchangeType outputExchange, int[] hashColumns, boolean leaf) {
        this.stageId = stageId;
        this.sql = sql;
        this.outputExchange = outputExchange;
        this.hashColumns = hashColumns;
        this.leaf = leaf;
    }

    /**
     * Creates a leaf fragment that reads from data files.
     *
     * @param stageId        unique stage identifier within the plan
     * @param sql            DataFusion SQL to execute on each worker
     * @param outputExchange how this stage's output flows to the next stage
     * @param hashColumns    column indices for HASH partitioning (null for non-HASH)
     */
    public static PlanFragment leaf(int stageId, String sql, ExchangeType outputExchange, int[] hashColumns) {
        return new PlanFragment(stageId, sql, outputExchange, hashColumns, true);
    }

    /**
     * Creates an intermediate or final fragment that reads from exchange input.
     *
     * @param stageId        unique stage identifier within the plan
     * @param sql            DataFusion SQL over {@code __exchange_input__}
     * @param outputExchange how this stage's output flows to the next stage (NONE for final)
     * @param hashColumns    column indices for HASH partitioning (null for non-HASH)
     */
    public static PlanFragment intermediate(int stageId, String sql, ExchangeType outputExchange, int[] hashColumns) {
        return new PlanFragment(stageId, sql, outputExchange, hashColumns, false);
    }

    public int getStageId() {
        return stageId;
    }

    public String getSql() {
        return sql;
    }

    public ExchangeType getOutputExchange() {
        return outputExchange;
    }

    public int[] getHashColumns() {
        return hashColumns;
    }

    public boolean isLeaf() {
        return leaf;
    }

    @Override
    public String toString() {
        return "PlanFragment{stageId=" + stageId
            + ", leaf=" + leaf
            + ", exchange=" + outputExchange
            + ", sql=" + sql + "}";
    }
}

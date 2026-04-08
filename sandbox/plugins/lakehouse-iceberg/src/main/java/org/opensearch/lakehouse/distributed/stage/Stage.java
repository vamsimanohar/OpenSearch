/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed.stage;

import java.util.List;

/** Represents a single execution stage in a distributed query plan. */
public final class Stage {
    /** The type of stage in the execution pipeline. */
    public enum StageType {
        /** Leaf stage that scans data files. */
        SCAN,
        /** Middle stage for partial aggregation. */
        INTERMEDIATE,
        /** Root stage producing final results. */
        FINAL
    }

    private final StageId id;
    private final String sql;
    private final String tableName;
    private final InputSpec inputSpec;
    private final PartitioningScheme outputPartitioning;
    private final StageType type;
    private final List<StageId> sourceStages;

    /**
     * Creates a new Stage with the given parameters.
     *
     * @param id                 the stage identifier
     * @param sql                SQL fragment for this stage
     * @param tableName          table name this stage operates on
     * @param inputSpec          input specification for this stage
     * @param outputPartitioning output partitioning scheme
     * @param type               the stage type
     * @param sourceStages       IDs of upstream source stages
     */
    public Stage(StageId id, String sql, String tableName, InputSpec inputSpec,
                 PartitioningScheme outputPartitioning, StageType type, List<StageId> sourceStages) {
        this.id = id;
        this.sql = sql;
        this.tableName = tableName;
        this.inputSpec = inputSpec;
        this.outputPartitioning = outputPartitioning;
        this.type = type;
        this.sourceStages = List.copyOf(sourceStages);
    }

    /** Returns the stage identifier. */
    public StageId getId() { return id; }
    /** Returns the SQL fragment for this stage. */
    public String getSql() { return sql; }
    /** Returns the table name this stage operates on. */
    public String getTableName() { return tableName; }
    /** Returns the input specification for this stage. */
    public InputSpec getInputSpec() { return inputSpec; }
    /** Returns the output partitioning scheme. */
    public PartitioningScheme getOutputPartitioning() { return outputPartitioning; }
    /** Returns the stage type. */
    public StageType getType() { return type; }
    /** Returns the IDs of upstream source stages. */
    public List<StageId> getSourceStages() { return sourceStages; }
    /** Returns true if this stage has no upstream dependencies. */
    public boolean isLeaf() { return sourceStages.isEmpty(); }

    @Override
    public String toString() {
        return String.format("Stage{id=%s, type=%s, sql='%.60s...', input=%s, output=%s}",
            id, type, sql, inputSpec, outputPartitioning);
    }
}

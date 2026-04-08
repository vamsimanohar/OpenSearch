/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed.stage;

import java.util.List;

public final class Stage {
    public enum StageType { SCAN, INTERMEDIATE, FINAL }

    private final StageId id;
    private final String sql;
    private final String tableName;
    private final InputSpec inputSpec;
    private final PartitioningScheme outputPartitioning;
    private final StageType type;
    private final List<StageId> sourceStages;

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

    public StageId getId() { return id; }
    public String getSql() { return sql; }
    public String getTableName() { return tableName; }
    public InputSpec getInputSpec() { return inputSpec; }
    public PartitioningScheme getOutputPartitioning() { return outputPartitioning; }
    public StageType getType() { return type; }
    public List<StageId> getSourceStages() { return sourceStages; }
    public boolean isLeaf() { return sourceStages.isEmpty(); }

    @Override
    public String toString() {
        return String.format("Stage{id=%s, type=%s, sql='%.60s...', input=%s, output=%s}",
            id, type, sql, inputSpec, outputPartitioning);
    }
}

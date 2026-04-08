/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed.stage;

import org.opensearch.cluster.node.DiscoveryNode;

import java.util.List;
import java.util.Map;

/** A concrete task assigned to a specific node for executing a partition of a stage. */
public final class StageTask {
    private final StageId stageId;
    private final int partitionIndex;
    private final DiscoveryNode targetNode;
    private final String sql;
    private final String tableName;
    private final List<String> filePaths;
    private final Map<String, String> storageConfig;
    private final Map<String, byte[]> exchangeInputs;

    /**
     * Creates a new StageTask with the given parameters.
     *
     * @param stageId        the stage identifier
     * @param partitionIndex partition index for this task
     * @param targetNode     node to execute on
     * @param sql            SQL fragment for this task
     * @param tableName      table name to operate on
     * @param filePaths      data file paths to scan
     * @param storageConfig  storage access configuration
     * @param exchangeInputs exchange input data by table name
     */
    public StageTask(StageId stageId, int partitionIndex, DiscoveryNode targetNode,
                     String sql, String tableName,
                     List<String> filePaths, Map<String, String> storageConfig,
                     Map<String, byte[]> exchangeInputs) {
        this.stageId = stageId;
        this.partitionIndex = partitionIndex;
        this.targetNode = targetNode;
        this.sql = sql;
        this.tableName = tableName;
        this.filePaths = filePaths != null ? List.copyOf(filePaths) : List.of();
        this.storageConfig = storageConfig != null ? Map.copyOf(storageConfig) : Map.of();
        this.exchangeInputs = exchangeInputs != null ? Map.copyOf(exchangeInputs) : Map.of();
    }

    /** Returns the stage identifier for this task. */
    public StageId getStageId() { return stageId; }
    /** Returns the partition index assigned to this task. */
    public int getPartitionIndex() { return partitionIndex; }
    /** Returns the target node where this task will execute. */
    public DiscoveryNode getTargetNode() { return targetNode; }
    /** Returns the SQL fragment for this task. */
    public String getSql() { return sql; }
    /** Returns the table name this task operates on. */
    public String getTableName() { return tableName; }
    /** Returns the file paths to scan. */
    public List<String> getFilePaths() { return filePaths; }
    /** Returns the storage configuration for file access. */
    public Map<String, String> getStorageConfig() { return storageConfig; }
    /** Returns the exchange input data keyed by source table name. */
    public Map<String, byte[]> getExchangeInputs() { return exchangeInputs; }
    /** Returns true if this task has scan input files. */
    public boolean hasScanInput() { return !filePaths.isEmpty(); }
    /** Returns true if this task has exchange input data. */
    public boolean hasExchangeInput() { return !exchangeInputs.isEmpty(); }
}

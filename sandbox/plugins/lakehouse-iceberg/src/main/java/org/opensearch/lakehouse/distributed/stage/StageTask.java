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

public final class StageTask {
    private final StageId stageId;
    private final int partitionIndex;
    private final DiscoveryNode targetNode;
    private final String sql;
    private final String tableName;
    private final List<String> filePaths;
    private final Map<String, String> storageConfig;
    private final Map<String, byte[]> exchangeInputs;

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

    public StageId getStageId() { return stageId; }
    public int getPartitionIndex() { return partitionIndex; }
    public DiscoveryNode getTargetNode() { return targetNode; }
    public String getSql() { return sql; }
    public String getTableName() { return tableName; }
    public List<String> getFilePaths() { return filePaths; }
    public Map<String, String> getStorageConfig() { return storageConfig; }
    public Map<String, byte[]> getExchangeInputs() { return exchangeInputs; }
    public boolean hasScanInput() { return !filePaths.isEmpty(); }
    public boolean hasExchangeInput() { return !exchangeInputs.isEmpty(); }
}

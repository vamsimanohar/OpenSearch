/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed.exchange;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.lakehouse.distributed.stage.PartitioningScheme;
import org.opensearch.lakehouse.distributed.stage.StageId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages data flow between stages in the multi-stage executor.
 *
 * <p>When a stage completes, its output IPC bytes are registered here.
 * Downstream stages query the exchange service to get their input data,
 * already partitioned according to the stage's output partitioning scheme.
 */
public final class ExchangeService {

    private static final Logger logger = LogManager.getLogger(ExchangeService.class);

    /** Creates a new ExchangeService. */
    public ExchangeService() {}

    /** stageId -> list of IPC outputs (one per worker that completed this stage) */
    private final Map<StageId, List<byte[]>> stageOutputs = new ConcurrentHashMap<>();

    /** stageId -> output partitioning scheme */
    private final Map<StageId, PartitioningScheme> outputSchemes = new ConcurrentHashMap<>();

    /**
     * Registers the output partitioning scheme for a stage.
     *
     * @param stageId the stage identifier
     * @param scheme  the output partitioning scheme
     */
    public void registerOutputScheme(StageId stageId, PartitioningScheme scheme) {
        outputSchemes.put(stageId, scheme);
    }

    /**
     * Called when a worker completes a stage task and returns IPC bytes.
     *
     * @param stageId  the stage identifier
     * @param ipcBytes the IPC output bytes
     */
    public synchronized void addStageOutput(StageId stageId, byte[] ipcBytes) {
        stageOutputs.computeIfAbsent(stageId, k -> Collections.synchronizedList(new ArrayList<>()))
            .add(ipcBytes);
        logger.info("[ExchangeService] Received output for {}: {} bytes (total outputs: {})",
            stageId, ipcBytes.length, stageOutputs.get(stageId).size());
    }

    /**
     * Gets the input IPC batches for a target partition of a downstream stage.
     *
     * @param sourceStageId  the upstream stage whose output we want
     * @param targetPartition the target partition index (for hash partitioning)
     * @param targetCount    total number of target partitions
     * @return IPC byte arrays to merge as input for this partition
     */
    public byte[][] getInputForPartition(StageId sourceStageId, int targetPartition, int targetCount) {
        List<byte[]> outputs = stageOutputs.getOrDefault(sourceStageId, List.of());
        PartitioningScheme scheme = outputSchemes.getOrDefault(sourceStageId, PartitioningScheme.gather());

        List<List<byte[]>> partitioned = ArrowPartitioner.partition(outputs, scheme, targetCount);

        List<byte[]> myData = targetPartition < partitioned.size()
            ? partitioned.get(targetPartition) : List.of();

        logger.info("[ExchangeService] {} partition {} of {}: {} IPC batches",
            sourceStageId, targetPartition, targetCount, myData.size());
        return myData.toArray(new byte[0][]);
    }

    /**
     * Gets all output from a stage (for GATHER mode — all to coordinator).
     *
     * @param sourceStageId the upstream stage
     */
    public byte[][] getAllOutputs(StageId sourceStageId) {
        List<byte[]> outputs = stageOutputs.getOrDefault(sourceStageId, List.of());
        return outputs.toArray(new byte[0][]);
    }

    /** Clears all exchange data (call after query completes). */
    public void clear() {
        stageOutputs.clear();
        outputSchemes.clear();
    }

    /**
     * Returns the number of outputs received for the given stage.
     *
     * @param stageId the stage identifier
     */
    public int getOutputCount(StageId stageId) {
        return stageOutputs.getOrDefault(stageId, List.of()).size();
    }
}

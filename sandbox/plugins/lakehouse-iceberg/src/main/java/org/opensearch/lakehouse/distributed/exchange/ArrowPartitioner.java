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

import java.util.ArrayList;
import java.util.List;

/**
 * Partitions Arrow IPC byte arrays for inter-stage data routing.
 *
 * <p>For GATHER, all outputs go to a single partition (the coordinator).
 * For BROADCAST, replicates each worker's output to all target partitions.
 * For HASH, distributes worker outputs across target partitions using round-robin
 * (each worker's entire output goes to one target).
 */
public final class ArrowPartitioner {

    private static final Logger logger = LogManager.getLogger(ArrowPartitioner.class);

    private ArrowPartitioner() {}

    /**
     * Routes worker outputs to target partitions based on the partitioning scheme.
     *
     * @param workerOutputs IPC bytes from each worker (indexed by source worker)
     * @param scheme        the partitioning scheme
     * @param targetCount   number of target partitions
     * @return list of IPC byte arrays per target partition
     */
    public static List<List<byte[]>> partition(List<byte[]> workerOutputs,
                                                PartitioningScheme scheme,
                                                int targetCount) {
        return switch (scheme.getType()) {
            case GATHER -> gatherPartition(workerOutputs, targetCount);
            case BROADCAST -> broadcastPartition(workerOutputs, targetCount);
            case HASH -> hashPartition(workerOutputs, targetCount);
            case NONE -> gatherPartition(workerOutputs, targetCount);
        };
    }

    /** All worker outputs go to partition 0. */
    private static List<List<byte[]>> gatherPartition(List<byte[]> outputs, int targetCount) {
        List<List<byte[]>> result = new ArrayList<>();
        result.add(new ArrayList<>(outputs));
        for (int i = 1; i < targetCount; i++) {
            result.add(List.of());
        }
        logger.debug("[ArrowPartitioner] GATHER: {} worker outputs -> partition 0", outputs.size());
        return result;
    }

    /** Every target partition gets a copy of every worker output. */
    private static List<List<byte[]>> broadcastPartition(List<byte[]> outputs, int targetCount) {
        List<List<byte[]>> result = new ArrayList<>();
        for (int i = 0; i < targetCount; i++) {
            result.add(new ArrayList<>(outputs));
        }
        long totalBytes = outputs.stream().mapToLong(b -> b.length).sum();
        logger.debug("[ArrowPartitioner] BROADCAST: {} outputs x {} targets, {} bytes each",
            outputs.size(), targetCount, totalBytes);
        return result;
    }

    /**
     * Distributes worker outputs across target partitions using round-robin.
     * For the POC, we use worker-index based assignment.
     */
    private static List<List<byte[]>> hashPartition(List<byte[]> outputs, int targetCount) {
        List<List<byte[]>> result = new ArrayList<>();
        for (int i = 0; i < targetCount; i++) {
            result.add(new ArrayList<>());
        }
        for (int i = 0; i < outputs.size(); i++) {
            int target = i % targetCount;
            result.get(target).add(outputs.get(i));
        }
        logger.debug("[ArrowPartitioner] HASH: {} outputs -> {} targets (round-robin)",
            outputs.size(), targetCount);
        return result;
    }
}

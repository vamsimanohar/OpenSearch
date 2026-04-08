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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-node singleton that buffers stage outputs for pull-based exchange.
 *
 * <p>After a worker executes a stage, it stores the IPC output here.
 * Other workers (or the coordinator) can pull the output via
 * {@link ExchangePullAction}. This enables Trino-style worker-to-worker
 * data exchange instead of funneling all data through the coordinator.
 *
 * <p>Outputs are keyed by {@code queryId:stageId} and cleaned up
 * after each query completes.
 */
public final class WorkerOutputManager {

    private static final Logger logger = LogManager.getLogger(WorkerOutputManager.class);

    private static final WorkerOutputManager INSTANCE = new WorkerOutputManager();

    /** Key: "queryId:stageId" -> IPC bytes produced by this worker for that stage. */
    private final Map<String, byte[]> outputs = new ConcurrentHashMap<>();

    private WorkerOutputManager() {}

    /**
     * Returns the singleton instance.
     *
     * @return the singleton instance
     */
    public static WorkerOutputManager instance() {
        return INSTANCE;
    }

    /**
     * Registers the IPC output for a stage execution on this worker.
     *
     * @param queryId  the unique query execution ID
     * @param stageId  the stage ID (e.g., "Stage-0")
     * @param ipcBytes the Arrow IPC output bytes
     */
    public void registerOutput(String queryId, String stageId, byte[] ipcBytes) {
        String key = makeKey(queryId, stageId);
        outputs.put(key, ipcBytes);
        logger.info("[WorkerOutputManager] Registered output: key={}, {} bytes", key, ipcBytes.length);
    }

    /**
     * Retrieves the IPC output for a stage execution on this worker.
     *
     * @param queryId the unique query execution ID
     * @param stageId the stage ID
     * @return the IPC bytes, or null if not found
     */
    public byte[] getOutput(String queryId, String stageId) {
        String key = makeKey(queryId, stageId);
        byte[] result = outputs.get(key);
        logger.info("[WorkerOutputManager] Get output: key={}, found={}", key, result != null);
        return result;
    }

    /**
     * Removes all outputs for a given query, freeing memory.
     *
     * @param queryId the query execution ID to clean up
     */
    public void cleanup(String queryId) {
        String prefix = queryId + ":";
        int removed = 0;
        var iter = outputs.entrySet().iterator();
        while (iter.hasNext()) {
            if (iter.next().getKey().startsWith(prefix)) {
                iter.remove();
                removed++;
            }
        }
        logger.info("[WorkerOutputManager] Cleanup queryId={}: removed {} entries", queryId, removed);
    }

    /**
     * Returns the number of stored outputs (for diagnostics).
     *
     * @return the number of stored entries
     */
    public int size() {
        return outputs.size();
    }

    private static String makeKey(String queryId, String stageId) {
        return queryId + ":" + stageId;
    }
}

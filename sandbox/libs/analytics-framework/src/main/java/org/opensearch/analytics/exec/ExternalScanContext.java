/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Carries the resolved scan context from an external table plugin (e.g., Iceberg)
 * to the native execution backend (e.g., DataFusion).
 */
public class ExternalScanContext {

    /**
     * Global backend executor for distributed worker queries.
     * Registered by the plan executor when the backend becomes available.
     * Used by distributed worker transport actions to execute file scans.
     */
    private static volatile Function<ExternalScanContext, Iterable<Object[]>> globalBackendExecutor;

    /**
     * Global IPC executor for distributed worker queries.
     * Returns Arrow IPC bytes instead of Object[] rows, for efficient network transport.
     */
    private static volatile Function<ExternalScanContext, byte[]> globalIpcExecutor;

    /**
     * Registers the global backend executor for distributed worker queries.
     *
     * @param executor function that executes an {@link ExternalScanContext} and returns result rows
     */
    public static void setGlobalBackendExecutor(Function<ExternalScanContext, Iterable<Object[]>> executor) {
        globalBackendExecutor = executor;
    }

    /** Returns the global backend executor, or {@code null} if not yet registered. */
    public static Function<ExternalScanContext, Iterable<Object[]>> getGlobalBackendExecutor() {
        return globalBackendExecutor;
    }

    /**
     * Registers the global IPC executor for distributed worker queries.
     *
     * @param executor function that executes an {@link ExternalScanContext} and returns Arrow IPC bytes
     */
    public static void setGlobalIpcExecutor(Function<ExternalScanContext, byte[]> executor) {
        globalIpcExecutor = executor;
    }

    /** Returns the global IPC executor, or {@code null} if not yet registered. */
    public static Function<ExternalScanContext, byte[]> getGlobalIpcExecutor() {
        return globalIpcExecutor;
    }

    private final String tableName;
    private final List<String> dataFilePaths;
    private final String sqlQuery;
    private final Map<String, String> storageConfig;

    /**
     * Pre-computed results from distributed execution. When non-null, the plan executor
     * should return these directly instead of calling {@code executeRemoteQuery()}.
     */
    private volatile Iterable<Object[]> preComputedResults;

    /**
     * Arrow IPC byte arrays from distributed workers, for coordinator merge.
     * When non-null, the backend executor should merge these instead of scanning files.
     */
    private byte[][] ipcBatches;

    /**
     * Creates a new scan context.
     *
     * @param tableName      table name matching the Substrait plan
     * @param dataFilePaths  pruned data file paths to scan
     * @param sqlQuery       SQL query string for the target execution engine
     * @param storageConfig  storage configuration (S3 region, bucket, credentials)
     */
    public ExternalScanContext(
        String tableName,
        List<String> dataFilePaths,
        String sqlQuery,
        Map<String, String> storageConfig
    ) {
        this.tableName = tableName;
        this.dataFilePaths = dataFilePaths;
        this.sqlQuery = sqlQuery;
        this.storageConfig = storageConfig;
    }

    /** Table name as registered in the Calcite schema (must match Substrait plan references). */
    public String getTableName() { return tableName; }

    /** Pruned list of data file paths (e.g., s3://bucket/data/file.parquet). */
    public List<String> getDataFilePaths() { return dataFilePaths; }

    /** SQL query string for the target execution engine. */
    public String getSqlQuery() { return sqlQuery; }

    /**
     * Storage configuration for the external data source.
     * Keys: s3Region, s3Bucket, s3AccessKeyId (optional), s3SecretAccessKey (optional),
     *        s3SessionToken (optional), s3Endpoint (optional).
     */
    public Map<String, String> getStorageConfig() { return storageConfig; }

    /**
     * Returns pre-computed results from distributed execution, or {@code null}
     * if the query should be executed via the normal single-node backend path.
     */
    public Iterable<Object[]> getPreComputedResults() { return preComputedResults; }

    /**
     * Sets pre-computed results from distributed execution. When set, the plan executor
     * will return these directly instead of delegating to the backend's
     * {@code executeRemoteQuery()}.
     *
     * @param results the pre-computed result rows from distributed execution
     */
    public void setPreComputedResults(Iterable<Object[]> results) { this.preComputedResults = results; }

    /** Returns the Arrow IPC byte arrays for coordinator merge, or {@code null}. */
    public byte[][] getIpcBatches() { return ipcBatches; }

    /**
     * Sets Arrow IPC byte arrays from distributed workers for coordinator merge.
     * @param ipcBatches the IPC byte arrays, one per worker
     */
    public void setIpcBatches(byte[][] ipcBatches) { this.ipcBatches = ipcBatches; }
}

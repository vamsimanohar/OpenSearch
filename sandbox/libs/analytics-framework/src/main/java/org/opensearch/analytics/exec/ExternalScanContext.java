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

/**
 * Carries the resolved scan context from an external table plugin (e.g., Iceberg)
 * to the native execution backend (e.g., DataFusion).
 */
public class ExternalScanContext {
    private final String tableName;
    private final List<String> dataFilePaths;
    private final long[] fileSizes;
    private final String sqlQuery;
    private final Map<String, String> storageConfig;
    private Iterable<Object[]> preComputedResults;

    /**
     * Creates a new scan context.
     *
     * @param tableName      table name matching the query plan
     * @param dataFilePaths  pruned data file paths to scan
     * @param fileSizes      file sizes in bytes, parallel to dataFilePaths
     * @param sqlQuery       SQL query string for the target execution engine
     * @param storageConfig  storage configuration (S3 region, bucket, credentials)
     */
    public ExternalScanContext(String tableName, List<String> dataFilePaths, long[] fileSizes, String sqlQuery, Map<String, String> storageConfig) {
        this.tableName = tableName;
        this.dataFilePaths = dataFilePaths;
        this.fileSizes = fileSizes;
        this.sqlQuery = sqlQuery;
        this.storageConfig = storageConfig;
    }

    /** Table name as registered in the Calcite schema. */
    public String getTableName() {
        return tableName;
    }

    /** Pruned list of data file paths (e.g., s3://bucket/data/file.parquet). */
    public List<String> getDataFilePaths() {
        return dataFilePaths;
    }

    /** File sizes in bytes, parallel to dataFilePaths. */
    public long[] getFileSizes() {
        return fileSizes;
    }

    /** SQL query string for the target execution engine. */
    public String getSqlQuery() {
        return sqlQuery;
    }

    /**
     * Storage configuration for the external data source.
     * Keys: s3Region, s3Bucket, s3AccessKeyId (optional), s3SecretAccessKey (optional),
     *        s3SessionToken (optional), s3Endpoint (optional).
     */
    public Map<String, String> getStorageConfig() {
        return storageConfig;
    }

    /**
     * Returns pre-computed results from distributed execution, or null.
     * When non-null, the execution backend should be skipped — results are already merged.
     */
    public Iterable<Object[]> getPreComputedResults() {
        return preComputedResults;
    }

    /**
     * Sets pre-computed results from distributed execution.
     * Used when a distributed scan executor has already merged results from multiple workers.
     */
    public void setPreComputedResults(Iterable<Object[]> results) {
        this.preComputedResults = results;
    }
}

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
    private final byte[] substraitPlan;
    private final Map<String, String> storageConfig;

    /**
     * Creates a new scan context.
     *
     * @param tableName      table name matching the Substrait plan
     * @param dataFilePaths  pruned data file paths to scan
     * @param substraitPlan  serialized Substrait plan bytes
     * @param storageConfig  storage configuration (S3 region, bucket, credentials)
     */
    public ExternalScanContext(
        String tableName,
        List<String> dataFilePaths,
        byte[] substraitPlan,
        Map<String, String> storageConfig
    ) {
        this.tableName = tableName;
        this.dataFilePaths = dataFilePaths;
        this.substraitPlan = substraitPlan;
        this.storageConfig = storageConfig;
    }

    /** Table name as registered in the Calcite schema (must match Substrait plan references). */
    public String getTableName() { return tableName; }

    /** Pruned list of data file paths (e.g., s3://bucket/data/file.parquet). */
    public List<String> getDataFilePaths() { return dataFilePaths; }

    /** Serialized Substrait plan bytes (Calcite RelNode converted to protobuf). */
    public byte[] getSubstraitPlan() { return substraitPlan; }

    /**
     * Storage configuration for the external data source.
     * Keys: s3Region, s3Bucket, s3AccessKeyId (optional), s3SecretAccessKey (optional),
     *        s3SessionToken (optional), s3Endpoint (optional).
     */
    public Map<String, String> getStorageConfig() { return storageConfig; }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.exec;

import java.util.List;

/**
 * Carries all parameters needed for a single Iceberg query execution.
 */
public class IcebergExecutionContext {
    private final String tableName;
    private final List<String> dataFilePaths;
    private final byte[] substraitPlan;
    private final List<String> projectedColumns;
    private final String s3Region;
    private final String s3Bucket;
    private final String accessKeyId;     // nullable
    private final String secretAccessKey; // nullable
    private final String sessionToken;    // nullable

    public IcebergExecutionContext(
        String tableName,
        List<String> dataFilePaths,
        byte[] substraitPlan,
        List<String> projectedColumns,
        String s3Region,
        String s3Bucket,
        String accessKeyId,
        String secretAccessKey,
        String sessionToken
    ) {
        this.tableName = tableName;
        this.dataFilePaths = dataFilePaths;
        this.substraitPlan = substraitPlan;
        this.projectedColumns = projectedColumns;
        this.s3Region = s3Region;
        this.s3Bucket = s3Bucket;
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
        this.sessionToken = sessionToken;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getDataFilePaths() {
        return dataFilePaths;
    }

    public byte[] getSubstraitPlan() {
        return substraitPlan;
    }

    public List<String> getProjectedColumns() {
        return projectedColumns;
    }

    public String getS3Region() {
        return s3Region;
    }

    public String getS3Bucket() {
        return s3Bucket;
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    public String getSessionToken() {
        return sessionToken;
    }
}

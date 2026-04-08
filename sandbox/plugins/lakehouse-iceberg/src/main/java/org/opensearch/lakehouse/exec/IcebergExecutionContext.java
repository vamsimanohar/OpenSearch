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
    private final String sqlQuery;
    private final List<String> projectedColumns;
    private final String s3Region;
    private final String s3Bucket;
    private final String accessKeyId;     // nullable
    private final String secretAccessKey; // nullable
    private final String sessionToken;    // nullable

    /**
     * Creates an execution context with all parameters needed for Iceberg query execution.
     *
     * @param tableName        the Iceberg table name
     * @param dataFilePaths    pruned list of data file paths to scan
     * @param sqlQuery    SQL query string for DataFusion
     * @param projectedColumns columns to project
     * @param s3Region         AWS region for S3 access
     * @param s3Bucket         S3 bucket name
     * @param accessKeyId      AWS access key ID, or null for default credentials
     * @param secretAccessKey  AWS secret access key, or null for default credentials
     * @param sessionToken     AWS session token, or null if not using temporary credentials
     */
    public IcebergExecutionContext(
        String tableName,
        List<String> dataFilePaths,
        String sqlQuery,
        List<String> projectedColumns,
        String s3Region,
        String s3Bucket,
        String accessKeyId,
        String secretAccessKey,
        String sessionToken
    ) {
        this.tableName = tableName;
        this.dataFilePaths = dataFilePaths;
        this.sqlQuery = sqlQuery;
        this.projectedColumns = projectedColumns;
        this.s3Region = s3Region;
        this.s3Bucket = s3Bucket;
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
        this.sessionToken = sessionToken;
    }

    /** Returns the Iceberg table name. */
    public String getTableName() {
        return tableName;
    }

    /** Returns the pruned data file paths to scan. */
    public List<String> getDataFilePaths() {
        return dataFilePaths;
    }

    /** Returns the SQL query string for DataFusion. */
    public String getSqlQuery() {
        return sqlQuery;
    }

    /** Returns the projected column names. */
    public List<String> getProjectedColumns() {
        return projectedColumns;
    }

    /** Returns the AWS region for S3 access. */
    public String getS3Region() {
        return s3Region;
    }

    /** Returns the S3 bucket name. */
    public String getS3Bucket() {
        return s3Bucket;
    }

    /** Returns the AWS access key ID, or null for default credentials. */
    public String getAccessKeyId() {
        return accessKeyId;
    }

    /** Returns the AWS secret access key, or null for default credentials. */
    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    /** Returns the AWS session token, or null if not using temporary credentials. */
    public String getSessionToken() {
        return sessionToken;
    }
}

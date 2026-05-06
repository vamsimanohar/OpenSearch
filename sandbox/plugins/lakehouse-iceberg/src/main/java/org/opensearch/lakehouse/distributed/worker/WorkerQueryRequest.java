/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed.worker;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Transport request sent from coordinator to worker nodes for distributed Iceberg query execution.
 * <p>
 * Contains the SQL query, list of Parquet file paths to process, corresponding file sizes,
 * storage configuration (S3 credentials, region, bucket), and the table name.
 *
 * @opensearch.internal
 */
public class WorkerQueryRequest extends ActionRequest {

    private final String sqlQuery;
    private final List<String> filePaths;
    private final long[] fileSizes;
    private final Map<String, String> storageConfig;
    private final String tableName;

    /**
     * Creates a new worker query request.
     *
     * @param sqlQuery      the SQL query to execute on the worker
     * @param filePaths     Parquet file paths this worker should process
     * @param fileSizes     corresponding file sizes in bytes
     * @param storageConfig storage configuration (s3Region, s3Bucket, credentials)
     * @param tableName     the table name as registered in the query plan
     */
    public WorkerQueryRequest(String sqlQuery, List<String> filePaths, long[] fileSizes, Map<String, String> storageConfig, String tableName) {
        this.sqlQuery = sqlQuery;
        this.filePaths = filePaths;
        this.fileSizes = fileSizes;
        this.storageConfig = storageConfig;
        this.tableName = tableName;
    }

    /**
     * Creates a request from a stream.
     *
     * @param in the stream input
     * @throws IOException if reading fails
     */
    public WorkerQueryRequest(StreamInput in) throws IOException {
        super(in);
        this.sqlQuery = in.readString();
        this.filePaths = in.readStringList();
        this.fileSizes = in.readLongArray();
        this.storageConfig = in.readMap(StreamInput::readString, StreamInput::readString);
        this.tableName = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(sqlQuery);
        out.writeStringCollection(filePaths);
        out.writeLongArray(fileSizes);
        out.writeMap(storageConfig, StreamOutput::writeString, StreamOutput::writeString);
        out.writeString(tableName);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (sqlQuery == null || sqlQuery.isEmpty()) {
            validationException = addValidationError("sqlQuery is missing or empty", validationException);
        }
        if (filePaths == null || filePaths.isEmpty()) {
            validationException = addValidationError("filePaths is missing or empty", validationException);
        }
        if (tableName == null || tableName.isEmpty()) {
            validationException = addValidationError("tableName is missing or empty", validationException);
        }
        return validationException;
    }

    /** Returns the SQL query to execute. */
    public String getSqlQuery() {
        return sqlQuery;
    }

    /** Returns the Parquet file paths to process. */
    public List<String> getFilePaths() {
        return filePaths;
    }

    /** Returns the file sizes in bytes, parallel to filePaths. */
    public long[] getFileSizes() {
        return fileSizes;
    }

    /** Returns the storage configuration map. */
    public Map<String, String> getStorageConfig() {
        return storageConfig;
    }

    /** Returns the table name. */
    public String getTableName() {
        return tableName;
    }
}

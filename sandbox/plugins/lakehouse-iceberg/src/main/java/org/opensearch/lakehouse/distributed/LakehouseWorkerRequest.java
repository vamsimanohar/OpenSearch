/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Transport request sent from the coordinator node to a worker node for
 * distributed Iceberg query execution. Contains the file paths assigned
 * to this worker, the SQL query to execute, S3 storage configuration,
 * the table name, and optional multi-stage metadata (queryId, stageId).
 *
 * <p>For INTERMEDIATE stages, filePaths may be empty and exchangeInputs
 * tells the worker which upstream nodes to pull data from.
 */
public class LakehouseWorkerRequest extends ActionRequest {

    private final String[] filePaths;
    private final String sqlQuery;
    private final Map<String, String> storageConfig;
    private final String tableName;
    private final String queryId;
    private final String stageId;
    private final List<ExchangeInput> exchangeInputs;

    /**
     * Creates a new worker request (backward-compatible, no multi-stage metadata).
     *
     * @param filePaths     S3 or file paths for this worker's file subset
     * @param sqlQuery      SQL query string for the worker to execute
     * @param storageConfig storage configuration (e.g., s3Region, s3Bucket, credentials)
     * @param tableName     table name for DataFusion table registration
     */
    public LakehouseWorkerRequest(String[] filePaths, String sqlQuery,
                                   Map<String, String> storageConfig, String tableName) {
        this(filePaths, sqlQuery, storageConfig, tableName, "", "", List.of());
    }

    /**
     * Creates a new worker request with multi-stage metadata.
     *
     * @param filePaths     S3 or file paths for this worker's file subset
     * @param sqlQuery      SQL query string for the worker to execute
     * @param storageConfig storage configuration (e.g., s3Region, s3Bucket, credentials)
     * @param tableName     table name for DataFusion table registration
     * @param queryId       unique query execution ID for pull-based exchange
     * @param stageId       stage ID within the query DAG
     */
    public LakehouseWorkerRequest(String[] filePaths, String sqlQuery,
                                   Map<String, String> storageConfig, String tableName,
                                   String queryId, String stageId) {
        this(filePaths, sqlQuery, storageConfig, tableName, queryId, stageId, List.of());
    }

    /**
     * Creates a new worker request with multi-stage metadata and exchange inputs.
     *
     * @param filePaths       S3 or file paths (may be empty for INTERMEDIATE stages)
     * @param sqlQuery        SQL query string for the worker to execute
     * @param storageConfig   storage configuration
     * @param tableName       table name for DataFusion table registration
     * @param queryId         unique query execution ID for pull-based exchange
     * @param stageId         stage ID within the query DAG
     * @param exchangeInputs  upstream stages/nodes to pull data from
     */
    public LakehouseWorkerRequest(String[] filePaths, String sqlQuery,
                                   Map<String, String> storageConfig, String tableName,
                                   String queryId, String stageId,
                                   List<ExchangeInput> exchangeInputs) {
        this.filePaths = filePaths;
        this.sqlQuery = sqlQuery;
        this.storageConfig = storageConfig;
        this.tableName = tableName;
        this.queryId = queryId != null ? queryId : "";
        this.stageId = stageId != null ? stageId : "";
        this.exchangeInputs = exchangeInputs != null ? exchangeInputs : List.of();
    }

    /**
     * Deserialization constructor.
     *
     * @param in the stream input to deserialize from
     * @throws IOException if deserialization fails
     */
    public LakehouseWorkerRequest(StreamInput in) throws IOException {
        super(in);
        this.filePaths = in.readStringArray();
        this.sqlQuery = in.readString();
        this.storageConfig = in.readMap(StreamInput::readString, StreamInput::readString);
        this.tableName = in.readString();
        this.queryId = in.readString();
        this.stageId = in.readString();
        this.exchangeInputs = in.readList(ExchangeInput::new);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        // For INTERMEDIATE stages, filePaths can be empty
        if ((filePaths == null || filePaths.length == 0) && exchangeInputs.isEmpty()) {
            validationException = new ActionRequestValidationException();
            validationException.addValidationError("filePaths or exchangeInputs must be provided");
        }
        if (sqlQuery == null || sqlQuery.isEmpty()) {
            if (validationException == null) {
                validationException = new ActionRequestValidationException();
            }
            validationException.addValidationError("sqlQuery must not be null or empty");
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(filePaths);
        out.writeString(sqlQuery);
        out.writeMap(storageConfig, StreamOutput::writeString, StreamOutput::writeString);
        out.writeString(tableName);
        out.writeString(queryId);
        out.writeString(stageId);
        out.writeList(exchangeInputs);
    }

    /** Returns the S3 or file paths assigned to this worker. */
    public String[] getFilePaths() { return filePaths; }

    /** Returns the SQL query string for the worker to execute. */
    public String getSqlQuery() { return sqlQuery; }

    /** Returns the storage configuration map. */
    public Map<String, String> getStorageConfig() { return storageConfig; }

    /** Returns the table name for DataFusion table registration. */
    public String getTableName() { return tableName; }

    /** Returns the query execution ID (empty string if not a multi-stage query). */
    public String getQueryId() { return queryId; }

    /** Returns the stage ID (empty string if not a multi-stage query). */
    public String getStageId() { return stageId; }

    /** Returns true if this request has multi-stage metadata (queryId is set). */
    public boolean hasQueryId() { return queryId != null && !queryId.isEmpty(); }

    /** Returns the exchange inputs for INTERMEDIATE stages (empty for SCAN stages). */
    public List<ExchangeInput> getExchangeInputs() { return exchangeInputs; }

    /** Returns true if this request has exchange inputs (INTERMEDIATE stage). */
    public boolean hasExchangeInputs() { return !exchangeInputs.isEmpty(); }

    /**
     * Describes an upstream stage + node list that this worker should pull data from.
     * The worker pulls IPC from each node in the list for the given source stage,
     * then registers the combined data as a MemTable with the given table name.
     */
    public static class ExchangeInput implements Writeable {
        private final String sourceStageId;
        private final String memTableName;
        private final List<String> sourceNodeIds;

        /**
         * Creates a new ExchangeInput.
         *
         * @param sourceStageId the upstream stage ID to pull from
         * @param memTableName  the table name to register pulled data as
         * @param sourceNodeIds node IDs that ran the source stage
         */
        public ExchangeInput(String sourceStageId, String memTableName, List<String> sourceNodeIds) {
            this.sourceStageId = sourceStageId;
            this.memTableName = memTableName;
            this.sourceNodeIds = sourceNodeIds;
        }

        /**
         * Deserialization constructor.
         *
         * @param in the stream input
         * @throws IOException if deserialization fails
         */
        public ExchangeInput(StreamInput in) throws IOException {
            this.sourceStageId = in.readString();
            this.memTableName = in.readString();
            this.sourceNodeIds = in.readStringList();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(sourceStageId);
            out.writeString(memTableName);
            out.writeStringCollection(sourceNodeIds);
        }

        /** Returns the upstream stage ID. */
        public String getSourceStageId() { return sourceStageId; }
        /** Returns the MemTable name for pulled data. */
        public String getMemTableName() { return memTableName; }
        /** Returns the node IDs to pull from. */
        public List<String> getSourceNodeIds() { return sourceNodeIds; }
    }
}

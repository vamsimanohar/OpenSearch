/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Map;

/**
 * Transport request sent from the coordinator node to a worker node for
 * distributed Iceberg query execution. Contains the file paths assigned
 * to this worker, the serialized Substrait query plan, S3 storage
 * configuration, and the table name referenced in the plan.
 */
public class LakehouseWorkerRequest extends TransportRequest {

    private final String[] filePaths;
    private final byte[] substraitPlan;
    private final Map<String, String> storageConfig;
    private final String tableName;

    /**
     * Creates a new worker request.
     *
     * @param filePaths     S3 or file paths for this worker's file subset
     * @param substraitPlan serialized Substrait protobuf plan
     * @param storageConfig storage configuration (e.g., s3Region, s3Bucket, credentials)
     * @param tableName     table name matching Substrait plan references
     */
    public LakehouseWorkerRequest(String[] filePaths, byte[] substraitPlan, Map<String, String> storageConfig, String tableName) {
        this.filePaths = filePaths;
        this.substraitPlan = substraitPlan;
        this.storageConfig = storageConfig;
        this.tableName = tableName;
    }

    /**
     * Deserialization constructor.
     */
    public LakehouseWorkerRequest(StreamInput in) throws IOException {
        super(in);
        this.filePaths = in.readStringArray();
        this.substraitPlan = in.readByteArray();
        this.storageConfig = in.readMap(StreamInput::readString, StreamInput::readString);
        this.tableName = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(filePaths);
        out.writeByteArray(substraitPlan);
        out.writeMap(storageConfig, StreamOutput::writeString, StreamOutput::writeString);
        out.writeString(tableName);
    }

    public String[] getFilePaths() {
        return filePaths;
    }

    public byte[] getSubstraitPlan() {
        return substraitPlan;
    }

    public Map<String, String> getStorageConfig() {
        return storageConfig;
    }

    public String getTableName() {
        return tableName;
    }
}

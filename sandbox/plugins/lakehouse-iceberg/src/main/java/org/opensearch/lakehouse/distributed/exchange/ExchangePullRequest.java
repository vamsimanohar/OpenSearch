/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed.exchange;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;

/**
 * Transport request to pull stage output from a worker node.
 *
 * <p>Sent by downstream workers (or the coordinator) to an upstream worker
 * to retrieve the IPC bytes produced by a completed stage.
 */
public class ExchangePullRequest extends TransportRequest {

    private final String queryId;
    private final String stageId;

    /**
     * Creates a new ExchangePullRequest.
     *
     * @param queryId the unique query execution ID
     * @param stageId the stage ID to pull output for
     */
    public ExchangePullRequest(String queryId, String stageId) {
        this.queryId = queryId;
        this.stageId = stageId;
    }

    /**
     * Deserialization constructor.
     *
     * @param in the stream input
     * @throws IOException if deserialization fails
     */
    public ExchangePullRequest(StreamInput in) throws IOException {
        super(in);
        this.queryId = in.readString();
        this.stageId = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(queryId);
        out.writeString(stageId);
    }

    /** Returns the query execution ID. */
    public String getQueryId() {
        return queryId;
    }

    /** Returns the stage ID. */
    public String getStageId() {
        return stageId;
    }
}

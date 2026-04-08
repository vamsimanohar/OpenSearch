/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed.exchange;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Transport response containing the IPC bytes for a stage output.
 *
 * <p>Returned by a worker node in response to an {@link ExchangePullRequest}.
 * Contains the Arrow IPC serialized output of the requested stage.
 */
public class ExchangePullResponse extends ActionResponse {

    private final byte[] ipcBytes;

    /**
     * Creates a new ExchangePullResponse.
     *
     * @param ipcBytes the Arrow IPC output bytes (may be empty if stage output not found)
     */
    public ExchangePullResponse(byte[] ipcBytes) {
        this.ipcBytes = ipcBytes;
    }

    /**
     * Deserialization constructor.
     *
     * @param in the stream input
     * @throws IOException if deserialization fails
     */
    public ExchangePullResponse(StreamInput in) throws IOException {
        super(in);
        this.ipcBytes = in.readByteArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByteArray(ipcBytes);
    }

    /** Returns the IPC bytes. */
    public byte[] getIpcBytes() {
        return ipcBytes;
    }

    /** Returns whether this response contains data. */
    public boolean hasData() {
        return ipcBytes != null && ipcBytes.length > 0;
    }
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Transport response returned from a worker node after executing its
 * portion of a distributed Iceberg query.
 *
 * <p>Supports two serialization formats:
 * <ul>
 *   <li><b>Arrow IPC</b> (preferred): compact columnar binary format</li>
 *   <li><b>Object[][]</b> (fallback): row-by-row generic value serialization</li>
 * </ul>
 *
 * <p>The format byte at the start of the wire format distinguishes between them:
 * {@code 0} = legacy Object[][] format, {@code 1} = Arrow IPC format.
 */
public class LakehouseWorkerResponse extends ActionResponse {

    private static final byte FORMAT_ROWS = 0;
    private static final byte FORMAT_IPC = 1;

    private final Object[][] rows;
    private final String[] columnNames;
    private final byte[] ipcBytes;

    /**
     * Creates a response with Object[][] rows (legacy format).
     * @param rows        result rows
     * @param columnNames column names
     */
    public LakehouseWorkerResponse(Object[][] rows, String[] columnNames) {
        this.rows = rows;
        this.columnNames = columnNames;
        this.ipcBytes = null;
    }

    /**
     * Creates a response with Arrow IPC bytes (preferred format).
     * @param ipcBytes Arrow IPC stream bytes
     */
    public LakehouseWorkerResponse(byte[] ipcBytes) {
        this.rows = new Object[0][];
        this.columnNames = new String[0];
        this.ipcBytes = ipcBytes;
    }

    /**
     * Deserialization constructor.
     * @param in the stream input to deserialize from
     */
    public LakehouseWorkerResponse(StreamInput in) throws IOException {
        super(in);
        byte format = in.readByte();
        if (format == FORMAT_IPC) {
            this.ipcBytes = in.readByteArray();
            this.rows = new Object[0][];
            this.columnNames = new String[0];
        } else {
            this.ipcBytes = null;
            this.columnNames = in.readStringArray();
            int numRows = in.readVInt();
            int numCols = columnNames.length;
            this.rows = new Object[numRows][];
            for (int i = 0; i < numRows; i++) {
                Object[] row = new Object[numCols];
                for (int j = 0; j < numCols; j++) {
                    row[j] = in.readGenericValue();
                }
                rows[i] = row;
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (ipcBytes != null) {
            out.writeByte(FORMAT_IPC);
            out.writeByteArray(ipcBytes);
        } else {
            out.writeByte(FORMAT_ROWS);
            out.writeStringArray(columnNames);
            out.writeVInt(rows.length);
            int numCols = columnNames.length;
            for (Object[] row : rows) {
                for (int j = 0; j < numCols; j++) {
                    out.writeGenericValue(row[j]);
                }
            }
        }
    }

    /** Returns true if this response carries Arrow IPC bytes. */
    public boolean hasIpcBytes() {
        return ipcBytes != null;
    }

    /** Returns the Arrow IPC bytes, or null if using legacy row format. */
    public byte[] getIpcBytes() {
        return ipcBytes;
    }

    /** Returns the result rows (empty if using IPC format). */
    public Object[][] getRows() {
        return rows;
    }

    /** Returns the column names (empty if using IPC format). */
    public String[] getColumnNames() {
        return columnNames;
    }
}

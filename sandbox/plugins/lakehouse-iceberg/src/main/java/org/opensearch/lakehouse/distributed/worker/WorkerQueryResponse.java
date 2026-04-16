/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed.worker;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Transport response from worker nodes back to the coordinator.
 * <p>
 * Contains serialized result data as column-oriented arrays with schema metadata.
 * This avoids an Arrow IPC dependency; results are serialized as column names,
 * column types, row count, and a flat Object array of values (column-major order).
 *
 * @opensearch.internal
 */
public class WorkerQueryResponse extends ActionResponse implements ToXContentObject {

    private final List<String> columnNames;
    private final List<String> columnTypes;
    private final int rowCount;
    private final Object[][] columnData;
    private final byte[] arrowIpcData;

    /**
     * Creates a new worker query response.
     *
     * @param columnNames names of the result columns
     * @param columnTypes type names of the result columns (e.g., "VARCHAR", "INTEGER")
     * @param rowCount    number of result rows
     * @param columnData  column-major data: columnData[colIndex][rowIndex]
     */
    public WorkerQueryResponse(List<String> columnNames, List<String> columnTypes, int rowCount, Object[][] columnData) {
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.rowCount = rowCount;
        this.columnData = columnData;
        this.arrowIpcData = null;
    }

    /**
     * Creates a new Arrow IPC worker query response.
     * <p>
     * In this mode the response carries raw Arrow IPC bytes. Column metadata and
     * row count are unknown until the coordinator deserializes the Arrow data.
     *
     * @param arrowIpcData Arrow IPC serialized record batches
     */
    public WorkerQueryResponse(byte[] arrowIpcData) {
        this.arrowIpcData = arrowIpcData;
        this.columnNames = List.of();
        this.columnTypes = List.of();
        this.rowCount = -1;
        this.columnData = null;
    }

    /**
     * Creates a response from a stream.
     *
     * @param in the stream input
     * @throws IOException if reading fails
     */
    public WorkerQueryResponse(StreamInput in) throws IOException {
        byte format = in.readByte();
        if (format == 1) {
            this.arrowIpcData = in.readByteArray();
            this.columnNames = List.of();
            this.columnTypes = List.of();
            this.rowCount = -1;
            this.columnData = null;
        } else {
            this.arrowIpcData = null;
            this.columnNames = in.readStringList();
            this.columnTypes = in.readStringList();
            this.rowCount = in.readVInt();
            int numCols = in.readVInt();
            this.columnData = new Object[numCols][];
            for (int col = 0; col < numCols; col++) {
                this.columnData[col] = new Object[rowCount];
                for (int row = 0; row < rowCount; row++) {
                    this.columnData[col][row] = in.readGenericValue();
                }
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte((byte) (arrowIpcData != null ? 1 : 0));
        if (arrowIpcData != null) {
            out.writeByteArray(arrowIpcData);
        } else {
            out.writeStringCollection(columnNames);
            out.writeStringCollection(columnTypes);
            out.writeVInt(rowCount);
            out.writeVInt(columnData.length);
            for (Object[] column : columnData) {
                for (int row = 0; row < rowCount; row++) {
                    out.writeGenericValue(column[row]);
                }
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (arrowIpcData != null) {
            builder.field("format", "arrow_ipc");
            builder.field("arrowIpcBytes", arrowIpcData.length);
        } else {
            builder.field("rowCount", rowCount);
            builder.startArray("columns");
            for (int i = 0; i < columnNames.size(); i++) {
                builder.startObject();
                builder.field("name", columnNames.get(i));
                builder.field("type", columnTypes.get(i));
                builder.endObject();
            }
            builder.endArray();
        }
        builder.endObject();
        return builder;
    }

    /** Returns the column names. */
    public List<String> getColumnNames() {
        return columnNames;
    }

    /** Returns the column type names. */
    public List<String> getColumnTypes() {
        return columnTypes;
    }

    /** Returns the number of result rows. */
    public int getRowCount() {
        return rowCount;
    }

    /** Returns the column-major result data. */
    public Object[][] getColumnData() {
        return columnData;
    }

    /** Returns the raw Arrow IPC bytes, or {@code null} if this is a legacy response. */
    public byte[] getArrowIpcData() {
        return arrowIpcData;
    }

    /** Returns {@code true} if this response carries Arrow IPC data. */
    public boolean isArrowIpc() {
        return arrowIpcData != null;
    }
}

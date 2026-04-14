/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

    private static final Logger logger = LogManager.getLogger(WorkerQueryResponse.class);

    private final List<String> columnNames;
    private final List<String> columnTypes;
    private final int rowCount;
    private final Object[][] columnData;

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
    }

    /**
     * Creates a response from a stream.
     *
     * @param in the stream input
     * @throws IOException if reading fails
     */
    public WorkerQueryResponse(StreamInput in) throws IOException {
        long start = System.currentTimeMillis();
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
        logger.info("[WorkerQueryResponse] Deserialized: {} cols, {} rows in {}ms", numCols, rowCount, System.currentTimeMillis() - start);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        long start = System.currentTimeMillis();
        out.writeStringCollection(columnNames);
        out.writeStringCollection(columnTypes);
        out.writeVInt(rowCount);
        out.writeVInt(columnData.length);
        for (Object[] column : columnData) {
            for (int row = 0; row < rowCount; row++) {
                out.writeGenericValue(column[row]);
            }
        }
        logger.info("[WorkerQueryResponse] Serialized: {} cols, {} rows in {}ms", columnData.length, rowCount, System.currentTimeMillis() - start);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("rowCount", rowCount);
        builder.startArray("columns");
        for (int i = 0; i < columnNames.size(); i++) {
            builder.startObject();
            builder.field("name", columnNames.get(i));
            builder.field("type", columnTypes.get(i));
            builder.endObject();
        }
        builder.endArray();
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
}

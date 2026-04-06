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
import org.opensearch.core.transport.TransportResponse;

import java.io.IOException;

/**
 * Transport response returned from a worker node after executing its
 * portion of a distributed Iceberg query. Contains the result rows
 * and column names.
 */
public class LakehouseWorkerResponse extends TransportResponse {

    private final Object[][] rows;
    private final String[] columnNames;

    /**
     * Creates a new worker response.
     *
     * @param rows        result rows, where each row is an array of cell values
     * @param columnNames column names for the result set
     */
    public LakehouseWorkerResponse(Object[][] rows, String[] columnNames) {
        this.rows = rows;
        this.columnNames = columnNames;
    }

    /**
     * Deserialization constructor.
     */
    public LakehouseWorkerResponse(StreamInput in) throws IOException {
        super(in);
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(columnNames);
        out.writeVInt(rows.length);
        int numCols = columnNames.length;
        for (Object[] row : rows) {
            for (int j = 0; j < numCols; j++) {
                out.writeGenericValue(row[j]);
            }
        }
    }

    public Object[][] getRows() {
        return rows;
    }

    public String[] getColumnNames() {
        return columnNames;
    }
}

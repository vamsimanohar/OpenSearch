/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ppl.action;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Transport-layer response carrying column names and result rows
 * from the unified PPL query execution pipeline.
 *
 * @opensearch.internal
 */
public class PPLResponse extends ActionResponse {

    private final List<String> columns;
    private final List<Object[]> rows;

    /** Creates a response with the given columns and rows.
     * @param columns the column names
     * @param rows the result rows
     */
    public PPLResponse(List<String> columns, List<Object[]> rows) {
        this.columns = columns;
        this.rows = rows;
    }

    /** Creates a response from a stream.
     * @param in the stream input
     */
    public PPLResponse(StreamInput in) throws IOException {
        super(in);
        this.columns = in.readStringList();
        int rowCount = in.readVInt();
        this.rows = new ArrayList<>(rowCount);
        for (int i = 0; i < rowCount; i++) {
            int colCount = in.readVInt();
            Object[] row = new Object[colCount];
            for (int j = 0; j < colCount; j++) {
                row[j] = in.readGenericValue();
            }
            rows.add(row);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(columns);
        out.writeVInt(rows.size());
        for (Object[] row : rows) {
            out.writeVInt(row.length);
            for (Object val : row) {
                out.writeGenericValue(val);
            }
        }
    }

    /** Returns the column names.
     * @return the column names
     */
    public List<String> getColumns() {
        return columns;
    }

    /** Returns the result rows.
     * @return the result rows
     */
    public List<Object[]> getRows() {
        return rows;
    }
}

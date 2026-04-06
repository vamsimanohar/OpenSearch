/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.integration;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Transport response carrying column names, types, and result rows from SQL execution.
 */
public class SqlResponse extends ActionResponse {

    private final List<String> columns;
    private final List<String> columnTypes;
    private final List<Object[]> rows;

    public SqlResponse(List<String> columns, List<String> columnTypes, List<Object[]> rows) {
        this.columns = columns;
        this.columnTypes = columnTypes;
        this.rows = rows;
    }

    public SqlResponse(StreamInput in) throws IOException {
        super(in);
        this.columns = in.readStringList();
        this.columnTypes = in.readStringList();
        int rowCount = in.readVInt();
        this.rows = new ArrayList<>(rowCount);
        int colCount = columns.size();
        for (int i = 0; i < rowCount; i++) {
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
        out.writeStringCollection(columnTypes);
        out.writeVInt(rows.size());
        for (Object[] row : rows) {
            for (Object val : row) {
                out.writeGenericValue(val);
            }
        }
    }

    public List<String> getColumns() {
        return columns;
    }

    public List<String> getColumnTypes() {
        return columnTypes;
    }

    public List<Object[]> getRows() {
        return rows;
    }

    public int getTotal() {
        return rows.size();
    }
}

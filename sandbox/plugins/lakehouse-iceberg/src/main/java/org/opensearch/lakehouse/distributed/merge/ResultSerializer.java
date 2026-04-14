/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed.merge;

import org.opensearch.lakehouse.distributed.worker.WorkerQueryResponse;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility for converting between row-oriented and column-oriented result representations.
 * <p>
 * The coordinator receives column-oriented {@link WorkerQueryResponse} from workers
 * and must merge them into row-oriented {@code Object[]} arrays for the query pipeline.
 * This class provides static methods for both directions of conversion.
 * <p>
 * This is a lightweight alternative to Arrow IPC serialization that avoids adding
 * new dependencies. It can be upgraded to Arrow IPC in a follow-up PR if needed.
 *
 * @opensearch.internal
 */
public final class ResultSerializer {

    private ResultSerializer() {}

    /**
     * Converts a column-oriented {@link WorkerQueryResponse} back to row-oriented data.
     *
     * @param response the worker query response with column-major data
     * @return list of row arrays, each with one element per column
     */
    public static List<Object[]> toRows(WorkerQueryResponse response) {
        int rowCount = response.getRowCount();
        Object[][] columnData = response.getColumnData();
        int numCols = columnData.length;

        List<Object[]> rows = new ArrayList<>(rowCount);
        for (int row = 0; row < rowCount; row++) {
            Object[] rowData = new Object[numCols];
            for (int col = 0; col < numCols; col++) {
                rowData[col] = columnData[col][row];
            }
            rows.add(rowData);
        }
        return rows;
    }

    /**
     * Converts row-oriented data to a column-oriented {@link WorkerQueryResponse}.
     *
     * @param rows        list of row arrays
     * @param columnNames names for each column
     * @param columnTypes type names for each column
     * @return a WorkerQueryResponse with column-major data
     */
    public static WorkerQueryResponse toColumnResponse(List<Object[]> rows, List<String> columnNames, List<String> columnTypes) {
        if (rows.isEmpty()) {
            return new WorkerQueryResponse(columnNames, columnTypes, 0, new Object[0][]);
        }
        int numCols = rows.get(0).length;
        int numRows = rows.size();
        Object[][] columnData = new Object[numCols][numRows];
        for (int row = 0; row < numRows; row++) {
            Object[] rowData = rows.get(row);
            for (int col = 0; col < numCols; col++) {
                columnData[col][row] = col < rowData.length ? rowData[col] : null;
            }
        }
        return new WorkerQueryResponse(columnNames, columnTypes, numRows, columnData);
    }
}

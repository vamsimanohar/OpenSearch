/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import java.util.List;

/**
 * Executes SQL queries against data warehouse files (e.g., Parquet on S3)
 * using a native query engine.
 * <p>
 * Implementations are provided by query backend plugins (e.g., analytics-backend-datafusion)
 * and discovered via {@link org.opensearch.plugins.ExtensiblePlugin} SPI.
 *
 * @opensearch.internal
 */
public interface DataWarehouseQueryEngine {

    /**
     * Executes a SQL query against data files described by the scan context.
     *
     * @param scanContext the scan context containing SQL, file paths, and storage config
     * @return result rows
     */
    Iterable<Object[]> executeQuery(DataWarehouseScanContext scanContext);

    /**
     * Executes a SQL query against data files described by the scan context
     * and returns the results as Arrow IPC bytes.
     *
     * @param scanContext the scan context containing SQL, file paths, and storage config
     * @return Arrow IPC serialized result bytes
     * @throws UnsupportedOperationException if the backend does not support Arrow IPC
     */
    default byte[] executeQueryArrowIpc(DataWarehouseScanContext scanContext) {
        throw new UnsupportedOperationException("Arrow IPC execution not supported by this backend");
    }

    /**
     * Deserializes Arrow IPC bytes into row-oriented results.
     *
     * @param arrowIpcData the Arrow IPC serialized bytes
     * @return result rows deserialized from the Arrow IPC data
     * @throws UnsupportedOperationException if the backend does not support Arrow IPC
     */
    default Iterable<Object[]> readArrowIpc(byte[] arrowIpcData) {
        throw new UnsupportedOperationException("Arrow IPC deserialization not supported by this backend");
    }

    /**
     * Executes a merge SQL query against Arrow IPC data collected from workers
     * using a StreamingTable approach.
     *
     * @param workerArrowIpcData list of Arrow IPC serialized bytes from each worker
     * @param mergeSql the SQL query to execute over the combined worker results
     * @return merged result rows
     * @throws UnsupportedOperationException if the backend does not support merge execution
     */
    default Iterable<Object[]> executeMerge(List<byte[]> workerArrowIpcData, String mergeSql) {
        throw new UnsupportedOperationException("Merge execution not supported by this backend");
    }

    /**
     * Reads the column names from the Arrow IPC schema header without reading all data.
     *
     * @param arrowIpcData the Arrow IPC serialized bytes
     * @return list of column names from the IPC schema
     * @throws UnsupportedOperationException if the backend does not support Arrow IPC
     */
    default List<String> readArrowIpcColumnNames(byte[] arrowIpcData) {
        throw new UnsupportedOperationException("Arrow IPC column name reading not supported by this backend");
    }
}

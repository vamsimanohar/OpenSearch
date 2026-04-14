/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

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
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

/**
 * Executes SQL queries against external data files (e.g., Parquet on S3) using a
 * native query engine. This is the minimal capability interface that external table
 * plugins (like lakehouse-iceberg) depend on.
 * <p>
 * Implementations are provided by query backend plugins (e.g., analytics-backend-datafusion).
 *
 * @opensearch.internal
 */
public interface ExternalQueryBackend {

    /**
     * Executes a query against remote data files using the native engine.
     *
     * @param scanContext the resolved scan context containing SQL, file paths, and storage config
     * @return result rows
     */
    Iterable<Object[]> executeRemoteQuery(ExternalScanContext scanContext);
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.analytics.exec.ExternalQueryBackend;
import org.opensearch.analytics.exec.ExternalScanContext;

/**
 * SPI extension point for back-end query engines for query planning and execution capabilities
 * as needed by the {@link org.opensearch.analytics.exec.QueryPlanExecutor}
 */
public interface AnalyticsSearchBackendPlugin extends SearchExecEngineProvider, ExternalQueryBackend {

    /**
     * Executes a query against remote data files using the native engine.
     * The scan context contains the SQL query, file paths, and storage config.
     *
     * @param scanContext the resolved scan context from an external table plugin
     * @return result rows
     * @throws UnsupportedOperationException if this backend does not support remote execution
     */
    default Iterable<Object[]> executeRemoteQuery(ExternalScanContext scanContext) {
        throw new UnsupportedOperationException(name() + " does not support remote query execution");
    }
}

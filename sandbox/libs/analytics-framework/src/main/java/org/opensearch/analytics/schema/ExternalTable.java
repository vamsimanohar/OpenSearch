/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.schema;

/**
 * Marker interface for Calcite tables backed by external data sources
 * (e.g., Iceberg, Delta Lake) rather than OpenSearch indices.
 * <p>
 * When the {@link org.opensearch.analytics.exec.QueryPlanExecutor} encounters
 * a {@code TableScan} whose table implements this interface, it routes execution
 * through an {@link org.opensearch.analytics.exec.ExternalTableExecutor} instead
 * of the default shard-based path.
 *
 * @opensearch.internal
 */
public interface ExternalTable {
}

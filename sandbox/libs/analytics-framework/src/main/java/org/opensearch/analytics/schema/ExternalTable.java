/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.schema;

/**
 * Marker interface for external (non-OpenSearch) tables in the Calcite schema.
 * Implementations carry catalog metadata needed to plan and execute scans
 * against external data sources (e.g., Iceberg, Delta Lake).
 *
 * @opensearch.internal
 */
public interface ExternalTable {

    /** Returns the fully-qualified table name in the external catalog. */
    String qualifiedName();

    /** Returns the table format identifier (e.g., "iceberg", "delta", "hudi"). */
    String format();
}

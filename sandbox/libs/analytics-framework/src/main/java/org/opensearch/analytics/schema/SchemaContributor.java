/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.schema;

import org.apache.calcite.schema.SchemaPlus;
import org.opensearch.cluster.metadata.IndexMetadata;

/**
 * SPI for plugins that contribute additional tables to the Calcite schema.
 * <p>
 * Implementations are discovered by the analytics-engine hub via
 * {@link org.opensearch.plugins.ExtensiblePlugin.ExtensionLoader} and called
 * after the base OpenSearch index schema is built. This allows external data
 * source plugins (e.g., Iceberg, Delta Lake) to register their tables into
 * the Calcite schema without creating compile-time dependencies from the
 * analytics engine to those plugins.
 * <p>
 * Implementations may also override {@link #claims(IndexMetadata)} to indicate
 * that certain OpenSearch indices are owned by the contributor and should be
 * skipped by the default schema builder.
 *
 * @opensearch.internal
 */
public interface SchemaContributor {

    /**
     * Adds tables to the given Calcite schema.
     *
     * @param schema       the mutable Calcite schema to enrich
     * @param clusterState the current cluster state (opaque Object to avoid
     *                     server dependency in the library)
     */
    void contributeSchema(SchemaPlus schema, Object clusterState);

    /**
     * Returns true if this contributor owns the given index and will handle
     * its schema registration. Indices claimed by any contributor are skipped
     * by {@code OpenSearchSchemaBuilder}.
     *
     * @param indexMetadata the index metadata to check
     * @return true if this contributor handles this index
     */
    default boolean claims(IndexMetadata indexMetadata) {
        return false;
    }
}

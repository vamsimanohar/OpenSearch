/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.schema.ExternalTable;

/**
 * Executes a query plan against an external (non-OpenSearch) table.
 * <p>
 * Implementations are discovered by
 * {@link org.opensearch.plugins.ExtensiblePlugin} and injected into the
 * {@link QueryPlanExecutor} so that Iceberg/Delta/etc. tables bypass the
 * normal shard-based execution path.
 *
 * @opensearch.internal
 */
@FunctionalInterface
public interface ExternalTableExecutor {

    /**
     * Executes the given logical plan against an external table.
     *
     * @param logicalPlan   the optimized Calcite plan
     * @param externalTable the external table found in the plan
     * @return result rows produced by the external engine
     */
    Iterable<Object[]> execute(RelNode logicalPlan, ExternalTable externalTable);
}

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
 * Prepares a scan context for queries against external (non-OpenSearch) tables.
 * Implementations are discovered by {@link org.opensearch.plugins.ExtensiblePlugin.ExtensionLoader}
 * and injected into the QueryPlanExecutor. The actual execution is delegated to the analytics backend.
 *
 * @opensearch.internal
 */
public interface ExternalTableExecutor {

    /**
     * Returns whether this executor handles the given external table.
     * Used by the plan executor to route queries when multiple format plugins are installed.
     *
     * @param externalTable the external table to check
     * @return {@code true} if this executor can plan scans for the table
     */
    boolean supports(ExternalTable externalTable);

    /**
     * Prepares a scan context for an external table query.
     *
     * @param logicalPlan   the optimized Calcite logical plan
     * @param externalTable the external table found in the plan
     * @return scan context for native execution
     */
    ExternalScanContext prepareScan(RelNode logicalPlan, ExternalTable externalTable);
}

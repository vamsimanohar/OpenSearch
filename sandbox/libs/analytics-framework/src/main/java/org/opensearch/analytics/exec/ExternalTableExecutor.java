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
 * Implementations are discovered by ExtensiblePlugin and injected into the
 * QueryPlanExecutor. The actual execution is delegated to the analytics backend.
 */
public interface ExternalTableExecutor {

    /**
     * Prepares a scan context for an external table query.
     *
     * @param logicalPlan   the optimized Calcite plan
     * @param externalTable the external table found in the plan
     * @return scan context for native execution
     */
    ExternalScanContext prepareScan(RelNode logicalPlan, ExternalTable externalTable);
}

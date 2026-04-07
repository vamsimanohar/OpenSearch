/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed;

import org.opensearch.action.ActionType;

/**
 * Internal action type for distributed Iceberg worker execution.
 * Sent from a coordinator node to worker nodes so each worker
 * processes its assigned subset of Parquet files.
 */
public class LakehouseWorkerAction extends ActionType<LakehouseWorkerResponse> {

    /** Action name following OpenSearch internal action conventions. */
    public static final String NAME = "internal:lakehouse/worker/execute";

    /** Singleton instance. */
    public static final LakehouseWorkerAction INSTANCE = new LakehouseWorkerAction();

    private LakehouseWorkerAction() {
        super(NAME, LakehouseWorkerResponse::new);
    }
}

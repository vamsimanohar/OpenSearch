/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed.worker;

import org.opensearch.action.ActionType;

/**
 * Action singleton for worker-side distributed query execution.
 * <p>
 * The coordinator sends {@link WorkerQueryRequest} to workers via this action.
 * Workers execute the SQL query against their assigned Parquet file subset
 * and return a {@link WorkerQueryResponse} with column-oriented result data.
 *
 * @opensearch.internal
 */
public class WorkerQueryAction extends ActionType<WorkerQueryResponse> {

    /** The action name used for transport registration. */
    public static final String NAME = "cluster:internal/lakehouse/worker/query";

    /** The singleton instance. */
    public static final WorkerQueryAction INSTANCE = new WorkerQueryAction();

    private WorkerQueryAction() {
        super(NAME, WorkerQueryResponse::new);
    }
}

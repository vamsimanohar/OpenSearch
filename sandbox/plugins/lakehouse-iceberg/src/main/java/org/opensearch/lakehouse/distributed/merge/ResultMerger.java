/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed.merge;

import org.opensearch.lakehouse.distributed.worker.WorkerQueryResponse;

import java.util.Collections;
import java.util.List;

/**
 * Combines partial {@link WorkerQueryResponse} results from distributed workers into a single
 * merged response according to the given {@link MergeStrategy}.
 * <p>
 * All distributable strategies (CONCAT, GLOBAL_MERGE, TOPK_MERGE) are handled by the
 * DataFusion native pipeline via Arrow IPC. Only SINGLE_NODE passes through here.
 *
 * @opensearch.internal
 */
public final class ResultMerger {

    private ResultMerger() {}

    /**
     * Merges multiple worker responses. Only SINGLE_NODE is still handled in Java;
     * all other strategies are routed through the DataFusion native pipeline.
     *
     * @param responses the worker responses to merge
     * @param strategy  the merge strategy
     * @return the merged response
     */
    public static WorkerQueryResponse merge(List<WorkerQueryResponse> responses, MergeStrategy strategy) {
        if (strategy != MergeStrategy.SINGLE_NODE) {
            throw new IllegalStateException(strategy + " merge is handled by DataFusion native pipeline");
        }
        for (WorkerQueryResponse r : responses) {
            if (r.getRowCount() > 0) return r;
        }
        return emptyResponse(responses);
    }

    static WorkerQueryResponse emptyResponse(List<WorkerQueryResponse> responses) {
        if (!responses.isEmpty()) {
            WorkerQueryResponse first = responses.get(0);
            return new WorkerQueryResponse(first.getColumnNames(), first.getColumnTypes(), 0, new Object[0][]);
        }
        return new WorkerQueryResponse(Collections.emptyList(), Collections.emptyList(), 0, new Object[0][]);
    }

}

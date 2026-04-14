/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed.merge;

/**
 * Merge strategy for combining partial results from distributed worker nodes.
 *
 * @opensearch.internal
 */
public enum MergeStrategy {
    /**
     * Simple row concatenation. Used for scan/filter/project queries
     * where each worker returns a disjoint subset of rows.
     */
    CONCAT,

    /**
     * Re-aggregate global results from single-row partial aggregates.
     * Used for global aggregations without GROUP BY (e.g., SELECT COUNT(*), SUM(x), MIN(y)).
     * Each worker returns one row; the coordinator combines them (SUM of COUNTs, MIN of MINs, etc.).
     */
    GLOBAL_MERGE,

    /**
     * Merge-sort pre-sorted worker results and take top K.
     * Used for ORDER BY + LIMIT without aggregation. Each worker returns its
     * local top-K; the coordinator merge-sorts and takes the global top-K.
     */
    TOPK_MERGE,

    /**
     * Route the entire query to a single node for execution.
     * Used for queries that cannot be trivially distributed: GROUP BY, DISTINCT,
     * AVG aggregates, JOINs, or any pattern not covered by the other strategies.
     * This is deterministic routing, not an error fallback.
     */
    SINGLE_NODE
}

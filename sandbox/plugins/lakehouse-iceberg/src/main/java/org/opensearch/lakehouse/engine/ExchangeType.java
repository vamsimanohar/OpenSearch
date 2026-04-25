/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.engine;

/**
 * Type of data exchange between stages in a distributed query plan.
 * <p>
 * Each exchange type determines how Arrow record batches flow from
 * producer tasks to consumer tasks via Arrow Flight RPC.
 *
 * @opensearch.internal
 */
public enum ExchangeType {

    /**
     * All producer tasks send their output to a single consumer task (the coordinator).
     * Used for: global aggregation merge, top-K merge-sort, simple scan concatenation.
     * Arrow Flight: each producer is a FlightEndpoint; coordinator reads all endpoints sequentially.
     */
    GATHER,

    /**
     * Producers partition output rows by hash of specified columns and send each partition
     * to the corresponding consumer task. Used for: distributed GROUP BY where each consumer
     * gets a disjoint subset of group keys, enabling correct per-group aggregation with LIMIT.
     * Arrow Flight: each producer exposes N FlightEndpoints (one per consumer partition);
     * each consumer reads its partition endpoint from every producer.
     */
    HASH,

    /**
     * No exchange — the fragment executes locally and its output is the final result.
     * Used for single-node execution or for the final coordinator stage that reads
     * from an exchange input.
     */
    NONE
}

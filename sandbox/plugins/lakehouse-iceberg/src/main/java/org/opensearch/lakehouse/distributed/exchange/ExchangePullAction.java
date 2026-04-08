/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse.distributed.exchange;

/**
 * Constants for the exchange pull transport action.
 *
 * <p>This action allows any node to pull stage output from any other node.
 * Workers register their stage output in {@link WorkerOutputManager},
 * and downstream nodes pull via this transport action.
 */
public final class ExchangePullAction {

    /** Transport action name for pulling stage output from a worker. */
    public static final String NAME = "indices:data/read/lakehouse_exchange_pull";

    private ExchangePullAction() {}
}

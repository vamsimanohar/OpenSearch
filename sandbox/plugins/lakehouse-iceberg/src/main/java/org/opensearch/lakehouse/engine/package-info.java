/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Multi-stage distributed query engine for the lakehouse plugin.
 * <p>
 * Models query execution as a pipeline of stages connected by Arrow Flight exchanges.
 * Inspired by Trino's stage/fragment/exchange architecture, adapted for DataFusion
 * per-node execution and Arrow Flight RPC as the exchange transport.
 */
package org.opensearch.lakehouse.engine;

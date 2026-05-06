/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * DataFusion-backed implementations of the analytics-engine stage-exchange SPI.
 *
 * <p>This package provides:
 * <ul>
 *   <li>{@link org.opensearch.be.datafusion.exchange.DataFusionExchangeSinkProvider} — factory registered via
 *       {@link org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin#getExchangeSinkProvider()}.</li>
 *   <li>{@link org.opensearch.be.datafusion.exchange.DataFusionExchangeSink} — accumulates Arrow batches fed from
 *       child stages and, on {@code close()}, executes a coordinator SQL fragment over them via the
 *       native DataFusion runtime.</li>
 * </ul>
 *
 * <p>For P1 the coordinator fragment bytes handed to {@code createSink(byte[])} are a UTF-8-encoded
 * SQL string. Richer encodings (e.g., Substrait) will replace this in later phases.
 */
package org.opensearch.be.datafusion.exchange;

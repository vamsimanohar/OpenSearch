/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.exchange;

import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.ExchangeSinkProvider;
import org.opensearch.be.datafusion.DataFusionService;

import java.nio.charset.StandardCharsets;

/**
 * {@link ExchangeSinkProvider} for the DataFusion backend.
 *
 * <p>For P1 the {@code coordinatorFragmentBytes} passed to {@link #createSink(byte[])}
 * are the UTF-8 encoding of a coordinator SQL string. A later phase will replace this
 * with a richer encoding (e.g., Substrait).
 *
 * <h2>Framework gap note</h2>
 * <p>The SPI signature {@code createSink(byte[])} does not hand a downstream sink to the
 * provider, so the sink constructed here is created with {@code downstream=null}. See
 * {@link DataFusionExchangeSink} for details on how this changes result handling.
 *
 * @opensearch.internal
 */
public final class DataFusionExchangeSinkProvider implements ExchangeSinkProvider {

    private final DataFusionService dfService;

    /**
     * Creates a provider bound to the given {@link DataFusionService}, whose native runtime
     * is used for every sink created by this instance.
     *
     * @param dfService backend runtime; must not be {@code null}
     */
    public DataFusionExchangeSinkProvider(DataFusionService dfService) {
        if (dfService == null) {
            throw new IllegalArgumentException("dfService must not be null");
        }
        this.dfService = dfService;
    }

    @Override
    public ExchangeSink createSink(byte[] coordinatorFragmentBytes) {
        if (coordinatorFragmentBytes == null) {
            throw new IllegalArgumentException("coordinatorFragmentBytes must not be null");
        }
        String coordinatorSql = new String(coordinatorFragmentBytes, StandardCharsets.UTF_8);
        return new DataFusionExchangeSink(coordinatorSql, null, dfService);
    }
}

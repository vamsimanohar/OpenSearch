/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.exchange;

import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.analytics.spi.ExchangeSinkContext;
import org.opensearch.analytics.spi.ExchangeSinkProvider;
import org.opensearch.be.datafusion.DataFusionService;

import java.nio.charset.StandardCharsets;

/**
 * {@link ExchangeSinkProvider} for the DataFusion backend.
 *
 * <p>The {@code coordinatorFragmentBytes} from {@link ExchangeSinkContext#fragmentBytes()}
 * are the UTF-8 encoding of a coordinator SQL string. A later phase will replace this
 * with a richer encoding (e.g., Substrait).
 *
 * @opensearch.internal
 */
public final class DataFusionExchangeSinkProvider implements ExchangeSinkProvider {

    private final DataFusionService dfService;

    /**
     * Creates a provider bound to the given {@link DataFusionService}.
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
    public ExchangeSink createSink(ExchangeSinkContext context) {
        if (context == null) {
            throw new IllegalArgumentException("context must not be null");
        }
        byte[] fragmentBytes = context.fragmentBytes();
        if (fragmentBytes == null) {
            throw new IllegalArgumentException("fragmentBytes must not be null");
        }
        String coordinatorSql = new String(fragmentBytes, StandardCharsets.UTF_8);
        return new DataFusionExchangeSink(coordinatorSql, context.downstream(), dfService);
    }
}

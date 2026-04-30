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
import org.opensearch.be.datafusion.DataFusionService;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;

import static org.mockito.Mockito.mock;

/**
 * Tests for {@link DataFusionExchangeSinkProvider}.
 */
public class DataFusionExchangeSinkProviderTests extends OpenSearchTestCase {

    public void testConstructorRejectsNullService() {
        expectThrows(IllegalArgumentException.class, () -> new DataFusionExchangeSinkProvider(null));
    }

    public void testCreateSinkReturnsSinkWithDecodedSql() {
        DataFusionService service = mock(DataFusionService.class);
        DataFusionExchangeSinkProvider provider = new DataFusionExchangeSinkProvider(service);

        byte[] bytes = "SELECT * FROM input".getBytes(StandardCharsets.UTF_8);
        ExchangeSinkContext ctx = new ExchangeSinkContext("q1", 0, bytes, null, null, null);
        ExchangeSink sink = provider.createSink(ctx);

        assertNotNull("createSink must return a non-null sink", sink);
        assertTrue(sink instanceof DataFusionExchangeSink);
        DataFusionExchangeSink dfSink = (DataFusionExchangeSink) sink;
        assertEquals("SELECT * FROM input", dfSink.coordinatorSql());
    }

    public void testCreateSinkRejectsNullContext() {
        DataFusionService service = mock(DataFusionService.class);
        DataFusionExchangeSinkProvider provider = new DataFusionExchangeSinkProvider(service);
        expectThrows(IllegalArgumentException.class, () -> provider.createSink(null));
    }

    public void testCreateSinkRejectsNullFragmentBytes() {
        DataFusionService service = mock(DataFusionService.class);
        DataFusionExchangeSinkProvider provider = new DataFusionExchangeSinkProvider(service);
        ExchangeSinkContext ctx = new ExchangeSinkContext("q1", 0, null, null, null, null);
        expectThrows(IllegalArgumentException.class, () -> provider.createSink(ctx));
    }

    public void testCreateSinkWithEmptyBytesProducesEmptySql() {
        DataFusionService service = mock(DataFusionService.class);
        DataFusionExchangeSinkProvider provider = new DataFusionExchangeSinkProvider(service);
        ExchangeSinkContext ctx = new ExchangeSinkContext("q1", 0, new byte[0], null, null, null);
        ExchangeSink sink = provider.createSink(ctx);
        assertNotNull(sink);
        assertEquals("", ((DataFusionExchangeSink) sink).coordinatorSql());
    }

    public void testCreateSinkHandlesUtf8Correctly() {
        DataFusionService service = mock(DataFusionService.class);
        DataFusionExchangeSinkProvider provider = new DataFusionExchangeSinkProvider(service);

        String sql = "SELECT '\u00e9\u00e8\u00e0' AS greeting";
        ExchangeSinkContext ctx = new ExchangeSinkContext("q1", 0, sql.getBytes(StandardCharsets.UTF_8), null, null, null);
        ExchangeSink sink = provider.createSink(ctx);
        assertEquals(sql, ((DataFusionExchangeSink) sink).coordinatorSql());
    }
}

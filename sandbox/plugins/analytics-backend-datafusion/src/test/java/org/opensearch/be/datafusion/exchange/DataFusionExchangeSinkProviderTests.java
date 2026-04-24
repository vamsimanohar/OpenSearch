/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.exchange;

import org.opensearch.analytics.spi.ExchangeSink;
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
        ExchangeSink sink = provider.createSink(bytes);

        assertNotNull("createSink must return a non-null sink", sink);
        assertTrue(sink instanceof DataFusionExchangeSink);
        DataFusionExchangeSink dfSink = (DataFusionExchangeSink) sink;
        assertEquals("SELECT * FROM input", dfSink.coordinatorSql());
        // In P1 the framework does not pipe the downstream through — this is a recorded gap.
        assertNull("Downstream must be null under the P1 framework contract", dfSink.downstream());
    }

    public void testCreateSinkRejectsNullBytes() {
        DataFusionService service = mock(DataFusionService.class);
        DataFusionExchangeSinkProvider provider = new DataFusionExchangeSinkProvider(service);
        expectThrows(IllegalArgumentException.class, () -> provider.createSink(null));
    }

    public void testCreateSinkWithEmptyBytesProducesEmptySql() {
        DataFusionService service = mock(DataFusionService.class);
        DataFusionExchangeSinkProvider provider = new DataFusionExchangeSinkProvider(service);
        ExchangeSink sink = provider.createSink(new byte[0]);
        assertNotNull(sink);
        assertEquals("", ((DataFusionExchangeSink) sink).coordinatorSql());
    }

    public void testCreateSinkHandlesUtf8Correctly() {
        DataFusionService service = mock(DataFusionService.class);
        DataFusionExchangeSinkProvider provider = new DataFusionExchangeSinkProvider(service);

        // Non-ASCII characters should survive the UTF-8 round-trip.
        String sql = "SELECT '\u00e9\u00e8\u00e0' AS greeting";
        ExchangeSink sink = provider.createSink(sql.getBytes(StandardCharsets.UTF_8));
        assertEquals(sql, ((DataFusionExchangeSink) sink).coordinatorSql());
    }
}

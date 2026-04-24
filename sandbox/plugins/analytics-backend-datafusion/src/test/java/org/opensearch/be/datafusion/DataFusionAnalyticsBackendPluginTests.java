/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.spi.ExchangeSinkProvider;
import org.opensearch.be.datafusion.exchange.DataFusionExchangeSinkProvider;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link DataFusionAnalyticsBackendPlugin} — focused on the
 * {@link DataFusionAnalyticsBackendPlugin#getExchangeSinkProvider()} wiring added in P1.
 */
public class DataFusionAnalyticsBackendPluginTests extends OpenSearchTestCase {

    public void testGetExchangeSinkProviderReturnsDataFusionProvider() {
        DataFusionService svc = mock(DataFusionService.class);
        DataFusionPlugin plugin = mock(DataFusionPlugin.class);
        when(plugin.getDataFusionService()).thenReturn(svc);
        when(plugin.name()).thenReturn("datafusion");

        DataFusionAnalyticsBackendPlugin spi = new DataFusionAnalyticsBackendPlugin(plugin);
        ExchangeSinkProvider provider = spi.getExchangeSinkProvider();

        assertNotNull(provider);
        assertTrue(provider instanceof DataFusionExchangeSinkProvider);
    }

    public void testNameDelegatesToPlugin() {
        DataFusionPlugin plugin = mock(DataFusionPlugin.class);
        when(plugin.name()).thenReturn("datafusion");
        DataFusionAnalyticsBackendPlugin spi = new DataFusionAnalyticsBackendPlugin(plugin);
        assertEquals("datafusion", spi.name());
    }
}

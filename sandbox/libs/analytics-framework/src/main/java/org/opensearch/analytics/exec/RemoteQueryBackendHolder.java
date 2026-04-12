/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;

/**
 * Static holder for the analytics backend used by distributed query workers.
 * <p>
 * Set by the analytics-engine plugin during initialization, read by the
 * lakehouse-iceberg plugin's worker transport action. This avoids Guice
 * binding issues with concrete backend classes that reference server-internal types.
 */
public final class RemoteQueryBackendHolder {

    private static volatile AnalyticsSearchBackendPlugin provider;

    private RemoteQueryBackendHolder() {}

    public static void setProvider(AnalyticsSearchBackendPlugin backend) {
        provider = backend;
    }

    public static AnalyticsSearchBackendPlugin getProvider() {
        return provider;
    }
}

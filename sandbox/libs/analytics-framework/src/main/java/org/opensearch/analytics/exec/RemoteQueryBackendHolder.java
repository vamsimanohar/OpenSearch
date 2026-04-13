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
 * Static holder for the analytics query backend, shared across plugins via
 * the analytics-framework lib classloader.
 * <p>
 * <b>Why this exists instead of normal Guice injection:</b>
 * The concrete backend ({@code DataFusionPlugin}) implements both
 * {@link AnalyticsSearchBackendPlugin} (query execution for external tables)
 * and {@code SearchBackEndPlugin} (shard-level storage engine for composite indices).
 * The {@code SearchBackEndPlugin} interface references server-internal types
 * ({@code ReaderManagerConfig}, {@code DataFormat}) that are not on the plugin
 * classloader. Guice eagerly introspects all methods on the concrete class during
 * binding — even those never called — hitting {@code ClassNotFoundException}.
 * A plain static field avoids Guice introspection entirely.
 * <p>
 * <b>Lifecycle:</b>
 * <ol>
 *   <li>analytics-engine discovers the backend via SPI ({@code ExtensionLoader})</li>
 *   <li>analytics-engine calls {@link #setProvider} during {@code createComponents()}</li>
 *   <li>lakehouse-iceberg's distributed workers call {@link #getProvider} at query time
 *       to execute SQL against external Parquet files via the backend</li>
 * </ol>
 * <p>
 * <b>Note:</b> The single-node path does not use this holder — analytics-engine's
 * {@code DefaultPlanExecutor} holds the backend reference directly (received via
 * constructor, not Guice). This holder is only needed for the distributed execution
 * path where worker nodes call the backend outside of {@code DefaultPlanExecutor}.
 * <p>
 * This lives in analytics-framework (a shared lib) so all plugins can access it
 * through the parent classloader without declaring {@code extendedPlugins} dependencies.
 *
 * @opensearch.internal
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

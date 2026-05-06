/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lakehouse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.lakehouse.catalog.AwsCredentials;
import org.opensearch.lakehouse.catalog.IcebergCatalogConnector;
import org.opensearch.lakehouse.catalog.LakehouseCredentialsProvider;
import org.opensearch.lakehouse.scan.IcebergScanPlanner;

import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Singleton holder for shared state across SPI-created instances.
 *
 * <p>OpenSearch SPI ({@code SPIClassIterator}) creates separate instances of
 * {@link LakehousePlugin} for each interface it implements. All instances
 * access shared state through this singleton.
 */
public final class LakehouseState {

    private static final Logger logger = LogManager.getLogger(LakehouseState.class);

    @SuppressWarnings("removal")
    private static final LakehouseState INSTANCE = new LakehouseState();

    private final IcebergCatalogConnector catalogConnector;
    private final ExecutorService scanExecutor;
    private final IcebergScanPlanner scanPlanner;

    @SuppressWarnings("removal")
    private LakehouseState() {
        this.catalogConnector = new IcebergCatalogConnector();
        this.scanExecutor = createPrivilegedExecutor();
        this.scanPlanner = new IcebergScanPlanner(scanExecutor);
    }

    /** Returns the singleton instance. */
    public static LakehouseState instance() {
        return INSTANCE;
    }

    /** Returns the shared catalog connector. */
    public IcebergCatalogConnector catalogConnector() {
        return catalogConnector;
    }

    /** Returns the shared scan planner. */
    public IcebergScanPlanner scanPlanner() {
        return scanPlanner;
    }

    /**
     * Shuts down the scan executor gracefully.
     * Called from {@link LakehousePlugin#close()}.
     */
    public void close() {
        logger.info("[LakehouseState] Shutting down scan executor");
        scanExecutor.shutdown();
        try {
            if (!scanExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                scanExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            scanExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Creates an executor that propagates the plugin's security context, classloader,
     * and ThreadLocal credentials to executor threads.
     *
     * <p>Iceberg's parallel manifest reads ({@code scan.planWith(executor).planFiles()})
     * run on executor threads that don't inherit the calling thread's doPrivileged or
     * ThreadLocal state. This executor wraps every task to:
     * <ol>
     *   <li>Set the thread context classloader to the plugin's classloader</li>
     *   <li>Propagate ThreadLocal credentials from the calling thread</li>
     *   <li>Run the task inside doPrivileged with the plugin's AccessControlContext</li>
     * </ol>
     */
    @SuppressWarnings("removal")
    private static ExecutorService createPrivilegedExecutor() {
        AccessControlContext acc = AccessController.getContext();
        ClassLoader pluginClassLoader = LakehouseState.class.getClassLoader();
        int threads = Math.max(2, Runtime.getRuntime().availableProcessors() / 2);
        ExecutorService delegate = Executors.newFixedThreadPool(threads);
        return new AbstractExecutorService() {
            @Override
            public void execute(Runnable command) {
                AwsCredentials creds = LakehouseCredentialsProvider.get();
                delegate.execute(() -> {
                    ClassLoader prev = Thread.currentThread().getContextClassLoader();
                    Thread.currentThread().setContextClassLoader(pluginClassLoader);
                    if (creds != null) {
                        LakehouseCredentialsProvider.set(creds);
                    }
                    try {
                        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                            command.run();
                            return null;
                        }, acc);
                    } finally {
                        LakehouseCredentialsProvider.clear();
                        Thread.currentThread().setContextClassLoader(prev);
                    }
                });
            }

            @Override
            public void shutdown() {
                delegate.shutdown();
            }

            @Override
            public List<Runnable> shutdownNow() {
                return delegate.shutdownNow();
            }

            @Override
            public boolean isShutdown() {
                return delegate.isShutdown();
            }

            @Override
            public boolean isTerminated() {
                return delegate.isTerminated();
            }

            @Override
            public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
                return delegate.awaitTermination(timeout, unit);
            }
        };
    }
}

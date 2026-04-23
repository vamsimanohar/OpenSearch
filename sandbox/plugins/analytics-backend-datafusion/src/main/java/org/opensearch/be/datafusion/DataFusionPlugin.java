/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.spi.SearchExecEngineProvider;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.dataformat.ReaderManagerConfig;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchBackEndPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Main plugin class for the DataFusion native engine integration.
 * <p>
 * Handles shard-level query execution via {@link SearchExecEngineProvider}.
 * Data warehouse query execution is handled by {@link DatafusionWarehouseQueryEngine},
 * which is discovered as a separate SPI.
 */
public class DataFusionPlugin extends Plugin implements SearchBackEndPlugin<DatafusionReader>, SearchExecEngineProvider {

    private static final Logger logger = LogManager.getLogger(DataFusionPlugin.class);

    private static final String SUPPORTED_FORMAT = "parquet";

    private static final long DEFAULT_MEMORY_POOL_LIMIT = 0L; // 0 = unlimited (GreedyMemoryPool(MAX))

    /** Memory pool limit for the DataFusion runtime (Rust heap, not JVM heap). */
    public static final Setting<Long> DATAFUSION_MEMORY_POOL_LIMIT = Setting.longSetting(
        "datafusion.memory_pool_limit_bytes",
        DEFAULT_MEMORY_POOL_LIMIT,
        0L,
        Setting.Property.NodeScope
    );

    /**
     * Memory pool type: "fair_spill" (default) or "greedy".
     * FairSpill = fair sharing across operators, spills to disk when exceeded. Best for production.
     * Greedy = first-come-first-served, slightly faster for single isolated queries.
     */
    public static final Setting<String> DATAFUSION_MEMORY_POOL_TYPE = Setting.simpleString(
        "datafusion.memory_pool_type",
        "fair_spill",
        Setting.Property.NodeScope
    );

    private static final long DEFAULT_SPILL_LIMIT = 100L * 1024 * 1024 * 1024; // 100GB — disk space, not memory

    /** Spill disk limit — max temp file space DataFusion can use for spilling to disk. */
    public static final Setting<Long> DATAFUSION_SPILL_MEMORY_LIMIT = Setting.longSetting(
        "datafusion.spill_memory_limit_bytes",
        DEFAULT_SPILL_LIMIT,
        0L,
        Setting.Property.NodeScope
    );

    /** Shared across plugin instance and SPI instances (separate classloader instances). */
    private static volatile DataFusionService sharedDataFusionService;
    private static final Object INIT_LOCK = new Object();

    /**
     * Creates the DataFusion plugin.
     */
    public DataFusionPlugin() {}

    /**
     * Returns the shared DataFusionService, lazy-initializing if needed.
     * Used by {@link DatafusionWarehouseQueryEngine} to access the native runtime.
     */
    static DataFusionService ensureSharedService() {
        DataFusionService svc = sharedDataFusionService;
        if (svc != null) {
            return svc;
        }
        synchronized (INIT_LOCK) {
            if (sharedDataFusionService == null) {
                long memPool = getConfiguredLong("datafusion_memory_pool_limit_bytes", DEFAULT_MEMORY_POOL_LIMIT);
                long spillLimit = getConfiguredLong("datafusion_spill_memory_limit_bytes", DEFAULT_SPILL_LIMIT);
                String poolType = System.getProperty("datafusion_memory_pool_type", "fair_spill");
                if ("fair_spill".equals(poolType) && memPool == 0) {
                    memPool = autoDetectPoolLimit();
                    logger.info("FairSpill pool with no explicit limit — auto-detected {}MB", memPool / (1024 * 1024));
                }
                long effectiveLimit = "fair_spill".equals(poolType) && memPool > 0 ? -memPool : memPool;
                String spillDir = System.getProperty("java.io.tmpdir");
                int cpuThreads = (int) getConfiguredLong("datafusion_cpu_threads", Runtime.getRuntime().availableProcessors() * 3L / 4);
                sharedDataFusionService = DataFusionService.builder()
                    .memoryPoolLimit(effectiveLimit)
                    .spillMemoryLimit(spillLimit)
                    .spillDirectory(spillDir)
                    .cpuThreads(cpuThreads)
                    .build();
                sharedDataFusionService.start();
                logger.info(
                    "DataFusion service lazy-initialized (SPI path) — pool type={}, memory pool {}B, spill limit {}B, cpuThreads={}",
                    poolType,
                    effectiveLimit,
                    spillLimit,
                    cpuThreads
                );
            }
            return sharedDataFusionService;
        }
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        Settings settings = environment.settings();
        long memoryPoolLimit = getConfiguredLong("datafusion_memory_pool_limit_bytes", DATAFUSION_MEMORY_POOL_LIMIT.get(settings));
        long spillMemoryLimit = getConfiguredLong("datafusion_spill_memory_limit_bytes", DATAFUSION_SPILL_MEMORY_LIMIT.get(settings));
        String spillDir = environment.dataFiles()[0].getParent().resolve("tmp").toAbsolutePath().toString();

        synchronized (INIT_LOCK) {
            if (sharedDataFusionService == null) {
                String poolType = System.getProperty("datafusion_memory_pool_type", DATAFUSION_MEMORY_POOL_TYPE.get(settings));
                if ("fair_spill".equals(poolType) && memoryPoolLimit == 0) {
                    memoryPoolLimit = autoDetectPoolLimit();
                    logger.info("FairSpill pool with no explicit limit — auto-detected {}MB", memoryPoolLimit / (1024 * 1024));
                }
                long effectiveLimit = "fair_spill".equals(poolType) && memoryPoolLimit > 0 ? -memoryPoolLimit : memoryPoolLimit;
                int cpuThreads = (int) getConfiguredLong("datafusion_cpu_threads", Runtime.getRuntime().availableProcessors() * 3L / 4);
                sharedDataFusionService = DataFusionService.builder()
                    .memoryPoolLimit(effectiveLimit)
                    .spillMemoryLimit(spillMemoryLimit)
                    .spillDirectory(spillDir)
                    .cpuThreads(cpuThreads)
                    .build();
                sharedDataFusionService.start();
                logger.info(
                    "DataFusion plugin initialized — pool type={}, memory pool {}B, spill limit {}B, cpuThreads={}",
                    poolType,
                    effectiveLimit,
                    spillMemoryLimit,
                    cpuThreads
                );
            }
        }

        return Collections.singletonList(sharedDataFusionService);
    }

    private static long getConfiguredLong(String key, long defaultValue) {
        try {
            String val = System.getProperty(key);
            if (val == null) val = System.getenv(key);
            if (val != null) {
                long parsed = Long.parseLong(val.trim());
                logger.info("Config {} = {} (from system property/env)", key, parsed);
                return parsed;
            }
        } catch (Exception e) {
            logger.warn("Failed to read config {}: {}", key, e.getMessage());
        }
        return defaultValue;
    }

    /**
     * Auto-detect a sensible DataFusion memory pool limit.
     * Uses: total physical RAM - JVM max heap - 4GB OS/kernel overhead.
     * Minimum: 2GB. Falls back to 16GB if detection fails.
     */
    private static long autoDetectPoolLimit() {
        try {
            long totalPhysical = ((com.sun.management.OperatingSystemMXBean) java.lang.management.ManagementFactory
                .getOperatingSystemMXBean()).getTotalMemorySize();
            long jvmMax = Runtime.getRuntime().maxMemory();
            long osOverhead = 4L * 1024 * 1024 * 1024; // 4GB for OS/kernel
            long available = totalPhysical - jvmMax - osOverhead;
            return Math.max(available, 2L * 1024 * 1024 * 1024); // at least 2GB
        } catch (Exception e) {
            logger.warn("Failed to auto-detect memory — defaulting to 16GB: {}", e.getMessage());
            return 16L * 1024 * 1024 * 1024;
        }
    }

    @Override
    public String name() {
        return "datafusion";
    }

    @Override
    public EngineReaderManager<DatafusionReader> createReaderManager(ReaderManagerConfig settings) throws IOException {
        return new DatafusionReaderManager(settings.format(), settings.shardPath(), sharedDataFusionService);
    }

    @Override
    public List<String> getSupportedFormats() {
        return List.of(SUPPORTED_FORMAT);
    }

    @Override
    public SearchExecEngine<ExecutionContext, EngineResultStream> createSearchExecEngine(ExecutionContext ctx) {
        DatafusionReader dfReader = ctx.getReader().getReader(
            new DataFormat() {
                @Override public String name() { return SUPPORTED_FORMAT; }
                @Override public long priority() { return 0; }
                @Override public Set<FieldTypeCapabilities> supportedFields() { return Set.of(); }
            },
            DatafusionReader.class
        );
        if (dfReader == null) {
            throw new IllegalStateException("No DatafusionReader available in the acquired reader");
        }
        DatafusionContext context = new DatafusionContext(ctx.getTask(), dfReader, sharedDataFusionService.getNativeRuntime());
        DatafusionSearchExecEngine engine = new DatafusionSearchExecEngine(context, sharedDataFusionService::newChildAllocator);
        engine.prepare(ctx);
        return engine;
    }

    @Override
    public void close() throws IOException {
        synchronized (INIT_LOCK) {
            if (sharedDataFusionService != null) {
                sharedDataFusionService.close();
                sharedDataFusionService = null;
            }
        }
    }
}

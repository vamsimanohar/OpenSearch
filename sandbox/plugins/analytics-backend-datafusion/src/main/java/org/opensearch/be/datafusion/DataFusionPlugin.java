/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.ExecutionContext;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.exec.ExternalScanContext;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.StreamHandle;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.engine.dataformat.DataFormat;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Main plugin class for the DataFusion native engine integration.
 * <p>
 * Initializes the {@link DataFusionService} at node startup and creates
 * per-shard {@link DatafusionSearchExecEngine} instances via the
 * {@link AnalyticsSearchBackendPlugin} SPI.
 */
public class DataFusionPlugin extends Plugin implements SearchBackEndPlugin<DatafusionReader>, AnalyticsSearchBackendPlugin {

    private static final Logger logger = LogManager.getLogger(DataFusionPlugin.class);

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
     * Ensures DataFusionService is initialized. When this plugin is loaded via SPI
     * (ExtensiblePlugin.loadExtensions), createComponents() is never called, so we
     * lazy-initialize with sensible defaults on first use.
     */
    private DataFusionService ensureDataFusionService() {
        DataFusionService svc = sharedDataFusionService;
        if (svc != null) {
            return svc;
        }
        synchronized (INIT_LOCK) {
            if (sharedDataFusionService == null) {
                long memPool = getConfiguredLong("datafusion_memory_pool_limit_bytes", DEFAULT_MEMORY_POOL_LIMIT);
                long spillLimit = getConfiguredLong("datafusion_spill_memory_limit_bytes", DEFAULT_SPILL_LIMIT);
                String poolType = System.getProperty("datafusion_memory_pool_type", "fair_spill");
                long effectiveLimit = "fair_spill".equals(poolType) && memPool > 0 ? -memPool : memPool;
                String spillDir = System.getProperty("java.io.tmpdir");
                sharedDataFusionService = DataFusionService.builder()
                    .memoryPoolLimit(effectiveLimit)
                    .spillMemoryLimit(spillLimit)
                    .spillDirectory(spillDir)
                    .build();
                sharedDataFusionService.start();
                logger.info("DataFusion service lazy-initialized (SPI path) — pool type={}, memory pool {}B, spill limit {}B", poolType, effectiveLimit, spillLimit);
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
                String poolType = DATAFUSION_MEMORY_POOL_TYPE.get(settings);
                long effectiveLimit = "fair_spill".equals(poolType) && memoryPoolLimit > 0 ? -memoryPoolLimit : memoryPoolLimit;
                sharedDataFusionService = DataFusionService.builder()
                    .memoryPoolLimit(effectiveLimit)
                    .spillMemoryLimit(spillMemoryLimit)
                    .spillDirectory(spillDir)
                    .build();
                sharedDataFusionService.start();
                logger.info("DataFusion plugin initialized — pool type={}, memory pool {}B, spill limit {}B", poolType, effectiveLimit, spillMemoryLimit);
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

    @Override
    public String name() {
        return "datafusion";
    }

    @Override
    public EngineReaderManager<DatafusionReader> createReaderManager(ReaderManagerConfig settings) throws IOException {
        return new DatafusionReaderManager(settings.format(), settings.shardPath(), sharedDataFusionService);
    }

    /**
     * Data formats this plugin can handle. Used by CompositeEngine to route queries.
     */
    public List<DataFormat> getSupportedFormats() {
        return List.of();
    }

    @Override
    public SearchExecEngine<ExecutionContext, EngineResultStream> createSearchExecEngine(ExecutionContext ctx) {
        DatafusionReader dfReader = null;
        List<DataFormat> formats = getSupportedFormats();
        if (formats != null) {
            for (DataFormat format : formats) {
                dfReader = ctx.getReader().getReader(format, DatafusionReader.class);
                if (dfReader != null) {
                    break;
                }
            }
        }
        if (dfReader == null) {
            throw new IllegalStateException("No DatafusionReader available in the acquired reader");
        }
        DatafusionContext context = new DatafusionContext(ctx.getTask(), dfReader, sharedDataFusionService.getNativeRuntime());
        DatafusionSearchExecEngine engine = new DatafusionSearchExecEngine(context, sharedDataFusionService::newChildAllocator);
        engine.prepare(ctx);
        return engine;
    }

    @Override
    public Iterable<Object[]> executeRemoteQuery(ExternalScanContext scanContext) {
        long t0 = System.currentTimeMillis();
        DataFusionService dfService = ensureDataFusionService();

        Map<String, String> config = scanContext.getStorageConfig();
        boolean localMode = "true".equals(config.get("localMode"));
        String s3Region = localMode ? "" : config.getOrDefault("s3Region", "us-east-1");
        String s3Bucket = config.get("s3Bucket");
        String s3AccessKeyId = config.get("s3AccessKeyId");
        String s3SecretAccessKey = config.get("s3SecretAccessKey");
        String s3SessionToken = config.get("s3SessionToken");
        String s3Endpoint = config.get("s3Endpoint");

        String[] filePaths = scanContext.getDataFilePaths().toArray(new String[0]);
        long[] fileSizes = scanContext.getFileSizes();
        String tableName = scanContext.getTableName();
        String sqlQuery = scanContext.getSqlQuery();

        if (filePaths.length == 0) {
            logger.info("[DataFusionPlugin] No data files for table [{}] — returning empty result", tableName);
            return List.of();
        }

        logger.info("[DataFusionPlugin] executeRemoteQuery: table={}, files={}, sql={}", tableName, filePaths.length, sqlQuery);

        NativeRuntimeHandle runtimeHandle = dfService.getNativeRuntime();
        long runtimePtr = runtimeHandle.get();
        CompletableFuture<Long> future = new CompletableFuture<>();
        try {
            NativeBridge.executeIcebergQueryAsync(
                s3Region,
                s3Bucket,
                s3AccessKeyId,
                s3SecretAccessKey,
                s3SessionToken,
                s3Endpoint,
                filePaths,
                fileSizes,
                tableName,
                sqlQuery,
                runtimePtr,
                new ActionListener<>() {
                    @Override
                    public void onResponse(Long streamPtr) {
                        future.complete(streamPtr);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        future.completeExceptionally(e);
                    }
                }
            );
        } catch (UnsatisfiedLinkError e) {
            // Native Iceberg executor not compiled in this build — return stub message
            logger.warn("[DataFusionPlugin] executeIcebergQueryAsync not available in native library: {}", e.getMessage());
            throw new UnsupportedOperationException(
                "Iceberg native execution not available — native library missing executeIcebergQueryAsync. "
                    + "Table: " + tableName + ", files: " + filePaths.length + ", sql: " + sqlQuery, e);
        }

        long streamPtr;
        try {
            streamPtr = future.get(15, TimeUnit.MINUTES);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new RuntimeException("Iceberg query execution timed out after 15 minutes — table: "
                + tableName + ", files: " + filePaths.length, e);
        } catch (ExecutionException e) {
            logger.error("[DataFusionPlugin] JNI execution failed: {}", e.getCause().getMessage(), e.getCause());
            throw new RuntimeException("Iceberg query execution failed via DataFusion", e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Iceberg query execution interrupted", e);
        }

        long t1 = System.currentTimeMillis();
        logger.info("[PERF] JNI execute_iceberg_query: {}ms", t1 - t0);

        // Stream Arrow batches and convert to Object[] rows
        StreamHandle streamHandle = new StreamHandle(streamPtr, runtimeHandle);
        BufferAllocator allocator = dfService.newChildAllocator();
        DatafusionResultStream resultStream = new DatafusionResultStream(streamHandle, allocator);

        List<Object[]> rows = new ArrayList<>();
        try {
            Iterator<EngineResultBatch> batchIterator = resultStream.iterator();
            while (batchIterator.hasNext()) {
                EngineResultBatch batch = batchIterator.next();
                List<String> fieldNames = batch.getFieldNames();
                for (int row = 0; row < batch.getRowCount(); row++) {
                    Object[] rowValues = new Object[fieldNames.size()];
                    for (int col = 0; col < fieldNames.size(); col++) {
                        Object val = batch.getFieldValue(fieldNames.get(col), row);
                        if (val instanceof org.apache.arrow.vector.util.Text) {
                            val = val.toString();
                        }
                        rowValues[col] = val;
                    }
                    rows.add(rowValues);
                }
            }
        } finally {
            resultStream.close();
        }
        long t2 = System.currentTimeMillis();
        logger.info("[PERF] Arrow stream read: {}ms ({} rows)", t2 - t1, rows.size());
        logger.info("[PERF] executeRemoteQuery total: {}ms", t2 - t0);
        return rows;
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

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
import org.opensearch.be.datafusion.jni.NativeBridge;
import org.opensearch.be.datafusion.jni.StreamHandle;
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
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.shard.ShardPath;
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

    /** Memory pool limit for the DataFusion runtime. */
    public static final Setting<Long> DATAFUSION_MEMORY_POOL_LIMIT = Setting.longSetting(
        "datafusion.memory_pool_limit_bytes",
        Runtime.getRuntime().maxMemory() / 4,
        0L,
        Setting.Property.NodeScope
    );

    /** Spill memory limit — when exceeded, DataFusion spills to disk. */
    public static final Setting<Long> DATAFUSION_SPILL_MEMORY_LIMIT = Setting.longSetting(
        "datafusion.spill_memory_limit_bytes",
        Runtime.getRuntime().maxMemory() / 8,
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
                long maxMem = Runtime.getRuntime().maxMemory();
                long memPool = getConfiguredLong("datafusion_memory_pool_limit_bytes", maxMem / 4);
                long spillLimit = getConfiguredLong("datafusion_spill_memory_limit_bytes", maxMem / 8);
                String spillDir = System.getProperty("java.io.tmpdir");
                sharedDataFusionService = DataFusionService.builder()
                    .memoryPoolLimit(memPool)
                    .spillMemoryLimit(spillLimit)
                    .spillDirectory(spillDir)
                    .build();
                sharedDataFusionService.start();
                logger.info("DataFusion service lazy-initialized (SPI path) — memory pool {}B, spill limit {}B", memPool, spillLimit);
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
                sharedDataFusionService = DataFusionService.builder()
                    .memoryPoolLimit(memoryPoolLimit)
                    .spillMemoryLimit(spillMemoryLimit)
                    .spillDirectory(spillDir)
                    .build();
                sharedDataFusionService.start();
                logger.info("DataFusion plugin initialized — memory pool {}B, spill limit {}B", memoryPoolLimit, spillMemoryLimit);
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
    public EngineReaderManager<DatafusionReader> createReaderManager(DataFormat format, ShardPath shardPath) throws IOException {
        return new DatafusionReaderManager(format, shardPath, sharedDataFusionService);
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
        DataFusionService dfService = ensureDataFusionService();

        Map<String, String> config = scanContext.getStorageConfig();
        String s3Region = config.getOrDefault("s3Region", "us-east-1");
        String s3Bucket = config.get("s3Bucket");
        String s3AccessKeyId = config.get("s3AccessKeyId");
        String s3SecretAccessKey = config.get("s3SecretAccessKey");
        String s3SessionToken = config.get("s3SessionToken");
        String s3Endpoint = config.get("s3Endpoint");

        String[] filePaths = scanContext.getDataFilePaths().toArray(new String[0]);
        String tableName = scanContext.getTableName();
        String sqlQuery = scanContext.getSqlQuery();

        // Short-circuit: if no data files match the scan predicates, return empty result
        // rather than calling native code which panics on empty file lists.
        if (filePaths.length == 0) {
            logger.info("[DataFusionPlugin] No data files for table [{}] — returning empty result", tableName);
            return List.of();
        }

        logger.debug("[DataFusionPlugin] executeRemoteQuery: table={}, files={}, sqlQuery={}", tableName, filePaths.length, sqlQuery);

        // Call DataFusion via JNI — pass the global runtime pointer so the Iceberg executor
        // shares the memory pool and disk manager (enabling spill-to-disk for large aggregations).
        NativeRuntimeHandle runtimeHandle = dfService.getNativeRuntime();
        long runtimePtr = runtimeHandle.get();
        CompletableFuture<Long> future = new CompletableFuture<>();
        NativeBridge.executeIcebergQueryAsync(
            s3Region,
            s3Bucket,
            s3AccessKeyId,
            s3SecretAccessKey,
            s3SessionToken,
            s3Endpoint,
            filePaths,
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

        long streamPtr;
        try {
            streamPtr = future.join();
        } catch (Exception e) {
            logger.error("[DataFusionPlugin] JNI execution failed: {}", e.getMessage(), e);
            throw new RuntimeException("Iceberg query execution failed via DataFusion", e);
        }

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
        logger.info("[DataFusionPlugin] Iceberg query returned {} rows via native execution", rows.size());
        return rows;
    }

    @Override
    public void close() throws IOException {
        if (sharedDataFusionService != null) {
            sharedDataFusionService.close();
        }
    }
}

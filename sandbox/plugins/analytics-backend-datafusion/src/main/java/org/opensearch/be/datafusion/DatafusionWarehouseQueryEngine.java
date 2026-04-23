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
import org.opensearch.analytics.exec.DataWarehouseQueryEngine;
import org.opensearch.analytics.exec.DataWarehouseScanContext;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.StreamHandle;
import org.opensearch.core.action.ActionListener;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * DataFusion implementation of {@link DataWarehouseQueryEngine}.
 * <p>
 * Executes SQL queries against Parquet files (S3 or local) via the DataFusion
 * native runtime. Uses the shared {@link DataFusionService} singleton for
 * runtime management and Arrow memory allocation.
 *
 * @opensearch.internal
 */
public class DatafusionWarehouseQueryEngine implements DataWarehouseQueryEngine {

    private static final Logger logger = LogManager.getLogger(DatafusionWarehouseQueryEngine.class);

    /**
     * Creates a new DataFusion warehouse query engine.
     */
    public DatafusionWarehouseQueryEngine() {}

    @Override
    public Iterable<Object[]> executeQuery(DataWarehouseScanContext scanContext) {
        long t0 = System.currentTimeMillis();
        DataFusionService dfService = DataFusionPlugin.ensureSharedService();

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
            logger.info("[DatafusionQueryEngine] No data files for table [{}] — returning empty result", tableName);
            return List.of();
        }

        logger.info("[DatafusionQueryEngine] executeQuery: table={}, files={}, sql={}", tableName, filePaths.length, sqlQuery);

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
            logger.warn("[DatafusionQueryEngine] executeIcebergQueryAsync not available in native library: {}", e.getMessage());
            throw new UnsupportedOperationException(
                "Iceberg native execution not available — native library missing executeIcebergQueryAsync. "
                    + "Table: " + tableName + ", files: " + filePaths.length + ", sql: " + sqlQuery,
                e
            );
        }

        long streamPtr;
        try {
            streamPtr = future.get(15, TimeUnit.MINUTES);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new RuntimeException(
                "Iceberg query execution timed out after 15 minutes — table: " + tableName + ", files: " + filePaths.length, e
            );
        } catch (ExecutionException e) {
            logger.error("[DatafusionQueryEngine] JNI execution failed: {}", e.getCause().getMessage(), e.getCause());
            throw new RuntimeException("Iceberg query execution failed via DataFusion", e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Iceberg query execution interrupted", e);
        }

        long t1 = System.currentTimeMillis();
        logger.info("[PERF] JNI execute_iceberg_query: {}ms", t1 - t0);

        StreamHandle streamHandle = new StreamHandle(streamPtr, runtimeHandle);
        BufferAllocator allocator = dfService.newChildAllocator();
        DatafusionResultStream resultStream = new DatafusionResultStream(streamHandle, allocator);

        // Arrow C Data imports (SchemaImporter) need flatbuffers on the TCCL.
        // When called from lakehouse worker threads, the TCCL is the lakehouse plugin's
        // classloader which doesn't have flatbuffers. Swap to this plugin's classloader.
        Thread currentThread = Thread.currentThread();
        ClassLoader originalCl = currentThread.getContextClassLoader();
        currentThread.setContextClassLoader(DatafusionWarehouseQueryEngine.class.getClassLoader());
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
            currentThread.setContextClassLoader(originalCl);
            resultStream.close();
        }
        long t2 = System.currentTimeMillis();
        logger.info("[PERF] Arrow stream read: {}ms ({} rows)", t2 - t1, rows.size());
        logger.info("[PERF] executeQuery total: {}ms", t2 - t0);
        return rows;
    }
}

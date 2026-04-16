/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.analytics.exec.DataWarehouseQueryEngine;
import org.opensearch.analytics.exec.DataWarehouseScanContext;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.StreamHandle;
import org.opensearch.core.action.ActionListener;

import java.io.ByteArrayInputStream;
import java.io.IOException;
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
        logger.info("[PERF] executeQuery total: {}ms", t2 - t0);
        return rows;
    }

    @Override
    public byte[] executeQueryArrowIpc(DataWarehouseScanContext scanContext) {
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
            logger.info("[DatafusionQueryEngine] No data files for table [{}] — returning empty IPC result", tableName);
            return new byte[0];
        }

        logger.info("[DatafusionQueryEngine] executeQueryArrowIpc: table={}, files={}, sql={}", tableName, filePaths.length, sqlQuery);

        NativeRuntimeHandle runtimeHandle = dfService.getNativeRuntime();
        long runtimePtr = runtimeHandle.get();
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        try {
            NativeBridge.executeIcebergQueryToIpcAsync(
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
                    public void onResponse(byte[] ipcBytes) {
                        future.complete(ipcBytes);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        future.completeExceptionally(e);
                    }
                }
            );
        } catch (UnsatisfiedLinkError e) {
            logger.warn("[DatafusionQueryEngine] executeIcebergQueryToIpcAsync not available in native library: {}", e.getMessage());
            throw new UnsupportedOperationException(
                "Iceberg native IPC execution not available — native library missing executeIcebergQueryToIpcAsync. "
                    + "Table: " + tableName + ", files: " + filePaths.length + ", sql: " + sqlQuery,
                e
            );
        }

        byte[] ipcBytes;
        try {
            ipcBytes = future.get(15, TimeUnit.MINUTES);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new RuntimeException(
                "Iceberg IPC query execution timed out after 15 minutes — table: " + tableName + ", files: " + filePaths.length, e
            );
        } catch (ExecutionException e) {
            logger.error("[DatafusionQueryEngine] JNI IPC execution failed: {}", e.getCause().getMessage(), e.getCause());
            throw new RuntimeException("Iceberg IPC query execution failed via DataFusion", e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Iceberg IPC query execution interrupted", e);
        }

        long t1 = System.currentTimeMillis();
        logger.info("[PERF] JNI execute_iceberg_query_to_ipc: {}ms ({} bytes)", t1 - t0, ipcBytes.length);
        return ipcBytes;
    }

    @Override
    public Iterable<Object[]> readArrowIpc(byte[] arrowIpcData) {
        long t0 = System.currentTimeMillis();
        DataFusionService dfService = DataFusionPlugin.ensureSharedService();

        if (arrowIpcData == null || arrowIpcData.length == 0) {
            return List.of();
        }

        BufferAllocator allocator = dfService.newChildAllocator();
        List<Object[]> rows = new ArrayList<>();
        try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(arrowIpcData), allocator)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            while (reader.loadNextBatch()) {
                int rowCount = root.getRowCount();
                List<FieldVector> vectors = root.getFieldVectors();
                int colCount = vectors.size();
                for (int row = 0; row < rowCount; row++) {
                    Object[] rowValues = new Object[colCount];
                    for (int col = 0; col < colCount; col++) {
                        Object val = vectors.get(col).getObject(row);
                        if (val instanceof org.apache.arrow.vector.util.Text) {
                            val = val.toString();
                        }
                        rowValues[col] = val;
                    }
                    rows.add(rowValues);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read Arrow IPC data", e);
        } finally {
            allocator.close();
        }

        long t1 = System.currentTimeMillis();
        logger.info("[PERF] readArrowIpc: {}ms ({} rows from {} bytes)", t1 - t0, rows.size(), arrowIpcData.length);
        return rows;
    }

    @Override
    public List<String> readArrowIpcColumnNames(byte[] arrowIpcData) {
        DataFusionService dfService = DataFusionPlugin.ensureSharedService();
        BufferAllocator allocator = dfService.newChildAllocator();
        try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(arrowIpcData), allocator)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            return root.getSchema().getFields().stream()
                .map(f -> f.getName())
                .toList();
        } catch (IOException e) {
            throw new RuntimeException("Failed to read Arrow IPC schema", e);
        } finally {
            allocator.close();
        }
    }

    @Override
    public Iterable<Object[]> executeMerge(List<byte[]> workerArrowIpcData, String mergeSql) {
        long t0 = System.currentTimeMillis();
        DataFusionService dfService = DataFusionPlugin.ensureSharedService();

        logger.info("[DatafusionQueryEngine] executeMerge: workers={}, sql={}", workerArrowIpcData.size(), mergeSql);

        NativeRuntimeHandle runtimeHandle = dfService.getNativeRuntime();
        long runtimePtr = runtimeHandle.get();
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        try {
            NativeBridge.executeFromIpcAsync(
                workerArrowIpcData,
                mergeSql,
                runtimePtr,
                new ActionListener<>() {
                    @Override
                    public void onResponse(byte[] ipcBytes) {
                        future.complete(ipcBytes);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        future.completeExceptionally(e);
                    }
                }
            );
        } catch (UnsatisfiedLinkError e) {
            logger.warn("[DatafusionQueryEngine] executeFromIpcAsync not available in native library: {}", e.getMessage());
            throw new UnsupportedOperationException(
                "Merge execution not available — native library missing executeFromIpcAsync. sql: " + mergeSql, e
            );
        }

        byte[] mergedIpcBytes;
        try {
            mergedIpcBytes = future.get(15, TimeUnit.MINUTES);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new RuntimeException("Merge execution timed out after 15 minutes — sql: " + mergeSql, e);
        } catch (ExecutionException e) {
            logger.error("[DatafusionQueryEngine] Merge execution failed: {}", e.getCause().getMessage(), e.getCause());
            throw new RuntimeException("Merge execution failed via DataFusion", e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Merge execution interrupted", e);
        }

        long t1 = System.currentTimeMillis();
        logger.info("[PERF] JNI execute_from_ipc: {}ms ({} bytes)", t1 - t0, mergedIpcBytes.length);

        Iterable<Object[]> result = readArrowIpc(mergedIpcBytes);
        long t2 = System.currentTimeMillis();
        logger.info("[PERF] executeMerge total: {}ms", t2 - t0);
        return result;
    }

    @Override
    public long executeQueryToBatches(DataWarehouseScanContext scanContext) {
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
            logger.info("[DatafusionQueryEngine] No data files for table [{}] — returning 0 batch handle", tableName);
            return 0;
        }

        logger.info(
            "[DatafusionQueryEngine] executeQueryToBatches: table={}, files={}, sql={}",
            tableName, filePaths.length, sqlQuery
        );

        NativeRuntimeHandle runtimeHandle = dfService.getNativeRuntime();
        long runtimePtr = runtimeHandle.get();
        CompletableFuture<Long> future = new CompletableFuture<>();
        try {
            NativeBridge.executeQueryToBatchesAsync(
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
                    public void onResponse(Long batchHandle) {
                        future.complete(batchHandle);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        future.completeExceptionally(e);
                    }
                }
            );
        } catch (UnsatisfiedLinkError e) {
            logger.warn(
                "[DatafusionQueryEngine] executeQueryToBatchesAsync not available in native library: {}", e.getMessage()
            );
            throw new UnsupportedOperationException(
                "Batch handle execution not available — native library missing executeQueryToBatchesAsync. "
                    + "Table: " + tableName + ", files: " + filePaths.length + ", sql: " + sqlQuery,
                e
            );
        }

        long batchHandle;
        try {
            batchHandle = future.get(15, TimeUnit.MINUTES);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new RuntimeException(
                "Batch handle execution timed out after 15 minutes — table: " + tableName + ", files: " + filePaths.length, e
            );
        } catch (ExecutionException e) {
            logger.error("[DatafusionQueryEngine] Batch handle execution failed: {}", e.getCause().getMessage(), e.getCause());
            throw new RuntimeException("Batch handle execution failed via DataFusion", e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Batch handle execution interrupted", e);
        }

        long t1 = System.currentTimeMillis();
        logger.info("[PERF] JNI execute_query_to_batches: {}ms (handle=0x{})", t1 - t0, Long.toHexString(batchHandle));
        return batchHandle;
    }

    @Override
    public Iterable<Object[]> executeMergeStreaming(long localBatchHandle, List<byte[]> remoteArrowIpcData, String mergeSql) {
        long t0 = System.currentTimeMillis();
        DataFusionService dfService = DataFusionPlugin.ensureSharedService();

        logger.info(
            "[DatafusionQueryEngine] executeMergeStreaming: localHandle=0x{}, remoteWorkers={}, sql={}",
            Long.toHexString(localBatchHandle), remoteArrowIpcData.size(), mergeSql
        );

        NativeRuntimeHandle runtimeHandle = dfService.getNativeRuntime();
        long runtimePtr = runtimeHandle.get();
        CompletableFuture<Long> future = new CompletableFuture<>();
        try {
            NativeBridge.executeMergeStreamingAsync(
                localBatchHandle,
                remoteArrowIpcData,
                mergeSql,
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
            // If batch handle was allocated, free it on error
            if (localBatchHandle != 0) {
                try {
                    NativeBridge.batchHandleFree(localBatchHandle);
                } catch (Exception ignored) {}
            }
            throw new UnsupportedOperationException("Streaming merge not available", e);
        }

        long streamPtr;
        try {
            streamPtr = future.get(15, TimeUnit.MINUTES);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new RuntimeException("Streaming merge timed out after 15 minutes", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Streaming merge failed", e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Streaming merge interrupted", e);
        }

        long t1 = System.currentTimeMillis();
        logger.info("[PERF] executeMergeStreaming native: {}ms", t1 - t0);

        // Stream results back using the same pattern as executeQuery
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
        logger.info("[PERF] executeMergeStreaming total: {}ms ({} rows)", t2 - t0, rows.size());
        return rows;
    }

    @Override
    public void freeBatchHandle(long batchHandle) {
        if (batchHandle != 0) {
            NativeBridge.batchHandleFree(batchHandle);
        }
    }
}

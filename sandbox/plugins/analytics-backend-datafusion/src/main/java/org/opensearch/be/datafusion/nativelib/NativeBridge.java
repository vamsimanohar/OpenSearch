/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.nativelib;

import org.opensearch.analytics.backend.jni.NativeHandle;
import org.opensearch.core.action.ActionListener;
import org.opensearch.nativebridge.spi.NativeCall;
import org.opensearch.nativebridge.spi.NativeLibraryLoader;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * FFM bridge to native DataFusion library.
 *
 * <h2>Pointer lifecycle (no Arena needed)</h2>
 * <p>Native pointers returned by {@code createGlobalRuntime}, {@code createDatafusionReader},
 * and {@code executeQueryAsync} are opaque {@code long} values — Rust heap addresses cast to
 * {@code i64}. They are <b>not</b> {@code MemorySegment}s and do not require an Arena. They
 * live until explicitly freed by the corresponding close method.</p>
 *
 * <h2>Arena usage</h2>
 * <p>{@link NativeCall} creates a confined Arena for short-lived allocations (strings, byte
 * arrays) that are only needed for the duration of the FFM call. The Arena is closed
 * immediately after the call returns, freeing all temp memory.</p>
 *
 * <h2>Error convention</h2>
 * <p>Functions return {@code i64}: {@code >= 0} is success, {@code < 0} is a negated pointer
 * to a heap-allocated error string. {@link NativeCall#invoke} reads and frees the error,
 * then throws.</p>
 */
public final class NativeBridge {

    private static final MethodHandle INIT_RUNTIME_MANAGER;
    private static final MethodHandle SHUTDOWN_RUNTIME_MANAGER;
    private static final MethodHandle CREATE_GLOBAL_RUNTIME;
    private static final MethodHandle CLOSE_GLOBAL_RUNTIME;
    private static final MethodHandle CREATE_READER;
    private static final MethodHandle CLOSE_READER;
    private static final MethodHandle EXECUTE_QUERY;
    private static final MethodHandle STREAM_GET_SCHEMA;
    private static final MethodHandle STREAM_NEXT;
    private static final MethodHandle STREAM_CLOSE;
    private static final MethodHandle EXECUTE_ICEBERG_QUERY;
    private static final MethodHandle EXECUTE_FROM_IPC;
    private static final MethodHandle SQL_TO_SUBSTRAIT;

    static {
        SymbolLookup lib = NativeLibraryLoader.symbolLookup();
        Linker linker = Linker.nativeLinker();

        INIT_RUNTIME_MANAGER = linker.downcallHandle(
            lib.find("df_init_runtime_manager").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.JAVA_INT)
        );

        SHUTDOWN_RUNTIME_MANAGER = linker.downcallHandle(
            lib.find("df_shutdown_runtime_manager").orElseThrow(),
            FunctionDescriptor.ofVoid()
        );

        CREATE_GLOBAL_RUNTIME = linker.downcallHandle(
            lib.find("df_create_global_runtime").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG
            )
        );

        CLOSE_GLOBAL_RUNTIME = linker.downcallHandle(
            lib.find("df_close_global_runtime").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG)
        );

        CREATE_READER = linker.downcallHandle(
            lib.find("df_create_reader").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG
            )
        );

        CLOSE_READER = linker.downcallHandle(lib.find("df_close_reader").orElseThrow(), FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG));

        EXECUTE_QUERY = linker.downcallHandle(
            lib.find("df_execute_query").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG
            )
        );

        STREAM_GET_SCHEMA = linker.downcallHandle(
            lib.find("df_stream_get_schema").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );

        STREAM_NEXT = linker.downcallHandle(
            lib.find("df_stream_next").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );

        STREAM_CLOSE = linker.downcallHandle(lib.find("df_stream_close").orElseThrow(), FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG));

        // i64 df_execute_iceberg_query(s3_region, s3_region_len, s3_bucket, s3_bucket_len,
        //   s3_access_key, s3_access_key_len, s3_secret_key, s3_secret_key_len,
        //   s3_session_token, s3_session_token_len, s3_endpoint, s3_endpoint_len,
        //   file_paths_ptr, file_paths_lens, file_sizes, files_count,
        //   table_name, table_name_len, sql_query, sql_query_len, runtime_ptr) -> i64
        EXECUTE_ICEBERG_QUERY = linker.downcallHandle(
            lib.find("df_execute_iceberg_query").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,    // s3_region
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,    // s3_bucket
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,    // s3_access_key
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,    // s3_secret_key
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,    // s3_session_token
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,    // s3_endpoint
                ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,  // files
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,    // table_name
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,    // sql_query
                ValueLayout.JAVA_LONG                          // runtime_ptr
            )
        );

        // i64 df_execute_from_ipc(ipc_bytes, ipc_len, sql_ptr, sql_len, runtime_ptr) -> i64
        EXECUTE_FROM_IPC = linker.downcallHandle(
            lib.find("df_execute_from_ipc").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,    // ipc_bytes
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,    // sql
                ValueLayout.JAVA_LONG                          // runtime_ptr
            )
        );

        // i64 df_sql_to_substrait(shard_ptr, table_ptr, table_len, sql_ptr, sql_len, runtime_ptr, out_ptr, out_cap, out_len)
        SQL_TO_SUBSTRAIT = linker.downcallHandle(
            lib.find("df_sql_to_substrait").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS
            )
        );
    }

    private NativeBridge() {}

    // ---- Tokio runtime management (no Arena needed — no string/buffer args) ----

    public static void initTokioRuntimeManager(int cpuThreads) {
        NativeCall.invokeVoid(INIT_RUNTIME_MANAGER, cpuThreads);
    }

    public static void shutdownTokioRuntimeManager() {
        NativeCall.invokeVoid(SHUTDOWN_RUNTIME_MANAGER);
    }

    // ---- DataFusion runtime (confined Arena for spillDir string only) ----

    /**
     * Creates a global DataFusion runtime. Returns an opaque native pointer ({@code long}).
     * This pointer is <b>not</b> a MemorySegment — it's a Rust heap address that lives
     * until {@link #closeGlobalRuntime} is called.
     */
    public static long createGlobalRuntime(long memoryLimit, long cacheManagerPtr, String spillDir, long spillLimit) {
        try (var call = new NativeCall()) {
            var dir = call.str(spillDir);
            return call.invoke(CREATE_GLOBAL_RUNTIME, memoryLimit, dir.segment(), dir.len(), spillLimit);
        }
    }

    /** Frees the native runtime. Safe to call once. */
    public static void closeGlobalRuntime(long ptr) {
        NativeCall.invokeVoid(CLOSE_GLOBAL_RUNTIME, ptr);
    }

    // ---- Reader management (confined Arena for path + file strings) ----

    /**
     * Creates a native reader. Returns an opaque native pointer.
     * Freed by {@link #closeDatafusionReader}.
     */
    public static long createDatafusionReader(String path, String[] files) {
        try (var call = new NativeCall()) {
            var p = call.str(path);
            var f = call.strArray(files);
            return call.invoke(CREATE_READER, p.segment(), p.len(), f.ptrs(), f.lens(), f.count());
        }
    }

    public static void closeDatafusionReader(long ptr) {
        NativeCall.invokeVoid(CLOSE_READER, ptr);
    }

    // ---- Query execution (confined Arena for tableName + plan bytes) ----

    public static void executeQueryAsync(
        long readerPtr,
        String tableName,
        byte[] substraitPlan,
        long runtimePtr,
        long contextId,
        ActionListener<Long> listener
    ) {
        try {
            NativeHandle.validatePointer(readerPtr, "reader");
            NativeHandle.validatePointer(runtimePtr, "runtime");
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        try (var call = new NativeCall()) {
            var table = call.str(tableName);
            long result = call.invoke(
                EXECUTE_QUERY,
                readerPtr,
                table.segment(),
                table.len(),
                call.bytes(substraitPlan),
                (long) substraitPlan.length,
                runtimePtr,
                contextId
            );
            listener.onResponse(result);
        } catch (Throwable t) {
            listener.onFailure(t instanceof Exception ? (Exception) t : new RuntimeException(t));
        }
    }

    // ---- Stream operations (no Arena needed — only long args) ----

    public static void streamGetSchema(long streamPtr, ActionListener<Long> listener) {
        try {
            NativeHandle.validatePointer(streamPtr, "stream");
            long result = NativeLibraryLoader.checkResult((long) STREAM_GET_SCHEMA.invokeExact(streamPtr));
            listener.onResponse(result);
        } catch (Throwable t) {
            listener.onFailure(t instanceof Exception ? (Exception) t : new RuntimeException(t));
        }
    }

    public static void streamNext(long runtimePtr, long streamPtr, ActionListener<Long> listener) {
        try {
            NativeHandle.validatePointer(streamPtr, "stream");
            long result = NativeLibraryLoader.checkResult((long) STREAM_NEXT.invokeExact(streamPtr));
            listener.onResponse(result);
        } catch (Throwable t) {
            listener.onFailure(t instanceof Exception ? (Exception) t : new RuntimeException(t));
        }
    }

    public static void streamClose(long streamPtr) {
        NativeCall.invokeVoid(STREAM_CLOSE, streamPtr);
    }

    // ---- Stubs ----

    public static byte[] sqlToSubstrait(long readerPtr, String tableName, String sql, long runtimePtr) {
        NativeHandle.validatePointer(readerPtr, "reader");
        NativeHandle.validatePointer(runtimePtr, "runtime");
        try (var call = new NativeCall()) {
            var table = call.str(tableName);
            var query = call.str(sql);
            var out = call.outBuffer(1024 * 1024);
            call.invoke(
                SQL_TO_SUBSTRAIT,
                readerPtr,
                table.segment(),
                table.len(),
                query.segment(),
                query.len(),
                runtimePtr,
                out.data(),
                (long) out.capacity(),
                out.lenOut()
            );
            return out.toByteArray();
        }
    }

    // ---- Iceberg / S3 query execution ----

    /**
     * Executes a SQL query against S3-backed (or local file://) Parquet files via DataFusion.
     * Marshals all arguments to native memory and calls {@code df_execute_iceberg_query}.
     * Returns a stream pointer via the listener on success.
     */
    public static void executeIcebergQueryAsync(
        String s3Region,
        String s3Bucket,
        String s3AccessKeyId,
        String s3SecretAccessKey,
        String s3SessionToken,
        String s3Endpoint,
        String[] filePaths,
        long[] fileSizes,
        String tableName,
        String sqlQuery,
        long runtimePtr,
        ActionListener<Long> listener
    ) {
        try {
            NativeHandle.validatePointer(runtimePtr, "runtime");
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        try (var call = new NativeCall()) {
            var region = call.str(s3Region != null ? s3Region : "");
            var bucket = call.str(s3Bucket != null ? s3Bucket : "");
            var accessKey = call.str(s3AccessKeyId != null ? s3AccessKeyId : "");
            var secretKey = call.str(s3SecretAccessKey != null ? s3SecretAccessKey : "");
            var sessionToken = call.str(s3SessionToken != null ? s3SessionToken : "");
            var endpoint = call.str(s3Endpoint != null ? s3Endpoint : "");
            var paths = call.strArray(filePaths);
            var sizes = call.buf(fileSizes.length * Long.BYTES);
            for (int i = 0; i < fileSizes.length; i++) {
                sizes.setAtIndex(ValueLayout.JAVA_LONG, i, fileSizes[i]);
            }
            var table = call.str(tableName);
            var sql = call.str(sqlQuery);

            long result = call.invoke(
                EXECUTE_ICEBERG_QUERY,
                region.segment(), region.len(),
                bucket.segment(), bucket.len(),
                accessKey.segment(), accessKey.len(),
                secretKey.segment(), secretKey.len(),
                sessionToken.segment(), sessionToken.len(),
                endpoint.segment(), endpoint.len(),
                paths.ptrs(), paths.lens(), sizes, paths.count(),
                table.segment(), table.len(),
                sql.segment(), sql.len(),
                runtimePtr
            );
            listener.onResponse(result);
        } catch (Throwable t) {
            listener.onFailure(t instanceof Exception ? (Exception) t : new RuntimeException(t));
        }
    }

    // ---- Execute SQL over accumulated Arrow IPC bytes (exchange-sink path) ----

    /**
     * Executes a SQL query over a serialized Arrow IPC stream via DataFusion.
     * The IPC bytes contain a full stream-format encoding (schema + record batches + EOS)
     * of the input. Rust reads the IPC, registers it as an in-memory table that {@code sql}
     * references, runs the SQL, and returns a stream pointer via the listener on success.
     *
     * @param ipc         full Arrow IPC stream-format bytes; must not be {@code null}
     * @param sql         coordinator SQL to evaluate over the IPC input
     * @param runtimePtr  native DataFusion runtime pointer
     * @param listener    receives the resulting stream pointer or any failure
     */
    public static void executeFromIpcAsync(byte[] ipc, String sql, long runtimePtr, ActionListener<Long> listener) {
        try {
            NativeHandle.validatePointer(runtimePtr, "runtime");
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        try (var call = new NativeCall()) {
            var ipcBuf = call.bytes(ipc);
            var sqlStr = call.str(sql != null ? sql : "");
            long result = call.invoke(
                EXECUTE_FROM_IPC,
                ipcBuf, (long) ipc.length,
                sqlStr.segment(), sqlStr.len(),
                runtimePtr
            );
            listener.onResponse(result);
        } catch (Throwable t) {
            listener.onFailure(t instanceof Exception ? (Exception) t : new RuntimeException(t));
        }
    }

    public static void cacheManagerAddFiles(long runtimePtr, String[] filePaths) {}

    public static void cacheManagerRemoveFiles(long runtimePtr, String[] filePaths) {}

    public static void initLogger() {}
}

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.exchange;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.be.datafusion.DataFusionService;
import org.opensearch.be.datafusion.NativeRuntimeHandle;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.StreamHandle;
import org.opensearch.core.action.ActionListener;

import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link ExchangeSink} that accumulates Arrow record batches from child stages
 * and, on {@link #close()}, executes a coordinator SQL fragment over them via
 * the native DataFusion runtime.
 *
 * <h2>Framework gap — P1</h2>
 * <p>The analytics-engine stage-exchange SPI currently does not hand a downstream
 * sink to the {@link org.opensearch.analytics.spi.ExchangeSinkProvider} — the
 * {@code createSink(byte[])} contract only receives fragment bytes. As a result
 * the {@code downstream} argument to this sink is typically {@code null} when
 * constructed via the provider; in that case this sink drains the DataFusion
 * result stream to release native resources but does not forward the records.
 * When a richer SPI carries the downstream through, pass it explicitly to the
 * {@link #DataFusionExchangeSink(String, ExchangeSink, DataFusionService) three-arg constructor}
 * and it will pipe the results through.
 *
 * <h2>Thread-safety</h2>
 * <p>{@link #feed(VectorSchemaRoot)} is called from transport threads and is
 * safe for concurrent invocation. {@link #close()} must be called exactly once
 * — subsequent calls are no-ops.
 *
 * @opensearch.internal
 */
public final class DataFusionExchangeSink implements ExchangeSink {

    private static final Logger logger = LogManager.getLogger(DataFusionExchangeSink.class);

    /** Timeout for waiting on the native query — defensive, should be well within any real workload. */
    static final long NATIVE_TIMEOUT_MINUTES = 15L;

    private final String coordinatorSql;
    private final ExchangeSink downstream;
    private final DataFusionService dfService;
    private final NativeExecutor nativeExecutor;
    private final ResultDrainer resultDrainer;

    /** Accumulated input batches. Guarded by {@code this}. */
    private final List<VectorSchemaRoot> batches = new ArrayList<>();

    /** Ensures {@link #close()} runs exactly once. */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Constructs a sink that uses {@link NativeBridge#executeFromIpcAsync} to run
     * the coordinator SQL over accumulated batches.
     *
     * @param coordinatorSql SQL fragment evaluated against the accumulated batches;
     *                       must not be {@code null}
     * @param downstream     optional downstream sink to forward results to; may be
     *                       {@code null} in P1 when the framework does not plumb it through
     * @param dfService      provides the native runtime pointer and child allocator
     */
    public DataFusionExchangeSink(String coordinatorSql, ExchangeSink downstream, DataFusionService dfService) {
        this(coordinatorSql, downstream, dfService, NativeBridge::executeFromIpcAsync, DataFusionExchangeSink::defaultDrain);
    }

    /**
     * Package-private constructor exposing test seams for the native call and drain loop.
     * Production code should use {@link #DataFusionExchangeSink(String, ExchangeSink, DataFusionService)}.
     */
    DataFusionExchangeSink(
        String coordinatorSql,
        ExchangeSink downstream,
        DataFusionService dfService,
        NativeExecutor nativeExecutor,
        ResultDrainer resultDrainer
    ) {
        if (coordinatorSql == null) {
            throw new IllegalArgumentException("coordinatorSql must not be null");
        }
        if (dfService == null) {
            throw new IllegalArgumentException("dfService must not be null");
        }
        if (nativeExecutor == null) {
            throw new IllegalArgumentException("nativeExecutor must not be null");
        }
        if (resultDrainer == null) {
            throw new IllegalArgumentException("resultDrainer must not be null");
        }
        this.coordinatorSql = coordinatorSql;
        this.downstream = downstream;
        this.dfService = dfService;
        this.nativeExecutor = nativeExecutor;
        this.resultDrainer = resultDrainer;
    }

    /** Returns the coordinator SQL string this sink will evaluate. */
    public String coordinatorSql() {
        return coordinatorSql;
    }

    /** Returns the downstream sink (may be {@code null} under the P1 framework gap). */
    public ExchangeSink downstream() {
        return downstream;
    }

    @Override
    public void feed(VectorSchemaRoot batch) {
        if (batch == null) {
            throw new IllegalArgumentException("batch must not be null");
        }
        if (closed.get()) {
            // Late feed after close — release immediately to avoid leaking.
            try {
                batch.close();
            } catch (Exception ignore) {
                // swallow — sink is already closed
            }
            return;
        }
        synchronized (this) {
            if (closed.get()) {
                try {
                    batch.close();
                } catch (Exception ignore) {
                    // swallow — sink is already closed
                }
                return;
            }
            batches.add(batch);
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true) == false) {
            return; // idempotent
        }

        // Snapshot batches under lock, then work outside of it.
        List<VectorSchemaRoot> snapshot;
        synchronized (this) {
            snapshot = new ArrayList<>(batches);
            batches.clear();
        }

        try {
            if (snapshot.isEmpty()) {
                logger.info("[DataFusionExchangeSink] close with no accumulated batches — skipping native execution");
                return;
            }
            runCoordinatorQuery(snapshot);
        } catch (Exception e) {
            logger.warn("[DataFusionExchangeSink] coordinator execution failed: {}", e.getMessage(), e);
            // Do NOT rethrow — close() is terminal and must not prevent downstream.close().
        } finally {
            releaseAll(snapshot);
            closeDownstream();
        }
    }

    private void runCoordinatorQuery(List<VectorSchemaRoot> snapshot) throws Exception {
        byte[] ipc = serializeToIpc(snapshot);
        logger.info(
            "[DataFusionExchangeSink] executing coordinator SQL over {} accumulated batches ({} bytes IPC)",
            snapshot.size(),
            ipc.length
        );

        NativeRuntimeHandle runtimeHandle = dfService.getNativeRuntime();
        long runtimePtr = runtimeHandle.get();

        CompletableFuture<Long> future = new CompletableFuture<>();
        nativeExecutor.executeFromIpc(ipc, coordinatorSql, runtimePtr, new ActionListener<>() {
            @Override
            public void onResponse(Long streamPtr) {
                future.complete(streamPtr);
            }

            @Override
            public void onFailure(Exception e) {
                future.completeExceptionally(e);
            }
        });

        long streamPtr;
        try {
            streamPtr = future.get(NATIVE_TIMEOUT_MINUTES, TimeUnit.MINUTES);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new RuntimeException("Coordinator SQL execution timed out after " + NATIVE_TIMEOUT_MINUTES + " minutes", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            throw new RuntimeException("Coordinator SQL execution failed", cause != null ? cause : e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Coordinator SQL execution interrupted", e);
        }

        StreamHandle streamHandle = new StreamHandle(streamPtr, runtimeHandle);
        resultDrainer.drain(streamHandle, downstream, dfService);
    }

    /**
     * Default production drainer: iterates the native result stream via the Arrow C-Data
     * interface, importing each batch, and optionally forwarding it into the downstream sink.
     * <p>For each record batch imported from Rust:
     * <ul>
     *   <li>If {@code downstream != null}, a fresh {@link VectorSchemaRoot} is created, the
     *       batch is imported into it, and the VSR is handed to the downstream sink
     *       (which takes ownership).</li>
     *   <li>Otherwise the batch is imported into a scratch VSR for accounting and closed
     *       immediately — P1 drop-on-floor path.</li>
     * </ul>
     */
    static void defaultDrain(StreamHandle streamHandle, ExchangeSink downstream, DataFusionService dfService) {
        BufferAllocator allocator = dfService.newChildAllocator();
        CDataDictionaryProvider dictProvider = new CDataDictionaryProvider();
        VectorSchemaRoot scratchRoot = null;
        long totalRows = 0;
        int batchCount = 0;
        try {
            // Import schema first to determine VSR shape.
            long schemaAddr = awaitLong(l -> NativeBridge.streamGetSchema(streamHandle.getPointer(), l));
            try (var arrowSchema = org.apache.arrow.c.ArrowSchema.wrap(schemaAddr)) {
                var structField = Data.importField(allocator, arrowSchema, dictProvider);
                if (structField.getType().getTypeID() != org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID.Struct) {
                    throw new IllegalStateException("ArrowSchema describes non-struct type");
                }
                var resultSchema = new org.apache.arrow.vector.types.pojo.Schema(
                    structField.getChildren(),
                    structField.getMetadata()
                );
                scratchRoot = VectorSchemaRoot.create(resultSchema, allocator);
            }

            while (true) {
                long arrayAddr = awaitLong(
                    l -> NativeBridge.streamNext(streamHandle.getRuntimeHandle().get(), streamHandle.getPointer(), l)
                );
                if (arrayAddr == 0L) {
                    break;
                }
                try (ArrowArray arrowArray = ArrowArray.wrap(arrayAddr)) {
                    Data.importIntoVectorSchemaRoot(allocator, arrowArray, scratchRoot, dictProvider);
                }
                batchCount++;
                totalRows += scratchRoot.getRowCount();

                if (downstream != null) {
                    // Hand ownership of a fresh VSR to the downstream, then reset scratch for the next batch.
                    VectorSchemaRoot handed = transferToFreshRoot(scratchRoot, allocator);
                    try {
                        downstream.feed(handed);
                    } catch (RuntimeException re) {
                        try {
                            handed.close();
                        } catch (Exception ignore) {
                            // swallow
                        }
                        throw re;
                    }
                }
            }

            logger.info("[DataFusionExchangeSink] drained {} batches / {} rows from native result stream", batchCount, totalRows);
        } finally {
            try {
                if (scratchRoot != null) {
                    scratchRoot.close();
                }
            } finally {
                try {
                    streamHandle.close();
                } finally {
                    try {
                        dictProvider.close();
                    } finally {
                        allocator.close();
                    }
                }
            }
        }
    }

    /**
     * Creates a fresh {@link VectorSchemaRoot} sharing the schema of {@code source}
     * with a transferred copy of each field's current data. {@code source} is
     * reset by way of the transfer.
     */
    private static VectorSchemaRoot transferToFreshRoot(VectorSchemaRoot source, BufferAllocator allocator) {
        VectorSchemaRoot target = VectorSchemaRoot.create(source.getSchema(), allocator);
        for (int i = 0; i < source.getFieldVectors().size(); i++) {
            var src = source.getFieldVectors().get(i);
            var dst = target.getFieldVectors().get(i);
            var tp = src.makeTransferPair(dst);
            tp.transfer();
        }
        target.setRowCount(source.getRowCount());
        return target;
    }

    private static long awaitLong(java.util.function.Consumer<ActionListener<Long>> fn) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        fn.accept(new ActionListener<>() {
            @Override
            public void onResponse(Long v) {
                future.complete(v);
            }

            @Override
            public void onFailure(Exception e) {
                future.completeExceptionally(e);
            }
        });
        return future.join();
    }

    /**
     * Serializes accumulated batches as a full Arrow IPC stream (schema + batches + EOS).
     * <p>All batches are assumed to share the schema of {@code snapshot[0]}. A scratch
     * {@link VectorSchemaRoot} is bound to the writer; each source batch is transferred
     * into that scratch VSR and written in turn. Transfer mutates the source batches
     * (they are released after transfer), which is fine because the caller is about to
     * release them anyway.
     */
    private byte[] serializeToIpc(List<VectorSchemaRoot> snapshot) throws Exception {
        VectorSchemaRoot template = snapshot.get(0);
        BufferAllocator scratchAlloc = dfService.newChildAllocator();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (VectorSchemaRoot scratch = VectorSchemaRoot.create(template.getSchema(), scratchAlloc);
             ArrowStreamWriter writer = new ArrowStreamWriter(scratch, null, Channels.newChannel(baos))) {
            writer.start();
            for (VectorSchemaRoot src : snapshot) {
                copyInto(scratch, src);
                writer.writeBatch();
            }
            writer.end();
        } finally {
            scratchAlloc.close();
        }
        return baos.toByteArray();
    }

    /**
     * Transfers vectors from {@code src} into {@code dst}. {@code dst} must have been
     * created with the same schema as {@code src}. After this call, {@code src} is
     * emptied (its buffers have moved to {@code dst}).
     */
    private static void copyInto(VectorSchemaRoot dst, VectorSchemaRoot src) {
        // Clear all current dst vectors to release any previously transferred buffers.
        for (var v : dst.getFieldVectors()) {
            v.clear();
        }
        for (int i = 0; i < src.getFieldVectors().size(); i++) {
            var srcVec = src.getFieldVectors().get(i);
            var dstVec = dst.getFieldVectors().get(i);
            var tp = srcVec.makeTransferPair(dstVec);
            tp.transfer();
        }
        dst.setRowCount(src.getRowCount());
    }

    private static void releaseAll(List<VectorSchemaRoot> roots) {
        for (VectorSchemaRoot r : roots) {
            try {
                r.close();
            } catch (Exception ignore) {
                // swallow — best-effort cleanup
            }
        }
    }

    private void closeDownstream() {
        if (downstream != null) {
            try {
                downstream.close();
            } catch (Exception e) {
                logger.warn("[DataFusionExchangeSink] downstream close failed: {}", e.getMessage(), e);
            }
        }
    }

    /**
     * Seam for the native FFM call. Production uses {@link NativeBridge#executeFromIpcAsync};
     * tests inject a mock to avoid loading the native library.
     */
    @FunctionalInterface
    interface NativeExecutor {
        void executeFromIpc(byte[] ipc, String sql, long runtimePtr, ActionListener<Long> listener);
    }

    /**
     * Seam for draining the DataFusion result stream into the downstream sink. Production
     * uses {@link #defaultDrain(StreamHandle, ExchangeSink, DataFusionService)} which calls
     * into the native stream API; tests inject an alternative that exercises the post-native
     * wiring without requiring the JNI library to be loaded.
     */
    @FunctionalInterface
    interface ResultDrainer {
        void drain(StreamHandle streamHandle, ExchangeSink downstream, DataFusionService dfService);
    }
}

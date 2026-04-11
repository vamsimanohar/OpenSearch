# DataFusion Memory Architecture & JNI Bridge

> Design document covering memory management, the Java-Rust bridge, and improvement proposals for the DataFusion native execution backend.

## Overview

The analytics-backend-datafusion plugin embeds Apache DataFusion (a Rust query engine) inside the OpenSearch JVM process. Java and Rust share the same OS process but manage memory independently. Understanding these boundaries is critical to avoiding OOM kills and ensuring stable concurrent query execution.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        OS Process (16 GB)                       │
│                                                                 │
│  ┌──────────────────────┐    ┌────────────────────────────────┐ │
│  │     JVM (managed)    │    │     Rust (native/malloc)       │ │
│  │                      │    │                                │ │
│  │  ┌────────────────┐  │    │  ┌──────────────────────────┐  │ │
│  │  │   Heap (-Xmx)  │  │    │  │  DataFusion MemoryPool   │  │ │
│  │  │  Java objects,  │  │    │  │  Hash tables, sort bufs, │  │ │
│  │  │  Calcite plans, │  │    │  │  join state, agg state   │  │ │
│  │  │  OS caches      │  │    │  └──────────────────────────┘  │ │
│  │  └────────────────┘  │    │                                │ │
│  │                      │    │  ┌──────────────────────────┐  │ │
│  │  ┌────────────────┐  │    │  │  Tokio Runtime           │  │ │
│  │  │ Direct/Off-Heap│  │    │  │  Thread stacks, I/O bufs │  │ │
│  │  │ Arrow Buffers  │◄─┼────┼──│  S3 download buffers     │  │ │
│  │  │ (BufferAlloc)  │  │    │  └──────────────────────────┘  │ │
│  │  └────────────────┘  │    │                                │ │
│  └──────────────────────┘    └────────────────────────────────┘ │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  OS Kernel: page cache, TCP buffers, thread stacks       │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

## Memory Pools

### 1. JVM Heap (`-Xmx`)

Standard Java heap managed by the garbage collector. Used by:
- OpenSearch core (cluster state, caches, transport buffers)
- Calcite query plans (RelNode trees, RexNode expressions)
- Materialized query results (`Object[]` rows converted from Arrow)
- Iceberg SDK metadata (table snapshots, manifest lists)

**Controlled by:** `-Xms` / `-Xmx` JVM flags.

### 2. JVM Direct Memory (`-XX:MaxDirectMemorySize`)

Off-heap byte buffers allocated via `ByteBuffer.allocateDirect()`. Used by:
- Apache Arrow `BufferAllocator` — holds imported Arrow batches from Rust
- Netty I/O buffers (OpenSearch transport layer)

**Controlled by:** `-XX:MaxDirectMemorySize` JVM flag.

**Arrow allocator hierarchy:**
```
RootAllocator (limit = datafusion_memory_pool_limit_bytes)
  └── ChildAllocator "datafusion-stream-0" (per query, limit = root limit)
  └── ChildAllocator "datafusion-stream-1" (per query, limit = root limit)
  └── ...
```

The `RootAllocator` enforces a global cap — all children combined cannot exceed the root's limit. Each child can individually use up to the root's full limit, but the root rejects allocations when the combined total is exceeded.

**Current code:** `DataFusionService.java:69`
```java
this.rootAllocator = new RootAllocator(memoryPoolLimit);
```

### 3. DataFusion Native Memory Pool (Rust `malloc`)

Rust-side memory managed by DataFusion's `MemoryPool`. Completely invisible to the JVM — not tracked by `-Xmx` or `MaxDirectMemorySize`. Used by:
- Hash tables for GROUP BY aggregations
- Sort buffers for ORDER BY
- Join build-side hash maps
- Window function state

**Controlled by:** `datafusion_memory_pool_limit_bytes` (JVM system property or env var).

**Current code:** `DataFusionPlugin.java:67-72`
```java
public static final Setting<Long> DATAFUSION_MEMORY_POOL_LIMIT = Setting.longSetting(
    "datafusion.memory_pool_limit_bytes",
    Runtime.getRuntime().maxMemory() / 4,  // default: 25% of JVM max heap
    0L,
    Setting.Property.NodeScope
);
```

### 4. DataFusion Spill Threshold

When a single DataFusion operator's memory exceeds this threshold, it writes intermediate data to disk (spill directory) and continues execution. This prevents one large GROUP BY from consuming the entire pool.

**Controlled by:** `datafusion_spill_memory_limit_bytes` (JVM system property or env var).

**Default:** 12.5% of JVM max heap (`Runtime.getRuntime().maxMemory() / 8`).

**Spill directory:** `<data-path>/tmp/` (plugin path) or system temp dir (SPI path).

**Limitation:** Not all DataFusion operators support spilling. Notably, TopK sort (ORDER BY + LIMIT on high-cardinality GROUP BY) does not spill — it fails with `Resources exhausted` when the pool is full.

### 5. Tokio Runtime (Rust)

The async I/O runtime that executes DataFusion queries. Uses native threads and OS memory invisible to JVM.

- Thread stacks: ~8 MB per worker thread (typically CPU cores / 2 threads)
- S3 download buffers: transient, ~10-50 MB per concurrent file download
- Total overhead: ~100-300 MB depending on concurrency

**Not configurable** separately — fixed overhead of running Rust in-process.

## The Gap: No Unified Memory Limit

Each subsystem only sees its own usage. There is no single enforcer that limits `JVM heap + Arrow direct + DataFusion native + Tokio overhead`:

```
JVM Heap:                    4 GB  (enforced by -Xmx)
Arrow RootAllocator:         4 GB  (enforced by RootAllocator limit)
DataFusion Rust Pool:        4 GB  (enforced by MemoryPool)
Tokio + S3 buffers:        ~0.3 GB (not enforced)
                           ──────
Potential total:           12.3 GB  ← no single limit checks this

Plus OS kernel:            ~2 GB
                           ──────
Process total:            14.3 GB  ← if RAM = 16 GB, only 1.7 GB headroom
```

If all pools are at capacity simultaneously, the Linux OOM killer terminates the process with no warning or graceful error.

## Recommended Memory Allocation

### Formula

```
JVM Heap + Arrow Direct + DataFusion Pool ≤ 60-70% of physical RAM
```

Leave 30-40% for the OS (page cache for Parquet files, TCP buffers, kernel).

### Example Configurations

#### 16 GB Machine (e.g., c5.2xlarge)
```bash
-Xms4g -Xmx4g
-XX:MaxDirectMemorySize=2g
-Ddatafusion_memory_pool_limit_bytes=4294967296    # 4 GB
-Ddatafusion_spill_memory_limit_bytes=2147483648   # 2 GB
# Total managed: 10 GB. OS headroom: 6 GB.
```

#### 32 GB Machine (e.g., c5.4xlarge)
```bash
-Xms8g -Xmx8g
-XX:MaxDirectMemorySize=4g
-Ddatafusion_memory_pool_limit_bytes=8589934592    # 8 GB
-Ddatafusion_spill_memory_limit_bytes=4294967296   # 4 GB
# Total managed: 20 GB. OS headroom: 12 GB.
```

#### 64 GB Machine (e.g., c5.9xlarge)
```bash
-Xms16g -Xmx16g
-XX:MaxDirectMemorySize=8g
-Ddatafusion_memory_pool_limit_bytes=17179869184   # 16 GB
-Ddatafusion_spill_memory_limit_bytes=8589934592   # 8 GB
# Total managed: 40 GB. OS headroom: 24 GB.
```

## JNI Bridge: How Java Talks to Rust

### The Call Chain

```
Java transport thread
  │
  ▼
NativeBridge.executeIcebergQueryAsync()       ← JNI call (native method)
  │
  ▼
Rust: Java_org_opensearch_be_datafusion_jni_NativeBridge_executeIcebergQueryAsync()
  │  Converts Java strings/arrays to Rust types
  │  Creates S3 ObjectStore with credentials
  │  Registers Parquet files as DataFusion table
  │
  ▼
  Spawns async task on Tokio runtime            ← returns immediately to Java
  │
  ▼
Java thread blocks on: future.get(5, MINUTES)
  │
  │  Meanwhile, Tokio worker threads:
  │    - Connect to S3
  │    - Download Parquet file chunks
  │    - Execute query plan (filter, aggregate, sort)
  │    - Produce RecordBatch stream
  │
  ▼
Tokio calls back via JNI: ActionListener.onResponse(streamPtr)
  │
  ▼
Java thread wakes up, reads stream:
  StreamHandle(streamPtr)  →  JNI stream_next()  →  Arrow C Data Interface
  │                                                   │
  │  ┌────────────────────────────────────────────────┘
  │  │
  ▼  ▼
  Same physical memory (zero-copy):
    Rust allocated at 0x7f001000 → Java reads from 0x7f001000
    No serialization, no copy.
  │
  ▼
Convert Arrow columnar → Object[] rows (this is the copy)
  │
  ▼
Return Iterable<Object[]> to Calcite Enumerable
```

### Key Pointers Passed Across the Bridge

| Pointer | What It Points To | Lifetime |
|---------|-------------------|----------|
| `runtimePtr` | Rust `DataFusionRuntime` (Tokio + MemoryPool + DiskManager) | Node lifetime — created at startup, dropped at shutdown |
| `streamPtr` | Rust `SendableRecordBatchStream` (query results) | Query lifetime — created per query, freed when Java closes the stream |
| Arrow C Data pointers | `ArrowSchema` + `ArrowArray` structs | Batch lifetime — freed when Java's `ArrowBuf` is released |

### Arrow C Data Interface (Zero-Copy)

When Rust produces a RecordBatch, it exports two C structs:
- `ArrowSchema` — field names, types, nullability
- `ArrowArray` — buffer pointers, lengths, null bitmaps

Java's Arrow library imports these structs and wraps the buffer pointers as `DirectByteBuffer`. **No data is copied.** Java reads the same bytes Rust wrote.

**Ownership transfer:** The C Data Interface includes a `release` callback function pointer. When Java is done with a batch (closes the `ArrowBuf`), it calls this callback, which invokes Rust's `drop()` to free the native memory. If Java leaks the reference (doesn't close `DatafusionResultStream`), the Rust memory leaks too.

**Critical cleanup code** (`DataFusionPlugin.java:306`):
```java
} finally {
    resultStream.close();  // triggers Rust release callbacks for all imported batches
}
```

## Concurrency Model

### Single Rust Runtime, Multiple Queries

All concurrent queries share one `DataFusionRuntime`:

```
Query 1 → executeRemoteQuery() → JNI ──┐
Query 2 → executeRemoteQuery() → JNI ──┼── Same runtimePtr
Query 3 → executeRemoteQuery() → JNI ──┘     Same Tokio runtime
                                              Same MemoryPool (shared 4 GB)
                                              Same DiskManager (shared spill dir)
```

Each query gets its own DataFusion `SessionContext` within the shared runtime. Queries share the memory pool — the pool limit applies across ALL concurrent queries, not per-query.

### Thread Model

```
JVM Threads (managed by OpenSearch):
  transport-worker-1    ← handles REST request, calls JNI, blocks on future
  transport-worker-2    ← handles another REST request concurrently
  ...

Tokio Threads (managed by Rust, invisible to JVM):
  tokio-worker-1        ← async I/O: S3 reads, Parquet decoding
  tokio-worker-2        ← async I/O: query execution
  ...                   (typically CPU_CORES / 2 threads)
```

JVM and Tokio threads share the same process but are scheduled independently by the OS.

## Plugin Initialization: Two Paths

### Path 1: Plugin Lifecycle (normal)

```
OpenSearch node starts
  → Reads plugin-descriptor.properties
  → Instantiates DataFusionPlugin()
  → Calls createComponents(client, environment, settings, ...)
  → Gets opensearch.yml settings, proper data directory
  → Initializes DataFusionService with configured limits
```

This path runs when `analytics-backend-datafusion` is installed as a plugin. It has full access to OpenSearch settings and environment.

### Path 2: SPI Extension Loading

```
OpenSearch node starts
  → AnalyticsPlugin.loadExtensions() runs
  → SPI reads META-INF/services/AnalyticsSearchBackendPlugin
  → Creates NEW DataFusionPlugin() instance (separate from Path 1)
  → NEVER calls createComponents() on this instance
  → First executeRemoteQuery() call triggers ensureDataFusionService()
  → Lazy-initializes with JVM system properties / env vars / defaults
```

This path runs because analytics-engine discovers backend plugins via `ExtensiblePlugin.loadExtensions()`. SPI creates a fresh instance that has no access to `Environment` or `Settings`.

**Why both exist:** The same class serves dual roles — OpenSearch plugin (lifecycle) and SPI service provider (discovery). The `static volatile sharedDataFusionService` ensures whichever path initializes first wins, and the other reuses it.

## Known Failure Modes

### 1. Linux OOM Kill

**Cause:** JVM heap + DataFusion native + Arrow buffers > physical RAM.

**Symptom:** Process killed with no Java exception. `dmesg` shows OOM killer.

**Seen in:** ClickBench benchmarks — 8GB heap + 4GB direct + 4GB DataFusion = 16GB+ on 15GB machine.

**Fix:** Follow the memory allocation formula. Set `JVM + Arrow + DataFusion ≤ 60-70% of RAM`.

### 2. DataFusion Memory Pool Exhausted

**Cause:** High-cardinality GROUP BY + ORDER BY + LIMIT (TopK). DataFusion's TopK operator cannot spill to disk.

**Symptom:** `RuntimeException: Resources exhausted: Memory pool limit reached`

**Seen in:** ClickBench Q17, Q19, Q34.

**Fix:** Increase `datafusion_memory_pool_limit_bytes` or wait for DataFusion upstream to add TopK spilling.

### 3. Arrow Buffer Leak

**Cause:** `DatafusionResultStream` not closed (missing `finally` block or exception before close).

**Symptom:** `RootAllocator` reports unreleased bytes. Over time, direct memory grows until `OutOfDirectMemoryError`.

**Fix:** Always close result streams in a `finally` block. The current code does this correctly.

### 4. Spill Disk Full

**Cause:** DataFusion spills large intermediate results to the spill directory. Disk fills up.

**Symptom:** Rust I/O error when writing spill files.

**Fix:** Monitor spill directory disk usage. Use a large ephemeral volume for the data directory.

## Proposed Improvements

### 1. Per-Query Memory Limit

DataFusion supports per-task memory reservations via `MemoryPool::register()`. Add a configurable per-query cap (e.g., 1 GB) so one bad query cannot starve others.

```
Current:  10 queries share 4 GB pool, no per-query limit
Proposed: 10 queries share 4 GB pool, each limited to 1 GB
```

### 2. OpenSearch Circuit Breaker Integration

Register DataFusion + Arrow memory usage with OpenSearch's parent circuit breaker. When total memory (JVM + native) exceeds threshold, reject new queries with a clear error instead of risking OOM.

```java
// Proposed: report native memory to OpenSearch
circuitBreakerService.getBreaker("datafusion")
    .addEstimateBytesAndMaybeBreak(dfPool.currentUsage() + arrowAllocator.getAllocatedMemory());
```

### 3. Concurrent Query Semaphore

Simple semaphore limiting maximum concurrent DataFusion queries. Cheap insurance against memory explosion from burst traffic.

```java
private static final Semaphore QUERY_PERMITS = new Semaphore(MAX_CONCURRENT_QUERIES);
```

### 4. Streaming Results (Avoid Full Materialization)

Currently `executeRemoteQuery()` materializes ALL rows into `List<Object[]>` in JVM heap. For a 100M row result, this OOMs the heap.

Proposed: Return a lazy `Iterator<Object[]>` that fetches Arrow batches on demand. The Arrow batch is converted to rows, returned, and then released before fetching the next batch. Peak memory = one batch, not all rows.

```
Current:   [Batch 1] + [Batch 2] + ... + [Batch N] → all in heap simultaneously
Proposed:  [Batch 1] → yield rows → release → [Batch 2] → yield rows → release
```

### 5. Separate Arrow Allocator Limit

Currently Arrow's `RootAllocator` limit equals the DataFusion pool limit. Since Arrow buffers are transient (freed after Object[] conversion) while DataFusion state is long-lived, the Arrow allocator should have a smaller cap:

```java
// Current
this.rootAllocator = new RootAllocator(memoryPoolLimit);

// Proposed
this.rootAllocator = new RootAllocator(memoryPoolLimit / 2);
```

### 6. Memory Usage Metrics

Expose DataFusion memory pool usage, Arrow allocator usage, and spill file size as OpenSearch node stats. This enables monitoring and alerting before OOM conditions.

```
GET _nodes/stats/datafusion
{
  "datafusion": {
    "memory_pool_used_bytes": 2147483648,
    "memory_pool_limit_bytes": 4294967296,
    "arrow_allocated_bytes": 104857600,
    "spill_files_count": 3,
    "spill_files_bytes": 536870912,
    "active_queries": 2
  }
}
```

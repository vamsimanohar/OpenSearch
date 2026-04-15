# Phase 2.5: Coordinator-as-Worker + Streaming Merge

## Status: IMPLEMENTED

## Problem

The current distributed execution has three inefficiencies:

1. **Coordinator sits idle** — excluded from workers, does zero scan work while waiting
2. **IPC round-trip for merge result** — Rust serializes merge result to IPC bytes, Java copies to `byte[]`, then deserializes with `ArrowStreamReader`. Three wasted steps for data that never leaves the process.
3. **Local worker would use IPC** — if coordinator ran as a worker, it would serialize RecordBatches to IPC bytes just to pass them back to itself for merge. Pointless serialization within the same JVM.

## Solution

Three changes:

1. **Coordinator participates as a worker** — gets 1/N of the files to scan
2. **Local worker keeps batches in Rust memory** — returns an opaque handle, no IPC
3. **Merge result streams to Java** — reuses the existing `stream_next`/`stream_close` cursor, no IPC for the result

## Serde Count: Before vs After

| Operation | Before | After |
|-----------|--------|-------|
| Local worker → IPC serialize | N/A (coordinator excluded) | **Eliminated** (batch handle) |
| Remote worker → IPC serialize | Yes (unavoidable, network) | Yes (unchanged) |
| Remote worker → Java heap copy | Yes (unavoidable, network) | Yes (unchanged) |
| Merge: deserialize remote IPC | Yes | Yes (unchanged) |
| Merge: serialize result to IPC | Yes | **Eliminated** (stream pointer) |
| Merge: Java copies result byte[] | Yes | **Eliminated** (stream pointer) |
| Merge: Java ArrowStreamReader | Yes | **Eliminated** (FFI batch read) |

Net: **3 fewer serde/copy operations** on the coordinator. Remote workers unchanged.

---

## New Rust API

### 1. `execute_query_to_batches`

Executes a query against S3 Parquet files and keeps the result RecordBatches in memory. Returns an opaque handle (heap pointer to `Vec<RecordBatch>`). The caller must either pass this handle to `execute_merge_streaming` (which consumes it) or call `batch_handle_free` to release it.

```rust
pub async fn execute_query_to_batches(
    s3_region: &str,
    s3_bucket: Option<&str>,
    s3_access_key: Option<&str>,
    s3_secret_key: Option<&str>,
    s3_session_token: Option<&str>,
    s3_endpoint: Option<&str>,
    file_paths: Vec<String>,
    file_sizes: Vec<i64>,
    table_name: &str,
    sql_query: &str,
    runtime: &DataFusionRuntime,
    cpu_executor: DedicatedExecutor,
    io_handle: Handle,
) -> Result<i64, DataFusionError>
```

Implementation: identical to `execute_iceberg_query_to_ipc` up to the `batches` collection, but instead of calling `batches_to_ipc`, boxes the batches and returns the pointer:

```rust
let batches: Vec<RecordBatch> = cross_rt_stream.try_collect().await?;
let schema = stream.schema();
let handle = Box::new((schema, batches));
Ok(Box::into_raw(handle) as i64)
```

### 2. `execute_merge_streaming`

Takes a local batch handle (from `execute_query_to_batches`) plus remote IPC byte slices, registers all as a `StreamingTable`, executes the merge SQL, and returns a `MemoryTrackingStream` pointer compatible with the existing `stream_next`/`stream_close` API.

```rust
pub async fn execute_merge_streaming(
    local_batch_handle: i64,        // 0 if no local batches
    remote_ipc_slices: Vec<&[u8]>,  // IPC bytes from remote workers
    sql: &str,
    runtime: &DataFusionRuntime,
    cpu_executor: DedicatedExecutor,
) -> Result<i64, DataFusionError>
```

Implementation:

```rust
// 1. Collect all batches into one list
let mut all_batches: Vec<RecordBatch> = Vec::new();
let mut schema: Option<SchemaRef> = None;

// Local batches (zero-copy — just move the Vec)
if local_batch_handle != 0 {
    let (local_schema, local_batches) = *Box::from_raw(local_batch_handle as *mut (SchemaRef, Vec<RecordBatch>));
    schema = Some(local_schema);
    all_batches.extend(local_batches);  // move, not copy
}

// Remote IPC batches (deserialize)
for ipc_bytes in &remote_ipc_slices {
    let reader = StreamReader::try_new(Cursor::new(ipc_bytes), None)?;
    let ipc_schema = reader.schema();
    if schema.is_none() { schema = Some(ipc_schema.clone()); }
    for batch in reader {
        all_batches.push(batch?);
    }
}

// 2. Register as StreamingTable
let partition = Arc::new(MemoryPartitionStream { schema, batches: all_batches });
let streaming_table = StreamingTable::try_new(schema, vec![partition])?;
ctx.register_table("input", Arc::new(streaming_table))?;

// 3. Execute merge SQL and return stream pointer
let dataframe = ctx.sql(sql).await?;
let plan = dataframe.create_physical_plan().await?;
let stream = execute_stream(plan, ctx.task_ctx())?;
let cross_rt_stream = CrossRtStream::new_with_df_error_stream(stream, cpu_executor);

let tracking = MemoryTrackingStream { inner: cross_rt_stream, ... };
Ok(Box::into_raw(Box::new(tracking)) as i64)
```

The returned `stream_ptr` is consumed by the existing `stream_next()` and `stream_close()` functions — no new Java-side streaming code needed.

### 3. `batch_handle_free`

Frees a batch handle without merging. Used on error paths.

```rust
pub unsafe fn batch_handle_free(handle: i64) {
    if handle != 0 {
        let _ = Box::from_raw(handle as *mut (SchemaRef, Vec<RecordBatch>));
    }
    mimalloc_purge();
}
```

## New FFM Bridge Functions

```rust
// ffm.rs
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_execute_query_to_batches(
    // same S3/file params as df_execute_iceberg_query_to_ipc
    ...
    runtime_ptr: i64,
) -> i64   // batch_handle

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_execute_merge_streaming(
    local_batch_handle: i64,
    ipc_ptrs: *const *const u8,
    ipc_lens: *const i64,
    ipc_count: i64,
    sql_ptr: *const u8,
    sql_len: i64,
    runtime_ptr: i64,
) -> i64   // stream_ptr

#[no_mangle]
pub unsafe extern "C" fn df_batch_handle_free(handle: i64)
```

---

## Java Interface Changes

### DataWarehouseQueryEngine (new default methods)

```java
// Execute query, keep result in native memory. Returns opaque handle.
// Caller must pass to executeMergeStreaming() or call freeBatchHandle().
default long executeQueryToBatches(DataWarehouseScanContext scanContext) {
    throw new UnsupportedOperationException();
}

// Merge local batches + remote IPC data, return streaming result.
// Consumes the localBatchHandle (caller must NOT call freeBatchHandle after this).
// Pass localBatchHandle=0 if there are no local batches.
default Iterable<Object[]> executeMergeStreaming(
    long localBatchHandle,
    List<byte[]> remoteIpcData,
    String mergeSql
) {
    throw new UnsupportedOperationException();
}

// Free a batch handle without merging (error path cleanup).
default void freeBatchHandle(long handle) {
    // no-op by default
}
```

### DatafusionWarehouseQueryEngine (implementation)

```java
@Override
public long executeQueryToBatches(DataWarehouseScanContext ctx) {
    // Calls NativeBridge.executeQueryToBatchesAsync(...)
    // Returns the batch_handle (i64) from Rust
}

@Override
public Iterable<Object[]> executeMergeStreaming(
    long localBatchHandle,
    List<byte[]> remoteIpcData,
    String mergeSql
) {
    // 1. Call NativeBridge.executeMergeStreamingAsync(localBatchHandle, remoteIpcData, mergeSql, runtimePtr)
    //    → returns stream_ptr
    // 2. Call NativeBridge.streamGetSchema(stream_ptr) → schema
    // 3. Return Iterable that calls stream_next(stream_ptr) per batch,
    //    converts FFI_ArrowArray to Object[] rows,
    //    calls stream_close(stream_ptr) when exhausted
    // This reuses the existing streaming iteration code from executeQuery()
}

@Override
public void freeBatchHandle(long handle) {
    NativeBridge.batchHandleFree(handle);
}
```

---

## DistributedScanExecutor Changes

### Current flow (simplified)

```java
public void executeAsync(..., ActionListener<Iterable<Object[]>> listener) {
    List<DiscoveryNode> workers = nodeDiscovery.getEligibleNodes();

    // Exclude coordinator from workers
    String localNodeId = clusterService.state().nodes().getLocalNodeId();
    List<DiscoveryNode> remoteWorkers = workers.stream()
        .filter(n -> !n.getId().equals(localNodeId))
        .filter(n -> transportService.nodeConnected(n))
        .toList();

    if (remoteWorkers.isEmpty()) {
        executeSingleNodeAsync(...);  // streaming path, no IPC
        return;
    }

    // Partition files across remote workers only
    List<FileAssignment> assignments = FilePartitioner.partition(filePaths, fileSizes, remoteWorkers.size());

    // Dispatch and collect all responses, then merge
    dispatchAndCollect(remoteWorkers, assignments, workerSql, ..., responses -> {
        List<byte[]> workerIpcData = extractIpcData(responses);
        String mergeSql = MergeSqlGenerator.generate(analysis, columnNames);
        Iterable<Object[]> rows = queryEngine.executeMerge(workerIpcData, mergeSql);
        listener.onResponse(rows);
    });
}
```

### New flow

```java
public void executeAsync(..., ActionListener<Iterable<Object[]>> listener) {
    List<DiscoveryNode> workers = nodeDiscovery.getEligibleNodes();

    // Include coordinator in worker count for file partitioning
    String localNodeId = clusterService.state().nodes().getLocalNodeId();
    List<DiscoveryNode> remoteWorkers = workers.stream()
        .filter(n -> !n.getId().equals(localNodeId))
        .filter(n -> transportService.nodeConnected(n))
        .toList();

    if (remoteWorkers.isEmpty()) {
        executeSingleNodeAsync(...);
        return;
    }

    // Partition files across ALL workers (remotes + 1 local)
    int totalWorkers = remoteWorkers.size() + 1;  // +1 for coordinator
    List<FileAssignment> assignments = FilePartitioner.partition(filePaths, fileSizes, totalWorkers);

    // Last assignment is for the local coordinator worker
    FileAssignment localAssignment = assignments.get(assignments.size() - 1);
    List<FileAssignment> remoteAssignments = assignments.subList(0, assignments.size() - 1);

    // Dispatch local worker (returns batch handle, no IPC)
    CompletableFuture<Long> localFuture = dispatchLocalWorker(
        localAssignment, workerSql, storageConfig, tableName
    );

    // Dispatch remote workers (returns IPC bytes via transport)
    dispatchAndCollect(remoteWorkers, remoteAssignments, workerSql, storageConfig, tableName,
        ActionListener.wrap(
            responses -> {
                try {
                    long localBatchHandle = localFuture.get(15, TimeUnit.MINUTES);
                    List<byte[]> remoteIpcData = extractIpcData(responses);

                    List<String> columnNames;
                    if (!remoteIpcData.isEmpty()) {
                        columnNames = queryEngine.readArrowIpcColumnNames(remoteIpcData.get(0));
                    } else {
                        columnNames = queryEngine.readBatchColumnNames(localBatchHandle);
                    }

                    String mergeSql = generateMergeSql(analysis, columnNames);

                    // Single Rust call: local batches + remote IPC → streaming result
                    Iterable<Object[]> rows = queryEngine.executeMergeStreaming(
                        localBatchHandle, remoteIpcData, mergeSql
                    );
                    listener.onResponse(rows);
                } catch (Exception e) {
                    // Free local batches on error
                    try { queryEngine.freeBatchHandle(localFuture.get()); } catch (Exception ignored) {}
                    listener.onFailure(e);
                }
            },
            distributedFailure -> {
                // Free local batches, fall back to single-node
                try { queryEngine.freeBatchHandle(localFuture.get()); } catch (Exception ignored) {}
                executeSingleNodeAsync(sqlQuery, filePaths, fileSizes, storageConfig, tableName, listener);
            }
        )
    );
}

// New method: dispatch local worker on lakehouse_worker thread pool
private CompletableFuture<Long> dispatchLocalWorker(
    FileAssignment assignment, String workerSql,
    Map<String, String> storageConfig, String tableName
) {
    CompletableFuture<Long> future = new CompletableFuture<>();
    Map<String, String> resolvedConfig = WorkerCredentialResolver.resolve(storageConfig, clusterService);
    DataWarehouseScanContext ctx = new DataWarehouseScanContext(
        tableName, assignment.getFilePaths(), assignment.getFileSizes(), workerSql, resolvedConfig
    );
    transportService.getThreadPool().executor(LakehousePlugin.LAKEHOUSE_WORKER_THREAD_POOL).execute(() -> {
        try {
            long handle = AccessController.doPrivileged(
                (PrivilegedAction<Long>) () -> queryEngine.executeQueryToBatches(ctx)
            );
            future.complete(handle);
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
    });
    return future;
}
```

### Key design decisions

1. **Local assignment is the last partition** — `FilePartitioner.partition(files, sizes, N+1)` where the extra slot is for the coordinator. The last assignment goes to the local worker.
2. **Local worker runs in parallel** with remote dispatch — `dispatchLocalWorker` returns a `CompletableFuture<Long>` that completes when Rust finishes scanning.
3. **Single merge call** — `executeMergeStreaming(localHandle, remoteIpc, sql)` handles both local and remote data in one Rust invocation. No Java-side concatenation.
4. **Error path cleanup** — if remote dispatch fails or merge fails, `freeBatchHandle(localHandle)` releases Rust memory.
5. **Column name reading** — prefers reading from remote IPC (cheaper). Falls back to reading from local batch handle if no remote workers returned data. Needs a new `readBatchColumnNames(long handle)` function in Rust.

---

## Detailed Before/After: All Strategy Types

### 1. GLOBAL_MERGE — Q3: `SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits`

Worker SQL (AVG decomposed):
```sql
SELECT SUM("AdvEngineID"), COUNT(*),
       SUM("ResolutionWidth") AS "__avg_sum_0",
       COUNT("ResolutionWidth") AS "__avg_count_0"
FROM "hits"
```

Merge SQL:
```sql
SELECT SUM("SUM(AdvEngineID)"), SUM("COUNT(*)"),
       CAST(SUM("__avg_sum_0") AS DOUBLE) / SUM("__avg_count_0")
FROM input
```

Worker result size: **1 row** per worker (tiny).

**Before** (2 remote workers, 15 files each):
```
node-2: scan 15 files → 1 row → IPC ~200B → Java byte[200] → transport
node-3: scan 15 files → 1 row → IPC ~200B → Java byte[200] → transport
node-1 (coordinator, idle):
  receives 2 × byte[200]
  executeMerge: Rust deserializes 2 IPC → 2 rows → merge → 1 row
    → serialize result IPC → Java copies → readArrowIpc → 1 row
  Serde: 3 serialize + 4 deserialize + 2 copies
  Coordinator scan work: 0 files
```

**After** (2 remote + 1 local worker, 10 files each):
```
node-2: scan 10 files → 1 row → IPC ~200B → transport
node-3: scan 10 files → 1 row → IPC ~200B → transport
node-1 (coordinator + local worker):
  local: scan 10 files → 1 row → batch_handle (no IPC)
  merge: executeMergeStreaming(handle, [200B, 200B], sql)
    Rust: local 1 row (in memory) + deserialize 2 remote → 3 rows → merge → 1 row
    → stream_ptr → Java reads via stream_next → 1 row → stream_close
  Serde: 1 serialize (remote) + 1 deserialize (remote)
  Coordinator scan work: 10 files (1/3 of total)
```

### 2. TWO_PHASE_GROUP_BY — Q17: `SELECT UserID, SearchPhrase, COUNT(*) FROM hits GROUP BY UserID, SearchPhrase ORDER BY COUNT(*) DESC LIMIT 10`

Worker SQL (ORDER BY/LIMIT stripped):
```sql
SELECT "UserID", "SearchPhrase", COUNT(*)
FROM "hits"
GROUP BY "UserID", "SearchPhrase"
```

Merge SQL:
```sql
SELECT "UserID", "SearchPhrase", SUM("COUNT(*)") AS "COUNT(*)"
FROM input
GROUP BY "UserID", "SearchPhrase"
ORDER BY "COUNT(*)" DESC LIMIT 10
```

Worker result size: **~6-8M rows** per worker (large — all unique (UserID, SearchPhrase) pairs).

**Before** (2 remote workers, 15 files each):
```
node-2: scan 15 files → hash agg → 8M rows → IPC ~150MB → byte[150MB] → transport
node-3: same
node-1 (coordinator, idle):
  receives 2 × byte[150MB] = 300MB on Java heap
  executeMerge:
    Rust deserializes 300MB IPC → 16M rows (400MB pool)
    hash agg → sort → top 10
    serialize result IPC → Java copies → readArrowIpc → 10 rows
  Peak: 300MB heap + 400MB pool = 700MB
  Coordinator scan work: 0 files
```

**After** (2 remote + 1 local worker, 10 files each):
```
node-2: scan 10 files → 6M rows → IPC ~100MB → transport
node-3: same
node-1 (coordinator + local worker):
  local: scan 10 files → 6M rows → 130MB in pool → batch_handle
  merge: executeMergeStreaming(handle, [100MB, 100MB], sql)
    Rust: local 6M rows (in memory) + deserialize 200MB → 12M rows
    hash agg → sort → top 10
    → stream_ptr → Java reads 10 rows → stream_close
  Peak: 130MB local + 200MB remote borrow + 260MB deserialized = ~590MB
  Coordinator scan work: 10 files (1/3 of total)
  Memory saved: ~110MB peak vs before
```

### 3. DISTINCT_EXPAND — Q14: `SELECT SearchPhrase, COUNT(DISTINCT UserID) FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY COUNT(DISTINCT UserID) DESC LIMIT 10`

Worker SQL (expanded):
```sql
SELECT DISTINCT "SearchPhrase", "UserID"
FROM "hits"
WHERE "SearchPhrase" <> ''
```

Merge SQL:
```sql
SELECT "SearchPhrase", COUNT(DISTINCT "UserID")
FROM input
GROUP BY "SearchPhrase"
ORDER BY COUNT(DISTINCT "UserID") DESC LIMIT 10
```

Worker result size: **~3-5M rows** per worker (unique (SearchPhrase, UserID) pairs).

**Before** (2 remote workers, 15 files each):
```
node-2: scan 15 files → DISTINCT → 5M rows → IPC ~200MB → byte[200MB] → transport
node-3: same
node-1 (coordinator, idle):
  receives 2 × byte[200MB] = 400MB on Java heap
  executeMerge:
    Rust deserializes 400MB IPC → 10M rows (600MB pool)
    COUNT(DISTINCT) → sort → top 10
    serialize IPC → Java copies → readArrowIpc → 10 rows
  Peak: 400MB heap + 600MB pool = 1GB
```

**After** (2 remote + 1 local worker, 10 files each):
```
node-2: scan 10 files → 3.5M rows → IPC ~130MB → transport
node-3: same
node-1 (coordinator + local worker):
  local: scan 10 files → 3.5M rows → 200MB in pool → batch_handle
  merge: executeMergeStreaming(handle, [130MB, 130MB], sql)
    Rust: local 3.5M rows + deserialize 260MB → 10.5M rows
    COUNT(DISTINCT) → sort → top 10
    → stream_ptr → 10 rows → stream_close
  Peak: 200MB local + 260MB remote + 460MB deserialized = ~660MB
  Memory saved: ~340MB peak vs before
```

### 4. TOPK_MERGE — `SELECT URL FROM hits WHERE ... ORDER BY EventTime DESC LIMIT 10`

Worker SQL (same, with LIMIT):
```sql
SELECT "URL" FROM "hits" WHERE ... ORDER BY "EventTime" DESC LIMIT 10
```

Merge SQL:
```sql
SELECT * FROM input ORDER BY "EventTime" DESC LIMIT 10
```

Worker result size: **10 rows** per worker (tiny).

**Before**:
```
node-2: scan 15 files → sort → top 10 → IPC ~2KB → transport
node-3: same
node-1 (coordinator, idle):
  executeMerge: 20 rows → sort → top 10 → IPC → Java → 10 rows
```

**After**:
```
node-2: scan 10 files → top 10 → IPC ~2KB → transport
node-3: same
node-1 (local): scan 10 files → top 10 → batch_handle
  merge: 30 rows → sort → top 10 → stream_ptr → 10 rows
  Memory savings: negligible (data is tiny)
  Latency savings: coordinator scans 1/3 of files in parallel
```

### 5. MIXED_DISTINCT — Q10: `SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10`

Worker SQL (group by expanded with distinct col):
```sql
SELECT "RegionID", SUM("AdvEngineID"), COUNT(*) AS c,
       SUM("ResolutionWidth") AS "__avg_sum_0",
       COUNT("ResolutionWidth") AS "__avg_count_0",
       "UserID"
FROM "hits"
GROUP BY "RegionID", "UserID"
```

Merge SQL:
```sql
SELECT "RegionID",
       SUM("SUM(AdvEngineID)"),
       SUM("c") AS "c",
       CAST(SUM("__avg_sum_0") AS DOUBLE) / SUM("__avg_count_0"),
       COUNT(DISTINCT "UserID")
FROM input
GROUP BY "RegionID"
ORDER BY "c" DESC LIMIT 10
```

Worker result size: **~8-12M rows** per worker (unique (RegionID, UserID) pairs — large).

**Before** (2 remote workers, 15 files each):
```
node-2: scan 15 files → GROUP BY (RegionID, UserID) → 12M rows → IPC ~250MB → transport
node-3: same
node-1 (coordinator, idle):
  receives 2 × byte[250MB] = 500MB on Java heap
  executeMerge:
    Rust deserializes 500MB → 24M rows (800MB pool)
    re-aggregate + COUNT(DISTINCT) → sort → top 10
    serialize IPC → Java copies → readArrowIpc → 10 rows
  Peak: 500MB heap + 800MB pool = 1.3GB
```

**After** (2 remote + 1 local worker, 10 files each):
```
node-2: scan 10 files → 8M rows → IPC ~170MB → transport
node-3: same
node-1 (coordinator + local worker):
  local: scan 10 files → 8M rows → 270MB in pool → batch_handle
  merge: executeMergeStreaming(handle, [170MB, 170MB], sql)
    Rust: local 8M rows + deserialize 340MB → 24M rows
    re-aggregate + COUNT(DISTINCT) → sort → top 10
    → stream_ptr → 10 rows → stream_close
  Peak: 270MB local + 340MB remote + 540MB deserialized = ~880MB
  Memory saved: ~420MB peak vs before
```

### 6. CONCAT — `SELECT * FROM hits WHERE URL LIKE '%google%' LIMIT 100`

Worker SQL (pass-through):
```sql
SELECT * FROM "hits" WHERE "URL" LIKE '%google%'
```

Merge SQL:
```sql
SELECT * FROM input LIMIT 100
```

Worker result size: **variable** (depends on filter selectivity, could be large).

**Before**:
```
Workers scan and filter → all matching rows → IPC → transport → coordinator concatenates
```

**After**:
```
3 workers (10 files each), including coordinator
Local worker: matching rows in memory → batch_handle
Remote workers: IPC → transport
Merge: SELECT * FROM input LIMIT 100 → stream_ptr → 100 rows
  Coordinator does 1/3 of scan work
```

---

## Memory Impact Summary (3-node cluster, 30 files)

| Strategy | Before Peak (coordinator) | After Peak (coordinator) | Saved |
|----------|--------------------------|--------------------------|-------|
| GLOBAL_MERGE | ~1MB | ~1MB | negligible |
| TWO_PHASE_GROUP_BY | ~700MB | ~590MB | ~110MB |
| DISTINCT_EXPAND | ~1.0GB | ~660MB | ~340MB |
| TOPK_MERGE | ~1MB | ~1MB | negligible |
| MIXED_DISTINCT | ~1.3GB | ~880MB | ~420MB |
| CONCAT | varies | varies | varies |

The biggest wins are on the memory-heavy strategies (TWO_PHASE_GROUP_BY, DISTINCT_EXPAND, MIXED_DISTINCT) where worker results are large.

## Latency Impact

- Each worker scans 10 files instead of 15 (33% fewer) → faster per-worker execution
- Coordinator scans in parallel with remote dispatch → overlapped work
- No IPC serialize/deserialize for merge result → saves CPU time
- Net: estimated 15-25% faster for scan-heavy queries

---

## Files to Change

| Component | File | Change | Lines (est.) |
|-----------|------|--------|-------------|
| Rust: new functions | `api.rs` | `execute_query_to_batches`, `execute_merge_streaming`, `batch_handle_free` | ~150 |
| Rust: FFM exports | `ffm.rs` | 3 new extern functions | ~80 |
| Java: SPI interface | `DataWarehouseQueryEngine.java` | 3 new default methods | ~20 |
| Java: implementation | `DatafusionWarehouseQueryEngine.java` | Implement 3 methods via NativeBridge | ~80 |
| Java: FFM bridge | `NativeBridge.java` | 3 new method handles | ~60 |
| Java: executor | `DistributedScanExecutor.java` | Include coordinator as worker, new merge path | ~80 |
| Tests | `DistributedScanExecutorTests.java` | Update mocks, add coordinator-as-worker tests | ~100 |
| **Total** | | | **~570** |

## Risk Assessment

| Risk | Mitigation |
|------|-----------|
| Coordinator memory contention (worker + merge on same node) | Worker scans fewer files (10 vs 15). Merge starts after worker finishes. Pool limit prevents runaway allocation. |
| Local batch handle leak on error | `freeBatchHandle` called in every error/fallback path. Java `finally` block ensures cleanup. |
| Rust lifetime of local batches | `execute_merge_streaming` consumes the handle (takes ownership via `Box::from_raw`). No dangling pointers. |
| Regression for small queries | GLOBAL_MERGE and TOPK_MERGE have tiny worker results. Overhead of batch handle is negligible vs IPC. |

## Future Work (Phase 3+)

- **Arrow Flight streaming transport** — replace OpenSearch transport with Arrow Flight for remote workers. Eliminates IPC materialization on workers. Adds backpressure.
- **True zero-copy Arrow transport** — share Rust/Java Arrow buffer pointers via FFM. Eliminates all serde except the unavoidable wire transfer.
- **Adaptive worker exclusion** — if coordinator is memory-pressured during merge, dynamically exclude it from worker pool for the next query.

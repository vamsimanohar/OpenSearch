# Distributed Query Engine Plan

## Current State

The lakehouse plugin has a working 2-3 stage distributed query engine:

```
SQL → Calcite → PlanFragmenter → SubPlan(stages) → DistributedScanExecutor
  → fan-out WorkerQueryRequest (SQL + file partition) to workers
  → workers: DataFusion native (Rust) → Iterable<Object[]> → WorkerQueryResponse (Object[][])
  → coordinator: Object[][] → Arrow VectorSchemaRoot → Arrow IPC → DataFusion merge → Object[] rows
```

### What Works

| Capability | Implementation |
|---|---|
| Simple scan/filter/project | 2-stage GATHER + CONCAT |
| Global aggregation (SUM/COUNT/MIN/MAX) | 2-stage GATHER + re-aggregate via DataFusion |
| Global AVG | SUM+COUNT decomposition on workers, `SUM(s)/SUM(c)` on coordinator |
| ORDER BY + LIMIT | 2-stage GATHER + top-K merge via DataFusion |
| GROUP BY (no LIMIT) | 2-stage GATHER + re-aggregate + re-group |
| GROUP BY + LIMIT | 3-stage HASH plan: leaf → intermediate (on coordinator) → final |
| COUNT DISTINCT | Dedup on workers, COUNT(DISTINCT) on coordinator |
| HAVING | Stripped from workers, rewritten for re-aggregation on coordinator |
| Bounded top-K | Expanded LIMIT on workers to prevent OOM on high-cardinality GROUP BY |

### What Doesn't Work

- **3-stage HASH plan runs intermediate on coordinator** (P1 simplification) — not truly distributed
- **No worker-to-worker data transfer** — all data funnels through coordinator
- **Wire format is `writeGenericValue()` per cell** — slow, lossy (column names lost, types inferred from first non-null)
- **4 unnecessary data conversions** in the distributed path:
  ```
  DataFusion (Arrow) → Object[][] → writeGenericValue → readGenericValue → Arrow VectorSchemaRoot → Arrow IPC → DataFusion
  ```

---

## Bottlenecks (Priority Order)

1. **Wire format**: `writeGenericValue()` per cell. DataFusion produces Arrow batches that get converted to `Object[][]`, serialized cell-by-cell, deserialized, converted back to Arrow, then sent to DataFusion again.

2. **Coordinator bottleneck**: All data funnels through one node for merge. High-cardinality GROUP BY OOMs the coordinator.

3. **No shuffle**: Same group key can appear on multiple workers. Without hash shuffle, workers can't produce correct final aggregates.

---

## Plan: 4 Phases

### Phase 0: Execution Lifecycle (State Machine Framework)

**Goal**: Add a generic `StateMachine<T>` and state enums to drive multi-stage query execution with event-driven cascading.

#### Why First

Without a state machine, multi-stage execution requires hand-wiring completion/failure/cleanup logic with ad-hoc callbacks. The current `DistributedScanExecutor` uses `GroupedActionListener` + `CompletableFuture` — this works for 2-stage gather but doesn't scale to N-stage shuffle where:
- Stage 0 completion must trigger Stage 1 dispatch
- Any task failure must cascade up and abort sibling stages
- Cancellation must flow top-down
- Resource cleanup (shuffle files, connections) must be guaranteed on terminal states

#### Components

```
StateMachine<T>              Generic thread-safe state container with async listener notification
  │                          Terminal states are absorbing (no transitions out)
  │                          Listeners fire asynchronously on dedicated executor
  │                          Listeners cleared on terminal state (GC-friendly)
  │
  ├── QueryState enum        PLANNING → STARTING → RUNNING → FINISHING → FINISHED / FAILED
  ├── StageState enum        PLANNED → SCHEDULING → RUNNING → FINISHED / ABORTED / FAILED
  └── TaskState enum         RUNNING → FLUSHING → FINISHED / CANCELING → CANCELED / FAILED
```

#### Cascading Architecture

```
                    ┌─────────────────────────────────────────────────┐
                    │              QueryStateMachine                   │
                    │  PLANNING → STARTING → RUNNING → FINISHING      │
                    │  → FINISHED / → FAILED                          │
                    │                                                  │
                    │  On FAILED: abort all stages, cleanup            │
                    │  On FINISHING: wait for all stages done          │
                    └──────────────────┬──────────────────────────────┘
                                       │ listens to stage states
                    ┌──────────────────▼──────────────────────────────┐
                    │        StageStateMachine (per stage)             │
                    │  PLANNED → SCHEDULING → RUNNING → FINISHED      │
                    │  / → ABORTED / → FAILED                         │
                    │                                                  │
                    │  On FINISHED: if all children done, start parent │
                    │  On FAILED: cascade to query                     │
                    └──────────────────┬──────────────────────────────┘
                                       │ listens to task states
                    ┌──────────────────▼──────────────────────────────┐
                    │         TaskStateMachine (per task)              │
                    │  RUNNING → FLUSHING → FINISHED                  │
                    │  / → CANCELING → CANCELED                       │
                    │  / → FAILING → FAILED                           │
                    │                                                  │
                    │  On FINISHED: decrement stage pending count      │
                    │  On FAILED: cascade to stage                     │
                    └─────────────────────────────────────────────────┘

Bottom-up: task done → stage done → query done
Top-down:  query cancel → abort stages → cancel tasks
```

#### Adaptation for OpenSearch

| Reference | OpenSearch | Change |
|---|---|---|
| `com.google.common.util.concurrent.ListenableFuture` | `java.util.concurrent.CompletableFuture` | Replace Guava with JDK |
| `com.google.common.collect.ImmutableList/Set` | `java.util.List.of()` / `Set.of()` | Replace Guava with JDK |
| `com.google.common.util.concurrent.SettableFuture` | `java.util.concurrent.CompletableFuture` | Replace Guava with JDK |
| `io.airlift.log.Logger` | `org.apache.logging.log4j.Logger` | Standard OpenSearch logging |
| External exception type | `org.opensearch.OpenSearchException` | Standard OpenSearch exception |
| `FutureStateChange` (long-poll) | `CompletableFuture`-based equivalent | Simplified — no HTTP long-poll needed |
| Async listener executor | `OpenSearchExecutors.newSinglePrioritizing` or `ThreadPool` | Use OpenSearch thread pool |

#### Files to Create

```
sandbox/plugins/lakehouse-iceberg/src/main/java/org/opensearch/lakehouse/execution/
├── StateMachine.java              # Generic state machine
├── StateChangeListener.java       # Listener interface
├── QueryState.java                # Query lifecycle states
├── StageState.java                # Stage lifecycle states
├── TaskState.java                 # Task lifecycle states
├── QueryStateMachine.java         # Query-level state machine with cascade logic
├── StageStateMachine.java         # Stage-level state machine
└── TaskStateMachine.java          # Task-level state machine
```

**Estimated effort**: 1 week.

---

### Phase 1: Arrow IPC on the Wire

**Goal**: Workers send Arrow IPC bytes directly. Eliminate all intermediate conversions.

```
BEFORE (current):
  Worker: DataFusion → Object[][] → writeGenericValue() per cell → transport
  Coordinator: readGenericValue() → WorkerResponseToArrow → Arrow IPC → DataFusion

AFTER:
  Worker: DataFusion → Arrow IPC bytes → writeByteArray() → transport
  Coordinator: readByteArray() → concatenate → DataFusion (executeFromIpcAsync)
```

#### Changes

| Component | Current | New |
|---|---|---|
| `WorkerQueryExecutor` | DataFusion → `Iterable<Object[]>` → sanitize → column-major `Object[][]` | DataFusion → Arrow batches → Arrow IPC `byte[]` directly |
| `WorkerQueryResponse` | `Object[][]` via `writeGenericValue()` per cell | `byte[] arrowIpcData` via `writeByteArray()` |
| `DistributedScanExecutor.mergeViaDataFusion()` | `WorkerResponseToArrow.convert()` → `serializeResponsesAsIpc()` → `executeFromIpcAsync()` | Concatenate raw IPC bytes → `executeFromIpcAsync()` |

#### What This Eliminates

- `WorkerResponseToArrow` class
- `ResultSerializer` class
- `sanitizeRow()` (LocalDateTime → String conversion)
- Fragile type-inference-from-class-name logic
- Column-name-loss problem (`col_0`, `col_1` synthetic names)

#### Why It Works

The native bridge already supports this: `NativeBridge.executeFromIpcAsync(ipc, sql, runtimePtr)` takes Arrow IPC bytes and runs SQL over them. Workers just need to serialize their DataFusion output as IPC instead of converting to `Object[][]`.

**Estimated effort**: 1-2 weeks.

---

### Phase 2: Materialize-to-Disk Shuffle (Ballista-style)

**Goal**: Worker-to-worker data transfer via hash-partitioned Arrow IPC files served over Arrow Flight.

Simplest shuffle that unblocks true distributed GROUP BY. No streaming, no in-memory buffers, no complex lifecycle management.

#### New Components

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          COORDINATOR                                     │
│                                                                          │
│  PlanFragmenter.fragment(relNode, sql) → SubPlan                        │
│       │                                                                  │
│       ▼                                                                  │
│  StageOrchestrator                                                       │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │ 1. Dispatch Stage 0 to all workers with file partitions          │   │
│  │ 2. Collect ShuffleMetadata from all Stage 0 workers              │   │
│  │ 3. Dispatch Stage 1 to workers with partition assignments        │   │
│  │ 4. Gather Stage 1 results (simple CONCAT)                       │   │
│  │ 5. Cleanup: delete temp shuffle files                            │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                     STAGE 0 WORKERS (all nodes)                          │
│                                                                          │
│  DataFusion scan + partial aggregate                                     │
│       │                                                                  │
│       ▼                                                                  │
│  ShuffleWriter                                                           │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │ 1. Hash-partition Arrow batches by specified columns              │   │
│  │    hash(group_keys) % num_partitions → partition assignment       │   │
│  │ 2. Write one Arrow IPC file per output partition to local disk    │   │
│  │    /tmp/shuffle/{queryId}/stage0/partition-{N}.arrow              │   │
│  │ 3. Report ShuffleMetadata back to coordinator:                    │   │
│  │    { partitionId → (filePath, rowCount, byteSize) }              │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  Flight doGet handler (extend existing ArrowFlightProducer)              │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │ Ticket: {queryId, stageId, partitionId, filePath}                │   │
│  │ Opens local Arrow IPC file, streams batches back via Flight      │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                     STAGE 1 WORKERS (one task per partition)              │
│                                                                          │
│  ShuffleReader                                                           │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │ For partition P, pull from ALL Stage 0 workers:                   │   │
│  │   Local: read Arrow IPC file directly from disk                   │   │
│  │   Remote: Flight doGet → stream Arrow batches                     │   │
│  │ Feed all batches into DataFusion (executeFromIpc or StreamingTable)│  │
│  └──────────────────────────────────────────────────────────────────┘   │
│       │                                                                  │
│       ▼                                                                  │
│  DataFusion: final GROUP BY + ORDER BY + LIMIT                           │
│       │                                                                  │
│       ▼                                                                  │
│  Return results to coordinator (Arrow IPC)                               │
└─────────────────────────────────────────────────────────────────────────┘
```

#### Execution Flow Example

Query: `SELECT searchphrase, COUNT(*) AS c FROM hits GROUP BY searchphrase ORDER BY c DESC LIMIT 10`

```
Step 1: Coordinator dispatches Stage 0
        ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
        │  Worker 1     │  │  Worker 2     │  │  Worker 3     │
        │  files 1-10   │  │  files 11-20  │  │  files 21-30  │
        │               │  │               │  │               │
        │  Scan + partial│  │  Scan + partial│  │  Scan + partial│
        │  GROUP BY      │  │  GROUP BY      │  │  GROUP BY      │
        │               │  │               │  │               │
        │  hello: 50    │  │  hello: 30    │  │  hello: 25    │
        │  world: 20    │  │  bar: 15      │  │  world: 10    │
        │  foo: 5       │  │  foo: 8       │  │  baz: 3       │
        └──────┬────────┘  └──────┬────────┘  └──────┬────────┘
               │                  │                   │
               ▼                  ▼                   ▼

Step 2: ShuffleWriter hash-partitions output (3 partitions)
        hash("hello") % 3 = 0    hash("world") % 3 = 2    hash("foo") % 3 = 1

        Worker 1 writes:          Worker 2 writes:          Worker 3 writes:
        p0.arrow: hello:50        p0.arrow: hello:30        p0.arrow: hello:25, baz:3
        p1.arrow: foo:5           p1.arrow: bar:15, foo:8   p1.arrow: (empty)
        p2.arrow: world:20        p2.arrow: (empty)         p2.arrow: world:10

Step 3: Coordinator collects ShuffleMetadata
        Partition 0 → [W1:p0.arrow, W2:p0.arrow, W3:p0.arrow]
        Partition 1 → [W1:p1.arrow, W2:p1.arrow]
        Partition 2 → [W1:p2.arrow, W3:p2.arrow]

Step 4: Coordinator dispatches Stage 1 (one task per partition)
        ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
        │ Task for Part 0   │  │ Task for Part 1   │  │ Task for Part 2   │
        │ (runs on Worker 1)│  │ (runs on Worker 2)│  │ (runs on Worker 3)│
        │                   │  │                   │  │                   │
        │ Pull p0 from:     │  │ Pull p1 from:     │  │ Pull p2 from:     │
        │  W1 (local read)  │  │  W1 (Flight)      │  │  W1 (Flight)      │
        │  W2 (Flight)      │  │  W2 (local read)  │  │  W3 (local read)  │
        │  W3 (Flight)      │  │                   │  │                   │
        │                   │  │                   │  │                   │
        │ DataFusion:       │  │ DataFusion:       │  │ DataFusion:       │
        │ GROUP BY + LIMIT  │  │ GROUP BY + LIMIT  │  │ GROUP BY + LIMIT  │
        │                   │  │                   │  │                   │
        │ hello:105, baz:3  │  │ foo:13, bar:15    │  │ world:30          │
        └────────┬──────────┘  └────────┬──────────┘  └────────┬──────────┘
                 │                      │                       │
                 └──────────────────────┼───────────────────────┘
                                        │
                                        ▼
Step 5: Coordinator gathers Stage 1 results (CONCAT + final ORDER BY + LIMIT)
        hello:105, baz:3, foo:13, bar:15, world:30
        → ORDER BY c DESC LIMIT 10
        → hello:105, world:30, bar:15, foo:13, baz:3
```

#### Why Materialize-to-Disk First

- **Simpler**: No producer-consumer lifecycle coordination
- **Fault-tolerant**: Data on disk survives transient failures
- **Natural stage boundaries**: Coordinator knows when Stage 0 is done before starting Stage 1
- **Can add streaming later** as an optimization (Phase 3)

**Estimated effort**: 3-4 weeks.

---

### Phase 3: Pipelined Streaming Exchange

**Goal**: Eliminate the stage boundary. Stage 1 starts pulling as soon as Stage 0 starts producing.

```
PHASE 2 (stage-at-a-time):
  Stage 0 runs to completion → writes all shuffle files → Stage 1 starts

PHASE 3 (pipelined):
  Stage 0 starts producing → OutputBuffer fills → Stage 1 pulls immediately
  Both stages run concurrently
```

#### Changes from Phase 2

| Aspect | Phase 2 | Phase 3 |
|---|---|---|
| Shuffle storage | Arrow IPC files on disk | In-memory `OutputBuffer` (bounded queues of Arrow batches) |
| Stage scheduling | Sequential (Stage 0 completes, then Stage 1) | Concurrent (both start together) |
| Data pull | Flight doGet reads from disk files | Flight doGet reads from in-memory buffer |
| Backpressure | None needed (disk absorbs) | OutputBuffer full → Stage 0 blocks |
| Spill | N/A (always on disk) | Spill to disk when memory pressure high (fallback to Phase 2) |

#### Architecture

```
Stage 0 Workers                              Stage 1 Workers
┌──────────────────┐                         ┌──────────────────┐
│ DataFusion scan   │                         │ DataFusion final  │
│ + partial agg     │                         │ aggregate         │
│       │           │                         │       ▲           │
│       ▼           │                         │       │           │
│ Hash-partition    │                         │ StreamingTable    │
│       │           │                         │  (async channel)  │
│       ▼           │                         │       ▲           │
│ OutputBuffer      │◄── Flight doGet pull ──│ ShuffleReader     │
│ [part0][part1][p2]│── Arrow IPC batches ──►│  (pulls from all  │
└──────────────────┘                         │   Stage 0 workers)│
                                             └──────────────────┘

All workers run both stages. N×N communication.
Coordinator only orchestrates — never touches data.
```

#### Native Bridge Already Supports This

`NativeBridge.registerPartitionStream()` + `senderSend()` creates a bounded mpsc channel (capacity 4) that DataFusion reads from as a `StreamingTable`. This is exactly what Stage 1 workers need — proven mechanism, just needs to be wired to Flight pull instead of coordinator push.

**Estimated effort**: 3-4 weeks (after Phase 2 is stable).

---

## Key Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Phase 1 transport | Arrow IPC bytes over OpenSearch TransportService | Already works, just change payload format |
| Phase 2 shuffle transport | Arrow Flight (existing plugin) | Already deployed, handles TLS/auth/streaming |
| Phase 2 shuffle storage | Local disk (Arrow IPC files) | Simplest, fault-tolerant, no lifecycle complexity |
| Phase 3 shuffle storage | In-memory OutputBuffer with disk spill | Low latency, backpressure via bounded channels |
| Hash function | murmur3 on partition columns | Standard, good distribution |
| Stage 1 input mechanism | DataFusion `executeFromIpc` (Phase 2) → `StreamingTable` (Phase 3) | Both proven in native bridge |
| Failure model | Retry entire query | Acceptable for analytics workloads |
| Coordinator role | Orchestrate only, never touch data (Phase 2+) | Eliminates coordinator bottleneck |

---

## Reference Architecture Mapping

| Reference Concept | Our Equivalent | Status |
|---|---|---|
| `PlanFragmenter` → `SubPlan` | `PlanFragmenter` → `SubPlan` | ✅ Done |
| `PlanFragment` + `PartitioningHandle` | `PlanFragment` + `ExchangeType` + `hashColumns` | ✅ Done |
| `SqlQueryExecution` orchestrator | `DistributedScanExecutor` | Extend in Phase 2 |
| `OutputBuffer` (partitioned) | Disk files (Phase 2) → in-memory buffer (Phase 3) | Phase 2-3 |
| `DirectExchangeClient` (pull) | Flight doGet (Phase 2-3) | Phase 2-3 |
| `HttpRemoteTask` | `WorkerQueryAction` transport | Extend in Phase 2 |
| `ExchangeOperator` → `Page` | `ShuffleReader` → Arrow batches → DataFusion | Phase 2-3 |
| `LocalExecutionPlanner` | DataFusion native (SQL or Substrait) | ✅ Done |
| `MemoryPool` hierarchy | `query_memory_pool_tracker.rs` | Partial — extend in Phase 3 |
| Fault tolerance (spooling exchange) | Retry whole query | Acceptable for now |

# Distributed Query Engine — Phase 2 Design Spec

**Date**: 2026-04-14
**Branch**: To be created off `analytics-dwh-engine`
**Status**: Phase 2.1 and 2.2 approved for implementation. Phase 2.3+ is proposed vision — will be redesigned after 2.1 and 2.2 learnings.

---

## 1. Problem Statement

Phase 1 delivered distributed query execution with 4 merge strategies (CONCAT, GLOBAL_MERGE, TOPK_MERGE, SINGLE_NODE). However, **33 out of 43 ClickBench queries (77%) run on a single node** because the engine cannot distribute:

- **GROUP BY** queries (needs hash shuffle to co-locate same keys)
- **AVG** aggregation (needs SUM+COUNT decomposition)
- **COUNT DISTINCT** (needs deduplication across workers)

### Current Query Distribution

| Strategy | Count | Queries |
|----------|-------|---------|
| GLOBAL_MERGE (distributed) | 5 | q1, q2, q7, q21, q30 |
| TOPK_MERGE (distributed) | 4 | q24, q25, q26, q27 |
| CONCAT (distributed) | 1 | q20 |
| SINGLE_NODE (no parallelism) | **33** | q3-q6, q8-q19, q22-q23, q28-q29, q31-q43 |

### Root Cause: Single-Stage Execution

The current engine uses a flat, single-stage model:

```
Coordinator → [same SQL + file partition] → Workers → [results] → Coordinator merges
```

Every worker runs the **exact same full SQL query**. This fundamentally cannot handle GROUP BY because the same group key (e.g., `searchphrase="hello"`) may appear across multiple workers' file partitions, and there's no mechanism to co-locate them for final aggregation.

---

## 2. Target Vision: Pipelined Multi-Stage Execution with Pull-Based Shuffle

> **NOTE**: This section describes the end-state architecture. Phases 2.1 and 2.2 are
> approved for implementation now. Phase 2.3+ will be redesigned based on learnings
> from 2.1 and 2.2.

### 2.1 Architecture (Trino-like pipelined model)

Both stages run **concurrently**. Stage 1 pulls data from Stage 0 as it becomes available:

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
│ OutputBuffer      │◄── FetchShuffle pull ──│ ExchangeOperator  │
│ [part0][part1][p2]│── Arrow IPC batches ──►│  (pulls from all  │
└──────────────────┘                         │   Stage 0 workers)│
                                             └──────────────────┘
All workers run both stages. N×N communication (3 workers = 9 transfers, 3 local + 6 remote).
Coordinator only orchestrates — never touches data.
```

### 2.2 How Hash Shuffle Works

Example: `GROUP BY searchphrase, COUNT(*) ORDER BY count DESC LIMIT 10`

**Stage 0** — Each worker scans its file partition, computes local GROUP BY:

```
Worker-1 (files 1-10):        Worker-2 (files 11-20):      Worker-3 (files 21-30):
 searchphrase | count          searchphrase | count          searchphrase | count
 hello        | 50             hello        | 30             hello        | 25
 world        | 20             bar          | 15             world        | 10
 foo          | 5              foo          | 8              baz          | 3
```

**Hash Shuffle (pull-based, worker-to-worker):**

Stage 0 workers hash-partition output into OutputBuffer partitions. Stage 1 workers pull their partition from all Stage 0 workers:

```
hash("hello") % 3 = 0  → partition 0
hash("world") % 3 = 2  → partition 2
hash("foo")   % 3 = 1  → partition 1

Stage 1 Worker-1 (owns partition 0): pulls p0 from Worker-1, Worker-2, Worker-3
Stage 1 Worker-2 (owns partition 1): pulls p1 from Worker-1, Worker-2, Worker-3
Stage 1 Worker-3 (owns partition 2): pulls p2 from Worker-1, Worker-2, Worker-3
```

After pull, each worker has all rows for its partition:

```
Worker-1: hello:50,30,25 + bar:15    Worker-2: foo:5,8 + baz:3    Worker-3: world:20,10
```

**Stage 1** — Each worker runs final aggregate via DataFusion StreamingTable:

```
Worker-1: hello:105, bar:15    Worker-2: foo:13, baz:3    Worker-3: world:30
```

**Gather** — Coordinator merge-sorts, applies LIMIT 10.

### 2.3 Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Shuffle model | Pull-based (Trino-like) | Stage 1 controls pace, natural backpressure |
| Data flow | Worker-to-worker direct | Coordinator never bottleneck |
| Stage 1 input | DataFusion StreamingTable | Built-in, supports async stream, enables pipelining |
| Transport | OpenSearch TransportService | Already handles node discovery, security, any-to-any |
| Serialization | Arrow IPC (end-to-end) | Rust→Java(bytes)→network→Java(bytes)→Rust, Java is pass-through |
| Stage scheduling | Concurrent (both stages start together) | True pipelining, not stage-at-a-time |
| AVG handling | SUM+COUNT decomposition in Stage 0 SQL | Standard two-phase aggregation |
| COUNT DISTINCT | Hash shuffle by distinct key | Exact count, no approximation |

### 2.4 Query Pattern → Execution Plan Mapping

| Pattern | Stages | Exchange | Stage 0 SQL | Stage 1 SQL |
|---------|--------|----------|-------------|-------------|
| Scan/filter (q20) | 1 | Gather | Full query | — |
| Global SUM/COUNT/MIN/MAX (q1,q2,q7) | 1 | Gather + reduce | Full query | — |
| Global AVG (q3,q4) | 1 | Gather + reduce | SUM+COUNT decomposition | — |
| ORDER BY + LIMIT (q24-q27) | 1 | Gather + merge-sort | Full query | — |
| GROUP BY + SUM/COUNT (q13,q15-q17) | 2 | Hash shuffle | Partial GROUP BY | Final GROUP BY + sort + limit |
| GROUP BY + AVG (q28,q31-q33) | 2 | Hash shuffle | Partial GROUP BY + SUM/COUNT decomp | Final GROUP BY + division |
| GROUP BY + COUNT DISTINCT (q9,q11) | 2 | Hash shuffle | Expand (no agg) | Final GROUP BY + COUNT DISTINCT |
| Global COUNT DISTINCT (q5,q6) | 2 | Hash shuffle | SELECT DISTINCT | COUNT(*) |
| ORDER BY without LIMIT (q8) | 2 | Hash shuffle | Scan | Sort (distributed) |

### 2.5 API Compatibility

**The user-facing API does not change.** Same SQL/PPL queries, same REST endpoints. The `DataWarehouseQueryEngine` SPI stays the same — workers still call `queryEngine.executeQuery()` with a `DataWarehouseScanContext`. The only change is the SQL in the context (partial aggregate SQL vs full query).

---

## 3. Implementation Phases

### Phase 2.1: Arrow IPC Transport [APPROVED — implement now]
**Goal**: Replace `writeGenericValue()` serialization with Arrow IPC.

**Why first**: Arrow IPC is the foundation for everything — shuffle, StreamingTable, all inter-node data transfer. It also validates the Rust↔Java Arrow byte passing that the entire vision depends on.

**Changes**:
- Worker side: DataFusion already returns Arrow batches → serialize as Arrow IPC directly (skip Object[][] conversion)
- New `ArrowSerializer` — Arrow RecordBatch ↔ byte[] (IPC format)
- `WorkerQueryResponse` carries `byte[] arrowIpcData` instead of `Object[][] columnData`
- `ResultMerger` updated to work with Arrow IPC bytes (deserialize to rows for existing merge logic)
- Coordinator side: deserialize Arrow IPC for final processing

**Tests**: Deploy to 3-node cluster. All 43 ClickBench queries produce identical results. Run correctness script.

**Learnings expected**:
- Arrow IPC byte sizes vs current serialization (network bandwidth impact)
- Serialization/deserialization overhead profile
- Any issues with Arrow type mapping (timestamps, decimals, nulls)
- FFM memory management patterns for Arrow buffers

---

### Phase 2.2: Coordinator StreamingTable PoC [APPROVED — implement now]
**Goal**: Replace Java ResultMerger with DataFusion StreamingTable on coordinator.

**Why second**: This validates the core mechanism that Stage 1 workers will use later. The coordinator is the simplest place to test StreamingTable because it already receives all worker results — no shuffle infrastructure needed.

**Changes**:
- New Rust API: `df_execute_from_stream(arrow_ipc_batches, sql, runtime_ptr) → stream_ptr`
  - Internally: creates StreamingTable from Arrow batches, registers as table, runs SQL
- New Java FFM binding: `NativeBridge.executeFromStream()`
- Coordinator uses StreamingTable instead of Java merge:
  - GLOBAL_MERGE: `SELECT SUM(col1), MIN(col2) FROM input`
  - TOPK_MERGE: `SELECT * FROM input ORDER BY col1 DESC LIMIT 10`
  - CONCAT: `SELECT * FROM input`
- Remove or deprecate Java `ResultMerger`, `AggregationReducer`, `TopKMerger`
- AVG decomposition: QueryAnalyzer rewritten SQL → workers return SUM+COUNT → coordinator StreamingTable runs `SELECT SUM(s)/SUM(c) as avg FROM input`

**Tests**: Deploy to 3-node cluster. All 43 ClickBench queries produce identical results. Run correctness script. Also validates:
- q3, q4 move from SINGLE_NODE to distributed (AVG decomposition)
- Coordinator merge now handled by DataFusion (same engine as workers)

**Learnings expected**:
- StreamingTable API behavior (schema inference, null handling, type coercion)
- Performance of DataFusion merge vs Java merge
- Memory usage patterns for streaming batches
- Any limitations of StreamingTable that affect the shuffle design
- Whether `df_execute_from_stream` API shape works for Stage 1 workers too

---

### Phase 2.3+: Multi-Stage Pipelined Execution [PROPOSED — redesign after 2.1 & 2.2]

> **This section is a proposed vision, not a committed plan.** After Phase 2.1 and 2.2
> are complete and deployed, we will redesign Phase 2.3+ based on actual learnings
> about Arrow IPC transport, StreamingTable behavior, and any limitations discovered.

**Proposed sub-phases** (subject to change):

#### Phase 2.3a: Execution Plan Model + Query Planner
- `ExecutionPlan` DAG data structure (stages + exchanges)
- `QueryPlanner` — Calcite RelNode → multi-stage ExecutionPlan
- Generates partial aggregate SQL (Stage 0) and final aggregate SQL (Stage 1)
- Handles GROUP BY, AVG decomposition, COUNT DISTINCT

#### Phase 2.3b: Pull-Based Shuffle Infrastructure
- `OutputBuffer` — Stage 0 workers hash-partition output, serve to pullers
- `FetchShuffleAction` — Stage 1 worker pulls partition from Stage 0 worker
- N×N worker-to-worker communication via TransportService
- Arrow IPC bytes as opaque pass-through (Java never deserializes shuffle data)

#### Phase 2.3c: Pipelined Stage Execution
- Both stages scheduled concurrently
- Stage 1 DataFusion reads via StreamingTable backed by async channel
- FetchShuffle pulls feed into StreamingTable channel
- Same `df_execute_from_stream` API proven in Phase 2.2

#### Phase 2.3d: COUNT DISTINCT + Remaining Patterns
- Extend QueryPlanner for all remaining SINGLE_NODE patterns
- All 43/43 ClickBench queries distributed

#### Phase 2.4: Optimizations
- LZ4 compression on Arrow IPC transfers
- Backpressure tuning (OutputBuffer limits, pull rate control)
- Partial LIMIT pushdown
- Spill to disk for large shuffles
- Adaptive execution

---

## 4. Reference Implementations

- **Ballista** (`~/opensource/ballista`): Arrow IPC + LZ4, hash shuffle, sort-based shuffle, stage DAG
- **Trino** (`~/opensource/trino`): Pipelined execution, pull-based exchanges, two-phase aggregation (PARTIAL/FINAL), OutputBuffer

Key files for reference:
- Ballista planner: `ballista/scheduler/src/planner.rs`
- Ballista shuffle writer: `ballista/core/src/execution_plans/shuffle_writer.rs`
- Ballista flight service: `ballista/executor/src/flight_service.rs`
- Trino aggregation steps: `core/trino-main/src/main/java/io/trino/sql/planner/plan/AggregationNode.java`
- Trino exchange: `core/trino-main/src/main/java/io/trino/sql/planner/plan/ExchangeNode.java`
- Trino partial agg pushdown: `core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/PushPartialAggregationThroughExchange.java`

---

## 5. Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| StreamingTable limitations | May not support all SQL patterns | Phase 2.2 PoC discovers this early |
| Arrow type mismatches | Wrong results | Phase 2.1 tests all 43 queries end-to-end |
| N×N shuffle network saturation | Slow shuffle | LZ4 compression, backpressure in Phase 2.4 |
| Hash skew (one key dominates) | Unbalanced shuffle | Partial LIMIT pushdown in Phase 2.4 |
| Shuffle data loss on worker failure | Query failure | Retry entire query (acceptable for analytics) |
| Pipelining complexity | Deadlocks, resource leaks | Phase 2.2 proves StreamingTable first in simple context |

---

## 6. Success Criteria

- **Phase 2.1**: All 43 ClickBench queries produce identical results with Arrow IPC transport
- **Phase 2.2**: Coordinator uses StreamingTable for merge. q3, q4 move to distributed. All 43 pass correctness.
- **Phase 2.3** (after redesign): All 43 ClickBench queries run distributed (0 SINGLE_NODE), pipelined execution
- **Overall**: 43/43 distributed, >3x speedup on 3-node cluster vs single-node

**Every phase is deployed to the 3-node EC2 cluster and tested with the full 99.9M row ClickBench dataset.** Unit tests run locally, but verification always happens on cluster via `~/deploy-cluster.sh` + `run-clickbench.sh` + `run_correctness.sh`.

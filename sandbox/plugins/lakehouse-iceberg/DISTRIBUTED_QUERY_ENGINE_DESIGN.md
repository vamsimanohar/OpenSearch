# Distributed Query Engine for OpenSearch Lakehouse

## Context

The current lakehouse-iceberg plugin executes all queries on a single node — one DataFusion instance processes ALL Parquet files from an Iceberg table. For a 100M-row ClickBench table (30 files, ~15GB), one node does all the work. This plan introduces distributed execution: splitting file-level work across cluster nodes, collecting and merging results on a coordinator.

**Goal**: Production-grade distributed SQL/PPL engine over Iceberg tables, built incrementally.

**Key Decision**: SQL-level distribution (each worker gets same SQL + file subset) for Phases 1-2, evolving to plan-level fragmentation for Phases 3-4.

**API**: Transparent — same `_lakehouse/sql` endpoint, auto-detects multi-node cluster.

---

## Architecture Overview

```
                         Client
                           |
                    POST _lakehouse/sql
                           |
                   ┌───────▼────────┐
                   │  Coordinator    │  (node that receives the request)
                   │                 │
                   │ 1. Parse SQL    │
                   │ 2. Calcite plan │
                   │ 3. Iceberg scan │  (file pruning via predicate pushdown)
                   │ 4. Analyze plan │  (detect agg/sort/limit/join)
                   │ 5. Partition    │  (split files across workers)
                   │ 6. Dispatch     │  (transport actions to workers)
                   │ 7. Merge        │  (combine worker results)
                   └──┬─────┬─────┬─┘
                      │     │     │
            ┌─────────▼┐ ┌─▼─────┴──┐ ┌──────────┐
            │ Worker-1  │ │ Worker-2  │ │ Worker-3  │
            │           │ │           │ │           │
            │ SQL query  │ │ SQL query  │ │ SQL query  │
            │ + files    │ │ + files    │ │ + files    │
            │ [0..9]    │ │ [10..19]  │ │ [20..29]  │
            │           │ │           │ │           │
            │ DataFusion│ │ DataFusion│ │ DataFusion│
            │ (JNI)     │ │ (JNI)     │ │ (JNI)     │
            └───────────┘ └───────────┘ └───────────┘
                 S3            S3            S3
```

**Coordinator** = whichever node receives the REST request (no dedicated coordinator role).
**Workers** = all nodes with the lakehouse plugin installed (including the coordinator itself).

---

## Data Communication: Arrow IPC

All inter-node data transfer uses **Apache Arrow IPC** (streaming format) — the native output of DataFusion. This avoids costly Object[][] serialization and enables zero-copy columnar processing.

```
Worker Node                          Coordinator Node
┌────────────────────┐               ┌──────────────────────┐
│ DataFusion (Rust)  │               │                      │
│   ↓ JNI            │               │  ArrowStreamReader   │
│ VectorSchemaRoot   │               │   ↓                  │
│   ↓                │  Transport    │  VectorSchemaRoot    │
│ ArrowStreamWriter  │ ──────────→   │   ↓                  │
│   ↓                │  (byte[])     │  ResultMerger        │
│ byte[] (IPC)       │               │  (operates on Arrow) │
└────────────────────┘               │   ↓                  │
                                     │  Object[][] (only at │
                                     │  REST response layer) │
                                     └──────────────────────┘
```

**Why Arrow IPC:**
- DataFusion already produces Arrow RecordBatches — no conversion overhead on workers
- Columnar format enables efficient merge operations (sort-merge, aggregate) on coordinator
- Same format reused for multi-stage exchange in Phase 3 (shuffle layer)
- Type-safe: schema travels with data, no ambiguous Object[] casting
- Compact: columnar encoding + optional compression

**Implementation:**
- Worker: `DataFusion JNI → VectorSchemaRoot → ArrowStreamWriter.writeBatch() → byte[]`
- Transport: `byte[]` payload in `WorkerQueryResponse` (OpenSearch transport handles framing)
- Coordinator: `ArrowStreamReader → VectorSchemaRoot → ResultMerger` (Arrow-native merge)
- REST layer: `VectorSchemaRoot → Object[][] → PPLResponse JSON` (conversion only at boundary)

**Existing Arrow dependencies** (already in analytics-backend-datafusion):
- `org.apache.arrow:arrow-vector` — VectorSchemaRoot, field vectors
- `org.apache.arrow:arrow-memory-netty` — memory allocator
- Arrow C Data Interface for JNI bridge (ArrowArray/ArrowSchema)
- `ArrowStreamWriter` / `ArrowStreamReader` available in `arrow-vector` for IPC serialization

---

## Component Breakdown

### 1. DistributedScanExecutor
**Responsibility**: Orchestrates distributed execution when multiple workers are available.
**Location**: `org.opensearch.lakehouse.distributed.DistributedScanExecutor`

- Called from `LakehousePlugin.prepareScan()` when cluster has >1 eligible node
- Inputs: `IcebergScanPlan` (pruned file list), SQL query string, storage config
- Discovers eligible worker nodes via `ClusterService`
- Delegates to `FilePartitioner` for file assignment
- Fans out `WorkerQueryRequest` via `TransportService`
- Collects results via `GroupedActionListener`
- Delegates to `ResultMerger` for final combination
- **Deterministic routing only**: queries with unsupported operators (e.g., COUNT DISTINCT before Phase 3) execute single-node based on QueryAnalyzer classification — NOT as error fallback. If distributed execution encounters an error, the error propagates to the client. No silent fallback.

### 2. FilePartitioner
**Responsibility**: Assigns files to workers, balanced by total file size.
**Location**: `org.opensearch.lakehouse.distributed.FilePartitioner`

- Input: `List<FileInfo>` (path + size), number of workers
- Output: `List<FileAssignment>` — one per worker, each containing a file subset
- Strategy: Greedy bin-packing (largest-file-first, assign to least-loaded worker)
- Ensures even distribution: no worker gets >2x the average load

### 3. WorkerQueryAction (Transport Action)
**Responsibility**: Executes a SQL query against a file subset on a worker node.
**Location**: `org.opensearch.lakehouse.distributed.WorkerQueryAction`

- Registered as `HandledTransportAction` with action name `cluster:internal/lakehouse/worker/query`
- Request: `WorkerQueryRequest` (SQL, file paths, file sizes, storage config, merge hint)
- Response: `WorkerQueryResponse` (Arrow IPC byte[] + schema metadata)
- Execution: calls DataFusion JNI → VectorSchemaRoot → ArrowStreamWriter → byte[]
- Handles: credential setup, classloader swap, doPrivileged
- Arrow IPC serialization avoids Object[][] overhead; schema travels with data

### 4. QueryAnalyzer
**Responsibility**: Inspects Calcite RelNode to determine merge strategy.
**Location**: `org.opensearch.lakehouse.distributed.QueryAnalyzer`

- Walks the RelNode tree, detects:
  - `hasAggregation` — LogicalAggregate present
  - `hasOrderBy` — LogicalSort with collation
  - `hasLimit` — LogicalSort with fetch/offset
  - `hasJoin` — LogicalJoin present (Phase 3)
  - `groupByKeys` — columns in GROUP BY
  - `aggregateFunctions` — SUM, COUNT, AVG, MIN, MAX, etc.
- Returns `QueryProfile` with merge strategy recommendation

### 5. SqlRewriter
**Responsibility**: Rewrites SQL for distributed partial execution on workers.
**Location**: `org.opensearch.lakehouse.distributed.SqlRewriter`

For aggregation queries, transforms:
```sql
-- Original (coordinator sends to workers)
SELECT region, COUNT(*), SUM(amount) FROM t WHERE x > 10 GROUP BY region

-- Worker SQL (partial aggregation)
SELECT region, COUNT(*) AS _cnt, SUM(amount) AS _sum FROM t WHERE x > 10 GROUP BY region

-- Coordinator merge: SUM(_cnt), SUM(_sum) per region
```

For ORDER BY + LIMIT:
```sql
-- Original
SELECT * FROM t ORDER BY amount DESC LIMIT 100

-- Worker SQL (local top-K)
SELECT * FROM t ORDER BY amount DESC LIMIT 100

-- Coordinator: merge-sort all worker results, take top 100
```

### 6. ResultMerger
**Responsibility**: Combines partial results from workers into final result.
**Location**: `org.opensearch.lakehouse.distributed.ResultMerger`

Operates directly on Arrow VectorSchemaRoot (deserialized from worker Arrow IPC responses).
Merge strategies:
- **CONCAT**: Concatenate Arrow batches from workers (scan/filter/project queries)
- **GLOBAL_MERGE**: Re-aggregate single-row partial results (global agg without GROUP BY: SUM counts, SUM sums, MIN of MINs, recompute AVG from SUM/COUNT)
- **AGGREGATE_MERGE**: Hash-merge partial GROUP BY results — for each unique group key across workers, combine aggregates (SUM partial COUNTs, SUM partial SUMs, MIN of partial MINs, recompute AVG)
- **SORT_MERGE**: K-way merge-sort from pre-sorted worker Arrow batches (ORDER BY queries)
- **TOPK_MERGE**: K-way merge-sort + early termination at limit (ORDER BY + LIMIT queries)

Final output: merged VectorSchemaRoot → Object[][] only at REST response boundary.

### 7. NodeDiscovery
**Responsibility**: Finds eligible worker nodes in the cluster.
**Location**: `org.opensearch.lakehouse.distributed.NodeDiscovery`

- Uses `ClusterService.state().nodes()` to get all nodes
- Filters by node attribute: nodes with lakehouse plugin register `lakehouse.worker=true`
- Returns `List<DiscoveryNode>` of eligible workers
- Includes the local node (coordinator participates as a worker too)

---

## Worker Count Decision

Unlike OpenSearch indices with shard ownership, Iceberg tables have no pre-assigned shards. The number of workers is determined dynamically:

```
Available workers = all nodes with lakehouse plugin (via NodeDiscovery)
Effective workers = min(available_workers, file_count)
```

**Rules:**
1. **All lakehouse nodes participate** — every node with the plugin is a worker
2. **Never more workers than files** — if 30 files and 10 nodes, only 3 files/node (all nodes used). If 5 files and 10 nodes, only 5 nodes get work.
3. **Phase 4 CBO may reduce** — for tiny queries (after filter, <100K rows expected), CBO may choose fewer workers or single-node to avoid overhead.

**Trino comparison**: Trino assigns "splits" (file chunks) to all available workers. Splits can be sub-file (e.g., row-group ranges within Parquet). Workers pull splits on demand. We start simpler: one file = one split, assigned statically at planning time. Phase 4 could add sub-file splitting.

---

## Coordinator-as-Worker Optimization

The coordinator node also participates as a worker. Communication differs by locality:

```
Coordinator receives query
  │
  ├── Local worker (this node):
  │   Direct function call → DataFusion JNI → Arrow batches
  │   No transport serialization, no Arrow IPC encode/decode
  │   Fastest path — zero network overhead
  │
  ├── Remote worker-2:
  │   TransportService.sendRequest() → Arrow IPC bytes → transport
  │   Worker-2: deserialize → DataFusion JNI → serialize → respond
  │
  └── Remote worker-3:
      Same as worker-2 (parallel with worker-2)
```

**Implementation**: `DistributedScanExecutor` checks if a worker is the local node:
- Local: call `executeLocally(sql, files, storageConfig)` → returns `VectorSchemaRoot` directly
- Remote: call `transportService.sendRequest(...)` → `WorkerQueryResponse` → deserialize Arrow IPC
- Both return the same type (`VectorSchemaRoot`) to `ResultMerger`

This matches the OpenSearch pattern in `SearchTransportService.sendLocalRequest()`.

---

## ResultMerger: Java-Based, Arrow-Native

The ResultMerger runs in **Java** on the coordinator, operating on Arrow `VectorSchemaRoot` objects.

**Why Java (not DataFusion/Rust)?**
- Merge logic needs query semantics (GROUP BY keys, agg types) which come from Calcite RelNode analysis
- Partial results from N workers are already Arrow VectorSchemaRoots in Java heap
- Simple merges (CONCAT, GLOBAL_MERGE, TOPK) are trivial in Java on columnar Arrow data
- Avoids round-trip: Java → serialize → JNI → Rust → execute → JNI → Java

**When DataFusion merge makes sense (Phase 3+):**
- For very large intermediate results that need spilling
- For complex merge operations (hash-agg on millions of groups)
- Strategy: register intermediate Arrow batches as in-memory DataFusion tables via JNI, execute merge SQL

```
Phase 1-2: Java ResultMerger (simple, no JNI overhead)
  ┌─────────────┐
  │ Worker Arrow │──┐
  │ batches (3x) │  │   Java Arrow API
  └─────────────┘  ├──→ ResultMerger ──→ merged VectorSchemaRoot ──→ Object[][] → REST
  ┌─────────────┐  │   (hash-merge, sort-merge, concat)
  │ Worker Arrow │──┘
  │ batches      │
  └─────────────┘

Phase 3+: DataFusion merge (for large intermediate data)
  ┌─────────────┐
  │ Worker Arrow │──┐
  │ batches      │  │   JNI: register as in-memory tables
  └─────────────┘  ├──→ DataFusion (Rust) ──→ Arrow batches ──→ Object[][] → REST
  ┌─────────────┐  │   (merge SQL on registered tables)
  │ Worker Arrow │──┘
  │ batches      │
  └─────────────┘
```

---

## Execution Flow (Detailed)

### Simple Query (scan + filter + project)
```
1. REST → LakehouseQueryTransportAction.doExecute()
2. Calcite parse → RelNode: Project(Filter(TableScan))
3. PushDownPlanner → OpenSearchBoundaryTableScan
4. LakehousePlugin.prepareScan():
   a. Iceberg predicate pushdown → pruned 15 files (of 30)
   b. NodeDiscovery → 3 workers available
   c. FilePartitioner → [5 files, 5 files, 5 files]
   d. QueryAnalyzer → CONCAT merge strategy
   e. DistributedScanExecutor.execute():
      - Send WorkerQueryRequest to all 3 workers (parallel)
      - Each worker: DataFusion JNI(SQL, 5 files) → rows
      - GroupedActionListener collects 3 responses
      - ResultMerger.concat(responses) → combined rows
5. Return PPLResponse(columns, rows)
```

### Aggregation Query (GROUP BY + SUM/COUNT)
```
1-3. Same as above
4. LakehousePlugin.prepareScan():
   a. Iceberg scan → 30 files
   b. 3 workers available
   c. FilePartitioner → [10 files, 10 files, 10 files]
   d. QueryAnalyzer → AGGREGATE_MERGE strategy
      - groupKeys: [region]
      - aggFunctions: [COUNT(*), SUM(amount)]
   e. SqlRewriter rewrites for partial aggregation
   f. DistributedScanExecutor.execute():
      - Send partial-agg SQL to all 3 workers
      - Each worker: partial GROUP BY on its 10 files
      - Coordinator collects 3 partial results
      - ResultMerger.aggregateMerge():
        * Group by region across all partials
        * SUM the COUNTs, SUM the SUMs
        * For AVG: track SUM + COUNT, compute SUM/COUNT
5. Return final aggregated PPLResponse
```

### ORDER BY + LIMIT Query
```
1-3. Same as above
4. LakehousePlugin.prepareScan():
   d. QueryAnalyzer → TOPK_MERGE strategy
      - sortKeys: [amount DESC]
      - limit: 100
   e. Worker SQL: same query (each returns top-100 locally)
   f. DistributedScanExecutor.execute():
      - Each worker returns up to 100 rows (pre-sorted)
      - ResultMerger.topKMerge():
        * Merge-sort 3 sorted streams
        * Take top 100 from merged result
5. Return top-100 rows
```

---

## Phased Implementation Roadmap

### Phase 1: Walking Skeleton (PRs 1-3)

**Goal**: File-level distribution with Arrow IPC transport. Handles 14 queries: scan/filter/project + global aggregations (no GROUP BY). All other queries fall back to single-node.

**Queries distributed**: Q1-Q4, Q7, Q20-Q21, Q24-Q27, Q30 (global aggs + scan/sort)
**Queries 1N fallback**: Q5-Q6, Q8-Q19, Q22-Q23, Q28-Q29, Q31-Q43 (GROUP BY, DISTINCT)

#### PR 1: Transport Action + Node Discovery + Arrow IPC (~400 lines)
- `WorkerQueryAction` + `WorkerQueryRequest` + `WorkerQueryResponse` (Arrow IPC byte[] payload)
- Arrow IPC serialization: `VectorSchemaRoot → ArrowStreamWriter → byte[]` on worker
- Arrow IPC deserialization: `byte[] → ArrowStreamReader → VectorSchemaRoot` on coordinator
- `NodeDiscovery` — find eligible lakehouse nodes via node attributes
- Node attribute registration in `LakehousePlugin`
- Unit tests with mocked transport

#### PR 2: File Partitioner + Distributed Executor + Global Merge (~400 lines)
- `FilePartitioner` — greedy bin-packing by file size
- `DistributedScanExecutor` — orchestration with `GroupedActionListener`
- `ResultMerger` — CONCAT, GLOBAL_MERGE (SUM counts, SUM sums, MIN of MINs, recompute AVG), TOPK_MERGE
- Basic `QueryAnalyzer` — detect global agg vs scan vs sort+limit (no GROUP BY analysis yet)
- Credential propagation in `WorkerQueryRequest`
- Single-node fallback for GROUP BY queries and when cluster size = 1

#### PR 3: End-to-End Wiring + Integration Tests (~300 lines)
- Wire distributed path into `LakehousePlugin.prepareScan()`
- Integration tests: multi-node `internalClusterTest`
- Benchmark: run ClickBench Q1-Q7, Q20-Q21, Q24-Q27, Q30 on 3-node cluster
- Verify row counts and values match single-node exactly

**Phase 1 Deliverable**: 14 queries distributed with ~2.5-3x speedup on scan-heavy queries. All 43 queries correct (29 via single-node fallback).

### Phase 2: Smart Aggregation & Sorting (PRs 4-6)

**Goal**: GROUP BY queries return correct distributed results via partial aggregation + coordinator merge. Brings distributed count from 14 → 35 queries.

**Newly distributed**: Q8, Q13, Q15-Q19, Q22, Q28-Q29, Q31-Q43 (all GROUP BY with distributable aggs)
**Still 1N fallback**: Q5-Q6, Q9-Q12, Q14, Q23 (have COUNT DISTINCT — need shuffle)

#### PR 4: Full Query Analyzer + SQL Rewriter (~400 lines)
- Enhanced `QueryAnalyzer` — walk Calcite RelNode tree, detect GROUP BY keys, aggregate functions (SUM/COUNT/AVG/MIN/MAX vs COUNT DISTINCT), HAVING, ORDER BY, LIMIT
- `SqlRewriter` — rewrite SQL for partial aggregation on workers:
  - AVG(x) → `SUM(x) AS _sum_x, COUNT(x) AS _cnt_x` (coordinator computes SUM/COUNT)
  - COUNT(*) → stays as-is (coordinator SUMs)
  - HAVING → removed from worker SQL (applied on coordinator after merge)
  - ORDER BY + LIMIT → kept on workers (each returns local top-K)
- Detect COUNT DISTINCT → flag query for single-node fallback
- Unit tests for every ClickBench query pattern

#### PR 5: GROUP BY Result Merger (~400 lines)
- `ResultMerger.AGGREGATE_MERGE` — operates on Arrow VectorSchemaRoot:
  - Hash-merge partial GROUP BY results from all workers
  - For each unique group key: SUM partial COUNTs, SUM partial SUMs, MIN of MINs, MAX of MAXs
  - Recompute AVG from merged SUM/COUNT
  - Apply HAVING predicate on merged results
  - Re-sort by ORDER BY keys, apply LIMIT
- Unit tests for all merge operations with Arrow data

#### PR 6: Integration + Full Benchmark (~300 lines)
- Wire full analyzer + rewriter + merger into distributed executor
- Integration tests: all GROUP BY query patterns on multi-node cluster
- Benchmark: full ClickBench Q1-Q43 on 3-node vs single-node
- Correctness verification against DataFusion CLI
- **Key test**: Q34/Q35 (100M unique URLs) — may now work with 3x memory!

**Phase 2 Deliverable**: 35/43 queries distributed. Q34/Q35 potentially solved (3 workers each build 1/3 hash table). ~1.5-3x speedup depending on query type.

### Phase 3: Data Shuffle & Joins (PRs 7-9)

**Goal**: Distributed COUNT(DISTINCT), joins, and high-cardinality GROUP BY via Arrow IPC shuffle. All 43 queries distributed.

**Newly distributed**: Q5-Q6, Q9-Q12, Q14, Q23 (COUNT DISTINCT via shuffle)
**Multi-stage execution**: Scan stage → Shuffle stage → Aggregate stage

#### PR 7: Exchange Service (~500 lines)
- `ExchangeService` — inter-node Arrow IPC data transfer over OpenSearch transport
- Pull-based protocol (more reliable, per previous Phase 2 experience)
- `HashPartitioner` — hash rows by key columns, route to destination partition
- Backpressure via memory tracking (respect DataFusion memory pool)
- Retry with timeout (5 attempts, 5s each)
- Arrow IPC batches as the wire format (same as Phase 1 worker responses)

#### PR 8: Plan Fragmenter + Multi-Stage Execution (~400 lines)
- `PlanFragmenter` — split Calcite RelNode at shuffle boundaries into Stage DAG
- `StageScheduler` — execute stages in topological order
- Stage 1 (Scan+Partial): each worker scans its files, computes partial results
- Shuffle: repartition intermediate Arrow data by hash(group key) across nodes
- Stage 2 (Final): each node aggregates its partition of the key space
- Coordinator collects final results from all partitions

#### PR 9: Distributed Joins (~400 lines)
- Hash-partitioned join: both sides shuffled by join key
- Broadcast join for small tables (< configurable threshold, e.g., 10MB)
- Integration tests with multi-table queries

**Phase 3 Deliverable**: All 43 queries distributed. COUNT(DISTINCT) correct via shuffle. Joins supported.

#### Phase 3: Shuffle Architecture (Detailed Design)

**Problem**: COUNT(DISTINCT userId) across workers. User 123 may appear in files on worker-1 AND worker-2. Simple merge of local distinct counts double-counts user 123.

**Solution**: Hash-partition rows by the DISTINCT key, so ALL rows for user 123 go to the same node.

```
                    Stage 1: Scan + Partial
    ┌─────────────────────────────────────────────┐
    │                                             │
    │  Worker-1 (files 0-9)    Worker-2 (10-19)   Worker-3 (20-29)
    │  ┌──────────────┐        ┌──────────────┐   ┌──────────────┐
    │  │ Scan files    │        │ Scan files    │   │ Scan files    │
    │  │ Extract:      │        │ Extract:      │   │ Extract:      │
    │  │ regionId,     │        │ regionId,     │   │ regionId,     │
    │  │ userId        │        │ userId        │   │ userId        │
    │  └──────┬───────┘        └──────┬───────┘   └──────┬───────┘
    │         │                       │                   │
    └─────────┼───────────────────────┼───────────────────┼──────┘
              │                       │                   │
              ▼                       ▼                   ▼
    ┌─────────────────────────────────────────────────────────────┐
    │                    Shuffle Layer                             │
    │  Hash(userId) % 3 determines destination partition          │
    │                                                             │
    │  Partition 0 ◄── rows where hash(userId)%3=0 from ALL workers│
    │  Partition 1 ◄── rows where hash(userId)%3=1 from ALL workers│
    │  Partition 2 ◄── rows where hash(userId)%3=2 from ALL workers│
    │                                                             │
    │  Transport: Arrow IPC batches between nodes                  │
    └─────────────────────────────────────────────────────────────┘
              │                       │                   │
              ▼                       ▼                   ▼
    ┌─────────────────────────────────────────────────────────────┐
    │                Stage 2: Final Aggregate                      │
    │                                                             │
    │  Node-1 (partition 0)  Node-2 (partition 1)  Node-3 (part 2)│
    │  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐│
    │  │ All userId    │     │ All userId    │     │ All userId    ││
    │  │ where h%3=0   │     │ where h%3=1   │     │ where h%3=2   ││
    │  │               │     │               │     │               ││
    │  │ GROUP BY      │     │ GROUP BY      │     │ GROUP BY      ││
    │  │ regionId      │     │ regionId      │     │ regionId      ││
    │  │ COUNT(DISTINCT│     │ COUNT(DISTINCT│     │ COUNT(DISTINCT││
    │  │   userId)     │     │   userId)     │     │   userId)     ││
    │  └──────┬───────┘     └──────┬───────┘     └──────┬───────┘│
    └─────────┼───────────────────────┼───────────────────┼──────┘
              │                       │                   │
              ▼                       ▼                   ▼
    ┌─────────────────────────────────────────────────────────────┐
    │              Coordinator: Final Merge                        │
    │  For each regionId: SUM the distinct counts from partitions  │
    │  (Each userId was counted in exactly ONE partition, so       │
    │   summing is correct)                                       │
    │  Apply ORDER BY + LIMIT                                     │
    │  Return to client                                           │
    └─────────────────────────────────────────────────────────────┘
```

**Shuffle Protocol (Pull-Based):**
```
1. Coordinator tells all workers: "Start scan stage, buffer results partitioned by hash(key)"
2. Workers scan files → hash each row → buffer into N partition buckets (Arrow batches)
3. Workers signal "scan complete" to coordinator
4. Coordinator tells all nodes: "Pull partition P from all other nodes"
5. Each node pulls its assigned partition from all workers via ExchangeService
6. Each node runs final aggregate on its complete partition
7. Coordinator collects final results from all nodes → merge → return
```

**Why pull-based (not push)?**
- Previous Phase 2 attempt showed OpenSearch transport intermittently drops push responses
- Pull with retry (5 attempts, 5s timeout each) is more reliable
- Consumer controls memory pressure (doesn't get flooded)

#### Phase 3: Distributed Join Architecture

```
SELECT t1.col, t2.col FROM iceberg_table_1 t1 JOIN iceberg_table_2 t2 ON t1.key = t2.key

  Hash Join (both sides repartitioned by join key):

  Stage 1a: Scan t1 files     Stage 1b: Scan t2 files
  (parallel across workers)    (parallel across workers)
           │                            │
           ▼                            ▼
  Shuffle by hash(key) % N    Shuffle by hash(key) % N
           │                            │
           ▼                            ▼
  ┌────────────────────────────────────────┐
  │  Node-1: partition 0                   │
  │  t1 rows where h(key)%3=0             │
  │  t2 rows where h(key)%3=0             │
  │  → Local hash join (all matching keys  │
  │    guaranteed to be on this node)      │
  └────────────────────────────────────────┘

  Broadcast Join (small table < 10MB):

  Stage 1: Scan small table → broadcast to ALL workers
  Stage 2: Each worker scans its large table files
           and joins against the broadcasted small table locally
  → No shuffle needed for large table
```

### Phase 4: Optimization & Resilience (PRs 10-12)

**Goal**: Production hardening.

#### PR 10: Fault Tolerance (~300 lines)
- Worker failure detection (task timeout + retry on different node)
- Partial result handling (succeed with N-1 workers if possible)
- Graceful degradation to single-node

#### PR 11: Memory-Aware Scheduling (~300 lines)
- Track DataFusion memory usage per node
- Route work away from memory-pressured nodes
- Cluster-wide memory reporting

#### PR 12: Cost-Based Optimization (~400 lines)
- File-size-based cost estimation
- Predicate selectivity estimation
- Dynamic worker count selection (don't use all nodes for tiny queries)

#### Phase 4: Cost-Based Optimizer (Detailed Design)

**Problem**: Not all queries benefit from distribution. Q37-Q43 filter on `counterId=62` (one value out of ~7K), leaving <100K rows. Distributing these across 3 nodes adds ~0.8s overhead per worker (transport + JNI startup) but saves almost nothing in execution time.

**CBO Decision Flow:**
```
                    Query arrives
                         │
                         ▼
               ┌─────────────────────┐
               │  QueryAnalyzer      │
               │  + CostEstimator    │
               └─────────┬───────────┘
                         │
                         ▼
               ┌─────────────────────┐
               │  Estimate scan cost  │
               │                     │
               │  totalFileSize      │ ← from IcebergScanPlan
               │  fileCount          │
               │  predicateSelectivity│ ← estimated from Iceberg stats
               │  estimatedRows      │ ← totalRows * selectivity
               │  estimatedScanTime  │ ← totalFileSize / S3_throughput
               └─────────┬───────────┘
                         │
              ┌──────────┼──────────┐
              │          │          │
              ▼          ▼          ▼
     estimatedScanTime   N/A     estimatedScanTime
     > THRESHOLD?                < THRESHOLD?
              │                     │
              ▼                     ▼
     ┌────────────────┐   ┌────────────────┐
     │ DISTRIBUTE     │   │ SINGLE-NODE    │
     │                │   │                │
     │ workerCount =  │   │ No transport   │
     │ min(available, │   │ overhead       │
     │     fileCount) │   │ Direct JNI     │
     │                │   │                │
     │ But also:      │   └────────────────┘
     │ if fileCount<3 │
     │   → single-node│
     └────────────────┘
```

**Cost Estimation Inputs:**

| Signal | Source | How Used |
|--------|--------|----------|
| Total file size (bytes) | `IcebergScanPlan.getTotalFileSize()` | Estimate I/O time |
| File count | `IcebergScanPlan.fileCount()` | Max parallelism |
| Predicate selectivity | Iceberg column stats (min/max/null count per file) | Estimate rows after filter |
| Column count projected | Calcite plan analysis | Estimate output size per row |
| Available workers | `NodeDiscovery` | Potential parallelism |
| Fixed overhead per worker | Measured constant (~0.8s) | Cost of distribution |

**Decision Rules:**
1. `fileCount < 3` → single-node (not enough files to split)
2. `totalFileSize < 100MB` → single-node (overhead dominates)
3. `estimatedRows < 50K` (after predicate selectivity) → single-node
4. `estimatedScanTime < 2s` → single-node (overhead ≈ benefit)
5. Otherwise → distribute with `workerCount = min(availableWorkers, fileCount)`

**Predicate Selectivity from Iceberg Stats:**
```
Iceberg provides per-file column statistics:
  - lower_bound, upper_bound (for each column per file)
  - null_count, value_count
  - file_size_in_bytes

For filter `WHERE counterId = 62`:
  Files where 62 is outside [lower_bound, upper_bound] → selectivity = 0 (pruned)
  Files where 62 is inside range → estimate selectivity ≈ 1/distinct_count

Aggregate across all non-pruned files:
  estimatedRows = SUM(file.value_count * per_file_selectivity)
```

**Dynamic Worker Count:**
```
For Q37 (counterId=62, date range filter):
  - 30 files total → Iceberg prunes to ~3 files (only July 2013 data)
  - 3 files * ~500MB each = 1.5GB total
  - estimatedRows ≈ 100K (selective filter)
  - estimatedScanTime ≈ 1.5s
  - Overhead per worker ≈ 0.8s
  - CBO: 1.5s < 2s threshold → single-node ✓

For Q1 (COUNT(*), no filter):
  - 30 files, 15GB total
  - estimatedScanTime ≈ 15s
  - 15s >> 2s threshold → distribute with 3 workers
  - Each worker: 5s scan + 0.8s overhead = 5.8s
  - Total: ~6s (vs 15s single-node = 2.5x speedup) ✓
```

#### Phase 4: Fault Tolerance Design

```
                  Coordinator
                      │
         ┌────────────┼────────────┐
         │            │            │
    Worker-1     Worker-2     Worker-3
    (responds)   (TIMEOUT)    (responds)
         │            │            │
         ▼            ▼            ▼
    ┌─────────────────────────────────┐
    │  GroupedActionListener          │
    │  onResponse(1) ✓                │
    │  onFailure(2)  ✗ timeout        │
    │  onResponse(3) ✓                │
    │                                 │
    │  Policy: FAIL query             │
    │  (no silent fallback)           │
    │                                 │
    │  Error message includes:        │
    │  - Which worker failed          │
    │  - Failure reason               │
    │  - Suggestion to retry          │
    └─────────────────────────────────┘

    With retry (Phase 4):
    ┌─────────────────────────────────┐
    │  Worker-2 timeout detected      │
    │  → Reassign worker-2's files    │
    │    to worker-1 and worker-3     │
    │  → Re-dispatch to surviving     │
    │    workers only                  │
    │  → If 2nd attempt fails too     │
    │    → propagate error to client  │
    └─────────────────────────────────┘
```

**Fault tolerance is bounded**: max 1 retry. If the retry also fails, error propagates. No infinite retry loops.

---

## Testing & Benchmark Strategy

### Infrastructure
- **3-node cluster**: Nodes 1, 2, 3 (c6a.4xlarge, 32GB each)
  - node-1: 35.91.109.194
  - node-2: 52.25.144.154
  - node-3: 34.219.58.112
- **Single-node baseline**: Node-0 (35.80.117.84) — unchanged for comparison
- **AMI**: ami-09de3b0cbe9d198d1 (Java 25, Rust 1.94, full build)
- **SSH key**: ~/.ssh/clickbench-key.pem

### Benchmark Queries
- 43 ClickBench queries (existing in `benchmark/run-clickbench.sh`)
- Categories:
  - **Scan-heavy** (Q1-Q5): expect ~3x speedup with 3 nodes
  - **Aggregation** (Q6-Q33): expect 1.5-2.5x speedup
  - **High-cardinality GROUP BY** (Q34-Q36): OOM on single node, may work distributed
  - **Complex** (Q37-Q43): mixed expectations

### Metrics Tracked Per Phase
| Metric | Phase 1 | Phase 2 | Phase 3 | Phase 4 |
|--------|---------|---------|---------|---------|
| Queries passing | Q1-Q5 | Q1-Q43 | Q1-Q43 + joins | Q1-Q43 + joins |
| 3-node vs 1-node speedup | 2-3x (scan) | 1.5-2.5x (agg) | 2-3x (all) | 2-3x (all) |
| Correctness vs DF-CLI | 100% | 100% | 100% | 100% |
| Failure recovery | None | None | None | Auto-retry |

### Automated Benchmark Script
Extend existing `run-clickbench.sh` to:
- Accept `--compare-baseline` flag (runs both single-node and 3-node)
- Output speedup ratios per query
- Generate markdown report

---

## Cluster Setup Task (Pre-Implementation)

### 3-Node Cluster Setup Steps
1. SSH into all 3 nodes, clean old data
2. Configure `opensearch.yml` on each node:
   - `cluster.name: lakehouse-distributed`
   - `node.name: node-{1,2,3}`
   - `discovery.seed_hosts: [35.91.109.194, 52.25.144.154, 34.219.58.112]`
   - `cluster.initial_cluster_manager_nodes: [node-1]`
3. Start OpenSearch on all nodes
4. Verify cluster health: `GET _cat/nodes`
5. Register Glue catalog + hits_s3 table
6. Run single baseline benchmark to verify cluster works
7. Run ClickBench and compare with single-node baseline (node-0)

---

## Risk Analysis

| Risk | Mitigation |
|------|------------|
| Transport drops responses | Pull-based exchange with retry (Phase 3); Phase 4 adds worker reassignment. Error propagates to client (no silent fallback). |
| Worker OOM on large queries | 4GB JVM + FairSpill DataFusion; memory-aware scheduling in Phase 4 |
| Credential expiry during long queries | Refresh credentials before dispatch; 10-min buffer before expiry |
| Partial aggregation incorrect for AVG/MEDIAN | Rewrite AVG to SUM+COUNT; MEDIAN: deterministic single-node routing |
| COUNT DISTINCT not distributable (Phase 1-2) | QueryAnalyzer classifies → deterministic single-node routing. Phase 3 adds shuffle. |
| Uneven file sizes cause skew | Greedy bin-packing balances by total bytes, not file count |
| Network overhead exceeds parallelism benefit | Phase 4 CBO: skip distribution for tiny queries (<3 files or <100MB or <50K estimated rows) |
| Distributed execution error | Error propagates to client with context (which worker, what failed). No silent fallback to single-node. |

---

## Files to Modify (Existing)

| File | Change |
|------|--------|
| `LakehousePlugin.java` | Add node attribute, wire DistributedScanExecutor into prepareScan() |
| `LakehouseState.java` | Add TransportService reference, ClusterService reference |
| `build.gradle` | Add transport dependency (if needed) |

## Files to Create (New)

| File | Phase | Purpose |
|------|-------|---------|
| `distributed/NodeDiscovery.java` | 1 | Find eligible lakehouse worker nodes |
| `distributed/WorkerQueryAction.java` | 1 | Transport action for worker query execution |
| `distributed/WorkerQueryRequest.java` | 1 | Serializable request (SQL, files, creds) |
| `distributed/WorkerQueryResponse.java` | 1 | Arrow IPC byte[] response wrapper |
| `distributed/ArrowIpcSerializer.java` | 1 | VectorSchemaRoot ↔ byte[] via ArrowStreamWriter/Reader |
| `distributed/FilePartitioner.java` | 1 | Greedy bin-packing file→worker assignment |
| `distributed/DistributedScanExecutor.java` | 1 | Orchestrator: partition → dispatch → merge |
| `distributed/ResultMerger.java` | 1 | Arrow-native merge (CONCAT, GLOBAL_MERGE, TOPK) |
| `distributed/QueryAnalyzer.java` | 2 | RelNode inspection → QueryProfile + merge strategy |
| `distributed/SqlRewriter.java` | 2 | Rewrite SQL for partial aggregation on workers |
| `distributed/ExchangeService.java` | 3 | Arrow IPC inter-node shuffle over transport |
| `distributed/HashPartitioner.java` | 3 | Hash rows by key columns for shuffle routing |
| `distributed/PlanFragmenter.java` | 3 | Split RelNode into multi-stage DAG |
| `distributed/StageScheduler.java` | 3 | Topological stage execution with dependencies |

---

## Verification Plan

### After Each PR
1. `./gradlew :sandbox:plugins:lakehouse-iceberg:test` — all unit tests pass
2. `./gradlew :sandbox:plugins:lakehouse-iceberg:internalClusterTest` — integration tests pass

### After Phase 1
1. Deploy to 3-node cluster
2. Run `benchmark/run-clickbench.sh` for Q1-Q10
3. Compare latency vs single-node baseline (node-0)
4. Verify row counts match single-node exactly

### After Phase 2
1. Run full ClickBench (Q1-Q43) on 3-node cluster
2. Run correctness check vs DataFusion CLI
3. Compare speedup ratios across query categories
4. Document results in benchmark report

### After Phase 3-4
1. Multi-table join queries (new test dataset needed)
2. Chaos testing: kill a worker mid-query, verify retry/fallback
3. Memory stress testing: concurrent queries on cluster

---

## ClickBench 43-Query Execution Matrix

Legend for distributed strategy:
- **1N** = Single-node fallback (same as current single-node execution)
- **FP+C** = File-partition + Concat (workers return rows, coordinator concatenates)
- **FP+GM** = File-partition + Global Merge (workers return partial aggs, coordinator re-aggregates)
- **FP+TK** = File-partition + Top-K Merge (workers return local top-K, coordinator merge-sorts + limits)
- **FP+PM** = File-partition + Partial Merge (workers return partial GROUP BY, coordinator merges groups)
- **SH** = Shuffle (hash-repartition by GROUP BY/DISTINCT key across nodes, then local agg)

### Global Aggregation Queries (no GROUP BY)

| Q# | SQL Summary | Agg Functions | Phase 1 | Phase 2 | Final |
|----|-------------|---------------|---------|---------|-------|
| Q1 | `COUNT(*)` | COUNT | FP+GM: SUM worker counts | FP+GM | FP+GM |
| Q2 | `COUNT(*) WHERE advEngineId<>0` | COUNT+filter | FP+GM: SUM worker counts | FP+GM | FP+GM |
| Q3 | `SUM(advEngineId), COUNT(*), AVG(resWidth)` | SUM,COUNT,AVG | FP+GM: SUM sums, SUM counts, recompute AVG | FP+GM | FP+GM |
| Q4 | `AVG(userId)` | AVG | FP+GM: workers send SUM+COUNT, coord computes | FP+GM | FP+GM |
| Q5 | `COUNT(DISTINCT userId)` | COUNT DISTINCT | **1N** (distinct not mergeable) | **1N** | **SH**: hash-partition userIds, local distinct per partition |
| Q6 | `COUNT(DISTINCT searchPhrase)` | COUNT DISTINCT | **1N** | **1N** | **SH**: hash-partition phrases, local distinct |
| Q7 | `MIN(eventDate), MAX(eventDate)` | MIN,MAX | FP+GM: MIN of MINs, MAX of MAXs | FP+GM | FP+GM |
| Q21 | `COUNT(*) WHERE url LIKE '%google%'` | COUNT+filter | FP+GM: SUM worker counts | FP+GM | FP+GM |
| Q30 | `SUM(resWidth), SUM(resWidth+1), ...(x90)` | 90 SUMs | FP+GM: SUM each of 90 partial sums | FP+GM | FP+GM |

### Scan / Filter / Sort Queries (no aggregation)

| Q# | SQL Summary | Pattern | Phase 1 | Phase 2 | Final |
|----|-------------|---------|---------|---------|-------|
| Q20 | `WHERE userId = X` (point lookup) | Filter+Project | FP+C: concat worker rows | FP+C | FP+C |
| Q24 | `SELECT * WHERE url LIKE '%google%' ORDER BY eventTime LIMIT 10` | Filter+Sort+Limit | FP+TK: each worker returns 10, coord merge-sorts | FP+TK | FP+TK |
| Q25 | `searchPhrase WHERE <> '' ORDER BY eventTime LIMIT 10` | Filter+Sort+Limit | FP+TK | FP+TK | FP+TK |
| Q26 | `searchPhrase WHERE <> '' ORDER BY searchPhrase LIMIT 10` | Filter+Sort+Limit | FP+TK | FP+TK | FP+TK |
| Q27 | `searchPhrase WHERE <> '' ORDER BY eventTime, searchPhrase LIMIT 10` | Filter+Sort+Limit(2 keys) | FP+TK | FP+TK | FP+TK |

### GROUP BY + Distributable Aggregates (SUM, COUNT, AVG, MIN, MAX)

These queries have GROUP BY with only distributable aggregate functions (no COUNT DISTINCT). Workers execute partial GROUP BY on their file subsets, coordinator merges by group key.

| Q# | SQL Summary | GROUP BY Cardinality | Aggs | Phase 1 | Phase 2 | Final |
|----|-------------|---------------------|------|---------|---------|-------|
| Q8 | `advEngineId, COUNT(*) ORDER BY c DESC` | Low (~20 engines) | COUNT | **1N** | FP+PM: merge COUNTs per engine | FP+PM |
| Q13 | `searchPhrase, COUNT(*) WHERE <> '' ORDER BY c DESC LIMIT 10` | Medium (~10M phrases) | COUNT | **1N** | FP+PM: merge COUNTs per phrase, re-sort top-10 | FP+PM |
| Q15 | `searchEngineId, searchPhrase, COUNT(*) WHERE <> '' ORDER BY c DESC LIMIT 10` | Medium | COUNT | **1N** | FP+PM | FP+PM |
| Q16 | `userId, COUNT(*) ORDER BY c DESC LIMIT 10` | High (~17M users) | COUNT | **1N** | FP+PM: each worker returns top-K by count, coord merges | FP+PM |
| Q17 | `userId, searchPhrase, COUNT(*) ORDER BY c DESC LIMIT 10` | Very high | COUNT | **1N** | FP+PM | FP+PM |
| Q18 | `userId, searchPhrase, COUNT(*) LIMIT 10` | Very high (no ORDER BY) | COUNT | **1N** | FP+PM: merge then take any 10 | FP+PM |
| Q19 | `userId, EXTRACT(MIN FROM eventTime), searchPhrase, COUNT(*) ORDER BY c DESC LIMIT 10` | Very high (3-key) | COUNT | **1N** | FP+PM | FP+PM |
| Q22 | `searchPhrase, MIN(url), COUNT(*) WHERE url LIKE ORDER BY c DESC LIMIT 10` | Medium | MIN,COUNT | **1N** | FP+PM: merge MINs and COUNTs | FP+PM |
| Q28 | `counterId, AVG(CHAR_LENGTH(url)), COUNT(*) HAVING COUNT>100K ORDER BY l DESC LIMIT 25` | Low (~7K counters) | AVG,COUNT+HAVING | **1N** | FP+PM: SUM+COUNT for AVG, apply HAVING on coord | FP+PM |
| Q29 | `SUBSTRING(referer...), AVG(CHAR_LENGTH(referer)), COUNT(*), MIN(referer) HAVING>100K` | Medium | AVG,COUNT,MIN+HAVING | **1N** | FP+PM: complex expr in GROUP BY key, merge on coord | FP+PM |
| Q31 | `searchEngineId, clientIp, COUNT(*), SUM(isRefresh), AVG(resWidth) WHERE <> '' ORDER BY c DESC LIMIT 10` | Medium | COUNT,SUM,AVG | **1N** | FP+PM | FP+PM |
| Q32 | `watchId, clientIp, COUNT(*), SUM(isRefresh), AVG(resWidth) WHERE <> '' ORDER BY c DESC LIMIT 10` | High (watchId) | COUNT,SUM,AVG | **1N** | FP+PM | FP+PM |
| Q33 | `watchId, clientIp, COUNT(*), SUM(isRefresh), AVG(resWidth) ORDER BY c DESC LIMIT 10` | Very high (no filter) | COUNT,SUM,AVG | **1N** | FP+PM: heavy — large hash tables per worker | FP+PM |
| Q34 | `url, COUNT(*) ORDER BY c DESC LIMIT 10` | **~100M unique URLs** | COUNT | **1N** (OOM) | FP+PM: 3 workers each handle 1/3 files — may fit! | **SH**: hash URLs across nodes, each node handles subset of URL space |
| Q35 | `1, url, COUNT(*) ORDER BY c DESC LIMIT 10` | **~100M unique URLs** | COUNT | **1N** (OOM) | FP+PM: same as Q34 | **SH** |
| Q36 | `clientIp, clientIp-1, clientIp-2, clientIp-3, COUNT(*) ORDER BY c DESC LIMIT 10` | High (~30M IPs) | COUNT | **1N** | FP+PM | FP+PM |

### GROUP BY + Counter=62 Filtered Queries (small result sets)

These queries filter on `counterId=62 AND eventDate` range, producing tiny intermediate sets (~10K-100K rows). Distribution overhead may exceed benefit — cost-based routing in Phase 4 may choose single-node.

| Q# | SQL Summary | Aggs | Phase 1 | Phase 2 | Final (Phase 4) |
|----|-------------|------|---------|---------|-----------------|
| Q37 | `url, COUNT(*) WHERE counter=62 AND date range AND dontcount=0 AND refresh=0 ORDER BY DESC LIMIT 10` | COUNT | **1N** | FP+PM | CBO: likely 1N (tiny after filter) |
| Q38 | `title, COUNT(*) WHERE counter=62 AND date range AND dontcount=0 AND refresh=0 ORDER BY DESC LIMIT 10` | COUNT | **1N** | FP+PM | CBO: likely 1N |
| Q39 | `url, COUNT(*) WHERE counter=62 AND date range AND refresh=0 AND isLink<>0 ORDER BY DESC LIMIT 10` | COUNT | **1N** | FP+PM | CBO: likely 1N |
| Q40 | `trafficSrcId, searchEngineId, advEngineId, CASE..., url, COUNT(*) WHERE counter=62 ... ORDER BY DESC LIMIT 10` | COUNT | **1N** | FP+PM | CBO: likely 1N |
| Q41 | `urlHash, eventDate, COUNT(*) WHERE counter=62 AND ... AND trafficSrc IN (-1,6) AND refererHash=X ORDER BY DESC LIMIT 10` | COUNT | **1N** | FP+PM | CBO: likely 1N |
| Q42 | `windowWidth, windowHeight, COUNT(*) WHERE counter=62 AND ... AND urlHash=X ORDER BY DESC LIMIT 10` | COUNT | **1N** | FP+PM | CBO: likely 1N |
| Q43 | `FLOOR(eventTime TO MINUTE), COUNT(*) WHERE counter=62 AND date='2013-07-15/16' ORDER BY M LIMIT 10` | COUNT | **1N** | FP+PM | CBO: likely 1N |

### GROUP BY + COUNT(DISTINCT) Queries (require shuffle for correctness)

These queries contain COUNT(DISTINCT) within a GROUP BY. Without shuffle, the same distinct value may appear on multiple workers, making simple merge incorrect. Phase 2 falls back to single-node; Phase 3 adds shuffle.

| Q# | SQL Summary | Aggs | Phase 1 | Phase 2 | Final (Phase 3+) |
|----|-------------|------|---------|---------|-------------------|
| Q9 | `regionId, COUNT(DISTINCT userId) ORDER BY u DESC LIMIT 10` | COUNT DISTINCT | **1N** | **1N** (DISTINCT not mergeable) | **SH**: hash-partition by userId, local COUNT(DISTINCT) per region |
| Q10 | `regionId, SUM(advEngineId), COUNT(*), AVG(resWidth), COUNT(DISTINCT userId) ORDER BY c DESC LIMIT 10` | SUM,COUNT,AVG,COUNT DISTINCT | **1N** | **1N** (has DISTINCT) | **SH**: shuffle for DISTINCT, merge for SUM/COUNT/AVG |
| Q11 | `mobilePhoneModel, COUNT(DISTINCT userId) WHERE <> '' ORDER BY u DESC LIMIT 10` | COUNT DISTINCT | **1N** | **1N** | **SH** |
| Q12 | `mobilePhone, mobilePhoneModel, COUNT(DISTINCT userId) WHERE <> '' ORDER BY u DESC LIMIT 10` | COUNT DISTINCT | **1N** | **1N** | **SH** |
| Q14 | `searchPhrase, COUNT(DISTINCT userId) WHERE <> '' ORDER BY u DESC LIMIT 10` | COUNT DISTINCT | **1N** | **1N** | **SH** |
| Q23 | `searchPhrase, MIN(url), MIN(title), COUNT(*), COUNT(DISTINCT userId) WHERE ... ORDER BY c DESC LIMIT 10` | MIN,COUNT,COUNT DISTINCT | **1N** | **1N** | **SH**: shuffle for DISTINCT, merge for MIN/COUNT |

### Summary: Query Coverage Per Phase

| Phase | Distributed | Single-Node Fallback | Total Correct |
|-------|-------------|---------------------|---------------|
| **Phase 1** (Walking Skeleton) | 14 queries (Q1-Q4, Q7, Q20-Q21, Q24-Q27, Q30) | 29 queries (all GROUP BY, all DISTINCT) | 43/43 |
| **Phase 2** (Smart Aggregation) | 35 queries (+Q8, Q13, Q15-Q19, Q22, Q28-Q29, Q31-Q43) | 8 queries (Q5-Q6, Q9-Q12, Q14, Q23 — all have COUNT DISTINCT) | 43/43 |
| **Phase 3** (Shuffle) | 43 queries (+Q5-Q6, Q9-Q12, Q14, Q23 via shuffle) | 0 | 43/43 |
| **Phase 4** (CBO) | 43 queries (cost-based routing: small queries may choose 1N) | 0 (CBO may choose 1N when overhead > benefit) | 43/43 |

### Expected Speedup Categories (3-node cluster vs single-node)

| Category | Queries | Phase 2+ Expected Speedup | Why |
|----------|---------|--------------------------|-----|
| **Scan-heavy** (full table, simple agg) | Q1-Q4, Q7, Q30 | **2.5-3x** | I/O bound, near-linear with nodes |
| **Filter+Scan** (selective filter) | Q2, Q20-Q21 | **2-3x** | Predicate pushdown + parallel scan |
| **Sort+Limit** (full scan + sort) | Q24-Q27 | **2-3x** | Each node sorts 1/3 data, coord merges tiny results |
| **Medium GROUP BY** (moderate cardinality) | Q8, Q13, Q15, Q22, Q28-Q29, Q31 | **1.5-2.5x** | Partial agg reduces coordinator work |
| **High-cardinality GROUP BY** | Q16-Q19, Q32-Q33, Q36 | **1.5-2x** | Large hash tables, but split across workers |
| **OOM queries** (Q34-Q35) | Q34, Q35 | **Fixes OOM → works** | 3 workers each build 1/3 of hash table |
| **Highly-selective filter** (counter=62) | Q37-Q43 | **~1x (no benefit)** | After filter, <100K rows — distribution overhead dominates |
| **COUNT DISTINCT** (needs shuffle) | Q5-Q6, Q9-Q12, Q14, Q23 | **1N until Phase 3**, then **1.5-2x** | Shuffle adds network cost, partially offset by parallel scan |

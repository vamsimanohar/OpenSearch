# Distributed Query Engine — Fixed Nodes (Server Variant)

## Architecture Overview

```
+---------------------------------------------------------------+
|                     JAVA COORDINATOR                           |
|                                                                |
|  +---------------+ +----------------+ +---------------------+  |
|  | Query         | | Substrait      | | Stage Splitter      |  |
|  | Frontends     | | Producer       | | (insert exchanges)  |  |
|  | (SQL, Rel)    | |                | |                     |  |
|  +-------+-------+ +-------+--------+ +----------+----------+  |
|          |                 |                      |             |
|  +-------v-----------------v----------------------v----------+  |
|  |                Distributed Scheduler                      |  |
|  |  +------------+ +------------+ +----------------------+   |  |
|  |  | Query      | | Worker     | | Stage                |   |  |
|  |  | Queue +    | | Registry + | | Sequencer            |   |  |
|  |  | Resource   | | Health     | | (DAG execution)      |   |  |
|  |  | Groups     | | Monitor    | |                      |   |  |
|  |  +------------+ +------------+ +----------------------+   |  |
|  |  +------------+ +------------------------------------+    |  |
|  |  | Partition  | | Memory + Resource Accounting       |    |  |
|  |  | Assigner   | |                                    |    |  |
|  |  +------------+ +------------------------------------+    |  |
|  +-----------------------------------------------------------+  |
|          | gRPC (Substrait fragments + assignments)              |
+----------+------------------------------------------------------+
           |
           |  Arrow Flight (shuffle) between workers
           v
+---------------------------------------------------------------+
|                   RUST WORKER (DataFusion)                      |
|                                                                |
|  +------------------+  +----------------------------------+    |
|  | gRPC Server      |  | Substrait Consumer               |    |
|  | (receive tasks)  |  | (Substrait -> DataFusion plan)   |    |
|  +--------+---------+  +---------------+------------------+    |
|           |                            |                       |
|  +--------v----------------------------v-------------------+   |
|  |              DataFusion Execution Engine                 |   |
|  |  +---------+ +----------+ +---------+ +--------------+  |   |
|  |  | Parquet | | Custom   | | Morsel  | | Memory       |  |   |
|  |  | Reader  | | UDFs     | | Pipeline| | Manager      |  |   |
|  |  | (S3)    | |          | |         | |              |  |   |
|  |  +---------+ +----------+ +---------+ +--------------+  |   |
|  +----------------------------------------------------------+  |
|  +----------------------------------------------------------+  |
|  | Shuffle I/O                                               |  |
|  |  Arrow Flight server (send) + client (receive)            |  |
|  +----------------------------------------------------------+  |
+---------------------------------------------------------------+
```

---

## Phase 1: Single-Node Proof of Concept

Goal: Your SQL -> Substrait -> DataFusion -> correct results on ONE machine.

### 1.1 Define Substrait Extensions for Custom Functions

Catalog every custom function in your current engine.
Write YAML definitions with exact type signatures.

```yaml
# extensions/your_company_functions.yaml
scalar_functions:
  - name: "your_custom_hash"
    impls:
      - args:
          - value: string
          - value: i32
        return: i64

  - name: "geo_distance"
    impls:
      - args:
          - value: fp64
          - value: fp64
          - value: fp64
          - value: fp64
        return: fp64

aggregate_functions:
  - name: "approx_percentile"
    impls:
      - args:
          - value: fp64
          - value: fp64
        return: fp64
```

Validate all extensions:
```bash
substrait-validator your_plan.json --out-file report.html
```

### 1.2 Substrait Producer (Java)

Use the substrait-java library.

Both query frontends (your SQL variant + your relational language) produce
the SAME Substrait plan format. This is the whole point of Substrait as
the common IR.

```
Your SQL variant -----> Parser -> Logical Plan --+
                                                  +--> Substrait Plan (protobuf)
Your Relational Lang -> Parser -> Logical Plan --+
```

### 1.3 Rust Worker Binary

Minimal DataFusion setup:

- Substrait consumer: deserialize protobuf -> DataFusion LogicalPlan
- Register all custom UDFs/UDAFs in DataFusion
- Wire up S3 object_store for Parquet reading (using object_store crate)
- Execute plan, return Arrow RecordBatches

### 1.4 Dual Execution Testing

Run same queries on BOTH old Java engine AND new DataFusion worker.
Compare results row-by-row. Build regression test suite.

Deliverable: Java CLI -> Substrait plan -> Rust binary -> Arrow results

---

## Phase 2: Coordinator + Multi-Worker

Goal: Distribute queries across fixed worker nodes.

### 2.1 Stage Splitter (Java)

Walk the Substrait plan tree. At each node, decide if data needs to move
between machines. If yes, insert an exchange boundary = stage break.

Rules:

```
RULE 1: Aggregate with GROUP BY
  -> insert HASH EXCHANGE before it (partition by grouping keys)
  -> so all rows for same group land on same worker

  BEFORE:                        AFTER:
  Aggregate(GROUP BY region)     Stage 2: Aggregate(GROUP BY region)
    Scan(orders)                 ---- hash exchange(region) ----
                                 Stage 1: Scan(orders)


RULE 2: Hash Join / Merge Join
  -> insert HASH EXCHANGE on BOTH inputs (partition by join key)
  UNLESS one side is small (< threshold, e.g. 10MB)
  -> then BROADCAST the small side to all workers

  BEFORE:                        AFTER (large-large):
  Join(a.id = b.id)              Stage 3: Join(a.id = b.id)
    Scan(a)                      ---- hash exchange(id) ---- (from Stage 1)
    Scan(b)                      ---- hash exchange(id) ---- (from Stage 2)
                                 Stage 1: Scan(a)
                                 Stage 2: Scan(b)

  AFTER (small-large):
  Stage 2: Join(a.id = b.id)
  ---- broadcast exchange ---- (from Stage 1, small table)
  Stage 2 also does: Scan(b)    (partitioned, no exchange)
  Stage 1: Scan(a)              (small table, read fully)


RULE 3: Sort + Limit at root (ORDER BY ... LIMIT N)
  -> insert GATHER EXCHANGE (send everything to one node)
  -> optimization: each worker does local top-N first,
     then gather only top-N from each worker

  BEFORE:                        AFTER:
  Limit(10)                      Stage 2: Limit(10)
    Sort(revenue DESC)                     Sort(revenue DESC)
      Aggregate(...)             ---- gather exchange ----
                                 Stage 1: Limit(10)
                                           Sort(revenue DESC)
                                           Aggregate(...)


RULE 4: Everything else (filter, project, scan)
  -> NO exchange needed
  -> runs partition-parallel within a stage
```

Output: a DAG of stages. Each stage is a Substrait plan fragment.

```
Example DAG for:
  SELECT p.name, SUM(o.qty * o.price)
  FROM orders o JOIN products p ON o.product_id = p.id
  WHERE o.year = 2024
  GROUP BY p.name
  ORDER BY 2 DESC LIMIT 10

  Stage 1a: Scan(orders) + Filter(year=2024)
  Stage 1b: Scan(products)           -- small table
       |           |
       |     broadcast exchange
       |           |
  Stage 2: Join + Partial Aggregate
       |
       hash exchange(name)
       |
  Stage 3: Final Aggregate
       |
       gather exchange
       |
  Stage 4: Sort + Limit(10)          -- single node
```

### 2.2 Partition Assigner (Java)

```
INPUT:
  - Iceberg catalog tells us: 500 parquet files for this table
  - File sizes from Iceberg metadata (e.g., 50MB to 200MB each)
  - 3 workers, each with 16 slots = 48 total slots

STEP 1: Bin-pack files into tasks
  Target: ~128MB per task

  files sorted by size descending:
    file_100.parquet  200MB -> Task 0 (200MB, done)
    file_042.parquet  190MB -> Task 1 (190MB, done)
    file_007.parquet  180MB -> Task 2 (180MB, done)
    file_099.parquet   80MB -> Task 0 already full... Task 3 (80MB)
    file_055.parquet   60MB -> Task 3 (140MB, done)
    file_012.parquet   50MB -> Task 4 (50MB)
    file_088.parquet   50MB -> Task 4 (100MB)
    ...

  Result: ~30 tasks, each ~128MB

STEP 2: Assign tasks to workers
  Check slot availability per worker:
    Worker A: 12 free slots (4 busy with other queries)
    Worker B: 16 free slots (idle)
    Worker C:  8 free slots (8 busy)

  Round-robin across workers with free slots:
    Task 0  -> Worker B
    Task 1  -> Worker A
    Task 2  -> Worker C
    Task 3  -> Worker B
    Task 4  -> Worker A
    Task 5  -> Worker C
    ...

  (With locality-aware optimization: if Worker A has
   file_042.parquet in its local SSD cache, prefer assigning
   Task 1 to Worker A)

STEP 3: Attach file list to Substrait plan
  Each task's Substrait ReadRel gets an advancedExtension:

  {
    "read": {
      "namedTable": { "names": ["orders"] },
      "baseSchema": { ... },
      "advancedExtension": {
        "enhancement": {
          "@type": "your.company/ScanAssignment",
          "files": [
            "s3://bucket/orders/year=2024/part-042.parquet",
            "s3://bucket/orders/year=2024/part-055.parquet"
          ],
          "icebergSnapshotId": 123456789
        }
      }
    }
  }
```

### 2.3 gRPC Protocol: Coordinator <-> Workers

```protobuf
service WorkerService {
  // Coordinator sends a task to a worker
  rpc ExecuteTask(TaskRequest) returns (stream TaskStatus);

  // Coordinator cancels a running task
  rpc CancelTask(CancelRequest) returns (CancelResponse);

  // Coordinator polls worker health
  rpc GetHealth(Empty) returns (HealthResponse);
}

message TaskRequest {
  string query_id = 1;
  string stage_id = 2;
  int32  task_id = 3;
  bytes  substrait_plan = 4;           // the stage's Substrait fragment
  repeated FileAssignment files = 5;   // what to scan (for leaf stages)
  ShuffleOutputConfig shuffle_out = 6; // where to send output
  repeated ShuffleInput shuffle_in = 7;// where to read input (non-leaf)
}

message FileAssignment {
  string file_path = 1;               // s3://bucket/path/file.parquet
  int64  file_size_bytes = 2;
  optional int64 offset = 3;          // for splitting large files
  optional int64 length = 4;
}

message ShuffleOutputConfig {
  enum PartitionMethod {
    HASH = 0;
    BROADCAST = 1;
    GATHER = 2;                        // send all to one target
  }
  PartitionMethod method = 1;
  repeated int32 partition_keys = 2;   // column indices to hash on
  int32 num_output_partitions = 3;
  repeated WorkerEndpoint targets = 4; // Arrow Flight endpoints
}

message ShuffleInput {
  string source_stage_id = 1;
  repeated WorkerEndpoint sources = 2; // Arrow Flight endpoints to read from
  int32 partition_id = 3;             // which partition this worker reads
}

message WorkerEndpoint {
  string host = 1;
  int32  arrow_flight_port = 2;
}

message TaskStatus {
  enum State {
    RUNNING = 0;
    COMPLETED = 1;
    FAILED = 2;
  }
  State state = 1;
  int64 rows_processed = 2;
  int64 bytes_read = 3;
  optional string error_message = 4;
}

message HealthResponse {
  int32 total_slots = 1;
  int32 free_slots = 2;
  int64 memory_used_bytes = 3;
  int64 memory_total_bytes = 4;
  repeated string running_query_ids = 5;
}
```

### 2.4 Shuffle via Arrow Flight

Each worker runs an Arrow Flight server on a dedicated port.

```
HOW HASH SHUFFLE WORKS:

Stage 1, Worker X finishes executing its fragment.
Output: stream of Arrow RecordBatches.

For each batch:
  1. Compute hash(partition_keys) % num_stage2_workers for each row
  2. Split batch into N sub-batches (one per Stage 2 target)
  3. Stream each sub-batch to the correct target via Arrow Flight DoExchange

  Worker X output batch (1000 rows):
    hash(region) % 3 == 0 -> 340 rows -> Arrow Flight -> Worker A:9001
    hash(region) % 3 == 1 -> 330 rows -> Arrow Flight -> Worker B:9001
    hash(region) % 3 == 2 -> 330 rows -> Arrow Flight -> Worker C:9001

Stage 2, Worker A starts:
  1. Arrow Flight server receives streams from ALL Stage 1 workers
  2. Buffers incoming batches (or starts processing immediately if pipelined)
  3. Feeds them as input partitions to DataFusion
  4. DataFusion executes Stage 2 fragment (e.g., final aggregate)


HOW BROADCAST SHUFFLE WORKS:

Stage 1 has one worker that reads the small table fully.
Output: the complete small table as Arrow batches.

  Worker X -> Arrow Flight -> Worker A (full copy)
  Worker X -> Arrow Flight -> Worker B (full copy)
  Worker X -> Arrow Flight -> Worker C (full copy)

Each Stage 2 worker now has the full small table in memory
for the broadcast join.


HOW GATHER WORKS:

All Stage N workers send all their output to one designated worker.

  Worker A -> Arrow Flight -> Worker Z
  Worker B -> Arrow Flight -> Worker Z
  Worker C -> Arrow Flight -> Worker Z

Worker Z does: final sort, final limit, return to coordinator.
```

### 2.5 Stage Sequencer (Java)

Manages the DAG of stages for each running query.

```
State machine per query:

  QUEUED -> PLANNING -> EXECUTING -> COMPLETED
                           |
                        FAILED

Within EXECUTING, the stage DAG:

  Stage 1a: RUNNING -+
                      +-> when both done -> Stage 2: RUNNING
  Stage 1b: RUNNING -+                         |
                                           when done
                                                |
                                           Stage 3: RUNNING
                                                |
                                           when done
                                                |
                                           Stage 4: RUNNING
                                                |
                                           COMPLETED

Rules:
  - A stage launches when ALL its dependencies are COMPLETED
  - If any task in a stage fails: retry up to 3 times on a different worker
  - If a stage fails after retries: mark the query as FAILED
  - Speculative execution: if one task is 3x slower than the median
    for that stage, launch a duplicate on another worker, take whichever
    finishes first
```

### 2.6 Multi-Query Scheduling

The cluster handles many queries concurrently.

```
RESOURCE GROUPS (configurable):
  +------------------------------------------+
  | "interactive": max 20 concurrent queries  |
  |   max 40% of cluster slots               |
  |   priority: HIGH                          |
  +------------------------------------------+
  | "batch": max 50 concurrent queries        |
  |   max 60% of cluster slots               |
  |   priority: LOW                           |
  +------------------------------------------+
  | "admin": max 5 concurrent queries         |
  |   max 100% of cluster slots (override)   |
  |   priority: HIGHEST                       |
  +------------------------------------------+

SLOT ALLOCATION:
  48 total slots in cluster (3 workers x 16)
  Interactive: up to 19 slots
  Batch: up to 29 slots

  When a slot frees up:
    1. Check highest priority group with queued tasks
    2. Assign slot to that group's next task

MEMORY ACCOUNTING:
  Each worker: 64GB RAM
  Per-query memory limit: 8GB per worker (configurable)
  If a query exceeds limit:
    Option 1: spill to local disk (DataFusion supports this)
    Option 2: kill the query with OOM error
  Coordinator tracks memory reservations to avoid over-committing
```

---

## Phase 3: Production Hardening

### 3.1 Fault Tolerance

```
WORKER CRASH:
  - Coordinator detects via heartbeat timeout (e.g., 10s)
  - All tasks on that worker: mark as FAILED
  - Re-assign those tasks to other workers with free slots
  - For stages that depend on the dead worker's shuffle output:
    must re-run the upstream stage too (shuffle data is lost)

COORDINATOR CRASH:
  - Persist query state + stage DAG to durable store (Postgres, etcd)
  - New coordinator instance picks up in-flight queries
  - Workers continue executing; coordinator reconnects and resumes tracking
  - Queries in mid-shuffle may need to restart from last completed stage

STRAGGLER MITIGATION:
  - Track task completion times within each stage
  - If a task takes > 3x median: launch speculative copy on another worker
  - First to finish wins; cancel the other
```

### 3.2 Observability

```
PER-QUERY PROFILE:
  {
    "query_id": "q-12345",
    "sql": "SELECT ...",
    "stages": [
      {
        "stage_id": "s1",
        "tasks": 12,
        "wall_time_ms": 450,
        "rows_in": 10000000,
        "rows_out": 50000,
        "bytes_read": 1073741824,
        "bytes_shuffled": 4194304,
        "peak_memory_bytes": 536870912,
        "workers": ["w1", "w2", "w3"]
      },
      ...
    ],
    "total_wall_time_ms": 1200
  }

METRICS (export to Prometheus/Grafana):
  - cluster.slots.total, cluster.slots.used
  - cluster.queries.active, cluster.queries.queued
  - worker.{id}.cpu_percent, worker.{id}.memory_used
  - query.stage.duration_histogram
  - shuffle.bytes_transferred

DISTRIBUTED TRACING (OpenTelemetry):
  - Trace spans from coordinator through each stage and task
  - See exactly where time is spent per query
```

### 3.3 Iceberg-Specific Optimizations

```
1. PARTITION PRUNING
   Iceberg partition spec: partitioned by year, month
   Query has WHERE year = 2024 AND month = 3
   -> only read files in that partition (skip 90%+ of files)
   -> done in coordinator before assigning files to tasks

2. COLUMN STATS PRUNING
   Iceberg stores min/max per column per file
   Query has WHERE price > 1000
   -> skip files where max(price) < 1000

3. ROW GROUP PRUNING
   Parquet files have internal row groups with their own stats
   -> DataFusion's ParquetExec already does this automatically

4. PREDICATE PUSHDOWN
   Push filters into the Parquet reader
   -> DataFusion does this when you set the filter on ParquetExec

5. TIME TRAVEL
   Pass Iceberg snapshot ID in Substrait advancedExtension
   -> coordinator resolves snapshot -> file list at that point in time
```

---

## Component Checklist

```
JAVA COORDINATOR:
  [ ] Query frontends (SQL parser, relational language parser)
  [ ] Substrait producer (substrait-java)
  [ ] Iceberg catalog client (file listing, stats, partition pruning)
  [ ] Stage splitter (walk plan, insert exchanges)
  [ ] Partition assigner (bin-pack files into tasks)
  [ ] Stage sequencer (DAG execution, dependency tracking)
  [ ] Query queue + resource groups
  [ ] Worker registry + health monitoring
  [ ] Memory accounting
  [ ] gRPC client (send tasks to workers)
  [ ] Query result collector (receive final results)
  [ ] Observability (query profiles, metrics, tracing)

RUST WORKER:
  [ ] gRPC server (receive tasks from coordinator)
  [ ] Substrait consumer (deserialize -> DataFusion LogicalPlan)
  [ ] Custom UDF/UDAF registry
  [ ] S3 Parquet reader (object_store + DataFusion ParquetExec)
  [ ] Arrow Flight server (send shuffle output)
  [ ] Arrow Flight client (receive shuffle input)
  [ ] Memory manager (enforce per-task limits, spill to disk)
  [ ] Task executor (run DataFusion, report progress)
  [ ] Health reporter

SHARED:
  [ ] Substrait extension YAML files (all custom functions)
  [ ] gRPC protocol definitions (.proto files)
  [ ] Integration test suite (dual execution vs old engine)
  [ ] Benchmarks (TPC-H or your domain-specific queries)
```

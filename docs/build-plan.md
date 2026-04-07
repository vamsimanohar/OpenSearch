# Distributed Lakehouse Query Engine — Build Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a distributed lakehouse query engine on OpenSearch that executes PPL/SQL queries over Iceberg/Parquet data on S3, using SQL as the plan serialization format (with optional Substrait support) and DataFusion as the execution engine.

**Architecture:** Java coordinator (OpenSearch plugin) handles parsing, planning, and scheduling. Rust sidecar (DataFusion) executes query fragments on datawarehouse nodes. Communication via gRPC (task dispatch) and Arrow Flight (shuffle). SQL strings carry logical plans between Java and Rust (DataFusion parses SQL natively).

**Tech Stack:** Java 21, Rust (DataFusion 52.1.0), Apache Calcite, Apache Iceberg, gRPC/tonic, Arrow Flight, S3 (object_store). DataFusion's native SQL parser handles plan deserialization on the Rust side.

---

## Feature Dependency DAG

```
Phase 1: Proto + Rust Skeleton
    │
    ├──► Phase 2: Lakehouse Index + Catalog Connections
    │       │
    │       └──► Phase 3: Query Frontend Integration (Calcite ↔ Iceberg)
    │               │
    │               └──► Phase 4: SQL Producer (RelNode → SQL)
    │                       │
    ├───────────────────────┤
    │                       │
    │               Phase 5: Single-Worker End-to-End
    │                       │
    │                       ├──► Phase 6: Stage Splitter + Scheduler
    │                       │       │
    │                       │       └──► Phase 7: Shuffle (Arrow Flight)
    │                       │               │
    │                       │               └──► Phase 8: Multi-Worker E2E
    │                       │                       │
    │                       └──► Phase 9: Result Collector + Formatting
    │                                               │
    │                                       Phase 10: Hardening
    │                                       (Fault Tolerance, Observability, Security)
```

Each phase produces **testable, demo-able software**. PRs are scoped to individual tasks within a phase.

---

## Phase 1: Protocol Definitions + Rust Worker Skeleton

**Milestone:** A Rust binary starts, serves gRPC health checks, and the Java side can connect to it.

**Depends on:** Nothing (foundation layer)

**Design docs:** `07-grpc-protocol.md`, `08-rust-worker.md`

### Task 1.1: gRPC Proto Definitions

**Files:**
- Create: `proto/opensearch/lakehouse/worker.proto`
- Create: `proto/opensearch/lakehouse/iceberg_read_options.proto`

- [ ] Define `WorkerService` with RPCs: `ExecuteTask`, `CancelTask`, `GetWorkerStatus`, `RegisterUDF`, `Heartbeat`
- [ ] Define `TaskRequest`, `TaskProgress`, `TaskResult`, `WorkerStatusResponse` messages
- [ ] Define `IcebergReadOptions`, `DataFileEntry`, `ColumnStatistics` messages (from Component 2 §7)
- [ ] Generate Java stubs (protoc-grpc-java) and Rust stubs (tonic-build)
- [ ] Commit: `feat: add gRPC proto definitions with SQL-first TaskRequest`

**Test:** Proto compiles on both Java and Rust. Generated stubs importable.

### Task 1.2: Rust Worker Binary Skeleton

**Files:**
- Create: `opensearch-worker/Cargo.toml`
- Create: `opensearch-worker/src/main.rs`
- Create: `opensearch-worker/src/server.rs`
- Create: `opensearch-worker/src/config.rs`
- Create: `opensearch-worker/build.rs`

- [ ] Set up Cargo project with dependencies: `datafusion 52.1.0`, `tonic`, `arrow`, `object_store`, `tokio`
- [ ] Implement `main.rs`: CLI arg parsing (port, S3 config, memory limit), Tokio runtime setup
- [ ] Implement `server.rs`: Bare `WorkerService` impl with `GetWorkerStatus` returning node info and `Heartbeat` returning OK. Other RPCs return `UNIMPLEMENTED`
- [ ] Implement `config.rs`: `WorkerConfig` struct parsed from CLI args/env vars
- [ ] Write integration test: start binary, call `GetWorkerStatus` via gRPC, assert response
- [ ] Commit: `feat: rust worker skeleton with gRPC health check`

**Test:** `cargo test` passes. Binary starts and responds to gRPC health checks.

### Task 1.3: Java gRPC Client Wrapper

**Files:**
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/grpc/WorkerClient.java`
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/grpc/WorkerClientPool.java`
- Create: `lakehouse/src/test/java/.../grpc/WorkerClientTest.java`

- [ ] Implement `WorkerClient`: wraps a single gRPC channel to one worker, exposes `getStatus()`, `executeTask()`, `cancelTask()` methods
- [ ] Implement `WorkerClientPool`: manages `WorkerClient` instances per worker address, handles connection lifecycle
- [ ] Write unit test with a mock gRPC server that validates request/response round-trip
- [ ] Commit: `feat: java gRPC client for worker communication`

**Test:** Unit test with mock server passes.

### Task 1.4: Java-Rust Integration Smoke Test

- [ ] Write a test script that: starts Rust worker, uses Java `WorkerClient` to call `GetWorkerStatus`, asserts response contains expected fields
- [ ] Commit: `test: java-to-rust gRPC integration smoke test`

**Test:** End-to-end Java→Rust→Java round-trip works.

---

## Phase 2: Lakehouse Index Abstraction + Catalog Connections

**Milestone:** Users can create lakehouse indices via REST API, and the system can connect to Iceberg catalogs (Glue/Hive/REST).

**Depends on:** Nothing (independent of Phase 1)

**Design docs:** `00-lakehouse-index-abstraction.md`, `03-catalog-metadata.md`

### Task 2.1: LakehouseIndexMetadata + Cluster State

**Files:**
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/index/LakehouseIndexMetadata.java`
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/index/LakehouseIndicesMetadata.java`
- Create: `lakehouse/src/test/java/.../index/LakehouseIndexMetadataTests.java`

- [ ] Implement `LakehouseIndexMetadata` with Builder, `Writeable`, `ToXContentObject` (from doc §2)
- [ ] Implement `LakehouseIndicesMetadata` as `Metadata.Custom` container with `with()`, `without()`, `hasIndex()`
- [ ] Write serialization round-trip tests: build → writeTo → readFrom → assert equals
- [ ] Write XContent round-trip tests: build → toXContent → fromXContent → assert equals
- [ ] Commit: `feat: lakehouse index metadata model with cluster state serialization`

**Test:** `./gradlew :lakehouse:test --tests "*LakehouseIndexMetadata*"` passes.

### Task 2.2: Lakehouse Index REST API

**Files:**
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/rest/RestCreateLakehouseIndexAction.java`
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/rest/RestGetLakehouseIndexAction.java`
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/rest/RestDeleteLakehouseIndexAction.java`
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/rest/RestListLakehouseIndicesAction.java`
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/rest/RestRefreshLakehouseIndexAction.java`
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/action/LakehouseIndexService.java`

- [ ] Implement REST handlers for all 5 endpoints from doc §3 (PUT, GET, DELETE, POST _refresh, GET list)
- [ ] Implement `LakehouseIndexService` with cluster state update logic (submitStateUpdateTask)
- [ ] Validate: name conflicts with existing OpenSearch indices, required fields, S3 URI format
- [ ] Write REST integration tests: create, get, list, delete, verify cluster state
- [ ] Commit: `feat: REST API for lakehouse index CRUD`

**Test:** Integration test creates a lakehouse index, gets it, lists all, deletes it.

### Task 2.3: CatalogConnectionManager

**Files:**
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/catalog/CatalogConnectionManager.java`
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/catalog/GlueCatalogFactory.java`
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/catalog/HiveCatalogFactory.java`
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/catalog/RestCatalogFactory.java`
- Create: `lakehouse/src/test/java/.../catalog/CatalogConnectionManagerTests.java`

- [ ] Implement `CatalogConnectionManager` interface: `getCatalog()`, `validateConnection()`, `rotateCredentials()`, `getHealthStatus()`
- [ ] Implement factory for each catalog type (Glue, Hive, REST) reading credentials from OpenSearch keystore
- [ ] Connection pooling: one pool per `(catalogType, credentialsRef)` pair
- [ ] Write unit tests with mock keystore and mock Iceberg catalog
- [ ] Commit: `feat: catalog connection manager with Glue/Hive/REST support`

**Test:** Unit test creates mock catalogs, validates connection pooling and credential lookup.

### Task 2.4: SchemaCache

**Files:**
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/schema/SchemaCache.java`
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/schema/CaffeineSchemaCache.java`
- Create: `lakehouse/src/test/java/.../schema/SchemaCacheTests.java`

- [ ] Implement `SchemaCache` interface: `get()`, `put()`, `invalidate()`, `stats()`
- [ ] Implement `CaffeineSchemaCache` with per-entry TTL from `LakehouseIndexMetadata.schemaCacheTtlSeconds`
- [ ] Write tests: put + get, TTL expiry, invalidation, stats reporting
- [ ] Commit: `feat: schema cache with configurable per-index TTL`

**Test:** Cache hit/miss/expiry tests pass.

---

## Phase 3: Query Frontend Integration

**Milestone:** PPL/SQL queries targeting lakehouse indices produce a Calcite RelNode with Iceberg-backed table metadata. No execution yet.

**Depends on:** Phase 2 (needs catalog connections and index metadata)

**Design docs:** `01-query-frontends.md`

### Task 3.1: LakehouseCalciteSchema + LakehouseTable

**Files:**
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/schema/LakehouseCalciteSchema.java`
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/schema/LakehouseTable.java`
- Create: `lakehouse/src/test/java/.../schema/LakehouseCalciteSchemaTests.java`

- [ ] Implement `LakehouseCalciteSchema extends AbstractSchema`: wraps Iceberg `Catalog`, resolves tables by namespace
- [ ] Implement `LakehouseTable extends AbstractTable`: maps Iceberg schema to Calcite `RelDataType`
- [ ] Iceberg→Calcite type mapping: Boolean, Integer, Long, Float, Double, Decimal, String, Binary, Date, Time, Timestamp, Struct, List, Map, UUID
- [ ] Write unit tests: mock Iceberg catalog → LakehouseCalciteSchema.getTable() → verify RelDataType columns
- [ ] Commit: `feat: Calcite schema backed by Iceberg catalog`

**Test:** Unit test creates schema from mock Iceberg catalog, resolves table, verifies column types match.

### Task 3.2: IndexTypeResolver

**Files:**
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/index/IndexTypeResolver.java`
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/index/ClusterStateLakehouseIndexTypeResolver.java`
- Create: `lakehouse/src/test/java/.../index/IndexTypeResolverTests.java`

- [ ] Implement `IndexTypeResolver` interface with `resolve(indexName) → REGULAR | LAKEHOUSE | NOT_FOUND`
- [ ] Implement `ClusterStateLakehouseIndexTypeResolver`: reads from `ClusterState.Metadata.customs`
- [ ] Handle aliases (check if all alias targets are same type)
- [ ] Write tests: regular index → REGULAR, lakehouse index → LAKEHOUSE, missing → NOT_FOUND, alias resolution
- [ ] Commit: `feat: index type resolver for routing lakehouse vs regular queries`

**Test:** Resolver correctly distinguishes regular, lakehouse, and missing indices.

### Task 3.3: LakehouseContextFactory + LakehouseQueryRouter

**Files:**
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/LakehouseContextFactory.java`
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/LakehouseQueryRouter.java`
- Create: `lakehouse/src/test/java/.../LakehouseQueryRouterTests.java`

- [ ] Implement `LakehouseContextFactory.createContext()`: builds `UnifiedQueryContext` with `LakehouseCalciteSchema` registered under lakehouse catalog name
- [ ] Implement `LakehouseQueryRouter`: checks `IndexTypeResolver`, calls factory, runs `UnifiedQueryPlanner.plan()`, returns `PlannedQuery(RelNode, context)`
- [ ] Write test: mock lakehouse index, PPL query "source=orders | where amount > 100" → assert RelNode contains LogicalFilter over LogicalTableScan
- [ ] Write test: SQL query "SELECT * FROM orders WHERE amount > 100" → same RelNode structure
- [ ] Commit: `feat: lakehouse query router with Calcite planning integration`

**Test:** PPL and SQL queries both produce valid Calcite RelNode trees from Iceberg-backed schemas.

---

## Phase 4: SQL Producer

**Milestone:** A Calcite RelNode can be serialized to a SQL string targeting DataFusion's SQL dialect. Round-trip validated: Java produces SQL, Rust parses it natively.

**Depends on:** Phase 3 (needs RelNode from Calcite) + Phase 1 (proto definitions)

**Design docs:** `02-sql-producer.md`

### Task 4.1: DataFusion SQL Dialect

**Files:**
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/sql/DataFusionDialect.java`
- Create: `lakehouse/src/test/java/.../sql/DataFusionDialectTests.java`

- [ ] Implement dialect that handles DataFusion-specific SQL syntax (function names, type casts, identifier quoting)
- [ ] Function name mapping: Calcite standard names → DataFusion equivalents (e.g., `SUBSTRING` → `substr`, `CHAR_LENGTH` → `character_length`)
- [ ] Type cast syntax: DataFusion uses `CAST(x AS type)` and `::type` shorthand
- [ ] Identifier quoting: DataFusion uses double-quotes for identifiers
- [ ] Write tests: verify dialect produces valid DataFusion SQL for common patterns
- [ ] Commit: `feat: DataFusion SQL dialect for Calcite-to-SQL translation`

**Test:** Dialect correctly maps function names, type casts, and identifier quoting for DataFusion.

### Task 4.2: SqlProducer (RelNode → SQL String)

**Files:**
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/sql/SqlProducer.java`
- Create: `lakehouse/src/test/java/.../sql/SqlProducerTests.java`

- [ ] Implement `SqlProducer`: reuses `UnifiedQueryTranspiler.toSql(relNode, DataFusionDialect)` to convert Calcite RelNode to SQL string
- [ ] Handle all RelNode types: `LogicalTableScan`, `LogicalFilter`, `LogicalProject`, `LogicalAggregate`, `LogicalSort`, `LogicalJoin`, `LogicalValues`
- [ ] Write tests: build Calcite RelNode, produce SQL string, verify SQL syntax
- [ ] Commit: `feat: SQL producer converting Calcite RelNode to DataFusion SQL`

**Test:** Each RelNode type produces correct SQL. SQL string is syntactically valid.

### Task 4.3: SqlValidator

**Files:**
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/sql/SqlValidator.java`
- Create: `lakehouse/src/test/java/.../sql/SqlValidatorTests.java`

- [ ] Validate generated SQL is parseable (basic syntax check)
- [ ] Check for common issues: unbalanced parentheses, missing keywords, invalid identifiers
- [ ] Collect all violations before throwing (not fail-fast)
- [ ] Write tests: valid and invalid SQL strings
- [ ] Commit: `feat: SQL validator with syntax checks`

**Test:** Validator catches malformed SQL and passes valid SQL.

### Task 4.4: Java→Rust SQL Round-Trip Test

- [ ] Write integration test: build a Calcite RelNode (SELECT col1, col2 FROM t WHERE col1 > 10), produce SQL string via `SqlProducer`, send to Rust worker via gRPC `ExecuteTask`
- [ ] Rust parses SQL with `SessionContext::sql()` → verify LogicalPlan is correct
- [ ] The Rust worker should log the parsed DataFusion LogicalPlan without error
- [ ] Commit: `test: SQL round-trip validation Java producer → Rust consumer`

**Test:** Rust successfully parses Java-produced SQL string. No parse errors.

---

## Phase 5: Single-Worker End-to-End

**Milestone:** A PPL/SQL query against a lakehouse index runs through the full pipeline on a single worker and returns results.

**Depends on:** Phase 4 (SQL producer) + Phase 1 (Rust worker)

**Design docs:** `08-rust-worker.md`, `07-grpc-protocol.md`

### Task 5.1: Rust SQL Consumer

**Files:**
- Create or modify: `opensearch-worker/src/query_executor.rs`

- [ ] Implement SQL consumer: parse SQL string → DataFusion `LogicalPlan` via `ctx.sql(&sql_string).await`
- [ ] Extract `IcebergReadOptions` from `TaskRequest` metadata — use file list to pre-populate `DefaultListFilesCache` (skip S3 listing)
- [ ] If no `IcebergReadOptions`, fall back to existing S3 listing behavior
- [ ] Register table via `ListingTable` with schema from SQL DDL or metadata
- [ ] Write Rust tests: parse a known SQL string → verify LogicalPlan structure
- [ ] Commit: `feat: SQL consumer with IcebergReadOptions support`

**Test:** Rust test parses SQL string and produces correct DataFusion LogicalPlan.

### Task 5.2: Rust Task Executor

**Files:**
- Create: `opensearch-worker/src/task_executor.rs`

- [ ] Implement `TaskExecutor`: receives `TaskRequest`, parses SQL via SQL consumer, executes physical plan, streams `RecordBatch` output
- [ ] Task lifecycle: ACCEPTED → RUNNING → COMPLETED/FAILED
- [ ] Memory limit enforcement via DataFusion `MemoryPool`
- [ ] Cancellation support via `CancellationToken`
- [ ] Report progress via `TaskProgress` messages (rows produced, bytes scanned)
- [ ] Write test: execute a simple filter query over a local Parquet file
- [ ] Commit: `feat: rust task executor with lifecycle management`

**Test:** Task executor runs a query, produces Arrow RecordBatches, reports completion.

### Task 5.3: Wire ExecuteTask gRPC RPC

**Files:**
- Modify: `opensearch-worker/src/server.rs` (wire `ExecuteTask` RPC)
- Modify: `lakehouse/src/main/java/.../grpc/WorkerClient.java` (implement `executeTask()`)

- [ ] Implement `ExecuteTask` in Rust server: deserialize `TaskRequest`, create `TaskExecutor`, stream `TaskProgress` back
- [ ] Implement `executeTask()` in Java client: send `TaskRequest` with SQL string, consume `TaskProgress` stream, collect `RecordBatch` results
- [ ] Arrow IPC for result transfer: worker serializes RecordBatches as Arrow IPC bytes in `TaskResult.data`
- [ ] Write integration test: Java sends SQL string for `SELECT * FROM t WHERE x > 10` → Rust executes over test Parquet file → Java receives Arrow results → verify row count and values
- [ ] Commit: `feat: end-to-end task execution via gRPC`

**Test:** Java sends query, Rust executes, Java receives correct results.

### Task 5.4: Worker Registry (Basic)

**Files:**
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/worker/WorkerRegistry.java`
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/worker/WorkerInfo.java`

- [ ] Implement `WorkerRegistry`: listens to `ClusterChangedEvent`, maintains set of `datawarehouse` nodes
- [ ] Implement `WorkerInfo`: node ID, gRPC address, Flight address, status (ACTIVE/UNREACHABLE)
- [ ] Background health check loop: periodic `GetWorkerStatus` calls
- [ ] Write test: simulate node join/leave, verify registry state
- [ ] Commit: `feat: worker registry with cluster state discovery`

**Test:** Registry tracks worker nodes correctly on join/leave.

### Task 5.5: Full Single-Worker Pipeline Test

- [ ] Integration test combining all phases: create lakehouse index → PPL query "source=test_table | where amount > 100 | fields name, amount" → IndexTypeResolver → LakehouseQueryRouter → Calcite RelNode → SqlProducer → gRPC to Rust worker → DataFusion executes over S3 Parquet → Arrow results back to coordinator → verify results
- [ ] Use localstack or mock S3 with test Parquet files
- [ ] Commit: `test: full single-worker lakehouse query pipeline`

**Test:** Complete PPL query returns correct results from Parquet on S3.

---

## Phase 6: Stage Splitter + Distributed Scheduler

**Milestone:** Aggregation queries split into partial + final stages and run across multiple workers.

**Depends on:** Phase 5 (single-worker execution works)

**Design docs:** `04-stage-splitter.md`, `05-distributed-scheduler.md`

### Task 6.1: Stage Splitter Core

**Files:**
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/planner/StageSplitter.java`
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/planner/ExecutionDAG.java`
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/planner/Stage.java`
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/planner/ExchangeEdge.java`
- Create: `lakehouse/src/test/java/.../planner/StageSplitterTests.java`

- [ ] Implement Calcite RelNode tree walker: identify exchange boundaries at `LogicalAggregate` (hash on GROUP BY keys) and `LogicalJoin` (hash on join keys), generate per-stage SQL
- [ ] Plan surgery: cut plan at exchange boundaries, replace cross-stage references with placeholder `ReadRel`
- [ ] Partial aggregation: split `AggregateRel` into `INITIAL_TO_INTERMEDIATE` (leaf) + `INTERMEDIATE_TO_RESULT` (final)
- [ ] Build `ExecutionDAG` with `Stage` nodes and `ExchangeEdge` connections
- [ ] Exchange types: `HASH`, `BROADCAST`, `GATHER` — select based on estimated data size
- [ ] Write tests: simple scan (1 stage), aggregation (2 stages), join (3 stages), join + aggregate (4 stages)
- [ ] Commit: `feat: stage splitter with exchange insertion and partial aggregation`

**Test:** Various query shapes produce correct stage DAGs.

### Task 6.2: File-to-Task Assignment

**Files:**
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/scheduler/FileToTaskAssigner.java`
- Create: `lakehouse/src/test/java/.../scheduler/FileToTaskAssignerTests.java`

- [ ] Bin-pack Iceberg data files into tasks of ~128MB each
- [ ] Split large files across tasks by row-group boundaries
- [ ] Locality-aware: prefer workers that recently read nearby S3 objects (best-effort)
- [ ] Write tests: 10 files × 100MB → ~8 tasks, verify no file missed, verify approximate balance
- [ ] Commit: `feat: file-to-task bin-packing with target task size`

**Test:** Files are packed into balanced tasks; all files accounted for.

### Task 6.3: Distributed Scheduler

**Files:**
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/scheduler/DistributedScheduler.java`
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/scheduler/QueryExecution.java`
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/scheduler/TaskAssignment.java`
- Create: `lakehouse/src/test/java/.../scheduler/DistributedSchedulerTests.java`

- [ ] Implement `DistributedScheduler`: takes `ExecutionDAG` + `FileManifest`, produces `TaskAssignment` per worker
- [ ] Stage sequencing: launch leaf stages first, launch downstream stages when all upstream stages complete
- [ ] Task dispatch: send `ExecuteTask` RPCs to assigned workers via `WorkerClientPool`
- [ ] Stage state machine: `PENDING → RUNNING → COMPLETED | FAILED`
- [ ] Query admission: limit concurrent queries (configurable)
- [ ] Write tests with mock workers: 2-stage query → verify leaf stage runs first, final stage runs after
- [ ] Commit: `feat: distributed scheduler with stage sequencing`

**Test:** Multi-stage query executes stages in correct order on mock workers.

### Task 6.4: CatalogService for File Manifest + Partition Pruning

**Files:**
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/catalog/IcebergCatalogService.java`
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/catalog/IcebergPartitionPruner.java`
- Create: `lakehouse/src/test/java/.../catalog/IcebergCatalogServiceTests.java`

- [ ] Implement `IcebergCatalogService.getTableFiles()`: walk metadata chain (snapshot → manifest list → manifest → DataFileInfo)
- [ ] Implement `IcebergPartitionPruner.pruneFiles()`: apply partition predicates to eliminate non-matching files
- [ ] Support IDENTITY, YEAR, MONTH, DAY, HOUR transforms
- [ ] Metadata caching: Caffeine 3-tier cache (TableMetadata 60s TTL, ManifestList forever, ManifestFile LRU)
- [ ] Write tests with mock Iceberg table: verify file listing, verify pruning eliminates correct files
- [ ] Commit: `feat: Iceberg catalog service with partition pruning`

**Test:** Partition pruning correctly filters files; metadata cache hits work.

---

## Phase 7: Shuffle (Arrow Flight Exchange)

**Milestone:** Workers can exchange intermediate data via Arrow Flight for multi-stage queries.

**Depends on:** Phase 6 (scheduler dispatches multi-stage queries)

**Design docs:** `09-shuffle-arrow-flight.md`

### Task 7.1: Rust ShuffleWriter

**Files:**
- Create: `opensearch-worker/src/shuffle/writer.rs`
- Create: `opensearch-worker/src/shuffle/mod.rs`

- [ ] Implement `ShuffleWriter`: partition output `RecordBatch` by hash key using murmur3
- [ ] Buffer partitioned batches; flush to downstream workers via Arrow Flight `DoExchange`
- [ ] Handle multiple downstream partitions (one Flight stream per target worker)
- [ ] Backpressure: pause producing when downstream is slow (bounded channel)
- [ ] Write Rust tests: hash partition 1000 rows into 4 partitions, verify row distribution
- [ ] Commit: `feat: rust shuffle writer with hash partitioning`

**Test:** Rows partition correctly by hash key.

### Task 7.2: Rust ShuffleReader

**Files:**
- Create: `opensearch-worker/src/shuffle/reader.rs`

- [ ] Implement `ShuffleReader`: starts Arrow Flight `DoExchange` server endpoint
- [ ] Accept incoming partition streams from upstream workers
- [ ] Buffer received batches in a bounded queue for the downstream DataFusion operator to consume
- [ ] Implement as a DataFusion `ExecutionPlan` so it plugs into the physical plan tree
- [ ] Write Rust tests: mock upstream sends batches → ShuffleReader produces them in order
- [ ] Commit: `feat: rust shuffle reader as DataFusion execution plan`

**Test:** ShuffleReader correctly receives and buffers upstream data.

### Task 7.3: Java ShuffleManager

**Files:**
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/shuffle/ShuffleManager.java`

- [ ] Implement `ShuffleManager`: tracks shuffle state per query (which workers send to which)
- [ ] Inject shuffle metadata into `TaskRequest`: target worker addresses for each output partition
- [ ] Cleanup: after query completes, signal workers to release shuffle buffers
- [ ] Write test: verify shuffle metadata correctly wired into task requests
- [ ] Commit: `feat: java shuffle manager for coordinating worker-to-worker exchange`

**Test:** Shuffle metadata correctly assigned; cleanup signals sent.

### Task 7.4: Multi-Worker Shuffle Integration Test

- [ ] 3-worker setup, query: `SELECT region, SUM(amount) FROM orders GROUP BY region`
- [ ] Stage 1 (leaf): 3 workers each scan a subset of Parquet files, compute partial aggregates, hash-shuffle by `region`
- [ ] Stage 2 (final): workers receive shuffled data, compute final aggregates
- [ ] Verify: correct sum per region, no data loss
- [ ] Commit: `test: multi-worker shuffle integration test`

**Test:** Aggregation with shuffle produces correct results across 3 workers.

---

## Phase 8: Multi-Worker End-to-End

**Milestone:** Complex queries (joins, multi-level aggregations) execute correctly across multiple workers.

**Depends on:** Phase 7 (shuffle works)

### Task 8.1: Join Queries

- [ ] Test: `SELECT o.order_id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id WHERE o.amount > 100`
- [ ] Stage 1: scan orders (hash shuffle on customer_id)
- [ ] Stage 2: scan customers (hash shuffle on id)
- [ ] Stage 3: hash join + filter
- [ ] Verify: correct join results, no duplicates, no missing rows
- [ ] Commit: `test: distributed join query across workers`

### Task 8.2: Multi-Level Aggregation

- [ ] Test: `SELECT region, category, COUNT(*) FROM orders GROUP BY region, category ORDER BY COUNT(*) DESC LIMIT 10`
- [ ] Verify: partial aggregate → shuffle → final aggregate → sort → limit → correct results
- [ ] Commit: `test: multi-level aggregation with sort and limit`

### Task 8.3: Window Functions

- [ ] Test: `SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn FROM orders`
- [ ] Shuffle on partition key → window computation → return
- [ ] Commit: `test: distributed window function execution`

---

## Phase 9: Result Collector + Formatting

**Milestone:** Query results are properly collected, paginated, and formatted for client consumption.

**Depends on:** Phase 5 (single-worker works), independent of Phase 6-8 for basic path

**Design docs:** `10-query-result-collector.md`

### Task 9.1: QueryResultCollector

**Files:**
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/result/QueryResultCollector.java`
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/result/ResultStream.java`

- [ ] Implement `QueryResultCollector`: consumes Arrow IPC bytes from gRPC `TaskResult`, deserializes to `RecordBatch`
- [ ] Merge results from multiple workers (for gather exchange)
- [ ] Lazy pull model: consume batches on demand (don't buffer entire result in memory)
- [ ] Write tests: 3 workers return partial results → collector merges correctly
- [ ] Commit: `feat: query result collector with multi-worker merge`

**Test:** Results from multiple workers are correctly merged.

### Task 9.2: Result Formatting (JSON/JDBC/CSV)

**Files:**
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/result/ResultFormatter.java`
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/result/JsonResultFormatter.java`
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/result/JdbcResultFormatter.java`
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/result/CsvResultFormatter.java`

- [ ] Convert `RecordBatch` to JSON (OpenSearch SQL/PPL response format)
- [ ] Convert to JDBC format (column metadata + data rows)
- [ ] Convert to CSV
- [ ] Write tests for each format with various data types
- [ ] Commit: `feat: result formatters for JSON, JDBC, CSV`

**Test:** Each format produces correct output for all data types.

### Task 9.3: Pagination

**Files:**
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/result/PaginationManager.java`

- [ ] Cursor-based pagination: first request returns page + cursor, subsequent requests use cursor
- [ ] Result cache: hold pages in memory with TTL for cursor reuse
- [ ] Memory guard: limit total cached pages per query and globally
- [ ] Write tests: paginate through 10,000 rows in 1,000-row pages
- [ ] Commit: `feat: cursor-based pagination with result caching`

**Test:** Pagination returns all rows, no duplicates, no gaps.

### Task 9.4: REST Integration

- [ ] Wire result collector into OpenSearch REST layer: `POST /_plugins/_ppl` and `POST /_plugins/_sql`
- [ ] Lakehouse queries return results in the same format as existing Calcite queries
- [ ] Write integration test: HTTP request → lakehouse query → HTTP response with correct JSON
- [ ] Commit: `feat: lakehouse query results via existing PPL/SQL REST endpoints`

**Test:** HTTP client gets correct JSON response for a lakehouse PPL query.

---

## Phase 10: Hardening (Fault Tolerance, Observability, Security)

**Milestone:** Production-ready with retry, monitoring, and security integration.

**Depends on:** Phase 8 (multi-worker works end-to-end)

**Design docs:** `11-observability-insights.md`, `12-fault-tolerance.md`, `00-lakehouse-index-abstraction.md §7`

### Task 10.1: Task Retry + Fault Classification

**Files:**
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/fault/FaultToleranceManager.java`
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/fault/ErrorClassifier.java`

- [ ] Classify errors: RETRYABLE (S3 throttle, worker timeout, OOM), FATAL (schema mismatch, unknown function, auth failure)
- [ ] Retry with exponential backoff on alternate worker (max 3 retries)
- [ ] Stage re-execution: if worker dies and shuffle data is lost, re-run the upstream stage
- [ ] Write tests: simulate task failure → verify retry on different worker → verify eventual success
- [ ] Commit: `feat: fault tolerance with error classification and task retry`

### Task 10.2: Query Guardrails

- [ ] Max scan bytes per query (configurable, default 100GB)
- [ ] Query timeout (configurable, default 5 minutes)
- [ ] Max result size (configurable, default 10MB)
- [ ] Max concurrent queries per resource group
- [ ] Write tests for each guardrail
- [ ] Commit: `feat: query guardrails for scan bytes, timeout, result size`

### Task 10.3: Observability — Metrics + Query History

**Files:**
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/observability/QueryProfiler.java`
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/observability/QueryHistoryStore.java`

- [ ] Query profiler: collect per-stage timing, rows processed, bytes scanned, shuffle bytes
- [ ] Prometheus metrics: `lakehouse_queries_total`, `lakehouse_query_duration_seconds`, `lakehouse_scan_bytes_total`, `lakehouse_shuffle_bytes_total`
- [ ] Query history: persist profiles to `.opensearch_lakehouse_query_history` index
- [ ] REST API: `GET /_plugins/_lakehouse/queries` (active queries), `GET /_plugins/_lakehouse/queries/{id}` (query profile)
- [ ] Commit: `feat: query profiling, metrics, and history store`

### Task 10.4: EXPLAIN Support

- [ ] `EXPLAIN source=orders | where amount > 100` → show logical plan, SQL plan, stage DAG, file assignments
- [ ] Three modes: `EXPLAIN LOGICAL`, `EXPLAIN PHYSICAL`, `EXPLAIN DISTRIBUTED`
- [ ] Write tests for each explain mode
- [ ] Commit: `feat: EXPLAIN for lakehouse queries with logical/physical/distributed modes`

### Task 10.5: Security Integration (FLS/DLS)

**Files:**
- Create: `lakehouse/src/main/java/org/opensearch/lakehouse/security/LakehouseSecurityFilter.java`

- [ ] Field-Level Security: filter restricted columns from Iceberg schema BEFORE registering with Calcite (plan physically cannot reference restricted fields)
- [ ] Document-Level Security: inject DLS predicate as additional `WHERE` clause predicate in SQL
- [ ] Audit logging: all lakehouse queries logged with index name, user, timestamp (leverages existing Security plugin infrastructure)
- [ ] Write tests: user with FLS restriction → query omits restricted field → SQL plan has no reference to it
- [ ] Write tests: user with DLS filter → SQL plan includes extra WHERE clause
- [ ] Commit: `feat: FLS and DLS security integration for lakehouse queries`

### Task 10.6: Custom PPL Function UDFs in DataFusion

**Files:**
- Create: `opensearch-worker/src/udf/registry.rs`
- Create: `opensearch-worker/src/udf/grok.rs`
- Create: `opensearch-worker/src/udf/cidr_match.rs`

- [ ] Implement Rust UDFs for PPL-specific functions: `grok`, `cidr_match`, `ip_to_int`
- [ ] Register UDFs in DataFusion `SessionContext` at worker startup
- [ ] Map SQL function names registered as DataFusion UDFs
- [ ] Write Rust tests: each UDF produces correct output for sample inputs
- [ ] Commit: `feat: PPL custom function UDFs in DataFusion workers`

### Task 10.7: Worker Graceful Drain + Rolling Upgrade

- [ ] Drain: stop accepting new tasks, wait for in-flight tasks to complete, then shutdown
- [ ] Rolling upgrade: coordinator detects draining worker, reroutes new tasks
- [ ] Write test: drain worker mid-query → query still completes on remaining workers
- [ ] Commit: `feat: graceful worker drain for rolling upgrades`

---

## Phase Summary

| Phase | Milestone | Key Deliverable |
|-------|-----------|-----------------|
| 1 | Proto + Rust Skeleton | Java↔Rust gRPC connection works |
| 2 | Lakehouse Index + Catalog | REST API for lakehouse indices, Iceberg catalog connections |
| 3 | Query Frontend | PPL/SQL → Calcite RelNode with Iceberg-backed tables |
| 4 | SQL Producer | RelNode → SQL string, validated round-trip |
| 5 | Single-Worker E2E | Full query pipeline on one worker returns results |
| 6 | Stage Splitter + Scheduler | Multi-stage query DAG executed in order |
| 7 | Shuffle | Workers exchange data via Arrow Flight |
| 8 | Multi-Worker E2E | Joins, aggregations, window functions across workers |
| 9 | Result Collector | Pagination, JSON/CSV/JDBC formatting, REST integration |
| 10 | Hardening | Retry, monitoring, security, UDFs, drain |

## Parallelism Guide

These phases can be worked on simultaneously by different teams:

- **Phase 1 + Phase 2**: Fully independent. Start both immediately.
- **Phase 3**: Start as soon as Phase 2 is done.
- **Phase 4**: Start as soon as Phase 3 is done. Can begin TypeMapper/FunctionMapper before Phase 3 finishes.
- **Phase 5**: Needs Phase 1 + Phase 4.
- **Phase 6 + Phase 9**: Both need Phase 5. Can run in parallel.
- **Phase 7**: Needs Phase 6.
- **Phase 8**: Needs Phase 7.
- **Phase 10**: Start security (10.5) and observability (10.3) early; fault tolerance (10.1) after Phase 8.

```
Time →
Team A: [Phase 1] ──────────────────► [Phase 5.1-5.3] ──► [Phase 7] ──► [Phase 8]
Team B: [Phase 2] ──► [Phase 3] ──► [Phase 4] ─────────► [Phase 6] ──► [Phase 10.1-10.2]
Team C:                                                    [Phase 9] ──► [Phase 10.3-10.4]
Team D:                                     [Phase 10.5] ──────────────► [Phase 10.6-10.7]
```

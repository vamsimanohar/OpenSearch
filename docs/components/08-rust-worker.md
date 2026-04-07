# Component 8: Rust Worker (DataFusion Executor)

## 1. Overview and Responsibilities

The Rust Worker is a standalone sidecar process that runs alongside each OpenSearch data node's JVM. It is the execution engine of the distributed lakehouse query system: it receives SQL query fragments from the Coordinator, parses and executes them using Apache DataFusion's built-in SQL support, and streams Arrow record batches back to the coordinator or to peer workers via Arrow Flight.

### Core Responsibilities

- **gRPC server**: Accept `TaskRequest` messages from the Coordinator; report task status and heartbeats.
- **SQL parsing**: Parse SQL query strings into a DataFusion `LogicalPlan` using DataFusion's built-in SQL parser (`SessionContext::sql()`).
- **Table registration**: Resolve Parquet files on S3 (or local filesystem) via `object_store` and register them as DataFusion `ListingTable` providers.
- **Query execution**: Create a physical plan from the logical plan and stream `RecordBatch` output via Tokio async streams.
- **Shuffle exchange**: Partition output batches by hash and push them to downstream workers via Arrow Flight `DoExchange`; receive upstream partition data from peer workers.
- **Memory management**: Enforce per-task memory limits using DataFusion's `MemoryPool`; spill intermediate data to local disk when the limit is approached.
- **UDF registry**: Load and register custom scalar and aggregate functions (for PPL/SQL extensions) into the DataFusion `SessionContext`.
- **Graceful shutdown and cancellation**: Handle SIGTERM and per-task cancellation tokens without corrupting in-flight streams.

### Design Philosophy

The worker deliberately avoids embedding business logic. It executes exactly what the SQL query describes. All query planning, stage splitting, and scheduling live in the Java Coordinator. The worker is stateless between tasks: no persistent query state survives task completion.

---

## 2. Rust Module Structure and Cargo.toml

### Directory Layout

```
opensearch-worker/
├── Cargo.toml
└── src/
    ├── main.rs                  # Binary entry point, CLI arg parsing, server startup
    ├── server.rs                # WorkerServer: gRPC service impl (tonic)
    ├── task_executor.rs         # TaskExecutor: orchestrates a single task lifecycle
    ├── sql_parser.rs            # SqlParser: SQL string → DataFusion LogicalPlan
    ├── table_provider.rs        # S3DataSource: object_store config + ListingTable
    ├── shuffle/
    │   ├── mod.rs
    │   ├── writer.rs            # ShuffleWriter: hash-partition + Flight DoExchange send
    │   └── reader.rs            # ShuffleReader: Flight DoExchange receive
    ├── udf/
    │   ├── mod.rs
    │   └── registry.rs          # UdfRegistry: scalar + aggregate function registration
    ├── memory.rs                # MemoryManager: pool construction, spill config
    ├── runtime.rs               # RuntimeManager: IO runtime + CPU DedicatedExecutor
    ├── cross_rt_stream.rs       # CrossRtStream: CPU→IO runtime bridge (unchanged from JNI crate)
    └── config.rs                # WorkerConfig: all tunables parsed from CLI/env
```

### Cargo.toml

```toml
[package]
name    = "opensearch-worker"
version = "0.1.0"
edition = "2021"
license = "Apache-2.0"

[[bin]]
name = "opensearch-worker"
path = "src/main.rs"

[dependencies]
# DataFusion execution engine
datafusion            = "52.1.0"
datafusion-expr       = "52.1.0"
datafusion-datasource = "52.1.0"
datafusion-common     = "52.1.0"
datafusion-execution  = "52.1.0"
# Note: datafusion-substrait can be added later for Substrait plan support

# Arrow + Parquet
arrow         = { version = "57.3.0", features = ["ffi", "ipc"] }
arrow-array   = "57.3.0"
arrow-schema  = "57.3.0"
arrow-flight  = { version = "57.3.0", features = ["flight-sql-experimental", "tls"] }
parquet       = "57.3.0"

# Object store (S3, GCS, local)
object_store  = { version = "0.12.5", features = ["aws", "gcp", "azure"] }
url           = "2.0"

# gRPC server
tonic         = { version = "0.12", features = ["tls"] }
tonic-build   = "0.12"           # build.rs only

# Async runtime
tokio         = { version = "1.0", features = ["full"] }
tokio-stream  = "0.1.17"
futures       = "0.3"

# Concurrency utilities
parking_lot   = "0.12.5"
once_cell     = "1.21.3"

# Observability
tracing            = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
opentelemetry      = { version = "0.23", features = ["metrics", "trace"] }
opentelemetry-otlp = "0.16"

# Misc
clap       = { version = "4", features = ["derive", "env"] }
serde      = { version = "1", features = ["derive"] }
serde_json = "1"
anyhow     = "1"
uuid       = { version = "1", features = ["v4"] }
mimalloc   = { version = "0.1.48", default-features = false }
num_cpus   = "1.16"
bytes      = "1"
tokio-util = { version = "0.7", features = ["codec"] }

[build-dependencies]
tonic-build = "0.12"

[profile.release]
lto           = true
codegen-units = 1
strip         = false
```

---

## 3. Core Rust Traits and Structs

### 3.1 WorkerServer

The gRPC service implementation, generated from the `worker.proto` definition.

```rust
/// Proto-generated service trait (from worker.proto)
/// Implemented by WorkerServer to handle Coordinator RPC calls.
#[tonic::async_trait]
pub trait WorkerService: Send + Sync + 'static {
    /// Submit a task fragment for execution.
    async fn submit_task(
        &self,
        request: tonic::Request<TaskRequest>,
    ) -> Result<tonic::Response<TaskResponse>, tonic::Status>;

    /// Cancel an in-progress task.
    async fn cancel_task(
        &self,
        request: tonic::Request<CancelTaskRequest>,
    ) -> Result<tonic::Response<CancelTaskResponse>, tonic::Status>;

    /// Server-streaming heartbeat / status updates pushed to coordinator.
    type TaskStatusStream: futures::Stream<
        Item = Result<TaskStatusUpdate, tonic::Status>,
    > + Send + 'static;

    async fn task_status(
        &self,
        request: tonic::Request<TaskStatusRequest>,
    ) -> Result<tonic::Response<Self::TaskStatusStream>, tonic::Status>;
}

/// Concrete implementation of WorkerService.
pub struct WorkerServer {
    /// Executes submitted tasks.
    executor: Arc<TaskExecutor>,
    /// Tracks running tasks and their cancellation tokens.
    task_registry: Arc<TaskRegistry>,
    /// Worker configuration (memory limits, S3 settings, etc.).
    config: Arc<WorkerConfig>,
}

impl WorkerServer {
    pub fn new(config: Arc<WorkerConfig>) -> Self;

    /// Start the gRPC server and Arrow Flight server, block until SIGTERM.
    pub async fn serve(self) -> anyhow::Result<()>;
}

/// Tracks active tasks. Thread-safe.
struct TaskRegistry {
    tasks: parking_lot::RwLock<HashMap<TaskId, TaskHandle>>,
}

struct TaskHandle {
    cancel: tokio_util::sync::CancellationToken,
    status_tx: tokio::sync::watch::Sender<TaskStatus>,
}

pub type TaskId = String;

/// Protobuf-derived types (abbreviated).
pub struct TaskRequest {
    pub task_id:          String,
    /// The SQL query fragment to execute (primary plan format).
    /// References table names that are registered via `table_files` and
    /// shuffle inputs registered as table providers (e.g., `shuffle_input_0`).
    pub sql:              String,
    /// Table name → list of S3 object URIs.
    pub table_files:      HashMap<String, Vec<String>>,
    /// For shuffle stages: downstream worker addresses per partition.
    pub shuffle_targets:  Vec<ShuffleTarget>,
    /// For shuffle stages: which partition columns to hash on.
    pub partition_exprs:  Vec<String>,
    /// Memory limit in bytes for this task.
    pub memory_limit:     i64,
    /// S3 credentials/config (may override global config).
    pub s3_config:        Option<S3Config>,
}
```

### 3.2 TaskExecutor

Orchestrates the full lifecycle of a single task fragment.

```rust
pub struct TaskExecutor {
    runtime:      Arc<RuntimeManager>,
    udf_registry: Arc<UdfRegistry>,
    global_config: Arc<WorkerConfig>,
}

impl TaskExecutor {
    pub fn new(
        runtime:      Arc<RuntimeManager>,
        udf_registry: Arc<UdfRegistry>,
        config:       Arc<WorkerConfig>,
    ) -> Self;

    /// Execute one task fragment end-to-end.
    ///
    /// Returns when the task completes, fails, or is cancelled.
    /// Progress and errors are reported via `status_tx`.
    pub async fn execute(
        &self,
        request:   TaskRequest,
        cancel:    tokio_util::sync::CancellationToken,
        status_tx: tokio::sync::watch::Sender<TaskStatus>,
    ) -> Result<TaskResult, WorkerError>;

    /// Build a DataFusion SessionContext for this task with isolated
    /// memory pool, spill dir, and registered table providers.
    async fn build_session(
        &self,
        request: &TaskRequest,
    ) -> Result<(datafusion::prelude::SessionContext, Arc<MemoryManager>), WorkerError>;

    /// Register all tables listed in the TaskRequest as ListingTable providers.
    async fn register_tables(
        ctx:     &datafusion::prelude::SessionContext,
        request: &TaskRequest,
    ) -> Result<(), WorkerError>;

    /// Execute the physical plan, write output to shuffle or return stream.
    async fn run_plan(
        &self,
        ctx:     datafusion::prelude::SessionContext,
        request: TaskRequest,
        cancel:  tokio_util::sync::CancellationToken,
    ) -> Result<TaskResult, WorkerError>;
}

pub enum TaskResult {
    /// Final stage: results returned directly to coordinator via Arrow Flight.
    Final { rows_produced: u64 },
    /// Intermediate stage: results shuffled to downstream workers.
    Shuffled { partitions_sent: u32, rows_produced: u64 },
}

#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::common::DataFusionError),
    #[error("SQL parse error: {0}")]
    SqlParse(String),
    #[error("S3 error: {0}")]
    ObjectStore(#[from] object_store::Error),
    #[error("Flight error: {0}")]
    Flight(String),
    #[error("Task cancelled")]
    Cancelled,
    #[error("Memory limit exceeded: used {used} / limit {limit}")]
    MemoryExceeded { used: usize, limit: usize },
}
```

### 3.3 SqlParser

Parses a SQL query string into a DataFusion `LogicalPlan` using DataFusion's built-in SQL support.

```rust
pub struct SqlParser;

impl SqlParser {
    /// Parse a SQL string and convert to a DataFusion LogicalPlan.
    ///
    /// The `ctx` must already have all referenced tables registered
    /// (both data tables and shuffle input tables).
    pub async fn to_logical_plan(
        sql: &str,
        ctx: &datafusion::prelude::SessionContext,
    ) -> Result<datafusion::logical_expr::LogicalPlan, WorkerError> {
        let df = ctx.sql(sql)
            .await
            .map_err(|e| WorkerError::SqlParse(format!("SQL parse error: {e}")))?;

        Ok(df.logical_plan().clone())
    }

    /// Validate that the plan only references tables already registered in the context.
    pub fn validate_table_refs(
        plan: &datafusion::logical_expr::LogicalPlan,
        ctx:  &datafusion::prelude::SessionContext,
    ) -> Result<(), WorkerError>;
}
```

> **Future Substrait support**: A `substrait_plan` arm can be added to the `TaskRequest` oneof
> handler by importing the `datafusion-substrait` crate and using
> `from_substrait_plan(ctx.state(), &plan)` to decode Substrait bytes into a `LogicalPlan`.
> This is deferred from v1 to keep dependencies minimal.

### 3.4 ShuffleWriter

Hash-partitions output record batches and pushes each partition to the appropriate downstream worker via Arrow Flight `DoExchange`.

```rust
pub struct ShuffleWriter {
    /// One Flight client per downstream worker, keyed by partition index.
    clients:   Vec<Arc<FlightClient>>,
    /// DataFusion expressions used to compute the partition key.
    partition_exprs: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
    /// Number of output partitions.
    num_partitions: usize,
}

impl ShuffleWriter {
    /// Build a ShuffleWriter from a TaskRequest.
    pub async fn from_task(request: &TaskRequest) -> Result<Self, WorkerError>;

    /// Consume the full output stream, hash-partition each batch, and send
    /// each partition to its target worker via Arrow Flight DoExchange.
    ///
    /// `stream` is the DataFusion execution output stream.
    /// Returns per-partition row counts.
    pub async fn write(
        &self,
        mut stream: datafusion::physical_plan::SendableRecordBatchStream,
        cancel:     tokio_util::sync::CancellationToken,
    ) -> Result<Vec<u64>, WorkerError>;

    /// Partition a single RecordBatch into `num_partitions` sub-batches
    /// using hash(partition_exprs) % num_partitions.
    fn partition_batch(
        &self,
        batch: &arrow_array::RecordBatch,
    ) -> Result<Vec<arrow_array::RecordBatch>, WorkerError>;

    /// Open a streaming DoExchange call to a single downstream worker.
    async fn open_exchange(
        address:    &str,
        task_id:    &str,
        partition:  u32,
        schema:     arrow_schema::SchemaRef,
    ) -> Result<FlightExchangeSink, WorkerError>;
}

/// Wraps a single Arrow Flight DoExchange write-half.
struct FlightExchangeSink {
    sender: tonic::Streaming<arrow_flight::FlightData>,
    schema: arrow_schema::SchemaRef,
}

impl FlightExchangeSink {
    async fn send_batch(&mut self, batch: arrow_array::RecordBatch) -> Result<(), WorkerError>;
    async fn finish(self) -> Result<(), WorkerError>;
}
```

### 3.5 ShuffleReader

Receives Arrow record batches from upstream stage workers via Arrow Flight `DoExchange`, presenting them as a DataFusion `SendableRecordBatchStream`.

```rust
pub struct ShuffleReader {
    /// One Flight client per upstream worker.
    sources:  Vec<ShuffleSource>,
    schema:   arrow_schema::SchemaRef,
}

/// Describes one upstream shuffle partition source.
pub struct ShuffleSource {
    pub worker_address: String,
    pub task_id:        String,
    pub partition_id:   u32,
}

impl ShuffleReader {
    pub fn new(sources: Vec<ShuffleSource>, schema: arrow_schema::SchemaRef) -> Self;

    /// Open DoExchange streams to all upstream workers and merge them into
    /// a single interleaved RecordBatch stream (no ordering guarantees).
    pub async fn into_stream(
        self,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<datafusion::physical_plan::SendableRecordBatchStream, WorkerError>;

    /// Open a single DoExchange read from one upstream worker.
    async fn open_receive(
        source: ShuffleSource,
        schema: arrow_schema::SchemaRef,
    ) -> Result<impl futures::Stream<Item = Result<arrow_array::RecordBatch, WorkerError>>, WorkerError>;
}
```

### 3.6 UdfRegistry

Registers custom scalar and aggregate functions into a DataFusion `SessionContext`.

```rust
pub struct UdfRegistry {
    scalar_udfs:    Vec<Arc<datafusion::logical_expr::ScalarUDF>>,
    aggregate_udfs: Vec<Arc<datafusion::logical_expr::AggregateUDF>>,
    window_udfs:    Vec<Arc<datafusion::logical_expr::WindowUDF>>,
}

impl UdfRegistry {
    /// Create an empty registry.
    pub fn new() -> Self;

    /// Register all built-in PPL/SQL extension functions.
    pub fn with_builtins(self) -> Self;

    /// Register a custom scalar UDF at runtime (e.g., loaded from a plugin).
    pub fn register_scalar(
        &mut self,
        udf: Arc<datafusion::logical_expr::ScalarUDF>,
    );

    /// Register a custom aggregate UDF at runtime.
    pub fn register_aggregate(
        &mut self,
        udaf: Arc<datafusion::logical_expr::AggregateUDF>,
    );

    /// Register a window function.
    pub fn register_window(
        &mut self,
        udwf: Arc<datafusion::logical_expr::WindowUDF>,
    );

    /// Apply all registered UDFs to a SessionContext.
    pub fn apply(&self, ctx: &datafusion::prelude::SessionContext);
}

/// Example of a custom scalar UDF for PPL's `grok` function.
/// Implements datafusion::logical_expr::ScalarUDFImpl.
pub struct GrokUdf {
    signature: datafusion::logical_expr::Signature,
}

impl datafusion::logical_expr::ScalarUDFImpl for GrokUdf {
    fn name(&self) -> &str { "grok" }

    fn signature(&self) -> &datafusion::logical_expr::Signature { &self.signature }

    fn return_type(
        &self,
        arg_types: &[arrow_schema::DataType],
    ) -> datafusion::common::Result<arrow_schema::DataType>;

    fn invoke(
        &self,
        args: &[datafusion::physical_expr::ColumnarValue],
    ) -> datafusion::common::Result<datafusion::physical_expr::ColumnarValue>;
}
```

### 3.7 MemoryManager

Constructs and manages the DataFusion memory pool and spill configuration for a task.

```rust
pub struct MemoryManager {
    /// Shared memory pool enforcing the task-level byte limit.
    pub pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool>,
    /// Disk manager for spill-to-disk when memory is exhausted.
    pub disk_manager: Arc<datafusion::execution::disk_manager::DiskManager>,
    /// Byte limit for this task.
    pub limit_bytes: usize,
}

impl MemoryManager {
    /// Create a MemoryManager for one task.
    ///
    /// `limit_bytes`: hard limit; DataFusion will attempt to spill before
    /// exceeding this. Set to 0 for unlimited (not recommended in production).
    /// `spill_root`: directory where spill files are written (e.g., `/mnt/nvme/spill`).
    pub fn new(
        limit_bytes: usize,
        spill_root:  &std::path::Path,
        task_id:     &str,
    ) -> Result<Self, WorkerError>;

    /// Attach this manager to a DataFusion RuntimeEnvBuilder.
    pub fn apply(
        &self,
        builder: datafusion::execution::runtime_env::RuntimeEnvBuilder,
    ) -> datafusion::execution::runtime_env::RuntimeEnvBuilder;

    /// Current bytes reserved across all DataFusion consumers.
    pub fn bytes_used(&self) -> usize;

    /// Remaining bytes before the hard limit is hit.
    pub fn bytes_available(&self) -> usize;
}

// Internal pool type used: TrackConsumersPool<GreedyMemoryPool>
// - GreedyMemoryPool: allocates up to limit_bytes, returns OOM error when exceeded.
// - TrackConsumersPool: tracks top-N consumers for diagnostics; logs them on OOM.
```

### 3.8 S3DataSource

Configures an `object_store` S3 client and registers it with the DataFusion `RuntimeEnv`.

```rust
pub struct S3DataSource {
    store: Arc<dyn object_store::ObjectStore>,
    base_url: url::Url,
}

/// All tunables for one S3 bucket/endpoint.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct S3Config {
    pub bucket:            String,
    pub region:            String,
    /// Override endpoint for MinIO / LocalStack / VPC endpoint.
    pub endpoint:          Option<String>,
    pub access_key_id:     Option<String>,
    pub secret_access_key: Option<String>,
    /// IAM role ARN; if set, credentials are refreshed via STS AssumeRole.
    pub role_arn:          Option<String>,
    /// HTTP connection pool size per worker thread.
    pub max_connections:   usize,
    /// Request timeout in seconds.
    pub request_timeout_secs: u64,
    /// Whether to use path-style addressing (for MinIO).
    pub path_style:        bool,
}

impl S3DataSource {
    /// Build an S3ObjectStore from config and register it with the DataFusion
    /// RuntimeEnv so all ListingTable IO goes through the configured client.
    pub fn register(
        config:  &S3Config,
        runtime: &mut datafusion::execution::runtime_env::RuntimeEnvBuilder,
    ) -> Result<Self, WorkerError>;

    /// Return the object store handle (for pre-population of list-files cache).
    pub fn store(&self) -> Arc<dyn object_store::ObjectStore>;
}
```

### 3.9 RuntimeManager

Owns the two-tier Tokio runtime: an IO runtime for network + object-store operations, and a dedicated CPU executor for DataFusion compute kernels.

```rust
pub struct RuntimeManager {
    /// High-thread-count runtime for IO (S3 fetches, gRPC, Flight).
    pub io_runtime:   Arc<tokio::runtime::Runtime>,
    /// Separate runtime for CPU-bound DataFusion work.
    pub cpu_executor: DedicatedExecutor,
}

impl RuntimeManager {
    /// `cpu_threads`: number of DataFusion worker threads.
    /// IO threads = cpu_threads * 2.
    pub fn new(cpu_threads: usize) -> Self;
    pub fn cpu_executor(&self) -> DedicatedExecutor;
    pub fn shutdown(&self);
}

/// Runs CPU-bound futures on a dedicated Tokio runtime, isolated from IO.
/// Based on InfluxDB's executor pattern.
#[derive(Clone)]
pub struct DedicatedExecutor {
    state: Arc<parking_lot::RwLock<ExecutorState>>,
}

impl DedicatedExecutor {
    pub fn new(name: &str, builder: tokio::runtime::Builder) -> Self;
    pub fn spawn<T>(&self, task: T) -> impl std::future::Future<Output = Result<T::Output, JobError>>
    where
        T: std::future::Future + Send + 'static,
        T::Output: Send + 'static;
    pub fn shutdown(&self);
    pub fn join_blocking(&self);
}

#[derive(Debug)]
pub enum JobError {
    WorkerGone,
    Panic { msg: String },
}
```

---

## 4. Execution Lifecycle of a Single Task

```
Coordinator                   WorkerServer                TaskExecutor
    │                              │                           │
    │── SubmitTask(TaskRequest) ──>│                           │
    │                              │── execute(req, cancel) ──>│
    │                              │                           │
    │                              │              ┌────────────┤
    │                              │              │ 1. build_session()
    │                              │              │    MemoryManager::new(limit, spill_dir)
    │                              │              │    S3DataSource::register(s3_config)
    │                              │              │    RuntimeEnvBuilder → SessionContext
    │                              │              │    UdfRegistry::apply(ctx)
    │                              │              │
    │                              │              │ 2. register_tables()
    │                              │              │    per table_files entry:
    │                              │              │      ListingTableUrl::parse(s3://...)
    │                              │              │      DefaultListFilesCache::put(metas)
    │                              │              │      ListingOptions(ParquetFormat)
    │                              │              │      infer_schema() → ListingTableConfig
    │                              │              │      ctx.register_table(name, provider)
    │                              │              │
    │                              │              │ 3. SqlParser::to_logical_plan()
    │                              │              │    ctx.sql(&sql_string).await
    │                              │              │    → DataFrame → LogicalPlan
    │                              │              │
    │                              │              │ 4. ctx.execute_logical_plan(logical_plan)
    │                              │              │    → DataFrame
    │                              │              │    dataframe.create_physical_plan()
    │                              │              │    → Arc<dyn ExecutionPlan>
    │                              │              │
    │                              │              │ 5. execute_stream(physical_plan, ctx.task_ctx())
    │                              │              │    → SendableRecordBatchStream (CPU executor)
    │                              │              │    wrapped in CrossRtStream
    │                              │              │
    │                              │              │ 6a. Final stage:
    │                              │              │     stream results back via Arrow Flight
    │                              │              │     (coordinator calls DoGet or DoExchange)
    │                              │              │
    │                              │              │ 6b. Shuffle stage:
    │                              │              │     ShuffleWriter::write(stream)
    │                              │              │     hash-partition → DoExchange to peers
    │                              │              │
    │<── TaskResponse(ACCEPTED) ───│              │
    │                              │              │ 7. status_tx.send(COMPLETED | FAILED)
    │<── TaskStatusUpdate ─────────│<─────────────┤
```

### Step-by-step Detail

#### Step 1 — Receive TaskRequest via gRPC

`WorkerServer::submit_task` is invoked by tonic. It allocates a `CancellationToken` and a `watch` channel for status, inserts them into `TaskRegistry`, then spawns `task_executor.execute(...)` on the IO runtime. It returns `TaskResponse { status: ACCEPTED }` immediately (non-blocking).

#### Step 2 — Build Session

A per-task `SessionContext` is constructed with an isolated memory pool (via `MemoryManager`) and its own spill subdirectory. The global `RuntimeEnv` is cloned and patched with a fresh `DefaultListFilesCache` pre-populated with the S3 `ObjectMeta` entries provided in the `TaskRequest`, so DataFusion does not re-list S3 prefixes.

```
SessionConfig {
    target_partitions    = cpu_thread_count,
    batch_size           = 8192,
    parquet.pushdown_filters = true,   // enabled for sidecar (no JVM interference)
}
```

#### Step 3 — Register Table Providers

For each `(table_name, [s3_uris])` in the request, a `ListingTable` is registered. File metadata is resolved from the pre-populated cache; no S3 LIST calls are made at plan time.

#### Step 4 — Parse SQL → LogicalPlan

`SqlParser::to_logical_plan` calls `ctx.sql(&sql_string)` which uses DataFusion's built-in SQL parser (based on `sqlparser-rs`) to produce a `DataFrame`, from which the `LogicalPlan` is extracted. Any UDFs referenced in the SQL must already be registered by name in the `SessionContext` before this call, or the parser will fail with an "unknown function" error. For shuffle stages, the SQL references registered table names like `shuffle_input_0` that the worker registers as DataFusion table providers before parsing the SQL.

#### Step 5 — Create Physical Plan

`ctx.execute_logical_plan(logical_plan)` produces a `DataFrame`; `.create_physical_plan()` yields the final `Arc<dyn ExecutionPlan>`. DataFusion's optimizer runs at this point (predicate pushdown, projection pruning, column pruning, sort-merge decisions).

#### Step 6 — Execute with Tokio Runtime

`execute_stream(physical_plan, ctx.task_ctx())` is called on the CPU executor via `CrossRtStream`. Batches are bridged back to the IO runtime through a bounded `mpsc::channel(1)`, providing natural backpressure. CPU-bound work (decompression, vectorized expression evaluation) is isolated from IO-bound work (S3 GET, Flight writes).

#### Step 7 — Stream Output

**Final stage**: The coordinator opened a Flight `DoGet` or `DoExchange` call; the worker streams batches directly over that connection. When the DataFusion stream is exhausted, the Flight stream is closed with `FlightData { app_metadata: EOS_MARKER }`.

**Shuffle stage**: `ShuffleWriter::write` pulls batches from the DataFusion stream, computes `hash(partition_exprs) % num_partitions` for every row, splits each batch into per-partition sub-batches using `filter_record_batch`, and writes each sub-batch to the corresponding downstream worker's open `DoExchange` send stream. A sentinel batch is sent last to signal partition completion.

---

## 5. Custom UDF Registration

### Registering a New PPL/SQL Function

All custom functions must implement one of DataFusion's UDF traits. The registration path is:

1. Implement `ScalarUDFImpl` (or `AggregateUDFImpl` / `WindowUDFImpl`) in `src/udf/`.
2. Wrap in `ScalarUDF::new_from_impl(Arc::new(MyUdf::new()))`.
3. Call `registry.register_scalar(udf)` in `UdfRegistry::with_builtins()`.
4. `UdfRegistry::apply(&ctx)` calls `ctx.register_udf(udf)` for each entry.

### Example: `cidrmatch` scalar function (PPL extension)

```rust
use datafusion::logical_expr::{ScalarUDFImpl, Signature, TypeSignature, Volatility};
use datafusion::physical_expr::ColumnarValue;
use arrow_schema::DataType;

pub struct CidrMatchUdf {
    signature: Signature,
}

impl CidrMatchUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for CidrMatchUdf {
    fn name(&self) -> &str { "cidrmatch" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke(&self, args: &[ColumnarValue]) -> datafusion::common::Result<ColumnarValue> {
        // Evaluate CIDR membership using the `ipnet` crate.
        // Cast both args to StringArray, compute boolean output per row.
        todo!("CIDR match implementation")
    }
}
```

### Loading Plugin UDFs at Runtime

The `WorkerConfig` may specify a `udf_plugin_dir`. On startup, `main.rs` scans `*.so` / `*.dylib` files in that directory, `dlopen`s each one, and calls a well-known symbol `register_udfs(registry: &mut UdfRegistry)`. This allows shipping PPL extensions without recompiling the worker binary.

---

## 6. Memory Management

### Architecture

DataFusion memory management is controlled by a `MemoryPool` registered in the `RuntimeEnv`. The worker uses a two-level setup:

```
TrackConsumersPool<GreedyMemoryPool>
 ├── tracks top-5 consumers by allocation size (for OOM diagnostics)
 └── GreedyMemoryPool(limit_bytes)
      ├── allows allocation up to limit
      └── returns ResourcesExhausted when exceeded → DataFusion triggers spill
```

### Per-Task Isolation

Each task gets its own `MemoryManager` with a fresh pool. This means:
- Tasks cannot starve each other.
- The worker enforces `sum(task limits) <= node_memory * 0.75` at admission time in `WorkerServer::submit_task`. Tasks that would exceed node capacity are rejected with `RESOURCE_EXHAUSTED`.

### Spill-to-Disk

DataFusion's `DiskManager` manages temporary spill files. Configuration:

```rust
DiskManagerBuilder::default()
    .with_mode(DiskManagerMode::Directories(vec![
        spill_root.join(task_id),  // task-scoped subdirectory
    ]))
    .with_max_temp_directory_size(spill_limit_bytes)
```

Spill files are automatically deleted when the `DataFusion SessionContext` is dropped. The worker also registers a cleanup hook via `tokio::spawn` that removes the task spill directory on cancellation or timeout.

### Memory Pool Interaction with DataFusion

Operators that support spilling (Sort, HashAggregate, HashJoin) reserve memory via `MemoryConsumer::try_grow`. When the pool refuses an allocation, DataFusion returns `DataFusionError::ResourcesExhausted`, the operator spills its current state to the `DiskManager` temp file, releases memory, and resumes. If spill cannot free enough memory the error propagates up and the task fails with `WorkerError::MemoryExceeded`.

---

## 7. S3 Configuration

### Object Store Registration

Each `ListingTableUrl` with scheme `s3://` requires a matching `ObjectStore` registered in the DataFusion `RuntimeEnv`:

```rust
// In S3DataSource::register:
let builder = object_store::aws::AmazonS3Builder::new()
    .with_bucket_name(&config.bucket)
    .with_region(&config.region)
    .with_access_key_id(access_key)
    .with_secret_access_key(secret_key)
    .with_endpoint(config.endpoint.as_deref().unwrap_or_default())
    .with_allow_http(config.endpoint.is_some())  // MinIO over HTTP
    .with_virtual_hosted_style_request(!config.path_style);

let store: Arc<dyn ObjectStore> = Arc::new(builder.build()?);
runtime_builder.register_object_store(
    &url::Url::parse(&format!("s3://{}", config.bucket))?,
    store,
);
```

### IAM Role / STS Credential Refresh

When `role_arn` is set, the builder uses `object_store::aws::AmazonS3Builder::with_credentials`:

```rust
.with_credentials(Arc::new(StsCredentialProvider::new(role_arn)))
```

`StsCredentialProvider` calls STS `AssumeRole` in the background and caches credentials until 5 minutes before expiry.

### Connection Pooling

The underlying `reqwest` client used by `object_store` is configured with:

```
max_idle_per_host = config.max_connections  // default: 16
connection_timeout = 5s
request_timeout = config.request_timeout_secs  // default: 30s
tcp_keepalive = 60s
```

The same `ObjectStore` instance is shared across all concurrent `ListingTable` scans within a session, so connections are pooled per task.

### List-Files Cache Pre-population

To avoid redundant S3 LIST calls (the file set is already known from the coordinator's `TaskRequest`), the worker pre-populates DataFusion's `DefaultListFilesCache`:

```rust
let list_cache = Arc::new(DefaultListFilesCache::default());
for (table_name, uris) in &request.table_files {
    let metas: Vec<ObjectMeta> = resolve_object_metas(store, uris).await?;
    list_cache.put(
        &TableScopedPath { table: None, path: prefix.clone() },
        Arc::new(metas),
    );
}
// Attach to per-query RuntimeEnv via CacheManagerConfig
```

---

## 8. Arrow Flight Server for Shuffle

### DoExchange Protocol

The worker runs a second `tonic` server implementing `arrow_flight::flight_service_server::FlightService`. The coordinator and peer workers use this service for shuffle data transfer.

```
Upstream Worker (ShuffleWriter)          Downstream Worker (FlightService)
        │                                          │
        │── DoExchange(FlightDescriptor) ─────────>│
        │   descriptor.cmd = ShuffleMetadata {     │
        │     task_id, partition_id, schema_bytes  │
        │   }                                      │
        │── FlightData(schema IPC) ───────────────>│
        │── FlightData(batch_1 IPC) ──────────────>│
        │── FlightData(batch_2 IPC) ──────────────>│
        │── FlightData(EOS marker) ───────────────>│
        │                                          │
        │<── FlightData(ACK per batch) ────────────│ (optional flow control)
        │<── FlightData(done sentinel) ────────────│
```

### FlightService Implementation

```rust
pub struct WorkerFlightService {
    /// Active shuffle receive buffers, keyed by (task_id, partition_id).
    shuffle_buffers: Arc<parking_lot::RwLock<
        HashMap<ShuffleKey, tokio::sync::mpsc::Sender<arrow_array::RecordBatch>>
    >>,
}

#[tonic::async_trait]
impl FlightService for WorkerFlightService {
    /// Upstream workers push partition data here.
    type DoExchangeStream = /* tonic streaming response */;

    async fn do_exchange(
        &self,
        request: tonic::Request<tonic::Streaming<arrow_flight::FlightData>>,
    ) -> Result<tonic::Response<Self::DoExchangeStream>, tonic::Status> {
        // 1. Read first FlightData to extract ShuffleMetadata from descriptor.cmd
        // 2. Decode Arrow IPC schema from FlightData.data_header
        // 3. Look up (or create) the mpsc channel for this (task_id, partition_id)
        // 4. Stream remaining FlightData frames, decode IPC → RecordBatch, send to channel
        // 5. On EOS sentinel, close sender side of channel
        // 6. Respond with ACK stream
        todo!()
    }

    /// Final-stage results returned to coordinator.
    type DoGetStream = /* tonic streaming response */;

    async fn do_get(
        &self,
        request: tonic::Request<arrow_flight::Ticket>,
    ) -> Result<tonic::Response<Self::DoGetStream>, tonic::Status> {
        // Ticket.ticket = serialized TaskResultRequest { task_id }
        // Stream the completed result batches from the task's output buffer
        todo!()
    }
}

#[derive(Hash, Eq, PartialEq, Clone)]
struct ShuffleKey {
    task_id:      String,
    partition_id: u32,
}
```

### Flow Control

The Arrow Flight DoExchange uses a bounded `mpsc::channel(256)` per partition as the backpressure mechanism. If the downstream consumer (ShuffleReader) is slower than the upstream writer, the channel fills and the DoExchange handler's `await` on `tx.send(batch)` blocks, which propagates back-pressure through gRPC flow control to the upstream worker.

---

## 9. Graceful Shutdown and Task Cancellation

### Process-Level Shutdown

`main.rs` registers signal handlers for `SIGTERM` and `SIGINT`:

```rust
let mut sigterm = tokio::signal::unix::signal(SignalKind::terminate())?;
let mut sigint  = tokio::signal::unix::signal(SignalKind::interrupt())?;

tokio::select! {
    _ = sigterm.recv() => { tracing::info!("SIGTERM received"); }
    _ = sigint.recv()  => { tracing::info!("SIGINT received"); }
}

// 1. Stop accepting new tasks (close gRPC listener)
// 2. Broadcast shutdown to all active tasks via global CancellationToken
// 3. Wait up to 30s for tasks to drain
// 4. Force-shutdown IO runtime
runtime_manager.shutdown();
```

### Per-Task Cancellation

The coordinator can call `CancelTask(task_id)`. The handler:

1. Looks up the task's `CancellationToken` in `TaskRegistry`.
2. Calls `token.cancel()`.
3. DataFusion streams are wrapped with `tokio_util::sync::WaitForCancellationFuture`; when the token fires, the next `.await` in the CrossRtStream loop returns `Poll::Ready(None)`, ending the stream cleanly.
4. The `ShuffleWriter` checks the token before sending each partition batch; if cancelled, it sends an error sentinel to downstream workers and closes all Flight connections.
5. Spill files are cleaned up via a `drop` guard on the task's spill directory.

```rust
// In CrossRtStream polling (extended for cancellation):
tokio::select! {
    batch = stream.next() => { /* process batch */ }
    _ = cancel.cancelled() => {
        tracing::warn!(task_id, "Task cancelled mid-stream");
        return Poll::Ready(None);
    }
}
```

### Resource Cleanup Guarantees

All task-scoped resources (SessionContext, memory pool reservations, spill files, Flight connections) are owned by `TaskHandle` which is held in `TaskRegistry`. When a task completes (success, failure, or cancellation), `TaskRegistry::remove(task_id)` drops the `TaskHandle`, triggering Rust's RAII cleanup chain.

---

## 10. Difference from Existing JNI Approach

### Current Architecture (JNI)

```
Java OpenSearch JVM
 └── NativeBridge.java
      └── JNI calls → libopensearch_datafusion_jni.so
           ├── Tokio IO runtime (embedded in JVM process)
           ├── DedicatedExecutor (CPU runtime)
           └── DataFusion SessionContext per query
```

**Problems with JNI**:

| Issue | Impact |
|---|---|
| Shared process memory | A DataFusion panic or OOM abort kills the JVM, taking the OpenSearch node down with it |
| GC interference | JVM GC pauses interfere with Tokio's scheduler; Tokio tasks stall during stop-the-world GC events |
| Memory accounting | Java heap and DataFusion memory pool compete for the same physical memory; neither knows the other's true usage |
| ClassLoader coupling | Upgrading the Rust library requires restarting the JVM and reloading all OpenSearch shards |
| Limited parallelism | JNI call overhead and thread-pinning constraints limit the number of concurrent queries |
| No shuffle | JNI approach only handles single-node execution; multi-stage distributed queries require out-of-process coordination |

### New Architecture (Standalone Sidecar)

```
Java OpenSearch JVM                  Rust Worker Sidecar (separate process)
 └── WorkerClient.java                └── opensearch-worker binary
      │                                    ├── tonic gRPC server (TaskService)
      │── gRPC SubmitTask ──────────────>  ├── Arrow Flight server (shuffle + results)
      │<── Arrow Flight results ─────────  ├── DataFusion execution engine
                                           ├── object_store S3 client
                                           └── Tokio IO + CPU runtimes
```

**Benefits of the sidecar**:

| Benefit | Detail |
|---|---|
| Process isolation | DataFusion crash/OOM does not affect the OpenSearch JVM; the sidecar is restarted by the JVM's process watchdog |
| Independent memory | The sidecar has its own Linux cgroup / container memory limit; JVM and DataFusion no longer compete |
| Independent upgrades | The worker binary can be upgraded via rolling restart without touching the JVM classpath |
| Shuffle capability | The sidecar runs an Arrow Flight server that peer workers connect to directly; multi-stage queries are now possible |
| Better observability | Separate process → separate metrics endpoint, separate log stream, separate heap profiling |
| No JNI complexity | Eliminates `Box::into_raw` pointer contracts, GlobalRef management, JNI exception propagation, and thread-attachment overhead |
| FFM migration path | The existing `api.rs` layer is already bridge-agnostic; the JNI bridge (`lib.rs`) can be replaced by a gRPC stub without touching execution logic |

### Migration Strategy

The existing `api.rs` module is intentionally bridge-agnostic (no JNI types). The sidecar reuses `query_executor.rs`, `executor.rs`, `cross_rt_stream.rs`, `runtime_manager.rs`, and `api.rs` directly — only `lib.rs` (the JNI shim) is replaced by the new `server.rs` (tonic gRPC shim). This means the execution logic has already been validated in production via JNI and requires no re-testing.

The two modes can co-exist during rollout: nodes that have not yet deployed the sidecar continue to use JNI; nodes that have deployed the sidecar ignore the JNI bridge. The coordinator detects sidecar availability via the worker registration heartbeat and routes accordingly.

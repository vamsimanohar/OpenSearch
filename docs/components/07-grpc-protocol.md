# Component 7: gRPC Protocol (Coordinator-Worker)

## Table of Contents

1. [Overview and Responsibilities](#1-overview-and-responsibilities)
2. [Complete .proto Definitions](#2-complete-proto-definitions)
3. [Java Client Wrapper Interfaces (Coordinator Side)](#3-java-client-wrapper-interfaces-coordinator-side)
4. [Rust Server Trait Definitions (Worker Side)](#4-rust-server-trait-definitions-worker-side)
5. [Error Handling and Retry Semantics](#5-error-handling-and-retry-semantics)
6. [Streaming Protocol: TaskProgress Streams](#6-streaming-protocol-taskprogress-streams)
7. [Connection Management](#7-connection-management)
8. [TLS/Security](#8-tlssecurity)
9. [Backpressure and Flow Control](#9-backpressure-and-flow-control)
10. [Versioning Strategy](#10-versioning-strategy)

---

## 1. Overview and Responsibilities

### Role in the System

The gRPC protocol layer is the exclusive communication channel between the **Coordinator** (Java, running inside an OpenSearch plugin) and **Workers** (Rust/DataFusion sidecars co-located with each OpenSearch data node). It replaces all ad-hoc HTTP/REST calls for task dispatch and status reporting.

```
OpenSearch Coordinator Node
┌─────────────────────────────────────────────────┐
│  QueryPlanner  ──►  TaskDispatcher               │
│                          │                       │
│                    WorkerClientPool               │
│                     (gRPC stubs)                  │
└─────────────────────────┬───────────────────────┘
                          │ gRPC / TLS
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
    Worker Node 1   Worker Node 2   Worker Node 3
   ┌───────────┐   ┌───────────┐   ┌───────────┐
   │ DataFusion│   │ DataFusion│   │ DataFusion│
   │  Sidecar  │   │  Sidecar  │   │  Sidecar  │
   └───────────┘   └───────────┘   └───────────┘
```

### Coordinator Responsibilities

- Maintain a pool of gRPC channels, one per worker sidecar.
- Pass SQL query strings (or optional Substrait plans) in `TaskRequest`.
- Dispatch `ExecuteTask` RPCs and consume the resulting `TaskProgress` stream.
- Issue `CancelTask` on query timeout, user cancellation, or partial-failure abort.
- Poll `GetWorkerStatus` for admission control and load-aware scheduling.
- Register user-defined functions before query execution via `RegisterUDF`.
- Track per-RPC deadlines, enforce retries with backoff, and surface errors upward.

### Worker Responsibilities

- Accept `TaskRequest`, parse the SQL query (or decode the optional Substrait plan), and execute it via DataFusion.
- Stream incremental `TaskProgress` messages back to the coordinator.
- Honour `CancelTask` by interrupting the running DataFusion query.
- Respond accurately to `GetWorkerStatus` for slot/memory accounting.
- Store and expose registered UDFs across subsequent task executions.
- Perform shuffle output (write partitioned Arrow IPC files or stream via Arrow Flight) and shuffle input (read from peer workers).

---

## 2. Complete .proto Definitions

File: `proto/lakehouse/v1/worker.proto`

```protobuf
syntax = "proto3";

package lakehouse.v1;

option java_package = "org.opensearch.lakehouse.proto";
option java_outer_classname = "WorkerProto";
option java_multiple_files = true;
option optimize_for = SPEED;

// Rust prost/tonic attributes are injected via build.rs; the proto itself
// needs no Rust-specific options.

import "google/protobuf/empty.proto";
import "google/protobuf/timestamp.proto";

// ---------------------------------------------------------------------------
// Service Definition
// ---------------------------------------------------------------------------

// LakehouseWorkerService is the single gRPC service exposed by every Worker
// sidecar.  The Coordinator is always the client.
service LakehouseWorkerService {

  // Submit a task for execution.  Returns a server-side stream of progress
  // messages.  The stream is closed by the server when the task reaches a
  // terminal state (SUCCEEDED, FAILED, or CANCELLED).
  rpc ExecuteTask(TaskRequest) returns (stream TaskProgress);

  // Request immediate cancellation of a running task.  Safe to call even if
  // the task has already finished; the response reflects the final state.
  rpc CancelTask(CancelRequest) returns (CancelResponse);

  // Return current resource utilisation and slot availability.  Used by the
  // Coordinator for admission control and load-aware scheduling.
  rpc GetWorkerStatus(google.protobuf.Empty) returns (WorkerStatusResponse);

  // Register a scalar or aggregate UDF that subsequent tasks may reference.
  // Idempotent: registering the same (name, extension_uri) pair twice is a
  // no-op.
  rpc RegisterUDF(UDFRegistration) returns (UDFRegistrationResponse);
}

// ---------------------------------------------------------------------------
// Core Task Messages
// ---------------------------------------------------------------------------

// TaskRequest is the top-level message the Coordinator sends to dispatch work.
message TaskRequest {
  // Globally unique query identifier (UUID v4, 36 chars).
  string query_id = 1;

  // Stage number within the query plan (0-indexed).
  uint32 stage_id = 2;

  // Task number within the stage (0-indexed; one task per file partition).
  uint32 task_id = 3;

  // The query plan to execute.  SQL is the primary path (v1); Substrait
  // is available as an optional future path for pre-compiled plans.
  oneof plan {
    string sql = 4;           // SQL string (primary path, v1)
    bytes substrait_plan = 14; // Substrait Plan proto (optional, future)
  }

  // Files this task must read.
  repeated FileAssignment file_assignments = 5;

  // How this task should write its output for the next shuffle stage.
  // Absent when this is the final stage (output goes to coordinator via
  // Arrow Flight).
  ShuffleOutputConfig shuffle_output_config = 6;

  // Shuffle inputs from a previous stage that this task must read before
  // it can begin execution.  Empty for the initial scan stage.
  repeated ShuffleInput shuffle_inputs = 7;

  // Soft memory limit in bytes.  The worker SHOULD respect this; exceeding
  // it triggers spill-to-disk before returning RESOURCE_EXHAUSTED.
  uint64 memory_limit_bytes = 8;

  // Absolute deadline as milliseconds since the Unix epoch.  The worker
  // MUST cancel execution and return DEADLINE_EXCEEDED once this time passes.
  int64 deadline_ms = 9;

  // Opaque key-value metadata forwarded from the original query context
  // (e.g. tenant ID, trace ID, cost-centre tag).
  map<string, string> context_labels = 10;
}

// FileAssignment describes a single file (or file slice) assigned to this task.
message FileAssignment {
  // Absolute path or object-store URI (e.g. s3://bucket/prefix/part-00.parquet).
  string file_path = 1;

  // Uncompressed byte size hint used for progress reporting and memory budgeting.
  int64 file_size_bytes = 2;

  // Data format of the file.
  FileFormat format = 3;

  // Partition column values extracted from the Hive-style directory path
  // (e.g. {"year": "2024", "month": "01"}).  Workers inject these as
  // virtual columns into the DataFusion scan.
  map<string, string> partition_values = 4;

  // Optional byte-range within the file to read ([offset, offset+length)).
  // Both fields are 0 when the full file should be read.
  int64 byte_offset = 5;
  int64 byte_length = 6;
}

// FileFormat enumerates the supported input file formats.
enum FileFormat {
  FILE_FORMAT_UNSPECIFIED = 0;
  FILE_FORMAT_PARQUET     = 1;
  FILE_FORMAT_ORC         = 2;
  FILE_FORMAT_ARROW_IPC   = 3;  // Output of a previous shuffle stage.
  FILE_FORMAT_CSV         = 4;
  FILE_FORMAT_JSON        = 5;
}

// ShuffleOutputConfig controls how a task partitions and writes its output.
message ShuffleOutputConfig {
  ShuffleMethod method = 1;

  // Zero-based indices into the output schema of columns used for
  // partitioning.  Ignored when method is BROADCAST or SINGLE.
  repeated uint32 partition_column_indices = 2;

  // Target number of output partitions.
  uint32 num_partitions = 3;

  // For PUSH_BASED method: worker endpoints that will receive Arrow Flight
  // streams.  Length must equal num_partitions.
  repeated WorkerEndpoint target_endpoints = 4;

  // Directory URI where PULL_BASED shuffle files are materialised.
  // E.g. "file:///tmp/shuffle/query-uuid/stage-1/"
  string output_directory_uri = 5;
}

// ShuffleMethod enumerates the supported shuffle transport modes.
enum ShuffleMethod {
  SHUFFLE_METHOD_UNSPECIFIED = 0;
  // Coordinator fetches output via Arrow Flight (coordinator-pull).
  SHUFFLE_METHOD_COORDINATOR_PULL = 1;
  // Worker writes Arrow IPC files; peer workers pull them (file-based).
  SHUFFLE_METHOD_FILE_PULL        = 2;
  // Worker actively pushes Arrow Flight streams to peer workers.
  SHUFFLE_METHOD_PUSH             = 3;
  // Output is replicated to all downstream tasks (for broadcast joins).
  SHUFFLE_METHOD_BROADCAST        = 4;
}

// ShuffleInput describes one upstream partition this task must consume.
message ShuffleInput {
  // Stage that produced this shuffle output.
  uint32 source_stage_id = 1;

  // Endpoints of the workers that hold the data.  The local worker fetches
  // data from these peers via Arrow Flight.
  repeated WorkerEndpoint source_endpoints = 2;

  // The specific partition index within the source stage output.
  uint32 partition_id = 3;

  // URI of the Arrow IPC file when method is FILE_PULL.
  string file_uri = 4;
}

// WorkerEndpoint identifies a single Arrow Flight server on a worker sidecar.
message WorkerEndpoint {
  string host        = 1;
  uint32 flight_port = 2;
}

// ---------------------------------------------------------------------------
// Progress and Status Messages
// ---------------------------------------------------------------------------

// TaskProgress is streamed from worker to coordinator during task execution.
message TaskProgress {
  string     task_id          = 1;
  TaskState  state            = 2;

  // Cumulative rows emitted so far.
  int64  rows_processed    = 3;

  // Cumulative bytes read from storage.
  int64  bytes_read        = 4;

  // Cumulative bytes written to shuffle output.
  int64  bytes_written     = 5;

  // Peak heap + spill memory used by this task, in bytes.
  int64  peak_memory_bytes = 6;

  // Human-readable error message.  Non-empty only in FAILED state.
  string error_message     = 7;

  // Structured error code for programmatic handling.
  TaskErrorCode error_code = 8;

  // Completion percentage [0, 100].  May be approximate.
  float  progress_percent  = 9;

  // Wall-clock timestamp of this message on the worker.
  google.protobuf.Timestamp timestamp = 10;

  // Spill-to-disk bytes written since the last progress message.
  int64 spill_bytes = 11;
}

// TaskState is the lifecycle state machine for a task.
enum TaskState {
  TASK_STATE_UNSPECIFIED  = 0;
  TASK_STATE_QUEUED       = 1;  // Accepted, waiting for a DataFusion thread.
  TASK_STATE_RUNNING      = 2;  // Actively executing.
  TASK_STATE_SUCCEEDED    = 3;  // Terminal: all output written.
  TASK_STATE_FAILED       = 4;  // Terminal: unrecoverable error.
  TASK_STATE_CANCELLED    = 5;  // Terminal: cancelled by coordinator request.
}

// TaskErrorCode enables the coordinator to decide retry vs. fail-fast.
enum TaskErrorCode {
  TASK_ERROR_UNSPECIFIED         = 0;
  TASK_ERROR_IO                  = 1;  // Transient storage error — safe to retry.
  TASK_ERROR_SCHEMA_MISMATCH     = 2;  // Fatal — do not retry.
  TASK_ERROR_OOM                 = 3;  // Out of memory — retry on different node.
  TASK_ERROR_DEADLINE_EXCEEDED   = 4;  // Deadline passed — coordinator-level retry.
  TASK_ERROR_PLAN_INVALID        = 5;  // SQL/plan could not be parsed by DataFusion.
  TASK_ERROR_UDF_NOT_FOUND       = 6;  // Referenced UDF not registered.
  TASK_ERROR_INTERNAL            = 99; // Worker bug — alert and do not retry.
}

// WorkerStatusResponse gives the coordinator a current snapshot of the worker.
message WorkerStatusResponse {
  // Total execution slots (configured concurrency ceiling).
  uint32 total_slots  = 1;

  // Currently available slots (total_slots minus active tasks).
  uint32 free_slots   = 2;

  // Total JVM/RSS memory visible to the worker process, in bytes.
  int64  memory_total = 3;

  // Currently allocated memory (heap + off-heap + spill buffers), in bytes.
  int64  memory_used  = 4;

  // Number of tasks currently in QUEUED or RUNNING state.
  uint32 active_tasks = 5;

  // CPU utilisation [0.0, 100.0] averaged over the last 5 seconds.
  float  cpu_percent  = 6;

  // Worker software version string (semver).
  string version      = 7;

  // Monotonically increasing generation counter; bumped on worker restart.
  uint64 generation   = 8;
}

// ---------------------------------------------------------------------------
// Cancel Messages
// ---------------------------------------------------------------------------

message CancelRequest {
  string query_id = 1;
  uint32 stage_id = 2;
  uint32 task_id  = 3;
}

message CancelResponse {
  // State of the task at the time this response was generated.
  TaskState final_state = 1;

  // True if the cancellation signal was delivered to a running task.
  // False if the task had already reached a terminal state.
  bool was_running = 2;
}

// ---------------------------------------------------------------------------
// UDF Messages
// ---------------------------------------------------------------------------

// UDFRegistration sends a UDF descriptor to the worker.  The worker resolves
// the implementation at the extension_uri and caches it for future tasks.
message UDFRegistration {
  // Logical name used in SQL/Substrait expressions.
  string name = 1;

  // URI pointing to the compiled UDF artefact
  // (e.g. "file:///opt/lakehouse/udfs/my_udf.so" or
  //        "s3://lakehouse-udfs/v1/my_udf.wasm").
  string extension_uri = 2;

  // Type expression for the return value (Substrait encoding or Arrow schema bytes).
  bytes return_type = 3;

  // Type expressions for each argument, in order (Substrait encoding or Arrow schema bytes).
  repeated bytes arg_types = 4;

  // Whether the function is deterministic (enables constant-folding).
  bool is_deterministic = 5;

  // Execution runtime required by the UDF artefact.
  UDFRuntime runtime = 6;
}

// UDFRuntime specifies how the worker loads and sandboxes the UDF.
enum UDFRuntime {
  UDF_RUNTIME_UNSPECIFIED = 0;
  UDF_RUNTIME_NATIVE_SO   = 1;  // Native shared object (unsafe; pre-approved only).
  UDF_RUNTIME_WASM        = 2;  // WebAssembly (sandboxed via wasmtime).
  UDF_RUNTIME_ARROW_FLIGHT= 3;  // Remote UDF over Arrow Flight RPC.
}

message UDFRegistrationResponse {
  bool   success       = 1;
  string error_message = 2;

  // Final name under which the UDF was registered (may differ from requested
  // name if a namespace prefix was applied).
  string registered_name = 3;
}
```

---

## 3. Java Client Wrapper Interfaces (Coordinator Side)

### 3.1 Module Structure

```
coordinator/
└── src/main/java/org/opensearch/lakehouse/
    ├── grpc/
    │   ├── WorkerClient.java          // Per-worker gRPC stub wrapper
    │   ├── WorkerClientPool.java      // Channel pool and lifecycle manager
    │   ├── TaskProgressObserver.java  // StreamObserver adapter
    │   └── GrpcRetryPolicy.java       // Retry logic and backoff
    └── proto/                         // Generated sources (do not edit)
```

### 3.2 WorkerClient Interface

```java
package org.opensearch.lakehouse.grpc;

import org.opensearch.lakehouse.proto.*;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Wraps a single gRPC channel to one worker sidecar.
 * All methods are non-blocking; results are delivered via CompletableFuture
 * or callbacks.
 */
public interface WorkerClient extends AutoCloseable {

    /**
     * Returns the endpoint this client is connected to.
     */
    WorkerEndpoint endpoint();

    /**
     * Submits a task for execution and subscribes to its progress stream.
     *
     * The returned future completes when the stream is established (i.e., the
     * first TaskProgress message has been received or the RPC fails to open).
     * Progress messages are delivered to {@code progressConsumer} on an IO
     * thread until the task reaches a terminal state.
     *
     * @param request         the fully populated TaskRequest
     * @param progressConsumer called for each TaskProgress message; must not block
     * @param timeout         maximum wait for stream open (not for task completion)
     * @return future that resolves to the initial QUEUED or RUNNING TaskProgress
     * @throws WorkerUnavailableException if the channel is unhealthy
     */
    CompletableFuture<TaskProgress> executeTask(
            TaskRequest request,
            Consumer<TaskProgress> progressConsumer,
            Duration timeout);

    /**
     * Requests cancellation of a task.
     *
     * Safe to call after the task has already completed; the response reflects
     * the final observed state.
     *
     * @param queryId identifies the query
     * @param stageId stage within the query
     * @param taskId  task within the stage
     * @return future that resolves to the CancelResponse
     */
    CompletableFuture<CancelResponse> cancelTask(
            String queryId, int stageId, int taskId);

    /**
     * Fetches the current resource utilisation snapshot from the worker.
     * Cached for up to {@code staleness}; a fresh RPC is made only when stale.
     *
     * @param staleness maximum acceptable age of the cached status
     * @return future that resolves to the WorkerStatusResponse
     */
    CompletableFuture<WorkerStatusResponse> getWorkerStatus(Duration staleness);

    /**
     * Registers a UDF on this worker.  Idempotent.
     *
     * @param registration the UDF descriptor
     * @return future that resolves to the UDFRegistrationResponse
     */
    CompletableFuture<UDFRegistrationResponse> registerUdf(UDFRegistration registration);

    /**
     * Returns true if the underlying gRPC channel is in READY or IDLE state.
     */
    boolean isHealthy();

    /**
     * Drains in-flight RPCs and shuts down the channel.
     * Blocks until drain is complete or the timeout elapses.
     */
    void shutdown(Duration drainTimeout);
}
```

### 3.3 WorkerClientPool Interface

```java
package org.opensearch.lakehouse.grpc;

import org.opensearch.lakehouse.proto.WorkerEndpoint;
import org.opensearch.lakehouse.proto.WorkerStatusResponse;

import java.util.List;
import java.util.Optional;

/**
 * Manages the collection of WorkerClient instances for the entire cluster.
 * Responsible for channel lifecycle, health monitoring, and endpoint discovery.
 */
public interface WorkerClientPool extends AutoCloseable {

    /**
     * Returns a healthy WorkerClient for the given endpoint, creating and
     * caching a new one if necessary.
     *
     * @throws WorkerUnavailableException when the endpoint is unreachable
     */
    WorkerClient getClient(WorkerEndpoint endpoint);

    /**
     * Returns WorkerClients for all currently known healthy workers, ordered
     * by ascending CPU utilisation (lowest-load first) for greedy scheduling.
     */
    List<WorkerClient> allHealthyClients();

    /**
     * Returns the least-loaded worker that has at least one free slot,
     * or empty if all workers are saturated.
     */
    Optional<WorkerClient> leastLoaded();

    /**
     * Forces an immediate health check against all known endpoints and updates
     * internal status caches.  Called on coordinator startup and every
     * {@code health.check.interval.ms} thereafter.
     */
    void refreshAll();

    /**
     * Signals that an endpoint should be treated as temporarily unavailable
     * and removed from the healthy set for {@code banDuration}.
     */
    void banEndpoint(WorkerEndpoint endpoint, java.time.Duration banDuration);

    /**
     * Registers a listener that is notified whenever the pool's view of
     * worker availability changes (node added, removed, or health-changed).
     */
    void addChangeListener(WorkerPoolChangeListener listener);
}
```

### 3.4 TaskProgressObserver

```java
package org.opensearch.lakehouse.grpc;

import io.grpc.stub.StreamObserver;
import org.opensearch.lakehouse.proto.TaskProgress;
import org.opensearch.lakehouse.proto.TaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Adapts the gRPC StreamObserver callback model to the CompletableFuture /
 * Consumer model used by WorkerClient.
 *
 * Thread-safety: onNext, onError, and onCompleted may be called from any
 * gRPC IO thread.  All state transitions are guarded by the intrinsic lock.
 */
public class TaskProgressObserver implements StreamObserver<TaskProgress> {

    private static final Logger log = LoggerFactory.getLogger(TaskProgressObserver.class);

    private final CompletableFuture<TaskProgress> openFuture;
    private final Consumer<TaskProgress>           progressConsumer;
    private volatile boolean firstMessageReceived = false;

    public TaskProgressObserver(
            CompletableFuture<TaskProgress> openFuture,
            Consumer<TaskProgress> progressConsumer) {
        this.openFuture       = openFuture;
        this.progressConsumer = progressConsumer;
    }

    @Override
    public synchronized void onNext(TaskProgress progress) {
        if (!firstMessageReceived) {
            firstMessageReceived = true;
            openFuture.complete(progress);
        }
        try {
            progressConsumer.accept(progress);
        } catch (Exception e) {
            log.warn("Progress consumer threw; ignoring", e);
        }
    }

    @Override
    public synchronized void onError(Throwable t) {
        log.error("Task stream error", t);
        if (!firstMessageReceived) {
            openFuture.completeExceptionally(t);
        }
        // Surface a synthetic FAILED progress to the consumer so upstream
        // state machines can transition correctly.
        progressConsumer.accept(
            TaskProgress.newBuilder()
                .setState(TaskState.TASK_STATE_FAILED)
                .setErrorMessage(t.getMessage() != null ? t.getMessage() : t.getClass().getName())
                .build());
    }

    @Override
    public synchronized void onCompleted() {
        log.debug("Task progress stream closed by server");
        // Normal close; terminal state was already delivered via onNext.
    }
}
```

### 3.5 GrpcRetryPolicy

```java
package org.opensearch.lakehouse.grpc;

import io.grpc.Status;

import java.time.Duration;

/**
 * Encapsulates retry eligibility rules and exponential-backoff parameters.
 *
 * Used by WorkerClient implementations to decide whether to retry a failed
 * RPC and how long to wait before the next attempt.
 */
public class GrpcRetryPolicy {

    private final int     maxAttempts;
    private final Duration initialBackoff;
    private final double  backoffMultiplier;
    private final Duration maxBackoff;
    private final Duration jitter;

    /** Factory method with production defaults. */
    public static GrpcRetryPolicy defaultPolicy() {
        return new GrpcRetryPolicy(4, Duration.ofMillis(100), 2.0,
                                   Duration.ofSeconds(5), Duration.ofMillis(50));
    }

    public GrpcRetryPolicy(int maxAttempts, Duration initialBackoff,
                            double backoffMultiplier, Duration maxBackoff,
                            Duration jitter) {
        this.maxAttempts       = maxAttempts;
        this.initialBackoff    = initialBackoff;
        this.backoffMultiplier = backoffMultiplier;
        this.maxBackoff        = maxBackoff;
        this.jitter            = jitter;
    }

    /**
     * Returns true if the given gRPC status code is safe to retry.
     *
     * Only idempotent, transient errors are retryable.  INVALID_ARGUMENT and
     * ALREADY_EXISTS are never retried because they indicate a client bug.
     */
    public boolean isRetryable(Status status) {
        return switch (status.getCode()) {
            case UNAVAILABLE,       // Worker restarting or load-shedding.
                 RESOURCE_EXHAUSTED,// Temporary memory pressure on worker.
                 DEADLINE_EXCEEDED, // Network latency spike (re-sent with fresh deadline).
                 INTERNAL           -> true;  // Transient worker bug (limited retries).
            default                 -> false;
        };
    }

    /**
     * Computes the wait duration before attempt {@code attemptNumber} (1-based).
     * Returns Duration.ZERO for the first attempt.
     */
    public Duration backoffFor(int attemptNumber) {
        if (attemptNumber <= 1) return Duration.ZERO;
        long baseMs = (long)(initialBackoff.toMillis()
                * Math.pow(backoffMultiplier, attemptNumber - 2));
        long cappedMs = Math.min(baseMs, maxBackoff.toMillis());
        long jitterMs = (long)(Math.random() * jitter.toMillis());
        return Duration.ofMillis(cappedMs + jitterMs);
    }

    public int maxAttempts() { return maxAttempts; }
}
```

---

## 4. Rust Server Trait Definitions (Worker Side)

### 4.1 Cargo.toml Dependencies (relevant excerpt)

```toml
[dependencies]
tonic          = { version = "0.11", features = ["tls", "tls-roots"] }
prost          = "0.12"
tokio          = { version = "1", features = ["full"] }
tokio-stream   = "0.1"
datafusion     = "37"
arrow          = "51"
arrow-flight   = "51"
substrait      = { version = "0.35", optional = true }  # Only needed if Substrait plan path is enabled

[build-dependencies]
tonic-build = "0.11"
```

### 4.2 build.rs

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .type_attribute("lakehouse.v1.TaskRequest",   "#[derive(Debug, Clone)]")
        .type_attribute("lakehouse.v1.TaskProgress",  "#[derive(Debug, Clone)]")
        .compile(
            &["proto/lakehouse/v1/worker.proto"],
            &["proto", "third_party/protobuf"],
        )?;
    Ok(())
}
```

### 4.3 Core Trait

```rust
// src/grpc/service.rs

use crate::proto::lakehouse::v1::{
    CancelRequest, CancelResponse, TaskRequest, TaskProgress,
    UdfRegistration, UdfRegistrationResponse, WorkerStatusResponse,
};
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

/// The primary trait implemented by the worker's gRPC service handler.
///
/// Implementations MUST be Send + Sync + 'static because tonic requires it.
/// Each method corresponds to an RPC in LakehouseWorkerService.
#[async_trait::async_trait]
pub trait WorkerService: Send + Sync + 'static {
    /// The type of stream returned by execute_task.
    /// Must be a stream of Result<TaskProgress, Status>.
    type ExecuteTaskStream: Stream<Item = Result<TaskProgress, Status>>
        + Send
        + 'static;

    /// Execute the given task and stream progress updates back.
    ///
    /// Implementation responsibilities:
    /// 1. Validate the request (non-empty sql or substrait_plan, valid deadline_ms).
    /// 2. Acquire an execution slot from the semaphore; queue if none available.
    /// 3. Parse the SQL via DataFusion's SQL parser (or decode the Substrait plan
    ///    if provided) and spawn execution in a dedicated Tokio task group.
    /// 4. Create an mpsc channel; the DataFusion task sends TaskProgress to the
    ///    sender; this method returns a ReceiverStream wrapping the receiver.
    /// 5. The first message sent MUST have state TASK_STATE_QUEUED or
    ///    TASK_STATE_RUNNING.
    /// 6. The last message MUST have a terminal state.
    async fn execute_task(
        &self,
        request: Request<TaskRequest>,
    ) -> Result<Response<Self::ExecuteTaskStream>, Status>;

    /// Cancel the identified task.
    ///
    /// Implementation must look up the task by (query_id, stage_id, task_id),
    /// send a cancellation signal to its CancellationToken, and wait up to
    /// 500 ms for acknowledgement before returning.
    async fn cancel_task(
        &self,
        request: Request<CancelRequest>,
    ) -> Result<Response<CancelResponse>, Status>;

    /// Return current resource utilisation.
    ///
    /// Implementation MUST NOT block.  Read from shared AtomicU64/AtomicF32
    /// metrics that background threads update.
    async fn get_worker_status(
        &self,
        request: Request<()>,
    ) -> Result<Response<WorkerStatusResponse>, Status>;

    /// Register a UDF.  Must be idempotent.
    async fn register_udf(
        &self,
        request: Request<UdfRegistration>,
    ) -> Result<Response<UdfRegistrationResponse>, Status>;
}
```

### 4.4 Concrete Handler Skeleton

```rust
// src/grpc/handler.rs

use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use dashmap::DashMap;

use crate::executor::TaskExecutor;
use crate::proto::lakehouse::v1::*;
use super::service::WorkerService;

/// Shared state accessible to all concurrent RPC handlers.
pub struct WorkerHandler {
    /// Limits concurrent DataFusion executions.
    slots:    Arc<Semaphore>,
    /// Maps (query_id, stage_id, task_id) → CancellationToken.
    tasks:    Arc<DashMap<TaskKey, CancellationToken>>,
    /// Parses SQL (or decodes Substrait plans) and runs them via DataFusion,
    /// emitting TaskProgress via mpsc channels.
    executor: Arc<TaskExecutor>,
    /// Shared metrics updated by background threads.
    metrics:  Arc<WorkerMetrics>,
}

impl WorkerHandler {
    pub fn new(max_slots: usize, executor: Arc<TaskExecutor>,
               metrics: Arc<WorkerMetrics>) -> Self {
        Self {
            slots:    Arc::new(Semaphore::new(max_slots)),
            tasks:    Arc::new(DashMap::new()),
            executor,
            metrics,
        }
    }
}

#[async_trait::async_trait]
impl WorkerService for WorkerHandler {
    type ExecuteTaskStream = ReceiverStream<Result<TaskProgress, Status>>;

    async fn execute_task(
        &self,
        request: Request<TaskRequest>,
    ) -> Result<Response<Self::ExecuteTaskStream>, Status> {
        let req = request.into_inner();

        // Validate deadline before queueing.
        let deadline_ms = req.deadline_ms;
        let now_ms = chrono::Utc::now().timestamp_millis();
        if deadline_ms > 0 && deadline_ms <= now_ms {
            return Err(Status::deadline_exceeded("Task deadline already passed"));
        }

        let (tx, rx) = mpsc::channel::<Result<TaskProgress, Status>>(64);
        let token    = CancellationToken::new();
        let key      = TaskKey::from(&req);
        self.tasks.insert(key.clone(), token.clone());

        let slots_clone    = self.slots.clone();
        let executor_clone = self.executor.clone();
        let tasks_clone    = self.tasks.clone();

        // Emit QUEUED immediately so the coordinator knows the task is accepted.
        let _ = tx.send(Ok(TaskProgress {
            task_id: format!("{}/{}/{}", req.query_id, req.stage_id, req.task_id),
            state:   TaskState::TaskStateQueued as i32,
            ..Default::default()
        })).await;

        tokio::spawn(async move {
            // Acquire execution slot (backpressure: blocks if at capacity).
            let _permit = slots_clone.acquire().await.expect("semaphore closed");

            // Notify coordinator we are now running.
            let _ = tx.send(Ok(TaskProgress {
                state: TaskState::TaskStateRunning as i32,
                ..Default::default()
            })).await;

            // Run the DataFusion plan; executor calls tx.send() periodically.
            let result = executor_clone
                .execute(req, token, tx.clone())
                .await;

            // Emit terminal state.
            let terminal = match result {
                Ok(stats) => TaskProgress {
                    state:            TaskState::TaskStateSucceeded as i32,
                    rows_processed:   stats.rows_processed,
                    bytes_read:       stats.bytes_read,
                    bytes_written:    stats.bytes_written,
                    peak_memory_bytes:stats.peak_memory_bytes,
                    progress_percent: 100.0,
                    ..Default::default()
                },
                Err(e) => TaskProgress {
                    state:         TaskState::TaskStateFailed as i32,
                    error_message: e.to_string(),
                    error_code:    e.error_code() as i32,
                    ..Default::default()
                },
            };
            let _ = tx.send(Ok(terminal)).await;
            tasks_clone.remove(&key);
            // _permit is dropped here, releasing the slot.
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn cancel_task(
        &self,
        request: Request<CancelRequest>,
    ) -> Result<Response<CancelResponse>, Status> {
        let req = request.into_inner();
        let key = TaskKey::new(&req.query_id, req.stage_id, req.task_id);

        let (was_running, final_state) = if let Some(token) = self.tasks.get(&key) {
            token.cancel();
            (true, TaskState::TaskStateCancelled as i32)
        } else {
            (false, TaskState::TaskStateSucceeded as i32) // already done
        };

        Ok(Response::new(CancelResponse { final_state, was_running }))
    }

    async fn get_worker_status(
        &self,
        _request: Request<()>,
    ) -> Result<Response<WorkerStatusResponse>, Status> {
        let total = self.slots.available_permits() as u32
                  + self.metrics.active_tasks.load(std::sync::atomic::Ordering::Relaxed) as u32;
        Ok(Response::new(WorkerStatusResponse {
            total_slots:   total,
            free_slots:    self.slots.available_permits() as u32,
            memory_total:  self.metrics.memory_total_bytes(),
            memory_used:   self.metrics.memory_used_bytes(),
            active_tasks:  self.metrics.active_tasks.load(std::sync::atomic::Ordering::Relaxed),
            cpu_percent:   self.metrics.cpu_percent(),
            version:       env!("CARGO_PKG_VERSION").to_string(),
            generation:    self.metrics.generation.load(std::sync::atomic::Ordering::Relaxed),
        }))
    }

    async fn register_udf(
        &self,
        request: Request<UdfRegistration>,
    ) -> Result<Response<UdfRegistrationResponse>, Status> {
        let reg = request.into_inner();
        match self.executor.register_udf(reg).await {
            Ok(name) => Ok(Response::new(UdfRegistrationResponse {
                success: true,
                registered_name: name,
                ..Default::default()
            })),
            Err(e) => Ok(Response::new(UdfRegistrationResponse {
                success: false,
                error_message: e.to_string(),
                ..Default::default()
            })),
        }
    }
}

// ---------------------------------------------------------------------------
// Helper types
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct TaskKey {
    pub query_id: String,
    pub stage_id: u32,
    pub task_id:  u32,
}

impl TaskKey {
    pub fn new(query_id: &str, stage_id: u32, task_id: u32) -> Self {
        Self { query_id: query_id.to_owned(), stage_id, task_id }
    }
}

impl From<&TaskRequest> for TaskKey {
    fn from(r: &TaskRequest) -> Self {
        Self::new(&r.query_id, r.stage_id, r.task_id)
    }
}
```

---

## 5. Error Handling and Retry Semantics

### 5.1 gRPC Status Code Mapping

| Scenario | Worker returns | Coordinator action |
|---|---|---|
| SQL/plan is invalid or unparseable | `INVALID_ARGUMENT` | Fail the query immediately; do not retry |
| Worker has no free slots | `RESOURCE_EXHAUSTED` | Retry on a different worker after backoff |
| Storage I/O error (transient) | `INTERNAL` + `TASK_ERROR_IO` | Retry same task on same worker up to 3 times |
| Worker OOM | `RESOURCE_EXHAUSTED` + `TASK_ERROR_OOM` | Retry on a different worker; reduce memory limit by 20% |
| Task deadline exceeded | `DEADLINE_EXCEEDED` | Re-send with adjusted deadline; count against query budget |
| Worker process crash (stream drops) | Client sees `UNAVAILABLE` | Mark worker unhealthy; reschedule task on healthy worker |
| Cancellation acknowledged | `OK` with `TASK_STATE_CANCELLED` | No retry needed |
| UDF not found | `NOT_FOUND` + `TASK_ERROR_UDF_NOT_FOUND` | Re-register UDF, then retry task once |

### 5.2 Retry Decision Tree

```
RPC fails
   │
   ├─ INVALID_ARGUMENT / NOT_FOUND (after UDF fix) / DATA_LOSS
   │      └── Fail query immediately
   │
   ├─ UNAVAILABLE
   │      └── Worker is down; ban endpoint 30s; pick new worker; retry task
   │
   ├─ RESOURCE_EXHAUSTED
   │      └── attempt < maxAttempts?
   │            ├── yes → wait backoff(attempt); retry on least-loaded worker
   │            └── no  → fail query with INSUFFICIENT_RESOURCES
   │
   ├─ INTERNAL (error_code == TASK_ERROR_IO)
   │      └── attempt < 3? → wait exponential backoff; retry same worker
   │
   ├─ DEADLINE_EXCEEDED
   │      └── query still within budget?
   │            ├── yes → recalculate task deadline; retry
   │            └── no  → fail query with TIMEOUT
   │
   └─ Everything else → fail query
```

### 5.3 Idempotency Guarantee

`ExecuteTask` is **idempotent per (queryId, stageId, taskId)** because:

1. Each task writes to a deterministic output path derived from those three IDs.
2. Workers reject a second `ExecuteTask` for an already-completed (queryId, stageId, taskId) with `ALREADY_EXISTS`, which the coordinator treats as success (checks output exists).
3. In-progress duplicate calls (racing retries) are serialised using a `DashMap` keyed on `TaskKey`; the second arrival receives an error and the coordinator retries after the first finishes.

---

## 6. Streaming Protocol: TaskProgress Streams

### 6.1 Stream Lifecycle

```
Coordinator                                Worker
    │                                          │
    │──── ExecuteTask(TaskRequest) ───────────►│
    │                                          │ (internal: spawn task)
    │◄─── TaskProgress(QUEUED) ───────────────│  ← stream opens
    │                                          │
    │◄─── TaskProgress(RUNNING, 0%) ──────────│
    │                                          │  (scan/compute in progress)
    │◄─── TaskProgress(RUNNING, 35%, ...) ────│  ← periodic heartbeat
    │◄─── TaskProgress(RUNNING, 70%, ...) ────│
    │                                          │
    │◄─── TaskProgress(SUCCEEDED, 100%) ──────│  ← terminal; server closes stream
    │                                          │
```

### 6.2 Heartbeat Interval

- Workers send a `RUNNING` progress message every **500 ms** while executing.
- If the coordinator receives no message for **2 seconds**, it considers the stream stalled and re-issues `CancelTask` followed by rescheduling.
- The 500 ms / 2 s values are configurable via `lakehouse.grpc.progress_interval_ms` and `lakehouse.grpc.stream_stall_timeout_ms`.

### 6.3 Terminal Message Semantics

The server MUST send exactly one terminal message (SUCCEEDED, FAILED, or CANCELLED) as the **last** message before closing the stream. The coordinator MUST NOT act on the terminal state until `onCompleted()` is fired; this prevents races where a final message is delivered but a subsequent error replaces it.

### 6.4 Progress Percentage Estimation

Workers use row-group metadata (for Parquet) to compute total expected rows at scan start, enabling accurate progress estimation:

```
progress_percent = (row_groups_scanned / total_row_groups) * 100
```

For non-Parquet formats or unknown total size, `progress_percent` is left at 0 until task completion.

### 6.5 Back-channel Signals

The Coordinator may close its side of the stream (via RST_STREAM) to signal urgent cancellation when the network is congested. Workers detect this via `CancellationToken` linked to the tonic request context and treat it identically to a `CancelTask` RPC.

---

## 7. Connection Management

### 7.1 Channel Pool Design

```java
// WorkerClientPool implementation outline

public class DefaultWorkerClientPool implements WorkerClientPool {

    // One ManagedChannel per worker endpoint, kept alive indefinitely.
    // Key: "host:grpcPort"
    private final ConcurrentHashMap<String, ManagedChannel> channels;

    // Cached WorkerClient wrappers (thin; re-created on channel replacement).
    private final ConcurrentHashMap<String, WorkerClient>   clients;

    // Background thread refreshes status every health.check.interval.ms (default 5s).
    private final ScheduledExecutorService healthChecker;

    // Banned endpoints and their unban timestamps.
    private final ConcurrentHashMap<String, Instant> bannedUntil;
}
```

**Channel configuration (applied at channel creation):**

```java
ManagedChannelBuilder.forAddress(host, port)
    .useTransportSecurity()                          // TLS always on
    .sslContext(buildSslContext())                   // Node cert (§8)
    .keepAliveTime(30, TimeUnit.SECONDS)             // TCP keepalive
    .keepAliveTimeout(10, TimeUnit.SECONDS)
    .keepAliveWithoutCalls(true)                     // Maintain channel even when idle
    .maxInboundMessageSize(256 * 1024 * 1024)        // 256 MiB for large query plans
    .maxRetryAttempts(0)                             // Retries handled by GrpcRetryPolicy
    .intercept(new TracingInterceptor(),
               new MetricsInterceptor())
    .build();
```

### 7.2 Reconnection Strategy

gRPC's built-in connectivity state machine handles reconnection transparently. The pool adds:

1. **Proactive health checks** via `GetWorkerStatus` every 5 s.
2. **Exponential backoff on repeated failure**: 1 s → 2 s → 4 s → max 30 s.
3. **Circuit breaker**: after 3 consecutive health-check failures the worker is marked `DOWN` and its endpoint is banned for 60 s.
4. **Drain on graceful shutdown**: the OpenSearch plugin's `close()` lifecycle hook calls `pool.drainAll(Duration.ofSeconds(30))`.

### 7.3 Timeout Hierarchy

| Timeout | Default | Description |
|---|---|---|
| `connect_timeout_ms` | 3,000 | Fail fast if the sidecar is not listening |
| `stream_open_timeout_ms` | 5,000 | Wait for first TaskProgress message |
| `rpc_deadline_ms` | Derived from `TaskRequest.deadline_ms` | Hard cap on any single RPC |
| `stream_stall_timeout_ms` | 2,000 | Max gap between consecutive TaskProgress messages |
| `cancel_ack_timeout_ms` | 1,000 | Max wait for CancelResponse |
| `status_cache_ttl_ms` | 2,000 | GetWorkerStatus cached response lifetime |

---

## 8. TLS/Security

### 8.1 Certificate Reuse

Worker sidecars are co-located with OpenSearch data nodes. Each data node already has a TLS certificate issued by the OpenSearch security plugin's internal CA (stored in the keystore). The sidecar reuses the **same certificate and private key** for its gRPC listener:

```
OpenSearch keystore path:  $OPENSEARCH_PATH_CONF/node.pem  (cert + chain)
                           $OPENSEARCH_PATH_CONF/node.key  (private key)
```

The sidecar reads these at startup via a `SecurityCertProvider` interface that wraps OpenSearch's `KeyStoreUtil`.

### 8.2 Server-side TLS Configuration (Rust/tonic)

```rust
// src/grpc/server.rs

use tonic::transport::{Server, Identity, ServerTlsConfig};

pub async fn start_grpc_server(
    addr: std::net::SocketAddr,
    cert_pem: Vec<u8>,
    key_pem:  Vec<u8>,
    handler:  WorkerHandler,
) -> Result<(), Box<dyn std::error::Error>> {

    let identity = Identity::from_pem(cert_pem, key_pem);
    let tls_config = ServerTlsConfig::new().identity(identity);

    Server::builder()
        .tls_config(tls_config)?
        .add_service(
            LakehouseWorkerServiceServer::new(handler)
                .max_decoding_message_size(256 * 1024 * 1024)
                .max_encoding_message_size(256 * 1024 * 1024),
        )
        .serve(addr)
        .await?;

    Ok(())
}
```

### 8.3 Client-side TLS Configuration (Java)

```java
// Coordinator builds an SslContext using the cluster's CA certificate
// loaded from the OpenSearch trust store.

private SslContext buildSslContext(Path caCertPath) throws Exception {
    X509Certificate caCert = CertificateUtils.loadCert(caCertPath);
    return GrpcSslContexts.forClient()
        .trustManager(caCert)
        .keyManager(clientCert, clientKey)  // mTLS: coordinator presents its node cert
        .build();
}
```

### 8.4 Mutual TLS (mTLS)

Both sides validate certificates:

- **Worker** requires a client certificate signed by the cluster CA (`requireClientAuth = true` equivalent in tonic: `ServerTlsConfig::client_ca_root`).
- **Coordinator** verifies the worker's certificate CN matches the expected worker hostname to prevent SSRF to rogue endpoints.

### 8.5 Authorization

After mTLS, no additional application-level auth token is required within the cluster. Future multi-tenant isolation will be enforced at the SQL/plan level (row-filter expressions injected by the coordinator).

---

## 9. Backpressure and Flow Control

### 9.1 HTTP/2 Flow Control

gRPC runs over HTTP/2, which provides connection-level and stream-level flow control windows. The coordinator's `maxInboundMessageSize` and the worker's `max_encoding_message_size` cap individual message sizes (256 MiB). Default HTTP/2 initial window size (65 KiB) is overridden:

```java
// Coordinator channel builder
.flowControlWindow(4 * 1024 * 1024)  // 4 MiB per stream
```

```rust
// Worker server builder
Server::builder()
    .initial_stream_window_size(4 * 1024 * 1024)
    .initial_connection_window_size(32 * 1024 * 1024)
```

### 9.2 Execution Slot Semaphore

The `Semaphore` in `WorkerHandler` acts as the primary admission-control gate:

- Default capacity: `num_cpus * 2` (configurable via `LAKEHOUSE_WORKER_SLOTS`).
- A task waiting to acquire the semaphore emits `TASK_STATE_QUEUED` heartbeats every 500 ms so the coordinator can detect overload and potentially cancel.
- The coordinator checks `WorkerStatusResponse.free_slots` before dispatching; if `free_slots == 0`, it waits 200 ms and polls again before sending the task to that worker.

### 9.3 Progress Channel Backpressure

The `mpsc::channel(64)` between the DataFusion execution task and the gRPC stream handler has a fixed capacity of 64 messages. If the coordinator's network cannot consume progress updates fast enough, the sender (`tx.send(...).await`) will yield, slowing down the DataFusion task. This is intentional: it prevents unbounded memory growth from queued progress messages.

If the send times out (deadline exceeded), the DataFusion task cancels itself.

### 9.4 Coordinator-side Admission Control

The coordinator implements a token-bucket at the `WorkerClientPool` level:

- Maximum in-flight RPCs per worker: `total_slots * 1.5` (allows brief overcommit for pipelining).
- New task dispatches are queued in-process if the limit is reached.
- Queue bound: `total_workers * total_slots * 2`; beyond this, the query planner receives `QUEUE_FULL` and applies inter-query admission control.

---

## 10. Versioning Strategy

### 10.1 Protocol Numbering

The proto package is `lakehouse.v1`. Breaking changes require a new package (`lakehouse.v2`) and a new service endpoint. The `WorkerStatusResponse.version` field carries the worker's software version; the coordinator logs a warning when versions diverge.

### 10.2 Wire Compatibility Rules

| Change type | Allowed in `v1`? | Migration path |
|---|---|---|
| Add optional field to existing message | Yes (proto3 default values) | Forward/backward compatible |
| Add new RPC method to service | Yes (old clients ignore it) | Deploy workers first, then coordinator |
| Add enum value | Yes (old code uses default 0) | Add `UNSPECIFIED` guard in all switches |
| Rename field (same number) | Yes (wire is tag-based) | Update both sides in same release |
| Remove field | No (breaks wire) | Deprecate with `reserved`, then remove in `v2` |
| Change field number | No | Never; bump to `v2` |
| Change field type (incompatible) | No | Bump to `v2` |

### 10.3 Dual-Version Transition

When migrating from `v1` to `v2`:

1. Workers expose both `lakehouse.v1.LakehouseWorkerService` and `lakehouse.v2.LakehouseWorkerService` on the same port (gRPC server reflection used for discovery).
2. Coordinator upgrades first (speaks both); workers upgrade rolling.
3. After 100% worker upgrade, coordinator drops `v1` stubs.
4. Deprecation window: minimum one minor release cycle (≈ 2 weeks).

### 10.4 Service Reflection and Discovery

Workers register with gRPC server reflection:

```rust
use tonic_reflection::server::Builder as ReflectionBuilder;

Server::builder()
    .add_service(ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build()?)
    .add_service(LakehouseWorkerServiceServer::new(handler))
    .serve(addr)
    .await?;
```

This allows the coordinator to detect the protocol version exposed by each worker at runtime and choose the appropriate stub, enabling zero-downtime rolling upgrades.

### 10.5 Field Deprecation Example

```protobuf
message TaskRequest {
  // ... existing fields ...

  // Deprecated in v1.4; use shuffle_output_config.output_directory_uri instead.
  // Will be removed in v2.
  string shuffle_output_path = 100 [deprecated = true];
}
```

Deprecated fields are:
- Read by workers if set (backward compat with old coordinators).
- Ignored by coordinators once all workers are on a version that reads the replacement field.
- Removed only in the next major version package.

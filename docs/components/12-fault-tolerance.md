# Component 12: Fault Tolerance + Error Handling

## 1. Overview and Responsibilities

The Fault Tolerance and Error Handling subsystem ensures that distributed queries in the lakehouse engine complete successfully despite transient infrastructure failures. It operates as a cross-cutting concern woven through the coordinator, worker nodes, and task scheduler.

### Core Responsibilities

- Classify errors as retryable or fatal and route them to the appropriate handler
- Retry failed tasks with exponential backoff on alternate workers
- Detect and re-execute upstream stages whose shuffle output was lost due to worker failure
- Launch speculative task copies when stragglers are detected
- Enforce query guardrails (timeout, scan bytes, memory, result size)
- Persist coordinator state so a new coordinator can recover in-flight queries
- Handle S3 throttling and regional failures with jitter-based retries
- Propagate structured error messages to the client with query ID, stage, and root cause

### Non-Goals

- The system does not provide exactly-once semantics for writes; only read-side fault tolerance is addressed
- It does not dynamically add nodes to the cluster; it works within the fixed-node deployment

---

## 2. Java Interfaces

### 2.1 FaultToleranceManager

Central orchestrator. The coordinator holds one instance and routes all failure events through it.

```java
package org.opensearch.lakehouse.fault;

/**
 * Central coordinator for all fault tolerance actions.
 * Receives failure events and dispatches to the appropriate subsystems.
 */
public interface FaultToleranceManager {

    /**
     * Called when a worker reports task failure or when the coordinator
     * detects a task has exceeded its deadline without a heartbeat.
     *
     * @param event  Failure details including worker ID, task ID, error classification,
     *               attempt number, and exception payload.
     * @return       Decision: RETRY, FAIL_STAGE, or ABORT_QUERY.
     */
    TaskFailureDecision handleTaskFailure(TaskFailureEvent event);

    /**
     * Called when heartbeat monitoring determines a worker is dead.
     * Triggers impact analysis and re-assignment of all affected tasks.
     *
     * @param workerId   The worker that stopped responding.
     * @param deadSince  Wall-clock time of the last successful heartbeat.
     */
    void handleWorkerFailure(String workerId, Instant deadSince);

    /**
     * Called by the query timer when a query exceeds its maximum execution time.
     * Cancels all in-flight tasks for the query and returns a timeout error to the client.
     *
     * @param queryId     The query that timed out.
     * @param deadlineMs  The configured deadline in milliseconds.
     */
    void handleTimeout(String queryId, long deadlineMs);
}
```

---

### 2.2 RetryPolicy

Defines the rules for whether and how a task should be retried. Implementations are pluggable per error class.

```java
package org.opensearch.lakehouse.fault;

/**
 * Encapsulates retry rules for a category of errors.
 * The scheduler calls this before re-queuing a failed task.
 */
public interface RetryPolicy {

    /**
     * Maximum number of per-task retry attempts before the stage is failed.
     * Counted per task, not per stage.
     */
    int maxRetries();

    /**
     * Computes the delay before the next attempt.
     *
     * @param attemptNumber  1-based index of the attempt that just failed.
     * @return               Delay in milliseconds (may include jitter).
     */
    long backoffStrategy(int attemptNumber);

    /**
     * Returns the set of error types that are eligible for retry.
     * Errors not in this set are treated as fatal and skip the retry path.
     *
     * @return  Immutable set of retryable error classifications.
     */
    Set<ErrorType> retryableErrors();
}
```

**Default implementation: `ExponentialBackoffRetryPolicy`**

```java
public class ExponentialBackoffRetryPolicy implements RetryPolicy {

    private static final int MAX_RETRIES = 3;
    private static final long BASE_DELAY_MS = 200L;
    private static final double JITTER_FACTOR = 0.25;

    @Override public int maxRetries() { return MAX_RETRIES; }

    @Override
    public long backoffStrategy(int attemptNumber) {
        long delay = BASE_DELAY_MS * (1L << (attemptNumber - 1));   // 200, 400, 800 ms
        long jitter = (long) (delay * JITTER_FACTOR * Math.random());
        return delay + jitter;
    }

    @Override
    public Set<ErrorType> retryableErrors() {
        return EnumSet.of(
            ErrorType.S3_THROTTLE,
            ErrorType.S3_SLOW_DOWN,
            ErrorType.NETWORK_TIMEOUT,
            ErrorType.WORKER_UNAVAILABLE,
            ErrorType.TRANSIENT_IO
        );
    }
}
```

---

### 2.3 TaskRetryHandler

Decides whether a specific task instance should be retried and selects a target worker.

```java
package org.opensearch.lakehouse.fault;

/**
 * Per-task retry logic. Consulted by the scheduler after a task failure is
 * classified as retryable by RetryPolicy.
 */
public interface TaskRetryHandler {

    /**
     * Determines whether this specific task attempt should be retried.
     * Returns false if the attempt limit is reached or the error is fatal.
     *
     * @param context  Task identity, attempt count, and error details.
     * @return         true if the task should be re-queued.
     */
    boolean shouldRetry(TaskRetryContext context);

    /**
     * Returns the number of milliseconds to wait before re-queuing the task.
     * Delegates to RetryPolicy.backoffStrategy for the delay computation.
     *
     * @param context  Same context passed to shouldRetry.
     * @return         Delay in milliseconds.
     */
    long getRetryDelay(TaskRetryContext context);

    /**
     * Selects a worker for the retry attempt, explicitly excluding the worker
     * that produced the failure (and any other workers flagged as unhealthy).
     *
     * @param context          Task context including failed worker ID.
     * @param availableWorkers Live workers eligible to accept the task.
     * @return                 Worker ID to assign the retry to, or empty if none available.
     */
    Optional<String> getAlternateWorker(TaskRetryContext context, List<WorkerInfo> availableWorkers);
}
```

**`TaskRetryContext` value object:**

```java
public record TaskRetryContext(
    String queryId,
    int stageId,
    int taskId,
    String failedWorkerId,
    int attemptNumber,      // 1-based
    ErrorType errorType,
    String errorMessage,
    Instant failureTime
) {}
```

---

### 2.4 SpeculativeExecutor

Monitors stage progress and launches backup task copies for stragglers.

```java
package org.opensearch.lakehouse.fault;

/**
 * Identifies tasks that are running anomalously slowly and launches speculative
 * copies on different workers to mitigate straggler impact.
 */
public interface SpeculativeExecutor {

    /**
     * Scans the running tasks of a stage and returns any that qualify as
     * stragglers. A task qualifies when its elapsed runtime exceeds
     * STRAGGLER_THRESHOLD_MULTIPLIER (3x) times the median runtime of
     * completed tasks in the same stage.
     *
     * @param stageProgress  Current progress snapshot for all tasks in the stage.
     * @return               List of task IDs identified as stragglers.
     */
    List<Integer> detectStragglers(StageProgressSnapshot stageProgress);

    /**
     * Starts a speculative copy of the given task on a different worker.
     * Records the speculative task ID so the race can be resolved later.
     * No-op if the stage already has >= MAX_SPECULATIVE_FRACTION (10%) speculative tasks.
     *
     * @param originalTask  The straggler task to copy.
     * @param targetWorker  Worker to run the speculative copy on (must differ from original).
     * @return              The speculative task descriptor, or empty if the limit is reached.
     */
    Optional<SpeculativeTaskDescriptor> launchSpeculativeCopy(
        TaskDescriptor originalTask,
        WorkerInfo targetWorker
    );

    /**
     * Called when either the original or speculative copy completes first.
     * Cancels the slower copy and records the winning worker for lineage tracking.
     *
     * @param winner  The task attempt (original or speculative) that finished first.
     * @param loser   The task attempt to cancel.
     */
    void resolveRace(TaskAttempt winner, TaskAttempt loser);
}
```

**Constants:**

```java
public final class SpeculativeExecutorConstants {
    public static final double STRAGGLER_THRESHOLD_MULTIPLIER = 3.0;
    public static final double MAX_SPECULATIVE_FRACTION = 0.10;   // 10 % of stage tasks
    public static final int    MIN_COMPLETED_TASKS_FOR_MEDIAN = 3; // need at least 3 completions
}
```

---

### 2.5 QueryGuardrails

Enforces per-query resource ceilings. Workers report incremental usage; the coordinator enforces limits.

```java
package org.opensearch.lakehouse.fault;

/**
 * Defines the resource limits applied to a single query execution.
 * Limit values are set at query-plan time and checked continuously during execution.
 */
public interface QueryGuardrails {

    /** Wall-clock deadline for the entire query (milliseconds from submission). */
    long maxExecutionTimeMs();

    /** Maximum bytes that may be scanned from S3 / local storage before the query is aborted. */
    long maxScanBytes();

    /**
     * Maximum bytes allowed in shuffle (exchange) buffers across all workers.
     * Exceeding this limit triggers a SHUFFLE_OVERFLOW error, aborting the query.
     */
    long maxShuffleBytes();

    /** Maximum rows returned to the client. Rows beyond this limit are silently truncated. */
    long maxResultRows();

    /**
     * Maximum heap + off-heap memory a single worker may allocate for this query.
     * DataFusion first spills to disk; if RSS still exceeds this after spill, the
     * query is killed with OOM_KILLED error.
     */
    long maxMemoryPerQuery();

    /**
     * Returns true if any guardrail has been breached given the current usage snapshot.
     *
     * @param usage  Live usage counters aggregated from all workers.
     * @return       The violated guardrail, or empty if all limits are within bounds.
     */
    Optional<GuardrailViolation> check(QueryUsageSnapshot usage);
}
```

**`QueryUsageSnapshot`:**

```java
public record QueryUsageSnapshot(
    long elapsedMs,
    long scannedBytes,
    long shuffleBytes,
    long resultRows,
    long peakMemoryBytes    // max across all workers for this query
) {}

public record GuardrailViolation(GuardrailType type, String message) {}

public enum GuardrailType {
    TIMEOUT, SCAN_LIMIT, SHUFFLE_LIMIT, RESULT_LIMIT, MEMORY_LIMIT
}
```

---

### 2.6 CircuitBreaker

Prevents cascading failures by stopping requests to a consistently failing resource (e.g., a specific S3 prefix or a degraded worker).

```java
package org.opensearch.lakehouse.fault;

/**
 * Classic three-state circuit breaker (CLOSED -> OPEN -> HALF_OPEN -> CLOSED).
 * One instance per protected resource (worker endpoint, S3 bucket, etc.).
 */
public interface CircuitBreaker {

    /**
     * Returns true when the circuit is OPEN and requests should be rejected immediately
     * without attempting the operation.
     */
    boolean isOpen();

    /**
     * Records a successful operation. In HALF_OPEN state, sufficient successes
     * transition the circuit back to CLOSED.
     */
    void recordSuccess();

    /**
     * Records a failed operation. Increments the failure counter; transitions
     * to OPEN when the failure threshold is reached within the observation window.
     *
     * @param error  The error that triggered the failure record.
     */
    void recordFailure(Throwable error);

    /**
     * Forcibly resets the circuit to CLOSED state and clears all counters.
     * Called by the coordinator when a worker is confirmed healthy after recovery.
     */
    void reset();

    /** Current state of this circuit breaker instance. */
    CircuitBreakerState state();
}

public enum CircuitBreakerState { CLOSED, OPEN, HALF_OPEN }
```

**Configuration defaults:**

```
failureThreshold      = 5 failures within 60-second window
successThreshold      = 2 consecutive successes to close from HALF_OPEN
openDurationMs        = 30_000 ms before transitioning to HALF_OPEN
```

---

### 2.7 CoordinatorFailoverManager

Persists enough query execution state to allow a replacement coordinator to resume in-flight queries.

```java
package org.opensearch.lakehouse.fault;

/**
 * Manages durable query state so that a standby coordinator can take over
 * after the primary crashes or becomes unreachable.
 */
public interface CoordinatorFailoverManager {

    /**
     * Writes (or overwrites) the full execution state of a query to the
     * OpenSearch state index. Called after each stage transition, task
     * assignment change, and retry decision.
     *
     * @param state  Complete snapshot of query execution state.
     * @throws CoordinatorStateException if the write fails after retries.
     */
    void persistQueryState(QueryExecutionState state) throws CoordinatorStateException;

    /**
     * Called on coordinator startup. Reads the state index for all queries
     * in non-terminal states (RUNNING, PENDING_RETRY) and resumes their execution.
     * Sends re-registration requests to all workers so they report their
     * current task status to the new coordinator.
     *
     * @return  List of query IDs that were recovered and re-submitted to the scheduler.
     */
    List<String> recoverQueries();

    /**
     * Marks a query's state record as terminal (COMPLETED or FAILED) so it
     * is excluded from future recovery scans.
     *
     * @param queryId  The query to finalize.
     * @param terminal The terminal status to write.
     */
    void finalizeQueryState(String queryId, QueryTerminalStatus terminal);
}
```

**`QueryExecutionState` (persisted to OpenSearch):**

```java
public record QueryExecutionState(
    String queryId,
    String sqlText,
    String planSql,                                   // SQL string (primary, v1)
    QueryStatus status,
    List<StageExecutionState> stages,
    Map<Integer, String> taskToWorkerAssignment,   // taskId -> workerId
    int retryAttempt,
    Instant submittedAt,
    Instant lastUpdated,
    QueryGuardrailsConfig guardrails
) {}

public record StageExecutionState(
    int stageId,
    StageStatus status,
    List<TaskExecutionState> tasks,
    boolean shuffleOutputAvailable    // false if the producing worker died
) {}
```

---

## 3. Task Failure Handling

### 3.1 Error Classification

All errors are classified before any retry decision is made.

```
ErrorType (enum)
├── RETRYABLE
│   ├── S3_THROTTLE           — HTTP 503 SlowDown from S3
│   ├── S3_SLOW_DOWN          — HTTP 429 Too Many Requests
│   ├── NETWORK_TIMEOUT       — gRPC deadline exceeded on worker call
│   ├── WORKER_UNAVAILABLE    — Worker present but rejected the task (overloaded)
│   └── TRANSIENT_IO          — Disk read error on spill file (likely stale fd)
└── FATAL
    ├── OUT_OF_MEMORY         — Worker reports OOM even after spill attempt
    ├── CORRUPT_DATA          — Checksum mismatch on S3 object
    ├── INVALID_PLAN          — SQL plan rejected by DataFusion (parse or validation error)
    ├── SCHEMA_MISMATCH       — Column type mismatch at runtime
    └── INTERNAL_ERROR        — Unexpected worker panic / JVM crash signal
```

Classification logic lives in `ErrorClassifier.classify(Throwable t, int httpStatus)`. It inspects exception type, HTTP status code, and gRPC status codes.

### 3.2 Retry Flow

```
Task fails
    │
    ▼
ErrorClassifier.classify()
    │
    ├── FATAL ──────────────────────────────► FailStageException propagated to coordinator
    │
    └── RETRYABLE
            │
            ▼
        TaskRetryHandler.shouldRetry(context)
            │
            ├── attemptNumber >= maxRetries (3) ──► FailStageException
            │
            └── retry allowed
                    │
                    ▼
                getRetryDelay(context) → schedule delay
                    │
                    ▼
                getAlternateWorker(context, availableWorkers)
                    │
                    ├── no workers available ──► ABORT_QUERY
                    │
                    └── worker selected
                            │
                            ▼
                        Re-queue task on new worker
                        Increment attemptNumber
```

### 3.3 Retry Backoff Schedule

| Attempt | Base delay | Max jitter (25%) | Approximate range |
|---------|-----------|-----------------|-------------------|
| 1       | 200 ms    | 50 ms           | 200–250 ms        |
| 2       | 400 ms    | 100 ms          | 400–500 ms        |
| 3       | 800 ms    | 200 ms          | 800–1000 ms       |

After 3 failed attempts, the stage is failed. If the stage is not the first stage of the query, the error propagates to the coordinator which returns a structured error to the client.

---

## 4. Worker Failure Handling

### 4.1 Detection via Heartbeat

Each worker sends a heartbeat to the coordinator every `HEARTBEAT_INTERVAL_MS = 5000` ms. The coordinator runs a background `HeartbeatMonitor` thread that checks all worker registrations every `HEARTBEAT_CHECK_INTERVAL_MS = 2000` ms.

```java
// HeartbeatMonitor check logic (pseudo-code)
for (WorkerRegistration reg : workerRegistry.all()) {
    long silenceMs = now() - reg.lastHeartbeatMs();
    if (silenceMs > HEARTBEAT_TIMEOUT_MS) {           // default: 15_000 ms (3 missed)
        faultToleranceManager.handleWorkerFailure(reg.workerId(), reg.lastHeartbeat());
    }
}
```

`HEARTBEAT_TIMEOUT_MS = 15_000` (three missed heartbeats).

### 4.2 Impact Analysis

When `handleWorkerFailure(workerId, deadSince)` is called:

```java
WorkerImpactAnalysis impact = impactAnalyzer.analyze(workerId);
// impact contains:
//   Set<String>  affectedQueryIds
//   Set<TaskRef> runningTasks          — tasks currently assigned to dead worker
//   Set<StageRef> shuffleOutputStages  — stages whose output was buffered on dead worker
```

### 4.3 Shuffle Data Loss and Re-Execution

If the dead worker was holding shuffle output (exchange buffers) for a completed upstream stage, that stage's output is gone. The coordinator must re-execute it:

```
For each StageRef s in impact.shuffleOutputStages:
    s.status = PENDING_REEXECUTION
    s.shuffleOutputAvailable = false
    for each downstream stage that depends on s:
        cancel all running tasks in downstream stage
        mark downstream stage as WAITING_FOR_UPSTREAM
    re-submit all tasks of s to surviving workers
    persist updated QueryExecutionState
```

### 4.4 Task Reassignment

For running tasks on the dead worker:

```
For each TaskRef t in impact.runningTasks:
    context = TaskRetryContext(t, failedWorkerId=workerId, errorType=WORKER_UNAVAILABLE, attempt++)
    if TaskRetryHandler.shouldRetry(context):
        worker = getAlternateWorker(context, survivingWorkers)
        reassign t to worker
        update assignment map in QueryExecutionState
    else:
        fail the containing stage
```

### 4.5 Circuit Breaker Update

After a worker death, its `CircuitBreaker` instance is immediately set to `OPEN`. It transitions to `HALF_OPEN` only after the worker re-registers and sends two consecutive successful heartbeats. `reset()` is called on confirmed health.

---

## 5. Straggler Mitigation

### 5.1 Detection

The coordinator's `StageProgressMonitor` runs every `STRAGGLER_CHECK_INTERVAL_MS = 10_000` ms after at least `MIN_COMPLETED_TASKS_FOR_MEDIAN = 3` tasks in the stage have completed.

```java
// Straggler detection logic
long medianRuntimeMs = median(completedTasks.stream().map(TaskStatus::runtimeMs));
long threshold       = (long)(medianRuntimeMs * STRAGGLER_THRESHOLD_MULTIPLIER);  // 3x

List<Integer> stragglers = runningTasks.stream()
    .filter(t -> t.elapsedMs() > threshold)
    .map(TaskStatus::taskId)
    .collect(toList());
```

### 5.2 Speculative Launch Cap

Before launching a speculative copy, `SpeculativeExecutor` checks the cap:

```java
int totalTasks       = stage.totalTaskCount();
int speculativeNow   = stage.activeSpeculativeCount();
int maxSpeculative   = (int) Math.floor(totalTasks * MAX_SPECULATIVE_FRACTION);  // 10%

if (speculativeNow >= maxSpeculative) {
    return Optional.empty();   // cap reached, no new speculative tasks
}
```

### 5.3 Race Resolution

Both the original and speculative task write their output to the same logical partition key (identified by `stageId + partitionId`). The coordinator registers a `CompletionListener` on both task IDs. The first completion triggers:

```java
void resolveRace(TaskAttempt winner, TaskAttempt loser) {
    scheduler.cancel(loser.taskId());        // sends CANCEL gRPC to worker
    stageState.recordCompletion(winner);
    log.info("Speculative race resolved: winner={} loser={}", winner.taskId(), loser.taskId());
}
```

The losing worker discards any partial output. The winning partition output is committed to the shuffle buffer.

### 5.4 Speculative Eligibility Rules

A task is not eligible for speculative execution if:
- It is already a speculative copy
- The stage is in the last 5% of total tasks (not worth the overhead)
- No alternate worker with sufficient capacity is available

---

## 6. Query Guardrails

### 6.1 Enforcement Architecture

The coordinator runs a `GuardrailEnforcer` loop every `GUARDRAIL_CHECK_INTERVAL_MS = 2000` ms. Workers push usage updates via the heartbeat payload.

```java
// GuardrailEnforcer loop
for (ActiveQuery q : queryRegistry.running()) {
    QueryUsageSnapshot usage = usageAggregator.aggregate(q.queryId());
    Optional<GuardrailViolation> violation = q.guardrails().check(usage);
    violation.ifPresent(v -> faultToleranceManager.handleTimeout(q.queryId(), v));
}
```

### 6.2 Timeout Enforcement

```
maxExecutionTimeMs is checked at the coordinator every 2 seconds.
On violation:
    1. Broadcast CANCEL to all workers holding tasks for the query.
    2. Return QueryTimeoutException(queryId, elapsedMs, maxExecutionTimeMs) to client.
    3. Call finalizeQueryState(queryId, FAILED_TIMEOUT).
```

### 6.3 Scan Limit

Workers report `scannedBytes` in each progress heartbeat. The coordinator accumulates the total.

```
On violation:
    1. Broadcast ABORT_SCAN to all workers.
    2. Return ScanLimitExceededException(queryId, scannedBytes, maxScanBytes).
```

### 6.4 Memory Limit

Workers manage memory in two tiers:

1. **Spill tier**: DataFusion automatically spills sort/hash-join/aggregation buffers to local SSD when RSS exceeds `SPILL_THRESHOLD = 0.75 * maxMemoryPerQuery`.
2. **Kill tier**: If RSS still exceeds `maxMemoryPerQuery` after spill, the worker reports `OOM_KILLED` to the coordinator. The coordinator aborts the query.

```
Worker OOM flow:
    1. DataFusion reports MemoryExceeded to task executor.
    2. Task executor attempts spill (writes to /tmp/lakehouse-spill/<queryId>/).
    3. If RSS > maxMemoryPerQuery after spill:
           worker sends TaskFailure(taskId, OOM_KILLED) to coordinator.
    4. Coordinator classifies OOM_KILLED as FATAL.
    5. Query is aborted with OomKilledException(queryId, peakMemoryBytes, maxMemoryPerQuery).
```

### 6.5 Result Size Limit

The coordinator counts result rows as the final stage streams output to the client. When `resultRows >= maxResultRows`:

```
1. Send CANCEL to the final-stage workers.
2. Flush already-buffered rows to client.
3. Append a ResultTruncatedWarning to the response metadata.
```

Rows beyond the limit are silently dropped; this is not an error.

### 6.6 Default Guardrail Values

| Guardrail          | Default           | Override scope  |
|--------------------|-------------------|-----------------|
| maxExecutionTimeMs | 3,600,000 (1 hr)  | per-query hint  |
| maxScanBytes       | 5 TB              | per-query hint  |
| maxShuffleBytes    | 500 GB            | cluster config  |
| maxResultRows      | 10,000,000        | per-query hint  |
| maxMemoryPerQuery  | 32 GB             | cluster config  |

---

## 7. Coordinator Failover

### 7.1 State Persistence

Every mutable change to query execution state triggers a `persistQueryState` call before the action is taken (write-ahead pattern). The OpenSearch index is:

```
Index: .lakehouse-query-state
Mapping:
  queryId          keyword (document ID)
  status           keyword  (PENDING | RUNNING | COMPLETED | FAILED | PENDING_RETRY)
  planSql          text     (SQL string for the query plan)
  stagesJson       object   (StageExecutionState array)
  taskAssignments  object   (taskId -> workerId map)
  guardrails       object
  submittedAt      date
  lastUpdated      date
```

Writes use `op_type=index` (upsert) with `refresh=wait_for` to guarantee visibility before any downstream action is taken.

### 7.2 Recovery on Startup

```java
List<String> recoverQueries() {
    SearchResponse resp = openSearchClient.search(
        query: { term: { status: ["RUNNING", "PENDING_RETRY"] } },
        size: 1000
    );

    for (QueryExecutionState state : resp.hits()) {
        broadcastReRegistration(state.queryId());   // workers re-send task status
        scheduler.resume(state);                    // re-drive from last known state
        recovered.add(state.queryId());
    }
    return recovered;
}
```

Worker re-registration: on receiving a `RE_REGISTER` gRPC from the coordinator, each worker responds with a `WorkerStatusReport` listing all tasks it is currently running or has completed output for.

### 7.3 Worker Detection of Coordinator Change

Workers detect a coordinator change when:
1. A gRPC call to the coordinator returns `UNAVAILABLE` for more than `COORDINATOR_FAILOVER_DETECT_MS = 10_000` ms.
2. The service discovery layer (OpenSearch cluster state) advertises a new coordinator address.

On detection, workers:
1. Stop sending heartbeats to the old coordinator address.
2. Buffer pending task completions locally (up to `BUFFER_PENDING_COMPLETIONS_MS = 60_000` ms).
3. Connect to the new coordinator and send `WorkerStatusReport`.

### 7.4 Coordinator Election

Leader election uses an OpenSearch document with optimistic locking:

```
Document: .lakehouse-coordinator-lock / { leader: "<nodeId>", term: <n> }
Candidate acquires lock by updating term with version check.
Incumbent refreshes the document every LOCK_REFRESH_INTERVAL_MS = 5_000 ms.
If refresh fails for LOCK_TIMEOUT_MS = 15_000 ms, the lock expires and a new election occurs.
```

---

## 8. S3 Failure Handling

### 8.1 Error Taxonomy

| HTTP Status | S3 Error Code        | Classification    | Action                     |
|-------------|---------------------|-------------------|----------------------------|
| 503         | SlowDown            | RETRYABLE         | Exponential backoff + jitter |
| 429         | TooManyRequests     | RETRYABLE         | Exponential backoff + jitter |
| 500         | InternalError       | RETRYABLE         | Retry up to 5 times        |
| 404         | NoSuchKey           | FATAL             | Fail task immediately      |
| 403         | AccessDenied        | FATAL             | Fail query immediately     |
| Connection timeout | —             | RETRYABLE         | Retry with alternate endpoint |

### 8.2 S3 Retry with Jitter

S3 retries use full-jitter to avoid thundering herd:

```java
long s3Backoff(int attempt) {
    long cap  = 30_000L;                              // 30 s max
    long base = 100L * (1L << attempt);               // 100, 200, 400, 800 ...
    return (long)(Math.random() * Math.min(cap, base));  // uniform [0, min(cap, base)]
}
```

Max S3 retry attempts: **5** (independent of the task retry count).

### 8.3 Region Failover

If S3 operations against the primary region fail with connection-level errors for more than `S3_REGION_FAILOVER_THRESHOLD_MS = 5000` ms, the `S3ClientFactory` switches to a pre-configured secondary region endpoint. A `CircuitBreaker` on the primary endpoint controls this:

```
primary endpoint circuit opens  ──►  S3ClientFactory.useSecondaryRegion()
primary endpoint circuit closes ──►  S3ClientFactory.usePrimaryRegion()
```

Region failover is transparent to callers of the S3 abstraction layer.

### 8.4 S3 Request Hedging

For latency-sensitive small object reads (< 1 MB), a hedged request is issued to a second S3 prefix replica after `S3_HEDGE_DELAY_MS = 200` ms if the first request has not responded. The first response wins; the second is cancelled.

---

## 9. Graceful Degradation

### 9.1 Load Shedding

When the cluster enters an overloaded state (defined as: average worker CPU > 85% or pending task queue depth > `MAX_QUEUE_DEPTH = 2000`), the coordinator activates load-shedding mode:

```
LoadSheddingMode:
    1. Reject new query submissions with HTTP 503 and Retry-After: 30.
    2. Suspend launching new speculative tasks.
    3. Reduce straggler detection polling interval to 30 s (from 10 s).
    4. Deprioritize low-priority queries in the task scheduler queue.
```

### 9.2 Priority-Based Query Preemption

Queries carry a priority level (`HIGH`, `NORMAL`, `LOW`). Under load shedding, `LOW` priority queries whose first stage has not yet begun are cancelled with `PREEMPTED` status and re-queued with a `retryAfterMs` hint returned to the client.

```java
public enum QueryPriority { HIGH, NORMAL, LOW }

// Preemption threshold: triggered when freeWorkerSlots < MIN_FREE_SLOTS_FOR_LOW (5)
```

### 9.3 Backpressure from Workers

Workers expose a `getCapacity()` gRPC endpoint returning available task slots. The coordinator's task scheduler will not assign tasks to a worker that reports `availableSlots == 0`. This prevents OOM on workers by ensuring tasks are only dispatched when the worker has capacity.

---

## 10. Error Propagation to Client

### 10.1 Error Response Structure

All query errors are returned to the client as a structured `QueryError` object regardless of transport (REST, JDBC, gRPC):

```java
public record QueryError(
    String queryId,
    ErrorCode errorCode,          // machine-readable enum
    String message,               // human-readable summary
    ErrorLocation location,       // where the error originated
    String rootCause,             // deepest exception message
    List<String> retryHints,      // actionable hints if retryable
    Instant timestamp
) {}

public record ErrorLocation(
    int stageId,
    int taskId,
    String workerId,
    String workerHost
) {}
```

### 10.2 ErrorCode Enum

```java
public enum ErrorCode {
    // Transient / retriable
    QUERY_TIMEOUT,
    WORKER_FAILURE,
    S3_THROTTLE,

    // Resource limits
    SCAN_LIMIT_EXCEEDED,
    SHUFFLE_LIMIT_EXCEEDED,
    RESULT_TRUNCATED,          // warning, not error
    MEMORY_LIMIT_EXCEEDED,

    // Plan / schema errors
    INVALID_PLAN,
    SCHEMA_MISMATCH,
    UNSUPPORTED_OPERATION,

    // Infrastructure
    COORDINATOR_FAILOVER,
    CLUSTER_OVERLOADED,        // load shedding active
    INTERNAL_ERROR
}
```

### 10.3 Example Error Messages

**Timeout:**
```json
{
  "queryId": "q-20260406-001",
  "errorCode": "QUERY_TIMEOUT",
  "message": "Query exceeded maximum execution time of 3600000 ms (elapsed: 3601234 ms)",
  "location": { "stageId": -1, "taskId": -1, "workerId": "coordinator" },
  "rootCause": "QueryTimeoutException: deadline exceeded",
  "retryHints": ["Reduce scan range", "Add partition filters", "Increase timeout with QUERY_TIMEOUT hint"],
  "timestamp": "2026-04-06T14:23:01Z"
}
```

**OOM:**
```json
{
  "queryId": "q-20260406-002",
  "errorCode": "MEMORY_LIMIT_EXCEEDED",
  "message": "Worker dn-03 (10.0.1.3) exceeded memory limit of 32 GB for query (peak: 34.2 GB) at stage 4, task 17",
  "location": { "stageId": 4, "taskId": 17, "workerId": "dn-03", "workerHost": "10.0.1.3" },
  "rootCause": "DataFusion: MemoryExhausted after spill attempt",
  "retryHints": ["Reduce result set size", "Add aggregation push-down", "Increase maxMemoryPerQuery"],
  "timestamp": "2026-04-06T14:25:44Z"
}
```

**Worker failure (non-recoverable after 3 retries):**
```json
{
  "queryId": "q-20260406-003",
  "errorCode": "WORKER_FAILURE",
  "message": "Stage 2 failed after 3 retry attempts. Last failure on worker dn-07 (10.0.1.7): network timeout",
  "location": { "stageId": 2, "taskId": 9, "workerId": "dn-07", "workerHost": "10.0.1.7" },
  "rootCause": "gRPC DEADLINE_EXCEEDED after 5000 ms",
  "retryHints": ["Retry the query — transient network issue"],
  "timestamp": "2026-04-06T14:30:12Z"
}
```

### 10.4 Error Propagation Path

```
Worker task executor
    │  TaskFailureReport (gRPC)
    ▼
Coordinator FaultToleranceManager
    │  evaluates retry / fatal
    ▼
QueryStateManager
    │  updates QueryExecutionState
    ▼
QueryResultStreamer (if fatal)
    │  constructs QueryError
    ▼
Client (REST / JDBC / gRPC response)
```

Fatal errors short-circuit directly to the client. Retryable errors are handled internally; the client only sees an error if all retries are exhausted.

---

## 11. Key Configuration Reference

| Parameter                         | Default       | Description                                      |
|-----------------------------------|---------------|--------------------------------------------------|
| `HEARTBEAT_INTERVAL_MS`           | 5,000         | Worker → coordinator heartbeat interval          |
| `HEARTBEAT_TIMEOUT_MS`            | 15,000        | Missing heartbeat threshold for worker death     |
| `TASK_MAX_RETRIES`                | 3             | Per-task retry attempts before stage failure     |
| `RETRY_BASE_DELAY_MS`             | 200           | Base exponential backoff delay                   |
| `S3_MAX_RETRIES`                  | 5             | S3 operation retries (independent of task retry) |
| `S3_HEDGE_DELAY_MS`               | 200           | Hedged S3 read trigger delay                     |
| `STRAGGLER_THRESHOLD_MULTIPLIER`  | 3.0           | Multiple of median to classify straggler         |
| `MAX_SPECULATIVE_FRACTION`        | 0.10          | Max speculative tasks as fraction of stage size  |
| `GUARDRAIL_CHECK_INTERVAL_MS`     | 2,000         | Coordinator guardrail enforcement frequency      |
| `CIRCUIT_BREAKER_FAILURE_THRESHOLD` | 5           | Failures in window to open circuit               |
| `CIRCUIT_BREAKER_WINDOW_MS`       | 60,000        | Observation window for circuit breaker           |
| `CIRCUIT_BREAKER_OPEN_DURATION_MS`| 30,000        | Time before transitioning to HALF_OPEN           |
| `COORDINATOR_FAILOVER_DETECT_MS`  | 10,000        | Worker detection delay for coordinator failure   |
| `LOCK_REFRESH_INTERVAL_MS`        | 5,000         | Coordinator heartbeat to leader lock             |
| `LOCK_TIMEOUT_MS`                 | 15,000        | Leader lock expiry if not refreshed              |

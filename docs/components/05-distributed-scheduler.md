# Component 5: Distributed Scheduler

## Table of Contents

1. [Overview and Responsibilities](#1-overview-and-responsibilities)
2. [Java Interfaces and Data Structures](#2-java-interfaces-and-data-structures)
3. [Partition and File Assignment Algorithm](#3-partition-and-file-assignment-algorithm)
4. [Stage Sequencing and State Machines](#4-stage-sequencing-and-state-machines)
5. [Multi-Query Scheduling](#5-multi-query-scheduling)
6. [Speculative Execution for Stragglers](#6-speculative-execution-for-stragglers)
7. [Memory Accounting](#7-memory-accounting)
8. [Thread Model on the Coordinator](#8-thread-model-on-the-coordinator)
9. [Example: Lifecycle of 3 Concurrent Queries](#9-example-lifecycle-of-3-concurrent-queries)

---

## 1. Overview and Responsibilities

The Distributed Scheduler is the central coordinator component responsible for taking a logical query plan—expressed as an `ExecutionDAG` from the Stage Splitter plus a file manifest from the Catalog—and orchestrating its execution across the fixed-node worker pool inside the OpenSearch cluster.

### Inputs

| Input | Source | Description |
|---|---|---|
| `ExecutionDAG` | Stage Splitter | Directed acyclic graph of pipeline stages with inter-stage shuffle edges |
| `FileManifest` | Catalog | Per-table list of file paths, sizes, row-group metadata, and optional cache locality hints |
| `QueryRequest` | Query Coordinator | Query ID, resource group assignment, priority, memory limit |

### Outputs / Effects

| Output | Consumer | Description |
|---|---|---|
| `TaskAssignment` | Worker nodes | Which files/partitions to read, which stage logic to execute |
| `QueryStatus` | Query Coordinator | QUEUED, RUNNING, COMPLETED, FAILED with timing and error details |
| `StageCompletionEvent` | Internal scheduler loop | Triggers downstream stage launch when all upstream stages finish |
| Exchange location map | Exchange/Shuffle layer | Where intermediate results are materialized for downstream stages |

### Responsibilities

1. **Query admission control** — enforce per-resource-group concurrency limits; queue or reject excess queries.
2. **File-to-task bin-packing** — split or merge files into tasks of approximately 128 MB each.
3. **Locality-aware task assignment** — prefer workers that have the relevant Parquet/ORC files in their OS page cache or local SSD cache.
4. **Stage sequencing** — launch a stage only when all stages it depends on have reached `COMPLETED` state.
5. **Multi-query fair-share scheduling** — allocate worker slots across concurrent queries proportionally by resource group weight.
6. **Speculative execution** — detect and re-execute straggler tasks on alternative workers.
7. **Memory accounting** — track per-query and per-worker memory budgets; refuse task assignment that would exceed limits.
8. **Failure handling** — retry failed tasks up to a configurable limit; escalate to query failure when retries are exhausted.

### Position in the System

```
Query Coordinator
       |
       v
  [Distributed Scheduler]  <--  Catalog (file manifest + locality hints)
       |                    <--  Worker Registry (node list, slot counts, cache index)
       v
  Worker Pool (OpenSearch data nodes)
       |
       v
  Exchange / Shuffle Layer
```

---

## 2. Java Interfaces and Data Structures

### 2.1 QueryScheduler

The primary entry point for the query coordinator.

```java
package org.opensearch.lakehouse.scheduler;

import java.util.concurrent.CompletableFuture;

/**
 * Top-level scheduler interface. One instance per coordinator node.
 * Thread-safe: all methods may be called from any thread.
 */
public interface QueryScheduler {

    /**
     * Submit a query for execution. The scheduler may begin execution immediately
     * or enqueue the query depending on resource availability and admission control.
     *
     * @param request  Query metadata: ID, DAG, file manifest, resource group, priority, memory limit.
     * @return         A future that completes with the terminal QueryStatus
     *                 (COMPLETED or FAILED) when the query finishes.
     * @throws QueryRejectedException if the resource group admission limit is exceeded
     *                                and the queue is also full.
     */
    CompletableFuture<QueryStatus> submitQuery(QueryRequest request) throws QueryRejectedException;

    /**
     * Attempt to cancel a running or queued query.
     *
     * @param queryId  The query to cancel.
     * @param reason   Human-readable reason for audit logging.
     * @return         true if the query was found and a cancel signal was sent;
     *                 false if the query is already terminal.
     */
    boolean cancelQuery(String queryId, String reason);

    /**
     * Return a point-in-time snapshot of query execution state.
     *
     * @param queryId  The query to inspect.
     * @return         Current status snapshot, or empty if query is unknown.
     */
    java.util.Optional<QueryStatus> getQueryStatus(String queryId);

    /**
     * List all currently tracked queries (running + queued, optionally including
     * recently completed ones retained in a short TTL window).
     */
    java.util.List<QueryStatus> listActiveQueries();
}
```

### 2.2 QueryRequest

```java
package org.opensearch.lakehouse.scheduler;

import java.time.Duration;
import java.util.List;

public final class QueryRequest {

    /** Globally unique, externally assigned query identifier (e.g. UUID). */
    private final String queryId;

    /** DAG produced by the Stage Splitter. */
    private final ExecutionDAG dag;

    /** Files to scan, grouped by table/partition. */
    private final List<FileEntry> fileManifest;

    /** Resource group this query belongs to (e.g. "interactive", "batch"). */
    private final String resourceGroup;

    /** 1 (lowest) to 10 (highest) within the resource group. */
    private final int priority;

    /** Maximum memory this query may use across all workers, in bytes. */
    private final long maxQueryMemoryBytes;

    /** Wall-clock timeout after which the query is auto-cancelled. */
    private final Duration queryTimeout;

    // constructor, getters, builder omitted for brevity
}
```

### 2.3 QueryExecution

Internal mutable state for a single query in flight.

```java
package org.opensearch.lakehouse.scheduler.internal;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Mutable execution context for one query. Owned by the scheduler loop;
 * fields are updated under the scheduler's per-query lock.
 */
public final class QueryExecution {

    private final String queryId;
    private final ExecutionDAG dag;

    /** stageId -> StageExecution */
    private final Map<String, StageExecution> stageStates = new ConcurrentHashMap<>();

    private final Instant startTime;
    private volatile Instant endTime;

    /** QUEUED | ADMITTED | RUNNING | COMPLETED | FAILED | CANCELLED */
    private final AtomicReference<QueryLifecycleState> lifecycleState =
            new AtomicReference<>(QueryLifecycleState.QUEUED);

    /** Bytes currently reserved across all workers for this query. */
    private volatile long reservedMemoryBytes;

    /** Cancellation reason if applicable. */
    private volatile String cancelReason;

    /** Future to complete when query reaches a terminal state. */
    private final java.util.concurrent.CompletableFuture<QueryStatus> completionFuture =
            new java.util.concurrent.CompletableFuture<>();

    // constructor, getters, lifecycle transition helpers omitted
}
```

### 2.4 StageExecution

```java
package org.opensearch.lakehouse.scheduler.internal;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Execution state for one stage within a query.
 */
public final class StageExecution {

    private final String stageId;

    /** All task executions for this stage. */
    private final List<TaskExecution> tasks = new CopyOnWriteArrayList<>();

    /**
     * Current stage state.
     * Valid transitions: PENDING -> SCHEDULING -> RUNNING -> COMPLETED
     *                                                     -> FAILED
     */
    private final AtomicReference<StageState> state =
            new AtomicReference<>(StageState.PENDING);

    /** stageIds that must be COMPLETED before this stage may be launched. */
    private final Set<String> dependencyStageIds;

    /** Number of tasks that have reached COMPLETED state. */
    private final AtomicInteger completedTaskCount = new AtomicInteger(0);

    /** Number of tasks that have reached FAILED state (after all retries). */
    private final AtomicInteger failedTaskCount = new AtomicInteger(0);

    /** Wall-clock time when the stage transitioned to RUNNING. */
    private volatile long runningAtMs;

    /** Wall-clock time when the stage reached a terminal state. */
    private volatile long finishedAtMs;

    // constructor, getters, transition helpers omitted
}
```

### 2.5 TaskExecution

```java
package org.opensearch.lakehouse.scheduler.internal;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Execution state for a single task (the unit of work dispatched to one worker).
 */
public final class TaskExecution {

    /** Unique within the query. Format: "<queryId>/<stageId>/<taskIndex>". */
    private final String taskId;

    private final String stageId;

    /** Worker currently assigned to this task; null if PENDING. */
    private volatile String workerId;

    /**
     * Files (or row-group ranges within a file) this task must scan.
     * Each FileAssignment carries: path, offset, length, estimated size bytes.
     */
    private final List<FileAssignment> fileAssignments;

    /**
     * Current task state.
     * PENDING -> ASSIGNED -> RUNNING -> COMPLETED
     *                               -> FAILED -> (retry -> ASSIGNED)
     *                               -> SPECULATIVE_CLONE (parallel speculative copy)
     */
    private final AtomicReference<TaskState> state =
            new AtomicReference<>(TaskState.PENDING);

    /** How many times this task has been re-attempted after failure. */
    private final AtomicInteger retryCount = new AtomicInteger(0);

    /** Maximum allowed retries before the stage is marked FAILED. */
    private final int maxRetries;

    /** Runtime metrics reported back by the worker. */
    private volatile TaskMetrics metrics;

    // constructor, getters, transition helpers omitted
}
```

### 2.6 TaskMetrics

```java
package org.opensearch.lakehouse.scheduler.internal;

/**
 * Execution metrics reported by the worker for a task, used for speculative
 * execution decisions and monitoring.
 */
public final class TaskMetrics {

    private final long bytesRead;
    private final long rowsProcessed;
    private final long cpuTimeMs;
    private final long wallTimeMs;

    /** Bytes the task has allocated in the worker's off-heap memory pool. */
    private final long memoryUsedBytes;

    /** Fraction of the task's file assignments that have been processed [0.0, 1.0]. */
    private final double progressFraction;

    private final long lastHeartbeatMs;

    // constructor, getters omitted
}
```

### 2.7 TaskAssigner

```java
package org.opensearch.lakehouse.scheduler;

import java.util.List;

/**
 * Assigns a batch of pending tasks to workers.
 * Implementations apply locality and load-balance policies.
 */
public interface TaskAssigner {

    /**
     * Assign each task in {@code tasks} to an available worker slot.
     *
     * @param tasks         Tasks that need worker assignment. Each has its
     *                      file assignments already populated.
     * @param workerPool    Current snapshot of available workers and their
     *                      slot/memory state.
     * @param localityIndex Maps file path -> set of worker IDs that have the
     *                      file in local cache.
     * @return              List of assignments in the same order as {@code tasks}.
     *                      A task may be left unassigned (workerId = null) if no
     *                      eligible worker has a free slot — the caller must requeue.
     */
    List<TaskAssignment> assign(
            List<TaskExecution> tasks,
            WorkerPool workerPool,
            LocalityIndex localityIndex);
}
```

### 2.8 ResourceManager

```java
package org.opensearch.lakehouse.scheduler;

/**
 * Tracks per-worker slot availability and memory budgets.
 * All mutating methods are thread-safe.
 */
public interface ResourceManager {

    /**
     * Attempt to reserve {@code slots} execution slots and {@code memoryBytes}
     * of off-heap memory on {@code workerId}.
     *
     * @return true if both reservations succeeded (atomically); false if either
     *         limit would be exceeded.
     */
    boolean tryReserve(String workerId, int slots, long memoryBytes);

    /**
     * Release previously reserved resources. Called when a task completes,
     * fails, or is cancelled.
     */
    void release(String workerId, int slots, long memoryBytes);

    /**
     * Return a snapshot of current utilization for all workers.
     */
    WorkerPool snapshotWorkerPool();

    /**
     * Update the known capacity of a worker (e.g. after a node join/leave event).
     */
    void updateWorkerCapacity(String workerId, int totalSlots, long totalMemoryBytes);

    /**
     * Mark a worker as unavailable (e.g. heartbeat timeout).
     * All tasks assigned to the worker will be returned to PENDING by the scheduler.
     */
    void markWorkerUnavailable(String workerId);
}
```

### 2.9 WorkerPool and WorkerState

```java
package org.opensearch.lakehouse.scheduler;

import java.util.List;
import java.util.Map;

/** Immutable snapshot of the worker pool at a point in time. */
public final class WorkerPool {

    /** Ordered list of all known workers (including unavailable ones). */
    private final List<WorkerState> workers;

    public List<WorkerState> availableWorkers() {
        return workers.stream()
                .filter(w -> w.getStatus() == WorkerStatus.AVAILABLE)
                .filter(w -> w.getFreeSlots() > 0)
                .toList();
    }
}

public final class WorkerState {

    private final String workerId;       // OpenSearch node ID
    private final String nodeAddress;    // host:port for task dispatch RPC
    private final int totalSlots;
    private final int usedSlots;
    private final long totalMemoryBytes;
    private final long usedMemoryBytes;
    private final WorkerStatus status;   // AVAILABLE | DRAINING | UNAVAILABLE

    public int getFreeSlots() {
        return totalSlots - usedSlots;
    }

    public long getFreeMemoryBytes() {
        return totalMemoryBytes - usedMemoryBytes;
    }
}
```

### 2.10 ResourceGroup

```java
package org.opensearch.lakehouse.scheduler;

/**
 * A named policy unit that controls how many queries can run concurrently
 * and what fraction of cluster slots the group may occupy.
 */
public final class ResourceGroup {

    /** Logical name, e.g. "interactive", "batch", "admin". */
    private final String name;

    /** Hard ceiling on concurrently executing queries in this group. */
    private final int maxConcurrentQueries;

    /**
     * Maximum fraction of total cluster slots this group may hold at once.
     * Range [0.0, 1.0]. E.g. 0.4 means the group can use at most 40% of all
     * worker slots.
     */
    private final double maxSlotPercent;

    /**
     * Relative scheduling priority versus other groups during fair-share
     * allocation. Higher value = more slots when cluster is contested.
     * E.g. admin=10, interactive=5, batch=1.
     */
    private final int priority;

    /**
     * Maximum number of queries allowed to wait in the admission queue for
     * this group. Excess queries receive QueryRejectedException.
     */
    private final int maxQueueDepth;

    // constructor, getters, builder omitted
}
```

### 2.11 QueryQueue

```java
package org.opensearch.lakehouse.scheduler;

import java.util.Optional;

/**
 * Priority queue for queries awaiting admission into a resource group.
 * Thread-safe.
 */
public interface QueryQueue {

    /**
     * Add a query to the wait queue.
     *
     * @throws QueryRejectedException if the queue is at capacity.
     */
    void enqueue(QueryRequest request) throws QueryRejectedException;

    /**
     * Remove and return the highest-priority waiting query, if any.
     * Priority is determined by: (1) resource group priority, (2) per-query
     * priority field, (3) arrival time (FIFO tiebreak).
     */
    Optional<QueryRequest> dequeue();

    /**
     * Re-order a specific queued query (e.g. after an external priority update).
     *
     * @param queryId     The query to re-prioritize.
     * @param newPriority New priority value [1, 10].
     * @return            true if the query was found in the queue and updated.
     */
    boolean reprioritize(String queryId, int newPriority);

    /**
     * Remove a specific query from the queue (e.g. due to client-side cancel).
     *
     * @return true if the query was found and removed.
     */
    boolean remove(String queryId);

    int size();
}
```

### 2.12 Supporting Enumerations

```java
package org.opensearch.lakehouse.scheduler;

public enum QueryLifecycleState {
    QUEUED,      // Waiting in QueryQueue for admission
    ADMITTED,    // Admitted; stages not yet started
    RUNNING,     // At least one stage is RUNNING
    COMPLETED,   // All stages COMPLETED successfully
    FAILED,      // At least one stage FAILED after all retries
    CANCELLED    // Explicit cancel request received
}

public enum StageState {
    PENDING,     // Dependencies not yet complete
    SCHEDULING,  // Assigning tasks to workers
    RUNNING,     // At least one task is RUNNING
    COMPLETED,   // All tasks COMPLETED
    FAILED       // At least one task failed after all retries
}

public enum TaskState {
    PENDING,           // Not yet assigned
    ASSIGNED,          // Assignment sent to worker; awaiting ACK
    RUNNING,           // Worker acknowledged; heartbeats arriving
    COMPLETED,         // Worker reported success
    FAILED,            // Worker reported failure (may trigger retry)
    SPECULATIVE_CLONE  // Running as a speculative duplicate of a straggler
}

public enum WorkerStatus {
    AVAILABLE,    // Healthy, accepting new tasks
    DRAINING,     // No new tasks; waiting for in-flight tasks to finish
    UNAVAILABLE   // Heartbeat lost; tasks must be reassigned
}
```

---

## 3. Partition and File Assignment Algorithm

### 3.1 Input

- **FileManifest**: a list of `FileEntry` objects, each with:
  - `path`: storage path (e.g. `s3://bucket/table/part-00001.parquet`)
  - `fileSizeBytes`: total file size
  - `rowGroupMeta`: list of `{offset, compressedSize, uncompressedSize}` per Parquet row group
  - `cachedOnWorkers`: set of worker IDs that have this file in cache (populated by the Catalog's locality index)

- **Target task size**: configurable, default `134_217_728` bytes (128 MB uncompressed equivalent)

### 3.2 Bin-Packing Files into Tasks

The goal is to create tasks of approximately equal work. Because Parquet/ORC files are composed of row groups, we split at row-group boundaries rather than byte offsets.

#### Algorithm: `FilePartitioner.partition()`

```
Input:  fileManifest (list of FileEntry), targetTaskSizeBytes
Output: list of TaskFileBundle (each bundle becomes one TaskExecution)

1. Sort fileManifest by path (deterministic ordering).

2. currentBundle = new TaskFileBundle()
   currentBundleSize = 0
   result = []

3. For each FileEntry f in fileManifest:
   a. If f.fileSizeBytes <= targetTaskSizeBytes * 1.5:
      // Small file: add whole file to current bundle
      If currentBundleSize + f.fileSizeBytes > targetTaskSizeBytes AND currentBundle is non-empty:
          result.add(currentBundle)
          currentBundle = new TaskFileBundle()
          currentBundleSize = 0
      currentBundle.add(FileAssignment(f.path, wholeFile))
      currentBundleSize += f.fileSizeBytes

   b. Else (large file: split by row groups):
      Flush currentBundle if non-empty -> result
      currentBundle = new TaskFileBundle(), currentBundleSize = 0

      For each row group rg in f.rowGroupMeta:
          If currentBundleSize + rg.uncompressedSize > targetTaskSizeBytes AND currentBundle non-empty:
              result.add(currentBundle)
              currentBundle = new TaskFileBundle(), currentBundleSize = 0
          currentBundle.add(FileAssignment(f.path, rg.offset, rg.compressedSize))
          currentBundleSize += rg.uncompressedSize

4. If currentBundle is non-empty: result.add(currentBundle)

5. Return result
```

**Edge cases**:
- A single row group larger than `targetTaskSizeBytes` is not split further (Parquet row groups are the minimum granularity). It becomes a task by itself.
- For ORC files (stripe-based), replace row group with stripe.
- Files without column statistics (e.g. CSV) are split by byte range with an estimated row count.

### 3.3 Locality-Aware Assignment

After bin-packing, each `TaskFileBundle` contains one or more file assignments. The locality score for a worker-task pair is:

```
localityScore(worker, bundle) =
    sum(f.uncompressedSize for f in bundle if worker in f.cachedOnWorkers)
    / bundle.totalUncompressedSize
```

A score of 1.0 means all data is cached on that worker; 0.0 means no local data.

#### Assignment Policy (in `LocalityAwareTaskAssigner`)

```
For each task t (sorted by bundle size descending — large tasks assigned first):

    candidateWorkers = workers with freeSlots > 0 AND freeMemory > t.estimatedMemoryBytes

    If candidateWorkers is empty:
        mark t as UNASSIGNED (caller requeues)
        continue

    // Score and rank candidates
    scored = [ (w, localityScore(w, t.bundle)) for w in candidateWorkers ]
    scored.sort by (score DESC, freeSlots DESC)  // locality first, load balance tiebreak

    bestWorker = scored[0].worker
    If bestWorker.localityScore >= LOCALITY_THRESHOLD (default 0.5):
        assign t -> bestWorker   // locality win
    Else:
        // No good local candidate — use load-balanced fallback
        assign t -> candidateWorkers.minBy(usedSlots / totalSlots)

    resourceManager.tryReserve(bestWorker.id, slots=1, memoryBytes=t.estimatedMemoryBytes)
```

### 3.4 Load-Balanced Fallback

When locality is unavailable (cold cache, new data), the scheduler falls back to a weighted least-loaded policy:

```
score(worker) = (usedSlots / totalSlots) * 0.7
              + (usedMemoryBytes / totalMemoryBytes) * 0.3
```

The worker with the lowest combined score is preferred. This spreads IO evenly and avoids hot spots.

### 3.5 Rebalancing on Worker Failure

If a worker becomes `UNAVAILABLE`, all its `ASSIGNED` or `RUNNING` tasks are atomically moved back to `PENDING`. The next scheduler tick re-assigns them using the same locality-aware algorithm (which will score 0.0 for the failed worker's files, triggering a remote read).

---

## 4. Stage Sequencing and State Machines

### 4.1 DAG-Based Execution

The `ExecutionDAG` is a directed acyclic graph where:
- **Nodes** are stages (each stage is a pipeline of operators that runs entirely within a single task).
- **Edges** are shuffle dependencies: a directed edge from stage A to stage B means B depends on A's output.

The scheduler maintains a `ReadyQueue`: the set of stages whose dependency stages have all reached `COMPLETED`.

#### Initialization

```
1. Load DAG: stageNodes, dependencyEdges
2. For each stage s:
       stageExecution[s] = new StageExecution(s, deps=inEdges(s))
3. seed ReadyQueue with all stages that have no incoming edges (source stages)
```

#### Stage Launch Trigger

```
On StageCompleted(stageId):
    for each successor stage s of stageId in DAG:
        if all dependency stages of s are in COMPLETED state:
            ReadyQueue.add(s)

    schedulerLoop.signal()   // wake the scheduler tick
```

#### Scheduler Tick

The scheduler's main loop runs periodically (default 50 ms) and also when explicitly signaled:

```
while (query is RUNNING):
    for each stage s in ReadyQueue (in dependency order):
        if cluster has sufficient free slots:
            launch(s)
            ReadyQueue.remove(s)

    checkHeartbeats()
    checkSpeculativeExecution()
    checkMemoryBudgets()
    sleep(TICK_INTERVAL_MS)
```

### 4.2 Stage State Machine

```
                   +----------+
                   |  PENDING |   (dependencies not met)
                   +----+-----+
                        |  all dependencies COMPLETED
                        v
                +------------+
                | SCHEDULING |   (bin-packing tasks, assigning to workers)
                +-----+------+
                      |  all tasks ASSIGNED
                      v
                 +---------+
                 | RUNNING |   (heartbeats arriving, tasks executing)
                 +----+----+
                      |
           +----------+----------+
           |                     |
   all tasks COMPLETED    any task FAILED (retries exhausted)
           |                     |
           v                     v
      +-----------+         +--------+
      | COMPLETED |         | FAILED |
      +-----------+         +--------+
```

**Transition rules**:
- `PENDING -> SCHEDULING`: all predecessor stage IDs are in `COMPLETED` state.
- `SCHEDULING -> RUNNING`: at least one task transitions to `RUNNING`.
- `RUNNING -> COMPLETED`: `completedTaskCount == tasks.size()`.
- `RUNNING -> FAILED`: `failedTaskCount > 0` AND all retries for those tasks exhausted.
- A `FAILED` stage immediately triggers `QueryExecution` to transition to `FAILED`, which cancels all sibling stages.

### 4.3 Task State Machine

```
                +----------+
                |  PENDING |
                +----+-----+
                     |  worker assigned + RPC sent
                     v
               +----------+
               | ASSIGNED |   (awaiting worker ACK, timeout = 5s)
               +----+-----+
                    |  worker ACK received
                    v
              +---------+
              | RUNNING |   (heartbeat interval = 10s)
              +----+----+
                   |
       +-----------+------------+
       |                        |
  success reported         failure reported
       |                    OR heartbeat timeout
       v                        |
  +-----------+                 v
  | COMPLETED |            +--------+
  +-----------+            | FAILED |
                           +---+----+
                               |
                    retryCount < maxRetries?
                           +---+----+
                           |        |
                          yes       no
                           |        |
                           v        v
                       PENDING   (stage FAILED)
                    (re-enqueued)
```

**Key timing parameters**:
| Parameter | Default | Description |
|---|---|---|
| `taskAssignTimeout` | 5 s | If worker does not ACK within this window, task returns to PENDING |
| `taskHeartbeatInterval` | 10 s | Worker sends progress heartbeat this often |
| `taskHeartbeatTimeout` | 30 s | Task declared FAILED if no heartbeat received in this window |
| `maxTaskRetries` | 3 | Per-task retry limit |

---

## 5. Multi-Query Scheduling

### 5.1 Resource Groups

Three built-in resource groups with configurable parameters:

| Group | maxConcurrentQueries | maxSlotPercent | priority | maxQueueDepth |
|---|---|---|---|---|
| `admin` | 5 | 100% | 10 | 10 |
| `interactive` | 20 | 70% | 5 | 50 |
| `batch` | 50 | 40% | 1 | 200 |

- `admin` queries (e.g. ANALYZE, VACUUM) can use all slots but have low concurrency.
- `interactive` queries (user-facing dashboards) get majority of slots and moderate concurrency.
- `batch` queries (ETL, export jobs) get limited slots but high concurrency and large queue.
- Percentages are soft caps; `admin` overrides allow temporary excess.

### 5.2 Fair-Share Slot Allocation

At each scheduler tick, available cluster slots are divided among active resource groups proportionally to their `priority` weight, subject to `maxSlotPercent`.

```
totalClusterSlots = sum(w.totalSlots for w in allWorkers)
totalActiveWeight = sum(rg.priority for rg in resourceGroups if rg.hasActiveQueries())

for each resourceGroup rg:
    fairShare = (rg.priority / totalActiveWeight) * totalClusterSlots
    cappedShare = min(fairShare, rg.maxSlotPercent * totalClusterSlots)
    rg.currentSlotBudget = cappedShare

Within a resource group, slots are divided equally among its active queries.
```

If one group's queries are not using their full share (e.g. batch group has no active queries), the unused capacity is redistributed proportionally to other groups. This prevents slot starvation.

### 5.3 Query Admission Control

```
On submitQuery(request):
    rg = resourceGroups[request.resourceGroup]

    // Check hard concurrency limit
    if rg.activeQueryCount >= rg.maxConcurrentQueries:
        if queryQueue.size() < rg.maxQueueDepth:
            queryQueue.enqueue(request)
            return future  // will be resolved when admitted
        else:
            throw QueryRejectedException("Queue full for group " + rg.name)

    // Check cluster-wide memory
    if estimatedQueryMemory(request) > availableClusterMemory():
        queryQueue.enqueue(request)   // wait for memory to free up
        return future

    // Admit immediately
    admit(request)
    return request.completionFuture
```

`admit(request)` creates a `QueryExecution`, seeds the ReadyQueue with source stages, and increments `rg.activeQueryCount`.

### 5.4 Priority-Based Queue

`QueryQueue` is backed by a `PriorityBlockingQueue` with a comparator:

```java
Comparator<QueryRequest> QUEUE_ORDER = Comparator
    .comparingInt((QueryRequest r) -> -resourceGroups.get(r.getResourceGroup()).getPriority())
    .thenComparingInt(r -> -r.getPriority())
    .thenComparing(QueryRequest::getEnqueueTime);  // FIFO tiebreak
```

Higher resource group priority is served first. Within the same group, higher per-query priority wins. Ties broken by arrival time (oldest first).

### 5.5 Preemption (Optional / Future Work)

The current design does not preempt running tasks. However, a running batch query that has consumed its fair share for two consecutive scheduler windows may have new task assignments paused (no new tasks dispatched) until its share normalizes. In-flight tasks complete normally.

---

## 6. Speculative Execution for Stragglers

### 6.1 Straggler Detection

At each scheduler tick, for every `RUNNING` stage, the scheduler computes the median task progress and identifies stragglers:

```
tasks = all RUNNING tasks in the stage
if tasks.size() < MIN_TASKS_FOR_SPECULATION (default 4):
    skip   // too few tasks to compute meaningful statistics

medianProgress = median(t.metrics.progressFraction for t in tasks)
medianElapsed  = median(t.metrics.wallTimeMs for t in tasks)

for each task t in tasks:
    if t.metrics.progressFraction < medianProgress * STRAGGLER_PROGRESS_RATIO (default 0.5)
       AND t.metrics.wallTimeMs > medianElapsed * STRAGGLER_TIME_RATIO (default 1.5)
       AND t.speculativeClonesLaunched == 0:
           launchSpeculativeClone(t)
```

Both conditions must be met to avoid launching unnecessary clones for uniformly slow queries.

### 6.2 Speculative Clone Lifecycle

```
launchSpeculativeClone(originalTask t):
    clone = new TaskExecution(
        taskId     = t.taskId + "_spec",
        stageId    = t.stageId,
        fileAssignments = t.fileAssignments,   // same work
        state      = PENDING,
        isSpeculative = true
    )
    t.speculativeClonesLaunched++
    pendingTasks.add(clone)   // picked up by next assignment round
    // assign to a DIFFERENT worker than t.workerId
```

### 6.3 Completion Race

When either the original task or its speculative clone completes first:

```
On TaskCompleted(completedTask):
    if completedTask.isSpeculative:
        original = getOriginalTask(completedTask)
        sendCancelSignal(original)   // best-effort; worker may ignore if already done
    else:
        for each clone of completedTask:
            sendCancelSignal(clone)

    stageExecution.incrementCompleted()
    markTaskCompleted(completedTask)
    // The other copy's completion is ignored (idempotent)
```

### 6.4 Limits

- Maximum speculative clones per stage: `min(10, stage.tasks.size() * 0.1)` — cap at 10% of stage width.
- Maximum additional slot consumption from speculation: 5% of the query's fair-share slot budget.
- Speculation is disabled for stages that write shuffle output (to avoid duplicate data in the exchange layer); it applies only to scan/filter stages.

---

## 7. Memory Accounting

### 7.1 Memory Hierarchy

```
Cluster total memory
  └── Per-worker memory pool  (tracked in ResourceManager)
        └── Per-query allocation on this worker
              └── Per-task allocation
```

### 7.2 Per-Task Memory Estimate

Before assigning a task, the scheduler estimates its memory footprint:

```
estimatedTaskMemory(task) =
    task.bundle.totalUncompressedBytes * SCAN_AMPLIFICATION_FACTOR (default 2.0)
    + TASK_OVERHEAD_BYTES (default 32 MB)
```

The `SCAN_AMPLIFICATION_FACTOR` accounts for column decode buffers, filter bitmaps, and intermediate aggregation state. The factor is stage-type-dependent:
- Scan-only: 1.5×
- Scan + filter: 1.5×
- Scan + aggregation: 3.0×
- Hash join (build side): 5.0× of the smaller table

### 7.3 Per-Query Memory Limit

Each `QueryRequest` carries `maxQueryMemoryBytes`. The `ResourceManager` tracks per-query reservations:

```
queryMemoryUsed[queryId] = sum(t.reservedMemoryBytes for t in query.allRunningTasks)

On tryReserve(workerId, slots, memoryBytes) for task t of query q:
    if queryMemoryUsed[q.queryId] + memoryBytes > q.maxQueryMemoryBytes:
        return false  // query memory exceeded; task waits
    if workerState[workerId].usedMemory + memoryBytes > workerState[workerId].totalMemory:
        return false  // worker memory exceeded; try different worker
    // atomically update both counters
    workerState[workerId].usedMemory += memoryBytes
    queryMemoryUsed[q.queryId] += memoryBytes
    return true
```

### 7.4 Per-Worker Memory Limit

Worker total memory is registered at startup (derived from the OpenSearch node's heap + off-heap configuration). The `ResourceManager` refuses reservations that would push a worker beyond 90% utilization (`MEMORY_OVERCOMMIT_THRESHOLD`).

### 7.5 Runtime Memory Pressure

Workers report actual memory usage in heartbeats. If a worker reports usage above `MEMORY_PRESSURE_THRESHOLD` (85%):

1. The scheduler stops assigning new tasks to that worker.
2. If the worker reports OOM conditions (usage > 95%), the scheduler proactively cancels the lowest-priority task on that worker and marks it for retry on another worker.

### 7.6 Query Memory Exceeded at Runtime

If a running task's heartbeat reports actual memory usage that would push the query over its `maxQueryMemoryBytes`:

1. The scheduler logs a memory violation event.
2. If usage exceeds the limit by more than 20% (hard kill threshold), the query is transitioned to `FAILED` with reason `QUERY_MEMORY_EXCEEDED`.
3. All running tasks for the query receive cancel signals.

---

## 8. Thread Model on the Coordinator

The coordinator is a single OpenSearch node that runs the scheduler. All scheduler logic executes on a small, well-defined set of thread pools to avoid contention and simplify reasoning.

### 8.1 Thread Pools

| Thread Pool | Size | Purpose |
|---|---|---|
| `scheduler-main` | 1 | Single-threaded scheduler loop (tick, stage sequencing, speculative checks) |
| `task-dispatch` | 16 | Sends task assignment RPCs to workers asynchronously |
| `heartbeat-processor` | 4 | Processes incoming heartbeat/status messages from workers |
| `admission-control` | 2 | Handles incoming `submitQuery` / `cancelQuery` calls |
| `query-monitor` | 1 | Timeout checks, query TTL cleanup, metrics emission |

### 8.2 Concurrency Design

```
admission-control threads:
    - Validate QueryRequest
    - Enqueue or admit query (take admission lock briefly)
    - Return CompletableFuture to caller immediately

scheduler-main (single thread, runs every 50ms or on signal):
    - Drain ReadyQueue -> assign tasks via TaskAssigner
    - Call ResourceManager.tryReserve (ResourceManager is lock-free using CAS)
    - Submit assigned tasks to task-dispatch pool (non-blocking submit)
    - Run straggler detection
    - Run timeout detection

task-dispatch threads:
    - Send TaskAssignment RPC to worker (async HTTP/gRPC)
    - On ACK: transition task to RUNNING, post event to scheduler-main via queue
    - On timeout/error: transition task to FAILED, post event to scheduler-main

heartbeat-processor threads:
    - Parse TaskHeartbeat messages from workers
    - Update TaskMetrics in TaskExecution (volatile write; no lock needed)
    - Post significant events (task complete, task failed) to scheduler-main queue

query-monitor thread:
    - Scan all active QueryExecutions for wall-clock timeout
    - Emit per-query metrics to OpenSearch index
    - Evict completed queries from in-memory map after TTL (default 10 min)
```

### 8.3 Synchronization Strategy

- The `scheduler-main` thread is the single writer for `StageExecution.state` and `TaskExecution.state`. Other threads post events to an `LinkedBlockingQueue<SchedulerEvent>` that the scheduler-main drains each tick.
- `ResourceManager` uses `AtomicLong` for memory and slot counters with CAS loops — no locks.
- `QueryExecution.lifecycleState` is an `AtomicReference` updated by the scheduler-main with compare-and-set.
- `TaskMetrics` within `TaskExecution` is a volatile reference; heartbeat threads write a new object atomically.

### 8.4 Back-Pressure

If `task-dispatch` or `heartbeat-processor` pools are saturated:
- Unprocessed heartbeats are dropped (latest metrics take precedence; loss is acceptable).
- Task dispatch failures are surfaced as events on the scheduler queue; the scheduler retries on the next tick.

---

## 9. Example: Lifecycle of 3 Concurrent Queries

This section walks through a concrete scenario: three queries submitted within a short window to a cluster of 4 workers, each with 8 slots (32 total slots).

**Configuration**:
- Resource group `interactive`: maxConcurrentQueries=5, maxSlotPercent=70% (22 slots), priority=5
- Resource group `batch`: maxConcurrentQueries=10, maxSlotPercent=40% (12 slots), priority=1
- Workers: W1, W2, W3, W4, each with 8 slots and 64 GB memory

**Queries**:
- `Q1`: interactive, SELECT with 2-stage DAG (Scan → Aggregate). 6 scan tasks, 2 aggregate tasks.
- `Q2`: interactive, JOIN query, 3-stage DAG (ScanA → ScanB → HashJoin). 4+4+2 tasks.
- `Q3`: batch, large export, 2-stage DAG (Scan → Sort+Write). 12 scan tasks, 4 sort tasks.

---

### T=0ms — Query Submission

```
Q1 submitted (interactive, priority=5)
Q2 submitted (interactive, priority=7)   <- higher priority within group
Q3 submitted (batch, priority=3)
```

Admission control:
- Q1: interactive group has 0 active queries; admitted immediately.
- Q2: admitted immediately (group still under maxConcurrentQueries).
- Q3: batch group has 0 active queries; admitted immediately.

All three queries transition: `QUEUED -> ADMITTED`.

QueryExecutions created:
- Q1: stages S1_scan (no deps), S1_agg (depends on S1_scan)
- Q2: stages S2_scanA (no deps), S2_scanB (no deps), S2_join (depends on S2_scanA + S2_scanB)
- Q3: stages S3_scan (no deps), S3_sort (depends on S3_scan)

ReadyQueue: {S1_scan, S2_scanA, S2_scanB, S3_scan}

---

### T=50ms — First Scheduler Tick

**Slot budget calculation**:
- interactive group: active queries=2 → fairShare = (5/6) * 32 ≈ 26.7, capped at 22. Divided equally: 11 slots per query.
- batch group: active queries=1 → fairShare = (1/6) * 32 ≈ 5.3, capped at 12. → 5 slots for Q3.

**Task assignment** (scheduler processes ReadyQueue):

Stage S1_scan (Q1, 6 tasks, 128MB each):
- Bin-packing already done during query admission.
- Tasks T1_1..T1_6 assigned: W1→T1_1,T1_2; W2→T1_3,T1_4; W3→T1_5; W4→T1_6 (locality-aware)

Stage S2_scanA (Q2, 4 tasks):
- W1→T2A_1; W2→T2A_2; W3→T2A_3; W4→T2A_4 (all slots still available)

Stage S2_scanB (Q2, 4 tasks):
- W1→T2B_1; W2→T2B_2; W3→T2B_3; W4→T2B_4

Stage S3_scan (Q3, 12 tasks, 5-slot budget):
- 5 tasks dispatched: W1→T3_1; W2→T3_2; W3→T3_3; W4→T3_4; W1→T3_5 (W1 has a free slot)
- Remaining 7 tasks of S3_scan: left in pending pool (budget exhausted)

**Worker slot utilization after T=50ms**:
```
W1: 4/8 slots used  (T1_1, T1_2, T2A_1, T2B_1, T3_1, T3_5 = 5/8 actually)
W2: 5/8 slots used
W3: 4/8 slots used
W4: 4/8 slots used
```

All six queries transitions to `RUNNING`.

---

### T=800ms — Heartbeats and Progress

Heartbeats arrive from all workers. Progress fractions:
- S1_scan: T1_1..T1_5 at 60-70%, T1_6 at 20% (straggler on W4!)

**Straggler check**:
- median progress for S1_scan = 65%, T1_6 = 20% < 65% × 0.5 = 32.5% → straggler condition met
- T1_6 wall time = 750ms, median = 700ms → 750ms > 700ms × 1.5? No (750 < 1050) → time condition NOT yet met
- Speculative clone not launched yet (time threshold not breached)

S3_scan: 5 running tasks complete two are done. Two slots freed on W1 and W2.
- Scheduler admits 2 more S3_scan tasks from pending pool.

---

### T=1500ms — Stage S1_scan Approaches Completion

T1_1 through T1_5 COMPLETED. T1_6 still running (50% progress, 1450ms elapsed).
- median wall time of completed tasks ≈ 1100ms
- T1_6: 1450ms > 1100ms × 1.5 = 1650ms? Not yet.

---

### T=1800ms — Speculative Execution Triggered

T1_6: 1800ms elapsed, 60% progress. Median completed task time = 1100ms.
- 1800ms > 1100ms × 1.5 = 1650ms → time condition MET
- 60% progress < 65% × 0.5? No, 60% is now above threshold (median has shifted since other tasks completed)
- **No speculative clone launched** (progress condition no longer met)

T1_6 completes at T=2100ms. S1_scan → COMPLETED.

**Stage S1_scan COMPLETED event fires**:
- Check successors: S1_agg depends only on S1_scan → all deps met → add to ReadyQueue.

---

### T=2150ms — Second Scheduler Tick (post-S1_scan completion)

S1_agg tasks (2 tasks) dispatched to W2 and W3 (these workers have intermediate aggregate data from S1_scan exchange).

Meanwhile, S2_scanA and S2_scanB are at ~80% progress.

S3_scan has 10 of 12 tasks complete; 2 still running.

---

### T=2500ms — S2_scanA and S2_scanB Both Complete

S2_join dependency check: both S2_scanA AND S2_scanB → COMPLETED.
S2_join added to ReadyQueue.

---

### T=2550ms — Scheduler Tick

S2_join (2 tasks) dispatched. W1 and W4 selected (free slots, and they hold join build-side exchange data).

S3_scan fully COMPLETED. S3_sort (4 tasks) added to ReadyQueue and dispatched to W1, W2, W3, W4.

---

### T=3100ms — All Stages Completing

S1_agg: COMPLETED. Q1 → COMPLETED (all stages done). CompletableFuture<QueryStatus> resolved.
- interactive group: activeQueryCount decremented to 1.
- Q3's batch slot budget increases since interactive group freed slots.

S2_join: COMPLETED. Q2 → COMPLETED.
- interactive group: activeQueryCount = 0. All 22 slots freed.

S3_sort: COMPLETED. Q3 → COMPLETED.

---

### Timeline Summary

```
T(ms)  Event
0      Q1, Q2, Q3 admitted
50     S1_scan(6), S2_scanA(4), S2_scanB(4), S3_scan(5/12) dispatched
800    Heartbeats; S3_scan partial completion; 2 more S3 tasks dispatched
2100   S1_scan COMPLETED (T1_6 slow but not speculative)
2150   S1_agg(2) dispatched
2500   S2_scanA + S2_scanB COMPLETED
2550   S2_join(2) dispatched; S3_sort(4) dispatched
3100   S1_agg, S2_join, S3_sort all COMPLETED
       Q1 COMPLETED (total: ~3100ms)
       Q2 COMPLETED (total: ~3100ms)
       Q3 COMPLETED (total: ~3100ms)
```

---

## Appendix: Key Configuration Parameters

| Parameter | Default | Description |
|---|---|---|
| `scheduler.tickIntervalMs` | 50 | Main scheduler loop interval |
| `scheduler.targetTaskSizeBytes` | 134217728 | Target task size for bin-packing (128 MB) |
| `scheduler.localityThreshold` | 0.5 | Min local data fraction to prefer a worker |
| `scheduler.taskAssignTimeoutMs` | 5000 | Worker ACK timeout |
| `scheduler.taskHeartbeatIntervalMs` | 10000 | Expected heartbeat frequency from workers |
| `scheduler.taskHeartbeatTimeoutMs` | 30000 | Heartbeat timeout before task declared failed |
| `scheduler.maxTaskRetries` | 3 | Max per-task retry attempts |
| `scheduler.speculativeProgressRatio` | 0.5 | Straggler progress threshold (vs median) |
| `scheduler.speculativeTimeRatio` | 1.5 | Straggler time threshold (vs median) |
| `scheduler.minTasksForSpeculation` | 4 | Min tasks in a stage before speculative execution |
| `scheduler.memoryOvercommitThreshold` | 0.9 | Max worker memory utilization for new assignments |
| `scheduler.memoryPressureThreshold` | 0.85 | Memory level at which new tasks are paused |
| `scheduler.scanAmplificationFactor` | 2.0 | Default memory multiplier for scan stages |
| `scheduler.queryTtlAfterCompletionMs` | 600000 | How long to retain completed query state (10 min) |

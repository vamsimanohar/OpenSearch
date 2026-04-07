# Component 11: Observability + Query Insights

## Table of Contents

1. [Overview and Responsibilities](#1-overview-and-responsibilities)
2. [Java Interfaces](#2-java-interfaces)
3. [Metrics (Prometheus-Compatible)](#3-metrics-prometheus-compatible)
4. [Distributed Tracing (OpenTelemetry)](#4-distributed-tracing-opentelemetry)
5. [Slow Query Log](#5-slow-query-log)
6. [Query History Store](#6-query-history-store)
7. [EXPLAIN Output Format](#7-explain-output-format)
8. [REST APIs](#8-rest-apis)
9. [Dashboard Data Model](#9-dashboard-data-model)
10. [Alerting Hooks](#10-alerting-hooks)

---

## 1. Overview and Responsibilities

### Purpose

The Observability + Query Insights component provides comprehensive visibility into every aspect of distributed lakehouse query execution. It answers the three critical operational questions:

- **Why is my query slow?** — profiling, stage/task breakdowns, shuffle analysis, EXPLAIN plans
- **What is happening right now?** — live query monitoring, worker load, real-time progress
- **What happened to my failed query?** — query history, error context, full execution profiles

### Scope

This component spans both the **coordinator node** and **worker nodes**. It instruments the full query lifecycle: parse → plan → distribute → execute → aggregate → return.

### Responsibilities

| Responsibility | Description |
|---|---|
| Query Profiling | Collect per-stage and per-task execution metrics during query execution |
| Live Monitoring | Expose real-time views of active queries and worker node load |
| Query History | Persist query records (with profiles) in a searchable OpenSearch index |
| Explain Plans | Generate logical, physical, and distributed explain output for queries |
| Distributed Tracing | Emit OpenTelemetry spans for end-to-end query tracing |
| Slow Query Log | Detect and log queries exceeding configurable latency thresholds |
| Metrics Export | Expose Prometheus-compatible metrics at cluster, worker, query, and stage granularity |
| REST APIs | Provide developer and operator endpoints for query introspection |
| Alerting | Fire hooks for query timeouts, worker failures, and resource anomalies |

### Integration Points

- **Component 3 (Query Coordinator):** Registers profiling hooks, reads stage completion events
- **Component 4 (Worker Execution Engine):** Task-level instrumentation, CPU/memory telemetry
- **Component 6 (Shuffle/Exchange):** Shuffle byte counters, shuffle traffic alerting
- **Component 9 (Resource Manager):** Memory/slot utilization feeds into WorkerLoadInfo
- **OpenSearch REST Layer:** REST API endpoints mounted on the OpenSearch node HTTP server
- **External Systems:** Prometheus scrape endpoint, OpenTelemetry Collector, Grafana/Kibana dashboards

---

## 2. Java Interfaces

### 2.1 Query Profiling

#### `QueryProfiler`

Lifecycle manager for a single query's profiling session. One instance exists per query on the coordinator. It collects stage and task events as they arrive from workers via gRPC callbacks.

```java
package org.opensearch.lakehouse.observability.profiling;

/**
 * Manages the profiling lifecycle for a single query execution.
 * Created by the coordinator when a query is accepted; closed when the query
 * reaches a terminal state (completed, failed, or cancelled).
 *
 * Thread-safety: all methods must be safe for concurrent calls from multiple
 * stage-completion and task-completion callbacks.
 */
public interface QueryProfiler {

    /**
     * Initialises profiling state for a query. Must be called before any
     * stage or task events arrive.
     *
     * @param queryId    globally unique query identifier
     * @param sql        original SQL text submitted by the user
     * @param user       authenticated username
     * @param startTime  epoch milliseconds when the query was accepted
     */
    void startProfile(String queryId, String sql, String user, long startTime);

    /**
     * Records that a stage has completed (or failed) and merges the supplied
     * stage-level metrics into the in-progress profile.
     *
     * @param stageProfile fully populated metrics for the completed stage
     */
    void recordStageCompletion(StageProfile stageProfile);

    /**
     * Records that an individual task has completed (or failed) and merges
     * the supplied task-level metrics into the owning stage profile.
     *
     * @param stageId     the stage this task belongs to
     * @param taskProfile fully populated metrics for the completed task
     */
    void recordTaskCompletion(String stageId, TaskProfile taskProfile);

    /**
     * Finalises the profile and marks the query as ended.
     * After this call the profiler is immutable; subsequent calls to
     * recordStageCompletion / recordTaskCompletion are no-ops.
     *
     * @param endTime      epoch milliseconds when the query reached terminal state
     * @param finalStatus  one of: COMPLETED, FAILED, CANCELLED
     * @param errorMessage nullable; populated when finalStatus == FAILED
     * @return the fully assembled, immutable QueryProfile snapshot
     */
    QueryProfile endProfile(long endTime, QueryStatus finalStatus, String errorMessage);

    /**
     * Returns the best available snapshot of the profile at this instant.
     * May be called at any point — before, during, or after execution.
     * For in-progress queries, stage profiles for incomplete stages will
     * contain partial metrics.
     *
     * @return current (possibly partial) profile snapshot; never null
     */
    QueryProfile getProfile();
}
```

#### `QueryProfile`

Immutable value object representing the complete profiling record for a query.

```java
package org.opensearch.lakehouse.observability.profiling;

import java.util.List;

/**
 * Complete profiling record for a single query execution.
 * All numeric durations are in milliseconds unless noted otherwise.
 */
public interface QueryProfile {

    /** Globally unique query ID (UUID v4). */
    String getQueryId();

    /** Original SQL text as submitted by the user. */
    String getSql();

    /** Authenticated username who submitted the query. */
    String getUser();

    /** Epoch milliseconds at which the coordinator accepted the query. */
    long getStartTimeMs();

    /** Epoch milliseconds at which the query reached terminal state; -1 if still running. */
    long getEndTimeMs();

    /** Terminal status of the query. */
    QueryStatus getStatus();

    /** Nullable error message; populated only when status == FAILED. */
    String getErrorMessage();

    /**
     * Ordered list of stage profiles, one per distributed stage.
     * Stages are ordered by their topological execution order
     * (leaf/scan stages first, final aggregation stage last).
     */
    List<StageProfile> getStages();

    /**
     * Human-readable execution timeline listing key milestones:
     *   ACCEPTED, PLANNED, FIRST_STAGE_STARTED, LAST_STAGE_COMPLETED, etc.
     * Each entry is a (epochMs, label) pair.
     */
    List<TimelineEvent> getTimeline();

    /**
     * Wall-clock duration from query acceptance to terminal state.
     * Equals endTimeMs - startTimeMs; -1 if still in progress.
     */
    long getTotalWallTimeMs();

    /**
     * Sum of CPU time across all tasks and all stages.
     * CPU time may exceed wall time due to parallelism.
     */
    long getTotalCpuTimeMs();

    /**
     * Peak memory usage across all workers during this query's execution,
     * in bytes.
     */
    long getPeakMemoryBytes();
}
```

#### `StageProfile`

```java
package org.opensearch.lakehouse.observability.profiling;

import java.util.List;

/**
 * Execution metrics for a single distributed stage within a query.
 * A stage corresponds to a partition of the query plan that executes
 * between two exchange (shuffle) boundaries.
 */
public interface StageProfile {

    /** Stage identifier, e.g. "stage-0", "stage-1". Unique within a query. */
    String getStageId();

    /**
     * Ordered list of task profiles, one per worker task in this stage.
     * For scan stages this equals the number of tablets/splits assigned.
     */
    List<TaskProfile> getTasks();

    /** Total rows fed into this stage across all tasks. */
    long getInputRows();

    /** Total rows emitted from this stage across all tasks. */
    long getOutputRows();

    /**
     * Total bytes read from the shuffle (exchange) layer by this stage.
     * Zero for leaf (scan) stages.
     */
    long getShuffleBytesRead();

    /**
     * Total bytes written to the shuffle (exchange) layer by this stage.
     * Zero for the final aggregation stage that returns results to the client.
     */
    long getShuffleBytesWritten();

    /**
     * Peak memory consumed by any single task in this stage, in bytes.
     * Use this to identify memory-pressured stages.
     */
    long getPeakMemoryBytes();

    /**
     * Wall-clock duration from the earliest task start to the latest task end
     * within this stage, in milliseconds.
     */
    long getWallTimeMs();

    /**
     * Sum of CPU time across all tasks in this stage, in milliseconds.
     * Useful for identifying CPU-heavy stages.
     */
    long getCpuTimeMs();
}
```

#### `TaskProfile`

```java
package org.opensearch.lakehouse.observability.profiling;

/**
 * Execution metrics for a single task — the smallest unit of work,
 * executing on one worker node for one stage.
 */
public interface TaskProfile {

    /** Task identifier, unique within a stage, e.g. "task-0", "task-1". */
    String getTaskId();

    /** ID of the worker node that executed this task. */
    String getWorkerId();

    /**
     * Number of data files (Parquet/ORC/etc.) opened and read by this task.
     * Relevant for scan tasks only; 0 for pure compute tasks.
     */
    int getFilesScanned();

    /**
     * Total bytes read from storage (before predicate pushdown savings).
     * Reflects actual I/O, not logical data size.
     */
    long getBytesRead();

    /** Number of rows processed (post-filter) by this task. */
    long getRowsProcessed();

    /** Wall-clock duration for this task, in milliseconds. */
    long getWallTimeMs();

    /** Epoch milliseconds when this task began execution on the worker. */
    long getStartTimeMs();

    /** Epoch milliseconds when this task finished execution on the worker. */
    long getEndTimeMs();

    /** CPU time consumed by this task's thread(s), in milliseconds. */
    long getCpuTimeMs();

    /** Peak memory allocated by this task, in bytes. */
    long getPeakMemoryBytes();

    /** OpenTelemetry trace ID propagated into this task, for cross-system correlation. */
    String getTraceId();

    /** OpenTelemetry span ID for this task's root span. */
    String getSpanId();
}
```

#### Supporting Types

```java
package org.opensearch.lakehouse.observability.profiling;

public enum QueryStatus {
    QUEUED,
    PLANNING,
    RUNNING,
    COMPLETED,
    FAILED,
    CANCELLED
}

public interface TimelineEvent {
    long getEpochMs();
    String getLabel();       // e.g. "ACCEPTED", "PLANNED", "STAGE_0_STARTED"
    String getDetail();      // optional free-form detail string; may be null
}
```

---

### 2.2 Live Monitoring

#### `LiveQueryMonitor`

Real-time view of currently executing queries and worker load. Backed by in-memory state maintained by the coordinator. No persistence; data is lost if the coordinator restarts.

```java
package org.opensearch.lakehouse.observability.live;

import java.util.List;

/**
 * Provides a live, low-latency view of the cluster's current operational state.
 * All data is served from in-memory structures updated by the coordinator's
 * scheduling and execution tracking subsystems.
 *
 * Thread-safety: implementations must be safe for concurrent reads.
 */
public interface LiveQueryMonitor {

    /**
     * Returns a snapshot list of all queries currently in a non-terminal state
     * (QUEUED, PLANNING, or RUNNING).
     *
     * @return immutable list; empty if no queries are active
     */
    List<ActiveQueryInfo> getActiveQueries();

    /**
     * Returns a detailed live view of a single active query, including
     * per-stage progress and current resource consumption.
     *
     * @param queryId the query to inspect
     * @return live detail; null if the query is not currently active
     *         (either never existed or already reached terminal state)
     */
    ActiveQueryInfo getQueryDetail(String queryId);

    /**
     * Returns the current load snapshot for all registered worker nodes,
     * including workers that are idle or in a degraded state.
     *
     * @return immutable list; one entry per known worker node
     */
    List<WorkerLoadInfo> getWorkerLoad();

    /**
     * Returns the load snapshot for a single worker node.
     *
     * @param nodeId the worker node ID
     * @return load info; null if the node is not registered
     */
    WorkerLoadInfo getWorkerLoad(String nodeId);

    /**
     * Returns the number of query slots currently in use cluster-wide.
     * Useful for quick capacity checks without listing all queries.
     */
    int getUsedSlots();

    /**
     * Returns the total configured query slot capacity cluster-wide.
     */
    int getTotalSlots();
}
```

#### `ActiveQueryInfo`

```java
package org.opensearch.lakehouse.observability.live;

/**
 * Live state snapshot for a single active query.
 * All fields reflect the state at the moment the snapshot was taken.
 */
public interface ActiveQueryInfo {

    String getQueryId();

    /** Original SQL text. */
    String getSql();

    /** Authenticated user who submitted the query. */
    String getUser();

    /** Wall-clock milliseconds since the query was accepted. */
    long getElapsedMs();

    /**
     * ID of the stage currently executing.
     * For multi-stage queries, this is the deepest stage with at least one
     * running task.  Null if the query is still in QUEUED or PLANNING state.
     */
    String getCurrentStage();

    /**
     * Aggregate completion progress as a percentage [0.0, 100.0].
     * Computed as: (completedTasks / totalTasks) * 100.
     * This is an approximation — stage weights are not accounted for.
     */
    double getProgressPercent();

    /** Current resource consumption snapshot. */
    ResourceUsage getResourceUsage();

    /** Current query status. */
    QueryStatus getStatus();

    /**
     * Epoch milliseconds when the query was accepted by the coordinator.
     */
    long getStartTimeMs();
}

public interface ResourceUsage {
    /** Number of worker task slots currently consumed by this query. */
    int activeTasks();
    /** Estimated total memory allocated to this query across all workers, in bytes. */
    long estimatedMemoryBytes();
    /** Current shuffle egress rate for this query, in bytes per second. */
    long shuffleBytesPerSec();
}
```

#### `WorkerLoadInfo`

```java
package org.opensearch.lakehouse.observability.live;

/**
 * Current load snapshot for a single worker node.
 * Updated by the coordinator's heartbeat processing loop on every
 * heartbeat received from the worker (default interval: 5 seconds).
 */
public interface WorkerLoadInfo {

    /** Unique worker node ID (matches the OpenSearch node ID). */
    String getNodeId();

    /** Human-readable hostname or IP of the worker. */
    String getHostname();

    /** Number of tasks currently executing on this worker. */
    int getActiveTasks();

    /** Configured maximum concurrent task capacity for this worker. */
    int getTaskCapacity();

    /**
     * CPU utilization as a percentage [0.0, 100.0] at the time of the
     * last heartbeat. Averaged over all CPU cores.
     */
    double getCpuPercent();

    /**
     * Memory utilization as a percentage [0.0, 100.0] at the time of the
     * last heartbeat. Based on JVM heap usage.
     */
    double getMemoryPercent();

    /**
     * Current shuffle (exchange) traffic rate on this worker:
     * bytes per second being read from or written to the shuffle store.
     */
    long getShuffleTrafficBytesPerSec();

    /**
     * Aggregate bytes shuffled by this worker since startup (monotonically
     * increasing counter, suitable for rate calculations).
     */
    long getTotalShuffleBytesLifetime();

    /**
     * Epoch milliseconds of the most recent heartbeat from this worker.
     * If now - lastHeartbeatMs > heartbeatTimeoutMs the worker is considered
     * potentially unreachable.
     */
    long getLastHeartbeatMs();

    /** Whether this worker is currently accepting new task assignments. */
    boolean isHealthy();
}
```

---

### 2.3 Query History

#### `QueryHistoryStore`

Persistence layer for completed query records. Backed by an OpenSearch index (`_lakehouse_query_history`). Provides search and analytics capabilities over historical query data.

```java
package org.opensearch.lakehouse.observability.history;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Durable store for completed query execution records.
 *
 * Each record includes the full QueryProfile captured at query completion,
 * enabling post-hoc analysis of slow queries, failure patterns, and
 * resource usage trends.
 *
 * Backed by an OpenSearch index with configurable TTL (index lifecycle policy).
 */
public interface QueryHistoryStore {

    /**
     * Persists a completed query record.  Called by the coordinator
     * immediately after a query reaches a terminal state.
     *
     * @param entry the completed query record to store
     * @throws QueryHistoryException if the write fails after retries
     */
    void save(QueryHistoryEntry entry);

    /**
     * Retrieves a single query record by its unique ID.
     *
     * @param queryId the query to retrieve
     * @return the record, or empty if not found
     */
    Optional<QueryHistoryEntry> getById(String queryId);

    /**
     * Searches query history using the supplied filter criteria.
     * All filter fields are optional; omitted fields are treated as wildcards.
     * Results are sorted by startTime descending by default.
     *
     * @param filter    search criteria
     * @param offset    zero-based pagination offset
     * @param limit     maximum number of results to return (capped at 1000)
     * @return paginated search result
     */
    QueryHistorySearchResult search(QueryHistoryFilter filter, int offset, int limit);

    /**
     * Returns the N slowest queries in the given time window, ordered by
     * total wall time descending.
     *
     * @param since  start of the time window (inclusive)
     * @param until  end of the time window (inclusive)
     * @param topN   how many results to return
     * @return list of at most topN entries, sorted by wall time descending
     */
    List<QueryHistoryEntry> getSlowQueries(Instant since, Instant until, int topN);

    /**
     * Deletes all history records older than the specified cutoff.
     * Used for manual TTL enforcement when index lifecycle policies are
     * not configured.
     *
     * @param cutoff delete records with startTime before this instant
     * @return number of records deleted
     */
    long deleteOlderThan(Instant cutoff);
}
```

#### `QueryHistoryEntry`

```java
package org.opensearch.lakehouse.observability.history;

import org.opensearch.lakehouse.observability.profiling.QueryProfile;
import org.opensearch.lakehouse.observability.profiling.QueryStatus;

import java.time.Instant;

/**
 * Complete record for a single completed (or failed/cancelled) query.
 * Stored as a single OpenSearch document.
 */
public interface QueryHistoryEntry {

    String getQueryId();

    String getSql();

    String getUser();

    /** Client IP address or "internal" for system-generated queries. */
    String getClientAddress();

    Instant getStartTime();

    /** Null for queries that never completed (e.g. still in history due to crash). */
    Instant getEndTime();

    QueryStatus getStatus();

    /**
     * Full execution profile captured at completion.
     * May be null for very old records if profiles were pruned to save space.
     */
    QueryProfile getProfile();

    /**
     * Error message for FAILED queries.  Null for COMPLETED and CANCELLED.
     */
    String getErrorMessage();

    /**
     * Full stack trace for FAILED queries. Null otherwise.
     * Stored as a single newline-delimited string.
     */
    String getErrorStackTrace();

    /** Total wall time in milliseconds; duplicated from profile for fast sorting/filtering. */
    long getWallTimeMs();

    /** Total bytes scanned across all tasks; duplicated from profile for fast filtering. */
    long getTotalBytesScanned();

    /** Total rows returned to the client. */
    long getRowsReturned();

    /** OpenTelemetry trace ID for cross-system correlation. */
    String getTraceId();
}
```

#### `QueryHistoryFilter`

```java
package org.opensearch.lakehouse.observability.history;

import org.opensearch.lakehouse.observability.profiling.QueryStatus;

import java.time.Instant;

/**
 * Filter criteria for QueryHistoryStore.search().
 * All fields are nullable/optional.
 */
public interface QueryHistoryFilter {
    String getUser();                // exact match or null
    QueryStatus getStatus();         // exact match or null
    Instant getStartTimeAfter();     // startTime >= this, or null
    Instant getStartTimeBefore();    // startTime <= this, or null
    Long getMinWallTimeMs();         // wallTimeMs >= this, or null
    Long getMaxWallTimeMs();         // wallTimeMs <= this, or null
    String getSqlContains();         // case-insensitive substring match, or null
    String getErrorContains();       // case-insensitive substring match in errorMessage
}
```

#### `QueryHistorySearchResult`

```java
package org.opensearch.lakehouse.observability.history;

import java.util.List;

public interface QueryHistorySearchResult {
    List<QueryHistoryEntry> getEntries();
    long getTotalHits();
    int getOffset();
    int getLimit();
}
```

---

### 2.4 Explain Plan

#### `ExplainService`

Generates human-readable and machine-parseable query plan representations at logical, physical, and distributed granularity.

```java
package org.opensearch.lakehouse.observability.explain;

/**
 * Generates EXPLAIN output for SQL queries at three levels of detail.
 *
 * Logical:     Abstract relational algebra tree; operator names and columns only.
 * Physical:    Execution strategy decisions; join algorithms, scan methods, sort orders.
 * Distributed: Full distributed plan showing stage boundaries, exchanges, and
 *              estimated partition counts.
 */
public interface ExplainService {

    /**
     * Produces the logical plan — the abstract relational algebra tree
     * after parsing and semantic analysis, before any physical optimisation.
     *
     * @param sql the SQL query to explain
     * @return explain output in both text and JSON formats
     */
    ExplainOutput explainLogical(String sql);

    /**
     * Produces the physical plan — the fully optimised single-node execution
     * plan showing concrete operator choices (hash join vs merge join, etc.)
     * and column pruning decisions.
     *
     * @param sql the SQL query to explain
     * @return explain output in both text and JSON formats
     */
    ExplainOutput explainPhysical(String sql);

    /**
     * Produces the distributed plan — the physical plan fragmented into
     * stages with explicit exchange (shuffle) operators between them.
     * This is the plan that would actually execute across the cluster.
     * Includes estimated row counts, output sizes, and resource costs per stage.
     *
     * @param sql the SQL query to explain
     * @return explain output in both text and JSON formats
     */
    ExplainOutput explainDistributed(String sql);
}
```

#### `ExplainOutput`

```java
package org.opensearch.lakehouse.observability.explain;

import java.util.List;

/**
 * Container for EXPLAIN output in both human-readable and machine-parseable formats.
 */
public interface ExplainOutput {

    /**
     * ASCII art tree representation suitable for printing to a terminal.
     *
     * Example:
     *   [Stage 2] Aggregate (groupBy=[c_mktsegment], agg=[sum(revenue)])
     *     <- Exchange(HASH on c_mktsegment)
     *   [Stage 1] Project [c_mktsegment, revenue]
     *     HashJoin (orders.o_custkey = customer.c_custkey)
     *       <- Exchange(BROADCAST)
     *     [Stage 0] TableScan orders [o_custkey, o_totalprice]
     *         filter: o_orderdate > '1995-01-01'
     *         estimated rows: 1,500,000
     *         estimated bytes: 48 MB
     */
    String getTextTree();

    /**
     * JSON representation of the plan tree.
     * The root object has a "planType" field ("LOGICAL", "PHYSICAL", or "DISTRIBUTED")
     * and a "root" field containing the root PlanNode.
     */
    String getJsonTree();

    /**
     * Structured plan node tree for programmatic inspection.
     * The root node contains the entire plan as a recursive structure.
     */
    PlanNode getRootNode();

    /**
     * For DISTRIBUTED plans: ordered list of stage descriptors.
     * Empty for LOGICAL and PHYSICAL plans.
     */
    List<StageDescriptor> getStages();

    /**
     * Warnings generated during planning that the user should be aware of.
     * Examples: missing statistics, cartesian join detected, excessive partitions.
     */
    List<String> getWarnings();
}
```

#### `PlanNode`

```java
package org.opensearch.lakehouse.observability.explain;

import java.util.List;
import java.util.Map;

/**
 * A node in the explain plan tree.
 */
public interface PlanNode {

    /** Operator name, e.g. "TableScan", "HashJoin", "Aggregate", "Exchange". */
    String getOperatorName();

    /** Stage this node belongs to (for distributed plans); null for logical/physical. */
    String getStageId();

    /** Operator-specific attributes, e.g. {joinType: "HASH", condition: "a.id = b.id"}. */
    Map<String, String> getAttributes();

    /** Estimated number of output rows from this operator. -1 if unknown. */
    long getEstimatedOutputRows();

    /** Estimated output size in bytes from this operator. -1 if unknown. */
    long getEstimatedOutputBytes();

    /** Estimated CPU cost (relative units). -1 if unknown. */
    double getEstimatedCpuCost();

    /** Child nodes (inputs to this operator). */
    List<PlanNode> getChildren();
}
```

#### `StageDescriptor`

```java
package org.opensearch.lakehouse.observability.explain;

import java.util.List;

/**
 * High-level descriptor for a distributed stage in an EXPLAIN DISTRIBUTED output.
 */
public interface StageDescriptor {

    String getStageId();

    /** How output partitions are distributed: HASH, RANGE, BROADCAST, SINGLE, ROUND_ROBIN. */
    String getOutputPartitioningScheme();

    /** Estimated number of parallel tasks for this stage. */
    int getEstimatedParallelism();

    /** IDs of upstream stages whose output is consumed by this stage via exchanges. */
    List<String> getUpstreamStageIds();

    /** Root plan node for this stage's fragment. */
    PlanNode getRootNode();
}
```

---

## 3. Metrics (Prometheus-Compatible)

All metrics are exposed at `GET /_lakehouse/_metrics` in the OpenMetrics/Prometheus text format. The scrape interval is expected to be 15–30 seconds for production deployments.

### 3.1 Cluster-Level Metrics

```
# HELP lakehouse_active_queries_total Number of queries currently in non-terminal state
# TYPE lakehouse_active_queries_total gauge
lakehouse_active_queries_total{state="running"} 12
lakehouse_active_queries_total{state="queued"} 3
lakehouse_active_queries_total{state="planning"} 1

# HELP lakehouse_query_slots_used_total Query slots currently allocated
# TYPE lakehouse_query_slots_used_total gauge
lakehouse_query_slots_used_total 16

# HELP lakehouse_query_slots_capacity_total Total configured query slot capacity
# TYPE lakehouse_query_slots_capacity_total gauge
lakehouse_query_slots_capacity_total 64

# HELP lakehouse_shuffle_bytes_total Bytes written to the shuffle store (cluster-wide, monotonic)
# TYPE lakehouse_shuffle_bytes_total counter
lakehouse_shuffle_bytes_total 8472983648

# HELP lakehouse_shuffle_bytes_per_second Current shuffle egress rate cluster-wide
# TYPE lakehouse_shuffle_bytes_per_second gauge
lakehouse_shuffle_bytes_per_second 52428800

# HELP lakehouse_queries_completed_total Total queries completed since startup
# TYPE lakehouse_queries_completed_total counter
lakehouse_queries_completed_total{status="COMPLETED"} 18432
lakehouse_queries_completed_total{status="FAILED"} 94
lakehouse_queries_completed_total{status="CANCELLED"} 17

# HELP lakehouse_coordinator_planning_seconds Time to produce the distributed query plan
# TYPE lakehouse_coordinator_planning_seconds histogram
lakehouse_coordinator_planning_seconds_bucket{le="0.01"} 1200
lakehouse_coordinator_planning_seconds_bucket{le="0.05"} 4500
lakehouse_coordinator_planning_seconds_bucket{le="0.25"} 17800
lakehouse_coordinator_planning_seconds_bucket{le="1.0"}  18300
lakehouse_coordinator_planning_seconds_bucket{le="+Inf"} 18432
lakehouse_coordinator_planning_seconds_sum 1843.2
lakehouse_coordinator_planning_seconds_count 18432
```

### 3.2 Per-Worker Metrics

Labels: `node_id`, `hostname`

```
# HELP lakehouse_worker_cpu_percent CPU utilization on the worker node
# TYPE lakehouse_worker_cpu_percent gauge
lakehouse_worker_cpu_percent{node_id="node-1",hostname="worker-1.internal"} 72.4

# HELP lakehouse_worker_memory_percent JVM heap utilization on the worker node
# TYPE lakehouse_worker_memory_percent gauge
lakehouse_worker_memory_percent{node_id="node-1",hostname="worker-1.internal"} 58.1

# HELP lakehouse_worker_active_tasks Current number of executing tasks on the worker
# TYPE lakehouse_worker_active_tasks gauge
lakehouse_worker_active_tasks{node_id="node-1",hostname="worker-1.internal"} 4

# HELP lakehouse_worker_task_capacity Maximum concurrent tasks the worker can accept
# TYPE lakehouse_worker_task_capacity gauge
lakehouse_worker_task_capacity{node_id="node-1",hostname="worker-1.internal"} 8

# HELP lakehouse_worker_shuffle_bytes_total Bytes written to shuffle store (per worker, monotonic)
# TYPE lakehouse_worker_shuffle_bytes_total counter
lakehouse_worker_shuffle_bytes_total{node_id="node-1",hostname="worker-1.internal"} 2147483648

# HELP lakehouse_worker_shuffle_read_bytes_total Bytes read from shuffle store (per worker, monotonic)
# TYPE lakehouse_worker_shuffle_read_bytes_total counter
lakehouse_worker_shuffle_read_bytes_total{node_id="node-1",hostname="worker-1.internal"} 1932735283

# HELP lakehouse_worker_heartbeat_age_seconds Seconds since the last heartbeat was received
# TYPE lakehouse_worker_heartbeat_age_seconds gauge
lakehouse_worker_heartbeat_age_seconds{node_id="node-1",hostname="worker-1.internal"} 3.2
```

### 3.3 Per-Query Metrics

Labels: `query_id`, `user` (high-cardinality; use Prometheus recording rules to aggregate)

```
# HELP lakehouse_query_duration_seconds End-to-end query wall time
# TYPE lakehouse_query_duration_seconds histogram
lakehouse_query_duration_seconds_bucket{le="0.1"} 840
lakehouse_query_duration_seconds_bucket{le="0.5"} 3200
lakehouse_query_duration_seconds_bucket{le="1.0"} 6100
lakehouse_query_duration_seconds_bucket{le="5.0"} 15000
lakehouse_query_duration_seconds_bucket{le="30.0"} 18200
lakehouse_query_duration_seconds_bucket{le="+Inf"} 18432
lakehouse_query_duration_seconds_sum  27648.0
lakehouse_query_duration_seconds_count 18432

# HELP lakehouse_query_bytes_scanned_total Bytes scanned per query
# TYPE lakehouse_query_bytes_scanned_total histogram
lakehouse_query_bytes_scanned_total_bucket{le="1048576"}    200      # < 1 MB
lakehouse_query_bytes_scanned_total_bucket{le="104857600"}  4000     # < 100 MB
lakehouse_query_bytes_scanned_total_bucket{le="1073741824"} 14000    # < 1 GB
lakehouse_query_bytes_scanned_total_bucket{le="+Inf"}       18432
lakehouse_query_bytes_scanned_total_sum   4831838208000
lakehouse_query_bytes_scanned_total_count 18432

# HELP lakehouse_query_rows_returned_total Rows returned to client per query
# TYPE lakehouse_query_rows_returned_total histogram
lakehouse_query_rows_returned_total_bucket{le="100"}     6000
lakehouse_query_rows_returned_total_bucket{le="10000"}   14000
lakehouse_query_rows_returned_total_bucket{le="1000000"} 18000
lakehouse_query_rows_returned_total_bucket{le="+Inf"}    18432
```

### 3.4 Per-Stage Metrics

Labels: `query_id`, `stage_id`

```
# HELP lakehouse_stage_duration_ms Stage wall time in milliseconds
# TYPE lakehouse_stage_duration_ms gauge
lakehouse_stage_duration_ms{query_id="q-abc123",stage_id="stage-0"} 1240

# HELP lakehouse_stage_shuffle_bytes Bytes shuffled by a stage (read + written)
# TYPE lakehouse_stage_shuffle_bytes gauge
lakehouse_stage_shuffle_bytes{query_id="q-abc123",stage_id="stage-1",direction="read"}    536870912
lakehouse_stage_shuffle_bytes{query_id="q-abc123",stage_id="stage-0",direction="written"} 536870912

# HELP lakehouse_stage_rows_in Total rows fed into a stage
# TYPE lakehouse_stage_rows_in gauge
lakehouse_stage_rows_in{query_id="q-abc123",stage_id="stage-1"} 4500000

# HELP lakehouse_stage_rows_out Total rows emitted from a stage
# TYPE lakehouse_stage_rows_out gauge
lakehouse_stage_rows_out{query_id="q-abc123",stage_id="stage-1"} 1200000
```

---

## 4. Distributed Tracing (OpenTelemetry)

### 4.1 Trace Hierarchy

Each query produces a single OpenTelemetry trace with a four-level span hierarchy:

```
Trace: query (root span)
  └── Stage 0 (span)
        ├── Task 0 on worker-1 (span)
        │     ├── TableScan operator (span)
        │     └── Filter operator (span)
        └── Task 1 on worker-2 (span)
              ├── TableScan operator (span)
              └── Filter operator (span)
  └── Stage 1 (span)
        ├── Task 0 on worker-1 (span)
        │     ├── ShuffleRead operator (span)
        │     ├── HashJoin operator (span)
        │     └── Aggregate operator (span)
        └── Task 1 on worker-3 (span)
              ...
```

### 4.2 Span Definitions

#### Query Root Span

```
Span name:   "lakehouse.query"
Kind:        SERVER
Attributes:
  lakehouse.query.id          = "q-abc123"
  lakehouse.query.sql         = "<original SQL>"   (truncated to 1024 chars)
  lakehouse.query.user        = "alice"
  lakehouse.query.status      = "COMPLETED" | "FAILED" | "CANCELLED"
  lakehouse.query.stages      = 3
  lakehouse.query.total_tasks = 12
Events:
  "query.accepted"  at query start
  "planning.done"   after plan generation
  "execution.done"  at query end
```

#### Stage Span

```
Span name:   "lakehouse.stage"
Kind:        INTERNAL
Parent:      query root span
Attributes:
  lakehouse.stage.id              = "stage-0"
  lakehouse.stage.parallelism     = 4
  lakehouse.stage.input_rows      = 4500000
  lakehouse.stage.output_rows     = 1200000
  lakehouse.stage.shuffle_written = 536870912
  lakehouse.stage.shuffle_read    = 0
```

#### Task Span

```
Span name:   "lakehouse.task"
Kind:        INTERNAL
Parent:      stage span
Attributes:
  lakehouse.task.id            = "task-0"
  lakehouse.task.worker_id     = "node-1"
  lakehouse.task.files_scanned = 12
  lakehouse.task.bytes_read    = 134217728
  lakehouse.task.rows_out      = 1125000
  lakehouse.task.cpu_ms        = 820
  lakehouse.task.peak_memory   = 67108864
```

#### Operator Span

```
Span name:   "lakehouse.operator.<OperatorName>"
             e.g. "lakehouse.operator.TableScan"
                  "lakehouse.operator.HashJoin"
                  "lakehouse.operator.Aggregate"
                  "lakehouse.operator.ShuffleRead"
                  "lakehouse.operator.ShuffleWrite"
Kind:        INTERNAL
Parent:      task span
Attributes:
  lakehouse.operator.name         = "TableScan"
  lakehouse.operator.input_rows   = 0          (for source operators)
  lakehouse.operator.output_rows  = 4500000
  lakehouse.operator.wall_ms      = 340
  lakehouse.operator.cpu_ms       = 290
```

### 4.3 Trace Context Propagation (Coordinator → Worker via gRPC)

Trace context is propagated using the W3C TraceContext format embedded in gRPC metadata headers.

#### On the Coordinator (Task Dispatch)

```java
// In TaskDispatchService.dispatchTask():
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapSetter;
import io.grpc.Metadata;

Metadata grpcMetadata = new Metadata();

// W3C TextMapSetter writes traceparent + tracestate headers into gRPC metadata
TextMapSetter<Metadata> grpcSetter = (carrier, key, value) -> {
    Metadata.Key<String> metaKey = Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER);
    carrier.put(metaKey, value);
};

// Inject the current span context (stage span) into outgoing metadata
openTelemetry.getPropagators()
    .getTextMapPropagator()
    .inject(Context.current(), grpcMetadata, grpcSetter);

// Attach metadata to the gRPC stub
TaskServiceGrpc.TaskServiceStub stub = taskStub.withInterceptors(
    MetadataUtils.newAttachHeadersInterceptor(grpcMetadata)
);
stub.executeTask(taskRequest, responseObserver);
```

#### On the Worker (Task Receipt)

```java
// In TaskExecutionService (gRPC server-side interceptor):
import io.opentelemetry.context.propagation.TextMapGetter;
import io.grpc.Metadata;

TextMapGetter<Metadata> grpcGetter = new TextMapGetter<>() {
    @Override public String get(Metadata carrier, String key) {
        Metadata.Key<String> metaKey = Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER);
        return carrier.get(metaKey);
    }
    @Override public Iterable<String> keys(Metadata carrier) {
        return carrier.keys();
    }
};

// Extract parent context from incoming gRPC metadata
Context extractedContext = openTelemetry.getPropagators()
    .getTextMapPropagator()
    .extract(Context.current(), grpcMetadata, grpcGetter);

// Start task root span as a child of the coordinator's stage span
Span taskSpan = tracer.spanBuilder("lakehouse.task")
    .setParent(extractedContext)
    .setSpanKind(SpanKind.INTERNAL)
    .startSpan();
```

### 4.4 OTel SDK Configuration (application.yml)

```yaml
lakehouse:
  observability:
    tracing:
      enabled: true
      exporter: otlp                # otlp | jaeger | zipkin | logging
      endpoint: "http://otel-collector:4317"
      sampling:
        strategy: parentbased_traceidratio
        ratio: 0.1                  # sample 10% of queries in production
        always_sample_slow_ms: 5000 # always trace queries slower than 5s
      propagation: tracecontext,baggage
```

---

## 5. Slow Query Log

Queries whose wall time exceeds a configurable threshold are written to a dedicated OpenSearch index (`_lakehouse_slow_queries`) and optionally to the coordinator's application log.

### 5.1 Configuration

```yaml
lakehouse:
  observability:
    slow-query-log:
      enabled: true
      threshold-ms: 5000            # log queries slower than 5 seconds
      index-name: "_lakehouse_slow_queries"
      log-to-application-log: true  # also write to coordinator log at WARN level
      include-profile: true         # embed the full QueryProfile in the log record
      include-explain: false        # optionally include EXPLAIN DISTRIBUTED output
      max-sql-length: 4096          # truncate SQL beyond this length
```

### 5.2 Slow Query Log Document Schema

```json
{
  "queryId":         "q-abc123",
  "sql":             "SELECT c.c_mktsegment, sum(o.o_totalprice) ...",
  "user":            "alice",
  "clientAddress":   "10.0.1.55",
  "startTime":       "2026-04-06T14:22:01.000Z",
  "endTime":         "2026-04-06T14:22:09.432Z",
  "wallTimeMs":      8432,
  "status":          "COMPLETED",
  "bytesScanned":    5368709120,
  "rowsReturned":    1200,
  "stageCount":      3,
  "peakMemoryBytes": 2147483648,
  "totalCpuMs":      42000,
  "traceId":         "4bf92f3577b34da6a3ce929d0e0e4736",
  "slowReason":      "HIGH_SHUFFLE_VOLUME",   // SLOW_SCAN | HIGH_SHUFFLE_VOLUME | MEMORY_SPILL | UNKNOWN
  "profile":         { /* full QueryProfile as nested object */ },
  "@timestamp":      "2026-04-06T14:22:09.432Z"
}
```

### 5.3 SlowQueryDetector Interface

```java
package org.opensearch.lakehouse.observability.slowquery;

/**
 * Analyses completed query profiles and classifies slow queries,
 * then persists them to the slow query index.
 */
public interface SlowQueryDetector {

    /**
     * Evaluates a completed profile against configured thresholds.
     * If the query is classified as slow, persists it to the slow query index
     * and emits a log entry if log-to-application-log is enabled.
     *
     * @param profile the completed query profile to evaluate
     */
    void evaluate(QueryProfile profile);

    /**
     * Returns the configured slow query threshold in milliseconds.
     */
    long getThresholdMs();
}
```

### 5.4 Slow Query Index Mapping

```json
{
  "mappings": {
    "properties": {
      "queryId":         { "type": "keyword" },
      "sql":             { "type": "text", "analyzer": "standard" },
      "user":            { "type": "keyword" },
      "clientAddress":   { "type": "ip" },
      "startTime":       { "type": "date" },
      "endTime":         { "type": "date" },
      "wallTimeMs":      { "type": "long" },
      "status":          { "type": "keyword" },
      "bytesScanned":    { "type": "long" },
      "rowsReturned":    { "type": "long" },
      "peakMemoryBytes": { "type": "long" },
      "totalCpuMs":      { "type": "long" },
      "traceId":         { "type": "keyword" },
      "slowReason":      { "type": "keyword" },
      "profile":         { "type": "object", "enabled": false },
      "@timestamp":      { "type": "date" }
    }
  },
  "settings": {
    "index.lifecycle.name": "lakehouse-slow-queries-policy",
    "number_of_shards": 1,
    "number_of_replicas": 1
  }
}
```

---

## 6. Query History Store

### 6.1 OpenSearch Index Configuration

Index name: `_lakehouse_query_history`

```json
{
  "mappings": {
    "properties": {
      "queryId":          { "type": "keyword" },
      "sql":              { "type": "text", "analyzer": "standard",
                            "fields": { "keyword": { "type": "keyword", "ignore_above": 256 } } },
      "user":             { "type": "keyword" },
      "clientAddress":    { "type": "ip" },
      "startTime":        { "type": "date" },
      "endTime":          { "type": "date" },
      "status":           { "type": "keyword" },
      "wallTimeMs":       { "type": "long" },
      "totalBytesScanned":{ "type": "long" },
      "rowsReturned":     { "type": "long" },
      "errorMessage":     { "type": "text" },
      "errorStackTrace":  { "type": "text", "index": false },
      "traceId":          { "type": "keyword" },
      "profile":          { "type": "object", "enabled": false },
      "@timestamp":       { "type": "date" }
    }
  },
  "settings": {
    "index.lifecycle.name":       "lakehouse-query-history-policy",
    "index.lifecycle.rollover_alias": "lakehouse_query_history",
    "number_of_shards":   3,
    "number_of_replicas": 1
  }
}
```

### 6.2 Index Lifecycle Policy (TTL)

```json
{
  "policy": {
    "phases": {
      "hot": {
        "min_age": "0ms",
        "actions": {
          "rollover": { "max_size": "50gb", "max_age": "7d" }
        }
      },
      "warm": {
        "min_age": "7d",
        "actions": { "forcemerge": { "max_num_segments": 1 } }
      },
      "delete": {
        "min_age": "90d",
        "actions": { "delete": {} }
      }
    }
  }
}
```

### 6.3 Write Path

1. Query reaches terminal state on coordinator
2. `QueryProfiler.endProfile()` produces the final `QueryProfile`
3. `QueryHistoryStore.save()` assembles a `QueryHistoryEntry` and indexes it via the OpenSearch Java client
4. Write uses `index` action with `op_type=create`; if the document already exists (duplicate terminal event), the existing record is preserved
5. Writes are fire-and-forget with async retry (up to 3 attempts, exponential backoff) to avoid blocking query teardown
6. `SlowQueryDetector.evaluate()` is called in parallel on the same profile

### 6.4 Read Path

`QueryHistoryStore.getById()` uses a `GET /{index}/_doc/{queryId}` request.

`QueryHistoryStore.search()` translates a `QueryHistoryFilter` into an OpenSearch `bool` query:

```json
{
  "query": {
    "bool": {
      "filter": [
        { "term": { "user": "alice" } },
        { "term": { "status": "FAILED" } },
        { "range": { "startTime": { "gte": "2026-04-01", "lte": "2026-04-06" } } },
        { "range": { "wallTimeMs": { "gte": 5000 } } }
      ],
      "must": [
        { "match": { "sql": "orders" } }
      ]
    }
  },
  "sort": [{ "startTime": "desc" }],
  "from": 0,
  "size": 50
}
```

---

## 7. EXPLAIN Output Format

### 7.1 Text Tree Format

The text tree uses indentation (2 spaces per level) and Unicode box-drawing characters for visual clarity. Each node line follows the pattern:

```
[<StageId>] <OperatorName>  (<key>=<value>, ...)  est_rows=<N>  est_bytes=<X>
```

**Example — EXPLAIN DISTRIBUTED:**

```
Distributed Query Plan
======================
[Stage 2] FinalAggregate  (groupBy=[c_mktsegment], agg=[sum(revenue)])
            est_rows=5  est_bytes=120B
  <- Exchange(HASH on c_mktsegment, est_partitions=8)
  [Stage 1] PartialAggregate  (groupBy=[c_mktsegment], agg=[partial_sum(o_totalprice)])
              est_rows=1,200,000  est_bytes=38.4MB
    HashJoin  (type=INNER, condition=o.o_custkey = c.c_custkey, algo=HASH_BUILD_RIGHT)
                est_rows=1,500,000  est_bytes=48MB
      <- Exchange(BROADCAST, est_size=2.4MB)
      [Stage 0-B] TableScan customer  (cols=[c_custkey, c_mktsegment])
                    est_rows=150,000  est_bytes=2.4MB
      [Stage 0-A] TableScan orders  (cols=[o_custkey, o_totalprice, o_orderdate])
                    filter: o_orderdate > DATE '1995-01-01'
                    est_rows=1,500,000  est_bytes=48MB
                    partitions_pruned: 18 of 24

Warnings:
  [W001] Statistics for table 'orders' are 30 days old; estimates may be inaccurate.
```

### 7.2 JSON Format

```json
{
  "planType": "DISTRIBUTED",
  "root": {
    "operatorName": "FinalAggregate",
    "stageId": "stage-2",
    "attributes": {
      "groupBy": "c_mktsegment",
      "aggregates": "sum(revenue)"
    },
    "estimatedOutputRows": 5,
    "estimatedOutputBytes": 120,
    "estimatedCpuCost": 1.2,
    "children": [
      {
        "operatorName": "Exchange",
        "stageId": "stage-2",
        "attributes": {
          "partitioning": "HASH",
          "partitionKey": "c_mktsegment",
          "estimatedPartitions": "8"
        },
        "estimatedOutputRows": 1200000,
        "estimatedOutputBytes": 40265318,
        "children": [
          {
            "operatorName": "PartialAggregate",
            "stageId": "stage-1",
            "attributes": {
              "groupBy": "c_mktsegment",
              "aggregates": "partial_sum(o_totalprice)"
            },
            "estimatedOutputRows": 1200000,
            "estimatedOutputBytes": 40265318,
            "children": [
              {
                "operatorName": "HashJoin",
                "stageId": "stage-1",
                "attributes": {
                  "type": "INNER",
                  "condition": "o.o_custkey = c.c_custkey",
                  "algorithm": "HASH_BUILD_RIGHT"
                },
                "estimatedOutputRows": 1500000,
                "estimatedOutputBytes": 50331648,
                "children": [
                  {
                    "operatorName": "Exchange",
                    "stageId": "stage-1",
                    "attributes": { "partitioning": "BROADCAST" },
                    "estimatedOutputRows": 150000,
                    "estimatedOutputBytes": 2516582,
                    "children": [
                      {
                        "operatorName": "TableScan",
                        "stageId": "stage-0-b",
                        "attributes": {
                          "table": "customer",
                          "columns": "c_custkey, c_mktsegment"
                        },
                        "estimatedOutputRows": 150000,
                        "estimatedOutputBytes": 2516582,
                        "children": []
                      }
                    ]
                  },
                  {
                    "operatorName": "TableScan",
                    "stageId": "stage-0-a",
                    "attributes": {
                      "table": "orders",
                      "columns": "o_custkey, o_totalprice, o_orderdate",
                      "filter": "o_orderdate > DATE '1995-01-01'",
                      "partitionsPruned": "18 of 24"
                    },
                    "estimatedOutputRows": 1500000,
                    "estimatedOutputBytes": 50331648,
                    "children": []
                  }
                ]
              }
            ]
          }
        ]
      }
    ]
  },
  "stages": [
    { "stageId": "stage-0-a", "outputPartitioning": "ROUND_ROBIN", "estimatedParallelism": 6, "upstreamStageIds": [] },
    { "stageId": "stage-0-b", "outputPartitioning": "BROADCAST",   "estimatedParallelism": 1, "upstreamStageIds": [] },
    { "stageId": "stage-1",   "outputPartitioning": "HASH",         "estimatedParallelism": 8, "upstreamStageIds": ["stage-0-a", "stage-0-b"] },
    { "stageId": "stage-2",   "outputPartitioning": "SINGLE",       "estimatedParallelism": 1, "upstreamStageIds": ["stage-1"] }
  ],
  "warnings": [
    "Statistics for table 'orders' are 30 days old; estimates may be inaccurate."
  ]
}
```

---

## 8. REST APIs

All endpoints are mounted on the OpenSearch node HTTP server under the `/_lakehouse` prefix. Authentication and authorisation follow OpenSearch's standard security plugin mechanisms.

### 8.1 Query Status (Live or Historical)

```
GET /_lakehouse/_query/{queryId}
```

Returns live status if the query is active; falls back to history if it has completed.

**Response (active query):**
```json
{
  "queryId":        "q-abc123",
  "source":         "live",
  "status":         "RUNNING",
  "sql":            "SELECT ...",
  "user":           "alice",
  "startTime":      "2026-04-06T14:22:01.000Z",
  "elapsedMs":      4210,
  "currentStage":   "stage-1",
  "progressPercent": 62.5,
  "resourceUsage": {
    "activeTasks":         8,
    "estimatedMemoryBytes": 1073741824,
    "shuffleBytesPerSec":  52428800
  }
}
```

**Response (completed query):**
```json
{
  "queryId":      "q-abc123",
  "source":       "history",
  "status":       "COMPLETED",
  "sql":          "SELECT ...",
  "user":         "alice",
  "startTime":    "2026-04-06T14:22:01.000Z",
  "endTime":      "2026-04-06T14:22:09.432Z",
  "wallTimeMs":   8432,
  "rowsReturned": 1200,
  "bytesScanned": 5368709120,
  "traceId":      "4bf92f3577b34da6a3ce929d0e0e4736"
}
```

**Error responses:**
- `404 Not Found` — query ID not found in live state or history
- `403 Forbidden` — caller is not the query owner and lacks admin role

---

### 8.2 Query Profile

```
GET /_lakehouse/_query/{queryId}/profile
```

Returns the full execution profile (stage and task breakdowns).

**Response:**
```json
{
  "queryId":       "q-abc123",
  "sql":           "SELECT ...",
  "user":          "alice",
  "status":        "COMPLETED",
  "startTimeMs":   1744000921000,
  "endTimeMs":     1744000929432,
  "totalWallTimeMs": 8432,
  "totalCpuTimeMs":  42000,
  "peakMemoryBytes": 2147483648,
  "timeline": [
    { "epochMs": 1744000921000, "label": "ACCEPTED",          "detail": null },
    { "epochMs": 1744000921120, "label": "PLANNING_DONE",     "detail": "3 stages, 15 tasks" },
    { "epochMs": 1744000921200, "label": "STAGE_0_STARTED",   "detail": null },
    { "epochMs": 1744000923100, "label": "STAGE_0_COMPLETED", "detail": null },
    { "epochMs": 1744000923150, "label": "STAGE_1_STARTED",   "detail": null },
    { "epochMs": 1744000927800, "label": "STAGE_1_COMPLETED", "detail": null },
    { "epochMs": 1744000927850, "label": "STAGE_2_STARTED",   "detail": null },
    { "epochMs": 1744000929432, "label": "STAGE_2_COMPLETED", "detail": null }
  ],
  "stages": [
    {
      "stageId":           "stage-0",
      "inputRows":         0,
      "outputRows":        4500000,
      "shuffleBytesRead":  0,
      "shuffleBytesWritten": 536870912,
      "peakMemoryBytes":   536870912,
      "wallTimeMs":        1900,
      "cpuTimeMs":         14000,
      "tasks": [
        {
          "taskId":        "task-0",
          "workerId":      "node-1",
          "filesScanned":  12,
          "bytesRead":     134217728,
          "rowsProcessed": 1125000,
          "wallTimeMs":    1820,
          "startTimeMs":   1744000921200,
          "endTimeMs":     1744000923020,
          "cpuTimeMs":     3200,
          "peakMemoryBytes": 134217728,
          "traceId":       "4bf92f3577b34da6a3ce929d0e0e4736",
          "spanId":        "a3ce929d0e0e4736"
        }
      ]
    }
  ]
}
```

**Error responses:**
- `404` — query not found or profile was pruned
- `202 Accepted` — query still running; partial profile returned with `"partial": true`

---

### 8.3 List Active Queries

```
GET /_lakehouse/_queries
```

Query parameters:

| Parameter | Type   | Default | Description |
|-----------|--------|---------|-------------|
| `user`    | string | —       | Filter by user |
| `status`  | string | —       | `QUEUED`, `PLANNING`, or `RUNNING` |
| `min_elapsed_ms` | long | — | Only return queries running longer than this |

**Response:**
```json
{
  "total": 16,
  "queries": [
    {
      "queryId":        "q-abc123",
      "user":           "alice",
      "status":         "RUNNING",
      "elapsedMs":      4210,
      "currentStage":   "stage-1",
      "progressPercent": 62.5,
      "sql":            "SELECT c.c_mktsegment ..."
    },
    {
      "queryId":        "q-def456",
      "user":           "bob",
      "status":         "QUEUED",
      "elapsedMs":      830,
      "currentStage":   null,
      "progressPercent": 0.0,
      "sql":            "SELECT count(*) FROM ..."
    }
  ]
}
```

---

### 8.4 Worker Status Dashboard

```
GET /_lakehouse/_workers
```

Query parameters:

| Parameter  | Type    | Default | Description |
|------------|---------|---------|-------------|
| `healthy`  | boolean | —       | Filter to healthy/unhealthy nodes only |

**Response:**
```json
{
  "totalWorkers":   8,
  "healthyWorkers": 7,
  "usedSlots":      16,
  "totalSlots":     64,
  "workers": [
    {
      "nodeId":                  "node-1",
      "hostname":                "worker-1.internal",
      "activeTasks":             4,
      "taskCapacity":            8,
      "cpuPercent":              72.4,
      "memoryPercent":           58.1,
      "shuffleTrafficBytesPerSec": 52428800,
      "totalShuffleBytesLifetime": 2147483648,
      "lastHeartbeatMs":         1744000928000,
      "healthy":                 true
    },
    {
      "nodeId":    "node-7",
      "hostname":  "worker-7.internal",
      "activeTasks": 0,
      "taskCapacity": 8,
      "cpuPercent":   0.0,
      "memoryPercent": 12.0,
      "shuffleTrafficBytesPerSec": 0,
      "totalShuffleBytesLifetime": 847288320,
      "lastHeartbeatMs": 1744000870000,
      "healthy": false
    }
  ]
}
```

---

### 8.5 Explain Query

```
POST /_lakehouse/_sql?explain=true
POST /_lakehouse/_sql?explain=logical
POST /_lakehouse/_sql?explain=physical
POST /_lakehouse/_sql?explain=distributed
```

Request body:
```json
{
  "query": "SELECT c.c_mktsegment, sum(o.o_totalprice) FROM orders o JOIN customer c ON o.o_custkey = c.c_custkey GROUP BY c.c_mktsegment",
  "format": "text"    // "text" | "json" | "both"
}
```

**Response (`format=both`):**
```json
{
  "planType":  "DISTRIBUTED",
  "textTree":  "Distributed Query Plan\n======================\n[Stage 2] FinalAggregate ...",
  "jsonTree":  { ... },
  "stages":    [ ... ],
  "warnings":  [ "Statistics for table 'orders' are 30 days old." ]
}
```

---

### 8.6 Cluster Stats

```
GET /_lakehouse/_stats
```

**Response:**
```json
{
  "timestamp":           "2026-04-06T14:22:10.000Z",
  "cluster": {
    "activeQueries":     16,
    "queuedQueries":     3,
    "planningQueries":   1,
    "totalSlotsUsed":    16,
    "totalSlots":        64,
    "shuffleBytesPerSec": 52428800
  },
  "counters": {
    "queriesCompleted":  18432,
    "queriesFailed":     94,
    "queriesCancelled":  17
  },
  "latencyPercentiles": {
    "p50Ms":   420,
    "p90Ms":   2100,
    "p95Ms":   4800,
    "p99Ms":   18000,
    "p999Ms":  45000
  },
  "workers": {
    "total":   8,
    "healthy": 7,
    "avgCpuPercent":    48.2,
    "avgMemoryPercent": 41.7
  }
}
```

---

### 8.7 Query History Search

```
GET /_lakehouse/_queries/history
```

Query parameters:

| Parameter        | Type   | Default | Description |
|------------------|--------|---------|-------------|
| `user`           | string | —       | Filter by user |
| `status`         | string | —       | Terminal status |
| `from`           | ISO date | —    | Start time lower bound |
| `to`             | ISO date | —    | Start time upper bound |
| `min_wall_ms`    | long   | —       | Minimum wall time |
| `sql_contains`   | string | —       | Substring match in SQL |
| `offset`         | int    | 0       | Pagination offset |
| `limit`          | int    | 50      | Page size (max 1000) |

**Response:**
```json
{
  "totalHits": 94,
  "offset":    0,
  "limit":     50,
  "entries": [
    {
      "queryId":    "q-def456",
      "user":       "bob",
      "status":     "FAILED",
      "startTime":  "2026-04-06T12:00:00.000Z",
      "endTime":    "2026-04-06T12:00:02.100Z",
      "wallTimeMs": 2100,
      "errorMessage": "Worker node-3 lost during shuffle phase",
      "traceId":    "abc123"
    }
  ]
}
```

---

## 9. Dashboard Data Model

### 9.1 Grafana Dashboard Panels

#### Cluster Overview Row

| Panel | Visualization | Query / Source |
|-------|--------------|----------------|
| Active Queries | Stat + sparkline | `lakehouse_active_queries_total` |
| Slot Utilization % | Gauge (0–100) | `lakehouse_query_slots_used_total / lakehouse_query_slots_capacity_total * 100` |
| Query Throughput | Time series | `rate(lakehouse_queries_completed_total[5m])` |
| Shuffle Throughput | Time series | `lakehouse_shuffle_bytes_per_second` |
| Query Error Rate | Time series | `rate(lakehouse_queries_completed_total{status="FAILED"}[5m])` |
| P50/P95/P99 Latency | Time series | `histogram_quantile(0.95, rate(lakehouse_query_duration_seconds_bucket[5m]))` |

#### Worker Health Row

| Panel | Visualization | Query / Source |
|-------|--------------|----------------|
| Worker CPU Heatmap | Heatmap | `lakehouse_worker_cpu_percent` by node |
| Worker Memory Heatmap | Heatmap | `lakehouse_worker_memory_percent` by node |
| Task Load per Worker | Bar chart | `lakehouse_worker_active_tasks / lakehouse_worker_task_capacity` |
| Shuffle I/O per Worker | Time series | `rate(lakehouse_worker_shuffle_bytes_total[1m])` |
| Unhealthy Workers | Alert list | Workers where `lakehouse_worker_heartbeat_age_seconds > 30` |

#### Query Analysis Row

| Panel | Visualization | Query / Source |
|-------|--------------|----------------|
| Bytes Scanned Distribution | Histogram | `lakehouse_query_bytes_scanned_total` |
| Rows Returned Distribution | Histogram | `lakehouse_query_rows_returned_total` |
| Slow Queries (last 24h) | Table | OpenSearch query on `_lakehouse_slow_queries` |
| Failed Queries (last 24h) | Table | OpenSearch query on `_lakehouse_query_history` where status=FAILED |
| Top Users by Query Count | Bar chart | OpenSearch aggregation on `_lakehouse_query_history` |
| Top Users by CPU | Bar chart | OpenSearch aggregation on `_lakehouse_query_history` by `totalCpuMs` |

#### Live Query Table

A table panel refreshed every 10 seconds showing:

| Column | Source |
|--------|--------|
| Query ID (link to detail) | `/_lakehouse/_queries` |
| User | `ActiveQueryInfo.user` |
| Elapsed | `ActiveQueryInfo.elapsedMs` |
| Progress | `ActiveQueryInfo.progressPercent` (progress bar) |
| Current Stage | `ActiveQueryInfo.currentStage` |
| Memory | `ResourceUsage.estimatedMemoryBytes` |
| Shuffle Rate | `ResourceUsage.shuffleBytesPerSec` |
| SQL (truncated) | `ActiveQueryInfo.sql` |

### 9.2 Kibana / OpenSearch Dashboards

Dashboards are built directly over the OpenSearch indices:

| Dashboard | Index | Key Visualizations |
|-----------|-------|--------------------|
| Slow Query Analysis | `_lakehouse_slow_queries` | Wall time timeline, slow reason breakdown, top users |
| Query Failure Analysis | `_lakehouse_query_history` | Error message word cloud, failure rate over time, top failing users |
| Resource Usage Trends | `_lakehouse_query_history` | Bytes scanned over time, CPU usage by user, memory peak distribution |

---

## 10. Alerting Hooks

### 10.1 AlertingHook Interface

```java
package org.opensearch.lakehouse.observability.alerting;

/**
 * Extension point for reacting to observability events.
 * Multiple hooks may be registered; all registered hooks are called for each event.
 * Implementations must be non-blocking — do not perform synchronous I/O.
 */
public interface AlertingHook {

    /**
     * Called when a query has been running longer than the configured timeout.
     *
     * @param event details of the timeout event
     */
    void onQueryTimeout(QueryTimeoutEvent event);

    /**
     * Called when a worker node fails to deliver a heartbeat within the
     * configured timeout window.
     *
     * @param event details of the suspected worker failure
     */
    void onWorkerDown(WorkerDownEvent event);

    /**
     * Called when cluster-wide shuffle volume exceeds the configured
     * bytes-per-second threshold.
     *
     * @param event details of the high shuffle event
     */
    void onHighShuffleVolume(HighShuffleVolumeEvent event);

    /**
     * Called when a query fails with an unhandled error.
     *
     * @param event details of the query failure
     */
    void onQueryFailure(QueryFailureEvent event);

    /**
     * Called when cluster slot utilization exceeds the configured threshold.
     *
     * @param event current utilization snapshot
     */
    void onHighSlotUtilization(SlotUtilizationEvent event);
}
```

### 10.2 Event Types

```java
package org.opensearch.lakehouse.observability.alerting;

public interface QueryTimeoutEvent {
    String getQueryId();
    String getSql();
    String getUser();
    long getElapsedMs();
    long getConfiguredTimeoutMs();
    QueryProfile getPartialProfile();   // snapshot at time of timeout
}

public interface WorkerDownEvent {
    String getNodeId();
    String getHostname();
    long getLastHeartbeatMs();
    long getHeartbeatTimeoutMs();
    int getActiveTasksAtLastHeartbeat();
    /** IDs of queries that had tasks running on this worker. */
    java.util.List<String> getAffectedQueryIds();
}

public interface HighShuffleVolumeEvent {
    long getCurrentBytesPerSec();
    long getThresholdBytesPerSec();
    /** Node IDs generating the most shuffle traffic. */
    java.util.List<String> getTopShuffleNodes();
    /** Query IDs contributing the most to current shuffle volume. */
    java.util.List<String> getTopShuffleQueryIds();
}

public interface QueryFailureEvent {
    String getQueryId();
    String getSql();
    String getUser();
    String getErrorMessage();
    String getErrorStackTrace();
    long getWallTimeMs();
    QueryProfile getPartialProfile();
}

public interface SlotUtilizationEvent {
    int getUsedSlots();
    int getTotalSlots();
    double getUtilizationPercent();
    double getConfiguredThresholdPercent();
}
```

### 10.3 Built-in Hook Implementations

#### OpenSearch Alerting Hook

Indexes alert events to `_lakehouse_alerts` for use with OpenSearch Alerting/Notifications.

```java
/**
 * Routes alert events to the OpenSearch Alerting plugin via the
 * /_plugins/_alerting/monitors API, or directly indexes to _lakehouse_alerts.
 */
public class OpenSearchAlertingHook implements AlertingHook { ... }
```

#### Webhook Hook

Posts JSON payloads to a configured HTTP endpoint (Slack, PagerDuty, custom webhook).

```java
/**
 * Sends alert events as HTTP POST requests with a JSON body to a configured URL.
 * Supports configurable per-event-type payload templates.
 */
public class WebhookAlertingHook implements AlertingHook { ... }
```

#### Log-Only Hook

Writes structured alert events to the coordinator's application log at `ERROR` or `WARN` level. Used when no external alerting system is configured.

```java
public class LogAlertingHook implements AlertingHook { ... }
```

### 10.4 Alerting Configuration

```yaml
lakehouse:
  observability:
    alerting:
      hooks:
        - type: webhook
          url: "https://hooks.slack.com/services/..."
          events: [QUERY_TIMEOUT, WORKER_DOWN, QUERY_FAILURE]
        - type: opensearch
          index: "_lakehouse_alerts"
          events: [QUERY_TIMEOUT, WORKER_DOWN, HIGH_SHUFFLE_VOLUME, HIGH_SLOT_UTILIZATION, QUERY_FAILURE]
        - type: log
          events: [QUERY_TIMEOUT, WORKER_DOWN]
      thresholds:
        query-timeout-ms:            300000   # 5 minutes
        worker-heartbeat-timeout-ms: 30000    # 30 seconds
        high-shuffle-bytes-per-sec:  524288000 # 500 MB/s
        high-slot-utilization-pct:   90.0
```

### 10.5 Alert Index Schema (`_lakehouse_alerts`)

```json
{
  "mappings": {
    "properties": {
      "alertType":   { "type": "keyword" },
      "severity":    { "type": "keyword" },
      "timestamp":   { "type": "date" },
      "queryId":     { "type": "keyword" },
      "nodeId":      { "type": "keyword" },
      "message":     { "type": "text" },
      "detail":      { "type": "object", "enabled": false },
      "resolved":    { "type": "boolean" },
      "resolvedAt":  { "type": "date" }
    }
  }
}
```

---

*End of Component 11: Observability + Query Insights LLD*

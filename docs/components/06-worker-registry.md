# Component 6: Worker Registry + Health Monitor

## Table of Contents

1. [Overview and Responsibilities](#1-overview-and-responsibilities)
2. [Java Interfaces and Data Structures](#2-java-interfaces-and-data-structures)
3. [Worker Discovery via OpenSearch Cluster State](#3-worker-discovery-via-opensearch-cluster-state)
4. [Sidecar Process Management](#4-sidecar-process-management)
5. [Health Monitoring Protocol](#5-health-monitoring-protocol)
6. [Slot and Memory Tracking](#6-slot-and-memory-tracking)
7. [Worker Lifecycle State Machine](#7-worker-lifecycle-state-machine)
8. [Graceful Drain and Decommission](#8-graceful-drain-and-decommission)
9. [Integration with OpenSearch APIs](#9-integration-with-opensearch-apis)
10. [Thread Model and Synchronization](#10-thread-model-and-synchronization)
11. [Example: Node Join, Steady State, and Node Leave](#11-example-node-join-steady-state-and-node-leave)

---

## 1. Overview and Responsibilities

The Worker Registry + Health Monitor is the component that maintains the authoritative, live view of every worker node available for query execution. It bridges two worlds: the OpenSearch cluster membership layer (which tracks JVM nodes) and the Rust DataFusion sidecar process (which actually executes query fragments). The Distributed Scheduler consults this component on every scheduling tick to know which workers exist, how loaded they are, and whether they are healthy.

### Inputs

| Input | Source | Description |
|---|---|---|
| `ClusterChangedEvent` | OpenSearch `ClusterService` | Node join/leave events from the cluster state |
| `WorkerHealthReport` | Rust sidecar via gRPC heartbeat | Per-node live metrics: CPU, memory, active tasks, sidecar alive flag |
| `DrainRequest` | Operator / rolling-upgrade tooling | Explicit drain trigger for a named node |
| `TaskCompletionEvent` | Distributed Scheduler | Released slots and memory after a task finishes |

### Outputs / Effects

| Output | Consumer | Description |
|---|---|---|
| `List<WorkerInfo>` | Distributed Scheduler | Snapshot of healthy workers with free slots and memory |
| `WorkerLifecycleEvent` | Scheduler, Observability | `ACTIVE`, `DRAINING`, `LOST` transitions |
| Sidecar process start/stop | OS process manager | Launches or terminates the Rust binary on the local node |
| OpenSearch node attributes | Cluster state | Publishes `grpcPort`, `flightPort` as node attributes so peers can connect |

### Responsibilities

1. **Worker enumeration** — maintain the canonical set of nodes with the `datawarehouse` role by listening to cluster state changes.
2. **Sidecar lifecycle management** — start the Rust/DataFusion process when the local node joins as a `datawarehouse` node; stop it on graceful shutdown or decommission.
3. **Endpoint advertisement** — write gRPC and Arrow Flight ports into OpenSearch node attributes so any coordinator can connect to any worker without a separate service-discovery layer.
4. **Health monitoring** — run a background loop that issues gRPC health-check pings to each worker's sidecar; declare a worker `UNREACHABLE` when heartbeats are missed beyond the timeout threshold.
5. **Slot and memory accounting** — track total and used execution slots and memory per worker; expose atomic reservation/release APIs consumed by the Scheduler's `ResourceManager`.
6. **Lifecycle state machine** — drive each worker through `STARTING → ACTIVE → DRAINING → DECOMMISSIONED` (or `→ UNREACHABLE`) with correct transitions and listener notification.
7. **Graceful drain** — coordinate with the Scheduler to stop dispatching new tasks to a draining worker while it waits for in-flight tasks to complete.

### Position in the System

```
OpenSearch ClusterService (node join/leave events)
          |
          v
  [Worker Registry + Health Monitor]  <--  gRPC heartbeats from Rust sidecars
          |                           -->  sidecar start/stop (local node only)
          |
          +---> Distributed Scheduler (WorkerInfo snapshots, lifecycle events)
          |
          +---> Observability / Query Insights (health metrics)
          |
          +---> gRPC Protocol Layer (resolves nodeId -> grpcPort/flightPort)
```

---

## 2. Java Interfaces and Data Structures

All classes live under `org.opensearch.lakehouse.worker`.

### 2.1 WorkerRegistry

The primary read/write interface for all other components.

```java
package org.opensearch.lakehouse.worker;

import java.util.List;
import java.util.Optional;

/**
 * Maintains the live set of datawarehouse worker nodes.
 * Thread-safe: all methods may be called concurrently from any thread.
 */
public interface WorkerRegistry {

    /**
     * Return an immutable snapshot of all workers currently in ACTIVE status.
     * Workers in STARTING, DRAINING, UNREACHABLE, or DECOMMISSIONED state are
     * excluded. The list is ordered by nodeId for deterministic scheduling.
     *
     * @return unmodifiable list; never null; may be empty if cluster is starting.
     */
    List<WorkerInfo> getActiveWorkers();

    /**
     * Look up a specific worker by its OpenSearch node ID.
     *
     * @param nodeId  The OpenSearch-assigned node ID (e.g. "8gFe3kQzT2y...").
     * @return        The WorkerInfo if the node is tracked (any status), or empty.
     */
    Optional<WorkerInfo> getWorker(String nodeId);

    /**
     * Called by the ClusterStateListener when a new datawarehouse node appears
     * in the cluster state. Triggers sidecar startup on the local node if
     * {@code nodeId} matches the local node's ID.
     *
     * <p>This method is idempotent: calling it for an already-known node is a no-op.
     *
     * @param nodeId   OpenSearch node ID of the joining node.
     * @param host     Hostname or IP address.
     * @param grpcPort gRPC server port advertised in node attributes.
     * @param flightPort Arrow Flight server port advertised in node attributes.
     * @param totalSlots   Configured execution slot count for this node.
     * @param totalMemoryBytes Off-heap memory budget for this node in bytes.
     */
    void onWorkerJoined(
        String nodeId,
        String host,
        int grpcPort,
        int flightPort,
        int totalSlots,
        long totalMemoryBytes
    );

    /**
     * Called by the ClusterStateListener when a datawarehouse node leaves the
     * cluster (graceful or crash). If the node was ACTIVE or DRAINING, all its
     * in-flight tasks must be reassigned by the Scheduler.
     *
     * <p>This method transitions the worker to DECOMMISSIONED and fires
     * {@link WorkerLifecycleListener#onWorkerLost(String)} if the departure
     * was unexpected (i.e. the node was not already in DRAINING state).
     *
     * @param nodeId  The OpenSearch node ID of the departing node.
     */
    void onWorkerLeft(String nodeId);

    /**
     * Return aggregate capacity figures for the whole cluster.
     * Includes only ACTIVE workers.
     *
     * @return immutable snapshot; never null.
     */
    ClusterCapacity getClusterCapacity();

    /**
     * Register a listener to receive worker lifecycle state-change events.
     * Listeners are called on the health-monitor thread; implementations must
     * be non-blocking.
     *
     * @param listener  The listener to add.
     */
    void addLifecycleListener(WorkerLifecycleListener listener);

    /**
     * Remove a previously registered lifecycle listener.
     *
     * @param listener  The listener to remove.
     */
    void removeLifecycleListener(WorkerLifecycleListener listener);
}
```

### 2.2 WorkerInfo

Immutable value object representing a single worker node at a point in time.

```java
package org.opensearch.lakehouse.worker;

import java.time.Instant;

/**
 * Immutable snapshot of a worker node's identity and resource state.
 *
 * <p>WorkerInfo objects are replaced (not mutated) whenever slot counts or
 * status change. All fields reflect the state at the moment of construction.
 */
public final class WorkerInfo {

    /** OpenSearch-assigned node ID; stable for the node's lifetime. */
    private final String nodeId;

    /** Hostname or IP address of the node. */
    private final String host;

    /**
     * Port on which the Rust sidecar's gRPC server listens for task
     * submissions from the Coordinator.
     */
    private final int grpcPort;

    /**
     * Port on which the Rust sidecar's Arrow Flight server listens for
     * shuffle data transfers between workers.
     */
    private final int flightPort;

    /** Total number of concurrent task slots provisioned on this node. */
    private final int totalSlots;

    /** Number of slots not currently occupied by a running task. */
    private final int freeSlots;

    /** Total off-heap memory budget in bytes allocated to the Rust sidecar. */
    private final long totalMemoryBytes;

    /** Off-heap bytes currently in use by active tasks on this node. */
    private final long usedMemoryBytes;

    /** Current lifecycle status of the worker. */
    private final WorkerStatus status;

    /**
     * OS process ID of the Rust sidecar, as reported by the JVM after fork.
     * -1 if the sidecar has not been started or its PID is unknown.
     */
    private final long sidecarPid;

    /** Wall-clock time when this worker's sidecar became ACTIVE. */
    private final Instant startTime;

    public WorkerInfo(
        String nodeId,
        String host,
        int grpcPort,
        int flightPort,
        int totalSlots,
        int freeSlots,
        long totalMemoryBytes,
        long usedMemoryBytes,
        WorkerStatus status,
        long sidecarPid,
        Instant startTime
    ) {
        this.nodeId          = nodeId;
        this.host            = host;
        this.grpcPort        = grpcPort;
        this.flightPort      = flightPort;
        this.totalSlots      = totalSlots;
        this.freeSlots       = freeSlots;
        this.totalMemoryBytes = totalMemoryBytes;
        this.usedMemoryBytes  = usedMemoryBytes;
        this.status          = status;
        this.sidecarPid      = sidecarPid;
        this.startTime       = startTime;
    }

    public String getNodeId()            { return nodeId; }
    public String getHost()              { return host; }
    public int getGrpcPort()             { return grpcPort; }
    public int getFlightPort()           { return flightPort; }
    public int getTotalSlots()           { return totalSlots; }
    public int getFreeSlots()            { return freeSlots; }
    public long getTotalMemoryBytes()    { return totalMemoryBytes; }
    public long getUsedMemoryBytes()     { return usedMemoryBytes; }
    public WorkerStatus getStatus()      { return status; }
    public long getSidecarPid()          { return sidecarPid; }
    public Instant getStartTime()        { return startTime; }

    public long getFreeMemoryBytes() {
        return totalMemoryBytes - usedMemoryBytes;
    }

    /** Return a copy with updated slot and memory counts. */
    public WorkerInfo withResources(int freeSlots, long usedMemoryBytes) {
        return new WorkerInfo(nodeId, host, grpcPort, flightPort,
            totalSlots, freeSlots, totalMemoryBytes, usedMemoryBytes,
            status, sidecarPid, startTime);
    }

    /** Return a copy with a new status. */
    public WorkerInfo withStatus(WorkerStatus newStatus) {
        return new WorkerInfo(nodeId, host, grpcPort, flightPort,
            totalSlots, freeSlots, totalMemoryBytes, usedMemoryBytes,
            newStatus, sidecarPid, startTime);
    }
}
```

### 2.3 WorkerStatus

```java
package org.opensearch.lakehouse.worker;

/**
 * Lifecycle status of a worker node as seen by the registry.
 */
public enum WorkerStatus {

    /**
     * The JVM node has joined the cluster and the sidecar process has been
     * launched, but the sidecar has not yet responded to its first health ping.
     * No tasks will be assigned in this state.
     */
    STARTING,

    /**
     * The sidecar is running and responding to health pings. The Scheduler
     * may assign tasks to this worker.
     */
    ACTIVE,

    /**
     * An operator has requested graceful removal of this node (e.g. rolling
     * upgrade). The registry stops advertising this worker to the Scheduler
     * for new task assignments. Existing in-flight tasks run to completion.
     */
    DRAINING,

    /**
     * Health pings have failed beyond the timeout threshold, or the sidecar
     * process has exited unexpectedly. The Scheduler must reassign all tasks
     * that were running on this worker.
     */
    UNREACHABLE,

    /**
     * The OpenSearch node has left the cluster (or drain + drain-completion
     * sequence finished). The WorkerInfo record is retained briefly for
     * diagnostic purposes, then removed from the registry.
     */
    DECOMMISSIONED
}
```

### 2.4 HealthMonitor

```java
package org.opensearch.lakehouse.worker;

import java.util.Optional;

/**
 * Background service that periodically pings each worker's sidecar over gRPC
 * and updates the registry when a worker's health status changes.
 *
 * <p>The HealthMonitor runs independently of the Distributed Scheduler. It
 * owns the heartbeat schedule and failure-detection timer.
 */
public interface HealthMonitor {

    /**
     * Start the background monitoring loop. Called once during node startup,
     * after the local sidecar has been launched.
     *
     * @throws IllegalStateException if already started.
     */
    void start();

    /**
     * Gracefully stop monitoring. Outstanding pings are drained; the sidecar
     * process is NOT terminated (that is {@link SidecarManager}'s job).
     * Blocks until the monitoring thread has exited.
     */
    void stop();

    /**
     * Return the most recent health report for a worker, if one has been
     * received within the last {@code worker.health.reportTtlMs} milliseconds.
     *
     * @param nodeId  The OpenSearch node ID to query.
     * @return        The latest report, or empty if the worker is unknown or
     *                its last report has expired.
     */
    Optional<WorkerHealthReport> getWorkerHealth(String nodeId);

    /**
     * Convenience method: return true if the worker is ACTIVE and its most
     * recent health report arrived within the heartbeat-timeout window.
     *
     * @param nodeId  Node to check.
     * @return        true iff the worker can safely receive new tasks.
     */
    boolean isHealthy(String nodeId);

    /**
     * Force an immediate out-of-band health check for a specific node.
     * Used after a task dispatch failure to quickly confirm whether the
     * worker is still alive before declaring it UNREACHABLE.
     *
     * @param nodeId  Node to probe.
     */
    void probeNow(String nodeId);
}
```

### 2.5 WorkerHealthReport

Immutable data object populated by each gRPC heartbeat response.

```java
package org.opensearch.lakehouse.worker;

/**
 * Health snapshot reported by a worker's Rust sidecar in response to a
 * gRPC health-check ping or pushed proactively by the sidecar.
 *
 * <p>All percentage fields are in the range [0.0, 100.0].
 */
public final class WorkerHealthReport {

    /** Node that produced this report. */
    private final String nodeId;

    /**
     * Epoch milliseconds when the sidecar generated the report.
     * Used to detect stale reports (e.g. if the coordinator's clock skews).
     */
    private final long lastHeartbeatMs;

    /** Total CPU utilization across all cores (user + sys), percent. */
    private final double cpuPercent;

    /**
     * Fraction of the sidecar's configured off-heap memory pool in use.
     * Computed as {@code usedMemoryBytes / totalMemoryBytes * 100}.
     */
    private final double memoryPercent;

    /**
     * Fraction of the data directory's disk capacity used, percent.
     * Monitors spill-to-disk usage by DataFusion operators.
     */
    private final double diskUsagePercent;

    /** Number of DataFusion task fragments actively executing right now. */
    private final int activeTaskCount;

    /**
     * JVM heap utilization on the OpenSearch side of the same node, percent.
     * Provided so the Coordinator can detect JVM-side memory pressure that
     * might delay heartbeat responses.
     */
    private final double jvmHeapPercent;

    /**
     * true if the Rust sidecar process is alive (PID still running) as
     * verified by the JVM watchdog thread on the same node.
     * false triggers an immediate UNREACHABLE transition regardless of
     * other fields.
     */
    private final boolean sidecarAlive;

    /**
     * Observed network bandwidth (sum of ingress + egress) in Mbps at
     * the time of the report. Used by the Scheduler for shuffle placement.
     */
    private final double networkBandwidthMbps;

    public WorkerHealthReport(
        String nodeId,
        long lastHeartbeatMs,
        double cpuPercent,
        double memoryPercent,
        double diskUsagePercent,
        int activeTaskCount,
        double jvmHeapPercent,
        boolean sidecarAlive,
        double networkBandwidthMbps
    ) {
        this.nodeId               = nodeId;
        this.lastHeartbeatMs      = lastHeartbeatMs;
        this.cpuPercent           = cpuPercent;
        this.memoryPercent        = memoryPercent;
        this.diskUsagePercent     = diskUsagePercent;
        this.activeTaskCount      = activeTaskCount;
        this.jvmHeapPercent       = jvmHeapPercent;
        this.sidecarAlive         = sidecarAlive;
        this.networkBandwidthMbps = networkBandwidthMbps;
    }

    public String getNodeId()                 { return nodeId; }
    public long getLastHeartbeatMs()          { return lastHeartbeatMs; }
    public double getCpuPercent()             { return cpuPercent; }
    public double getMemoryPercent()          { return memoryPercent; }
    public double getDiskUsagePercent()       { return diskUsagePercent; }
    public int getActiveTaskCount()           { return activeTaskCount; }
    public double getJvmHeapPercent()         { return jvmHeapPercent; }
    public boolean isSidecarAlive()           { return sidecarAlive; }
    public double getNetworkBandwidthMbps()   { return networkBandwidthMbps; }
}
```

### 2.6 WorkerLifecycleListener

```java
package org.opensearch.lakehouse.worker;

/**
 * Callback interface for components that need to react to worker state changes.
 *
 * <p>All methods are called on the health-monitor thread. Implementations
 * MUST NOT block (no I/O, no heavy computation). Use an async handoff queue
 * if downstream work is expensive.
 */
public interface WorkerLifecycleListener {

    /**
     * A worker has transitioned from STARTING to ACTIVE and is ready to
     * accept task assignments.
     *
     * @param nodeId   The worker that became active.
     * @param info     Full WorkerInfo snapshot at the moment of activation.
     */
    void onWorkerActive(String nodeId, WorkerInfo info);

    /**
     * A worker has entered the DRAINING state. The listener should stop
     * sending new task assignments to this worker. In-flight tasks may
     * continue to completion.
     *
     * @param nodeId   The worker entering drain.
     * @param info     WorkerInfo at the moment drain was triggered.
     */
    void onWorkerDraining(String nodeId, WorkerInfo info);

    /**
     * A worker has become UNREACHABLE (heartbeat timeout or sidecar crash)
     * or has been DECOMMISSIONED (node left the cluster).
     *
     * <p>The listener is responsible for re-queuing tasks that were assigned
     * to this worker.
     *
     * @param nodeId   The worker that was lost.
     * @param reason   Human-readable explanation (e.g. "heartbeat timeout",
     *                 "node left cluster", "sidecar process exited").
     */
    void onWorkerLost(String nodeId, String reason);
}
```

### 2.7 ClusterCapacity

```java
package org.opensearch.lakehouse.worker;

/**
 * Aggregate resource snapshot for the entire active worker pool.
 * Used by admission control to estimate query feasibility before scheduling.
 */
public final class ClusterCapacity {

    /** Number of workers currently in ACTIVE status. */
    private final int activeWorkerCount;

    /** Sum of {@link WorkerInfo#getTotalSlots()} across all ACTIVE workers. */
    private final int totalSlots;

    /** Sum of {@link WorkerInfo#getFreeSlots()} across all ACTIVE workers. */
    private final int freeSlots;

    /** Sum of {@link WorkerInfo#getTotalMemoryBytes()} across all ACTIVE workers. */
    private final long totalMemoryBytes;

    /** Sum of {@link WorkerInfo#getFreeMemoryBytes()} across all ACTIVE workers. */
    private final long freeMemoryBytes;

    public ClusterCapacity(
        int activeWorkerCount,
        int totalSlots,
        int freeSlots,
        long totalMemoryBytes,
        long freeMemoryBytes
    ) {
        this.activeWorkerCount = activeWorkerCount;
        this.totalSlots        = totalSlots;
        this.freeSlots         = freeSlots;
        this.totalMemoryBytes  = totalMemoryBytes;
        this.freeMemoryBytes   = freeMemoryBytes;
    }

    public int getActiveWorkerCount()    { return activeWorkerCount; }
    public int getTotalSlots()           { return totalSlots; }
    public int getFreeSlots()            { return freeSlots; }
    public long getTotalMemoryBytes()    { return totalMemoryBytes; }
    public long getFreeMemoryBytes()     { return freeMemoryBytes; }
    public double getSlotUtilization()   { return totalSlots > 0 ? (double)(totalSlots - freeSlots) / totalSlots : 0.0; }
    public double getMemoryUtilization() { return totalMemoryBytes > 0 ? (double)(totalMemoryBytes - freeMemoryBytes) / totalMemoryBytes : 0.0; }
}
```

### 2.8 SidecarManager

Manages the Rust process lifecycle on the local node.

```java
package org.opensearch.lakehouse.worker;

/**
 * Manages the lifecycle of the Rust/DataFusion sidecar process on the local
 * OpenSearch node. Only the local node's sidecar is managed here; remote
 * sidecars are monitored via health pings but never directly started/stopped.
 */
public interface SidecarManager {

    /**
     * Start the Rust sidecar process. Blocks until the process has written
     * its ready signal (via a pipe or a local gRPC ready probe), or throws
     * if the process fails to start within {@code sidecar.startTimeoutMs}.
     *
     * <p>Called by the WorkerRegistry when the local node's {@code onWorkerJoined}
     * event fires for the first time.
     *
     * @param config  Port assignments and memory configuration to pass to the binary.
     * @return        OS process ID of the started sidecar.
     * @throws SidecarStartException if the process fails to start or times out.
     */
    long startSidecar(SidecarConfig config) throws SidecarStartException;

    /**
     * Gracefully stop the sidecar. Sends SIGTERM; waits up to
     * {@code sidecar.stopGracePeriodMs} for the process to exit; then
     * sends SIGKILL if still running.
     *
     * @param pid  Process ID returned by {@link #startSidecar}.
     */
    void stopSidecar(long pid);

    /**
     * Return true if the sidecar process identified by {@code pid} is
     * currently alive according to the OS (i.e. /proc/{pid} exists on Linux,
     * or equivalent on other platforms).
     *
     * @param pid  Process ID to check.
     */
    boolean isSidecarAlive(long pid);

    /**
     * Restart the sidecar after a crash. Equivalent to
     * {@code stopSidecar(pid)} followed by {@code startSidecar(config)},
     * but logs a crash-restart event for observability.
     *
     * @param pid     Previous (now dead) process ID.
     * @param config  Configuration to use for the new process.
     * @return        New process ID.
     * @throws SidecarStartException if the restart fails.
     */
    long restartSidecar(long pid, SidecarConfig config) throws SidecarStartException;
}
```

### 2.9 SidecarConfig

```java
package org.opensearch.lakehouse.worker;

/**
 * Configuration passed to the Rust sidecar at startup via environment
 * variables and/or command-line arguments.
 */
public final class SidecarConfig {

    /** Port the gRPC task-submission server should bind to. */
    private final int grpcPort;

    /** Port the Arrow Flight shuffle server should bind to. */
    private final int flightPort;

    /** Maximum off-heap memory the DataFusion engine may allocate, in bytes. */
    private final long maxMemoryBytes;

    /** Directory for spilling operator state to disk. */
    private final String spillDirectory;

    /** Path to the Rust sidecar binary. */
    private final String binaryPath;

    /** OpenSearch node ID, passed so the sidecar can self-identify in logs. */
    private final String nodeId;

    /** S3 / object-store endpoint for Parquet reads. */
    private final String objectStoreEndpoint;

    /** TLS certificate path for mutual TLS on gRPC (null = plaintext). */
    private final String tlsCertPath;

    /** TLS key path for mutual TLS on gRPC (null = plaintext). */
    private final String tlsKeyPath;

    // constructor and getters omitted for brevity
}
```

---

## 3. Worker Discovery via OpenSearch Cluster State

### 3.1 Custom Node Role: `datawarehouse`

OpenSearch nodes are assigned roles via `opensearch.yml`. A worker node carries both a standard `data` role (for index storage) and the custom `datawarehouse` role:

```yaml
# opensearch.yml on a worker node
node.roles: [ data, datawarehouse ]
node.attr.grpc_port: 9400
node.attr.flight_port: 9401
node.attr.worker_slots: 32
node.attr.worker_memory_bytes: 68719476736   # 64 GB
```

The `grpc_port`, `flight_port`, `worker_slots`, and `worker_memory_bytes` attributes are read by the registry from the cluster state without any additional RPC. This eliminates a chicken-and-egg problem: the coordinator can learn a worker's ports before it can connect to it.

### 3.2 Registering the Custom Role

The `FlightStreamPlugin` (which already implements `ClusterPlugin`) is extended to register the `datawarehouse` role:

```java
// In FlightStreamPlugin.java — new method added to ClusterPlugin impl
@Override
public Map<String, Supplier<DiscoveryNodeRole>> getRoles() {
    return Collections.singletonMap(
        DatawarehouseNodeRole.ROLE_NAME,
        DatawarehouseNodeRole::new
    );
}
```

```java
package org.opensearch.lakehouse.worker;

import org.opensearch.cluster.node.DiscoveryNodeRole;

/**
 * Sentinel role that marks an OpenSearch node as a DataFusion query worker.
 * Nodes with this role will have their sidecar managed by the WorkerRegistry.
 */
public final class DatawarehouseNodeRole extends DiscoveryNodeRole {

    public static final String ROLE_NAME = "datawarehouse";

    public DatawarehouseNodeRole() {
        super(ROLE_NAME, "d");  // "d" is the single-char abbreviation used in _cat/nodes
    }

    @Override
    public boolean isEnabledByDefault(Settings settings) {
        return false;  // must be explicitly opted in
    }
}
```

### 3.3 ClusterStateListener Implementation

`WorkerRegistryImpl` implements `ClusterStateListener` and is registered with `ClusterService` during plugin initialization.

```java
package org.opensearch.lakehouse.worker;

import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * Core implementation of WorkerRegistry. Listens to cluster state events and
 * maintains the live worker map.
 */
public class WorkerRegistryImpl implements WorkerRegistry, ClusterStateListener {

    // nodeId -> mutable worker state cell (see Section 6)
    private final java.util.concurrent.ConcurrentHashMap<String, WorkerCell> workerCells =
        new java.util.concurrent.ConcurrentHashMap<>();

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (!event.nodesChanged()) {
            return;  // fast path: nothing to do
        }

        DiscoveryNodes previousNodes = event.previousState().nodes();
        DiscoveryNodes currentNodes  = event.state().nodes();

        // Detect newly joined datawarehouse nodes
        for (DiscoveryNode node : currentNodes) {
            if (isDatawarehouseNode(node) && !previousNodes.nodeExists(node.getId())) {
                handleNodeJoined(node);
            }
        }

        // Detect departed datawarehouse nodes
        for (DiscoveryNode node : previousNodes) {
            if (isDatawarehouseNode(node) && !currentNodes.nodeExists(node.getId())) {
                onWorkerLeft(node.getId());
            }
        }
    }

    private boolean isDatawarehouseNode(DiscoveryNode node) {
        return node.getRoles().stream()
            .anyMatch(role -> DatawarehouseNodeRole.ROLE_NAME.equals(role.roleName()));
    }

    private void handleNodeJoined(DiscoveryNode node) {
        String nodeId = node.getId();
        String host   = node.getAddress().getAddress();

        // Read ports and capacity from node attributes set in opensearch.yml
        int grpcPort   = Integer.parseInt(node.getAttributes().getOrDefault("grpc_port",   "9400"));
        int flightPort = Integer.parseInt(node.getAttributes().getOrDefault("flight_port",  "9401"));
        int slots      = Integer.parseInt(node.getAttributes().getOrDefault("worker_slots", "16"));
        long memBytes  = Long.parseLong(  node.getAttributes().getOrDefault("worker_memory_bytes",
                                         String.valueOf(32L * 1024 * 1024 * 1024)));  // default 32 GB

        onWorkerJoined(nodeId, host, grpcPort, flightPort, slots, memBytes);
    }
}
```

### 3.4 Local vs. Remote Node Handling

When `onWorkerJoined` fires, the registry checks whether the joining node is the **local** node (i.e. `clusterService.localNode().getId().equals(nodeId)`):

- **Local node**: the registry delegates to `SidecarManager.startSidecar()` to launch the Rust process. The worker stays in `STARTING` until the first successful health ping.
- **Remote node**: no process management is needed. The worker enters `STARTING` immediately and transitions to `ACTIVE` on the first successful ping.

```
onWorkerJoined(nodeId, ...):
    cell = new WorkerCell(nodeId, host, grpcPort, flightPort, totalSlots, totalMemory, STARTING)
    workerCells.putIfAbsent(nodeId, cell)   // idempotent

    if nodeId == localNode.getId():
        pid = sidecarManager.startSidecar(buildSidecarConfig(cell))
        cell.updateSidecarPid(pid)
        // Sidecar now starting; first health ping will fire STARTING -> ACTIVE
    else:
        // Remote node: health monitor will transition STARTING -> ACTIVE
        healthMonitor.probeNow(nodeId)
```

---

## 4. Sidecar Process Management

### 4.1 Startup Sequence

```
JVM node starts
    |
    v
FlightStreamPlugin.createComponents() called by OpenSearch
    |
    +-- WorkerRegistryImpl created + registered as ClusterStateListener
    +-- SidecarManagerImpl created
    +-- HealthMonitorImpl created, start() called
    |
    v
ClusterChangedEvent fires (local node appears in cluster state)
    |
    v
onWorkerJoined(localNodeId, ...) called
    |
    v
SidecarManager.startSidecar(config):
    1. Resolve binary path from config (default: $OPENSEARCH_HOME/plugins/lakehouse-worker/worker-sidecar)
    2. Build environment: GRPC_PORT, FLIGHT_PORT, MAX_MEMORY_BYTES, SPILL_DIR, NODE_ID, OBJECT_STORE_ENDPOINT
    3. ProcessBuilder.start() → Process object
    4. Register shutdown hook: Runtime.getRuntime().addShutdownHook(stopSidecarThread)
    5. Spawn watchdog thread: polls Process.isAlive() every 5s; calls registry.onSidecarCrash(pid) if dead
    6. Wait for ready probe: gRPC ping with 30s timeout
    7. Return Process.pid()
```

### 4.2 Ready Probe

The sidecar is considered "up" when a gRPC `Health.Check` call to `grpc.health.v1.Health/Check` returns `SERVING`. The `SidecarManager` polls this endpoint every 1 second for up to `sidecar.startTimeoutMs` (default 30 s). If the timeout expires, a `SidecarStartException` is thrown and the local node's status remains `STARTING` (causing the registry to retry startup after `sidecar.retryDelayMs`).

### 4.3 Crash Recovery

```
Watchdog thread detects Process.isAlive() == false
    |
    v
registry.onSidecarCrash(nodeId, pid):
    1. Transition worker ACTIVE/DRAINING -> UNREACHABLE
    2. Notify all WorkerLifecycleListeners: onWorkerLost(nodeId, "sidecar process exited")
    3. If crashCount < MAX_AUTO_RESTARTS (default 3) AND node still in cluster state:
        a. Wait exponential backoff: min(2^crashCount * 1s, 30s)
        b. SidecarManager.restartSidecar(deadPid, config)
        c. On success: reset worker to STARTING, probeNow(nodeId)
    4. If crashCount >= MAX_AUTO_RESTARTS:
        a. Log CRITICAL alert
        b. Leave worker in UNREACHABLE state
        c. Alert the operator via OpenSearch alerting API
```

### 4.4 Graceful Shutdown

```
OpenSearch node receives SIGTERM
    |
    v
Plugin.close() called
    |
    v
WorkerRegistryImpl.close():
    1. Transition local worker ACTIVE -> DRAINING
    2. Notify listeners: onWorkerDraining(localNodeId, ...)
    3. Wait for activeTaskCount == 0 (poll every 500ms, up to drainTimeoutMs = 60s)
    4. SidecarManager.stopSidecar(localPid):
        a. Send SIGTERM to process
        b. Wait up to stopGracePeriodMs (default 10s) for graceful exit
        c. If still alive: send SIGKILL
    5. HealthMonitor.stop()
    6. Transition local worker DRAINING -> DECOMMISSIONED
```

---

## 5. Health Monitoring Protocol

### 5.1 Heartbeat Architecture

The health monitor uses a **pull model**: the Java coordinator pings each worker's gRPC endpoint on a fixed schedule. This is intentional — it avoids the complexity of a push-based system where worker failures silently stop sending heartbeats without the coordinator knowing whether it lost the heartbeat or the worker.

Workers MAY also push unsolicited status updates (e.g. when a task completes or memory spikes), but the health monitor does not depend on them for failure detection.

### 5.2 gRPC Health Service Definition

The Rust sidecar implements the standard gRPC Health Checking Protocol plus a custom `WorkerHealth` service:

```protobuf
// health.proto (standard gRPC health check)
service Health {
  rpc Check(HealthCheckRequest) returns (HealthCheckResponse);
}

// worker_health.proto (custom, richer metrics)
service WorkerHealth {
  rpc GetHealthReport(HealthReportRequest) returns (HealthReportResponse);
}

message HealthReportRequest {
  string node_id = 1;
}

message HealthReportResponse {
  string node_id             = 1;
  int64  timestamp_ms        = 2;
  double cpu_percent         = 3;
  double memory_percent      = 4;
  double disk_usage_percent  = 5;
  int32  active_task_count   = 6;
  double jvm_heap_percent    = 7;  // filled in by JVM proxy if available
  bool   sidecar_alive       = 8;
  double network_bandwidth_mbps = 9;
}
```

### 5.3 Monitoring Loop

```java
// HealthMonitorImpl — simplified monitoring loop
private void monitoringLoop() {
    while (!stopped) {
        long tickStart = System.currentTimeMillis();

        for (WorkerCell cell : registry.getAllCells()) {
            if (cell.getStatus() == WorkerStatus.DECOMMISSIONED) continue;

            submitHealthPing(cell);   // non-blocking; result handled in callback
        }

        long elapsed = System.currentTimeMillis() - tickStart;
        long sleepMs = Math.max(0, PING_INTERVAL_MS - elapsed);
        Thread.sleep(sleepMs);
    }
}

private void submitHealthPing(WorkerCell cell) {
    String nodeId = cell.getNodeId();
    long deadline = System.currentTimeMillis() + PING_TIMEOUT_MS;

    workerHealthStub.getHealthReport(
        HealthReportRequest.newBuilder().setNodeId(nodeId).build(),
        new StreamObserver<HealthReportResponse>() {
            @Override
            public void onNext(HealthReportResponse resp) {
                WorkerHealthReport report = toHealthReport(resp);
                cell.updateHealthReport(report);
                handleHealthReport(cell, report);
            }

            @Override
            public void onError(Throwable t) {
                handlePingFailure(cell, t);
            }

            @Override
            public void onCompleted() {}
        }
    );
}
```

### 5.4 Failure Detection

Each `WorkerCell` tracks consecutive ping failures. When a ping fails (gRPC error, timeout, or `sidecarAlive == false`), the failure counter increments. On success it resets to zero.

```
handlePingFailure(cell, error):
    cell.incrementConsecutiveFailures()

    if cell.consecutiveFailures == WARN_THRESHOLD (default 2):
        log.warn("Worker {} ping failed {} times: {}", nodeId, count, error)

    if cell.consecutiveFailures >= FAILURE_THRESHOLD (default 3):
        // Heartbeat timeout exceeded: PING_INTERVAL_MS * FAILURE_THRESHOLD = 30s default
        if cell.getStatus() == ACTIVE or cell.getStatus() == STARTING:
            registry.transitionStatus(nodeId, ACTIVE -> UNREACHABLE, "heartbeat timeout after " + elapsed + "ms")
            notifyListeners: onWorkerLost(nodeId, "heartbeat timeout")

handleHealthReport(cell, report):
    cell.resetConsecutiveFailures()
    cell.setLastReportMs(report.getLastHeartbeatMs())

    if !report.isSidecarAlive():
        // Sidecar process died; JVM is still alive but sidecar is gone
        registry.transitionStatus(nodeId, * -> UNREACHABLE, "sidecar process not alive")
        notifyListeners: onWorkerLost(nodeId, "sidecar process exited")
        return

    if cell.getStatus() == STARTING:
        // First successful ping: worker is now ready
        registry.transitionStatus(nodeId, STARTING -> ACTIVE, "first health ping succeeded")
        notifyListeners: onWorkerActive(nodeId, cell.snapshot())
```

### 5.5 Timing Parameters

| Parameter | Default | Effective Timeout |
|---|---|---|
| `worker.health.pingIntervalMs` | 10,000 ms | Ping sent every 10 s |
| `worker.health.pingTimeoutMs` | 5,000 ms | Individual ping RPC timeout |
| `worker.health.warnThreshold` | 2 | Log warning after 2 consecutive misses (20 s) |
| `worker.health.failureThreshold` | 3 | Declare UNREACHABLE after 3 consecutive misses (30 s) |
| `worker.health.reportTtlMs` | 60,000 ms | Cached report valid for 60 s |
| `sidecar.startTimeoutMs` | 30,000 ms | Max time to wait for sidecar ready signal |
| `sidecar.stopGracePeriodMs` | 10,000 ms | SIGTERM-to-SIGKILL wait |
| `sidecar.retryDelayMs` | 5,000 ms | Base delay before restart attempt |

---

## 6. Slot and Memory Tracking

### 6.1 WorkerCell — Mutable State Container

Each entry in the registry is a `WorkerCell` that holds mutable counters. The immutable `WorkerInfo` view is derived on demand.

```java
package org.opensearch.lakehouse.worker;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Mutable per-worker state. Slot and memory fields use atomic types to allow
 * lock-free reservation/release from multiple Scheduler threads concurrently.
 */
final class WorkerCell {

    private final String nodeId;
    private final String host;
    private final int grpcPort;
    private final int flightPort;
    private final int totalSlots;
    private final long totalMemoryBytes;
    private final Instant startTime;

    /** Atomically decremented/incremented by tryReserve/release. */
    private final AtomicInteger freeSlots;

    /** Atomically incremented/decremented by tryReserve/release (bytes in use). */
    private final AtomicLong usedMemoryBytes;

    /** Lifecycle status; updated by registry and health monitor. */
    private final AtomicReference<WorkerStatus> status;

    /** PID of the Rust sidecar, -1 if not yet known. */
    private volatile long sidecarPid = -1;

    /** Number of consecutive ping failures. */
    private volatile int consecutiveFailures = 0;

    /** Last health report received from the sidecar; volatile reference swap. */
    private volatile WorkerHealthReport lastHealthReport;

    WorkerCell(String nodeId, String host, int grpcPort, int flightPort,
               int totalSlots, long totalMemoryBytes) {
        this.nodeId           = nodeId;
        this.host             = host;
        this.grpcPort         = grpcPort;
        this.flightPort       = flightPort;
        this.totalSlots       = totalSlots;
        this.totalMemoryBytes = totalMemoryBytes;
        this.freeSlots        = new AtomicInteger(totalSlots);
        this.usedMemoryBytes  = new AtomicLong(0);
        this.status           = new AtomicReference<>(WorkerStatus.STARTING);
        this.startTime        = Instant.now();
    }

    /**
     * Attempt to atomically reserve {@code slots} slots and {@code memoryBytes}
     * of memory. Returns true only if both reservations succeed simultaneously.
     *
     * <p>This is a compare-and-set loop: if slots CAS succeeds but memory CAS
     * fails, the slot reservation is rolled back before returning false.
     *
     * @param slots        Number of task slots to reserve (typically 1 per task).
     * @param memoryBytes  Off-heap memory to reserve in bytes.
     * @return             true if reservation succeeded; false if insufficient capacity.
     */
    boolean tryReserve(int slots, long memoryBytes) {
        // Phase 1: reserve slots
        while (true) {
            int current = freeSlots.get();
            if (current < slots) return false;
            if (freeSlots.compareAndSet(current, current - slots)) break;
        }

        // Phase 2: reserve memory — roll back slots if insufficient
        while (true) {
            long currentMem = usedMemoryBytes.get();
            if (currentMem + memoryBytes > totalMemoryBytes) {
                freeSlots.addAndGet(slots);  // rollback
                return false;
            }
            if (usedMemoryBytes.compareAndSet(currentMem, currentMem + memoryBytes)) break;
        }

        return true;
    }

    /**
     * Release previously reserved slots and memory. Called when a task
     * completes, fails, or is cancelled.
     *
     * @param slots        Slots to return.
     * @param memoryBytes  Bytes to return.
     */
    void release(int slots, long memoryBytes) {
        freeSlots.addAndGet(slots);
        usedMemoryBytes.addAndGet(-memoryBytes);
    }

    /** Construct an immutable snapshot for external consumers. */
    WorkerInfo snapshot() {
        return new WorkerInfo(
            nodeId, host, grpcPort, flightPort,
            totalSlots, freeSlots.get(),
            totalMemoryBytes, usedMemoryBytes.get(),
            status.get(), sidecarPid, startTime
        );
    }

    // Getters and setters for mutable fields omitted for brevity
}
```

### 6.2 Overcommit Safety

The registry enforces a configurable overcommit ceiling to avoid filling workers to 100% and leaving no room for JVM GC and OS overhead:

```
MAX_SLOT_UTILIZATION    = worker.slots.maxUtilization   (default 1.0 — no slot overcommit)
MAX_MEMORY_UTILIZATION  = worker.memory.maxUtilization  (default 0.90 — 90% memory ceiling)
```

`tryReserve` compares against `totalMemoryBytes * MAX_MEMORY_UTILIZATION`, not the raw total. This is baked into the comparison threshold set at construction time.

### 6.3 Updating Capacity from Health Reports

When a health report arrives with an `activeTaskCount` that disagrees with the registry's internal accounting (e.g. after a coordinator restart), the registry reconciles:

```
if abs(report.activeTaskCount - (totalSlots - cell.freeSlots.get())) > RECONCILE_THRESHOLD:
    log.warn("Slot count mismatch on {}; reconciling from sidecar report", nodeId)
    cell.freeSlots.set(totalSlots - report.activeTaskCount)
```

This prevents slot leaks if the coordinator crashes while tasks are running and re-starts without task state.

---

## 7. Worker Lifecycle State Machine

### 7.1 States

```
+----------+     sidecar started,          +--------+     drain          +---------+
| STARTING |  ---first ping succeeds--->   | ACTIVE |  ---requested--->  | DRAINING|
+----------+                               +--------+                    +---------+
     |                                         |                              |
     | sidecar failed to start                 | heartbeat timeout            | all tasks done /
     | (after MAX_AUTO_RESTARTS)               | OR sidecar crash             | node left cluster
     v                                         v                              v
+-----------+                          +-------------+              +---------------+
| UNREACHABLE|<----- heartbeat  ------>| UNREACHABLE |              | DECOMMISSIONED|
+-----------+    timeout from STARTING +-------------+              +---------------+
     |                                       |
     | node leaves cluster                   | node leaves cluster
     v                                       v
+---------------+                   +---------------+
| DECOMMISSIONED|                   | DECOMMISSIONED|
+---------------+                   +---------------+
```

### 7.2 Transition Table

| From | To | Trigger | Action |
|---|---|---|---|
| `STARTING` | `ACTIVE` | First successful health ping | Notify `onWorkerActive`; begin accepting task assignments |
| `STARTING` | `UNREACHABLE` | `failureThreshold` misses or sidecar crash | Notify `onWorkerLost`; attempt auto-restart |
| `ACTIVE` | `DRAINING` | `DrainRequest` received | Notify `onWorkerDraining`; stop new assignments |
| `ACTIVE` | `UNREACHABLE` | `failureThreshold` misses or `sidecarAlive=false` | Notify `onWorkerLost`; scheduler reassigns tasks |
| `DRAINING` | `DECOMMISSIONED` | All tasks drained AND node leaves cluster state | Sidecar stopped; cell removed after TTL |
| `DRAINING` | `UNREACHABLE` | Sidecar crashes during drain | Notify `onWorkerLost`; remaining tasks reassigned |
| `UNREACHABLE` | `ACTIVE` | Auto-restart succeeds and ping resumes | Notify `onWorkerActive` |
| `UNREACHABLE` | `DECOMMISSIONED` | Node leaves cluster state | Cell removal scheduled |
| Any | `DECOMMISSIONED` | Node leaves `ClusterState` | Final cleanup |

### 7.3 Transition Implementation

Status transitions use a CAS loop to avoid races between the health monitor thread and the cluster-state listener thread:

```java
/**
 * Atomically transition a worker's status if it is currently in {@code expected}.
 * @return true if the transition was applied; false if the current status was not {@code expected}.
 */
boolean transitionStatus(String nodeId, WorkerStatus expected, WorkerStatus next, String reason) {
    WorkerCell cell = workerCells.get(nodeId);
    if (cell == null) return false;

    boolean changed = cell.getStatus().compareAndSet(expected, next);
    if (changed) {
        log.info("Worker {} status: {} -> {} (reason: {})", nodeId, expected, next, reason);
        fireLifecycleEvent(nodeId, next, reason);
    }
    return changed;
}
```

---

## 8. Graceful Drain and Decommission

### 8.1 Drain Initiation

Drain is triggered by one of three sources:
1. **Operator command** via a REST endpoint: `POST /_lakehouse/workers/{nodeId}/_drain`
2. **Rolling-upgrade tooling** that calls the drain API before taking the node offline
3. **Automatic drain** when the health monitor detects `consecutiveFailures == WARN_THRESHOLD` (early warning), giving the node a chance to drain before being declared fully unreachable

```java
/**
 * Initiate a graceful drain of the given worker.
 *
 * @param nodeId          Worker to drain.
 * @param drainTimeoutMs  Maximum time to wait for in-flight tasks to complete.
 *                        If exceeded, the worker is forced to DECOMMISSIONED.
 */
public void drainWorker(String nodeId, long drainTimeoutMs) {
    boolean transitioned = transitionStatus(nodeId, WorkerStatus.ACTIVE, WorkerStatus.DRAINING,
        "operator-initiated drain");
    if (!transitioned) {
        log.warn("Could not drain worker {} (current status: {})", nodeId,
            workerCells.get(nodeId).getStatus());
        return;
    }

    // Fire listener so Scheduler stops routing new tasks to this worker
    // (listener's onWorkerDraining removes the node from the active worker set)

    // Schedule timeout enforcement
    drainExecutor.schedule(() -> forceDrainComplete(nodeId), drainTimeoutMs, TimeUnit.MILLISECONDS);
}
```

### 8.2 Drain Completion Check

The registry polls the cell's `freeSlots` counter. When `freeSlots == totalSlots` (all slots returned), all tasks have completed:

```
// Runs on drain-executor thread every 500ms
drainCompletionPoller(nodeId):
    cell = workerCells.get(nodeId)
    if cell == null or cell.getStatus() != DRAINING:
        return  // drain already complete or worker gone

    if cell.freeSlots.get() == cell.totalSlots:
        log.info("Worker {} fully drained", nodeId)
        transitionStatus(nodeId, DRAINING, DECOMMISSIONED, "drain complete")
        sidecarManager.stopSidecar(cell.sidecarPid)
        scheduleRemoval(nodeId, DECOMMISSIONED_TTL_MS)
    else:
        log.debug("Worker {} draining: {} tasks still running",
            nodeId, cell.totalSlots - cell.freeSlots.get())
```

### 8.3 Forced Drain Completion

If `drainTimeoutMs` elapses before all tasks finish:

```
forceDrainComplete(nodeId):
    cell = workerCells.get(nodeId)
    if cell.getStatus() != DRAINING: return  // already done

    remainingTasks = cell.totalSlots - cell.freeSlots.get()
    log.warn("Worker {} drain timeout; {} tasks still running; forcing DECOMMISSIONED", nodeId, remainingTasks)

    // Notify scheduler to reassign the remaining in-flight tasks
    notifyListeners: onWorkerLost(nodeId, "drain timeout — forced decommission")

    transitionStatus(nodeId, DRAINING, DECOMMISSIONED, "drain timeout")
    sidecarManager.stopSidecar(cell.sidecarPid)   // SIGTERM then SIGKILL
    scheduleRemoval(nodeId, DECOMMISSIONED_TTL_MS)
```

### 8.4 Cell Removal

`WorkerCell` entries are not removed immediately on `DECOMMISSIONED` to allow in-flight diagnostic queries (`getWorker(nodeId)`) to succeed briefly. They are evicted after `worker.decommissioned.ttlMs` (default 300,000 ms / 5 minutes):

```java
private void scheduleRemoval(String nodeId, long ttlMs) {
    cleanupExecutor.schedule(() -> {
        workerCells.remove(nodeId);
        log.debug("Worker {} removed from registry after TTL", nodeId);
    }, ttlMs, TimeUnit.MILLISECONDS);
}
```

---

## 9. Integration with OpenSearch APIs

### 9.1 Plugin Initialization Sequence

```java
// WorkerRegistryImpl integration within FlightStreamPlugin.createComponents()

@Override
public Collection<Object> createComponents(
    Client client,
    ClusterService clusterService,
    ThreadPool threadPool,
    ...
) {
    SidecarManager sidecarManager   = new SidecarManagerImpl(environment);
    WorkerRegistryImpl registry     = new WorkerRegistryImpl(
        clusterService.localNode(),
        sidecarManager,
        settings
    );
    HealthMonitorImpl healthMonitor = new HealthMonitorImpl(registry, threadPool, settings);

    registry.setHealthMonitor(healthMonitor);

    // Register for cluster state changes
    clusterService.addListener(registry);

    // Start background monitoring
    healthMonitor.start();

    // Expose to the Distributed Scheduler via IoC
    return Arrays.asList(registry, healthMonitor, sidecarManager);
}
```

### 9.2 Node Attributes Publication

The local node's gRPC and Flight ports are written to `opensearch.yml` at deployment time (static). However, if ports are dynamically assigned (e.g. ephemeral port allocation in test environments), the `SidecarManager` can update node attributes via the `NodeService` API after the sidecar binds to its port:

```java
// After sidecar confirms its bound ports via the ready-probe response:
Map<String, String> updatedAttrs = new HashMap<>(localNode.getAttributes());
updatedAttrs.put("grpc_port",   String.valueOf(actualGrpcPort));
updatedAttrs.put("flight_port", String.valueOf(actualFlightPort));
// Note: OpenSearch does not support dynamic attribute mutation in production;
// this is a test-environment affordance. In production, ports are fixed in opensearch.yml.
```

### 9.3 REST API for Drain

The `FlightStreamPlugin` registers a REST handler for operator drain commands:

```
POST  /_lakehouse/workers/{nodeId}/_drain
      Body: { "timeout_ms": 60000 }

GET   /_lakehouse/workers
      Returns: list of all workers with status, slots, memory, last heartbeat

GET   /_lakehouse/workers/{nodeId}
      Returns: single worker detail including full WorkerHealthReport
```

### 9.4 OpenSearch Action for Cross-Node Registry Queries

The coordinator needs to query the registry even when the Scheduler runs on a different node. A dedicated OpenSearch transport action forwards registry queries:

```java
// Registered in FlightStreamPlugin.getActions()
new ActionHandler<>(WorkerRegistryAction.INSTANCE, TransportWorkerRegistryAction.class)
```

`TransportWorkerRegistryAction` forwards the request to the elected coordinator node, which holds the in-memory `WorkerRegistryImpl`.

---

## 10. Thread Model and Synchronization

### 10.1 Thread Pools

| Thread Pool | Size | Purpose |
|---|---|---|
| `worker-health-monitor` | 1 | Runs the monitoring loop tick; owns all status transitions |
| `worker-health-pings` | 8 | gRPC ping stubs; non-blocking async calls |
| `worker-drain-executor` | 2 | Drain completion polling and timeout enforcement |
| `worker-sidecar-watchdog` | 1 per local sidecar | Polls `Process.isAlive()` for the local sidecar |
| OpenSearch `cluster` thread | (framework-managed) | Delivers `ClusterChangedEvent` to `clusterChanged()` |

### 10.2 Concurrency Design

```
cluster thread (ClusterService):
    - Calls onWorkerJoined / onWorkerLeft
    - Creates new WorkerCell with status=STARTING (putIfAbsent is atomic)
    - Does NOT write to slot/memory counters

worker-health-monitor thread (single, owns transitions):
    - Reads all WorkerCell statuses
    - Calls submitHealthPing (non-blocking; result delivered to ping callback thread)
    - Processes ping results via a queue (LinkedBlockingQueue<PingResult>)
    - Applies status transitions via CAS on WorkerCell.status
    - Notifies lifecycle listeners (must be fast/non-blocking)

worker-health-pings threads (8 threads, gRPC callbacks):
    - Receive gRPC responses / errors
    - Enqueue PingResult onto health-monitor's result queue
    - Do NOT update WorkerCell directly (avoids races with monitor thread)

Scheduler threads (ResourceManager calls — any thread):
    - Call WorkerCell.tryReserve() / release() — lock-free CAS loops
    - Read WorkerCell.snapshot() — safe because WorkerInfo is immutable
    - Do NOT call status transition methods

Drain executor threads (2 threads):
    - Read WorkerCell.freeSlots (volatile read)
    - Call transitionStatus (CAS — safe from any thread)
    - Call SidecarManager.stopSidecar (blocking, acceptable on drain executor)
```

### 10.3 Avoiding Lock Contention

The hot path — `tryReserve` and `release` called on every task start/finish — uses only `AtomicInteger` and `AtomicLong` CAS operations. No `synchronized` blocks or `ReentrantLock` are on the reservation path. This is critical because the Scheduler's single `scheduler-main` thread calls `tryReserve` for every task in every scheduling tick.

The status transition path uses `AtomicReference.compareAndSet` which is also non-blocking. The worst case is that two threads race to transition the same worker (e.g. health monitor declares UNREACHABLE at the same moment the cluster state listener fires `onWorkerLeft`): the CAS ensures exactly one transition wins, and the loser's CAS returns false, leaving the state clean.

---

## 11. Example: Node Join, Steady State, and Node Leave

This section walks through the complete lifecycle of a worker node from cold start through failure and auto-restart.

**Cluster**: 1 coordinator node (C), 3 existing worker nodes (W1, W2, W3). A fourth worker (W4) is being added.

---

### T=0ms — W4 Node Starts

W4's OpenSearch JVM boots. Its `opensearch.yml`:
```yaml
node.roles: [ data, datawarehouse ]
node.attr.grpc_port: 9400
node.attr.flight_port: 9401
node.attr.worker_slots: 32
node.attr.worker_memory_bytes: 68719476736
```

W4's JVM joins the cluster and publishes its node entry to the cluster state.

---

### T=150ms — Cluster State Propagates to Coordinator

`ClusterChangedEvent` fires on the coordinator (C). `WorkerRegistryImpl.clusterChanged()` detects W4 as a new `datawarehouse` node.

```
handleNodeJoined(W4):
    cell = WorkerCell(nodeId="W4", host="10.0.0.4", grpcPort=9400, flightPort=9401,
                      totalSlots=32, totalMemory=64GB)
    cell.status = STARTING
    workerCells.put("W4", cell)
    // W4 is remote; not starting sidecar locally
    healthMonitor.probeNow("W4")
```

*On W4's own node*: its `FlightStreamPlugin.createComponents()` also fires, the local `onWorkerJoined` triggers, and `SidecarManager.startSidecar()` launches the Rust binary.

---

### T=180ms — W4's Sidecar Starts

Rust binary forks. Binds gRPC on :9400, Arrow Flight on :9401. Writes "SERVING" to the gRPC health endpoint.

`SidecarManager` on W4 polls the ready probe and gets `SERVING`. Returns `pid=78234`.

`WorkerCell.sidecarPid` = 78234.

---

### T=800ms — First Health Ping from Coordinator Succeeds

`HealthMonitorImpl` sends a `GetHealthReport` RPC to W4:9400. Response:
```
nodeId              = "W4"
timestamp_ms        = 1712345678800
cpu_percent         = 2.1
memory_percent      = 0.5
active_task_count   = 0
sidecar_alive       = true
```

```
handleHealthReport(W4_cell, report):
    consecutiveFailures = 0
    cell.lastHealthReport = report
    cell.status is STARTING → transition STARTING -> ACTIVE
    fireLifecycleEvent: onWorkerActive("W4", W4_cell.snapshot())
```

**Scheduler notified**: W4 is now in the active worker pool. `getActiveWorkers()` returns [W1, W2, W3, W4].

---

### T=1000ms — W4 Accepts Tasks

`ClusterCapacity` snapshot: 4 workers, 128 total slots (4 × 32), 128 free slots.

The Scheduler begins assigning scan tasks to W4 in the next scheduling tick.

```
W4_cell.tryReserve(slots=1, memoryBytes=2_147_483_648):  // 1 slot, 2 GB
    freeSlots: 32 -> 31
    usedMemory: 0 -> 2GB
    return true
```

---

### T=45000ms — W4 Sidecar Crashes (Simulated OOM)

The Rust process exits with code 137 (SIGKILL from OOM killer).

W4's watchdog thread detects `Process.isAlive() == false`:
```
onSidecarCrash("W4", pid=78234):
    transitionStatus("W4", ACTIVE -> UNREACHABLE, "sidecar process exited with code 137")
    notifyListeners: onWorkerLost("W4", "sidecar process exited")
    crashCount = 1 (< MAX_AUTO_RESTARTS=3)
    schedule restart in 1s (backoff: 2^1 * 1s)
```

**Scheduler notified** via `onWorkerLost`: all tasks on W4 are moved back to `PENDING`.

---

### T=46000ms — Auto-Restart Attempt

```
SidecarManager.restartSidecar(oldPid=78234, config):
    stopSidecar(78234)  // already dead, no-op
    startSidecar(config)
    ready probe: SERVING after 250ms
    return newPid=79012
```

```
W4_cell.sidecarPid = 79012
W4_cell.status stays UNREACHABLE until first ping succeeds
healthMonitor.probeNow("W4")
```

---

### T=46300ms — Ping Confirms Recovery

Health ping to W4:9400 returns `SERVING`.

```
transitionStatus("W4", UNREACHABLE -> ACTIVE, "health ping resumed after restart")
notifyListeners: onWorkerActive("W4", ...)
```

**Scheduler notified**: W4 re-enters the active pool. Previously reassigned tasks may have already started on W1-W3; W4 will receive new tasks from the next scheduling tick.

---

### T=120000ms — Operator Drains W4 for Maintenance

```
POST /_lakehouse/workers/W4/_drain  { "timeout_ms": 60000 }
```

```
drainWorker("W4", drainTimeoutMs=60000):
    transitionStatus("W4", ACTIVE -> DRAINING, "operator drain")
    notifyListeners: onWorkerDraining("W4", ...)
    // Scheduler stops sending new tasks to W4
    schedule forceDrainComplete in 60s
```

W4 currently has 5 tasks running. Over the next ~8 seconds they complete:

```
W4_cell.release(1, memBytes)  // × 5 times
// freeSlots: 27 -> 32
```

Drain poller fires at T=120500ms:
```
freeSlots (32) == totalSlots (32) → drain complete
transitionStatus("W4", DRAINING -> DECOMMISSIONED, "drain complete")
SidecarManager.stopSidecar(79012)  // SIGTERM -> graceful exit
scheduleRemoval("W4", 300_000ms)
```

W4's cell is removed from the registry at T=420000ms.

---

### Timeline Summary

```
T(ms)     Event
0         W4 JVM boots
150       ClusterChangedEvent; WorkerCell created (STARTING)
180       Sidecar pid=78234 starts on W4
800       First health ping succeeds → ACTIVE; Scheduler notified
1000      W4 receives first task assignment
45000     Sidecar crashes (OOM); → UNREACHABLE; tasks reassigned
46000     Auto-restart: pid=79012
46300     Health ping resumes → ACTIVE
120000    Operator drain requested → DRAINING
120500    All tasks complete → DECOMMISSIONED; sidecar SIGTERM
420000    WorkerCell removed from registry
```

---

## Appendix: Key Configuration Parameters

| Parameter | Default | Description |
|---|---|---|
| `worker.health.pingIntervalMs` | 10,000 | How often the monitor pings each worker |
| `worker.health.pingTimeoutMs` | 5,000 | Per-ping gRPC deadline |
| `worker.health.warnThreshold` | 2 | Consecutive misses before logging a warning |
| `worker.health.failureThreshold` | 3 | Consecutive misses before declaring UNREACHABLE |
| `worker.health.reportTtlMs` | 60,000 | Cached health report validity window |
| `worker.slots.maxUtilization` | 1.0 | Maximum slot fill fraction (1.0 = no overcommit) |
| `worker.memory.maxUtilization` | 0.90 | Maximum memory fill fraction before rejecting reservations |
| `worker.decommissioned.ttlMs` | 300,000 | How long a DECOMMISSIONED cell is retained for diagnostics |
| `worker.reconcile.threshold` | 2 | Active-task count delta triggering slot reconciliation |
| `sidecar.binaryPath` | `$OPENSEARCH_HOME/plugins/lakehouse-worker/worker-sidecar` | Path to the Rust binary |
| `sidecar.startTimeoutMs` | 30,000 | Max wait for sidecar ready probe |
| `sidecar.stopGracePeriodMs` | 10,000 | SIGTERM-to-SIGKILL grace period on shutdown |
| `sidecar.retryDelayMs` | 1,000 | Base delay for exponential restart backoff |
| `sidecar.maxAutoRestarts` | 3 | Auto-restart attempts before alerting and giving up |
| `sidecar.drainTimeoutMs` | 60,000 | Maximum drain wait before forced DECOMMISSIONED |
| `node.attr.grpc_port` | 9400 | Node attribute: gRPC task-submission port |
| `node.attr.flight_port` | 9401 | Node attribute: Arrow Flight shuffle port |
| `node.attr.worker_slots` | 16 | Node attribute: execution slot count |
| `node.attr.worker_memory_bytes` | 34359738368 | Node attribute: off-heap memory budget (32 GB default) |

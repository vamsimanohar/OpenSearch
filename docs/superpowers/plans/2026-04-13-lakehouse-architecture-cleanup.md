# Lakehouse Architecture Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Clean up lakehouse plugin architecture by extracting a focused interface, eliminating the static backend holder, and removing the DefaultPlanExecutor middleman so lakehouse owns its full execution lifecycle.

**Architecture:** Three incremental PRs. PR 1 extracts `ExternalQueryBackend` from `AnalyticsSearchBackendPlugin` so lakehouse only depends on the `executeRemoteQuery()` capability. PR 2 eliminates `RemoteQueryBackendHolder` by Guice-binding the new interface via a lambda adapter (avoids Guice introspecting DataFusionPlugin). PR 3 makes `LakehouseQueryTransportAction` call lakehouse execution directly, bypassing `UnifiedQueryService → PushDownPlanner → DefaultPlanExecutor → ExternalTableExecutor` callback chain.

**Tech Stack:** Java 25, OpenSearch Guice, Calcite, analytics-framework SPI

---

## File Structure

### New Files
| File | PR | Responsibility |
|------|-----|----------------|
| `sandbox/libs/analytics-framework/src/main/java/org/opensearch/analytics/exec/ExternalQueryBackend.java` | 1 | Interface with just `executeRemoteQuery()` |
| `sandbox/plugins/lakehouse-iceberg/src/main/java/org/opensearch/lakehouse/exec/LakehouseQueryExecutor.java` | 3 | Parses SQL/PPL → RelNode, executes via Iceberg scan + DataFusion |

### Modified Files
| File | PR | Change |
|------|-----|--------|
| `AnalyticsSearchBackendPlugin.java` | 1 | Extend `ExternalQueryBackend` |
| `RemoteQueryBackendHolder.java` | 1→2 | PR 1: use `ExternalQueryBackend` type. PR 2: delete |
| `WorkerQueryExecutor.java` | 1→2 | PR 1: use `ExternalQueryBackend` type. PR 2: remove static field, take backend as param |
| `WorkerQueryTransportAction.java` | 2 | Inject `ExternalQueryBackend`, pass to executor |
| `DistributedScanExecutor.java` | 2 | Constructor takes `ExternalQueryBackend`, passes to `WorkerQueryExecutor` |
| `LakehouseState.java` | 2 | `initDistributedExecutor()` receives backend |
| `AnalyticsPlugin.java` | 2→3 | PR 2: Guice-bind `ExternalQueryBackend` via lambda. PR 3: remove ExternalTableExecutor loading |
| `LakehouseQueryTransportAction.java` | 3 | Use `LakehouseQueryExecutor` instead of `UnifiedQueryService` |
| `LakehousePlugin.java` | 3 | Remove `ExternalTableExecutor` implementation, remove `prepareScan()` |
| `ExternalScanContext.java` | 3 | Remove `preComputedResults` field |
| `DefaultPlanExecutor.java` | 3 | Remove external table routing |

### Deleted Files
| File | PR |
|------|-----|
| `RemoteQueryBackendHolder.java` | 2 |
| `META-INF/services/org.opensearch.analytics.exec.ExternalTableExecutor` | 3 |
| `ExternalTableExecutor.java` | 3 |

---

## Task 1: Extract `ExternalQueryBackend` Interface (PR 1)

**Goal:** Create a focused interface for the "execute SQL on external files" capability. Lakehouse code references only this interface, not the composite `AnalyticsSearchBackendPlugin`.

**Files:**
- Create: `sandbox/libs/analytics-framework/src/main/java/org/opensearch/analytics/exec/ExternalQueryBackend.java`
- Modify: `sandbox/libs/analytics-framework/src/main/java/org/opensearch/analytics/spi/AnalyticsSearchBackendPlugin.java:9`
- Modify: `sandbox/libs/analytics-framework/src/main/java/org/opensearch/analytics/exec/RemoteQueryBackendHolder.java:47-57`
- Modify: `sandbox/plugins/lakehouse-iceberg/src/main/java/org/opensearch/lakehouse/distributed/WorkerQueryExecutor.java:15,51,59,64,78,111-122`
- Modify: `sandbox/plugins/lakehouse-iceberg/src/test/java/org/opensearch/lakehouse/distributed/WorkerQueryTransportActionTests.java:12,47-48`
- Modify: `sandbox/plugins/lakehouse-iceberg/src/test/java/org/opensearch/lakehouse/distributed/DistributedScanExecutorTests.java:14,48`

- [ ] **Step 1: Create the `ExternalQueryBackend` interface**

```java
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

/**
 * Executes SQL queries against external data files (e.g., Parquet on S3) using a
 * native query engine. This is the minimal capability interface that external table
 * plugins (like lakehouse-iceberg) depend on.
 * <p>
 * Implementations are provided by query backend plugins (e.g., analytics-backend-datafusion).
 *
 * @opensearch.internal
 */
public interface ExternalQueryBackend {

    /**
     * Executes a query against remote data files using the native engine.
     *
     * @param scanContext the resolved scan context containing SQL, file paths, and storage config
     * @return result rows
     */
    Iterable<Object[]> executeRemoteQuery(ExternalScanContext scanContext);
}
```

- [ ] **Step 2: Make `AnalyticsSearchBackendPlugin` extend `ExternalQueryBackend`**

In `sandbox/libs/analytics-framework/src/main/java/org/opensearch/analytics/spi/AnalyticsSearchBackendPlugin.java`, change:

```java
// OLD
public interface AnalyticsSearchBackendPlugin extends SearchExecEngineProvider {
```
```java
// NEW
import org.opensearch.analytics.exec.ExternalQueryBackend;

public interface AnalyticsSearchBackendPlugin extends SearchExecEngineProvider, ExternalQueryBackend {
```

And remove the `executeRemoteQuery` default method body since `ExternalQueryBackend` now declares it. Keep the `@Override default` in `AnalyticsSearchBackendPlugin` so existing implementations that don't override it still get the `UnsupportedOperationException`:

```java
// Keep existing default (now overrides ExternalQueryBackend)
@Override
default Iterable<Object[]> executeRemoteQuery(ExternalScanContext scanContext) {
    throw new UnsupportedOperationException(name() + " does not support remote query execution");
}
```

- [ ] **Step 3: Update `RemoteQueryBackendHolder` to use `ExternalQueryBackend` type**

In `sandbox/libs/analytics-framework/src/main/java/org/opensearch/analytics/exec/RemoteQueryBackendHolder.java`:

```java
// OLD
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
...
private static volatile AnalyticsSearchBackendPlugin provider;
...
public static void setProvider(AnalyticsSearchBackendPlugin backend) {
public static AnalyticsSearchBackendPlugin getProvider() {
```
```java
// NEW — remove AnalyticsSearchBackendPlugin import, use ExternalQueryBackend
private static volatile ExternalQueryBackend provider;

private RemoteQueryBackendHolder() {}

public static void setProvider(ExternalQueryBackend backend) {
    provider = backend;
}

public static ExternalQueryBackend getProvider() {
    return provider;
}
```

- [ ] **Step 4: Update `WorkerQueryExecutor` to use `ExternalQueryBackend` type**

In `sandbox/plugins/lakehouse-iceberg/src/main/java/org/opensearch/lakehouse/distributed/WorkerQueryExecutor.java`:

Replace all `AnalyticsSearchBackendPlugin` references with `ExternalQueryBackend`:

```java
// OLD imports
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
```
```java
// NEW imports
import org.opensearch.analytics.exec.ExternalQueryBackend;
```

Then in the class body, change every occurrence:

```java
// OLD
private static volatile AnalyticsSearchBackendPlugin backendProvider;
public static void setBackendProvider(AnalyticsSearchBackendPlugin provider) {
static AnalyticsSearchBackendPlugin getBackendProvider() {
AnalyticsSearchBackendPlugin provider = resolveBackend();
static AnalyticsSearchBackendPlugin resolveBackend() {
    AnalyticsSearchBackendPlugin provider = backendProvider;
```
```java
// NEW
private static volatile ExternalQueryBackend backendProvider;
public static void setBackendProvider(ExternalQueryBackend provider) {
static ExternalQueryBackend getBackendProvider() {
ExternalQueryBackend provider = resolveBackend();
static ExternalQueryBackend resolveBackend() {
    ExternalQueryBackend provider = backendProvider;
```

- [ ] **Step 5: Update test files to use `ExternalQueryBackend`**

In `WorkerQueryTransportActionTests.java`:
```java
// OLD
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
...
AnalyticsSearchBackendPlugin mockProvider = mock(AnalyticsSearchBackendPlugin.class);
```
```java
// NEW
import org.opensearch.analytics.exec.ExternalQueryBackend;
...
ExternalQueryBackend mockProvider = mock(ExternalQueryBackend.class);
```

In `DistributedScanExecutorTests.java`:
```java
// OLD
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
...
AnalyticsSearchBackendPlugin mockProvider = mock(AnalyticsSearchBackendPlugin.class);
```
```java
// NEW
import org.opensearch.analytics.exec.ExternalQueryBackend;
...
ExternalQueryBackend mockProvider = mock(ExternalQueryBackend.class);
```

- [ ] **Step 6: Build and test**

Run: `./gradlew -Dsandbox.enabled=true :sandbox:plugins:lakehouse-iceberg:test`
Expected: All 346+ tests pass (pure type rename, no logic change)

Also: `./gradlew -Dsandbox.enabled=true :sandbox:plugins:analytics-engine:test`
Expected: All analytics-engine tests pass (AnalyticsSearchBackendPlugin still used there, unchanged)

- [ ] **Step 7: Commit**

```bash
git add sandbox/libs/analytics-framework/src/main/java/org/opensearch/analytics/exec/ExternalQueryBackend.java \
  sandbox/libs/analytics-framework/src/main/java/org/opensearch/analytics/spi/AnalyticsSearchBackendPlugin.java \
  sandbox/libs/analytics-framework/src/main/java/org/opensearch/analytics/exec/RemoteQueryBackendHolder.java \
  sandbox/plugins/lakehouse-iceberg/src/main/java/org/opensearch/lakehouse/distributed/WorkerQueryExecutor.java \
  sandbox/plugins/lakehouse-iceberg/src/test/java/org/opensearch/lakehouse/distributed/WorkerQueryTransportActionTests.java \
  sandbox/plugins/lakehouse-iceberg/src/test/java/org/opensearch/lakehouse/distributed/DistributedScanExecutorTests.java
git commit -m "refactor: extract ExternalQueryBackend interface from AnalyticsSearchBackendPlugin

Lakehouse code now depends on ExternalQueryBackend (just executeRemoteQuery)
instead of the full AnalyticsSearchBackendPlugin which also carries
SearchExecEngineProvider methods for the composite engine path."
```

---

## Task 2: Eliminate `RemoteQueryBackendHolder` via Guice Binding (PR 2)

**Goal:** Replace the static holder with proper dependency injection. The backend is Guice-bound as a lambda adapter (avoiding Guice introspection of DataFusionPlugin), injected into `WorkerQueryTransportAction`, and passed to `WorkerQueryExecutor` as a method parameter.

**Key insight:** Guice eagerly introspects all methods on the concrete class during binding. `DataFusionPlugin` implements `SearchBackEndPlugin` which references server-internal types (`ReaderManagerConfig`, `DataFormat`) → `ClassNotFoundException`. A lambda adapter `scanContext -> backEnds.get(0).executeRemoteQuery(scanContext)` implements only `ExternalQueryBackend`, so Guice never introspects `DataFusionPlugin`.

**Files:**
- Modify: `sandbox/plugins/analytics-engine/src/main/java/org/opensearch/analytics/AnalyticsPlugin.java:20,105,121-130`
- Modify: `sandbox/plugins/lakehouse-iceberg/src/main/java/org/opensearch/lakehouse/distributed/WorkerQueryTransportAction.java:40-56,60-62`
- Modify: `sandbox/plugins/lakehouse-iceberg/src/main/java/org/opensearch/lakehouse/distributed/WorkerQueryExecutor.java:51-66,77-78,111-123`
- Modify: `sandbox/plugins/lakehouse-iceberg/src/main/java/org/opensearch/lakehouse/distributed/DistributedScanExecutor.java:54-80,148-157,272-282`
- Modify: `sandbox/plugins/lakehouse-iceberg/src/main/java/org/opensearch/lakehouse/LakehouseState.java:47,78-81`
- Delete: `sandbox/libs/analytics-framework/src/main/java/org/opensearch/analytics/exec/RemoteQueryBackendHolder.java`
- Modify: `sandbox/plugins/lakehouse-iceberg/src/test/java/org/opensearch/lakehouse/distributed/WorkerQueryTransportActionTests.java`
- Modify: `sandbox/plugins/lakehouse-iceberg/src/test/java/org/opensearch/lakehouse/distributed/DistributedScanExecutorTests.java`

- [ ] **Step 1: Add Guice binding for `ExternalQueryBackend` in `AnalyticsPlugin`**

In `sandbox/plugins/analytics-engine/src/main/java/org/opensearch/analytics/AnalyticsPlugin.java`:

Remove `RemoteQueryBackendHolder` import and usage:
```java
// REMOVE this import
import org.opensearch.analytics.exec.RemoteQueryBackendHolder;
```

Remove from `createComponents()`:
```java
// REMOVE these lines
if (!backEnds.isEmpty()) {
    RemoteQueryBackendHolder.setProvider(backEnds.get(0));
}
```

Add import and Guice binding in `createGuiceModules()`:
```java
// ADD import
import org.opensearch.analytics.exec.ExternalQueryBackend;
```

```java
// In createGuiceModules(), add binding:
@Override
@SuppressWarnings("unchecked")
public Collection<Module> createGuiceModules() {
    return List.of(b -> {
        b.bind(new TypeLiteral<QueryPlanExecutor<RelNode, Iterable<Object[]>>>() {
        }).to(DefaultPlanExecutor.class);
        b.bind(EngineContext.class).to(DefaultEngineContext.class);
        // Bind ExternalQueryBackend via lambda adapter to avoid Guice introspecting
        // DataFusionPlugin's SearchBackEndPlugin methods (references server-internal types).
        if (!backEnds.isEmpty()) {
            ExternalQueryBackend adapter = scanContext -> backEnds.get(0).executeRemoteQuery(scanContext);
            b.bind(ExternalQueryBackend.class).toInstance(adapter);
        }
    });
}
```

- [ ] **Step 2: Inject `ExternalQueryBackend` in `WorkerQueryTransportAction`**

In `sandbox/plugins/lakehouse-iceberg/src/main/java/org/opensearch/lakehouse/distributed/WorkerQueryTransportAction.java`:

```java
// NEW
import org.opensearch.analytics.exec.ExternalQueryBackend;

public class WorkerQueryTransportAction extends HandledTransportAction<WorkerQueryRequest, WorkerQueryResponse> {

    private static final Logger logger = LogManager.getLogger(WorkerQueryTransportAction.class);

    private final ClusterService clusterService;
    private final ExternalQueryBackend queryBackend;

    @Inject
    public WorkerQueryTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        ExternalQueryBackend queryBackend
    ) {
        super(WorkerQueryAction.NAME, transportService, actionFilters, WorkerQueryRequest::new, ThreadPool.Names.GENERIC);
        this.clusterService = clusterService;
        this.queryBackend = queryBackend;
        LakehouseState.instance().initDistributedExecutor(transportService, clusterService, queryBackend);
    }

    @Override
    protected void doExecute(Task task, WorkerQueryRequest request, ActionListener<WorkerQueryResponse> listener) {
        try {
            WorkerQueryResponse response = WorkerQueryExecutor.execute(request, clusterService, queryBackend);
            listener.onResponse(response);
        } catch (Exception e) {
            logger.error("[WorkerQuery] Execution failed", e);
            listener.onFailure(e);
        }
    }
}
```

- [ ] **Step 3: Update `WorkerQueryExecutor` — remove static field, take backend as param**

In `sandbox/plugins/lakehouse-iceberg/src/main/java/org/opensearch/lakehouse/distributed/WorkerQueryExecutor.java`:

Remove static backend field and resolve method:
```java
// REMOVE these fields/methods:
private static volatile ExternalQueryBackend backendProvider;
public static void setBackendProvider(ExternalQueryBackend provider) { ... }
static ExternalQueryBackend getBackendProvider() { ... }
static ExternalQueryBackend resolveBackend() { ... }

// REMOVE import:
import org.opensearch.analytics.exec.RemoteQueryBackendHolder;
```

Change `execute()` to accept backend as parameter:
```java
/**
 * Executes a worker query request and returns the response.
 *
 * @param request        the worker query request
 * @param clusterService the cluster service for credential resolution
 * @param backend        the query backend for DataFusion execution
 * @return the worker query response
 */
@SuppressWarnings("removal")
public static WorkerQueryResponse execute(WorkerQueryRequest request, ClusterService clusterService, ExternalQueryBackend backend) {
    if (backend == null) {
        throw new IllegalStateException("No analytics backend registered for worker query execution");
    }

    Map<String, String> storageConfig = resolveCredentials(request.getStorageConfig(), clusterService);

    ExternalScanContext scanContext = new ExternalScanContext(
        request.getTableName(),
        request.getFilePaths(),
        request.getFileSizes(),
        request.getSqlQuery(),
        storageConfig
    );

    logger.info(
        "[WorkerQuery] Executing: table={}, files={}, sql={}",
        request.getTableName(),
        request.getFilePaths().size(),
        request.getSqlQuery()
    );

    long t0 = System.currentTimeMillis();
    Iterable<Object[]> rows = AccessController.doPrivileged(
        (PrivilegedAction<Iterable<Object[]>>) () -> backend.executeRemoteQuery(scanContext)
    );
    long t1 = System.currentTimeMillis();

    WorkerQueryResponse response = buildResponse(rows);
    logger.info("[PERF] Worker query: {}ms ({} rows)", t1 - t0, response.getRowCount());
    return response;
}
```

- [ ] **Step 4: Update `DistributedScanExecutor` — take backend, pass to WorkerQueryExecutor**

In `sandbox/plugins/lakehouse-iceberg/src/main/java/org/opensearch/lakehouse/distributed/DistributedScanExecutor.java`:

Add field and update constructors:
```java
import org.opensearch.analytics.exec.ExternalQueryBackend;

public class DistributedScanExecutor {
    // ... existing fields ...
    private final ExternalQueryBackend queryBackend;

    public DistributedScanExecutor(TransportService transportService, ClusterService clusterService, ExternalQueryBackend queryBackend) {
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.nodeDiscovery = new NodeDiscovery(clusterService);
        this.queryBackend = queryBackend;
    }

    DistributedScanExecutor(TransportService transportService, ClusterService clusterService, NodeDiscovery nodeDiscovery, ExternalQueryBackend queryBackend) {
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.nodeDiscovery = nodeDiscovery;
        this.queryBackend = queryBackend;
    }
```

Update `executeSingleNode()`:
```java
private Iterable<Object[]> executeSingleNode(...) {
    WorkerQueryRequest request = new WorkerQueryRequest(sqlQuery, filePaths, fileSizes, storageConfig, tableName);
    WorkerQueryResponse response = WorkerQueryExecutor.execute(request, clusterService, queryBackend);
    return ResultSerializer.toRows(response);
}
```

Update `dispatchLocal()`:
```java
void dispatchLocal(WorkerQueryRequest request, ActionListener<WorkerQueryResponse> listener) {
    logger.debug("[ScanExecutor] Executing locally (direct, no transport): {} files", request.getFilePaths().size());
    transportService.getThreadPool().executor(ThreadPool.Names.GENERIC).execute(() -> {
        try {
            WorkerQueryResponse response = WorkerQueryExecutor.execute(request, clusterService, queryBackend);
            listener.onResponse(response);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    });
}
```

- [ ] **Step 5: Update `LakehouseState.initDistributedExecutor()` to receive backend**

In `sandbox/plugins/lakehouse-iceberg/src/main/java/org/opensearch/lakehouse/LakehouseState.java`:

```java
import org.opensearch.analytics.exec.ExternalQueryBackend;

// Change initDistributedExecutor signature:
public void initDistributedExecutor(TransportService transportService, ClusterService clusterService, ExternalQueryBackend queryBackend) {
    this.distributedScanExecutor = new DistributedScanExecutor(transportService, clusterService, queryBackend);
    logger.info("[LakehouseState] Distributed scan executor initialized");
}
```

- [ ] **Step 6: Delete `RemoteQueryBackendHolder.java`**

Delete: `sandbox/libs/analytics-framework/src/main/java/org/opensearch/analytics/exec/RemoteQueryBackendHolder.java`

- [ ] **Step 7: Update tests — `WorkerQueryTransportActionTests`**

Replace all `WorkerQueryExecutor.setBackendProvider(...)` calls with passing the backend as a method parameter. Tests now pass `ExternalQueryBackend` directly to `WorkerQueryExecutor.execute()`:

In `sandbox/plugins/lakehouse-iceberg/src/test/java/org/opensearch/lakehouse/distributed/WorkerQueryTransportActionTests.java`:

Remove setUp/tearDown that reset static field:
```java
// REMOVE:
@Override
public void setUp() throws Exception {
    super.setUp();
    WorkerQueryExecutor.setBackendProvider(null);
}

@Override
public void tearDown() throws Exception {
    WorkerQueryExecutor.setBackendProvider(null);
    super.tearDown();
}
```

Update `testSetAndGetBackendProvider` — DELETE this test (no longer has static field).

Update `testExecuteReturnsResponse`:
```java
public void testExecuteReturnsResponse() {
    ExternalQueryBackend mockBackend = mock(ExternalQueryBackend.class);
    when(mockBackend.executeRemoteQuery(any(ExternalScanContext.class)))
        .thenReturn(List.of(
            new Object[]{1, "hello"},
            new Object[]{2, "world"}
        ));

    ClusterService clusterService = mock(ClusterService.class);
    Map<String, String> storageConfig = new HashMap<>();
    storageConfig.put("localMode", "true");

    WorkerQueryRequest request = new WorkerQueryRequest(
        "SELECT * FROM t", List.of("/tmp/file1.parquet"), new long[]{1024L}, storageConfig, "test_table"
    );

    WorkerQueryResponse response = WorkerQueryExecutor.execute(request, clusterService, mockBackend);
    assertEquals(2, response.getRowCount());
    verify(mockBackend).executeRemoteQuery(any(ExternalScanContext.class));
}
```

Update `testExecuteWithNoBackendThrows`:
```java
public void testExecuteWithNoBackendThrows() {
    ClusterService clusterService = mock(ClusterService.class);
    WorkerQueryRequest request = new WorkerQueryRequest(
        "SELECT * FROM t", List.of("/tmp/file1.parquet"), new long[]{1024L},
        Map.of("localMode", "true"), "test_table"
    );
    expectThrows(IllegalStateException.class, () ->
        WorkerQueryExecutor.execute(request, clusterService, null)
    );
}
```

Similar updates for `testExecuteWithDefaultAuthSkipsCredentials` — pass mock backend as third param.

- [ ] **Step 8: Update tests — `DistributedScanExecutorTests`**

Update constructor calls and mock setup to pass `ExternalQueryBackend`:

```java
// In setupMockBackend — just return the mock, don't set static:
private ExternalQueryBackend setupMockBackend(Object[]... rows) {
    ExternalQueryBackend mockBackend = mock(ExternalQueryBackend.class);
    when(mockBackend.executeRemoteQuery(any(ExternalScanContext.class)))
        .thenReturn(List.of(rows));
    return mockBackend;
}
```

Update tests to pass backend to DistributedScanExecutor constructor:
```java
public void testSingleNodeFallbackExecutesLocally() {
    ExternalQueryBackend mockBackend = setupMockBackend(new Object[]{1, "hello"}, new Object[]{2, "world"});
    // ... node setup ...
    DistributedScanExecutor executor = new DistributedScanExecutor(transportService, clusterService, mockBackend);
    // ... rest of test ...
}
```

Remove setUp/tearDown that reset `WorkerQueryExecutor.setBackendProvider(null)`.

- [ ] **Step 9: Build and test**

Run: `./gradlew -Dsandbox.enabled=true :sandbox:plugins:lakehouse-iceberg:test`
Expected: All tests pass

Run: `./gradlew -Dsandbox.enabled=true :sandbox:plugins:analytics-engine:test`
Expected: All tests pass

Run: `./gradlew -Dsandbox.enabled=true :sandbox:plugins:lakehouse-iceberg:compileJava`
Expected: Compile succeeds (no references to deleted RemoteQueryBackendHolder)

- [ ] **Step 10: Commit**

```bash
git add -A
git commit -m "refactor: eliminate RemoteQueryBackendHolder via Guice-bound ExternalQueryBackend

Replace static holder with proper dependency injection. AnalyticsPlugin
binds ExternalQueryBackend via a lambda adapter, avoiding Guice introspection
of DataFusionPlugin (which implements SearchBackEndPlugin with server-internal
types). WorkerQueryTransportAction injects the backend and passes it through
the execution chain."
```

---

## Task 3: Bypass DefaultPlanExecutor — Lakehouse Owns Execution (PR 3)

**Goal:** `LakehouseQueryTransportAction` parses SQL/PPL via Calcite, then calls lakehouse execution directly. No more `UnifiedQueryService → PushDownPlanner → DefaultPlanExecutor → ExternalTableExecutor.prepareScan()` round-trip. Remove `ExternalTableExecutor`, `preComputedResults`, and the SPI callback.

**Files:**
- Create: `sandbox/plugins/lakehouse-iceberg/src/main/java/org/opensearch/lakehouse/exec/LakehouseQueryExecutor.java`
- Create: `sandbox/plugins/lakehouse-iceberg/src/test/java/org/opensearch/lakehouse/exec/LakehouseQueryExecutorTests.java`
- Modify: `sandbox/plugins/lakehouse-iceberg/src/main/java/org/opensearch/lakehouse/action/LakehouseQueryTransportAction.java`
- Modify: `sandbox/plugins/lakehouse-iceberg/src/main/java/org/opensearch/lakehouse/LakehousePlugin.java:76,100-107,115-202`
- Delete: `sandbox/plugins/lakehouse-iceberg/src/main/resources/META-INF/services/org.opensearch.analytics.exec.ExternalTableExecutor`
- Modify: `sandbox/libs/analytics-framework/src/main/java/org/opensearch/analytics/exec/ExternalScanContext.java:24,35-63,98-100`
- Modify: `sandbox/plugins/analytics-engine/src/main/java/org/opensearch/analytics/AnalyticsPlugin.java:18,70,78,103`
- Modify: `sandbox/plugins/analytics-engine/src/main/java/org/opensearch/analytics/exec/DefaultPlanExecutor.java:48,59-70,76-107`
- Modify: `sandbox/plugins/analytics-engine/src/test/java/org/opensearch/analytics/exec/DefaultPlanExecutorTests.java:189-255`
- Delete: `sandbox/plugins/lakehouse-iceberg/src/test/java/org/opensearch/lakehouse/distributed/ExternalScanContextDistributedTests.java`

- [ ] **Step 1: Create `LakehouseQueryExecutor`**

This class owns the full lakehouse query lifecycle: parse → Iceberg scan → distribute → DataFusion → results.

```java
/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.lakehouse.exec;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.iceberg.expressions.Expression;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.EngineContext;
import org.opensearch.analytics.exec.ExternalQueryBackend;
import org.opensearch.analytics.exec.ExternalScanContext;
import org.opensearch.lakehouse.LakehouseState;
import org.opensearch.lakehouse.catalog.CatalogConfig;
import org.opensearch.lakehouse.catalog.IcebergCatalogConnector;
import org.opensearch.lakehouse.distributed.DistributedScanExecutor;
import org.opensearch.lakehouse.scan.CalciteToIcebergPredicateConverter;
import org.opensearch.lakehouse.scan.IcebergScanPlan;
import org.opensearch.lakehouse.schema.IcebergCalciteTable;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.UnifiedQueryPlanner;
import org.opensearch.sql.executor.QueryType;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Executes SQL and PPL queries against Iceberg tables.
 * <p>
 * Owns the full lakehouse query lifecycle:
 * <ol>
 *   <li>Parse SQL/PPL → Calcite RelNode (via {@link UnifiedQueryPlanner})</li>
 *   <li>Iceberg scan planning (manifest pruning → data file paths)</li>
 *   <li>Convert RelNode to DataFusion SQL</li>
 *   <li>Execute via {@link DistributedScanExecutor} or single-node via {@link ExternalQueryBackend}</li>
 * </ol>
 *
 * @opensearch.internal
 */
public class LakehouseQueryExecutor {

    private static final Logger logger = LogManager.getLogger(LakehouseQueryExecutor.class);
    private static final String DEFAULT_CATALOG = "opensearch";

    private final EngineContext engineContext;
    private final ExternalQueryBackend queryBackend;

    public LakehouseQueryExecutor(EngineContext engineContext, ExternalQueryBackend queryBackend) {
        this.engineContext = engineContext;
        this.queryBackend = queryBackend;
    }

    /**
     * Executes a SQL query and returns the response.
     */
    public PPLResponse executeSql(String sql) {
        return executeInternal(sql, QueryType.SQL);
    }

    /**
     * Executes a PPL query and returns the response.
     */
    public PPLResponse executePpl(String ppl) {
        return executeInternal(ppl, QueryType.PPL);
    }

    private PPLResponse executeInternal(String queryText, QueryType queryType) {
        long t0 = System.currentTimeMillis();
        SchemaPlus schema = engineContext.getSchema();

        UnifiedQueryContext context = UnifiedQueryContext.builder()
            .language(queryType)
            .catalog(DEFAULT_CATALOG, schema)
            .defaultNamespace(DEFAULT_CATALOG)
            .build();

        try {
            // 1. Parse query → Calcite RelNode
            UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);
            RelNode logicalPlan = planner.plan(queryText);
            long t1 = System.currentTimeMillis();
            logger.info("[PERF] Parse+plan: {}ms", t1 - t0);

            // 2. Extract column names from plan
            List<String> columns = logicalPlan.getRowType().getFieldNames();

            // 3. Execute lakehouse-specific pipeline
            Iterable<Object[]> result = executeLakehouse(logicalPlan);

            // 4. Build response
            List<Object[]> rows = new ArrayList<>();
            for (Object[] row : result) {
                rows.add(row);
            }
            logger.info("[PERF] Total query: {}ms, {} rows", System.currentTimeMillis() - t0, rows.size());
            return new PPLResponse(columns, rows);
        } catch (Exception e) {
            if (e instanceof RuntimeException) throw (RuntimeException) e;
            throw new RuntimeException("Failed to execute " + queryType + " query: " + e.getMessage(), e);
        } finally {
            try { context.close(); } catch (Exception ignored) {}
        }
    }

    /**
     * Executes the Iceberg scan pipeline: predicate pushdown → file pruning →
     * DataFusion SQL generation → distributed/single-node execution.
     */
    @SuppressWarnings("removal")
    Iterable<Object[]> executeLakehouse(RelNode logicalPlan) {
        // Find the IcebergCalciteTable in the plan
        IcebergCalciteTable icebergTable = extractIcebergTable(logicalPlan);
        if (icebergTable == null) {
            throw new IllegalArgumentException("No Iceberg table found in query plan");
        }

        IcebergCatalogConnector connector = LakehouseState.instance().catalogConnector();

        // 1. Extract Iceberg predicates for manifest-level file pruning
        Expression filterExpr = extractIcebergFilter(logicalPlan);
        List<Expression> predicates = filterExpr != null ? List.of(filterExpr) : List.of();

        // 2. Plan scan — resolves manifests to pruned data file paths
        CatalogConfig catalogConfig = icebergTable.catalogConfig();
        if (catalogConfig != null) connector.setCredentialsOnThread(catalogConfig);

        long t1 = System.currentTimeMillis();
        IcebergScanPlan scanPlan;
        try {
            scanPlan = AccessController.doPrivileged(
                (PrivilegedAction<IcebergScanPlan>) () -> LakehouseState.instance()
                    .scanPlanner()
                    .planScan(icebergTable.icebergTable(), icebergTable.snapshotId(), predicates, null)
            );
        } finally {
            if (catalogConfig != null) connector.clearCredentialsOnThread();
        }
        long t2 = System.currentTimeMillis();
        logger.info("[PERF] Iceberg scan planning: {}ms ({} files, {} bytes)", t2 - t1, scanPlan.fileCount(), scanPlan.getTotalFileSize());

        // 3. Convert Calcite RelNode to DataFusion SQL
        String tableName = extractTableName(logicalPlan);
        String sqlQuery = convertToDataFusionSql(logicalPlan, tableName);

        // 4. Build storage config
        Map<String, String> storageConfig = buildStorageConfig(connector, icebergTable, scanPlan);

        // 5. Normalize file paths
        long[] fileSizes = scanPlan.getFiles().stream().mapToLong(IcebergScanPlan.FileInfo::getFileSizeInBytes).toArray();
        List<String> filePaths = normalizeFilePaths(scanPlan.getDataFilePaths());

        // 6. Execute via distributed or single-node
        DistributedScanExecutor scanExecutor = LakehouseState.instance().distributedScanExecutor();
        if (scanExecutor != null) {
            return scanExecutor.execute(logicalPlan, sqlQuery, filePaths, fileSizes, storageConfig, tableName);
        }

        // Fallback: single-node via backend directly
        ExternalScanContext scanContext = new ExternalScanContext(tableName, filePaths, fileSizes, sqlQuery, storageConfig);
        return queryBackend.executeRemoteQuery(scanContext);
    }

    // --- Helper methods (moved from LakehousePlugin) ---

    private IcebergCalciteTable extractIcebergTable(RelNode node) {
        if (node instanceof TableScan) {
            org.apache.calcite.schema.Table table = node.getTable().unwrap(org.apache.calcite.schema.Table.class);
            if (table instanceof IcebergCalciteTable) return (IcebergCalciteTable) table;
        }
        for (RelNode input : node.getInputs()) {
            IcebergCalciteTable found = extractIcebergTable(input);
            if (found != null) return found;
        }
        return null;
    }

    private Expression extractIcebergFilter(RelNode node) {
        if (node instanceof Filter) {
            Filter filter = (Filter) node;
            if (filter.getInput() instanceof TableScan) {
                RelDataType inputRowType = filter.getInput().getRowType();
                return CalciteToIcebergPredicateConverter.convert(filter.getCondition(), inputRowType);
            }
        }
        for (RelNode input : node.getInputs()) {
            Expression result = extractIcebergFilter(input);
            if (result != null) return result;
        }
        return null;
    }

    private String extractTableName(RelNode node) {
        if (node instanceof TableScan) {
            List<String> qn = node.getTable().getQualifiedName();
            return qn.get(qn.size() - 1);
        }
        for (RelNode input : node.getInputs()) {
            String name = extractTableName(input);
            if (name != null) return name;
        }
        throw new IllegalArgumentException("No TableScan found in plan");
    }

    private String convertToDataFusionSql(RelNode logicalPlan, String tableName) {
        try {
            SqlDialect dialect = DataFusionSqlDialect.DEFAULT;
            RelToSqlConverter converter = new RelToSqlConverter(dialect);
            SqlNode sqlNode = converter.visitRoot(logicalPlan).asStatement();
            String sql = sqlNode.toSqlString(dialect).getSql();
            return stripSchemaQualifiers(sql, tableName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert query plan to SQL", e);
        }
    }

    private String stripSchemaQualifiers(String sql, String tableName) {
        String quotedTable = "\"" + tableName + "\"";
        return sql.replaceAll("\"\\w+\"\\." + java.util.regex.Pattern.quote(quotedTable), quotedTable);
    }

    private Map<String, String> buildStorageConfig(
        IcebergCatalogConnector connector, IcebergCalciteTable icebergTable, IcebergScanPlan scanPlan
    ) {
        Map<String, String> config = new HashMap<>();
        CatalogConfig catalogConfig = icebergTable.catalogConfig();
        if (catalogConfig != null && catalogConfig.region() != null) config.put("s3Region", catalogConfig.region());
        List<String> paths = scanPlan.getDataFilePaths();
        if (!paths.isEmpty()) {
            String firstPath = paths.get(0);
            if (firstPath.startsWith("s3://")) {
                String withoutScheme = firstPath.substring(5);
                int slashIdx = withoutScheme.indexOf('/');
                if (slashIdx > 0) config.put("s3Bucket", withoutScheme.substring(0, slashIdx));
            }
            if (firstPath.startsWith("file:") || firstPath.startsWith("/")) config.put("localMode", "true");
        }
        if (catalogConfig != null) {
            config.put("indexName", catalogConfig.indexName());
            config.put("authType", catalogConfig.authType());
        }
        return config;
    }

    private List<String> normalizeFilePaths(List<String> paths) {
        return paths.stream()
            .map(p -> {
                if (p.startsWith("file:/") && !p.startsWith("file://")) return "file://" + p.substring("file:".length());
                else if (p.startsWith("/")) return "file://" + p;
                return p;
            })
            .toList();
    }
}
```

Note: `DataFusionSqlDialect` import would be `org.opensearch.lakehouse.exec.DataFusionSqlDialect` (already exists in the exec package).

- [ ] **Step 2: Update `LakehouseQueryTransportAction` to use `LakehouseQueryExecutor`**

```java
package org.opensearch.lakehouse.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.analytics.EngineContext;
import org.opensearch.analytics.exec.ExternalQueryBackend;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.lakehouse.exec.LakehouseQueryExecutor;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

public class LakehouseQueryTransportAction extends HandledTransportAction<LakehouseQueryRequest, PPLResponse> {

    private static final Logger logger = LogManager.getLogger(LakehouseQueryTransportAction.class);
    private final LakehouseQueryExecutor queryExecutor;

    @Inject
    public LakehouseQueryTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        EngineContext engineContext,
        ExternalQueryBackend queryBackend
    ) {
        super(LakehouseQueryAction.NAME, transportService, actionFilters, LakehouseQueryRequest::new, ThreadPool.Names.GENERIC);
        this.queryExecutor = new LakehouseQueryExecutor(engineContext, queryBackend);
    }

    @Override
    protected void doExecute(Task task, LakehouseQueryRequest request, ActionListener<PPLResponse> listener) {
        try {
            PPLResponse response;
            if (request.isSql()) {
                logger.info("[Lakehouse] Executing SQL: {}", request.getQueryText());
                response = queryExecutor.executeSql(request.getQueryText());
            } else {
                logger.info("[Lakehouse] Executing PPL: {}", request.getQueryText());
                response = queryExecutor.executePpl(request.getQueryText());
            }
            listener.onResponse(response);
        } catch (Exception e) {
            logger.error("[Lakehouse] Query execution failed", e);
            listener.onFailure(e);
        }
    }
}
```

- [ ] **Step 3: Remove `ExternalTableExecutor` from `LakehousePlugin`**

In `sandbox/plugins/lakehouse-iceberg/src/main/java/org/opensearch/lakehouse/LakehousePlugin.java`:

Remove `ExternalTableExecutor` from implements clause:
```java
// OLD
public class LakehousePlugin extends Plugin implements SchemaContributor, ExternalTableExecutor, ActionPlugin, Closeable {
// NEW
public class LakehousePlugin extends Plugin implements SchemaContributor, ActionPlugin, Closeable {
```

Remove these methods entirely:
- `supports(ExternalTable)` (lines 105-107)
- `prepareScan(RelNode, ExternalTable)` (lines 115-202)
- `extractIcebergFilter(RelNode)` (lines 204-217)
- `stripSchemaQualifiers(String, String)` (lines 224-228)
- `extractTableName(RelNode)` (lines 230-239)
- `buildStorageConfig(...)` (lines 242-273)

Remove unused imports for these methods.

- [ ] **Step 4: Delete ExternalTableExecutor SPI service file**

Delete: `sandbox/plugins/lakehouse-iceberg/src/main/resources/META-INF/services/org.opensearch.analytics.exec.ExternalTableExecutor`

- [ ] **Step 5: Remove `preComputedResults` from `ExternalScanContext`**

In `sandbox/libs/analytics-framework/src/main/java/org/opensearch/analytics/exec/ExternalScanContext.java`:

Remove the `preComputedResults` field, 6-arg constructor, and `getPreComputedResults()` method. Keep only the 5-arg constructor:

```java
public class ExternalScanContext {
    private final String tableName;
    private final List<String> dataFilePaths;
    private final long[] fileSizes;
    private final String sqlQuery;
    private final Map<String, String> storageConfig;

    public ExternalScanContext(String tableName, List<String> dataFilePaths, long[] fileSizes, String sqlQuery, Map<String, String> storageConfig) {
        this.tableName = tableName;
        this.dataFilePaths = dataFilePaths;
        this.fileSizes = fileSizes;
        this.sqlQuery = sqlQuery;
        this.storageConfig = storageConfig;
    }

    // Keep all existing getters except getPreComputedResults()
    // ... getTableName(), getDataFilePaths(), getFileSizes(), getSqlQuery(), getStorageConfig() ...
}
```

- [ ] **Step 6: Clean up `AnalyticsPlugin` — remove ExternalTableExecutor loading**

In `sandbox/plugins/analytics-engine/src/main/java/org/opensearch/analytics/AnalyticsPlugin.java`:

```java
// REMOVE:
import org.opensearch.analytics.exec.ExternalTableExecutor;
private final List<ExternalTableExecutor> externalTableExecutors = new ArrayList<>();
externalTableExecutors.addAll(loader.loadExtensions(ExternalTableExecutor.class));
ExternalTableExecutor externalExecutor = externalTableExecutors.isEmpty() ? null : externalTableExecutors.get(0);

// Change DefaultPlanExecutor constructor:
// OLD
new DefaultPlanExecutor(backEnds, null, clusterService, externalExecutor),
// NEW
new DefaultPlanExecutor(backEnds, null, clusterService),
```

- [ ] **Step 7: Clean up `DefaultPlanExecutor` — remove external table routing**

In `sandbox/plugins/analytics-engine/src/main/java/org/opensearch/analytics/exec/DefaultPlanExecutor.java`:

Remove `ExternalTableExecutor` field and constructor parameter. Remove the external table check in `execute()`:

```java
public class DefaultPlanExecutor implements QueryPlanExecutor<RelNode, Iterable<Object[]>> {
    private final Map<String, AnalyticsSearchBackendPlugin> backEnds;
    private volatile IndicesService indicesService;
    private final ClusterService clusterService;

    public DefaultPlanExecutor(
        List<AnalyticsSearchBackendPlugin> providers,
        IndicesService indicesService,
        ClusterService clusterService
    ) {
        this.backEnds = new LinkedHashMap<>();
        for (AnalyticsSearchBackendPlugin provider : providers) {
            this.backEnds.put(provider.name(), provider);
        }
        this.indicesService = indicesService;
        this.clusterService = clusterService;
    }

    @Override
    public Iterable<Object[]> execute(RelNode logicalFragment, Object context) {
        // No more external table routing — lakehouse handles its own execution
        String tableName = extractTableName(logicalFragment);
        AnalyticsSearchBackendPlugin provider = selectBackEnd();
        // ... rest of shard-level execution unchanged ...
    }
```

Remove `extractExternalTable()` method. Remove unused imports for `ExternalTableExecutor`, `ExternalScanContext`, `ExternalTable`.

- [ ] **Step 8: Delete `ExternalTableExecutor` interface**

Delete: `sandbox/libs/analytics-framework/src/main/java/org/opensearch/analytics/exec/ExternalTableExecutor.java`

Note: Only delete if no other plugins implement it. Currently only LakehousePlugin does.

- [ ] **Step 9: Update tests**

Delete `ExternalScanContextDistributedTests.java` (tests preComputedResults which no longer exists).

Update `DefaultPlanExecutorTests.java`:
- Remove `testExecuteThrowsForExternalTableWithNoExecutor` test
- Remove `testExecuteRoutesExternalTableToBackend` test
- Remove `MockExternalCalciteTable` interface
- Update `testExtractExternalTableReturnsNullForRegularTable` → DELETE (method removed)
- Update `testExtractExternalTableFindsExternalTable` → DELETE (method removed)
- Update constructor calls: `new DefaultPlanExecutor(List.of(backendPlugin), indicesService, clusterService)` (no 4th param)

- [ ] **Step 10: Build and test**

Run: `./gradlew -Dsandbox.enabled=true :sandbox:plugins:lakehouse-iceberg:test`
Expected: All tests pass

Run: `./gradlew -Dsandbox.enabled=true :sandbox:plugins:analytics-engine:test`
Expected: All tests pass

Run: `./gradlew -Dsandbox.enabled=true :sandbox:plugins:lakehouse-iceberg:compileJava`
Expected: No references to ExternalTableExecutor or preComputedResults

- [ ] **Step 11: Commit**

```bash
git add -A
git commit -m "refactor: lakehouse owns execution lifecycle, remove ExternalTableExecutor callback

LakehouseQueryTransportAction now parses SQL/PPL via Calcite and calls
LakehouseQueryExecutor directly, bypassing DefaultPlanExecutor and
PushDownPlanner. Remove ExternalTableExecutor interface, preComputedResults
hack, and the ExternalTableExecutor SPI service file."
```

---

## Verification

After all 3 PRs:

```bash
# Full build
./gradlew -Dsandbox.enabled=true :sandbox:plugins:lakehouse-iceberg:test
./gradlew -Dsandbox.enabled=true :sandbox:plugins:analytics-engine:test

# Verify no dangling references
grep -r "RemoteQueryBackendHolder" sandbox/ --include="*.java"   # should be empty
grep -r "ExternalTableExecutor" sandbox/ --include="*.java"      # should be empty
grep -r "preComputedResults" sandbox/ --include="*.java"          # should be empty

# Compile all sandbox plugins
./gradlew -Dsandbox.enabled=true assemble
```

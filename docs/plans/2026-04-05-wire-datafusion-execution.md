# Wire DataFusion Execution for Iceberg Queries — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the ad-hoc Java in-memory query execution in `LakehousePlugin` with the DataFusion native engine via JNI, reusing the existing Rust `iceberg_executor.rs` and `s3_store.rs` code.

**Architecture:** `LakehousePlugin.prepareScan()` resolves Iceberg metadata → pruned S3 file paths + Substrait bytes. `DefaultPlanExecutor` routes to `DataFusionPlugin.executeRemoteQuery()` which calls `NativeBridge.executeIcebergQueryAsync()` → Rust DataFusion executes the Substrait plan against S3 Parquet → Arrow batch stream → `Object[]` rows. This is additive-first: new methods are added with defaults, then old code is removed.

**Tech Stack:** Java 21, Apache Calcite, Apache Iceberg SDK, Apache DataFusion (Rust), Substrait (protobuf), Arrow C Data Interface, JNI

**Design alignment check (run after EVERY task):**
1. Does `LakehousePlugin` only handle Iceberg metadata (catalog, manifests, file resolution)?
2. Does `DefaultPlanExecutor` route ALL queries (OS index AND Iceberg) through `AnalyticsSearchBackendPlugin`?
3. Does `DataFusionPlugin` handle ALL native execution (JNI, Arrow, memory)?
4. Is `CalciteSubstraitConverter` the bridge between Calcite and DataFusion (no raw RelNode interpretation)?
5. Does compilation pass for all sandbox plugins?

---

## File Structure

### New files
| File | Responsibility |
|------|---------------|
| `sandbox/libs/analytics-framework/src/main/java/org/opensearch/analytics/exec/ExternalScanContext.java` | Data class carrying pruned file paths, Substrait bytes, and storage config from lakehouse plugin to DataFusion backend |

### Modified files
| File | Change |
|------|--------|
| `sandbox/libs/analytics-framework/.../exec/ExternalTableExecutor.java` | Add `prepareScan()` default method alongside existing `execute()` |
| `sandbox/libs/analytics-framework/.../spi/AnalyticsSearchBackendPlugin.java` | Add `executeRemoteQuery()` default method |
| `sandbox/plugins/analytics-engine/.../exec/DefaultPlanExecutor.java` | Route external tables through `prepareScan()` → `executeRemoteQuery()` with fallback |
| `sandbox/plugins/analytics-backend-datafusion/.../jni/NativeBridge.java` | Add `executeIcebergQueryAsync` native method declaration (Rust side already exists) |
| `sandbox/plugins/analytics-backend-datafusion/.../DataFusionPlugin.java` | Override `executeRemoteQuery()` — call JNI, stream Arrow batches, return `Object[]` rows |
| `sandbox/plugins/lakehouse-iceberg/.../LakehousePlugin.java` | Override `prepareScan()` using `IcebergScanPlanner` + `CalciteSubstraitConverter`. Remove `execute()`, `applyAggregate()`, `computeAggregation()`, `minMax()`, `extractProjectedColumns()`, `extractFilterExpression()`, `findScanRowType()` |
| `sandbox/plugins/lakehouse-iceberg/.../schema/IcebergCalciteTable.java` | Add `catalogConfig` field so `prepareScan()` can extract S3 region/credentials |
| `sandbox/plugins/lakehouse-iceberg/.../schema/IcebergSchemaEnricher.java` | Pass `CatalogConfig` when constructing `IcebergCalciteTable` |

### Files to verify (no changes expected)
| File | Why verify |
|------|-----------|
| `sandbox/plugins/analytics-backend-datafusion/jni/src/lib.rs` | Confirm `executeIcebergQueryAsync` JNI binding exists (lines 243-367) |
| `sandbox/plugins/analytics-backend-datafusion/jni/src/api.rs` | Confirm `execute_iceberg_query` API exists (lines 344-384) |
| `sandbox/plugins/analytics-backend-datafusion/jni/src/iceberg_executor.rs` | Confirm S3 ListingTable + Substrait execution works |
| `sandbox/plugins/analytics-backend-datafusion/jni/src/s3_store.rs` | Confirm S3 object store registration works |

### Files to remove content from (cleanup)
| File | What to remove |
|------|---------------|
| `sandbox/plugins/lakehouse-iceberg/.../LakehousePlugin.java` | ~150 lines: `applyAggregate()`, `computeAggregation()`, `minMax()`, `extractProjectedColumns()`, `extractFilterExpression()`, `findScanRowType()`, in-memory record iteration |
| `sandbox/plugins/lakehouse-iceberg/.../scan/CalciteToIcebergPredicateConverter.java` | Keep — still needed for Iceberg manifest-level predicate pushdown |

---

### Task 1: Add ExternalScanContext and new interface methods (additive only)

**Files:**
- Create: `sandbox/libs/analytics-framework/src/main/java/org/opensearch/analytics/exec/ExternalScanContext.java`
- Modify: `sandbox/libs/analytics-framework/src/main/java/org/opensearch/analytics/exec/ExternalTableExecutor.java`
- Modify: `sandbox/libs/analytics-framework/src/main/java/org/opensearch/analytics/spi/AnalyticsSearchBackendPlugin.java`

This task is purely additive — existing code continues to work unchanged.

- [ ] **Step 1: Create ExternalScanContext**

```java
// sandbox/libs/analytics-framework/src/main/java/org/opensearch/analytics/exec/ExternalScanContext.java
package org.opensearch.analytics.exec;

import java.util.List;
import java.util.Map;

/**
 * Carries the resolved scan context from an external table plugin (e.g., Iceberg)
 * to the native execution backend (e.g., DataFusion).
 */
public class ExternalScanContext {
    private final String tableName;
    private final List<String> dataFilePaths;
    private final byte[] substraitPlan;
    private final Map<String, String> storageConfig;

    public ExternalScanContext(
        String tableName,
        List<String> dataFilePaths,
        byte[] substraitPlan,
        Map<String, String> storageConfig
    ) {
        this.tableName = tableName;
        this.dataFilePaths = dataFilePaths;
        this.substraitPlan = substraitPlan;
        this.storageConfig = storageConfig;
    }

    /** Table name as registered in the Calcite schema (must match Substrait plan references). */
    public String getTableName() { return tableName; }

    /** Pruned list of data file paths (e.g., s3://bucket/data/file.parquet). */
    public List<String> getDataFilePaths() { return dataFilePaths; }

    /** Serialized Substrait plan bytes (Calcite RelNode converted to protobuf). */
    public byte[] getSubstraitPlan() { return substraitPlan; }

    /**
     * Storage configuration for the external data source.
     * Keys: s3Region, s3Bucket, s3AccessKeyId (optional), s3SecretAccessKey (optional),
     *        s3SessionToken (optional), s3Endpoint (optional).
     */
    public Map<String, String> getStorageConfig() { return storageConfig; }
}
```

- [ ] **Step 2: Add prepareScan default method to ExternalTableExecutor**

Add the following default method BELOW the existing `execute` method in `ExternalTableExecutor.java`:

```java
// Add after the execute() method (line 34)

    /**
     * Prepares a scan context for an external table query.
     * Returns file paths, Substrait plan bytes, and storage config
     * so the analytics backend can execute via its native engine.
     *
     * @param logicalPlan   the optimized Calcite plan
     * @param externalTable the external table found in the plan
     * @return scan context for native execution, or null if not supported
     */
    default ExternalScanContext prepareScan(RelNode logicalPlan, ExternalTable externalTable) {
        return null;
    }
```

Also add import: `import org.opensearch.analytics.exec.ExternalScanContext;` (same package, but explicit for clarity). Actually this is same package so no import needed.

- [ ] **Step 3: Add executeRemoteQuery default method to AnalyticsSearchBackendPlugin**

```java
// sandbox/libs/analytics-framework/src/main/java/org/opensearch/analytics/spi/AnalyticsSearchBackendPlugin.java
package org.opensearch.analytics.spi;

import org.opensearch.analytics.exec.ExternalScanContext;

/**
 * SPI extension point for back-end query engines.
 */
public interface AnalyticsSearchBackendPlugin extends SearchExecEngineProvider {

    /**
     * Executes a query against remote data files using the native engine.
     * The scan context contains Substrait plan bytes, file paths, and storage config.
     *
     * @param scanContext the resolved scan context from an external table plugin
     * @return result rows
     * @throws UnsupportedOperationException if this backend does not support remote execution
     */
    default Iterable<Object[]> executeRemoteQuery(ExternalScanContext scanContext) {
        throw new UnsupportedOperationException(name() + " does not support remote query execution");
    }
}
```

- [ ] **Step 4: Compile check**

Run: `./gradlew :sandbox:libs:analytics-framework:compileJava :sandbox:plugins:analytics-engine:compileJava :sandbox:plugins:lakehouse-iceberg:compileJava`
Expected: SUCCESS (all changes are additive, nothing breaks)

- [ ] **Step 5: Design alignment check**

Verify: new interfaces are in the shared `analytics-framework` lib, accessible by all plugins. No plugin-specific types leaked into shared interfaces. `ExternalScanContext` uses only primitive types + standard collections.

- [ ] **Step 6: Commit**

```bash
git add sandbox/libs/analytics-framework/src/main/java/org/opensearch/analytics/exec/ExternalScanContext.java \
       sandbox/libs/analytics-framework/src/main/java/org/opensearch/analytics/exec/ExternalTableExecutor.java \
       sandbox/libs/analytics-framework/src/main/java/org/opensearch/analytics/spi/AnalyticsSearchBackendPlugin.java
git commit -m "feat(analytics): add ExternalScanContext and remote execution interfaces

Add prepareScan() to ExternalTableExecutor and executeRemoteQuery() to
AnalyticsSearchBackendPlugin as default methods. This enables external
table plugins to provide scan context (file paths, Substrait plan, S3
config) while the backend engine handles native execution.

Signed-off-by: Vamsi Manohar <reddyvam@amazon.com>"
```

---

### Task 2: Update DefaultPlanExecutor to route external queries via backend

**Files:**
- Modify: `sandbox/plugins/analytics-engine/src/main/java/org/opensearch/analytics/exec/DefaultPlanExecutor.java`

- [ ] **Step 1: Modify the external table branch in execute()**

Replace lines 87-95 of `DefaultPlanExecutor.java` with:

```java
    @Override
    public Iterable<Object[]> execute(RelNode logicalFragment, Object context) {
        // Route external (non-OpenSearch) tables through the native backend
        ExternalTable externalTable = extractExternalTable(logicalFragment);
        if (externalTable != null) {
            if (externalTableExecutor == null) {
                throw new IllegalStateException("Query references an external table but no ExternalTableExecutor is registered");
            }

            // Try the new prepareScan → executeRemoteQuery path (DataFusion native execution)
            ExternalScanContext scanContext = externalTableExecutor.prepareScan(logicalFragment, externalTable);
            if (scanContext != null) {
                AnalyticsSearchBackendPlugin provider = selectBackEnd();
                if (provider == null) {
                    throw new IllegalStateException("No analytics backend registered for remote query execution");
                }
                logger.info("[DefaultPlanExecutor] Routing external table to native backend [{}]", provider.name());
                return provider.executeRemoteQuery(scanContext);
            }

            // Fallback to legacy in-process execution (will be removed once DataFusion path is wired)
            logger.info("[DefaultPlanExecutor] Falling back to legacy external table executor");
            return externalTableExecutor.execute(logicalFragment, externalTable);
        }

        // ... rest of the method unchanged (OS index path)
```

Also add import at the top:
```java
import org.opensearch.analytics.exec.ExternalScanContext;
```

- [ ] **Step 2: Compile check**

Run: `./gradlew :sandbox:plugins:analytics-engine:compileJava`
Expected: SUCCESS (fallback to old `execute()` preserves current behavior)

- [ ] **Step 3: Design alignment check**

Verify: `DefaultPlanExecutor` tries `prepareScan` first (native DataFusion path), falls back to `execute` (Java in-memory). Once Task 4+5 are done, the fallback is never hit. The backend provider is selected the same way for both OS index and Iceberg queries.

- [ ] **Step 4: Commit**

```bash
git add sandbox/plugins/analytics-engine/src/main/java/org/opensearch/analytics/exec/DefaultPlanExecutor.java
git commit -m "feat(analytics): route external tables through native backend

DefaultPlanExecutor now tries prepareScan() + executeRemoteQuery() for
external tables before falling back to the legacy execute() path. This
enables Iceberg queries to run through DataFusion instead of Java
in-memory execution.

Signed-off-by: Vamsi Manohar <reddyvam@amazon.com>"
```

---

### Task 3: Add executeIcebergQueryAsync native method declaration

**Files:**
- Modify: `sandbox/plugins/analytics-backend-datafusion/src/main/java/org/opensearch/be/datafusion/jni/NativeBridge.java`

The Rust JNI binding already exists in `lib.rs:243-367`. We need the Java side.

- [ ] **Step 1: Verify Rust JNI binding exists**

Run: `grep -n "executeIcebergQueryAsync" sandbox/plugins/analytics-backend-datafusion/jni/src/lib.rs`
Expected: Match at line ~246 showing `Java_org_opensearch_be_datafusion_jni_NativeBridge_executeIcebergQueryAsync`

- [ ] **Step 2: Add native method to NativeBridge.java**

Add after the `executeQueryAsync` method (after line 126):

```java
    // ---- Iceberg / S3 query execution ----

    /**
     * Executes a Substrait plan against S3-backed Parquet files via DataFusion.
     * Used for Iceberg external table queries where files are on S3, not local disk.
     *
     * @param s3Region         AWS region
     * @param s3Bucket         S3 bucket name
     * @param s3AccessKeyId    AWS access key ID, or null for default credentials
     * @param s3SecretAccessKey AWS secret access key, or null for default credentials
     * @param s3SessionToken   AWS session token, or null
     * @param s3Endpoint       S3 endpoint override, or null (for local testing with MinIO etc.)
     * @param filePaths        array of S3 Parquet file paths to scan
     * @param tableName        table name (must match the name in the Substrait plan)
     * @param substraitPlan    serialized Substrait plan bytes
     * @param listener         callback receiving the stream pointer (Long) or error
     */
    public static native void executeIcebergQueryAsync(
        String s3Region,
        String s3Bucket,
        String s3AccessKeyId,
        String s3SecretAccessKey,
        String s3SessionToken,
        String s3Endpoint,
        String[] filePaths,
        String tableName,
        byte[] substraitPlan,
        org.opensearch.core.action.ActionListener<Long> listener
    );
```

- [ ] **Step 3: Compile check**

Run: `./gradlew :sandbox:plugins:analytics-backend-datafusion:compileJava`
Expected: SUCCESS (native methods are just declarations)

- [ ] **Step 4: Design alignment check**

Verify: The JNI method signature matches the Rust JNI function parameter order exactly. The Rust function at `lib.rs:246` takes: `s3_region, s3_bucket, s3_access_key_id, s3_secret_access_key, s3_session_token, s3_endpoint, file_paths, table_name, substrait_bytes, listener` — must match the Java declaration order.

- [ ] **Step 5: Commit**

```bash
git add sandbox/plugins/analytics-backend-datafusion/src/main/java/org/opensearch/be/datafusion/jni/NativeBridge.java
git commit -m "feat(datafusion): add executeIcebergQueryAsync native method declaration

Java-side declaration for the existing Rust JNI binding that executes
Substrait plans against S3-backed Parquet files via DataFusion.

Signed-off-by: Vamsi Manohar <reddyvam@amazon.com>"
```

---

### Task 4: Implement executeRemoteQuery in DataFusionPlugin

**Files:**
- Modify: `sandbox/plugins/analytics-backend-datafusion/src/main/java/org/opensearch/be/datafusion/DataFusionPlugin.java`

- [ ] **Step 1: Override executeRemoteQuery**

Add the following method to `DataFusionPlugin.java`:

```java
    @Override
    public Iterable<Object[]> executeRemoteQuery(ExternalScanContext scanContext) {
        if (dataFusionService == null) {
            throw new IllegalStateException("DataFusionService not initialized");
        }

        Map<String, String> config = scanContext.getStorageConfig();
        String s3Region = config.getOrDefault("s3Region", "us-east-1");
        String s3Bucket = config.get("s3Bucket");
        String s3AccessKeyId = config.get("s3AccessKeyId");
        String s3SecretAccessKey = config.get("s3SecretAccessKey");
        String s3SessionToken = config.get("s3SessionToken");
        String s3Endpoint = config.get("s3Endpoint");

        String[] filePaths = scanContext.getDataFilePaths().toArray(new String[0]);
        String tableName = scanContext.getTableName();
        byte[] substraitPlan = scanContext.getSubstraitPlan();

        // Call DataFusion via JNI — returns a stream pointer
        CompletableFuture<Long> future = new CompletableFuture<>();
        NativeBridge.executeIcebergQueryAsync(
            s3Region, s3Bucket,
            s3AccessKeyId, s3SecretAccessKey, s3SessionToken, s3Endpoint,
            filePaths, tableName, substraitPlan,
            new ActionListener<>() {
                @Override
                public void onResponse(Long streamPtr) { future.complete(streamPtr); }
                @Override
                public void onFailure(Exception e) { future.completeExceptionally(e); }
            }
        );

        long streamPtr;
        try {
            streamPtr = future.join();
        } catch (Exception e) {
            throw new RuntimeException("Iceberg query execution failed via DataFusion", e);
        }

        // Stream Arrow batches and convert to Object[] rows
        NativeRuntimeHandle runtimeHandle = dataFusionService.getNativeRuntime();
        StreamHandle streamHandle = new StreamHandle(streamPtr, runtimeHandle);
        BufferAllocator allocator = dataFusionService.newChildAllocator();
        DatafusionResultStream resultStream = new DatafusionResultStream(streamHandle, allocator);

        List<Object[]> rows = new ArrayList<>();
        try {
            Iterator<EngineResultBatch> batchIterator = resultStream.iterator();
            while (batchIterator.hasNext()) {
                EngineResultBatch batch = batchIterator.next();
                List<String> fieldNames = batch.getFieldNames();
                for (int row = 0; row < batch.getRowCount(); row++) {
                    Object[] rowValues = new Object[fieldNames.size()];
                    for (int col = 0; col < fieldNames.size(); col++) {
                        rowValues[col] = batch.getFieldValue(fieldNames.get(col), row);
                    }
                    rows.add(rowValues);
                }
            }
        } finally {
            resultStream.close();
        }
        logger.info("[DataFusionPlugin] Iceberg query returned {} rows via native execution", rows.size());
        return rows;
    }
```

Add imports at the top of the file:
```java
import org.opensearch.analytics.exec.ExternalScanContext;
import org.opensearch.analytics.backend.EngineResultBatch;
import org.opensearch.be.datafusion.jni.NativeBridge;
import org.opensearch.be.datafusion.jni.StreamHandle;
import org.opensearch.core.action.ActionListener;
import org.apache.arrow.memory.BufferAllocator;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
```

- [ ] **Step 2: Compile check**

Run: `./gradlew :sandbox:plugins:analytics-backend-datafusion:compileJava`
Expected: SUCCESS

- [ ] **Step 3: Design alignment check**

Verify: DataFusionPlugin handles ALL native execution concerns (JNI call, Arrow batch streaming, memory allocation). No Iceberg-specific code here — it receives generic `ExternalScanContext` with file paths and Substrait bytes. The same method could work for Delta Lake or any other external table format.

- [ ] **Step 4: Commit**

```bash
git add sandbox/plugins/analytics-backend-datafusion/src/main/java/org/opensearch/be/datafusion/DataFusionPlugin.java
git commit -m "feat(datafusion): implement executeRemoteQuery for S3-backed Parquet

DataFusionPlugin.executeRemoteQuery() calls NativeBridge.executeIcebergQueryAsync()
to execute Substrait plans against S3 Parquet files via the native DataFusion
engine. Streams Arrow record batches back to Java and converts to Object[] rows.

Signed-off-by: Vamsi Manohar <reddyvam@amazon.com>"
```

---

### Task 5: Store CatalogConfig in IcebergCalciteTable

**Files:**
- Modify: `sandbox/plugins/lakehouse-iceberg/src/main/java/org/opensearch/lakehouse/schema/IcebergCalciteTable.java`
- Modify: `sandbox/plugins/lakehouse-iceberg/src/main/java/org/opensearch/lakehouse/schema/IcebergSchemaEnricher.java`
- Modify: `sandbox/plugins/lakehouse-iceberg/src/main/java/org/opensearch/lakehouse/LakehousePlugin.java` (contributeSchema only)

`prepareScan()` needs S3 region/credentials. These come from the `CatalogConfig`. Store it in `IcebergCalciteTable` so it's available at query time.

- [ ] **Step 1: Read IcebergCalciteTable.java**

Read the file to understand current fields and constructor.

- [ ] **Step 2: Add catalogConfig field to IcebergCalciteTable**

Add to `IcebergCalciteTable.java`:
```java
import org.opensearch.lakehouse.catalog.CatalogConfig;

// Add field:
private final CatalogConfig catalogConfig;

// Modify constructor to accept CatalogConfig:
public IcebergCalciteTable(Table icebergTable, CatalogConfig catalogConfig) {
    this.icebergTable = icebergTable;
    this.pinnedSnapshotId = icebergTable.currentSnapshot() != null
        ? icebergTable.currentSnapshot().snapshotId() : -1;
    this.catalogConfig = catalogConfig;
}

// Add getter:
/** Returns the catalog config for storage access. */
public CatalogConfig getCatalogConfig() { return catalogConfig; }
```

- [ ] **Step 3: Update IcebergSchemaEnricher to accept CatalogConfig**

Modify `IcebergSchemaEnricher.enrich()` signature and callers to pass `Map<String, CatalogConfig>` alongside `Map<String, Table>`:

```java
public static void enrich(SchemaPlus schema, Map<String, Table> tables, Map<String, CatalogConfig> catalogConfigs) {
    for (Map.Entry<String, Table> entry : tables.entrySet()) {
        String name = entry.getKey();
        Table table = entry.getValue();
        CatalogConfig config = catalogConfigs.get(name);
        schema.add(name, new IcebergCalciteTable(table, config));
    }
}
```

- [ ] **Step 4: Update LakehousePlugin.contributeSchema() to pass CatalogConfig**

In `LakehousePlugin.contributeSchema()`, build a parallel map of table name → CatalogConfig:

```java
Map<String, Table> icebergTables = new HashMap<>();
Map<String, CatalogConfig> tableCatalogConfigs = new HashMap<>();
for (Map.Entry<String, Map<String, String>> entry : metadata.tables().entrySet()) {
    String tableName = entry.getKey();
    Map<String, String> binding = entry.getValue();
    String catalogName = binding.get("catalog");
    // ... existing loadTable code ...
    try {
        Table table = catalogConnector.loadTable(catalogName, TableIdentifier.of(namespace, icebergTableName));
        icebergTables.put(tableName, table);
        // Find the CatalogConfig for this table's catalog
        Map<String, String> catalogConfigMap = metadata.catalogs().get(catalogName);
        if (catalogConfigMap != null) {
            CatalogConfig config = new CatalogConfig(
                catalogName,
                CatalogType.valueOf(catalogConfigMap.getOrDefault("type", "GLUE").toUpperCase(java.util.Locale.ROOT)),
                catalogConfigMap.get("uri"),
                catalogConfigMap.get("warehouse"),
                catalogConfigMap.get("region"),
                catalogConfigMap.get("database"),
                catalogConfigMap.getOrDefault("credential_provider", "default"),
                Duration.ofMinutes(5)
            );
            tableCatalogConfigs.put(tableName, config);
        }
    } catch (Exception e) {
        // ... existing error handling ...
    }
}

if (!icebergTables.isEmpty()) {
    IcebergSchemaEnricher.enrich(schema, icebergTables, tableCatalogConfigs);
}
```

- [ ] **Step 5: Compile check**

Run: `./gradlew :sandbox:plugins:lakehouse-iceberg:compileJava`
Expected: SUCCESS

- [ ] **Step 6: Design alignment check**

Verify: `CatalogConfig` is stored in `IcebergCalciteTable` at schema-build time. `prepareScan()` (next task) can access S3 config without needing cluster state. No changes to the ExternalTable interface in analytics-framework.

- [ ] **Step 7: Commit**

```bash
git add sandbox/plugins/lakehouse-iceberg/src/main/java/org/opensearch/lakehouse/schema/IcebergCalciteTable.java \
       sandbox/plugins/lakehouse-iceberg/src/main/java/org/opensearch/lakehouse/schema/IcebergSchemaEnricher.java \
       sandbox/plugins/lakehouse-iceberg/src/main/java/org/opensearch/lakehouse/LakehousePlugin.java
git commit -m "feat(lakehouse): store CatalogConfig in IcebergCalciteTable

IcebergCalciteTable now carries the CatalogConfig for its parent catalog.
This makes S3 region and credentials available at query time without
needing access to cluster state.

Signed-off-by: Vamsi Manohar <reddyvam@amazon.com>"
```

---

### Task 6: Implement LakehousePlugin.prepareScan and remove Java execution

**Files:**
- Modify: `sandbox/plugins/lakehouse-iceberg/src/main/java/org/opensearch/lakehouse/LakehousePlugin.java`

This is the core change. `prepareScan()` uses `IcebergScanPlanner` for file resolution and `CalciteSubstraitConverter` for plan conversion. Then we remove ALL the Java in-memory execution code.

- [ ] **Step 1: Add fields for scan planner**

Add to `LakehousePlugin.java` class body:

```java
private final ExecutorService scanExecutor = java.util.concurrent.Executors.newFixedThreadPool(
    Math.max(2, Runtime.getRuntime().availableProcessors() / 2));
private final IcebergScanPlanner scanPlanner = new IcebergScanPlanner(scanExecutor);
```

Add imports:
```java
import org.opensearch.analytics.exec.ExternalScanContext;
import org.opensearch.lakehouse.scan.IcebergScanPlanner;
import org.opensearch.lakehouse.scan.IcebergScanPlan;
import org.opensearch.lakehouse.substrait.CalciteSubstraitConverter;
import java.util.concurrent.ExecutorService;
```

- [ ] **Step 2: Implement prepareScan()**

Add the `prepareScan()` override:

```java
@Override
public ExternalScanContext prepareScan(RelNode logicalPlan, ExternalTable externalTable) {
    if (!(externalTable instanceof IcebergCalciteTable)) {
        throw new IllegalArgumentException("Expected IcebergCalciteTable but got: " + externalTable.getClass().getSimpleName());
    }
    IcebergCalciteTable icebergTable = (IcebergCalciteTable) externalTable;
    Table table = icebergTable.getIcebergTable();
    logger.info("[LakehousePlugin] Preparing scan for Iceberg table: {}", table.name());

    // 1. Extract Iceberg predicates for manifest-level pruning
    Expression filterExpr = extractIcebergFilter(logicalPlan);
    List<Expression> predicates = filterExpr != null ? List.of(filterExpr) : List.of();

    // 2. Plan scan — resolves manifests to pruned S3 Parquet file paths
    IcebergScanPlan scanPlan = scanPlanner.planScan(
        table,
        icebergTable.getPinnedSnapshotId(),
        predicates,
        null  // all columns — DataFusion handles projection from Substrait plan
    );
    logger.info("[LakehousePlugin] Scan plan: {} files, {} bytes total",
        scanPlan.fileCount(), scanPlan.getTotalFileSize());

    // 3. Convert Calcite RelNode to Substrait bytes
    byte[] substraitBytes;
    try {
        substraitBytes = CalciteSubstraitConverter.toSubstrait(logicalPlan);
    } catch (Exception e) {
        throw new RuntimeException("Failed to convert query plan to Substrait", e);
    }

    // 4. Build storage config from CatalogConfig
    Map<String, String> storageConfig = buildStorageConfig(icebergTable, scanPlan);

    // 5. Extract table name from the Calcite plan (must match Substrait reference)
    String tableName = extractTableName(logicalPlan);

    return new ExternalScanContext(tableName, scanPlan.getDataFilePaths(), substraitBytes, storageConfig);
}

private Expression extractIcebergFilter(RelNode node) {
    Filter filter = findNode(node, Filter.class);
    if (filter == null) {
        return null;
    }
    RelDataType inputRowType = filter.getInput().getRowType();
    return CalciteToIcebergPredicateConverter.convert(filter.getCondition(), inputRowType);
}

private String extractTableName(RelNode node) {
    if (node instanceof org.apache.calcite.rel.core.TableScan) {
        List<String> qn = node.getTable().getQualifiedName();
        return qn.get(qn.size() - 1);
    }
    for (RelNode input : node.getInputs()) {
        String name = extractTableName(input);
        if (name != null) return name;
    }
    throw new IllegalArgumentException("No TableScan found in plan");
}

private Map<String, String> buildStorageConfig(IcebergCalciteTable icebergTable, IcebergScanPlan scanPlan) {
    Map<String, String> config = new HashMap<>();
    CatalogConfig catalogConfig = icebergTable.getCatalogConfig();
    if (catalogConfig != null) {
        if (catalogConfig.region() != null) config.put("s3Region", catalogConfig.region());
    }
    // Extract bucket from first file path
    List<String> paths = scanPlan.getDataFilePaths();
    if (!paths.isEmpty()) {
        String firstPath = paths.get(0);
        if (firstPath.startsWith("s3://")) {
            String withoutScheme = firstPath.substring(5);
            int slashIdx = withoutScheme.indexOf('/');
            if (slashIdx > 0) config.put("s3Bucket", withoutScheme.substring(0, slashIdx));
        }
    }
    // For file:// paths (local testing), set endpoint for local S3
    if (!paths.isEmpty() && paths.get(0).startsWith("file://")) {
        config.put("s3Endpoint", "file://");
    }
    return config;
}
```

- [ ] **Step 3: Remove old execute() and all in-memory execution methods**

Remove the following methods from `LakehousePlugin.java`:
- `execute(RelNode, ExternalTable)` — the old interface method (entire method, ~50 lines)
- `applyAggregate(Aggregate, List<Object[]>)` — ~42 lines
- `computeAggregation(AggregateCall, List<Object[]>)` — ~33 lines
- `minMax(List<Object[]>, int, boolean)` — ~12 lines
- `extractProjectedColumns(RelNode, Table)` — ~25 lines
- `extractFilterExpression(RelNode)` — replaced by `extractIcebergFilter`
- `findScanRowType(RelNode)` — ~12 lines

Keep:
- `findNode(RelNode, Class)` — still used by `extractIcebergFilter` and `extractTableName`
- `contributeSchema(SchemaPlus, ClusterState)` — unchanged

Remove these imports that are no longer needed:
```java
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
```

- [ ] **Step 4: Remove @FunctionalInterface from ExternalTableExecutor**

Since `ExternalTableExecutor` now has two methods (`execute` and `prepareScan`), remove `@FunctionalInterface` annotation from line 24 of `ExternalTableExecutor.java`.

- [ ] **Step 5: Compile check**

Run: `./gradlew :sandbox:plugins:lakehouse-iceberg:compileJava :sandbox:plugins:analytics-engine:compileJava`
Expected: SUCCESS

- [ ] **Step 6: Design alignment check**

Verify:
- `LakehousePlugin` ONLY handles Iceberg metadata resolution (catalogs, manifests, file paths) ✓
- No Java in-memory execution (no aggregation, no row iteration) ✓
- `CalciteSubstraitConverter` bridges Calcite → DataFusion ✓
- `CalciteToIcebergPredicateConverter` is kept for manifest-level pushdown ✓
- `IcebergScanPlanner` resolves which files to read ✓
- DataFusion handles ALL query execution (ORDER BY, LIMIT, aggregation, expressions) ✓

- [ ] **Step 7: Commit**

```bash
git add sandbox/plugins/lakehouse-iceberg/src/main/java/org/opensearch/lakehouse/LakehousePlugin.java \
       sandbox/libs/analytics-framework/src/main/java/org/opensearch/analytics/exec/ExternalTableExecutor.java
git commit -m "feat(lakehouse): wire DataFusion execution, remove Java in-memory engine

LakehousePlugin.prepareScan() resolves Iceberg table metadata to pruned
S3 file paths via IcebergScanPlanner, converts the Calcite plan to
Substrait via CalciteSubstraitConverter, and returns ExternalScanContext.

Removes ~150 lines of ad-hoc Java execution: applyAggregate(),
computeAggregation(), minMax(), extractProjectedColumns(),
extractFilterExpression(), findScanRowType(). DataFusion now handles
all query execution natively (including ORDER BY, LIMIT, HAVING,
expressions, aggregation).

Signed-off-by: Vamsi Manohar <reddyvam@amazon.com>"
```

---

### Task 7: Remove legacy execute() fallback and clean up

**Files:**
- Modify: `sandbox/plugins/analytics-engine/src/main/java/org/opensearch/analytics/exec/DefaultPlanExecutor.java`
- Modify: `sandbox/libs/analytics-framework/src/main/java/org/opensearch/analytics/exec/ExternalTableExecutor.java`

- [ ] **Step 1: Remove fallback in DefaultPlanExecutor**

In `DefaultPlanExecutor.execute()`, remove the fallback branch:

```java
// Remove this block:
// Fallback to legacy in-process execution (will be removed once DataFusion path is wired)
logger.info("[DefaultPlanExecutor] Falling back to legacy external table executor");
return externalTableExecutor.execute(logicalFragment, externalTable);
```

Change the `prepareScan` null check to throw instead of falling back:

```java
ExternalScanContext scanContext = externalTableExecutor.prepareScan(logicalFragment, externalTable);
if (scanContext == null) {
    throw new IllegalStateException("ExternalTableExecutor.prepareScan() returned null for " + externalTable);
}
```

- [ ] **Step 2: Remove execute() from ExternalTableExecutor**

Replace the entire `ExternalTableExecutor.java` with:

```java
package org.opensearch.analytics.exec;

import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.schema.ExternalTable;

/**
 * Prepares a scan context for queries against external (non-OpenSearch) tables.
 * Implementations are discovered by ExtensiblePlugin and injected into the
 * QueryPlanExecutor. The actual execution is delegated to the analytics backend.
 */
public interface ExternalTableExecutor {

    /**
     * Prepares a scan context for an external table query.
     *
     * @param logicalPlan   the optimized Calcite plan
     * @param externalTable the external table found in the plan
     * @return scan context for native execution
     */
    ExternalScanContext prepareScan(RelNode logicalPlan, ExternalTable externalTable);
}
```

- [ ] **Step 3: Compile check**

Run: `./gradlew :sandbox:libs:analytics-framework:compileJava :sandbox:plugins:analytics-engine:compileJava :sandbox:plugins:lakehouse-iceberg:compileJava`
Expected: SUCCESS

- [ ] **Step 4: Commit**

```bash
git add sandbox/plugins/analytics-engine/src/main/java/org/opensearch/analytics/exec/DefaultPlanExecutor.java \
       sandbox/libs/analytics-framework/src/main/java/org/opensearch/analytics/exec/ExternalTableExecutor.java
git commit -m "refactor(analytics): remove legacy execute() from ExternalTableExecutor

All external table queries now go through prepareScan() + executeRemoteQuery().
The old execute() method and its fallback in DefaultPlanExecutor are removed.

Signed-off-by: Vamsi Manohar <reddyvam@amazon.com>"
```

---

### Task 8: Update unit tests

**Files:**
- Modify: Test files for `DefaultPlanExecutor`, `LakehousePlugin`, `IcebergCalciteTable`, `IcebergSchemaEnricher`

- [ ] **Step 1: Update DefaultPlanExecutorTests**

The mock `ExternalTableExecutor` needs to implement `prepareScan()` instead of `execute()`. Update accordingly:

```java
ExternalTableExecutor mockExecutor = (plan, table) -> new ExternalScanContext(
    "test_table",
    List.of("s3://bucket/file.parquet"),
    new byte[]{1, 2, 3},  // dummy substrait
    Map.of("s3Region", "us-east-1", "s3Bucket", "bucket")
);
```

- [ ] **Step 2: Update IcebergCalciteTableTests**

Update constructor calls to pass `null` for `CatalogConfig` in existing tests:
```java
new IcebergCalciteTable(mockTable, null)
```

- [ ] **Step 3: Update IcebergSchemaEnricherTests**

Update `enrich()` calls to pass empty `catalogConfigs` map:
```java
IcebergSchemaEnricher.enrich(schema, tables, Map.of());
```

- [ ] **Step 4: Compile and run tests**

Run: `./gradlew :sandbox:plugins:lakehouse-iceberg:test :sandbox:plugins:analytics-engine:test`
Expected: All existing tests pass (some may need minor signature updates)

- [ ] **Step 5: Commit**

```bash
git add -A sandbox/
git commit -m "test: update unit tests for DataFusion execution path

Update test mocks and constructors for new ExternalTableExecutor.prepareScan()
interface and IcebergCalciteTable CatalogConfig parameter.

Signed-off-by: Vamsi Manohar <reddyvam@amazon.com>"
```

---

### Task 9: Full compilation and E2E verification

- [ ] **Step 1: Full sandbox compilation**

Run: `./gradlew :sandbox:plugins:lakehouse-iceberg:assemble :sandbox:plugins:analytics-engine:assemble :sandbox:plugins:analytics-backend-datafusion:assemble :sandbox:plugins:dsl-query-executor:assemble`
Expected: SUCCESS — all plugin ZIPs built

- [ ] **Step 2: Verify native library has executeIcebergQueryAsync**

Run: `nm -D sandbox/plugins/analytics-backend-datafusion/jni/target/release/libopensearch_datafusion_jni.so | grep -i iceberg`
Expected: Symbol `Java_org_opensearch_be_datafusion_jni_NativeBridge_executeIcebergQueryAsync` is present

If NOT present, rebuild native library:
```bash
cd sandbox/plugins/analytics-backend-datafusion/jni && cargo build --release
```

- [ ] **Step 3: Start OpenSearch and install plugins**

```bash
# Build distribution and install plugins
# (Follow existing test setup — create test Iceberg table, register catalog/table)
```

- [ ] **Step 4: Test SQL queries through DataFusion**

```bash
# Basic SELECT
curl -s -X POST "localhost:9200/_analytics/sql" -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM test_events LIMIT 5"}'

# Filter pushdown
curl -s -X POST "localhost:9200/_analytics/sql" -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM test_events WHERE `value` > 20"}'

# Aggregation
curl -s -X POST "localhost:9200/_analytics/sql" -H 'Content-Type: application/json' \
  -d '{"query": "SELECT category, COUNT(*), SUM(`value`) FROM test_events GROUP BY category"}'

# ORDER BY + LIMIT (previously not working!)
curl -s -X POST "localhost:9200/_analytics/sql" -H 'Content-Type: application/json' \
  -d '{"query": "SELECT * FROM test_events ORDER BY `value` DESC LIMIT 3"}'

# HAVING (previously not working!)
curl -s -X POST "localhost:9200/_analytics/sql" -H 'Content-Type: application/json' \
  -d '{"query": "SELECT category, COUNT(*) as cnt FROM test_events GROUP BY category HAVING COUNT(*) > 2"}'
```

- [ ] **Step 5: Final design alignment check**

Run the 5-point checklist from the plan header:
1. LakehousePlugin only handles Iceberg metadata? ✓
2. DefaultPlanExecutor routes ALL queries through backend? ✓
3. DataFusionPlugin handles ALL native execution? ✓
4. CalciteSubstraitConverter bridges Calcite ↔ DataFusion? ✓
5. Compilation passes? ✓

- [ ] **Step 6: Commit and push**

```bash
git push vamsi feature/lakehouse-iceberg
```

---

## Self-Review Checklist

| Requirement | Task | Status |
|-------------|------|--------|
| Replace Java in-memory execution with DataFusion | Task 6 | ✓ |
| Use existing IcebergScanPlanner for file resolution | Task 6 | ✓ |
| Use existing CalciteSubstraitConverter for plan conversion | Task 6 | ✓ |
| Use existing Rust iceberg_executor.rs + s3_store.rs | Task 4 (via JNI) | ✓ |
| Wire existing JNI binding | Task 3 | ✓ |
| Keep CalciteToIcebergPredicateConverter for manifest pruning | Task 6 | ✓ |
| Remove ~150 lines of Java execution code | Task 6 | ✓ |
| Remove old execute() interface | Task 7 | ✓ |
| Compilation works after every task | All tasks | ✓ |
| Design alignment verified after every task | All tasks | ✓ |
| ORDER BY / LIMIT now works (DataFusion handles it) | Task 9 | ✓ |
| HAVING now works (DataFusion handles it) | Task 9 | ✓ |
| Memory management via DataFusion pool + spill | Task 4 | ✓ |

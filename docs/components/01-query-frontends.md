# Component 1: Query Frontends — Unified Query Framework Integration

## Table of Contents

1. [Overview](#1-overview)
2. [Existing Unified Query Framework](#2-existing-unified-query-framework)
   - [UnifiedQueryParser](#21-unifiedqueryparser)
   - [UnifiedQueryContext](#22-unifiedquerycontext)
   - [UnifiedQueryPlanner](#23-unifiedqueryplanner)
   - [UnifiedQueryCompiler](#24-unifiedquerycompiler)
   - [UnifiedQueryTranspiler](#25-unifiedquerytranspiler)
   - [UnifiedFunction](#26-unifiedfunction)
   - [UnifiedFunctionRepository](#27-unifiedfunctionrepository)
   - [UnifiedFunctionCalciteAdapter](#28-unifiedfunctioncalciteadapter)
3. [Three Execution Paths from RelNode](#3-three-execution-paths-from-relnode)
4. [What We Need to Build](#4-what-we-need-to-build)
5. [Java Interfaces for New Classes](#5-java-interfaces-for-new-classes)
6. [Table Resolution via Calcite Schema Registration](#6-table-resolution-via-calcite-schema-registration)
7. [Function Handling and SQL Function Mappings](#7-function-handling-and-sql-function-mappings)
8. [Integration Flow: Query String to SQL Plan](#8-integration-flow-query-string-to-sql-plan)
9. [What Does NOT Change](#9-what-does-not-change)
10. [Integration with Component 00 (Lakehouse Index Abstraction)](#10-integration-with-component-00-lakehouse-index-abstraction)

---

## 1. Overview

The OpenSearch SQL plugin already contains a **Unified Query Framework** that handles both SQL and PPL query execution. This framework parses queries into Calcite `RelNode` trees that can then be compiled or transpiled to multiple backends.

**We are NOT building new parsers or frontends.** Instead, we are:

1. **Reusing** `UnifiedQueryPlanner` to parse both SQL and PPL into Calcite `RelNode`
2. **Adding** `LakehouseCalciteSchema` and `LakehouseTable` so that Iceberg-backed tables are visible to the framework as Calcite `Schema`/`Table` objects
3. **Adding** `LakehouseQueryRouter` to detect lakehouse-targeted queries and dispatch to the new execution path
4. **Adding** `LakehouseContextFactory` to construct a properly configured `UnifiedQueryContext` with Iceberg-backed schemas registered
5. **Adding** `SqlProducer` (Component 2) as a new compilation target that accepts a `RelNode` and produces a SQL string for DataFusion

The existing parsers, analyzers, `CalciteRelNodeVisitor`, and optimization rules are all **reused without modification**.

### Position in the Pipeline

```
User query text (PPL or SQL)
        │
        ▼
┌────────────────────────────────┐
│  LakehouseQueryRouter          │  ← detects lakehouse query target
│  LakehouseContextFactory       │  ← builds UnifiedQueryContext w/ Iceberg schemas
│  UnifiedQueryPlanner.plan()    │  ← Component 1 (this document)
└──────────────┬─────────────────┘
               │ RelNode (Calcite logical plan)
               ▼
┌────────────────────────────────┐
│  SqlProducer                   │  ← Component 2 (new, Path C)
└──────────────┬─────────────────┘
               │ SQL string
               ▼
  Stage Splitter → DataFusion workers
```

---

## 2. Existing Unified Query Framework

The following classes already exist in
`opensearch-sql/api/src/main/java/org/opensearch/sql/api/` and are the foundation
upon which the lakehouse path is built. We do not modify any of them.

### 2.1 `UnifiedQueryParser`

**File:** `parser/UnifiedQueryParser.java`

```java
public interface UnifiedQueryParser<T> {
    T parse(String query);
}
```

Language-neutral parser interface. The concrete type `T` is language-specific:

| Language | Implementation | `T` |
|---|---|---|
| PPL | `PPLQueryParser` | `UnresolvedPlan` |
| SQL | `CalciteSqlQueryParser` | `SqlNode` |

The correct parser is selected automatically by `UnifiedQueryContext.Builder.build()` based on the `QueryType` (PPL or SQL).

### 2.2 `UnifiedQueryContext`

**File:** `UnifiedQueryContext.java`

The central configuration object for a single query execution. Constructed via a fluent builder:

```java
UnifiedQueryContext ctx = UnifiedQueryContext.builder()
    .language(QueryType.PPL)
    .catalog("lakehouse", lakehouseSchema)     // register Iceberg-backed schema
    .defaultNamespace("lakehouse.sales")       // unqualified names resolve here
    .profiling(true)
    .setting("plugins.query.size_limit", 10000)
    .build();
```

Key capabilities:

| Builder Method | Purpose |
|---|---|
| `.language(QueryType)` | Select PPL or SQL parser+planning strategy |
| `.catalog(name, Schema)` | Register a Calcite `Schema` under a catalog name; tables within it become referenceable |
| `.defaultNamespace(path)` | Dot-separated default path for unqualified table names (e.g., `"lakehouse.sales"`) |
| `.cacheMetadata(bool)` | Enable/disable Calcite schema metadata caching |
| `.profiling(bool)` | Enable query profiling; metrics retrievable via `ctx.getProfile()` |
| `.setting(name, value)` | Override planning settings (query size limit, subsearch limits, etc.) |

`UnifiedQueryContext` also provides `measure(MetricName, Callable<T>)` for profiling phases
outside the planner, and implements `AutoCloseable` to release the underlying Calcite JDBC
connection.

For the lakehouse path, `LakehouseContextFactory` (§4.3) is responsible for constructing
`UnifiedQueryContext` instances with Iceberg-backed schemas registered.

### 2.3 `UnifiedQueryPlanner`

**File:** `UnifiedQueryPlanner.java`

The core planning entry point. Accepts a raw query string and returns a Calcite `RelNode`:

```java
UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);
RelNode logicalPlan = planner.plan(queryString);
```

Internally uses two planning strategies, selected by query type:

**SQL path — `CalciteNativeStrategy`:**
```
SqlParser.parse(query)
    → SqlNode
    → planner.validate(sqlNode)    // SqlValidator: type checking, name resolution
    → planner.rel(validatedNode)   // SqlToRelConverter: SqlNode → RelRoot
    → relRoot.project()            // RelNode (with collation preserved)
```

**PPL path — `CustomVisitorStrategy`:**
```
PPLQueryParser.parse(query)
    → UnresolvedPlan (ANTLR → AstBuilder visitor)
    → CalciteRelNodeVisitor.analyze(ast, planContext)
    → RelNode
    → (collation preservation)
```

Both paths produce the **same** `RelNode` output type. The caller never needs to know which
language was used to produce the plan.

### 2.4 `UnifiedQueryCompiler`

**File:** `compiler/UnifiedQueryCompiler.java`

Compiles a `RelNode` into an executable `PreparedStatement` via Calcite's Enumerable
convention (local in-process execution). This is **Path A** — the existing default execution
path.

```java
UnifiedQueryCompiler compiler = new UnifiedQueryCompiler(context);
PreparedStatement stmt = compiler.compile(relNode);
ResultSet rs = stmt.executeQuery();
```

Internally applies a `RelHomogeneousShuttle` to convert `LogicalTableScan` nodes to
`BindableTableScan` (required for Calcite's interpreter to handle schema-backed tables), then
delegates to a `RelRunner` obtained from the context's JDBC connection.

For the lakehouse path, we do **not** use `UnifiedQueryCompiler`. Instead, the `RelNode` goes
to `SqlProducer` (Path C).

### 2.5 `UnifiedQueryTranspiler`

**File:** `transpiler/UnifiedQueryTranspiler.java`

Transpiles a `RelNode` to a SQL string for a target dialect. This is **Path B** — the existing
SparkSQL/EMR execution path.

```java
UnifiedQueryTranspiler transpiler = UnifiedQueryTranspiler.builder()
    .dialect(SparkSqlDialect.DEFAULT)
    .build();
String sparkSql = transpiler.toSql(relNode);
// → send to EMR
```

Uses Calcite's `RelToSqlConverter` internally. The output SQL is faithful to the logical plan
structure and respects the target dialect's quoting, function names, and type syntax.

### 2.6 `UnifiedFunction`

**File:** `function/UnifiedFunction.java`

Engine-agnostic function descriptor. Types are represented as SQL type name strings
(`"VARCHAR"`, `"INTEGER"`, `"ARRAY<T>"`, etc.) to avoid a dedicated type abstraction.

```java
public interface UnifiedFunction extends Serializable {
    String getFunctionName();
    List<String> getInputTypes();
    String getReturnType();
    Object eval(List<Object> inputs);
}
```

`eval()` is used for in-process coordinator-side evaluation. For functions that run on worker
nodes via SQL, the function is identified by name and mapped to the corresponding SQL syntax
by the DataFusion dialect (handled by Component 2).

### 2.7 `UnifiedFunctionRepository`

**File:** `function/UnifiedFunctionRepository.java`

Loads all PPL built-in operators from `PPLBuiltinOperators` as `UnifiedFunctionDescriptor`
instances. Each descriptor carries a function name and a `UnifiedFunctionBuilder` that
constructs a `UnifiedFunction` for a given set of input types.

```java
UnifiedFunctionRepository repo = new UnifiedFunctionRepository(context);

// Load all PPL functions
List<UnifiedFunctionDescriptor> all = repo.loadFunctions();

// Load a specific function
Optional<UnifiedFunctionDescriptor> upper = repo.loadFunction("UPPER");
UnifiedFunction fn = upper.get().getBuilder().build(List.of("VARCHAR"));
fn.eval(List.of("hello")); // → "HELLO"
```

The repository filters `PPLBuiltinOperators.instance().getOperatorList()` for
`SqlUserDefinedFunction` instances, meaning **all PPL functions are automatically discoverable**
without any manual registration.

For the lakehouse path, Component 2 (`SqlProducer`) uses the function names discovered
here to produce SQL function calls via the DataFusion dialect.

### 2.8 `UnifiedFunctionCalciteAdapter`

**File:** `function/UnifiedFunctionCalciteAdapter.java`

Implements `UnifiedFunction` using Calcite's `RexExecutorImpl`. Created via:

```java
UnifiedFunctionCalciteAdapter fn =
    UnifiedFunctionCalciteAdapter.create(rexBuilder, "UPPER", List.of("VARCHAR"));
fn.eval(List.of("hello")); // → "HELLO"
```

Internally, `create()`:
1. Builds `RexNode[]` input refs for the given input types
2. Resolves the function via `PPLFuncImpTable.INSTANCE.resolve()`
3. Pre-compiles the resolved `RexNode` to a Java source string via `RexExecutorImpl.getExecutable()`
4. Stores the compiled source string (not the `RexNode`) to enable serialization across JVM boundaries

This design allows `UnifiedFunction` instances to be serialized and shipped to workers if needed,
though for the lakehouse path the function implementation runs on workers via DataFusion using
SQL function calls.

---

## 3. Three Execution Paths from RelNode

After `UnifiedQueryPlanner.plan()` produces a `RelNode`, there are three compilation targets:

```
                         ┌─────────────────────────┐
                         │  UnifiedQueryPlanner     │
                         │  .plan(queryString)      │
                         └──────────┬──────────────┘
                                    │ RelNode
                         ┌──────────┴──────────────┐
                         │                         │
                         ▼                         ▼
             (lakehouse index?)            (OpenSearch index)
                    │                              │
         ┌──────────┘                    ┌─────────┴──────────┐
         │                               │                    │
         ▼                               ▼                    ▼
 Path C (NEW)                       Path A (existing)   Path B (existing)
 SqlProducer                        UnifiedQueryCompiler UnifiedQueryTranspiler
 → SQL string                       → PreparedStatement  → SparkSQL string
 → Stage Splitter                   → Calcite Enumerable → EMR
 → DataFusion workers               (local coordinator)
```

### Path A — Local Calcite Enumerable (existing)

```
RelNode → UnifiedQueryCompiler.compile(plan) → PreparedStatement → JDBC ResultSet
```

Used when the query targets standard OpenSearch indices. Runs entirely on the coordinator node
using Calcite's interpreter/enumerable convention. No workers are involved.

### Path B — SparkSQL via EMR (existing)

```
RelNode → UnifiedQueryTranspiler.toSql(plan) → SparkSQL string → EMR async job → S3 results
```

Used by the async query path for existing `direct_query` data source types. The `RelNode` is
back-converted to SQL for the target dialect rather than being executed directly.

### Path C — SQL → DataFusion Workers (new, lakehouse)

```
RelNode → SqlProducer.toSql(plan, DataFusionDialect) → SQL string
        → StageSplitter.split(relNode) → per-stage SQL strings
        → DataFusion worker dispatch
        → result merge on coordinator
```

This is the new path added for lakehouse queries. The `SqlProducer` (Component 2)
translates the Calcite `RelNode` tree into a SQL string targeting the DataFusion dialect. The Stage
Splitter (Component 4) partitions the plan into per-worker fragments based on Iceberg file
metadata. DataFusion workers (Component 5) execute the SQL fragments over Parquet files on S3.

The `LakehouseQueryRouter` (§4.3) selects Path C when the query targets a lakehouse index.

> **Note:** Substrait may be supported as a future optional serialization path alongside SQL.

---

## 4. What We Need to Build

The following new classes must be created. They integrate with the existing Unified Query
Framework without modifying it.

### 4.1 `LakehouseCalciteSchema`

A Calcite `Schema` implementation that resolves table names to Iceberg table metadata. This is
registered into `UnifiedQueryContext` via `.catalog("lakehouse", lakehouseSchema)`, making all
Iceberg tables visible to both SQL and PPL queries.

**Package:** `org.opensearch.lakehouse.schema`

**Role:** Bridge between Iceberg `Catalog` and Calcite's `Schema` interface. When Calcite needs
to resolve a table reference (e.g., `lakehouse.sales.orders`), it calls
`LakehouseCalciteSchema.getTable("orders")` which loads the Iceberg `TableMetadata` and wraps
it in a `LakehouseTable`.

### 4.2 `LakehouseTable`

A Calcite `Table` implementation backed by Iceberg table metadata. Used by `UnifiedQueryPlanner`
(both SQL and PPL paths) for type checking and plan generation.

**Package:** `org.opensearch.lakehouse.schema`

**Role:** Exposes Iceberg schema fields as Calcite `RelDataType`, enabling the SQL validator and
`CalciteRelNodeVisitor` to resolve column references and infer types. Does **not** read Parquet
files — it is metadata-only during planning.

Also exposes the underlying Iceberg `Table` object so that the `SqlProducer` can access
partition specs, sort orders, and S3 location metadata.

### 4.3 `LakehouseContextFactory`

Builds a fully configured `UnifiedQueryContext` for a lakehouse query execution.

**Package:** `org.opensearch.lakehouse`

**Role:** Encapsulates the logic for constructing `UnifiedQueryContext` with:
- An `LakehouseCalciteSchema` registered under the lakehouse catalog name
- The default namespace set from the query's target index (e.g., `"lakehouse.sales"`)
- The query type (PPL or SQL)
- Settings sourced from the cluster settings service

This factory is called by `LakehouseQueryRouter` when routing to Path C.

### 4.4 `LakehouseQueryRouter`

Detects whether an incoming query targets a lakehouse index and routes it to Path C instead
of the default path.

**Package:** `org.opensearch.lakehouse`

**Role:** Sits in the query dispatch layer (alongside the existing `QueryService`). Checks the
target index's `index.type` setting (from Component 00) to determine if it is a lakehouse
index. If yes, constructs a `UnifiedQueryContext` via `LakehouseContextFactory`, runs
`UnifiedQueryPlanner.plan()`, and passes the resulting `RelNode` to `SqlProducer`.

---

## 5. Java Interfaces for New Classes

### 5.1 `LakehouseCalciteSchema`

```java
package org.opensearch.lakehouse.schema;

import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.Table;
import org.apache.iceberg.catalog.Catalog;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A Calcite Schema backed by an Iceberg catalog. Resolves table names to
 * LakehouseTable instances that expose Iceberg metadata to Calcite's planner.
 *
 * Registered into UnifiedQueryContext via:
 *   .catalog("lakehouse", new LakehouseCalciteSchema(icebergCatalog, "sales"))
 *
 * Supports nested schemas: LakehouseCalciteSchema for a database/namespace
 * returns sub-schemas via getSubSchemaMap() if needed, or flattens tables
 * into one level when the namespace is fixed at construction time.
 */
public class LakehouseCalciteSchema extends AbstractSchema {

    private final Catalog icebergCatalog;

    /**
     * The Iceberg namespace (database) this schema represents.
     * All tables loaded by this schema belong to this namespace.
     */
    private final String namespace;

    /** Cache of already-loaded table metadata to avoid repeated catalog calls. */
    private final Map<String, LakehouseTable> tableCache = new ConcurrentHashMap<>();

    /**
     * @param icebergCatalog  the Iceberg catalog client (REST, Glue, Hive, etc.)
     * @param namespace       the Iceberg database/namespace (e.g., "sales")
     */
    public LakehouseCalciteSchema(Catalog icebergCatalog, String namespace) {
        this.icebergCatalog = icebergCatalog;
        this.namespace = namespace;
    }

    /**
     * Returns all tables in this namespace as Calcite Table objects.
     * Called by Calcite's schema resolution when enumerating tables.
     *
     * @return map of table name → LakehouseTable
     */
    @Override
    protected Map<String, Table> getTableMap() {
        // Load all table names from the Iceberg catalog namespace,
        // wrap each in a LakehouseTable, and return.
        // Implementation calls icebergCatalog.listTables(Namespace.of(namespace)).
        throw new UnsupportedOperationException("implement in LakehouseCalciteSchema");
    }

    /**
     * Loads a single table by name, with caching.
     *
     * @param name the table name (case-insensitive)
     * @return the LakehouseTable, or null if not found
     */
    @Override
    public Table getTable(String name) {
        return tableCache.computeIfAbsent(name, this::loadTable);
    }

    private LakehouseTable loadTable(String tableName) {
        // Calls icebergCatalog.loadTable(TableIdentifier.of(namespace, tableName))
        // Wraps in LakehouseTable.
        // Returns null if NoSuchTableException is thrown.
        throw new UnsupportedOperationException("implement in LakehouseCalciteSchema");
    }
}
```

### 5.2 `LakehouseTable`

```java
package org.opensearch.lakehouse.schema;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;

/**
 * A Calcite Table backed by Iceberg table metadata. Used by UnifiedQueryPlanner
 * for type resolution during both SQL (SqlValidator) and PPL
 * (CalciteRelNodeVisitor) planning.
 *
 * This class is metadata-only. It does not read any Parquet files.
 * Physical reading is performed by DataFusion workers via SQL.
 */
public class LakehouseTable extends AbstractTable {

    private final Table icebergTable;

    /**
     * @param icebergTable the loaded Iceberg table (from Catalog.loadTable())
     */
    public LakehouseTable(Table icebergTable) {
        this.icebergTable = icebergTable;
    }

    /**
     * Returns the Calcite row type derived from the Iceberg schema.
     * Called by Calcite's SqlValidator and CalciteRelNodeVisitor during
     * name and type resolution.
     *
     * Iceberg → Calcite type mapping:
     *   BooleanType   → BOOLEAN
     *   IntegerType   → INTEGER
     *   LongType      → BIGINT
     *   FloatType     → FLOAT
     *   DoubleType    → DOUBLE
     *   DecimalType   → DECIMAL(precision, scale)
     *   StringType    → VARCHAR
     *   BinaryType    → VARBINARY
     *   DateType      → DATE
     *   TimeType      → TIME
     *   TimestampType → TIMESTAMP WITH LOCAL TIME ZONE
     *   StructType    → ROW(fields)
     *   ListType      → ARRAY(element)
     *   MapType       → MAP(key, value)
     *   UUIDType      → CHAR(36)
     *
     * @param typeFactory the Calcite type factory
     * @return the row type for this table
     */
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (Types.NestedField field : icebergTable.schema().columns()) {
            builder.add(field.name(), toRelDataType(field.type(), typeFactory));
        }
        return builder.build();
    }

    /**
     * Returns the underlying Iceberg Table. Used by SqlProducer (Component 2)
     * to access partition specs, sort orders, snapshot IDs, and S3 location metadata.
     *
     * @return the Iceberg Table object
     */
    public Table getIcebergTable() {
        return icebergTable;
    }

    /**
     * Returns the fully-qualified table identifier for use in SQL table references.
     *
     * @return list of [namespace, tableName] parts
     */
    public java.util.List<String> getQualifiedName() {
        return java.util.List.of(
            icebergTable.name()  // already fully-qualified by Iceberg catalog
        );
    }

    private RelDataType toRelDataType(
            org.apache.iceberg.types.Type type,
            RelDataTypeFactory typeFactory) {
        // Map Iceberg type → Calcite RelDataType
        // Implementation uses type.typeId() switch
        throw new UnsupportedOperationException("implement in LakehouseTable");
    }
}
```

### 5.3 `LakehouseContextFactory`

```java
package org.opensearch.lakehouse;

import org.apache.iceberg.catalog.Catalog;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.lakehouse.schema.LakehouseCalciteSchema;

/**
 * Factory that constructs UnifiedQueryContext instances configured for lakehouse
 * query execution. Called by LakehouseQueryRouter for each incoming query.
 */
public class LakehouseContextFactory {

    private final Catalog icebergCatalog;

    /**
     * @param icebergCatalog the shared Iceberg catalog client (long-lived, thread-safe)
     */
    public LakehouseContextFactory(Catalog icebergCatalog) {
        this.icebergCatalog = icebergCatalog;
    }

    /**
     * Builds a UnifiedQueryContext for a lakehouse query.
     *
     * The context registers a LakehouseCalciteSchema for the given namespace
     * under the "lakehouse" catalog name, so that queries can reference tables
     * as either "orders" (unqualified) or "lakehouse.sales.orders" (fully qualified).
     *
     * @param queryType      PPL or SQL
     * @param catalogName    the Iceberg catalog/schema name (e.g., "lakehouse")
     * @param namespace      the Iceberg database to use as default (e.g., "sales")
     * @param profilingEnabled whether to enable query profiling
     * @return a configured UnifiedQueryContext (caller must close after use)
     */
    public UnifiedQueryContext createContext(
            QueryType queryType,
            String catalogName,
            String namespace,
            boolean profilingEnabled) {
        LakehouseCalciteSchema schema =
            new LakehouseCalciteSchema(icebergCatalog, namespace);

        return UnifiedQueryContext.builder()
            .language(queryType)
            .catalog(catalogName, schema)
            .defaultNamespace(catalogName + "." + namespace)
            .profiling(profilingEnabled)
            .build();
    }
}
```

### 5.4 `LakehouseQueryRouter`

```java
package org.opensearch.lakehouse;

import org.apache.calcite.rel.RelNode;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.UnifiedQueryPlanner;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.lakehouse.index.LakehouseIndexSettings;

/**
 * Routes incoming queries to the appropriate execution path based on whether
 * the target index is a lakehouse index.
 *
 * Lakehouse indices are identified by their index.type setting
 * (see Component 00: LakehouseIndexAbstraction).
 *
 * For lakehouse indices: parses query → RelNode → SqlProducer → DataFusion
 * For other indices: falls through to the existing query execution path
 */
public class LakehouseQueryRouter {

    private final LakehouseContextFactory contextFactory;
    private final LakehouseIndexSettings indexSettings;

    /**
     * @param contextFactory  creates UnifiedQueryContext with Iceberg schemas
     * @param indexSettings   reads index metadata to determine if an index is a lakehouse index
     */
    public LakehouseQueryRouter(
            LakehouseContextFactory contextFactory,
            LakehouseIndexSettings indexSettings) {
        this.contextFactory = contextFactory;
        this.indexSettings = indexSettings;
    }

    /**
     * Determines if a query targeting the given index should be routed to the
     * lakehouse execution path (Path C).
     *
     * @param indexName the target OpenSearch index name
     * @return true if the index is a lakehouse index
     */
    public boolean isLakehouseQuery(String indexName) {
        return indexSettings.isLakehouseIndex(indexName);
    }

    /**
     * Plans a lakehouse query into a Calcite RelNode.
     *
     * The returned RelNode can be passed directly to SqlProducer.toSql().
     *
     * @param queryString the raw PPL or SQL query text
     * @param queryType   PPL or SQL
     * @param indexName   the target index name, used to look up the Iceberg namespace
     * @return the planned RelNode (caller must close the returned context after SqlProducer)
     */
    public PlannedQuery plan(
            String queryString,
            QueryType queryType,
            String indexName) {
        String catalogName = indexSettings.getIcebergCatalogName(indexName);
        String namespace = indexSettings.getIcebergNamespace(indexName);

        UnifiedQueryContext context =
            contextFactory.createContext(queryType, catalogName, namespace, false);
        UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);
        RelNode relNode = planner.plan(queryString);

        return new PlannedQuery(relNode, context);
    }

    /**
     * Holds the result of planning: the logical plan and the context needed to
     * keep Calcite's resources alive during SqlProducer execution.
     * The context must be closed after the SqlProducer finishes.
     */
    public record PlannedQuery(RelNode relNode, UnifiedQueryContext context)
        implements AutoCloseable {

        @Override
        public void close() throws Exception {
            context.close();
        }
    }
}
```

---

## 6. Table Resolution via Calcite Schema Registration

The key insight is that `UnifiedQueryContext.builder().catalog(name, schema)` registers a
Calcite `Schema` into the root schema. Once registered, Calcite's name resolution
machinery (both SQL's `SqlValidator` and PPL's `CalciteRelNodeVisitor`) can look up
table references within it.

### How Name Resolution Works

```
Query: "SELECT * FROM orders"
  → default namespace: "lakehouse.sales"
  → Calcite resolves: rootSchema → "lakehouse" sub-schema → "sales" sub-schema → "orders" table
  → calls LakehouseCalciteSchema.getTable("orders")
  → calls icebergCatalog.loadTable(TableIdentifier.of("sales", "orders"))
  → wraps in LakehouseTable
  → Calcite calls LakehouseTable.getRowType(typeFactory) to get column types
  → name resolution succeeds; all column references type-checked against Iceberg schema
```

```
Query: "source=lakehouse.sales.orders | where amount > 100"
  → PPL parser produces UnresolvedPlan with Relation("lakehouse.sales.orders")
  → CalciteRelNodeVisitor resolves the 3-part name through the registered catalog hierarchy
  → same resolution path as SQL
```

Both SQL and PPL reach the **same** `LakehouseCalciteSchema.getTable()` call because
`UnifiedQueryContext` registers one schema structure shared by both planning strategies.

### Schema Hierarchy

```
rootSchema (CalciteSchema root)
  └── "lakehouse"  (LakehouseCalciteSchema for namespace "sales")
        ├── "orders"   → LakehouseTable(icebergTable: sales.orders)
        ├── "customers" → LakehouseTable(icebergTable: sales.customers)
        └── "products"  → LakehouseTable(icebergTable: sales.products)
```

The `defaultNamespace("lakehouse.sales")` setting means that unqualified table names like
`orders` resolve to `lakehouse.sales.orders` automatically.

---

## 7. Function Handling and SQL Function Mappings

### How Functions Work Today

`UnifiedFunctionRepository.loadFunctions()` discovers all PPL built-in operators by scanning
`PPLBuiltinOperators.instance().getOperatorList()`. During planning, `CalciteRelNodeVisitor`
translates PPL function calls directly to Calcite `RexNode` expressions using
`PPLFuncImpTable`.

For Path A (local execution), these `RexNode` expressions are compiled by Calcite's code
generator and run on the coordinator.

### What Changes for Path C (SQL)

When `SqlProducer` (Component 2) converts a `RelNode` to SQL, each `RexCall`
(function call) in the plan is mapped directly to SQL syntax via the DataFusion dialect.
No extension URIs or separate mapping files are needed — the mapping is handled by
`UnifiedQueryTranspiler` with the `DataFusionDialect`. The function inventory starts here.

The function names used in the `RelNode` are the SQL operator names from
`PPLBuiltinOperators` (e.g., `"UPPER"`, `"DATE_ADD"`, `"REGEXP_EXTRACT"`). Component 2 maps
these names directly to SQL function calls:

```
PPL function name  →  SQL syntax (DataFusion dialect)
────────────────────────────────────────────────────────────────
UPPER              →  UPPER()    (standard SQL, no URI needed)
ABS                →  ABS()      (standard SQL, no URI needed)
DATE_ADD           →  date_add() (DataFusion's SQL function)
REGEXP_EXTRACT     →  regexp_match() (DataFusion's regex support)
...
```

The mapping is handled by the `DataFusionDialect` in `UnifiedQueryTranspiler`, which
translates Calcite's `RexCall` nodes to the appropriate SQL syntax for DataFusion.

### Functions that Cannot Run on Workers

Some PPL functions are OpenSearch-specific (e.g., `query_string()`, `match()`,
`highlight()`) and cannot be expressed as SQL for DataFusion. The `SqlProducer` (Component 2)
will raise an error when it encounters these in a lakehouse plan.

The `LakehouseQueryRouter` can optionally pre-check for such functions during routing and
return a user-friendly error before planning proceeds.

---

## 8. Integration Flow: Query String to SQL Plan

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  OpenSearch Coordinator Node                                                │
│                                                                             │
│  1. HTTP request: POST /_plugins/_ppl                                       │
│     body: { "query": "source=orders | where amount > 100 | stats sum(amount) by region" }  │
│                          │                                                  │
│  2. LakehouseQueryRouter.isLakehouseQuery("orders")                         │
│     → reads index settings via LakehouseIndexSettings                       │
│     → index.type = "lakehouse" → YES, route to Path C                       │
│                          │                                                  │
│  3. LakehouseContextFactory.createContext(PPL, "lakehouse", "sales", false) │
│     → new LakehouseCalciteSchema(icebergCatalog, "sales")                   │
│     → UnifiedQueryContext.builder()                                         │
│         .language(PPL)                                                      │
│         .catalog("lakehouse", lakehouseCalciteSchema)                       │
│         .defaultNamespace("lakehouse.sales")                                │
│         .build()                                                            │
│                          │                                                  │
│  4. UnifiedQueryPlanner.plan(queryString)                                   │
│     → PPLQueryParser.parse(query)                                           │
│         → ANTLR: OpenSearchPPLLexer + OpenSearchPPLParser                   │
│         → AstBuilder.visit(parseTree)                                       │
│         → UnresolvedPlan                                                    │
│     → CalciteRelNodeVisitor.analyze(unresolvedPlan, planContext)             │
│         → visitRelation("orders")                                           │
│             → Calcite name resolution                                       │
│             → LakehouseCalciteSchema.getTable("orders")                     │
│             → icebergCatalog.loadTable("sales.orders")                      │
│             → LakehouseTable.getRowType() → column types                    │
│             → LogicalTableScan(table=LakehouseTable)                        │
│         → visitFilter(amount > 100) → LogicalFilter                        │
│         → visitAggregation(stats sum(amount) by region) → LogicalAggregate  │
│     → RelNode (fully resolved Calcite logical plan)                         │
│                          │                                                  │
│  5. SqlProducer.toSql(relNode, DataFusionDialect)   [Component 2]           │
│     → walk RelNode tree via UnifiedQueryTranspiler                          │
│     → LogicalTableScan → FROM sales.orders                                  │
│         → from LakehouseTable.getIcebergTable(): partition spec, S3 loc     │
│     → LogicalFilter → WHERE amount > 100                                    │
│     → LogicalAggregate → GROUP BY region / SUM(amount)                      │
│     → function calls → SQL syntax via DataFusion dialect                    │
│     → SQL string                                                            │
│                          │                                                  │
│  6. StageSplitter.split(relNode)                    [Component 4]           │
│     → Iceberg file listing for "sales.orders"                               │
│     → assign file ranges to worker nodes                                    │
│     → per-stage SQL strings                                                 │
│                          │                                                  │
│  7. Worker dispatch + result merge                  [Components 4, 5]       │
│     → DataFusion workers execute SQL over S3 Parquet files                  │
│     → Arrow IPC results streamed back to coordinator                        │
│     → coordinator aggregates partial results                                │
│     → response formatted to user                                            │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 9. What Does NOT Change

The following components are **reused without modification**:

| Component | Location | Role |
|---|---|---|
| PPL ANTLR grammar | `language-grammar/.../OpenSearchPPLParser.g4` | PPL syntax definition |
| SQL ANTLR grammar | `language-grammar/.../OpenSearchSQLParser.g4` | SQL syntax definition |
| PPL `AstBuilder` | `ppl/.../ppl/parser/AstBuilder.java` | ParseTree → UnresolvedPlan |
| SQL `AstBuilder` | `sql/.../sql/parser/AstBuilder.java` | ParseTree → UnresolvedPlan |
| `CalciteRelNodeVisitor` | `core/.../calcite/CalciteRelNodeVisitor.java` | UnresolvedPlan → RelNode (PPL path) |
| Calcite `SqlValidator` | Calcite library | SQL name/type resolution |
| Calcite `SqlToRelConverter` | Calcite library | SqlNode → RelNode (SQL path) |
| `PPLBuiltinOperators` | `core/.../expression/function/PPLBuiltinOperators.java` | All PPL function registrations |
| `PPLFuncImpTable` | `core/.../expression/function/PPLFuncImpTable.java` | PPL function RexNode resolution |
| `UnifiedQueryPlanner` | `api/.../UnifiedQueryPlanner.java` | Entry point: query → RelNode |
| `UnifiedQueryContext` | `api/.../UnifiedQueryContext.java` | Configuration + profiling container |
| `UnifiedQueryCompiler` | `api/.../compiler/UnifiedQueryCompiler.java` | Path A: RelNode → JDBC |
| `UnifiedQueryTranspiler` | `api/.../transpiler/UnifiedQueryTranspiler.java` | Path B: RelNode → SparkSQL |
| `UnifiedFunctionRepository` | `api/.../function/UnifiedFunctionRepository.java` | Function discovery |
| `UnifiedFunctionCalciteAdapter` | `api/.../function/UnifiedFunctionCalciteAdapter.java` | Function evaluation |
| Calcite optimization rules | `core/.../calcite/` | LogicalPlan optimization passes |

The lakehouse path adds a **new branch after** `UnifiedQueryPlanner.plan()` without touching
any of the above.

---

## 10. Integration with Component 00 (Lakehouse Index Abstraction)

Component 00 defines the `LakehouseIndexAbstraction` which stores Iceberg catalog settings
per OpenSearch index in the cluster state. `LakehouseQueryRouter` consumes this abstraction
through `LakehouseIndexSettings`.

### Settings Read by Component 1

| Index Setting | Used By | Purpose |
|---|---|---|
| `index.type` | `LakehouseQueryRouter.isLakehouseQuery()` | Determines if the index is a lakehouse index |
| `index.lakehouse.catalog.name` | `LakehouseContextFactory.createContext()` | Calcite catalog name to register (e.g., `"lakehouse"`) |
| `index.lakehouse.catalog.namespace` | `LakehouseContextFactory.createContext()` | Iceberg database/namespace (e.g., `"sales"`) |
| `index.lakehouse.catalog.type` | `LakehouseContextFactory` → `Catalog` construction | Catalog type: `rest`, `glue`, `hive`, `jdbc` |
| `index.lakehouse.catalog.uri` | Catalog construction | REST catalog URI, Metastore URI, etc. |

### Resolution Flow from Index Name to Iceberg Table

```
PPL query: "source=orders | ..."
                │
                ▼
LakehouseQueryRouter.isLakehouseQuery("orders")
    → IndexMetadataService.getSettings("orders")
    → index.type == "lakehouse" → true
                │
                ▼
LakehouseQueryRouter.plan("source=orders | ...", PPL, "orders")
    → indexSettings.getIcebergCatalogName("orders") → "lakehouse"
    → indexSettings.getIcebergNamespace("orders")   → "sales"
    → LakehouseContextFactory.createContext(PPL, "lakehouse", "sales", ...)
        → LakehouseCalciteSchema(icebergCatalog, "sales")
        → UnifiedQueryContext.builder()
              .catalog("lakehouse", lakehouseCalciteSchema)
              .defaultNamespace("lakehouse.sales")
              ...
                │
                ▼
    → UnifiedQueryPlanner.plan("source=orders | ...")
        → "orders" resolves via rootSchema["lakehouse"]["orders"]
        → LakehouseCalciteSchema.getTable("orders")
        → icebergCatalog.loadTable(TableIdentifier.of("sales", "orders"))
        → LakehouseTable wrapping the Iceberg Table
```

The OpenSearch index name (`orders`) serves as the **routing key** only. The actual Iceberg
table is identified by the `(namespace, tableName)` pair stored in the index settings, and
the table name used inside the query is the Iceberg table name — which by convention matches
the OpenSearch index name, but this is enforced by Component 00, not Component 1.

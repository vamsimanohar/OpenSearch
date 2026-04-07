# Component 2: SQL Producer (Plan Serialisation)

## Table of Contents

1. [Overview and Responsibilities](#1-overview-and-responsibilities)
2. [What Exists Today and Gaps for Distributed Lakehouse](#2-what-exists-today-and-gaps-for-distributed-lakehouse)
3. [Java Interfaces](#3-java-interfaces)
   - 3.1 [SqlProducer](#31-sqlproducer)
   - 3.2 [DataFusionDialect](#32-datafusiondialect)
   - 3.3 [SqlValidator](#33-sqlvalidator)
4. [Type Mapping: Calcite Types to DataFusion SQL Syntax](#4-type-mapping-calcite-types-to-datafusion-sql-syntax)
5. [Function Mapping: SQL Function Syntax](#5-function-mapping-sql-function-syntax)
6. [Relation Mapping: RelNode to SQL Clauses](#6-relation-mapping-relnode-to-sql-clauses)
7. [Table References and Iceberg Metadata](#7-table-references-and-iceberg-metadata)
8. [Validation Strategy](#8-validation-strategy)
9. [Integration with Upstream and Downstream](#9-integration-with-upstream-and-downstream)
10. [Coverage and Limitations](#10-coverage-and-limitations)
11. [Future: Substrait Path](#11-future-substrait-path)

---

## 1. Overview and Responsibilities

The SQL Producer is the serialisation bridge between the OpenSearch SQL/PPL query planner and the distributed DataFusion execution layer. It takes a fully resolved Calcite `RelNode` (optimised logical plan) produced by the query frontend and emits a **SQL string** that DataFusion can parse and execute natively. This reuses the existing `UnifiedQueryTranspiler` from the Unified Query Framework, configured with a DataFusion SQL dialect.

### Why SQL Instead of Substrait

The v1 architecture uses SQL as the primary plan serialisation format for several reasons:

- **Reuse**: The `UnifiedQueryTranspiler` already exists and handles RelNode-to-SQL conversion. Adding a DataFusion dialect is incremental work.
- **Simplicity**: SQL string generation avoids the complexity of mapping every type, function, and operator to Substrait proto equivalents. DataFusion's SQL parser handles the heavy lifting.
- **Debuggability**: SQL strings are human-readable, making it straightforward to inspect, log, and debug plans sent to workers.
- **Coverage**: SQL covers ~95-98% of query patterns needed for the distributed lakehouse. The remaining edge cases can be handled by a Substrait path when needed.

### Position in the System

```
SQL/PPL Query
      |
      v
+-----------------------+
|  Query Frontend       |  (Component 1)
|  SQL/PPL -> RelNode   |
|  (Calcite LogicalPlan)|
+-----------+-----------+
            |  RelNode (optimised)
            v
+-----------------------+
|  SQL Producer         |  (Component 2)  <-- THIS COMPONENT
|  RelNode ->           |
|  SQL String           |
|  (UnifiedQueryTranspiler + DataFusionDialect)
+-----------+-----------+
            |  String (SQL)
            v
+-----------------------+
|  Stage Splitter       |  (Component 4)
|  Plan -> ExecutionDAG |
+-----------------------+
            |
            v
  DataFusion Workers (Rust)
  Parse SQL string natively
  -> LogicalPlan -> PhysicalPlan -> Arrow stream
```

### Primary Responsibilities

1. **Plan translation** -- Convert a Calcite `RelNode` tree into a SQL string using `UnifiedQueryTranspiler.toSql(relNode, DataFusionDialect)`.
2. **Dialect handling** -- Use the `DataFusionDialect` to produce SQL syntax that DataFusion's parser accepts (function names, type casts, quoting rules, etc.).
3. **Type mapping** -- Map Calcite types to SQL type syntax that DataFusion understands (e.g., `BIGINT`, `DOUBLE`, `VARCHAR`, `TIMESTAMP`).
4. **Function mapping** -- Map Calcite function calls to SQL function syntax that DataFusion supports (e.g., `SUBSTRING()`, `COALESCE()`, `CAST()`, `DATE_TRUNC()`).
5. **Table resolution** -- Emit fully-qualified table names that DataFusion workers can resolve against their registered table providers.
6. **SQL validation** -- Verify the generated SQL is parseable and well-formed before handing it to the Stage Splitter or shipping to workers.

### Non-Responsibilities

- The SQL Producer does **not** optimise the logical plan (that is the optimiser's job upstream).
- It does **not** split the plan into stages (that is Component 4: Stage Splitter).
- It does **not** execute the plan or interact with DataFusion directly.
- It does **not** resolve table metadata from the catalog (it receives already-resolved `RelNode` trees from the query frontend).

---

## 2. What Exists Today and Gaps for Distributed Lakehouse

### What Exists Today

| Area | Current State |
|---|---|
| **UnifiedQueryTranspiler** | Exists in the Unified Query Framework. Supports converting Calcite RelNode trees to SQL strings for multiple dialects. A DataFusion-specific dialect needs to be added. |
| **SQL consumption (Rust)** | DataFusion natively parses SQL strings via its built-in SQL parser (`datafusion-sql`). No additional consumer library is needed -- DataFusion can execute SQL directly. |
| **DataFusion version** | DataFusion `52.1.0`. The SQL Producer must generate SQL that DataFusion 52.1.0's parser and planner accept. |
| **Type system** | `ExprCoreType` defines 17 core types. SQL type name mappings are straightforward (e.g., `INTEGER` -> `INT`, `TIMESTAMP` -> `TIMESTAMP`). |
| **Function system** | `BuiltinFunctionRepository` registers 100+ functions across 12 categories. Most map directly to standard SQL syntax that DataFusion supports. |
| **Wiring placeholder** | `DatafusionSearchExecEngine.java` has a `TODO` comment: `// TODO: wire Substrait conversion (RelNode -> Substrait bytes)`. This will be replaced with SQL generation. |

### Gaps for Distributed Lakehouse

| Gap | Impact | Solution |
|---|---|---|
| **No DataFusion SQL dialect** | Cannot generate DataFusion-compatible SQL from RelNode | Implement `DataFusionDialect` for `UnifiedQueryTranspiler` |
| **No SqlProducer wiring** | Nothing connects the query frontend to DataFusion workers | Implement `SqlProducer` that wraps `UnifiedQueryTranspiler` |
| **No SQL validation** | Malformed SQL sent to workers causes parse errors | Add `SqlValidator` that verifies generated SQL is parseable |
| **PPL-specific operators** | `LogicalDedupe`, `LogicalRareTopN`, `LogicalEval`, `LogicalTrendline` must be lowered to standard SQL before transpilation | Lower to equivalent SQL compositions (subqueries, window functions, CTEs) upstream in the Calcite plan |
| **Custom PPL functions** | `grok`, `cidr_match` have no SQL equivalent in DataFusion | Register as UDFs on DataFusion workers; emit `grok(...)` SQL syntax directly |
| **Iceberg metadata passing** | SQL strings cannot embed snapshot IDs or file manifests | Pass Iceberg metadata via `TaskRequest` proto fields alongside the SQL string |

### Comparison with Substrait Approach

| Aspect | SQL Path (v1) | Substrait Path (future) |
|---|---|---|
| **Implementation effort** | Low -- add dialect to existing transpiler | High -- build full producer from scratch |
| **Type mapping** | Simple SQL type names | Complex proto message construction |
| **Function mapping** | SQL syntax strings | Extension URI registration + YAML |
| **Debugging** | Read the SQL string | Decode proto bytes |
| **Coverage** | ~95-98% of query patterns | 100% (including complex window functions, nested subqueries) |
| **Dependencies** | None additional | `substrait-java-core` library |

---

## 3. Java Interfaces

All classes live under `org.opensearch.lakehouse.sql`.

### 3.1 SqlProducer

The top-level entry point. Converts a Calcite `RelNode` into a SQL string for DataFusion.

```java
package org.opensearch.lakehouse.sql;

import org.apache.calcite.rel.RelNode;

/**
 * Converts a Calcite RelNode (optimised logical plan) into a SQL string
 * that DataFusion can parse and execute.
 *
 * <p>Thread-safe: wraps the stateless UnifiedQueryTranspiler with a
 * DataFusion-specific dialect.
 */
public interface SqlProducer {

    /**
     * Convert a fully resolved RelNode into a DataFusion-compatible SQL string.
     *
     * @param relNode the root of the optimised Calcite logical plan
     * @return a SQL string that DataFusion can parse and execute
     * @throws SqlProducerException if any node in the plan cannot be converted to SQL
     */
    String toSql(RelNode relNode) throws SqlProducerException;
}
```

```java
package org.opensearch.lakehouse.sql;

import org.apache.calcite.rel.RelNode;

/**
 * Default implementation of SqlProducer.
 * Delegates to UnifiedQueryTranspiler with a DataFusionDialect.
 */
public class DefaultSqlProducer implements SqlProducer {

    private final UnifiedQueryTranspiler transpiler;
    private final DataFusionDialect dialect;
    private final SqlValidator validator;

    public DefaultSqlProducer(
            UnifiedQueryTranspiler transpiler,
            DataFusionDialect dialect,
            SqlValidator validator) {
        this.transpiler = transpiler;
        this.dialect = dialect;
        this.validator = validator;
    }

    @Override
    public String toSql(RelNode relNode) throws SqlProducerException {
        // 1. Transpile RelNode to SQL string using the DataFusion dialect
        String sql = transpiler.toSql(relNode, dialect);

        // 2. Validate the generated SQL is parseable
        validator.validate(sql);

        return sql;
    }
}
```

### 3.2 DataFusionDialect

Handles DataFusion-specific SQL syntax, including function names, type casting, identifier quoting, and operator precedence.

```java
package org.opensearch.lakehouse.sql;

import org.apache.calcite.sql.SqlDialect;

/**
 * SQL dialect configuration for DataFusion.
 * Controls how Calcite RelNode trees are rendered as SQL strings
 * that DataFusion's parser accepts.
 *
 * <p>Key DataFusion-specific behaviours:
 * <ul>
 *   <li>Identifiers are quoted with double quotes ("table"."column")</li>
 *   <li>String literals use single quotes ('value')</li>
 *   <li>CAST syntax: CAST(expr AS type)</li>
 *   <li>Date/time functions: DATE_TRUNC('unit', expr) not DATETRUNC</li>
 *   <li>Boolean literals: TRUE/FALSE (not 1/0)</li>
 *   <li>NULL handling: IS NULL, IS NOT NULL, COALESCE()</li>
 *   <li>Window frames: ROWS BETWEEN ... AND ...</li>
 * </ul>
 */
public class DataFusionDialect extends SqlDialect {

    public static final DataFusionDialect INSTANCE = new DataFusionDialect(
            EMPTY_CONTEXT
                    .withIdentifierQuoteString("\"")
                    .withDatabaseProduct(DatabaseProduct.UNKNOWN)
                    .withLiteralQuoteString("'")
    );

    private DataFusionDialect(Context context) {
        super(context);
    }

    @Override
    public boolean supportsAliasedValues() {
        return true;
    }

    @Override
    public boolean supportsWindowFrameTypes() {
        return true;
    }

    /**
     * Maps Calcite function names to DataFusion equivalents.
     * Most standard SQL functions pass through unchanged.
     * PPL-specific functions (grok, cidr_match) are emitted as-is,
     * assuming they are registered as UDFs on the DataFusion worker.
     */
    public String mapFunctionName(String calciteFunctionName) {
        return switch (calciteFunctionName.toUpperCase()) {
            case "CHARACTER_LENGTH", "CHAR_LENGTH" -> "LENGTH";
            case "SUBSTRING" -> "SUBSTR";
            case "POWER" -> "POWER";
            case "LN" -> "LN";
            case "LOG10" -> "LOG10";
            case "LOG2" -> "LOG2";
            case "APPROX_COUNT_DISTINCT" -> "APPROX_DISTINCT";
            case "PERCENTILE_APPROX" -> "APPROX_PERCENTILE_CONT";
            default -> calciteFunctionName;
        };
    }

    /**
     * Maps Calcite type names to DataFusion SQL type syntax.
     */
    public String mapTypeName(String calciteTypeName) {
        return switch (calciteTypeName.toUpperCase()) {
            case "TINYINT" -> "TINYINT";
            case "SMALLINT" -> "SMALLINT";
            case "INTEGER", "INT" -> "INT";
            case "BIGINT" -> "BIGINT";
            case "FLOAT", "REAL" -> "FLOAT";
            case "DOUBLE" -> "DOUBLE";
            case "VARCHAR", "CHAR", "CHARACTER VARYING" -> "VARCHAR";
            case "BOOLEAN" -> "BOOLEAN";
            case "DATE" -> "DATE";
            case "TIME" -> "TIME";
            case "TIMESTAMP" -> "TIMESTAMP";
            case "INTERVAL" -> "INTERVAL";
            case "BINARY", "VARBINARY" -> "BYTEA";
            default -> calciteTypeName;
        };
    }
}
```

### 3.3 SqlValidator

Validates that the generated SQL string is parseable by DataFusion's SQL grammar.

```java
package org.opensearch.lakehouse.sql;

/**
 * Validates a generated SQL string before it is sent to DataFusion workers.
 * Uses a lightweight SQL parser to verify the SQL is syntactically correct.
 */
public interface SqlValidator {

    /**
     * Validate that the given SQL string is syntactically valid.
     *
     * @param sql the generated SQL string
     * @throws SqlValidationException if the SQL is malformed or contains
     *         unsupported syntax for DataFusion
     */
    void validate(String sql) throws SqlValidationException;
}
```

```java
package org.opensearch.lakehouse.sql;

import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParseException;

/**
 * Default implementation that uses Calcite's SQL parser to verify syntax.
 * This provides a baseline check -- DataFusion may accept a superset of
 * what Calcite parses, but any SQL that Calcite rejects is likely invalid.
 */
public class DefaultSqlValidator implements SqlValidator {

    @Override
    public void validate(String sql) throws SqlValidationException {
        if (sql == null || sql.isBlank()) {
            throw new SqlValidationException("SQL string is null or empty");
        }

        try {
            SqlParser parser = SqlParser.create(sql);
            parser.parseStmt();
        } catch (SqlParseException e) {
            throw new SqlValidationException(
                    "Generated SQL failed parse validation: " + e.getMessage(), e);
        }

        // Size guard: reject excessively large SQL strings
        if (sql.length() > 1_000_000) {
            throw new SqlValidationException(
                    "Generated SQL exceeds 1 MB size limit (" + sql.length() + " chars)");
        }
    }
}
```

```java
package org.opensearch.lakehouse.sql;

public class SqlValidationException extends SqlProducerException {

    public SqlValidationException(String message) {
        super(message);
    }

    public SqlValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}
```

```java
package org.opensearch.lakehouse.sql;

public class SqlProducerException extends RuntimeException {

    public SqlProducerException(String message) {
        super(message);
    }

    public SqlProducerException(String message, Throwable cause) {
        super(message, cause);
    }
}
```

---

## 4. Type Mapping: Calcite Types to DataFusion SQL Syntax

Instead of mapping Calcite types to Substrait proto messages, the SQL path maps Calcite types to SQL type syntax strings that DataFusion understands. This is used when generating `CAST()` expressions and typed literals.

| OpenSearch ExprCoreType | Calcite Type | DataFusion SQL Type | Notes |
|---|---|---|---|
| `BYTE` | `TINYINT` | `TINYINT` | 8-bit signed integer |
| `SHORT` | `SMALLINT` | `SMALLINT` | 16-bit signed integer |
| `INTEGER` | `INTEGER` | `INT` | 32-bit signed integer |
| `LONG` | `BIGINT` | `BIGINT` | 64-bit signed integer |
| `FLOAT` | `FLOAT` | `FLOAT` | 32-bit IEEE 754 float |
| `DOUBLE` | `DOUBLE` | `DOUBLE` | 64-bit IEEE 754 double |
| `STRING` | `VARCHAR` | `VARCHAR` | UTF-8 variable-length string |
| `BOOLEAN` | `BOOLEAN` | `BOOLEAN` | |
| `DATE` | `DATE` | `DATE` | Days since Unix epoch |
| `TIME` | `TIME` | `TIME` | Time of day |
| `TIMESTAMP` | `TIMESTAMP` | `TIMESTAMP` | Microsecond precision, no timezone |
| `INTERVAL` | `INTERVAL` | `INTERVAL` | Day-time interval |
| `IP` | `VARCHAR` | `VARCHAR` | IP addresses stored as strings; max 39 chars for IPv6 |
| `GEO_POINT` | `ROW(DOUBLE, DOUBLE)` | `STRUCT(lat DOUBLE, lon DOUBLE)` | No native geo type in DataFusion |
| `BINARY` | `VARBINARY` | `BYTEA` | Variable-length bytes |
| `STRUCT` | `ROW(...)` | `STRUCT(...)` | Children populated from field schema |
| `ARRAY` | `ARRAY(...)` | `LIST(...)` | Element type from schema |
| `UNKNOWN` | -- | Unsupported | Throw `SqlProducerException` |
| `UNDEFINED` | -- | `NULL` | Emit `NULL` literal |

### CAST Examples in Generated SQL

```sql
-- Integer cast
CAST(price AS DOUBLE)

-- Timestamp literal
TIMESTAMP '2024-01-15 10:30:00'

-- Date literal
DATE '2024-01-15'

-- Boolean literal
TRUE

-- NULL with type
CAST(NULL AS VARCHAR)
```

---

## 5. Function Mapping: SQL Function Syntax

Instead of mapping functions to Substrait extension URIs, the SQL path maps Calcite function calls directly to SQL syntax that DataFusion supports. Most standard SQL functions pass through unchanged.

### Standard SQL Functions (Direct Mapping)

| Category | OpenSearch Function | DataFusion SQL | Notes |
|---|---|---|---|
| **Arithmetic** | `ABS(x)` | `ABS(x)` | Direct |
| | `CEIL(x)` | `CEIL(x)` | Direct |
| | `FLOOR(x)` | `FLOOR(x)` | Direct |
| | `SQRT(x)` | `SQRT(x)` | Direct |
| | `POWER(x, y)` | `POWER(x, y)` | Direct |
| | `LN(x)` | `LN(x)` | Direct |
| | `LOG10(x)` | `LOG10(x)` | Direct |
| | `LOG2(x)` | `LOG2(x)` | Direct |
| **Comparison** | `=`, `!=`, `<`, `>`, `<=`, `>=` | Same operators | Direct |
| | `IS NULL` | `IS NULL` | Direct |
| | `IS NOT NULL` | `IS NOT NULL` | Direct |
| | `IN (...)` | `IN (...)` | Direct |
| | `BETWEEN x AND y` | `BETWEEN x AND y` | Direct |
| **String** | `SUBSTRING(s, start, len)` | `SUBSTR(s, start, len)` | Name change |
| | `TRIM(s)` | `TRIM(s)` | Direct |
| | `UPPER(s)` | `UPPER(s)` | Direct |
| | `LOWER(s)` | `LOWER(s)` | Direct |
| | `CONCAT(a, b)` | `CONCAT(a, b)` | Direct |
| | `LENGTH(s)` | `LENGTH(s)` | Direct |
| | `LIKE` | `LIKE` | Direct |
| | `REGEXP` | `~ pattern` | DataFusion regex operator |
| **Date/Time** | `DATE_TRUNC(unit, ts)` | `DATE_TRUNC('unit', ts)` | Unit as string literal |
| | `YEAR(ts)` | `EXTRACT(YEAR FROM ts)` | Lowered to EXTRACT |
| | `MONTH(ts)` | `EXTRACT(MONTH FROM ts)` | Lowered to EXTRACT |
| | `DAY(ts)` | `EXTRACT(DAY FROM ts)` | Lowered to EXTRACT |
| | `HOUR(ts)` | `EXTRACT(HOUR FROM ts)` | Lowered to EXTRACT |
| | `MINUTE(ts)` | `EXTRACT(MINUTE FROM ts)` | Lowered to EXTRACT |
| | `SECOND(ts)` | `EXTRACT(SECOND FROM ts)` | Lowered to EXTRACT |
| | `NOW()` | `NOW()` | Direct |
| **Aggregate** | `COUNT(*)` | `COUNT(*)` | Direct |
| | `SUM(x)` | `SUM(x)` | Direct |
| | `AVG(x)` | `AVG(x)` | Direct |
| | `MIN(x)` | `MIN(x)` | Direct |
| | `MAX(x)` | `MAX(x)` | Direct |
| | `STDDEV(x)` | `STDDEV(x)` | Direct |
| | `VARIANCE(x)` | `VARIANCE(x)` | Direct |
| | `APPROX_COUNT_DISTINCT(x)` | `APPROX_DISTINCT(x)` | Name change |
| | `PERCENTILE_APPROX(x, p)` | `APPROX_PERCENTILE_CONT(x, p)` | Name change |
| **Window** | `ROW_NUMBER()` | `ROW_NUMBER()` | Direct |
| | `RANK()` | `RANK()` | Direct |
| | `DENSE_RANK()` | `DENSE_RANK()` | Direct |
| | `LEAD(x, n)` | `LEAD(x, n)` | Direct |
| | `LAG(x, n)` | `LAG(x, n)` | Direct |
| **Conditional** | `COALESCE(a, b)` | `COALESCE(a, b)` | Direct |
| | `CASE WHEN ... THEN ... END` | `CASE WHEN ... THEN ... END` | Direct |
| | `NULLIF(a, b)` | `NULLIF(a, b)` | Direct |

### Custom PPL Functions (UDF Mapping)

PPL-specific functions that have no SQL equivalent are registered as UDFs on DataFusion workers. The SQL Producer emits them as regular function calls:

| PPL Function | Generated SQL | Worker Requirement |
|---|---|---|
| `grok(pattern, input)` | `grok('pattern', input)` | Register `grok` UDF |
| `cidr_match(cidr, ip)` | `cidr_match('cidr', ip)` | Register `cidr_match` UDF |
| `ip_to_int(ip)` | `ip_to_int(ip)` | Register `ip_to_int` UDF |

---

## 6. Relation Mapping: RelNode to SQL Clauses

The `UnifiedQueryTranspiler` handles the mapping of Calcite `RelNode` tree nodes to SQL clauses. The `DataFusionDialect` adjusts syntax details. Below are representative examples of the SQL output for common plan patterns.

### 6.1 Table Scan

```
RelNode: LogicalTableScan(table=[logs_2024_01])
```

Generated SQL:

```sql
SELECT * FROM "logs_2024_01"
```

### 6.2 Filter

```
RelNode: LogicalFilter(condition=[AND(>(age, 21), =(country, 'US'))])
```

Generated SQL:

```sql
SELECT * FROM "users" WHERE "age" > 21 AND "country" = 'US'
```

### 6.3 Project

```
RelNode: LogicalProject(full_name=[$0], doubled_age=[*($1, 2)])
```

Generated SQL:

```sql
SELECT "name" AS "full_name", "age" * 2 AS "doubled_age" FROM "users"
```

### 6.4 Aggregation

```
RelNode: LogicalAggregate(group=[{status}], cnt=[COUNT()], avg_rt=[AVG(response_time)])
```

Generated SQL:

```sql
SELECT "status", COUNT(*) AS "cnt", AVG("response_time") AS "avg_rt"
FROM "requests"
GROUP BY "status"
```

### 6.5 Sort with Limit

```
RelNode: LogicalSort(sort=[timestamp ASC, score DESC], fetch=100)
```

Generated SQL:

```sql
SELECT * FROM "events"
ORDER BY "timestamp" ASC NULLS LAST, "score" DESC NULLS FIRST
LIMIT 100
```

### 6.6 Limit/Offset

```
RelNode: LogicalSort(offset=200, fetch=50)
```

Generated SQL:

```sql
SELECT * FROM "events" LIMIT 50 OFFSET 200
```

### 6.7 Window Function

```
RelNode: LogicalWindow(row_number OVER (PARTITION BY department ORDER BY salary))
```

Generated SQL:

```sql
SELECT *, ROW_NUMBER() OVER (
    PARTITION BY "department"
    ORDER BY "salary" ASC
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS "rn"
FROM "employees"
```

### 6.8 Join

```
RelNode: LogicalJoin(condition=[=(left.id, right.id)], joinType=[INNER])
```

Generated SQL:

```sql
SELECT * FROM "orders"
INNER JOIN "customers" ON "orders"."customer_id" = "customers"."id"
```

Join type mapping:

| OpenSearch JoinType | SQL Syntax |
|---|---|
| `INNER` | `INNER JOIN` |
| `LEFT` | `LEFT OUTER JOIN` |
| `RIGHT` | `RIGHT OUTER JOIN` |
| `FULL` | `FULL OUTER JOIN` |
| `CROSS` | `CROSS JOIN` |
| `LEFT_SEMI` | `LEFT SEMI JOIN` (DataFusion supports this) |
| `LEFT_ANTI` | `LEFT ANTI JOIN` (DataFusion supports this) |

### 6.9 Dedup (PPL) -- Lowered to SQL

PPL `dedup` is lowered to a window function + filter pattern before SQL generation:

```sql
-- dedup 1 by category
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY "category" ORDER BY "category") AS "__rn"
    FROM "events"
) WHERE "__rn" = 1

-- dedup 3 by category (allow 3 duplicates)
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY "category" ORDER BY "category") AS "__rn"
    FROM "events"
) WHERE "__rn" <= 3
```

### 6.10 Rare/Top (PPL) -- Lowered to SQL

PPL `rare`/`top` commands are lowered to aggregate + sort + limit:

```sql
-- rare 10 category by region
SELECT "category", "region", COUNT(*) AS "__count"
FROM "events"
GROUP BY "category", "region"
ORDER BY "__count" ASC
LIMIT 10

-- top 10 category by region
SELECT "category", "region", COUNT(*) AS "__count"
FROM "events"
GROUP BY "category", "region"
ORDER BY "__count" DESC
LIMIT 10
```

---

## 7. Table References and Iceberg Metadata

### Table Names in SQL

A `LogicalRelation` with `relationName = "logs_2024_01"` is emitted as a quoted identifier in the SQL:

```sql
SELECT * FROM "logs_2024_01"
```

If the table has a fully-qualified path (catalog, database, table), the SQL emits:

```sql
SELECT * FROM "opensearch_lakehouse"."default"."logs_2024_01"
```

### Iceberg Metadata: Out-of-Band via TaskRequest Proto

SQL strings cannot embed Iceberg snapshot IDs or file manifests. Instead, this metadata is passed alongside the SQL string in the `TaskRequest` proto:

```protobuf
message TaskRequest {
    // The query plan -- SQL string for v1, Substrait bytes for future
    oneof plan {
        string sql = 1;
        bytes substrait_plan = 2;
    }

    // Iceberg read metadata, passed alongside the plan
    IcebergReadContext iceberg_context = 3;
}

message IcebergReadContext {
    // Per-table Iceberg metadata
    map<string, TableIcebergMeta> table_metadata = 1;
}

message TableIcebergMeta {
    int64 snapshot_id = 1;
    int32 schema_version = 2;
    repeated DataFileEntry data_files = 3;
    ColumnStatistics column_statistics = 4;
}

message DataFileEntry {
    // S3 URI of the Parquet file (e.g. "s3://bucket/prefix/data/part-0001.parquet")
    string path = 1;
    int64 size_bytes = 2;
    int64 row_count = 3;
    map<string, string> partition_values = 4;
}

message ColumnStatistics {
    map<string, string> min_values = 1;
    map<string, string> max_values = 2;
    map<string, int64>  null_counts = 3;
    map<string, int64>  distinct_counts = 4;
}
```

### DataFusion Worker Consumption

On the Rust side, when a worker receives a `TaskRequest` with a SQL string:

1. Parse the SQL string using DataFusion's built-in SQL parser.
2. Extract `IcebergReadContext` from the `TaskRequest` proto.
3. For each table referenced in the SQL, look up the corresponding `TableIcebergMeta`.
4. Pre-populate DataFusion's list-files cache with the `DataFileEntry` list (replacing S3 listing).
5. Execute the resulting `LogicalPlan` -> `PhysicalPlan` -> Arrow stream.

---

## 8. Validation Strategy

Validation verifies that the generated SQL is syntactically correct and within size bounds before it is sent to workers.

### Validation Checks

| Check | What Is Validated | Error |
|---|---|---|
| **Non-empty SQL** | SQL string is not null or blank | "SQL string is null or empty" |
| **Parse check** | SQL parses successfully via Calcite SQL parser | "Generated SQL failed parse validation: ..." |
| **Size guard** | SQL string length <= 1 MB | "Generated SQL exceeds 1 MB size limit" |
| **Single statement** | SQL contains exactly one statement | "Expected single SQL statement, found multiple" |
| **No DDL** | SQL is a SELECT/query statement, not CREATE/DROP/ALTER | "DDL statements not allowed in distributed query path" |

### Additional Runtime Validation

DataFusion workers perform additional validation when they receive the SQL:

1. **Parse validation**: DataFusion's SQL parser rejects invalid syntax.
2. **Schema validation**: DataFusion's planner rejects references to non-existent tables or columns.
3. **Type validation**: DataFusion's type checker rejects type mismatches.

These runtime checks provide defence-in-depth beyond the pre-flight validation done by the SQL Producer.

---

## 9. Integration with Upstream and Downstream

### 9.1 Upstream: RelNode from Query Frontend

The SQL Producer receives a Calcite `RelNode` from Component 1 (Query Frontend). The plan is fully resolved and optimised.

**Contract the producer expects from the RelNode:**

| Property | Guarantee |
|---|---|
| All table references | Resolved, with schema available |
| All expressions | Type-checked with non-null types |
| All function calls | Registered in the function repository |
| No ML/AD nodes | Rejected upstream before lakehouse routing |
| No unresolved references | Analyser has run; no unresolved attributes |

**Integration point:**

```java
// In the Query Coordinator / execution router:
RelNode relNode = queryService.analyzeAndOptimize(query);  // Component 1 output

// Route to lakehouse path
if (isLakehouseQuery(relNode)) {
    String sql = sqlProducer.toSql(relNode);
    IcebergReadContext icebergCtx = catalogService.getIcebergContext(relNode);  // Component 3
    TaskRequest task = TaskRequest.newBuilder()
            .setSql(sql)
            .setIcebergContext(icebergCtx)
            .build();
    stageSplitter.split(task);  // Component 4
}
```

### 9.2 Downstream: Stage Splitter

The Stage Splitter (Component 4) receives the SQL string (and Iceberg context) from the producer. For the SQL path, stage splitting operates on the SQL string level:

**What the Stage Splitter expects:**

| Expectation | Guarantee from Producer |
|---|---|
| Valid, parseable SQL string | Enforced by SqlValidator |
| Single SELECT statement | Enforced by validator |
| Fully-qualified table names | Producer resolves all table references |
| DataFusion-compatible syntax | DataFusionDialect ensures compatibility |

**What the Stage Splitter does with the SQL:**

1. Parses the SQL to identify aggregation and join boundaries.
2. Splits the query at shuffle boundaries, producing sub-queries as SQL strings.
3. Each `StageFragment` contains a SQL string and associated Iceberg context, shipped to DataFusion workers via gRPC.

### 9.3 Downstream: DataFusion Workers (Rust)

Workers receive `TaskRequest` protos via gRPC (Component 7). The SQL path is simpler than the Substrait path:

```rust
// Extract SQL from TaskRequest
let sql = task_request.sql;

// Parse and execute natively
let logical_plan = ctx.state().create_logical_plan(&sql).await?;
let physical_plan = ctx.state().create_physical_plan(&logical_plan).await?;
let results = collect(physical_plan, ctx.task_ctx()).await?;
```

For custom PPL functions, the DataFusion worker must register UDFs at startup:

```rust
ctx.register_udf(create_grok_udf());
ctx.register_udf(create_cidr_match_udf());
ctx.register_udf(create_ip_to_int_udf());
```

### 9.4 Wiring in DatafusionSearchExecEngine

The current `DatafusionSearchExecEngine.prepare()` placeholder:

```java
// TODO: wire Substrait conversion (RelNode -> Substrait bytes)
byte[] substraitBytes = null;
```

Becomes:

```java
@Override
public void prepare(ExecutionContext requestContext) {
    RelNode relNode = requestContext.getRelNode();
    String sql = sqlProducer.toSql(relNode);
    IcebergReadContext icebergCtx = catalogService.getIcebergContext(relNode);
    TaskRequest task = TaskRequest.newBuilder()
            .setSql(sql)
            .setIcebergContext(icebergCtx)
            .build();
    datafusionContext.setTask(task);
}
```

### 9.5 Component Dependency Summary

```
Component 1 (Query Frontend)
  +-> RelNode (Calcite optimised logical plan)
        +-> Component 2 (SQL Producer)   <-- THIS COMPONENT
              +- uses: UnifiedQueryTranspiler, DataFusionDialect
              +- reads: RelNode, table schemas
              +-> SQL String
                    +-> Component 4 (Stage Splitter)
                          +-> StageFragment[] (SQL sub-queries + Iceberg context)
                                +-> Component 7 (gRPC Protocol -> Component 8 DataFusion Workers)
```

---

## 10. Coverage and Limitations

### SQL Path Coverage (~95-98%)

The SQL path covers the vast majority of query patterns:

- All standard SELECT/FROM/WHERE/GROUP BY/ORDER BY/LIMIT queries
- JOINs (inner, outer, cross, semi, anti)
- Subqueries (scalar, EXISTS, IN)
- Common table expressions (CTEs / WITH clauses)
- Standard aggregate functions (COUNT, SUM, AVG, MIN, MAX, STDDEV, VARIANCE)
- Basic window functions (ROW_NUMBER, RANK, DENSE_RANK, LEAD, LAG)
- UNION / INTERSECT / EXCEPT set operations
- CASE/WHEN/THEN conditional expressions
- CAST, COALESCE, NULLIF
- Standard string, math, and date/time functions

### Known Limitations (~2-5%)

The following patterns may require the Substrait path when it is added:

| Pattern | Limitation | Workaround |
|---|---|---|
| **Complex window frames** | Some advanced frame specifications may not transpile cleanly to SQL | Simplify frame to ROWS BETWEEN ... AND ... |
| **Deeply nested correlated subqueries** | Transpiler may produce incorrect SQL for triple-nested correlations | Rewrite as JOINs in the optimiser |
| **Custom aggregation phases** | Two-phase partial/final aggregation cannot be expressed in standard SQL | Stage Splitter handles this at the fragment level |
| **Advanced Substrait extensions** | Some future Substrait features (e.g., user-defined relations) have no SQL equivalent | Use Substrait path when available |

---

## 11. Future: Substrait Path

The Substrait serialisation path can be added as a second option alongside the SQL path. The `TaskRequest` proto already supports this via the `oneof plan` field:

```protobuf
oneof plan {
    string sql = 1;           // v1: SQL string (current)
    bytes substrait_plan = 2; // v2: Substrait plan bytes (future)
}
```

### When to Add Substrait

Substrait support should be added when:

1. Complex window functions or nested subqueries that cannot be expressed in SQL become common query patterns.
2. Two-phase aggregation needs to be encoded directly in the plan (rather than handled by the Stage Splitter).
3. Custom relational operators (beyond what SQL can express) are needed.
4. Performance profiling shows that SQL parsing on workers is a bottleneck (unlikely, but possible at extreme scale).

### Original Substrait Producer Design

The original design for a full Substrait producer is preserved below for reference. When the Substrait path is needed, this design can be implemented as an alternative to the SQL path.

#### Key Components

- **`SubstraitProducer`** -- Top-level interface converting `LogicalPlan` to serialised Substrait bytes.
- **`TypeMapper`** -- Maps every `ExprCoreType` to a Substrait `Type` proto message (I8, I16, I32, I64, FP32, FP64, String, Boolean, Date, Time, Timestamp, IntervalDay, FixedChar(39) for IP, Struct for GEO_POINT, Binary, List for ARRAY).
- **`FunctionMapper`** -- Maps OpenSearch function names to Substrait extension function references and URIs (arithmetic, comparison, string, datetime, aggregate extensions plus custom OpenSearch extension YAML for grok, cidr_match, etc.).
- **`RelationConverter`** -- `LogicalPlanNodeVisitor` implementation that walks the plan tree bottom-up, converting each node to its Substrait `Rel` equivalent (ReadRel, FilterRel, ProjectRel, AggregateRel, SortRel, FetchRel, WindowRel, JoinRel, etc.).
- **`SubstraitPlanValidator`** -- Validates the assembled Substrait plan before serialisation (checks for non-empty plan, declared extensions, ReadRel schemas, size limits, etc.).

#### Dependencies

```groovy
// build.gradle
implementation "io.substrait:substrait-java-core:0.36.0"  // generates substrait 0.62.x protos
```

#### Target Versions

- Substrait proto: `0.62.0`
- DataFusion: `52.1.0`
- `datafusion-substrait`: `52.1.0`

The Rust consumer side (`query_executor.rs`) already fully supports decoding Substrait bytes -> DataFusion LogicalPlan -> PhysicalPlan -> Arrow stream using `datafusion-substrait 52.1.0`. The Java producer is the missing piece that would be implemented when the Substrait path is enabled.

#### IcebergReadOptions in Substrait

When using the Substrait path, Iceberg metadata can be embedded directly in `ReadRel.advanced_extension.optimization` as a custom protobuf `IcebergReadOptions` message (containing snapshot_id, schema_version, data_files list, and column_statistics). This eliminates the need for out-of-band metadata passing that the SQL path requires.

---

*Document version: 2.0 | v1 plan format: SQL string | Future plan format: Substrait 0.62.0 | Target DataFusion version: 52.1.0*

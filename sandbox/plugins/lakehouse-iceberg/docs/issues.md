# Known Issues

## Open

### 1. EXPR$N column names for unnamed aggregates
**GitHub**: [opensearch-project/sql#5332](https://github.com/opensearch-project/sql/issues/5332)
**Severity**: Low (cosmetic)
**Component**: UnifiedQueryPlanner (dsl-query-executor)

When executing SQL queries with unnamed aggregate expressions, Calcite produces `EXPR$0`, `EXPR$1`, etc. instead of the original SQL expression text.

```sql
SELECT COUNT(*) FROM hits
-- Returns column name: EXPR$0
-- Expected: count(*) or count
```

Explicitly aliased expressions (`SELECT COUNT(*) AS cnt`) work correctly.

**Root cause**: `SqlToRelConverter` replaces unnamed expressions with `EXPR$N`. The original text is available in the `SqlNode` but lost during conversion to `RelNode`.

**Workaround**: Always use aliases in queries (`AS cnt`, `AS total`, etc.).

### 2. Q34/Q35 timeout on high-cardinality GROUP BY
**Severity**: Medium
**Component**: DataFusion execution

GROUP BY on URL column (~100M unique values) times out. DataFusion builds an unbounded hash table that exceeds practical limits.

```sql
SELECT "URL", COUNT(*) AS c FROM hits GROUP BY "URL" ORDER BY c DESC LIMIT 10
```

**Workaround**: None — requires DataFusion TopK optimization for high-cardinality GROUP BY.

### 3. Q36 OOM on TopK with high-cardinality clientip
**Severity**: Medium
**Component**: DataFusion execution

TopK operator cannot spill to disk for high-cardinality columns.

```sql
SELECT 1, "URL", COUNT(*) AS c FROM hits GROUP BY 1, "URL" ORDER BY c DESC LIMIT 10
```

**Workaround**: None — requires DataFusion spill support for TopK.

### 4. PPL Object[] cast in stats/rare/top commands
**Severity**: Medium
**Component**: Calcite Enumerable integration

12 PPL tests fail with `ClassCastException` when stats, rare, or top commands produce aggregated results through the Enumerable adapter.

**Root cause**: The Enumerable integration returns raw `Object[]` arrays that need special handling in the PPL result builder.

### 5. LIMIT before UNION not supported
**Severity**: Low
**Component**: SQL parser

4 SQL queries fail because the parser does not support `LIMIT` before `UNION` (e.g., `(SELECT ... LIMIT 10) UNION ALL (SELECT ... LIMIT 10)`).

### 6. Timestamp subtraction not supported
**Severity**: Low
**Component**: DataFusion SQL

`TIMESTAMP - TIMESTAMP` interval arithmetic is not supported in the current DataFusion SQL dialect.

## Resolved

_(None yet — issues will be moved here as they are fixed.)_

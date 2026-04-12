# DataFusion Memory Strategies & SQL Translation Report

## Memory Pool Strategies

The DataFusion Rust layer supports three memory pool strategies, configured via a sign convention in the memory pool limit parameter (`-Ddatafusion_memory_pool_limit_bytes`).

### 1. GreedyMemoryPool (default, `limit > 0`)

First-come-first-served memory allocation. All operators share a common pool with a fixed ceiling. When memory is exhausted, new allocations fail (triggering spill or OOM error).

**Best for**: Single-query OLAP workloads, benchmarks, predictable queries.

| Pros | Cons |
|------|------|
| Minimal overhead | One operator can starve others |
| Fast for single queries | No protection against runaway queries |
| No fairness arbitration delays | High-cardinality GROUP BY can OOM |

### 2. FairSpillPool (`limit < 0` via `datafusion.memory_pool_type=fair_spill`)

Fair memory allocation across operators. Each gets a proportional share. Operators spill to disk when exceeding their fair share. Wrapped in `TrackConsumersPool` with max 5 concurrent operators.

**Best for**: Concurrent queries, production environments, unpredictable workloads.

| Pros | Cons |
|------|------|
| Fair distribution across operators | Higher overhead than Greedy |
| Spill to disk prevents OOM on GROUP BY | Spill I/O degrades query perf |
| Protects against single-operator starvation | TopK still can't spill (DataFusion limitation) |

### 3. UnboundedMemoryPool (`limit == 0`)

`GreedyMemoryPool(usize::MAX)` — no tracking or limits. Operators allocate until OS OOM killer intervenes.

**Best for**: Development/testing only. NOT suitable for production.

| Pros | Cons |
|------|------|
| Zero allocation overhead | Crashes process on OOM |
| Maximum single-query perf | No protection at all |

### Configuration

```java
// DataFusionPlugin.java
// JVM system properties:
-Ddatafusion_memory_pool_limit_bytes=12884901888   // 12GB greedy
-Ddatafusion_memory_pool_type=fair_spill            // use FairSpillPool
-Ddatafusion_spill_memory_limit_bytes=107374182400  // 100GB spill disk
```

### Why 32GB Machine OOMs While datafusion-cli Doesn't

| Component | OpenSearch | datafusion-cli |
|-----------|-----------|----------------|
| JVM heap | 8GB | 0 |
| Arrow direct buffers | ~4GB | 0 |
| DataFusion Rust pool | 12GB | ~30GB |
| OS/kernel overhead | ~8GB | ~2GB |
| **Available for queries** | **12GB** | **~30GB** |

High-cardinality GROUP BY (e.g., Q34 URL column with ~100M unique values) builds a hash table that exceeds 12GB but fits in 30GB.

TopK operator (ORDER BY + LIMIT after GROUP BY) materializes the entire unsorted result before selecting top-K — it **cannot spill to disk** in current DataFusion versions.

### Recommended Configurations for 32GB Machine

**Config A: Single-query benchmark (max perf)**
```
-Xms4g -Xmx4g                                    # Reduce JVM to 4GB
-Ddatafusion_memory_pool_limit_bytes=21474836480  # 20GB greedy
-Ddatafusion_spill_memory_limit_bytes=107374182400
```
Rationale: JVM 4GB + Rust 20GB = 24GB, leaves 8GB for OS.

**Config B: Balanced (spill-safe)**
```
-Xms4g -Xmx4g
-Ddatafusion_memory_pool_type=fair_spill
-Ddatafusion_memory_pool_limit_bytes=17179869184  # 16GB fair_spill
-Ddatafusion_spill_memory_limit_bytes=107374182400
```

**Config C: Conservative (production-like)**
```
-Xms8g -Xmx8g
-Ddatafusion_memory_pool_type=fair_spill
-Ddatafusion_memory_pool_limit_bytes=12884901888  # 12GB fair_spill
-Ddatafusion_spill_memory_limit_bytes=53687091200  # 50GB spill
```

---

## SQL Dialect Translation Pipeline

### Flow

```
User SQL → REST (_lakehouse/sql)
  → Calcite parse → RelNode optimization
  → IcebergScanPlanner (file pruning via manifest predicates)
  → DataFusionSqlDialect (RelNode → DataFusion-compatible SQL)
  → Rust FFM bridge → DataFusion parse → physical plan → execute
  → Arrow batches → Java Object[] rows → JSON response
```

### Key Function Name Mappings

| Calcite Function | DataFusion Equivalent | Reason |
|-----------------|----------------------|--------|
| `SIGN(x)` | `SIGNUM(x)` | DataFusion naming |
| `TRUNCATE(x, d)` | `TRUNC(x)` | Different name |
| `MOD(a, b)` | `a % b` | Binary operator |
| `/INT(a, b)` | `a / b` | Calcite integer division syntax |
| `YEAR(x)` | `date_part('year', x)` | PostgreSQL-style extraction |
| `MONTH(x)` | `date_part('month', x)` | Same |
| `DATE(x)` | `CAST(x AS DATE)` | Explicit cast |
| `LENGTH(x)` | `CHAR_LENGTH(x)` | DataFusion preference |

### Predicate Pushdown (Dual Filtering)

1. **Iceberg level**: `CalciteToIcebergPredicateConverter` translates WHERE filters to Iceberg Expressions → file pruning via manifest column stats
2. **DataFusion level**: The same filter appears in the generated SQL → row-level filtering within selected files

Only filters **directly above TableScan** are pushed to Iceberg. HAVING filters above aggregates reference aliases that don't exist in the table schema → safely skipped.

Supported Iceberg predicates: `=`, `!=`, `>`, `<`, `>=`, `<=`, `AND`, `OR`, `NOT`, `IN`, `IS NULL`, `IS NOT NULL`. Unsupported → `alwaysTrue()` (safe fallback, no incorrect pruning).

### Identifier Handling

- DataFusion is **case-sensitive** with quoted identifiers
- `DataFusionSqlDialect` uses double quotes: `"ColumnName"`
- PPL wraps tables under `"opensearch"` schema → `stripSchemaQualifiers()` removes prefix
- Reserved words (`value`, `name`) must be quoted per Lex.MYSQL config

### Known Limitations

| Issue | Cause | Status |
|-------|-------|--------|
| TopK OOM | Can't spill, materializes all groups | Waiting on DataFusion PR |
| Q34/Q35 timeout | URL GROUP BY ~100M unique values | No workaround |
| LIMIT before UNION | Parser limitation | 4 queries affected |
| Timestamp subtraction | Not supported in DataFusion SQL | 1 query affected |
| EXPR$N column names | Calcite drops unnamed aggregate text | Use aliases |

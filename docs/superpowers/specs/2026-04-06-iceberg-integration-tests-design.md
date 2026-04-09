# Iceberg Integration Test Suite — Design Spec

> **Goal:** Build a comprehensive SQL + PPL integration test suite (245 tests) for Iceberg external tables, parametrized across S3/Glue and local filesystem backends.

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Test Data Strategy](#test-data-strategy)
4. [Test Infrastructure](#test-infrastructure)
5. [SQL Test Classes (124 tests)](#sql-test-classes-124-tests)
6. [PPL Test Classes (121 tests)](#ppl-test-classes-121-tests)
7. [Unsupported Operations (Lucene-Specific)](#unsupported-operations-lucene-specific)
8. [Error Handling & Backlog](#error-handling--backlog)
9. [Directory Layout](#directory-layout)

---

## Overview

The lakehouse-iceberg plugin enables SQL and PPL queries against Apache Iceberg tables via the analytics engine and DataFusion (Rust) backend. Both SQL and PPL converge on the same execution path:

```
SQL text → Calcite SqlParser → SqlNode → SqlValidator → RelNode
PPL text → UnifiedQueryPlanner → Calcite RelNode → PushDownPlanner
                                                          ↓
               Both paths → DefaultPlanExecutor.execute(RelNode)
                                      ↓ (detects ExternalTable)
               IcebergTableExecutor.prepareScan()
                                      ↓
               Substrait → JNI → Rust DataFusion → Arrow batches → JSON
```

This test suite validates that all standard SQL and PPL operations work correctly against Iceberg tables, covering the full execution pipeline end-to-end.

## Architecture

### Execution Paths

- **SQL**: `POST _analytics/sql` → `SqlQueryAction` REST handler → Calcite parse/validate/convert → `planExecutor.execute(relNode)` → DataFusion
- **PPL**: Transport action `UnifiedPPLExecuteAction` → `TestPPLTransportAction` → `UnifiedQueryService.execute(ppl)` → PushDownPlanner → `planExecutor.execute(relNode)` → DataFusion

Both use `SqlStdOperatorTable.instance()` for the function catalog (Calcite standard SQL functions).

### Key Constraints

- **All tests use `LIMIT`** to avoid scanning 3.7M rows on S3
- **PPL has no REST endpoint** — tests use transport action via `client().execute(UnifiedPPLExecuteAction.INSTANCE, request)`
- **SQL uses REST** — tests use `POST _analytics/sql` via HTTP client
- **`value` is a Calcite reserved keyword** — must be backtick-quoted

### Plugins Required

```java
nodePlugins() → List.of(
    LakehousePlugin.class,      // Iceberg catalog/table management
    AnalyticsPlugin.class,       // Analytics engine (Calcite, plan executor)
    DataFusionPlugin.class,      // DataFusion Rust backend
    DslQueryExecutorPlugin.class, // SQL REST handler
    TestPPLPlugin.class          // PPL transport action (test-only)
)
```

## Test Data Strategy

### S3/Glue (Primary — Integration Tests)

| Property | Value |
|----------|-------|
| Glue Database | `iceberg_benchmark_db` |
| Glue Table | `nyc_yellow_taxi_iceberg` |
| S3 Bucket | `s3://iceberg-benchmark-test-263689514295` |
| Region | `us-west-2` |
| Rows | 3,724,889 |
| Columns | 20 |

**Schema:**

| Column | Type | Nullable | Notes |
|--------|------|----------|-------|
| `vendorid` | INT | yes | 1 or 2 |
| `tpep_pickup_datetime` | TIMESTAMP | yes | |
| `tpep_dropoff_datetime` | TIMESTAMP | yes | |
| `passenger_count` | BIGINT | yes | 0-9 |
| `trip_distance` | DOUBLE | yes | |
| `ratecodeid` | BIGINT | yes | 1-6 |
| `store_and_fwd_flag` | STRING | yes | 'Y' or 'N' |
| `pulocationid` | INT | yes | 1-265 |
| `dolocationid` | INT | yes | 1-265 |
| `payment_type` | BIGINT | yes | 1-5 |
| `fare_amount` | DOUBLE | yes | |
| `extra` | DOUBLE | yes | |
| `mta_tax` | DOUBLE | yes | |
| `tip_amount` | DOUBLE | yes | |
| `tolls_amount` | DOUBLE | yes | |
| `improvement_surcharge` | DOUBLE | yes | |
| `total_amount` | DOUBLE | yes | |
| `congestion_surcharge` | DOUBLE | yes | |
| `airport_fee` | DOUBLE | yes | |
| `cbd_congestion_fee` | DOUBLE | yes | |

### Local Filesystem (Fallback — Unit/Offline Tests)

Generated at `@BeforeClass` time using the Iceberg Java SDK. Creates a local Iceberg table at `/tmp/iceberg-integ-test-warehouse` with the **same 20-column schema** as the NYC taxi table but only ~100 rows of synthetic data. This enables running the full test suite without AWS credentials.

## Test Infrastructure

### Base Test Class: `AbstractIcebergQueryIT`

Location: `sandbox/plugins/lakehouse-iceberg/src/internalClusterTest/java/org/opensearch/lakehouse/integration/`

Responsibilities:
- `@ClusterScope(scope = SUITE, numDataNodes = 1)` — single shared cluster for all tests
- `nodePlugins()` — loads all 5 required plugins
- `@BeforeClass` — generates local Iceberg test data (100 rows, 20-column NYC taxi schema)
- `setUp()` — registers catalog + table via cluster state update (Hadoop/local or Glue/S3 depending on parameter)
- **`executeSql(String sql)`** — sends `POST _analytics/sql`, parses JSON response, returns `QueryResult` (schema + rows)
- **`executePpl(String ppl)`** — `client().execute(UnifiedPPLExecuteAction.INSTANCE, new PPLRequest(ppl))`, returns `PPLResponse`
- **`assertResultNotEmpty(result)`** — verifies non-empty result
- **`assertColumnCount(result, n)`** — verifies column count
- **`assertRowCount(result, n)`** — verifies exact row count
- **`assertContainsColumn(result, name)`** — verifies column exists in schema
- Parameterized via `@ParametersFactory` over `{local, s3}` — but S3 tests are skipped when `AWS_PROFILE` is not set

### QueryResult DTO

```java
record QueryResult(
    String query,
    List<SchemaField> schema,
    List<Object[]> rows,
    int total
) {
    record SchemaField(String name, String type) {}
}
```

### SQL Table Name

- Local: `nyc_taxi` (registered pointing to local Hadoop catalog)
- S3: `nyc_taxi` (registered pointing to Glue catalog)

Both are registered under the same SQL name so queries are identical across backends.

### PPL Table Name

- PPL uses `source=nyc_taxi` (unqualified)

## SQL Test Classes (124 tests)

All SQL test classes extend `AbstractIcebergQueryIT` and live in:
`sandbox/plugins/lakehouse-iceberg/src/internalClusterTest/java/org/opensearch/lakehouse/integration/sql/`

### 1. `BasicSelectIT` (12 tests)

| # | Test | SQL |
|---|------|-----|
| 1 | `testSelectStar` | `SELECT * FROM nyc_taxi LIMIT 10` |
| 2 | `testSelectSpecificColumns` | `SELECT vendorid, trip_distance, total_amount FROM nyc_taxi LIMIT 10` |
| 3 | `testSelectWithAlias` | `SELECT vendorid AS vendor, trip_distance AS dist FROM nyc_taxi LIMIT 10` |
| 4 | `testSelectDistinct` | `SELECT DISTINCT vendorid FROM nyc_taxi LIMIT 10` |
| 5 | `testSelectWithLimit` | `SELECT * FROM nyc_taxi LIMIT 5` |
| 6 | `testSelectCountStar` | `SELECT COUNT(*) FROM nyc_taxi` |
| 7 | `testSelectLiteral` | `SELECT 1 AS one, 'hello' AS greeting FROM nyc_taxi LIMIT 1` |
| 8 | `testSelectExpression` | `SELECT fare_amount + tip_amount AS total_with_tip FROM nyc_taxi LIMIT 10` |
| 9 | `testSelectAllColumnsExplicit` | `SELECT vendorid, tpep_pickup_datetime, ..., cbd_congestion_fee FROM nyc_taxi LIMIT 5` |
| 10 | `testSelectWithTableAlias` | `SELECT t.vendorid, t.trip_distance FROM nyc_taxi t LIMIT 10` |
| 11 | `testSelectDistinctMultipleColumns` | `SELECT DISTINCT vendorid, payment_type FROM nyc_taxi LIMIT 20` |
| 12 | `testSelectWithNullHandling` | `SELECT vendorid, COALESCE(congestion_surcharge, 0) AS surcharge FROM nyc_taxi LIMIT 10` |

### 2. `WhereFilterIT` (15 tests)

| # | Test | SQL |
|---|------|-----|
| 1 | `testWhereEquals` | `SELECT * FROM nyc_taxi WHERE vendorid = 1 LIMIT 10` |
| 2 | `testWhereNotEquals` | `SELECT * FROM nyc_taxi WHERE vendorid <> 1 LIMIT 10` |
| 3 | `testWhereGreaterThan` | `SELECT * FROM nyc_taxi WHERE trip_distance > 10 LIMIT 10` |
| 4 | `testWhereLessThan` | `SELECT * FROM nyc_taxi WHERE fare_amount < 5 LIMIT 10` |
| 5 | `testWhereGreaterThanOrEqual` | `SELECT * FROM nyc_taxi WHERE passenger_count >= 5 LIMIT 10` |
| 6 | `testWhereLessThanOrEqual` | `SELECT * FROM nyc_taxi WHERE tip_amount <= 0 LIMIT 10` |
| 7 | `testWhereAnd` | `SELECT * FROM nyc_taxi WHERE vendorid = 1 AND trip_distance > 5 LIMIT 10` |
| 8 | `testWhereOr` | `SELECT * FROM nyc_taxi WHERE vendorid = 1 OR vendorid = 2 LIMIT 10` |
| 9 | `testWhereNot` | `SELECT * FROM nyc_taxi WHERE NOT vendorid = 1 LIMIT 10` |
| 10 | `testWhereIn` | `SELECT * FROM nyc_taxi WHERE payment_type IN (1, 2, 3) LIMIT 10` |
| 11 | `testWhereNotIn` | `SELECT * FROM nyc_taxi WHERE payment_type NOT IN (1, 2) LIMIT 10` |
| 12 | `testWhereBetween` | `SELECT * FROM nyc_taxi WHERE fare_amount BETWEEN 10 AND 50 LIMIT 10` |
| 13 | `testWhereIsNull` | `SELECT * FROM nyc_taxi WHERE congestion_surcharge IS NULL LIMIT 10` |
| 14 | `testWhereIsNotNull` | `SELECT * FROM nyc_taxi WHERE congestion_surcharge IS NOT NULL LIMIT 10` |
| 15 | `testWhereLike` | `SELECT * FROM nyc_taxi WHERE store_and_fwd_flag LIKE 'Y%' LIMIT 10` |

### 3. `AggregationIT` (18 tests)

| # | Test | SQL |
|---|------|-----|
| 1 | `testCount` | `SELECT COUNT(*) AS cnt FROM nyc_taxi` |
| 2 | `testCountColumn` | `SELECT COUNT(congestion_surcharge) AS cnt FROM nyc_taxi` |
| 3 | `testCountDistinct` | `SELECT COUNT(DISTINCT vendorid) AS cnt FROM nyc_taxi` |
| 4 | `testSum` | `SELECT SUM(total_amount) AS total FROM nyc_taxi` |
| 5 | `testAvg` | `SELECT AVG(trip_distance) AS avg_dist FROM nyc_taxi` |
| 6 | `testMin` | `SELECT MIN(fare_amount) AS min_fare FROM nyc_taxi` |
| 7 | `testMax` | `SELECT MAX(fare_amount) AS max_fare FROM nyc_taxi` |
| 8 | `testMultipleAggregations` | `SELECT COUNT(*), SUM(total_amount), AVG(trip_distance) FROM nyc_taxi` |
| 9 | `testGroupBy` | `SELECT vendorid, COUNT(*) AS cnt FROM nyc_taxi GROUP BY vendorid` |
| 10 | `testGroupByMultipleColumns` | `SELECT vendorid, payment_type, COUNT(*) FROM nyc_taxi GROUP BY vendorid, payment_type` |
| 11 | `testGroupByWithHaving` | `SELECT vendorid, COUNT(*) AS cnt FROM nyc_taxi GROUP BY vendorid HAVING COUNT(*) > 100` |
| 12 | `testGroupByWithOrderBy` | `SELECT vendorid, COUNT(*) AS cnt FROM nyc_taxi GROUP BY vendorid ORDER BY cnt DESC` |
| 13 | `testGroupByWithSum` | `SELECT payment_type, SUM(total_amount) AS total FROM nyc_taxi GROUP BY payment_type` |
| 14 | `testGroupByWithAvg` | `SELECT vendorid, AVG(fare_amount) AS avg_fare FROM nyc_taxi GROUP BY vendorid` |
| 15 | `testGroupByWithMinMax` | `SELECT vendorid, MIN(trip_distance), MAX(trip_distance) FROM nyc_taxi GROUP BY vendorid` |
| 16 | `testGroupByWithMultipleAggs` | `SELECT vendorid, COUNT(*), AVG(fare_amount), SUM(tip_amount) FROM nyc_taxi GROUP BY vendorid` |
| 17 | `testGroupByWithAlias` | `SELECT vendorid AS vendor, COUNT(*) AS trips FROM nyc_taxi GROUP BY vendorid` |
| 18 | `testHavingWithMultipleConditions` | `SELECT vendorid, COUNT(*) AS cnt, AVG(fare_amount) AS avg_f FROM nyc_taxi GROUP BY vendorid HAVING cnt > 100 AND avg_f > 10` |

### 4. `OrderByIT` (10 tests)

| # | Test | SQL |
|---|------|-----|
| 1 | `testOrderByAsc` | `SELECT * FROM nyc_taxi ORDER BY trip_distance ASC LIMIT 10` |
| 2 | `testOrderByDesc` | `SELECT * FROM nyc_taxi ORDER BY total_amount DESC LIMIT 10` |
| 3 | `testOrderByDefault` | `SELECT * FROM nyc_taxi ORDER BY fare_amount LIMIT 10` |
| 4 | `testOrderByMultipleColumns` | `SELECT * FROM nyc_taxi ORDER BY vendorid ASC, trip_distance DESC LIMIT 10` |
| 5 | `testOrderByAlias` | `SELECT vendorid, trip_distance AS dist FROM nyc_taxi ORDER BY dist DESC LIMIT 10` |
| 6 | `testOrderByExpression` | `SELECT *, fare_amount + tip_amount AS total_with_tip FROM nyc_taxi ORDER BY total_with_tip DESC LIMIT 10` |
| 7 | `testOrderByWithNulls` | `SELECT vendorid, congestion_surcharge FROM nyc_taxi ORDER BY congestion_surcharge LIMIT 20` |
| 8 | `testOrderByNullsFirst` | `SELECT vendorid, congestion_surcharge FROM nyc_taxi ORDER BY congestion_surcharge NULLS FIRST LIMIT 20` |
| 9 | `testOrderByNullsLast` | `SELECT vendorid, congestion_surcharge FROM nyc_taxi ORDER BY congestion_surcharge NULLS LAST LIMIT 20` |
| 10 | `testOrderByColumnOrdinal` | `SELECT vendorid, trip_distance FROM nyc_taxi ORDER BY 2 DESC LIMIT 10` |

### 5. `MathFunctionsIT` (14 tests)

| # | Test | SQL |
|---|------|-----|
| 1 | `testAbs` | `SELECT ABS(fare_amount) FROM nyc_taxi LIMIT 10` |
| 2 | `testCeil` | `SELECT CEIL(trip_distance) FROM nyc_taxi LIMIT 10` |
| 3 | `testFloor` | `SELECT FLOOR(trip_distance) FROM nyc_taxi LIMIT 10` |
| 4 | `testRound` | `SELECT ROUND(fare_amount, 1) FROM nyc_taxi LIMIT 10` |
| 5 | `testPower` | `SELECT POWER(trip_distance, 2) FROM nyc_taxi LIMIT 10` |
| 6 | `testSqrt` | `SELECT SQRT(trip_distance) FROM nyc_taxi WHERE trip_distance > 0 LIMIT 10` |
| 7 | `testMod` | `SELECT MOD(vendorid, 2) FROM nyc_taxi LIMIT 10` |
| 8 | `testLog` | `SELECT LN(trip_distance) FROM nyc_taxi WHERE trip_distance > 0 LIMIT 10` |
| 9 | `testLog10` | `SELECT LOG10(fare_amount) FROM nyc_taxi WHERE fare_amount > 0 LIMIT 10` |
| 10 | `testExp` | `SELECT EXP(1) FROM nyc_taxi LIMIT 1` |
| 11 | `testSign` | `SELECT SIGN(fare_amount) FROM nyc_taxi LIMIT 10` |
| 12 | `testTruncate` | `SELECT TRUNCATE(trip_distance, 1) FROM nyc_taxi LIMIT 10` |
| 13 | `testArithmeticExpressions` | `SELECT fare_amount + tip_amount, fare_amount - tip_amount, fare_amount * 1.1, fare_amount / 2 FROM nyc_taxi LIMIT 10` |
| 14 | `testModuloOperator` | `SELECT vendorid % 2 FROM nyc_taxi LIMIT 10` |

### 6. `StringFunctionsIT` (12 tests)

| # | Test | SQL |
|---|------|-----|
| 1 | `testUpper` | `SELECT UPPER(store_and_fwd_flag) FROM nyc_taxi LIMIT 10` |
| 2 | `testLower` | `SELECT LOWER(store_and_fwd_flag) FROM nyc_taxi LIMIT 10` |
| 3 | `testLength` | `SELECT LENGTH(store_and_fwd_flag) FROM nyc_taxi LIMIT 10` |
| 4 | `testCharLength` | `SELECT CHAR_LENGTH(store_and_fwd_flag) FROM nyc_taxi LIMIT 10` |
| 5 | `testTrim` | `SELECT TRIM(store_and_fwd_flag) FROM nyc_taxi LIMIT 10` |
| 6 | `testSubstring` | `SELECT SUBSTRING(store_and_fwd_flag, 1, 1) FROM nyc_taxi LIMIT 10` |
| 7 | `testConcat` | `SELECT CONCAT(store_and_fwd_flag, '-', CAST(vendorid AS VARCHAR)) FROM nyc_taxi LIMIT 10` |
| 8 | `testReplace` | `SELECT REPLACE(store_and_fwd_flag, 'Y', 'YES') FROM nyc_taxi LIMIT 10` |
| 9 | `testPosition` | `SELECT POSITION('Y' IN store_and_fwd_flag) FROM nyc_taxi LIMIT 10` |
| 10 | `testOverlay` | `SELECT OVERLAY(store_and_fwd_flag PLACING 'X' FROM 1 FOR 1) FROM nyc_taxi LIMIT 10` |
| 11 | `testInitcap` | `SELECT INITCAP(store_and_fwd_flag) FROM nyc_taxi LIMIT 10` |
| 12 | `testConcatWithOperator` | `SELECT store_and_fwd_flag || '-' || CAST(vendorid AS VARCHAR) FROM nyc_taxi LIMIT 10` |

### 7. `DateTimeFunctionsIT` (10 tests)

| # | Test | SQL |
|---|------|-----|
| 1 | `testExtractYear` | `SELECT EXTRACT(YEAR FROM tpep_pickup_datetime) FROM nyc_taxi LIMIT 10` |
| 2 | `testExtractMonth` | `SELECT EXTRACT(MONTH FROM tpep_pickup_datetime) FROM nyc_taxi LIMIT 10` |
| 3 | `testExtractDay` | `SELECT EXTRACT(DAY FROM tpep_pickup_datetime) FROM nyc_taxi LIMIT 10` |
| 4 | `testExtractHour` | `SELECT EXTRACT(HOUR FROM tpep_pickup_datetime) FROM nyc_taxi LIMIT 10` |
| 5 | `testExtractMinute` | `SELECT EXTRACT(MINUTE FROM tpep_pickup_datetime) FROM nyc_taxi LIMIT 10` |
| 6 | `testCurrentTimestamp` | `SELECT CURRENT_TIMESTAMP FROM nyc_taxi LIMIT 1` |
| 7 | `testCurrentDate` | `SELECT CURRENT_DATE FROM nyc_taxi LIMIT 1` |
| 8 | `testDateDiff` | `SELECT tpep_dropoff_datetime - tpep_pickup_datetime FROM nyc_taxi LIMIT 10` |
| 9 | `testGroupByDatePart` | `SELECT EXTRACT(HOUR FROM tpep_pickup_datetime) AS hr, COUNT(*) FROM nyc_taxi GROUP BY EXTRACT(HOUR FROM tpep_pickup_datetime) ORDER BY hr` |
| 10 | `testCastToDate` | `SELECT CAST(tpep_pickup_datetime AS DATE) FROM nyc_taxi LIMIT 10` |

### 8. `ConditionalIT` (10 tests)

| # | Test | SQL |
|---|------|-----|
| 1 | `testCaseWhen` | `SELECT CASE WHEN vendorid = 1 THEN 'CMT' WHEN vendorid = 2 THEN 'VTS' ELSE 'Other' END FROM nyc_taxi LIMIT 10` |
| 2 | `testCaseWhenInGroupBy` | `SELECT CASE WHEN vendorid = 1 THEN 'CMT' ELSE 'VTS' END AS vendor_name, COUNT(*) FROM nyc_taxi GROUP BY CASE WHEN vendorid = 1 THEN 'CMT' ELSE 'VTS' END` |
| 3 | `testCoalesce` | `SELECT COALESCE(congestion_surcharge, 0) FROM nyc_taxi LIMIT 10` |
| 4 | `testNullIf` | `SELECT NULLIF(vendorid, 1) FROM nyc_taxi LIMIT 10` |
| 5 | `testNestedCase` | `SELECT CASE WHEN trip_distance < 1 THEN 'short' WHEN trip_distance < 5 THEN 'medium' WHEN trip_distance < 10 THEN 'long' ELSE 'very_long' END FROM nyc_taxi LIMIT 20` |
| 6 | `testCaseWithAggregation` | `SELECT SUM(CASE WHEN tip_amount > 0 THEN 1 ELSE 0 END) AS tipped, COUNT(*) AS total FROM nyc_taxi` |
| 7 | `testCoalesceMultipleArgs` | `SELECT COALESCE(congestion_surcharge, airport_fee, 0) FROM nyc_taxi LIMIT 10` |
| 8 | `testCaseInWhereClause` | `SELECT * FROM nyc_taxi WHERE CASE WHEN vendorid = 1 THEN trip_distance ELSE 0 END > 5 LIMIT 10` |
| 9 | `testNullIfInAggregation` | `SELECT AVG(NULLIF(tip_amount, 0)) FROM nyc_taxi` |
| 10 | `testCaseWithNull` | `SELECT CASE WHEN congestion_surcharge IS NULL THEN 'missing' ELSE 'present' END FROM nyc_taxi LIMIT 10` |

### 9. `TypeCastIT` (8 tests)

| # | Test | SQL |
|---|------|-----|
| 1 | `testCastIntToDouble` | `SELECT CAST(vendorid AS DOUBLE) FROM nyc_taxi LIMIT 10` |
| 2 | `testCastDoubleToInt` | `SELECT CAST(trip_distance AS INTEGER) FROM nyc_taxi LIMIT 10` |
| 3 | `testCastToVarchar` | `SELECT CAST(vendorid AS VARCHAR) FROM nyc_taxi LIMIT 10` |
| 4 | `testCastTimestampToDate` | `SELECT CAST(tpep_pickup_datetime AS DATE) FROM nyc_taxi LIMIT 10` |
| 5 | `testCastStringToInt` | `SELECT CAST('123' AS INTEGER) FROM nyc_taxi LIMIT 1` |
| 6 | `testCastInExpression` | `SELECT CAST(vendorid AS DOUBLE) + 0.5 FROM nyc_taxi LIMIT 10` |
| 7 | `testCastBigintToInt` | `SELECT CAST(passenger_count AS INTEGER) FROM nyc_taxi LIMIT 10` |
| 8 | `testCastInWhere` | `SELECT * FROM nyc_taxi WHERE CAST(vendorid AS BIGINT) = 1 LIMIT 10` |

### 10. `SubqueryIT` (8 tests)

| # | Test | SQL |
|---|------|-----|
| 1 | `testScalarSubquery` | `SELECT *, (SELECT AVG(trip_distance) FROM nyc_taxi) AS avg_dist FROM nyc_taxi LIMIT 10` |
| 2 | `testInSubquery` | `SELECT * FROM nyc_taxi WHERE vendorid IN (SELECT DISTINCT vendorid FROM nyc_taxi WHERE trip_distance > 20) LIMIT 10` |
| 3 | `testExistsSubquery` | `SELECT * FROM nyc_taxi t WHERE EXISTS (SELECT 1 FROM nyc_taxi WHERE vendorid = t.vendorid AND trip_distance > 50) LIMIT 10` |
| 4 | `testDerivedTable` | `SELECT avg_fare FROM (SELECT vendorid, AVG(fare_amount) AS avg_fare FROM nyc_taxi GROUP BY vendorid) sub LIMIT 10` |
| 5 | `testSubqueryInFrom` | `SELECT sub.vendor, sub.cnt FROM (SELECT vendorid AS vendor, COUNT(*) AS cnt FROM nyc_taxi GROUP BY vendorid) sub ORDER BY sub.cnt DESC` |
| 6 | `testNestedSubqueries` | `SELECT * FROM (SELECT vendorid, trip_distance FROM nyc_taxi WHERE trip_distance > 5 LIMIT 100) sub WHERE sub.vendorid = 1 LIMIT 10` |
| 7 | `testSubqueryWithAggregation` | `SELECT vendorid, COUNT(*) FROM nyc_taxi WHERE fare_amount > (SELECT AVG(fare_amount) FROM nyc_taxi) GROUP BY vendorid` |
| 8 | `testCorrelatedSubquery` | `SELECT vendorid, fare_amount FROM nyc_taxi t WHERE fare_amount > (SELECT AVG(fare_amount) FROM nyc_taxi WHERE vendorid = t.vendorid) LIMIT 10` |

### 11. `WindowFunctionIT` (10 tests)

| # | Test | SQL |
|---|------|-----|
| 1 | `testRowNumber` | `SELECT vendorid, trip_distance, ROW_NUMBER() OVER (ORDER BY trip_distance DESC) AS rn FROM nyc_taxi LIMIT 20` |
| 2 | `testRank` | `SELECT vendorid, trip_distance, RANK() OVER (ORDER BY trip_distance DESC) AS rnk FROM nyc_taxi LIMIT 20` |
| 3 | `testDenseRank` | `SELECT vendorid, trip_distance, DENSE_RANK() OVER (ORDER BY trip_distance DESC) AS drnk FROM nyc_taxi LIMIT 20` |
| 4 | `testPartitionBy` | `SELECT vendorid, trip_distance, ROW_NUMBER() OVER (PARTITION BY vendorid ORDER BY trip_distance DESC) AS rn FROM nyc_taxi LIMIT 20` |
| 5 | `testSumOver` | `SELECT vendorid, fare_amount, SUM(fare_amount) OVER (PARTITION BY vendorid ORDER BY fare_amount) AS running_total FROM nyc_taxi LIMIT 20` |
| 6 | `testAvgOver` | `SELECT vendorid, fare_amount, AVG(fare_amount) OVER (PARTITION BY vendorid) AS avg_by_vendor FROM nyc_taxi LIMIT 20` |
| 7 | `testCountOver` | `SELECT vendorid, COUNT(*) OVER (PARTITION BY vendorid) AS vendor_count FROM nyc_taxi LIMIT 20` |
| 8 | `testLag` | `SELECT vendorid, trip_distance, LAG(trip_distance) OVER (ORDER BY trip_distance) AS prev_dist FROM nyc_taxi LIMIT 20` |
| 9 | `testLead` | `SELECT vendorid, trip_distance, LEAD(trip_distance) OVER (ORDER BY trip_distance) AS next_dist FROM nyc_taxi LIMIT 20` |
| 10 | `testNtile` | `SELECT vendorid, trip_distance, NTILE(4) OVER (ORDER BY trip_distance) AS quartile FROM nyc_taxi LIMIT 20` |

### 12. `SetOperationsIT` (5 tests)

| # | Test | SQL |
|---|------|-----|
| 1 | `testUnion` | `SELECT vendorid, trip_distance FROM nyc_taxi WHERE vendorid = 1 LIMIT 5 UNION SELECT vendorid, trip_distance FROM nyc_taxi WHERE vendorid = 2 LIMIT 5` |
| 2 | `testUnionAll` | `SELECT vendorid FROM nyc_taxi WHERE vendorid = 1 LIMIT 5 UNION ALL SELECT vendorid FROM nyc_taxi WHERE vendorid = 2 LIMIT 5` |
| 3 | `testIntersect` | `SELECT vendorid FROM nyc_taxi WHERE trip_distance > 10 LIMIT 10 INTERSECT SELECT vendorid FROM nyc_taxi WHERE fare_amount > 30 LIMIT 10` |
| 4 | `testExcept` | `SELECT vendorid FROM nyc_taxi LIMIT 10 EXCEPT SELECT vendorid FROM nyc_taxi WHERE vendorid = 1 LIMIT 10` |
| 5 | `testUnionWithAggregation` | `SELECT 'short' AS category, COUNT(*) FROM nyc_taxi WHERE trip_distance < 2 UNION ALL SELECT 'long', COUNT(*) FROM nyc_taxi WHERE trip_distance >= 10` |

### 13. `ComplexQueriesIT` (10 tests)

| # | Test | SQL |
|---|------|-----|
| 1 | `testGroupByWithHavingAndOrderBy` | `SELECT vendorid, COUNT(*) AS cnt, AVG(fare_amount) AS avg_fare FROM nyc_taxi GROUP BY vendorid HAVING COUNT(*) > 1000 ORDER BY avg_fare DESC` |
| 2 | `testNestedAggregations` | `SELECT AVG(cnt) AS avg_trips FROM (SELECT vendorid, COUNT(*) AS cnt FROM nyc_taxi GROUP BY vendorid) sub` |
| 3 | `testMultipleCaseInSelect` | `SELECT CASE WHEN vendorid = 1 THEN 'CMT' ELSE 'VTS' END AS vendor, CASE WHEN tip_amount > 0 THEN 'tipped' ELSE 'no_tip' END AS tip_status, COUNT(*) FROM nyc_taxi GROUP BY vendorid, CASE WHEN tip_amount > 0 THEN 'tipped' ELSE 'no_tip' END` |
| 4 | `testComplexWhereWithParentheses` | `SELECT * FROM nyc_taxi WHERE (vendorid = 1 AND trip_distance > 5) OR (vendorid = 2 AND fare_amount > 20) LIMIT 10` |
| 5 | `testWindowWithGroupBy` | `SELECT vendor, cnt, ROW_NUMBER() OVER (ORDER BY cnt DESC) AS rnk FROM (SELECT vendorid AS vendor, COUNT(*) AS cnt FROM nyc_taxi GROUP BY vendorid) sub` |
| 6 | `testSubqueryWithWindowFunction` | `SELECT * FROM (SELECT vendorid, trip_distance, ROW_NUMBER() OVER (PARTITION BY vendorid ORDER BY trip_distance DESC) AS rn FROM nyc_taxi LIMIT 100) sub WHERE rn <= 3` |
| 7 | `testMultipleSubqueries` | `SELECT vendorid, fare_amount, (SELECT AVG(fare_amount) FROM nyc_taxi) AS global_avg, (SELECT MAX(fare_amount) FROM nyc_taxi) AS global_max FROM nyc_taxi LIMIT 10` |
| 8 | `testComplexExpressionInGroupBy` | `SELECT CASE WHEN trip_distance < 2 THEN 'short' WHEN trip_distance < 10 THEN 'medium' ELSE 'long' END AS dist_bucket, COUNT(*), AVG(fare_amount) FROM nyc_taxi GROUP BY CASE WHEN trip_distance < 2 THEN 'short' WHEN trip_distance < 10 THEN 'medium' ELSE 'long' END` |
| 9 | `testDeepNestedSubquery` | `SELECT avg_fare FROM (SELECT vendorid, AVG(fare_amount) AS avg_fare FROM (SELECT * FROM nyc_taxi WHERE trip_distance > 1 LIMIT 1000) sub1 GROUP BY vendorid) sub2 ORDER BY avg_fare DESC` |
| 10 | `testAnalyticsStyleQuery` | `SELECT vendorid, payment_type, COUNT(*) AS trips, AVG(trip_distance) AS avg_dist, SUM(total_amount) AS revenue, AVG(tip_amount / NULLIF(total_amount, 0)) AS avg_tip_pct FROM nyc_taxi WHERE fare_amount > 0 GROUP BY vendorid, payment_type ORDER BY revenue DESC LIMIT 20` |

**Total SQL: 12 + 15 + 18 + 10 + 14 + 12 + 10 + 10 + 8 + 8 + 10 + 5 + 10 = 142**

## PPL Test Classes (121 tests)

All PPL test classes extend `AbstractIcebergQueryIT` and live in:
`sandbox/plugins/lakehouse-iceberg/src/internalClusterTest/java/org/opensearch/lakehouse/integration/ppl/`

PPL queries use the pattern: `source=nyc_taxi | <commands>`

### 1. `PplBasicCommandsIT` (15 tests)

| # | Test | PPL |
|---|------|-----|
| 1 | `testSourceOnly` | `source=nyc_taxi \| head 10` |
| 2 | `testFields` | `source=nyc_taxi \| fields vendorid, trip_distance, total_amount \| head 10` |
| 3 | `testFieldsRemove` | `source=nyc_taxi \| fields - congestion_surcharge, airport_fee \| head 10` |
| 4 | `testWhere` | `source=nyc_taxi \| where vendorid = 1 \| head 10` |
| 5 | `testWhereGreaterThan` | `source=nyc_taxi \| where trip_distance > 10 \| head 10` |
| 6 | `testWhereAnd` | `source=nyc_taxi \| where vendorid = 1 and trip_distance > 5 \| head 10` |
| 7 | `testWhereOr` | `source=nyc_taxi \| where vendorid = 1 or vendorid = 2 \| head 10` |
| 8 | `testWhereNot` | `source=nyc_taxi \| where not vendorid = 1 \| head 10` |
| 9 | `testWhereIn` | `source=nyc_taxi \| where payment_type in (1, 2, 3) \| head 10` |
| 10 | `testWhereBetween` | `source=nyc_taxi \| where fare_amount between 10 and 50 \| head 10` |
| 11 | `testWhereIsNull` | `source=nyc_taxi \| where isnull(congestion_surcharge) \| head 10` |
| 12 | `testWhereIsNotNull` | `source=nyc_taxi \| where isnotnull(congestion_surcharge) \| head 10` |
| 13 | `testWhereLike` | `source=nyc_taxi \| where like(store_and_fwd_flag, 'Y%') \| head 10` |
| 14 | `testHead` | `source=nyc_taxi \| head 5` |
| 15 | `testHeadDefault` | `source=nyc_taxi \| head` |

### 2. `PplSortIT` (10 tests)

| # | Test | PPL |
|---|------|-----|
| 1 | `testSortAsc` | `source=nyc_taxi \| sort trip_distance \| head 10` |
| 2 | `testSortDesc` | `source=nyc_taxi \| sort - trip_distance \| head 10` |
| 3 | `testSortMultipleFields` | `source=nyc_taxi \| sort vendorid, - trip_distance \| head 10` |
| 4 | `testSortWithFields` | `source=nyc_taxi \| sort - total_amount \| fields vendorid, total_amount \| head 10` |
| 5 | `testSortWithWhere` | `source=nyc_taxi \| where vendorid = 1 \| sort - trip_distance \| head 10` |
| 6 | `testSortNullsOrder` | `source=nyc_taxi \| sort congestion_surcharge \| head 20` |
| 7 | `testSortByExpression` | `source=nyc_taxi \| sort - fare_amount + tip_amount \| head 10` |
| 8 | `testSortWithAggregation` | `source=nyc_taxi \| stats count() as cnt by vendorid \| sort - cnt` |
| 9 | `testSortAfterStats` | `source=nyc_taxi \| stats avg(fare_amount) as avg_fare by payment_type \| sort avg_fare` |
| 10 | `testSortMultipleDesc` | `source=nyc_taxi \| sort - vendorid, - trip_distance \| head 10` |

### 3. `PplStatsIT` (18 tests)

| # | Test | PPL |
|---|------|-----|
| 1 | `testStatsCount` | `source=nyc_taxi \| stats count()` |
| 2 | `testStatsCountBy` | `source=nyc_taxi \| stats count() by vendorid` |
| 3 | `testStatsSum` | `source=nyc_taxi \| stats sum(total_amount)` |
| 4 | `testStatsSumBy` | `source=nyc_taxi \| stats sum(total_amount) by payment_type` |
| 5 | `testStatsAvg` | `source=nyc_taxi \| stats avg(trip_distance)` |
| 6 | `testStatsAvgBy` | `source=nyc_taxi \| stats avg(fare_amount) by vendorid` |
| 7 | `testStatsMin` | `source=nyc_taxi \| stats min(fare_amount)` |
| 8 | `testStatsMax` | `source=nyc_taxi \| stats max(fare_amount)` |
| 9 | `testStatsMinMax` | `source=nyc_taxi \| stats min(trip_distance) as min_dist, max(trip_distance) as max_dist` |
| 10 | `testStatsMultipleAggs` | `source=nyc_taxi \| stats count() as cnt, sum(total_amount) as total, avg(trip_distance) as avg_dist` |
| 11 | `testStatsMultipleGroupBy` | `source=nyc_taxi \| stats count() by vendorid, payment_type` |
| 12 | `testStatsCountDistinct` | `source=nyc_taxi \| stats dc(vendorid)` |
| 13 | `testStatsWithWhere` | `source=nyc_taxi \| where vendorid = 1 \| stats count() as cnt, avg(fare_amount) as avg_fare` |
| 14 | `testStatsWithAlias` | `source=nyc_taxi \| stats count() as total_trips by vendorid` |
| 15 | `testStatsMultipleAggsByMultipleFields` | `source=nyc_taxi \| stats count(), avg(fare_amount) by vendorid, payment_type` |
| 16 | `testStatsMaxBy` | `source=nyc_taxi \| stats max(total_amount) by vendorid` |
| 17 | `testStatsMinBy` | `source=nyc_taxi \| stats min(trip_distance) by payment_type` |
| 18 | `testStatsSumMultipleFields` | `source=nyc_taxi \| stats sum(fare_amount) as total_fare, sum(tip_amount) as total_tip by vendorid` |

### 4. `PplEvalIT` (12 tests)

| # | Test | PPL |
|---|------|-----|
| 1 | `testEvalArithmetic` | `source=nyc_taxi \| eval total_with_tip = fare_amount + tip_amount \| head 10` |
| 2 | `testEvalSubtraction` | `source=nyc_taxi \| eval net_fare = total_amount - tip_amount \| head 10` |
| 3 | `testEvalMultiplication` | `source=nyc_taxi \| eval fare_110pct = fare_amount * 1.1 \| head 10` |
| 4 | `testEvalDivision` | `source=nyc_taxi \| eval half_fare = fare_amount / 2 \| head 10` |
| 5 | `testEvalMultipleFields` | `source=nyc_taxi \| eval tip_pct = tip_amount / total_amount, fare_pct = fare_amount / total_amount \| head 10` |
| 6 | `testEvalWithFields` | `source=nyc_taxi \| eval tip_pct = tip_amount / total_amount \| fields vendorid, tip_pct \| head 10` |
| 7 | `testEvalWithWhere` | `source=nyc_taxi \| eval tip_pct = tip_amount / total_amount \| where tip_pct > 0.2 \| head 10` |
| 8 | `testEvalWithStats` | `source=nyc_taxi \| eval cost = fare_amount + tip_amount + tolls_amount \| stats avg(cost)` |
| 9 | `testEvalConcat` | `source=nyc_taxi \| eval label = concat(store_and_fwd_flag, '-', cast(vendorid as varchar)) \| head 10` |
| 10 | `testEvalAbs` | `source=nyc_taxi \| eval abs_fare = abs(fare_amount) \| head 10` |
| 11 | `testEvalCeil` | `source=nyc_taxi \| eval ceil_dist = ceil(trip_distance) \| head 10` |
| 12 | `testEvalFloor` | `source=nyc_taxi \| eval floor_dist = floor(trip_distance) \| head 10` |

### 5. `PplDedupIT` (6 tests)

| # | Test | PPL |
|---|------|-----|
| 1 | `testDedupSingleField` | `source=nyc_taxi \| dedup vendorid` |
| 2 | `testDedupMultipleFields` | `source=nyc_taxi \| dedup vendorid, payment_type` |
| 3 | `testDedupWithFields` | `source=nyc_taxi \| dedup vendorid \| fields vendorid, trip_distance` |
| 4 | `testDedupWithSort` | `source=nyc_taxi \| sort - trip_distance \| dedup vendorid \| head 5` |
| 5 | `testDedupKeepEmpty` | `source=nyc_taxi \| dedup vendorid keepempty=true \| head 10` |
| 6 | `testDedupConsecutive` | `source=nyc_taxi \| dedup 1 vendorid consecutive=true \| head 10` |

### 6. `PplRenameMathIT` (10 tests)

| # | Test | PPL |
|---|------|-----|
| 1 | `testRename` | `source=nyc_taxi \| rename vendorid as vendor \| head 10` |
| 2 | `testRenameMultiple` | `source=nyc_taxi \| rename vendorid as vendor, trip_distance as distance \| head 10` |
| 3 | `testMathAbs` | `source=nyc_taxi \| eval v = abs(fare_amount) \| head 10` |
| 4 | `testMathCeil` | `source=nyc_taxi \| eval v = ceil(trip_distance) \| head 10` |
| 5 | `testMathFloor` | `source=nyc_taxi \| eval v = floor(trip_distance) \| head 10` |
| 6 | `testMathRound` | `source=nyc_taxi \| eval v = round(fare_amount, 1) \| head 10` |
| 7 | `testMathSqrt` | `source=nyc_taxi \| where trip_distance > 0 \| eval v = sqrt(trip_distance) \| head 10` |
| 8 | `testMathPow` | `source=nyc_taxi \| eval v = pow(trip_distance, 2) \| head 10` |
| 9 | `testMathLog` | `source=nyc_taxi \| where trip_distance > 0 \| eval v = ln(trip_distance) \| head 10` |
| 10 | `testMathModulo` | `source=nyc_taxi \| eval v = vendorid % 2 \| head 10` |

### 7. `PplStringFunctionsIT` (10 tests)

| # | Test | PPL |
|---|------|-----|
| 1 | `testUpper` | `source=nyc_taxi \| eval v = upper(store_and_fwd_flag) \| head 10` |
| 2 | `testLower` | `source=nyc_taxi \| eval v = lower(store_and_fwd_flag) \| head 10` |
| 3 | `testLength` | `source=nyc_taxi \| eval v = length(store_and_fwd_flag) \| head 10` |
| 4 | `testTrim` | `source=nyc_taxi \| eval v = trim(store_and_fwd_flag) \| head 10` |
| 5 | `testSubstring` | `source=nyc_taxi \| eval v = substring(store_and_fwd_flag, 1, 1) \| head 10` |
| 6 | `testConcat` | `source=nyc_taxi \| eval v = concat(store_and_fwd_flag, '-test') \| head 10` |
| 7 | `testReplace` | `source=nyc_taxi \| eval v = replace(store_and_fwd_flag, 'Y', 'YES') \| head 10` |
| 8 | `testLtrim` | `source=nyc_taxi \| eval v = ltrim(store_and_fwd_flag) \| head 10` |
| 9 | `testRtrim` | `source=nyc_taxi \| eval v = rtrim(store_and_fwd_flag) \| head 10` |
| 10 | `testLike` | `source=nyc_taxi \| where like(store_and_fwd_flag, 'Y') \| head 10` |

### 8. `PplDateTimeFunctionsIT` (8 tests)

| # | Test | PPL |
|---|------|-----|
| 1 | `testYear` | `source=nyc_taxi \| eval yr = year(tpep_pickup_datetime) \| head 10` |
| 2 | `testMonth` | `source=nyc_taxi \| eval mo = month(tpep_pickup_datetime) \| head 10` |
| 3 | `testDay` | `source=nyc_taxi \| eval dy = dayofmonth(tpep_pickup_datetime) \| head 10` |
| 4 | `testHour` | `source=nyc_taxi \| eval hr = hour(tpep_pickup_datetime) \| head 10` |
| 5 | `testMinute` | `source=nyc_taxi \| eval mi = minute(tpep_pickup_datetime) \| head 10` |
| 6 | `testNow` | `source=nyc_taxi \| eval ts = now() \| head 1` |
| 7 | `testGroupByDatePart` | `source=nyc_taxi \| eval hr = hour(tpep_pickup_datetime) \| stats count() by hr \| sort hr` |
| 8 | `testDayOfWeek` | `source=nyc_taxi \| eval dow = dayofweek(tpep_pickup_datetime) \| head 10` |

### 9. `PplConditionalIT` (8 tests)

| # | Test | PPL |
|---|------|-----|
| 1 | `testIf` | `source=nyc_taxi \| eval label = if(vendorid = 1, 'CMT', 'VTS') \| head 10` |
| 2 | `testIfNull` | `source=nyc_taxi \| eval v = ifnull(congestion_surcharge, 0) \| head 10` |
| 3 | `testNullIf` | `source=nyc_taxi \| eval v = nullif(vendorid, 1) \| head 10` |
| 4 | `testIsNull` | `source=nyc_taxi \| where isnull(congestion_surcharge) \| head 10` |
| 5 | `testIsNotNull` | `source=nyc_taxi \| where isnotnull(congestion_surcharge) \| head 10` |
| 6 | `testCase` | `source=nyc_taxi \| eval label = case(vendorid = 1, 'CMT', vendorid = 2, 'VTS') \| head 10` |
| 7 | `testCoalesce` | `source=nyc_taxi \| eval v = coalesce(congestion_surcharge, airport_fee, 0) \| head 10` |
| 8 | `testNestedIf` | `source=nyc_taxi \| eval label = if(trip_distance < 1, 'short', if(trip_distance < 5, 'medium', 'long')) \| head 10` |

### 10. `PplTypeCastIT` (6 tests)

| # | Test | PPL |
|---|------|-----|
| 1 | `testCastToInt` | `source=nyc_taxi \| eval v = cast(trip_distance as integer) \| head 10` |
| 2 | `testCastToDouble` | `source=nyc_taxi \| eval v = cast(vendorid as double) \| head 10` |
| 3 | `testCastToString` | `source=nyc_taxi \| eval v = cast(vendorid as string) \| head 10` |
| 4 | `testCastInWhere` | `source=nyc_taxi \| where cast(vendorid as double) > 1.5 \| head 10` |
| 5 | `testCastInStats` | `source=nyc_taxi \| stats avg(cast(passenger_count as double))` |
| 6 | `testCastToDate` | `source=nyc_taxi \| eval v = cast(tpep_pickup_datetime as date) \| head 10` |

### 11. `PplRareTopIT` (8 tests)

| # | Test | PPL |
|---|------|-----|
| 1 | `testTop` | `source=nyc_taxi \| top vendorid` |
| 2 | `testTopN` | `source=nyc_taxi \| top 3 payment_type` |
| 3 | `testTopByField` | `source=nyc_taxi \| top 3 payment_type by vendorid` |
| 4 | `testRare` | `source=nyc_taxi \| rare payment_type` |
| 5 | `testRareByField` | `source=nyc_taxi \| rare payment_type by vendorid` |
| 6 | `testTopWithWhere` | `source=nyc_taxi \| where trip_distance > 5 \| top 3 vendorid` |
| 7 | `testRareWithWhere` | `source=nyc_taxi \| where fare_amount > 50 \| rare vendorid` |
| 8 | `testTopWithFields` | `source=nyc_taxi \| top 3 vendorid \| fields vendorid` |

### 12. `PplComplexPipelinesIT` (10 tests)

| # | Test | PPL |
|---|------|-----|
| 1 | `testWhereEvalFields` | `source=nyc_taxi \| where vendorid = 1 \| eval tip_pct = tip_amount / total_amount \| fields vendorid, tip_pct \| head 10` |
| 2 | `testStatsSort` | `source=nyc_taxi \| stats count() as cnt by vendorid \| sort - cnt` |
| 3 | `testWhereStatsSort` | `source=nyc_taxi \| where trip_distance > 5 \| stats count() as cnt, avg(fare_amount) as avg_fare by vendorid \| sort - avg_fare` |
| 4 | `testEvalStatsGroupBy` | `source=nyc_taxi \| eval cost = fare_amount + tip_amount \| stats avg(cost) as avg_cost by vendorid` |
| 5 | `testMultipleEvals` | `source=nyc_taxi \| eval tip_pct = tip_amount / total_amount \| eval is_tipper = if(tip_pct > 0.15, 'generous', 'normal') \| fields vendorid, tip_pct, is_tipper \| head 10` |
| 6 | `testRenameFieldsSort` | `source=nyc_taxi \| rename vendorid as vendor, trip_distance as dist \| fields vendor, dist \| sort - dist \| head 10` |
| 7 | `testWhereStatsDedupSort` | `source=nyc_taxi \| stats avg(fare_amount) as avg_fare by vendorid, payment_type \| dedup vendorid \| sort - avg_fare` |
| 8 | `testEvalWhereStats` | `source=nyc_taxi \| eval total_cost = fare_amount + tip_amount + tolls_amount \| where total_cost > 50 \| stats count() as cnt by vendorid` |
| 9 | `testStatsEvalSort` | `source=nyc_taxi \| stats sum(fare_amount) as total_fare, sum(tip_amount) as total_tip by vendorid \| eval tip_ratio = total_tip / total_fare \| sort - tip_ratio` |
| 10 | `testComplexPipeline` | `source=nyc_taxi \| where trip_distance > 1 \| eval tip_pct = tip_amount / total_amount \| stats avg(tip_pct) as avg_tip_pct, count() as cnt by vendorid \| sort - avg_tip_pct` |

**Total PPL: 15 + 10 + 18 + 12 + 6 + 10 + 10 + 8 + 8 + 6 + 8 + 10 = 121**

## Unsupported Operations (Lucene-Specific)

These operations rely on Lucene index internals and are NOT expected to work against Iceberg external tables. They should be documented in a separate `UNSUPPORTED_OPERATIONS.md` file.

### Full-Text Search Functions
- `MATCH` / `MATCH_PHRASE` / `MATCH_PHRASE_PREFIX` / `MATCH_BOOL_PREFIX`
- `MULTI_MATCH` / `SIMPLE_QUERY_STRING` / `QUERY_STRING`
- `MATCH_QUERY` / `WILDCARD_QUERY`
- PPL: `match()`, `match_phrase()`, `match_bool_prefix()`, `multi_match()`, `simple_query_string()`, `query_string()`

### Relevance Scoring
- `SCORE` / `SCOREQUERY` / `_score`
- `HIGHLIGHT` / highlighting

### Index-Specific Operations
- `_id`, `_version`, `_routing`, `_index` pseudo-columns
- `NESTED` / nested field queries
- `geoip()` / geo functions depending on index mappings
- `cidrmatch()` (IP-specific)

### DDL/DML (Iceberg is read-only)
- `CREATE` / `DROP` / `ALTER`
- `INSERT` / `UPDATE` / `DELETE`
- `DESCRIBE` / `SHOW TABLES`

### OpenSearch-Specific PPL Commands
- `trendline` (OpenSearch ML extension)
- `AD` / `ML` commands (anomaly detection, ML)
- `patterns` (log pattern detection)
- `expand` (multi-value field expansion)

## Error Handling & Backlog

Tests that fail due to unsupported features in the Calcite/DataFusion pipeline (not Lucene-specific) should be:

1. Annotated with `@AwaitsFix(bugUrl = "link-to-issue")` or `@Ignore("reason")`
2. Added to `BACKLOG.md` with the error message and suggested fix
3. NOT removed from the test suite — they document expected behavior

## Directory Layout

```
sandbox/plugins/lakehouse-iceberg/
├── src/
│   ├── internalClusterTest/java/org/opensearch/lakehouse/integration/
│   │   ├── SingleNodeIcebergIT.java        (existing — cluster state tests)
│   │   ├── AbstractIcebergQueryIT.java      (NEW — base class)
│   │   ├── sql/
│   │   │   ├── BasicSelectIT.java           (12 tests)
│   │   │   ├── WhereFilterIT.java           (15 tests)
│   │   │   ├── AggregationIT.java           (18 tests)
│   │   │   ├── OrderByIT.java               (10 tests)
│   │   │   ├── MathFunctionsIT.java         (14 tests)
│   │   │   ├── StringFunctionsIT.java       (12 tests)
│   │   │   ├── DateTimeFunctionsIT.java     (10 tests)
│   │   │   ├── ConditionalIT.java           (10 tests)
│   │   │   ├── TypeCastIT.java              (8 tests)
│   │   │   ├── SubqueryIT.java              (8 tests)
│   │   │   ├── WindowFunctionIT.java        (10 tests)
│   │   │   ├── SetOperationsIT.java         (5 tests)
│   │   │   └── ComplexQueriesIT.java        (10 tests)
│   │   └── ppl/
│   │       ├── PplBasicCommandsIT.java      (15 tests)
│   │       ├── PplSortIT.java               (10 tests)
│   │       ├── PplStatsIT.java              (18 tests)
│   │       ├── PplEvalIT.java               (12 tests)
│   │       ├── PplDedupIT.java              (6 tests)
│   │       ├── PplRenameMathIT.java         (10 tests)
│   │       ├── PplStringFunctionsIT.java    (10 tests)
│   │       ├── PplDateTimeFunctionsIT.java  (8 tests)
│   │       ├── PplConditionalIT.java        (8 tests)
│   │       ├── PplTypeCastIT.java           (6 tests)
│   │       ├── PplRareTopIT.java            (8 tests)
│   │       └── PplComplexPipelinesIT.java   (10 tests)
│   └── dataGen/  (existing)
├── UNSUPPORTED_OPERATIONS.md                (NEW)
└── BACKLOG.md                               (NEW — populated as tests fail)
```

**Grand Total: 142 SQL + 121 PPL = 263 tests**

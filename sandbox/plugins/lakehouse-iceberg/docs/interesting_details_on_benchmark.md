# Interesting Details on Benchmark Behavior

## General Observations

### S3 Data Download: No Caching, Every Query Pays Full Cost

Each query re-downloads **all** Parquet data from S3. There is no local file cache.

**The path:**
1. Java (`WorkerQueryExecutor`) resolves AWS credentials per-query
2. FFM bridge passes S3 region, bucket, credentials, file paths, SQL to Rust
3. Rust (`api.rs`) builds a **fresh `AmazonS3` object store per query** — no reuse across queries
4. DataFusion's `ParquetExec` calls `ObjectStore::get_range()` for each row group
5. `CrossRuntimeObjectStore` bridges CPU executor threads to the async IO runtime for S3 fetches

**What IS cached (metadata only):**
- Parquet file footer statistics (schema, row counts, min/max per column)
- File listing metadata

**What is NOT cached:**
- Parquet row data — downloaded fresh every query
- Arrow batches — streamed, never persisted to disk
- S3 client — new one per query, dropped after completion

This means there is **no warm/cold distinction** for data reads. Every query pays the full S3 network cost.

### Cascade OOM: Why Sequential Benchmark Runs Fail

DataFusion uses a **greedy memory pool** (20GB per node). Heavy queries (e.g., Q17/Q24 with large GROUP BY) can exhaust the pool. The pool does **not** release memory back after a query completes if internal allocations fragment. All subsequent queries fail with `Resources exhausted` until OpenSearch is restarted.

### Distributed Merge Strategies (Phase 1 — `analytics-dwh-engine`)

| Strategy | When Used | How It Works |
|---|---|---|
| **GLOBAL_MERGE** | Global agg, no GROUP BY, only SUM/COUNT/MIN/MAX (no AVG, no DISTINCT) | Each worker returns 1 row; coordinator combines (SUM of COUNTs, MIN of MINs, etc.) |
| **TOPK_MERGE** | ORDER BY + LIMIT, no aggregation | Each worker returns local top-K; coordinator merge-sorts for global top-K |
| **CONCAT** | Simple scan/filter/project, no agg, no sort | Row concatenation from all workers |
| **SINGLE_NODE** | GROUP BY, COUNT DISTINCT, AVG, or any non-trivially distributable pattern | Routes entire query to one node |

---

## Per-Query Details

### Q1: `SELECT COUNT(*) FROM hits`
- **Strategy**: GLOBAL_MERGE
- **What it tests**: Full table row count
- **Detail**: DataFusion's `aggregate_statistics` optimizer uses Parquet footer metadata (`num_rows`) instead of scanning actual rows. This means COUNT(*) is effectively a metadata-only operation — it reads ~1-10KB of footer per file, not the actual data. DataFusion CLI returns in 0.16s. Our OpenSearch path takes ~3.9s but most of that is Glue catalog, Iceberg manifest parsing, credential resolution, and distributed coordination — the actual DataFusion JNI execution is only 341ms.
- **Implication**: COUNT(*) is not a meaningful benchmark for data throughput.

### Q2: `SELECT COUNT(*) FROM hits WHERE advengineid <> 0`
- **Strategy**: GLOBAL_MERGE
- **What it tests**: Filtered count — forces actual column scan
- **Detail**: Unlike Q1, the `WHERE` clause forces DataFusion to actually read the `advengineid` column from Parquet. This is the first real data download from S3. Each worker scans its 10 files, applies the filter, and returns a single COUNT row.

### Q3: `SELECT SUM(advengineid), COUNT(*), AVG(resolutionwidth) FROM hits`
- **Strategy**: SINGLE_NODE (has AVG)
- **What it tests**: Mixed global aggregates including AVG
- **Timings**: OS 2.5s (parse 160ms, Iceberg scan 159ms, JNI 198ms, Arrow stream 1956ms), DF CLI 5.0s
- **Detail**: Routed to single node because Phase 1 cannot distribute AVG (AVG is not decomposable as SUM+COUNT without two-phase logic). Phase 2 decomposes AVG(x) → SUM(x)/COUNT(x) on workers, then CAST(SUM/SUM AS DOUBLE) on coordinator.
- **Observation**: Arrow stream read is 91% of execution time (1956ms / 2154ms). JNI call returns fast (198ms) — actual S3 download + aggregation happens lazily during stream consumption. OS is 2x faster than DF CLI despite both using DataFusion, due to avoiding DF CLI's text formatting overhead. Only 2 columns (advengineid, resolutionwidth) are read from Parquet — columnar advantage over 105-column table.

### Q4: `SELECT AVG(userid) FROM hits`
- **Strategy**: SINGLE_NODE (has AVG)
- **What it tests**: Pure AVG aggregate
- **Timings**: OS 2.5s (parse 164ms, Iceberg scan 74ms, JNI 176ms, Arrow stream 2129ms), DF CLI 3.1s
- **Detail**: Same reason as Q3 — AVG cannot be distributed in Phase 1.
- **Observation**: Iceberg scan planning dropped from 159ms (Q3) to 74ms — manifest metadata is warm in JVM from prior query. Reading 1 column (userid) takes similar time as Q3's 2 columns (2129ms vs 1956ms), suggesting S3 row-group fetch is the bottleneck, not column count.

### Q5: `SELECT COUNT(DISTINCT userid) FROM hits`
- **Strategy**: SINGLE_NODE (has DISTINCT)
- **What it tests**: Count of distinct values over 99.9M rows
- **Timings**: OS 2.7s (parse 119ms, Iceberg scan 68ms, JNI 158ms, Arrow stream 2363ms), DF CLI 3.2s
- **Detail**: COUNT(DISTINCT) requires seeing all values to deduplicate. Phase 1 routes to single node. Phase 2 expands this: workers GROUP BY userid, coordinator applies COUNT(DISTINCT) over the union.
- **Observation**: 17.6M unique users out of 99.9M rows (17.6% cardinality). DF CLI shows 6.7s user-time on 3.2s wall-clock — first query with heavy CPU parallelism for hash-based deduplication. Arrow stream slightly slower than Q4 (2363ms vs 2129ms) despite same single column, due to DISTINCT hash set overhead.

### Q6: `SELECT COUNT(DISTINCT searchphrase) FROM hits`
- **Strategy**: SINGLE_NODE (has DISTINCT)
- **What it tests**: Count distinct on a string column with high cardinality
- **Timings**: OS 2.95s (parse 150ms, Iceberg scan 70ms, JNI 168ms, Arrow stream 2557ms), DF CLI 2.96s
- **Detail**: Same as Q5 but on a variable-length string column.
- **Observation**: String DISTINCT is slower than integer DISTINCT (2557ms vs Q5's 2363ms) despite lower cardinality (6M vs 17.6M) — hashing/comparing variable-length strings costs more per-element than fixed 8-byte integers. DF CLI user time 7.7s on 3.0s wall (2.6x CPU parallelism). OS and DF wall times nearly identical — DataFusion execution dominates both paths equally for CPU-bound queries.

### Q7: `SELECT MIN(eventdate), MAX(eventdate) FROM hits`
- **Strategy**: GLOBAL_MERGE (3 workers, 10 files each)
- **What it tests**: MIN/MAX aggregates
- **Timings**: OS 0.67s (parse 167ms, Iceberg scan 70ms, local worker 140ms with Arrow stream 2ms), DF CLI 0.004s
- **Detail**: GLOBAL_MERGE because MIN/MAX are decomposable: MIN(all) = MIN(MIN(part1), MIN(part2), MIN(part3)). QueryAnalyzer sees no GROUP BY, no AVG, no DISTINCT, only MIN/MAX → GLOBAL_MERGE.
- **Observation**: Another metadata-only query — Arrow stream is 2ms, meaning DataFusion reads MIN/MAX from Parquet column statistics (file footers), not actual rows. DF CLI confirms at 0.004s. Distributed overhead is visible: local worker finishes in 140ms but total is 661ms — the extra 521ms is network round-trips to 2 remote workers + coordinator merge. For metadata-only queries, distribution adds cost without benefit. OS returns dates as epoch-days (15888=2013-07-02), correctness script converts.

### Q8: `SELECT advengineid, COUNT(*) AS c FROM hits WHERE advengineid <> 0 GROUP BY advengineid ORDER BY c DESC`
- **Strategy**: SINGLE_NODE (has GROUP BY)
- **What it tests**: Filtered GROUP BY with ORDER BY
- **Timings**: OS 2.2s (parse 193ms, Iceberg scan 76ms, JNI 168ms, Arrow stream 1750ms, 18 rows), DF CLI 2.9s
- **Detail**: SINGLE_NODE because QueryAnalyzer sees `Aggregate` with non-empty `groupSet`. In Phase 1 any GROUP BY goes to single node — a key (e.g., advengineid=2) could span files on different workers, so partial counts would be incorrect. Phase 2 uses TWO_PHASE_GROUP_BY: workers compute partial counts per key, coordinator re-aggregates.
- **Observation**: Only 18 distinct advengineid values out of 630K matching rows (0.6% selectivity on `<> 0`). Ideal for TWO_PHASE_GROUP_BY since partial results are tiny. Despite high filter selectivity, DataFusion still scans all rows — `advengineid=0` is present in every row group so no row-group pruning is possible.

### Q9: `SELECT regionid, COUNT(DISTINCT userid) AS u FROM hits GROUP BY regionid ORDER BY u DESC LIMIT 10`
- **Strategy**: SINGLE_NODE (GROUP BY + COUNT DISTINCT)
- **What it tests**: GROUP BY with COUNT(DISTINCT) — two independent reasons for single-node
- **Timings**: OS 2.9s (parse 155ms, Iceberg scan 75ms, JNI 166ms, Arrow stream 2531ms, 10 rows), DF CLI 3.4s
- **Detail**: QueryAnalyzer sees `Aggregate` with non-empty `groupSet` AND `AggregateCall.isDistinct()`. Either alone triggers SINGLE_NODE.
- **Observation**: Reads 2 columns (regionid, userid) across 100M rows. Requires hash map of (regionid → HashSet<userid>) — region 229 alone has 2.8M distinct users. LIMIT 10 doesn't reduce work: must compute COUNT(DISTINCT) for all ~230 regions before sorting. DF CLI user time 8.7s on 3.4s wall — heavy CPU for hash-based dedup per group.

### Q10: `SELECT regionid, SUM(advengineid), COUNT(*), AVG(resolutionwidth), COUNT(DISTINCT userid) FROM hits GROUP BY regionid ORDER BY c DESC LIMIT 10`
- **Strategy**: SINGLE_NODE (GROUP BY + AVG + COUNT DISTINCT)
- **What it tests**: Mixed aggregates — the hardest pattern for distribution
- **Timings**: OS 3.4s (parse 155ms, Iceberg scan 68ms, JNI 257ms, Arrow stream 2932ms, 10 rows), DF CLI 7.3s
- **Detail**: Three independent reasons for SINGLE_NODE: GROUP BY, AVG, COUNT(DISTINCT). QueryAnalyzer hits GROUP BY check first and short-circuits. Phase 2 uses MIXED_DISTINCT: workers GROUP BY (regionid, userid), coordinator re-aggregates + COUNT(DISTINCT).
- **Observation**: OS is 2x faster than DF CLI (3.4s vs 7.3s). DF CLI has 7.1s user ≈ 7.3s wall — nearly single-threaded. OS embedded DataFusion runs with 12 CPU threads (`-Ddatafusion_cpu_threads=12`), parallelizing the GROUP BY + DISTINCT hash maps much better. Reads 4 columns (regionid, advengineid, resolutionwidth, userid) — heaviest column read so far.

### Q11: `SELECT mobilephonemodel, COUNT(DISTINCT userid) AS u FROM hits WHERE mobilephonemodel <> '' GROUP BY mobilephonemodel ORDER BY u DESC LIMIT 10`
- **Strategy**: SINGLE_NODE (GROUP BY + COUNT DISTINCT)
- **What it tests**: Filtered GROUP BY + COUNT(DISTINCT) on string column
- **Timings**: OS 2.6s (parse 141ms, Iceberg scan 83ms, JNI 205ms, Arrow stream 2207ms, 10 rows), DF CLI 5.2s
- **Observation**: Filter `mobilephonemodel <> ''` is highly selective — most rows are desktop visits with empty mobilephonemodel. This makes GROUP BY + DISTINCT much cheaper than Q9's full table scan. OS 2x faster than DF CLI, consistent with 12-thread advantage on GROUP BY + DISTINCT workloads.

### Q12: `SELECT mobilephone, mobilephonemodel, COUNT(DISTINCT userid) AS u FROM hits WHERE mobilephonemodel <> '' GROUP BY mobilephone, mobilephonemodel ORDER BY u DESC LIMIT 10`
- **Strategy**: SINGLE_NODE (composite GROUP BY + COUNT DISTINCT)
- **Timings**: OS 2.7s (parse 126ms, Iceberg scan 71ms, JNI 172ms, Arrow stream 2292ms, 10 rows), DF CLI 5.0s
- **Observation**: Nearly identical timing to Q11 — adding a second GROUP BY column (mobilephone integer) has negligible cost. Same filter selectivity, same rows scanned. The composite key splits Q11's "iPad" group (1.09M) into sub-groups by phone type ID (mobilephone=1 iPad = 931K).
- **What it tests**: Multi-column GROUP BY + COUNT(DISTINCT)

### Q13: `SELECT searchphrase, COUNT(*) AS c FROM hits WHERE searchphrase <> '' GROUP BY searchphrase ORDER BY c DESC LIMIT 10`
- **Strategy**: SINGLE_NODE (GROUP BY)
- **What it tests**: High-cardinality GROUP BY on string column
- **Timings**: OS 2.7s (parse 149ms, Iceberg scan 72ms, JNI 166ms, Arrow stream 2317ms, 10 rows), DF CLI 2.4s
- **Observation**: 6M-cardinality GROUP BY on strings completes in ~2.5s — DataFusion's hash aggregation handles it efficiently. DF CLI slightly faster here (2.4s vs 2.7s) with 7.9s user time (3.3x CPU parallelism). UTF-8 Cyrillic strings pass through Parquet → Arrow → Java without encoding issues.

### Q14: `SELECT searchphrase, COUNT(DISTINCT userid) AS u FROM hits WHERE searchphrase <> '' GROUP BY searchphrase ORDER BY u DESC LIMIT 10`
- **Strategy**: SINGLE_NODE (GROUP BY + COUNT DISTINCT)
- **What it tests**: High-cardinality GROUP BY + COUNT(DISTINCT)
- **Timings**: OS 3.6s (parse 148ms, Iceberg scan 69ms, JNI 198ms, Arrow stream 3225ms, 10 rows), DF CLI 5.3s
- **Observation**: +1s slower than Q13 (COUNT(*) → COUNT(DISTINCT userid)). Q13 maintains HashMap<String, i64> (6M entries). Q14 maintains HashMap<String, HashSet<u64>> — 6M groups each with a set of user IDs, much more memory and CPU. DF CLI user time 11.8s — heaviest CPU query so far.

### Q15: `SELECT searchengineid, searchphrase, COUNT(*) AS c FROM hits WHERE searchphrase <> '' GROUP BY searchengineid, searchphrase ORDER BY c DESC LIMIT 10`
- **Strategy**: SINGLE_NODE
- **What it tests**: Two-column GROUP BY with one high-cardinality key

### Q16: `SELECT userid, COUNT(*) AS c FROM hits GROUP BY userid ORDER BY c DESC LIMIT 10`
- **Strategy**: SINGLE_NODE
- **What it tests**: GROUP BY on a very high-cardinality INT64 column
- **Detail**: `userid` has tens of millions of distinct values. Large hash table required.

### Q17: `SELECT userid, searchphrase, COUNT(*) AS c FROM hits GROUP BY userid, searchphrase ORDER BY c DESC LIMIT 10`
- **Strategy**: SINGLE_NODE
- **What it tests**: Composite GROUP BY — two high-cardinality columns
- **Detail**: This is one of the heaviest queries. The combination of userid x searchphrase creates a massive hash table. Can use 18GB+ of the 20GB memory pool. Often triggers cascade OOM for subsequent queries.

### Q18: `SELECT userid, searchphrase, COUNT(*) AS c FROM hits GROUP BY userid, searchphrase LIMIT 10`
- **Strategy**: SINGLE_NODE
- **What it tests**: Same GROUP BY as Q17 but without ORDER BY
- **Detail**: Still needs the full hash table to compute GROUP BY, even though only 10 rows are returned. The LIMIT doesn't help with memory — DataFusion must aggregate all groups before picking 10.

### Q19: `SELECT userid, EXTRACT(MINUTE FROM eventtime) AS m, searchphrase, COUNT(*) AS c FROM hits GROUP BY userid, EXTRACT(MINUTE FROM eventtime), searchphrase ORDER BY c DESC LIMIT 10`
- **Strategy**: SINGLE_NODE
- **What it tests**: Three-column GROUP BY with expression extraction
- **Detail**: Even heavier than Q17 — adds minute-of-hour as a third grouping dimension.

### Q20: `SELECT userid FROM hits WHERE userid = 435090932899640449`
- **Strategy**: CONCAT
- **What it tests**: Point lookup on a single value
- **Detail**: One of the few CONCAT queries — no aggregation, no sort. Each worker scans its files for matching rows and returns them. Predicate pushdown to Parquet row group filtering makes this fast.

### Q21: `SELECT COUNT(*) FROM hits WHERE url LIKE '%google%'`
- **Strategy**: GLOBAL_MERGE
- **What it tests**: Pattern matching on a string column with full scan
- **Detail**: `LIKE '%google%'` cannot use any index or min/max pruning — must scan every row's `url` value. Full S3 download of the `url` column (~several GB of string data).

### Q22: `SELECT searchphrase, MIN(url), COUNT(*) AS c FROM hits WHERE url LIKE '%google%' AND searchphrase <> '' GROUP BY searchphrase ORDER BY c DESC LIMIT 10`
- **Strategy**: SINGLE_NODE (has GROUP BY)
- **What it tests**: Filtered GROUP BY with pattern matching

### Q23: `SELECT searchphrase, MIN(url), MIN(title), COUNT(*) AS c, COUNT(DISTINCT userid) FROM hits WHERE title LIKE '%Google%' AND url NOT LIKE '%.google.%' AND searchphrase <> '' GROUP BY searchphrase ORDER BY c DESC LIMIT 10`
- **Strategy**: SINGLE_NODE (has GROUP BY + DISTINCT)
- **What it tests**: Complex filtered GROUP BY with mixed aggregates including DISTINCT

### Q24: `SELECT * FROM hits WHERE url LIKE '%google%' ORDER BY eventtime LIMIT 10`
- **Strategy**: TOPK_MERGE
- **What it tests**: Full-row SELECT with filter, sort, and limit
- **Detail**: One of the few TOPK_MERGE queries. Each worker scans for `%google%` matches, sorts by `eventtime`, returns top 10. Coordinator merge-sorts the 30 rows (10 per worker) and picks the global top 10. Downloads ALL columns (SELECT *) which is expensive.

### Q25: `SELECT searchphrase FROM hits WHERE searchphrase <> '' ORDER BY eventtime LIMIT 10`
- **Strategy**: TOPK_MERGE
- **What it tests**: Filtered sort on a different column than projected

### Q26: `SELECT searchphrase FROM hits WHERE searchphrase <> '' ORDER BY searchphrase LIMIT 10`
- **Strategy**: TOPK_MERGE
- **What it tests**: Sort on the projected column itself

### Q27: `SELECT searchphrase FROM hits WHERE searchphrase <> '' ORDER BY eventtime, searchphrase LIMIT 10`
- **Strategy**: TOPK_MERGE
- **What it tests**: Multi-column sort

### Q28: `SELECT counterid, AVG(CHAR_LENGTH(url)) AS l, COUNT(*) AS c FROM hits WHERE url <> '' GROUP BY counterid HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25`
- **Strategy**: SINGLE_NODE (has GROUP BY + AVG)
- **What it tests**: GROUP BY with HAVING clause, AVG of expression
- **Detail**: HAVING is applied after aggregation. Phase 2 strips HAVING from worker queries and re-applies on the coordinator after re-aggregation.

### Q29: `SELECT SUBSTRING(referer ...) AS k, AVG(CHAR_LENGTH(referer)) AS l, COUNT(*) AS c, MIN(referer) FROM hits WHERE referer <> '' GROUP BY ... HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25`
- **Strategy**: SINGLE_NODE (has GROUP BY + AVG)
- **What it tests**: Complex expression in GROUP BY key with HAVING
- **Detail**: Groups by a computed substring of the referer URL (domain extraction). The expression is evaluated for every row.

### Q30: `SELECT SUM(resolutionwidth), SUM(resolutionwidth + 1), ... SUM(resolutionwidth + 89) FROM hits`
- **Strategy**: GLOBAL_MERGE
- **What it tests**: 90 SUM aggregates on the same column with offsets
- **Detail**: Tests vectorized aggregation throughput. All 90 expressions read the same `resolutionwidth` column. DataFusion's columnar engine processes this efficiently — reads the column once, computes all 90 sums in a single pass.

### Q31: `SELECT searchengineid, clientip, COUNT(*) AS c, SUM(isrefresh), AVG(resolutionwidth) FROM hits WHERE searchphrase <> '' GROUP BY searchengineid, clientip ORDER BY c DESC LIMIT 10`
- **Strategy**: SINGLE_NODE (has GROUP BY + AVG)
- **What it tests**: Multi-column GROUP BY with mixed aggregates

### Q32: `SELECT watchid, clientip, COUNT(*), SUM(isrefresh), AVG(resolutionwidth) FROM hits WHERE searchphrase <> '' GROUP BY watchid, clientip ORDER BY c DESC LIMIT 10`
- **Strategy**: SINGLE_NODE (has GROUP BY + AVG)
- **What it tests**: Very high-cardinality GROUP BY (watchid is nearly unique per row)
- **Detail**: `watchid` is almost a primary key — nearly 100M distinct values. The hash table is enormous.

### Q33: `SELECT watchid, clientip, COUNT(*), SUM(isrefresh), AVG(resolutionwidth) FROM hits GROUP BY watchid, clientip ORDER BY c DESC LIMIT 10`
- **Strategy**: SINGLE_NODE (has GROUP BY + AVG)
- **What it tests**: Same as Q32 but without the searchphrase filter — scans all rows
- **Detail**: No filter means all 99.9M rows go into the hash table. Peak memory usage.

### Q34: `SELECT url, COUNT(*) AS c FROM hits GROUP BY url ORDER BY c DESC LIMIT 10`
- **Strategy**: SINGLE_NODE
- **What it tests**: GROUP BY on URL — ~100M unique values
- **Detail**: Known timeout (>180s). URL column is variable-length string with nearly-unique values per row. The hash table exceeds available memory. This query is expected to fail.

### Q35: `SELECT 1 AS "one", url, COUNT(*) AS c FROM hits GROUP BY url ORDER BY c DESC LIMIT 10`
- **Strategy**: SINGLE_NODE
- **What it tests**: Same as Q34 with an extra constant column
- **Detail**: Known timeout. Same hash table explosion as Q34.

### Q36: `SELECT clientip, clientip - 1, clientip - 2, clientip - 3, COUNT(*) AS c FROM hits GROUP BY clientip, clientip - 1, clientip - 2, clientip - 3 ORDER BY c DESC LIMIT 10`
- **Strategy**: SINGLE_NODE (has GROUP BY)
- **What it tests**: GROUP BY with expression columns
- **Detail**: Groups by computed expressions on clientip. Moderate cardinality.

### Q37: `SELECT url, COUNT(*) AS PageViews FROM hits WHERE counterid = 62 AND eventdate >= '2013-07-01' AND eventdate <= '2013-07-31' AND ... GROUP BY url ORDER BY PageViews DESC LIMIT 10`
- **Strategy**: SINGLE_NODE (has GROUP BY)
- **What it tests**: Highly selective filter + GROUP BY
- **Detail**: The filter `counterid = 62 AND eventdate` narrows to a very small subset. Iceberg predicate pushdown prunes most files. Queries Q37-Q43 all share this pattern — they target a specific counter's July 2013 data. May fail with "Failed to plan query" if Calcite can't handle certain expressions (e.g., DATE literals).

### Q38: `SELECT title, COUNT(*) AS PageViews FROM hits WHERE counterid = 62 AND ... GROUP BY title ORDER BY PageViews DESC LIMIT 10`
- **Strategy**: SINGLE_NODE
- **What it tests**: Same selective filter as Q37, grouping by title
- **Detail**: Same potential planning failure as Q37.

### Q39: `SELECT url, COUNT(*) AS PageViews FROM hits WHERE counterid = 62 AND ... AND islink <> 0 AND isdownload = 0 GROUP BY url ORDER BY PageViews DESC LIMIT 10`
- **Strategy**: SINGLE_NODE
- **What it tests**: More filters than Q37 — link tracking analysis

### Q40: `SELECT traficsourceid, searchengineid, advengineid, CASE WHEN ... END AS Src, url AS Dst, COUNT(*) AS PageViews FROM hits WHERE counterid = 62 AND ... GROUP BY ... ORDER BY PageViews DESC LIMIT 10`
- **Strategy**: SINGLE_NODE
- **What it tests**: CASE expression in SELECT + multi-column GROUP BY
- **Detail**: Tests DataFusion's handling of CASE WHEN expressions in projection.

### Q41: `SELECT urlhash, eventdate, COUNT(*) AS PageViews FROM hits WHERE counterid = 62 AND ... AND traficsourceid IN (-1, 6) AND refererhash = ... GROUP BY urlhash, eventdate ORDER BY PageViews DESC LIMIT 10`
- **Strategy**: SINGLE_NODE
- **What it tests**: IN clause + hash equality filter + GROUP BY

### Q42: `SELECT windowclientwidth, windowclientheight, COUNT(*) AS PageViews FROM hits WHERE counterid = 62 AND ... AND urlhash = ... GROUP BY windowclientwidth, windowclientheight ORDER BY PageViews DESC LIMIT 10`
- **Strategy**: SINGLE_NODE
- **What it tests**: Narrow filter on urlhash + GROUP BY on screen resolution dimensions
- **Detail**: May fail with "Failed to plan query" due to Calcite expression handling.

### Q43: `SELECT FLOOR(eventtime TO MINUTE) AS M, COUNT(*) AS PageViews FROM hits WHERE counterid = 62 AND eventdate >= '2013-07-15' AND eventdate <= '2013-07-16' AND ... GROUP BY FLOOR(eventtime TO MINUTE) ORDER BY M LIMIT 10`
- **Strategy**: SINGLE_NODE
- **What it tests**: Time-bucketed aggregation (per-minute page views)
- **Detail**: FLOOR(eventtime TO MINUTE) creates minute-level time buckets. Very narrow date range (2 days). May fail with "Failed to plan query" due to FLOOR TO MINUTE expression.

---

## Strategy Distribution Summary (Phase 1)

| Strategy | Queries | Count |
|---|---|---|
| **GLOBAL_MERGE** | Q1, Q2, Q7, Q21, Q30 | 5 |
| **TOPK_MERGE** | Q24, Q25, Q26, Q27 | 4 |
| **CONCAT** | Q20 | 1 |
| **SINGLE_NODE** | Q3-Q6, Q8-Q19, Q22-Q23, Q28-Q29, Q31-Q43 | 33 |

In Phase 1, **33 out of 43 queries route to a single node** because they contain GROUP BY, AVG, or COUNT(DISTINCT) — patterns that require two-phase aggregation logic not yet available. Only 10 queries actually benefit from distributed execution.

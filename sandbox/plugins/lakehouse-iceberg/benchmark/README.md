# ClickBench Benchmark for OpenSearch Lakehouse

Runs the standard [ClickBench](https://benchmark.clickhouse.com/) 43-query
suite against an Iceberg table registered in the OpenSearch Lakehouse plugin.

## Prerequisites

1. OpenSearch running with the lakehouse-iceberg plugin loaded.
2. An Iceberg table registered (e.g. `hits_s3` backed by a Glue catalog).
3. `curl` and `python3` on PATH.

## Quick Start

```bash
# Run all 43 queries against local OpenSearch (default endpoint)
./run-clickbench.sh

# Point at a remote node
./run-clickbench.sh --endpoint http://35.80.117.84:9200

# Custom table name and timeout
./run-clickbench.sh --table my_iceberg_table --timeout 120

# Run only specific queries
./run-clickbench.sh --queries 1,2,3,7,37,38

# Custom output directory
./run-clickbench.sh --output /tmp/bench-results
```

## Options

| Flag | Default | Description |
|------|---------|-------------|
| `--endpoint URL` | `http://localhost:9200` | OpenSearch endpoint |
| `--table TABLE` | `hits_s3` | Registered Iceberg table name |
| `--timeout SECS` | `90` | Per-query timeout in seconds |
| `--output DIR` | `results/run_<timestamp>` | Output directory |
| `--queries Q1,Q2` | all (1-43) | Comma-separated query numbers |

## Output

Each run produces a timestamped directory under `results/` containing:

| File | Description |
|------|-------------|
| `results.json` | Full results with timing, status, row counts, errors |
| `results.csv` | CSV with columns: query, time_s, status, rows |
| `q01.json` .. `q43.json` | Raw HTTP responses from OpenSearch |

## Expected Behavior

- **41/43 queries pass** on 32GB nodes with the recommended memory config.
- **Q34 and Q35 timeout** at 90s because they GROUP BY `url` which has ~100M
  unique values, requiring a hash table larger than available memory.
- Q36 (GROUP BY `clientip`) passes on a fresh start but may fail after
  Q34/Q35 have exhausted memory.

## Recommended OpenSearch JVM Settings

For a 32GB node running lakehouse benchmarks:

```
-Xms4g -Xmx4g
--enable-native-access=ALL-UNNAMED
```

DataFusion defaults to Greedy Unlimited memory pool (no additional config needed).

## Correctness Suite

The `correctness/` subdirectory contains a separate tool that runs the same
43 queries on both OpenSearch and DataFusion CLI, then compares the results
row-by-row. See `correctness/README.md` for details.

## Directory Structure

```
benchmark/
  run-clickbench.sh          -- Primary benchmark script
  README.md                  -- This file
  correctness/               -- Correctness verification suite
    run_correctness.sh
    compare_results.py
    queries/
    results/
  results/                   -- Benchmark results (gitignored)
    run_<timestamp>/
      results.json
      results.csv
      q01.json .. q43.json
```

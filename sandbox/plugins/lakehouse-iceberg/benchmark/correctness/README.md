# Correctness Verification Suite

Compares query results from the OpenSearch lakehouse plugin against DataFusion CLI
to verify correctness across all 43 ClickBench queries.

DataFusion CLI results are treated as the **ground truth**. Both engines query the
same ClickBench dataset (99.9M rows) — OpenSearch via Iceberg/Glue, DataFusion CLI
reads the same S3 parquet files directly.

## Prerequisites

1. **OpenSearch** running with the lakehouse plugin and a registered Iceberg table
2. **datafusion-cli** installed (found via `PATH` or `~/.cargo/bin/`)
3. **S3 access** to the ClickBench parquet data (default), or local parquet files
4. **Python 3** available on the system

## Quick Start

```bash
# Default: run against localhost OpenSearch + S3 parquet data
# Works on both EC2 nodes and dev machine (with AWS_PROFILE=default)
./run_correctness.sh

# Run only specific queries
./run_correctness.sh --queries 1,2,3,7,37,38

# Use local parquet file instead of S3
./run_correctness.sh --data-path /path/to/hits.parquet

# Skip OpenSearch execution (use previous results)
./run_correctness.sh --skip-os

# Skip DataFusion execution (use previous results)
./run_correctness.sh --skip-df

# Compare existing results only (skip both executions)
./run_correctness.sh --skip-os --skip-df
```

## Options

| Flag | Default | Description |
|------|---------|-------------|
| `--endpoint URL` | `http://localhost:9200` | OpenSearch endpoint |
| `--table TABLE` | `hits_s3` | OpenSearch table name |
| `--data-path PATH` | S3 ClickBench path | Path to parquet data for DF CLI (S3 or local) |
| `--queries-dir DIR` | `./queries` | Custom queries directory |
| `--output-dir DIR` | `./results` | Output directory for results |
| `--timeout SECS` | `120` | Query timeout in seconds |
| `--max-rows N` | `100` | Max rows to compare per query |
| `--skip-os` | false | Skip OpenSearch execution |
| `--skip-df` | false | Skip DataFusion execution |
| `--queries Q1,Q2` | all (1-43) | Comma-separated query numbers |

## Interpreting Results

The output shows three phases:

1. **Phase 1**: Executes queries on OpenSearch, saves raw JSON responses
2. **Phase 2**: Executes queries on DataFusion CLI (ground truth), saves raw text output
3. **Phase 3**: Compares results using `compare_results.py`

Summary codes:
- **PASS**: Both engines produced identical results (after normalization)
- **FAIL**: Results differ (row count, column count, or value mismatch)
- **SKIP**: Both engines errored or no output files found
- **ERROR**: One engine errored while the other succeeded

Expected failures marked with `*` are queries known to be problematic:
- **Q34, Q35**: Timeout on high-cardinality GROUP BY (URL, ~100M unique values)
- **Q36**: TopK OOM on high-cardinality clientip GROUP BY

## Value Normalization

The comparison handles these representation differences:
- **Dates**: epoch-day integers (e.g., `15887`) match date strings (`2013-07-01`)
- **Floats**: compared to 6 significant figures
- **Booleans**: `true`/`false` normalized to `1`/`0`
- **NULLs**: empty strings, `NULL`, `null`, `None` all treated as NULL
- **Timestamps**: normalized format `YYYY-MM-DDTHH:MM:SS`
- **Column names**: compared case-insensitively, ignoring quotes

## Key Query Differences

The OS and DF versions of each query are semantically identical but use
different SQL dialects:

| Feature | OpenSearch (OS) | DataFusion (DF) |
|---------|----------------|-----------------|
| String length | `CHAR_LENGTH(col)` | `length(col)` |
| Date filter | `DATE '2013-07-01'` | `15887` (epoch days) |
| Timestamp extract | `EXTRACT(MINUTE FROM eventtime)` | `extract(minute FROM to_timestamp_seconds(eventtime))` |
| Timestamp truncate | `FLOOR(eventtime TO MINUTE)` | `DATE_TRUNC('minute', to_timestamp_seconds(eventtime))` |
| Regex replace | `SUBSTRING(... FROM ... FOR ...)` | `REGEXP_REPLACE(...)` |
| Literal alias | `1 AS "one"` | `1` (no alias) |

## Using compare_results.py Standalone

```bash
# Compare a single query
python3 compare_results.py single results/os_raw/q1.json results/df_raw/q1.txt \
    --query-id 1 --ordered

# Batch compare all queries
python3 compare_results.py batch \
    --os-dir results/os_raw \
    --df-dir results/df_raw \
    --max-rows 100
```

## Adding Custom Queries

1. Create `queries/q{NN}_os.sql` with the OpenSearch SQL variant
2. Create `queries/q{NN}_df.sql` with the DataFusion SQL variant
3. Both files should contain a single SQL statement
4. The table name `hits` in OS queries is automatically replaced with `--table`

## Directory Structure

```
correctness/
  run_correctness.sh       -- Main runner script
  compare_results.py       -- Python comparison engine
  create_df_table.sql      -- DataFusion table setup template
  README.md                -- This file
  queries/
    q01_os.sql .. q43_os.sql  -- OpenSearch query variants
    q01_df.sql .. q43_df.sql  -- DataFusion query variants
  results/                 -- Generated at runtime
    os_raw/                -- Raw OpenSearch JSON responses
    df_raw/                -- Raw DataFusion CLI text output
```

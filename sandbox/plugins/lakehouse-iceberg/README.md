# lakehouse-iceberg

Read external Apache Iceberg tables via SQL through the OpenSearch analytics engine. Queries are executed natively via DataFusion (Rust) over JNI.

## Prerequisites

- AWS credentials configured in `~/.aws/credentials` (the `[default]` profile is used)
- An existing Iceberg table registered in AWS Glue
- The Rust native library must be built for `analytics-backend-datafusion` (see below)

## Build

```bash
# Build the Rust native library (required first time, or after Rust changes)
cd sandbox/plugins/analytics-backend-datafusion
cargo build --release
cd ../../..

# Build all plugins
./gradlew :sandbox:plugins:lakehouse-iceberg:assemble \
          :sandbox:plugins:analytics-backend-datafusion:assemble \
          :sandbox:plugins:analytics-engine:assemble
```

## Run OpenSearch

```bash
./gradlew run -PinstalledPlugins='["analytics-engine","analytics-backend-datafusion","lakehouse-iceberg","dsl-query-executor"]'
```

To enable Rust-side DataFusion logging (logical/physical plans), set `RUST_LOG`:

```bash
RUST_LOG=opensearch_datafusion_jni=debug,datafusion=info \
  ./gradlew run -PinstalledPlugins='["analytics-engine","analytics-backend-datafusion","lakehouse-iceberg","dsl-query-executor"]'
```

## Setup: Register Catalog and Table

Once OpenSearch is running (listening on `localhost:9200`):

### 1. Register a Glue catalog

```bash
curl -X PUT 'localhost:9200/_lakehouse/catalog/my_catalog' \
  -H 'Content-Type: application/json' -d '{
  "type": "GLUE",
  "region": "us-west-2",
  "warehouse": "s3://my-bucket/iceberg-warehouse"
}'
```

| Field       | Required | Description                                      |
|-------------|----------|--------------------------------------------------|
| `type`      | yes      | Catalog type: `GLUE`, `REST`, or `HADOOP`        |
| `region`    | yes      | AWS region for Glue and S3                        |
| `warehouse` | yes      | S3 warehouse URI (`s3://...`) or `file://` for local testing |
| `uri`       | no       | REST catalog URI (required for `REST` type)       |

### 2. Register a table

```bash
curl -X PUT 'localhost:9200/_lakehouse/table/nyc_taxi' \
  -H 'Content-Type: application/json' -d '{
  "catalog": "my_catalog",
  "namespace": "my_glue_database",
  "table": "my_iceberg_table"
}'
```

| Field       | Required | Description                                             |
|-------------|----------|---------------------------------------------------------|
| `catalog`   | yes      | Name of a previously registered catalog                 |
| `namespace` | no       | Glue database name (defaults to `default`)              |
| `table`     | no       | Iceberg table name in Glue (defaults to the URL `{name}`) |

The URL path `{name}` (e.g., `nyc_taxi`) becomes the SQL table name you query against.

## Run Queries

```bash
# Simple select
curl -X POST 'localhost:9200/_analytics/sql' \
  -H 'Content-Type: application/json' -d '{
  "query": "SELECT * FROM nyc_taxi LIMIT 10"
}'

# Aggregation
curl -X POST 'localhost:9200/_analytics/sql' \
  -H 'Content-Type: application/json' -d '{
  "query": "SELECT COUNT(*), AVG(trip_distance), SUM(total_amount) FROM nyc_taxi"
}'

# Filter + ORDER BY
curl -X POST 'localhost:9200/_analytics/sql' \
  -H 'Content-Type: application/json' -d '{
  "query": "SELECT vendorid, trip_distance, total_amount FROM nyc_taxi WHERE trip_distance > 10 ORDER BY total_amount DESC LIMIT 20"
}'
```

## Logs

### Log locations

| File | Contents |
|------|----------|
| `build/testclusters/runTask-0/logs/runTask.log` | Java logs (OpenSearch, analytics-engine, lakehouse-iceberg) |
| `build/testclusters/runTask-0/logs/opensearch.stderr.log` | Rust DataFusion logs (logical plan, physical plan) |

### Enable Java debug logging

After OpenSearch starts, enable debug logging for the lakehouse execution pipeline:

```bash
curl -X PUT 'localhost:9200/_cluster/settings' \
  -H 'Content-Type: application/json' -d '{
  "transient": {
    "logger.org.opensearch.lakehouse": "DEBUG",
    "logger.org.opensearch.be.datafusion": "DEBUG",
    "logger.org.opensearch.analytics.exec": "DEBUG"
  }
}'
```

This enables debug logs for:
- **`org.opensearch.lakehouse`** — Calcite plan, Iceberg filter, scan plan file paths, Substrait plan (readable protobuf text), storage config, credential refresh
- **`org.opensearch.be.datafusion`** — JNI call parameters, S3 config, Arrow batch details (row count, columns)
- **`org.opensearch.analytics.exec`** — Routing decisions (external vs local table), scan context summary

### Enable Rust DataFusion logging

Set `RUST_LOG` environment variable before starting OpenSearch:

```bash
# Info level (logical + physical plans)
RUST_LOG=opensearch_datafusion_jni=info ./gradlew run -PinstalledPlugins='[...]'

# Debug level (+ Substrait decode details)
RUST_LOG=opensearch_datafusion_jni=debug,datafusion=info ./gradlew run -PinstalledPlugins='[...]'
```

Rust logs appear in `opensearch.stderr.log` and show:
- DataFusion **logical plan** (after Substrait → LogicalPlan conversion)
- DataFusion **physical plan** (how it actually executes: partitioning, aggregation strategy, file groups)

### Tail logs in real time

```bash
# Java logs
tail -f build/testclusters/runTask-0/logs/runTask.log | grep -E "IcebergTableExecutor|DataFusionPlugin|DefaultPlanExecutor|SubstraitConverter|CatalogConnector"

# Rust logs
tail -f build/testclusters/runTask-0/logs/opensearch.stderr.log | grep "DataFusion-Rust"
```

## Execution Flow

```
SQL query
  → Calcite parse + validate (analytics-engine)
  → DefaultPlanExecutor detects ExternalTable (IcebergCalciteTable)
  → IcebergTableExecutor.prepareScan()
      ├── Extract Iceberg filter predicates (manifest pruning)
      ├── Plan scan → resolve S3 Parquet file paths
      ├── Convert Calcite RelNode → Substrait protobuf bytes
      └── Build S3 storage config (region, bucket, credentials)
  → ExternalScanContext (table name, file paths, substrait bytes, storage config)
  → DataFusionPlugin.executeRemoteQuery()
      └── NativeBridge.executeIcebergQueryAsync() [JNI → Rust]
          ├── Register S3 object store
          ├── Register Parquet files as ListingTable
          ├── Decode Substrait → DataFusion LogicalPlan
          ├── Optimize → PhysicalPlan
          └── Execute → stream Arrow RecordBatches
  → Arrow batches → Object[] rows → JSON response
```

## Credential Refresh

AWS credentials (from `~/.aws/credentials`) are cached per-catalog with a 10-minute lifetime. When credentials expire, they are automatically re-resolved on the next query via `DefaultCredentialsProvider`. No restart needed.

Credential refresh is logged at INFO level:
```
[CatalogConnector] Refreshed credentials for catalog [my_catalog], sessionToken=present
```

## Local Testing (without AWS)

Generate a local Iceberg table with sample data:

```bash
./gradlew :sandbox:plugins:lakehouse-iceberg:generateIcebergTestData
```

Then register using a HADOOP catalog with `file://` warehouse:

```bash
curl -X PUT 'localhost:9200/_lakehouse/catalog/local_catalog' \
  -H 'Content-Type: application/json' -d '{
  "type": "HADOOP",
  "warehouse": "file:///tmp/iceberg-test-warehouse"
}'

curl -X PUT 'localhost:9200/_lakehouse/table/sensor_data' \
  -H 'Content-Type: application/json' -d '{
  "catalog": "local_catalog",
  "namespace": "default",
  "table": "sensor_data"
}'
```

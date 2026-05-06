# Lakehouse Iceberg Plugin — Getting Started

## Prerequisites

- JDK 25 (Amazon Corretto 25 recommended)
- Rust toolchain (`cargo`) for the native DataFusion library
- Sandbox mode enabled via `-Dsandbox.enabled=true`

## Build

### Build all plugins and native library

```bash
./gradlew -Dsandbox.enabled=true \
  :sandbox:libs:dataformat-native:buildRustLibrary \
  :sandbox:plugins:analytics-engine:assemble \
  :sandbox:plugins:analytics-backend-datafusion:assemble \
  :sandbox:plugins:lakehouse-iceberg:assemble \
  :sandbox:plugins:dsl-query-executor:assemble
```

Note: The Rust native library build (`buildRustLibrary`) takes ~12 minutes on the first run. Subsequent builds are incremental.

### Run unit tests

```bash
./gradlew -Dsandbox.enabled=true :sandbox:plugins:lakehouse-iceberg:test
```

### Run integration tests

```bash
# SQL integration tests
./gradlew -Dsandbox.enabled=true :sandbox:plugins:lakehouse-iceberg:internalClusterTest \
  --tests "org.opensearch.lakehouse.integration.sql.*"

# PPL integration tests
./gradlew -Dsandbox.enabled=true :sandbox:plugins:lakehouse-iceberg:internalClusterTest \
  --tests "org.opensearch.lakehouse.integration.ppl.*"

# All integration tests
./gradlew -Dsandbox.enabled=true :sandbox:plugins:lakehouse-iceberg:internalClusterTest
```

## Run OpenSearch with Plugins

```bash
./gradlew -Dsandbox.enabled=true run \
  -PinstalledPlugins='["analytics-engine","analytics-backend-datafusion","lakehouse-iceberg","dsl-query-executor"]'
```

Verify the node is running:

```bash
curl -s localhost:9200
```

## Register an Iceberg Table

Create an OpenSearch index with lakehouse settings that point to an Iceberg table.

### ClickBench (Glue catalog, S3)

```bash
curl -X PUT "localhost:9200/hits" -H 'Content-Type: application/json' -d '{
  "settings": {
    "index.lakehouse.enabled": true,
    "index.lakehouse.type": "glue",
    "index.lakehouse.region": "us-west-2",
    "index.lakehouse.warehouse": "s3://iceberg-benchmark-test-263689514295/iceberg-warehouse",
    "index.lakehouse.namespace": "iceberg_benchmark_db",
    "index.lakehouse.table": "hits",
    "index.lakehouse.auth_type": "default"
  }
}'
```

### Custom table

```bash
curl -X PUT "localhost:9200/<index-name>" -H 'Content-Type: application/json' -d '{
  "settings": {
    "index.lakehouse.enabled": true,
    "index.lakehouse.type": "glue",
    "index.lakehouse.region": "us-west-2",
    "index.lakehouse.warehouse": "s3://<your-bucket>/<warehouse-path>",
    "index.lakehouse.namespace": "<your-database>",
    "index.lakehouse.table": "<your-table>",
    "index.lakehouse.auth_type": "default"
  }
}'
```

### Settings reference

| Setting | Description |
|---------|-------------|
| `index.lakehouse.enabled` | Enable lakehouse mode for this index |
| `index.lakehouse.type` | Catalog type: `glue` or `hadoop` |
| `index.lakehouse.region` | AWS region (e.g. `us-west-2`) |
| `index.lakehouse.warehouse` | S3 path to the Iceberg warehouse |
| `index.lakehouse.namespace` | Database/namespace in the catalog |
| `index.lakehouse.table` | Table name within the namespace |
| `index.lakehouse.auth_type` | AWS auth: `default` (credential chain) or `iam_role` |

## Query

### SQL

```bash
curl -s -X POST "localhost:9200/_lakehouse/sql" \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM hits LIMIT 10"}'
```

### PPL

```bash
curl -s -X POST "localhost:9200/_lakehouse/ppl" \
  -H "Content-Type: application/json" \
  -d '{"query": "source = hits | head 10"}'
```

### Example SQL queries

```bash
# Count rows
curl -s -X POST "localhost:9200/_lakehouse/sql" \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT COUNT(*) FROM hits"}'

# Aggregation
curl -s -X POST "localhost:9200/_lakehouse/sql" \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT COUNT(*), SUM(\"AdvEngineID\") FROM hits WHERE \"AdvEngineID\" > 0"}'
```

### Example PPL queries

```bash
# Count rows
curl -s -X POST "localhost:9200/_lakehouse/ppl" \
  -H "Content-Type: application/json" \
  -d '{"query": "source = hits | stats count()"}'

# Filter and aggregate
curl -s -X POST "localhost:9200/_lakehouse/ppl" \
  -H "Content-Type: application/json" \
  -d '{"query": "source = hits | where AdvEngineID > 0 | stats count(), sum(AdvEngineID)"}'
```

## Architecture

The query flow:

1. REST endpoint (`_lakehouse/sql` or `_lakehouse/ppl`) receives the query
2. **analytics-engine** parses and plans via Apache Calcite
3. **lakehouse-iceberg** resolves the Iceberg table, discovers schema, and prunes files via predicate pushdown
4. **analytics-backend-datafusion** executes the query natively via DataFusion (Rust) against S3 Parquet files
5. Results are streamed back as Arrow batches and converted to JSON

# Component 0: Lakehouse Index Abstraction

## Table of Contents

1. [Overview and Motivation](#1-overview-and-motivation)
2. [Lakehouse Index Metadata Model](#2-lakehouse-index-metadata-model)
3. [REST API for Managing Lakehouse Indices](#3-rest-api-for-managing-lakehouse-indices)
4. [Integration with OpenSearch Index Resolution](#4-integration-with-opensearch-index-resolution)
5. [Catalog Connection Management](#5-catalog-connection-management)
6. [Schema Resolution and Caching](#6-schema-resolution-and-caching)
7. [Security Integration](#7-security-integration)
8. [Aliases and Index Patterns](#8-aliases-and-index-patterns)
9. [Limitations and Constraints](#9-limitations-and-constraints)

---

## 1. Overview and Motivation

### OpenSearch Today

An OpenSearch index is a collection of Lucene shards stored on local disk across the nodes of
the cluster. When a user queries an index, OpenSearch fans out the query to every shard, each
shard executes against its local Lucene segment files, and the coordinator merges the results.
The index lifecycle — create, reindex, delete, ISM rollover — is entirely managed by
OpenSearch. Data lives inside the cluster.

```
OpenSearch index "logs-2024"
  Shard 0  ← /var/data/nodes/0/indices/.../segments/
  Shard 1  ← /var/data/nodes/1/indices/.../segments/
  Shard 2  ← /var/data/nodes/2/indices/.../segments/
```

### The Problem

Lakehouse data lives outside OpenSearch. A typical deployment has petabytes of Iceberg tables
on S3, managed by AWS Glue, a Hive Metastore, or an Iceberg REST catalog. This data is
already queryable by Spark, Trino, and Athena — but not by OpenSearch's PPL/SQL query
surface.

Users who already use OpenSearch Dashboards for visualization, OpenSearch security for access
control, and PPL for analytics want to bring lakehouse data into the same query experience
without physically moving or reindexing it.

The naive approach — full reindex into Lucene — is untenable:
- Scale: petabyte tables cannot fit in a Lucene cluster optimized for search
- Freshness: Iceberg data is continuously updated by upstream pipelines; reindexing lag is
  unacceptable for analytical use cases
- Cost: S3 + Iceberg storage costs a fraction of Lucene shard storage at the same scale

### The Solution

We introduce the **lakehouse index**: a lightweight metadata record stored in OpenSearch
cluster state that maps an OpenSearch index name to an external Iceberg table. The lakehouse
index contains no Lucene shards and no local data. It is a pointer.

```
Lakehouse index "sales_orders"
  catalog_type:  GLUE
  catalog_uri:   (resolved from AWS region + account)
  database:      sales
  table:         orders
  s3_base_path:  s3://my-bucket/warehouse/sales/orders/
  credentials:   keystore:aws/sales_catalog
  ─────────────────────────────────────────
  actual data:   s3://my-bucket/warehouse/sales/orders/
                   └── year=2024/month=03/part-00042.parquet
                   └── year=2024/month=03/part-00043.parquet
                   └── ...  (managed entirely by Iceberg / upstream ETL)
```

When a user queries `sales_orders` with PPL or SQL, OpenSearch's query router detects that
`sales_orders` is a lakehouse index, resolves the Iceberg schema, builds a Calcite plan, and
dispatches to the DataFusion execution path (SQL → DataFusion workers → S3 Parquet) —
all transparent to the user.

### Benefits

| Capability | How It Is Preserved |
|---|---|
| PPL / SQL query surface | Queries reference the lakehouse index name; execution routes to DataFusion |
| OpenSearch Dashboards | Index patterns match lakehouse index names exactly like regular indices |
| Security plugin | Index-level, field-level, and document-level security rules apply by name |
| Aliases | Lakehouse indices participate in alias resolution the same way as regular indices |
| Cross-cluster search | `remote_cluster:sales_orders` works; the remote cluster resolves the pointer |
| Audit logging | All query access is logged against the lakehouse index name |
| No data movement | Data stays in S3; no reindex, no sync job, no duplication |

---

## 2. Lakehouse Index Metadata Model

### Where It Lives

Lakehouse index metadata is stored in `ClusterState.Metadata` alongside regular
`IndexMetadata` entries. It is replicated to every node via the cluster state mechanism —
the same transport path used for regular index settings, mappings, and aliases. No additional
distributed state store is required.

The metadata is **not** an `IndexMetadata` instance. It is a new top-level custom metadata
type registered via OpenSearch's `Custom` metadata extension point:

```
ClusterState
  └── Metadata
        ├── indices: Map<String, IndexMetadata>          ← regular indices (unchanged)
        └── customs: Map<String, Metadata.Custom>
              └── "lakehouse_indices": LakehouseIndicesMetadata
                    └── Map<String, LakehouseIndexMetadata>
                          ├── "sales_orders" → LakehouseIndexMetadata{...}
                          └── "product_catalog" → LakehouseIndexMetadata{...}
```

### Fields

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | `String` | yes | The OpenSearch index name (unique, matches map key) |
| `catalogType` | `CatalogType` enum | yes | `GLUE`, `HIVE`, or `REST` |
| `catalogUri` | `String` | conditional | Metastore URI (Hive) or REST catalog endpoint; null for GLUE (resolved from AWS config) |
| `database` | `String` | yes | Iceberg namespace / database name (e.g., `"sales"`) |
| `tableName` | `String` | yes | Iceberg table name within the database (e.g., `"orders"`) |
| `s3BasePath` | `String` | yes | S3 URI of the Iceberg table root (e.g., `s3://bucket/warehouse/sales/orders/`) |
| `credentialsRef` | `String` | yes | Key name in the OpenSearch keystore holding catalog credentials |
| `schemaCacheTtlSeconds` | `int` | no | TTL for the in-memory schema cache; default `300` (5 minutes) |
| `partitionSpecCache` | `String` (JSON) | no | Serialized Iceberg partition spec; populated on first access and on explicit refresh |
| `createdAt` | `Instant` | yes | Creation timestamp (set server-side, not user-supplied) |
| `lastRefreshedAt` | `Instant` | no | Timestamp of last successful schema refresh from the catalog |

**What is NOT stored here:**
- Actual Parquet data or Iceberg data files (these live in S3, managed by Iceberg)
- The resolved Iceberg schema (resolved lazily at query time, cached in memory — see §6)
- Query statistics or cardinality estimates (no catalog statistics are persisted in cluster state)
- Lucene mappings or shard routing tables (there are no shards)

### Java Classes

```java
package org.opensearch.lakehouse.index;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;

/**
 * Immutable metadata record for a single lakehouse index.
 * Stored in ClusterState.Metadata.customs under "lakehouse_indices".
 *
 * Serialized to/from cluster state via Writeable (binary, for transport)
 * and ToXContentObject (JSON, for REST and persistent cluster state snapshots).
 */
public final class LakehouseIndexMetadata implements Writeable, ToXContentObject {

    public enum CatalogType { GLUE, HIVE, REST }

    private final String name;
    private final CatalogType catalogType;
    private final String catalogUri;          // null for GLUE
    private final String database;
    private final String tableName;
    private final String s3BasePath;
    private final String credentialsRef;
    private final int schemaCacheTtlSeconds;
    private final String partitionSpecCache;  // JSON; null until first resolved
    private final Instant createdAt;
    private final Instant lastRefreshedAt;    // null until first refresh

    private LakehouseIndexMetadata(Builder builder) {
        this.name = builder.name;
        this.catalogType = builder.catalogType;
        this.catalogUri = builder.catalogUri;
        this.database = builder.database;
        this.tableName = builder.tableName;
        this.s3BasePath = builder.s3BasePath;
        this.credentialsRef = builder.credentialsRef;
        this.schemaCacheTtlSeconds = builder.schemaCacheTtlSeconds;
        this.partitionSpecCache = builder.partitionSpecCache;
        this.createdAt = builder.createdAt;
        this.lastRefreshedAt = builder.lastRefreshedAt;
    }

    // --- Writeable ---

    public LakehouseIndexMetadata(StreamInput in) throws IOException {
        this.name = in.readString();
        this.catalogType = in.readEnum(CatalogType.class);
        this.catalogUri = in.readOptionalString();
        this.database = in.readString();
        this.tableName = in.readString();
        this.s3BasePath = in.readString();
        this.credentialsRef = in.readString();
        this.schemaCacheTtlSeconds = in.readInt();
        this.partitionSpecCache = in.readOptionalString();
        this.createdAt = in.readInstant();
        this.lastRefreshedAt = in.readOptionalInstant();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeEnum(catalogType);
        out.writeOptionalString(catalogUri);
        out.writeString(database);
        out.writeString(tableName);
        out.writeString(s3BasePath);
        out.writeString(credentialsRef);
        out.writeInt(schemaCacheTtlSeconds);
        out.writeOptionalString(partitionSpecCache);
        out.writeInstant(createdAt);
        out.writeOptionalInstant(lastRefreshedAt);
    }

    // --- ToXContentObject ---

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("name", name);
        builder.field("catalog_type", catalogType.name().toLowerCase());
        if (catalogUri != null) builder.field("catalog_uri", catalogUri);
        builder.field("database", database);
        builder.field("table_name", tableName);
        builder.field("s3_base_path", s3BasePath);
        builder.field("credentials_ref", credentialsRef);
        builder.field("schema_cache_ttl_seconds", schemaCacheTtlSeconds);
        if (partitionSpecCache != null) builder.field("partition_spec_cache", partitionSpecCache);
        builder.timeField("created_at", createdAt.toEpochMilli());
        if (lastRefreshedAt != null) builder.timeField("last_refreshed_at", lastRefreshedAt.toEpochMilli());
        builder.endObject();
        return builder;
    }

    public static LakehouseIndexMetadata fromXContent(XContentParser parser) throws IOException {
        // Delegates to Builder, parsing each field by name.
        throw new UnsupportedOperationException("implement in LakehouseIndexMetadata");
    }

    // --- Builder ---

    public static Builder builder(String name) { return new Builder(name); }

    public Builder toBuilder() { return new Builder(this); }

    public static final class Builder {
        private String name;
        private CatalogType catalogType;
        private String catalogUri;
        private String database;
        private String tableName;
        private String s3BasePath;
        private String credentialsRef;
        private int schemaCacheTtlSeconds = 300;
        private String partitionSpecCache;
        private Instant createdAt = Instant.now();
        private Instant lastRefreshedAt;

        private Builder(String name) { this.name = name; }
        private Builder(LakehouseIndexMetadata existing) { /* copy all fields */ }

        public Builder catalogType(CatalogType v) { this.catalogType = v; return this; }
        public Builder catalogUri(String v) { this.catalogUri = v; return this; }
        public Builder database(String v) { this.database = v; return this; }
        public Builder tableName(String v) { this.tableName = v; return this; }
        public Builder s3BasePath(String v) { this.s3BasePath = v; return this; }
        public Builder credentialsRef(String v) { this.credentialsRef = v; return this; }
        public Builder schemaCacheTtlSeconds(int v) { this.schemaCacheTtlSeconds = v; return this; }
        public Builder partitionSpecCache(String v) { this.partitionSpecCache = v; return this; }
        public Builder lastRefreshedAt(Instant v) { this.lastRefreshedAt = v; return this; }

        public LakehouseIndexMetadata build() {
            if (name == null || catalogType == null || database == null
                    || tableName == null || s3BasePath == null || credentialsRef == null) {
                throw new IllegalStateException("name, catalogType, database, tableName, " +
                    "s3BasePath, and credentialsRef are required");
            }
            return new LakehouseIndexMetadata(this);
        }
    }

    // --- Getters (all fields have a getter; omitted for brevity) ---
    public String getName() { return name; }
    public CatalogType getCatalogType() { return catalogType; }
    public String getCatalogUri() { return catalogUri; }
    public String getDatabase() { return database; }
    public String getTableName() { return tableName; }
    public String getS3BasePath() { return s3BasePath; }
    public String getCredentialsRef() { return credentialsRef; }
    public int getSchemaCacheTtlSeconds() { return schemaCacheTtlSeconds; }
    public String getPartitionSpecCache() { return partitionSpecCache; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getLastRefreshedAt() { return lastRefreshedAt; }
}
```

### Container: `LakehouseIndicesMetadata`

All `LakehouseIndexMetadata` entries for the cluster are held in a single `Custom` metadata
object that participates in cluster state diffing and serialization:

```java
package org.opensearch.lakehouse.index;

import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * ClusterState.Metadata.Custom that holds all lakehouse index metadata.
 * Registered under the key "lakehouse_indices".
 *
 * Immutable; mutations produce a new instance via toBuilder().
 */
public final class LakehouseIndicesMetadata implements Metadata.Custom {

    public static final String TYPE = "lakehouse_indices";

    private final Map<String, LakehouseIndexMetadata> indices;

    public LakehouseIndicesMetadata(Map<String, LakehouseIndexMetadata> indices) {
        this.indices = Collections.unmodifiableMap(indices);
    }

    public LakehouseIndicesMetadata(StreamInput in) throws IOException {
        int size = in.readVInt();
        Map<String, LakehouseIndexMetadata> m = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            LakehouseIndexMetadata meta = new LakehouseIndexMetadata(in);
            m.put(meta.getName(), meta);
        }
        this.indices = Collections.unmodifiableMap(m);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(indices.size());
        for (LakehouseIndexMetadata meta : indices.values()) {
            meta.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("lakehouse_indices");
        for (LakehouseIndexMetadata meta : indices.values()) {
            builder.field(meta.getName());
            meta.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    public static LakehouseIndicesMetadata fromXContent(XContentParser parser) throws IOException {
        throw new UnsupportedOperationException("implement in LakehouseIndicesMetadata");
    }

    @Override
    public String getWriteableName() { return TYPE; }

    public Map<String, LakehouseIndexMetadata> getIndices() { return indices; }

    public boolean hasIndex(String name) { return indices.containsKey(name); }

    public LakehouseIndexMetadata getIndex(String name) { return indices.get(name); }

    /** Returns a new instance with the given entry added or replaced. */
    public LakehouseIndicesMetadata with(LakehouseIndexMetadata meta) {
        Map<String, LakehouseIndexMetadata> copy = new HashMap<>(indices);
        copy.put(meta.getName(), meta);
        return new LakehouseIndicesMetadata(copy);
    }

    /** Returns a new instance with the given entry removed. */
    public LakehouseIndicesMetadata without(String name) {
        Map<String, LakehouseIndexMetadata> copy = new HashMap<>(indices);
        copy.remove(name);
        return new LakehouseIndicesMetadata(copy);
    }
}
```

---

## 3. REST API for Managing Lakehouse Indices

All management operations go through a dedicated REST namespace `/_lakehouse`. These
endpoints are registered by the `LakehousePlugin` via `getRestHandlers()`.

### 3.1 Endpoint Summary

| Method | Path | Action |
|---|---|---|
| `PUT` | `/_lakehouse/{index_name}` | Create a new lakehouse index |
| `GET` | `/_lakehouse/{index_name}` | Retrieve metadata for one index |
| `DELETE` | `/_lakehouse/{index_name}` | Remove a lakehouse index |
| `POST` | `/_lakehouse/{index_name}/_refresh` | Force schema cache refresh from catalog |
| `GET` | `/_lakehouse` | List all lakehouse indices |

### 3.2 `PUT /_lakehouse/{index_name}` — Create

Creates a new lakehouse index. Fails with `400` if a regular OpenSearch index or another
lakehouse index with the same name already exists.

**Request body:**

```json
{
  "catalog": {
    "type": "glue",
    "database": "sales",
    "table": "orders",
    "s3_base_path": "s3://my-data-lake/warehouse/sales/orders/",
    "credentials_ref": "lakehouse.aws.sales_catalog",
    "schema_cache_ttl_seconds": 300
  }
}
```

For Hive or REST catalogs, include `catalog_uri`:

```json
{
  "catalog": {
    "type": "rest",
    "catalog_uri": "https://my-iceberg-catalog.example.com/",
    "database": "analytics",
    "table": "events",
    "s3_base_path": "s3://my-bucket/warehouse/analytics/events/",
    "credentials_ref": "lakehouse.rest.analytics_catalog"
  }
}
```

**Response `200 OK`:**

```json
{
  "acknowledged": true,
  "index": "sales_orders",
  "catalog_type": "glue",
  "database": "sales",
  "table_name": "orders",
  "s3_base_path": "s3://my-data-lake/warehouse/sales/orders/",
  "created_at": "2026-04-06T10:00:00Z"
}
```

**Error responses:**

| Status | Condition |
|---|---|
| `400` | Missing required field, unsupported catalog type, or malformed S3 URI |
| `409` | An index (regular or lakehouse) with this name already exists |
| `403` | Caller lacks `cluster:admin/lakehouse/create` privilege |

### 3.3 `GET /_lakehouse/{index_name}` — Get

Returns the stored metadata for a single lakehouse index. Does not contact the Iceberg
catalog; returns only what is persisted in cluster state.

**Response `200 OK`:**

```json
{
  "sales_orders": {
    "catalog_type": "glue",
    "database": "sales",
    "table_name": "orders",
    "s3_base_path": "s3://my-data-lake/warehouse/sales/orders/",
    "credentials_ref": "lakehouse.aws.sales_catalog",
    "schema_cache_ttl_seconds": 300,
    "created_at": "2026-04-06T10:00:00Z",
    "last_refreshed_at": "2026-04-06T10:30:00Z"
  }
}
```

**Error responses:**

| Status | Condition |
|---|---|
| `404` | No lakehouse index with this name exists |
| `403` | Caller lacks `indices:admin/get` on the index name |

### 3.4 `DELETE /_lakehouse/{index_name}` — Delete

Removes the lakehouse index metadata from cluster state. Does not affect the underlying
Iceberg table or any data in S3.

**Response `200 OK`:**

```json
{
  "acknowledged": true,
  "index": "sales_orders"
}
```

### 3.5 `POST /_lakehouse/{index_name}/_refresh` — Refresh Schema Cache

Forces a synchronous schema refresh: contacts the Iceberg catalog, reads the current table
metadata, updates the in-memory schema cache (§6), and stores the resolved partition spec in
cluster state.

**Response `200 OK`:**

```json
{
  "acknowledged": true,
  "index": "sales_orders",
  "schema_fields": 24,
  "partition_spec": "year/month",
  "snapshot_id": 8794561234567890,
  "refreshed_at": "2026-04-06T11:00:00Z"
}
```

**Error responses:**

| Status | Condition |
|---|---|
| `503` | Could not reach the Iceberg catalog (timeout, auth failure, etc.) |
| `404` | The Iceberg table no longer exists at the configured location |

### 3.6 `GET /_lakehouse` — List All

Returns a summary of all lakehouse indices registered in the cluster.

**Response `200 OK`:**

```json
{
  "lakehouse_indices": [
    {
      "name": "sales_orders",
      "catalog_type": "glue",
      "database": "sales",
      "table_name": "orders",
      "created_at": "2026-04-06T10:00:00Z",
      "last_refreshed_at": "2026-04-06T10:30:00Z"
    },
    {
      "name": "product_catalog",
      "catalog_type": "rest",
      "database": "analytics",
      "table_name": "products",
      "created_at": "2026-04-05T08:00:00Z",
      "last_refreshed_at": null
    }
  ],
  "total": 2
}
```

### 3.7 Cluster State Update Path

All write operations (`PUT`, `DELETE`, `POST /_refresh`) go through a cluster state update
task submitted to the master node, using `ClusterService.submitStateUpdateTask`. The task
updates the `LakehouseIndicesMetadata` custom within a `ClusterStateUpdateTask` and blocks
until the new state is acknowledged by a quorum.

```
REST handler receives request
    │
    ▼
Validate request parameters
    │
    ▼
LakehouseIndexService.create(request)  (or delete, refresh)
    │
    ▼
clusterService.submitStateUpdateTask(
    "create-lakehouse-index[" + indexName + "]",
    new ClusterStateUpdateTask() {
        @Override
        public ClusterState execute(ClusterState current) {
            LakehouseIndicesMetadata existing =
                current.metadata().custom(LakehouseIndicesMetadata.TYPE);
            LakehouseIndicesMetadata updated = existing.with(newMetadata);
            return ClusterState.builder(current)
                .metadata(Metadata.builder(current.metadata())
                    .putCustom(LakehouseIndicesMetadata.TYPE, updated)
                    .build())
                .build();
        }
    }
)
    │
    ▼
Wait for acknowledgement → respond to client
```

---

## 4. Integration with OpenSearch Index Resolution

### 4.1 The Routing Decision

When a PPL or SQL query arrives, the query dispatch layer must determine which execution
path to use. The determination is based solely on whether the target index name is registered
as a lakehouse index in cluster state.

```
Incoming query: "source=sales_orders | where amount > 100"
                              │
                              ▼
                  IndexTypeResolver.resolve("sales_orders")
                              │
              ┌───────────────┴──────────────────┐
              │                                  │
     LAKEHOUSE_INDEX                      REGULAR_INDEX
              │                                  │
              ▼                                  ▼
  LakehouseQueryRouter                  Existing OpenSearch
  → LakehouseContextFactory             query execution path
  → UnifiedQueryPlanner                 (Lucene shards, etc.)
  → SqlProducer
  → DataFusion workers
```

### 4.2 `IndexTypeResolver` Interface

```java
package org.opensearch.lakehouse.index;

/**
 * Determines the execution path for a query targeting a named index.
 *
 * Checked at query dispatch time (before planning). The resolution reads
 * only from local cluster state — no network calls, no I/O.
 */
public interface IndexTypeResolver {

    enum IndexType {
        /** Standard OpenSearch index backed by Lucene shards. */
        REGULAR,

        /** Lakehouse index: metadata pointer to an external Iceberg table. */
        LAKEHOUSE,

        /** Index does not exist (neither regular nor lakehouse). */
        NOT_FOUND
    }

    /**
     * Resolves the type of the given index name.
     *
     * For wildcard/pattern names (e.g., "sales_*"), this method checks if ALL
     * matched indices are of the same type. Mixed patterns (some regular, some
     * lakehouse) return MIXED — see LakehouseAliasResolver for handling.
     *
     * @param indexName the OpenSearch index name or alias to resolve
     * @return the type of the index
     */
    IndexType resolve(String indexName);
}
```

### 4.3 `ClusterStateLakehouseIndexTypeResolver` Implementation

```java
package org.opensearch.lakehouse.index;

import org.opensearch.cluster.service.ClusterService;

/**
 * Resolves index types by inspecting live cluster state.
 * Thread-safe: reads are lock-free (cluster state is immutable snapshots).
 */
public class ClusterStateLakehouseIndexTypeResolver implements IndexTypeResolver {

    private final ClusterService clusterService;

    public ClusterStateLakehouseIndexTypeResolver(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public IndexType resolve(String indexName) {
        var state = clusterService.state();

        // Check lakehouse indices first (custom metadata).
        LakehouseIndicesMetadata lakehouseMeta =
            state.metadata().custom(LakehouseIndicesMetadata.TYPE);
        if (lakehouseMeta != null && lakehouseMeta.hasIndex(indexName)) {
            return IndexType.LAKEHOUSE;
        }

        // Check regular indices (standard metadata).
        if (state.metadata().hasIndex(indexName)) {
            return IndexType.REGULAR;
        }

        // Check aliases (may point to either type — resolved by LakehouseAliasResolver).
        if (state.metadata().hasAlias(indexName)) {
            return resolveAlias(indexName, state, lakehouseMeta);
        }

        return IndexType.NOT_FOUND;
    }

    private IndexType resolveAlias(String alias, var state, LakehouseIndicesMetadata lakehouse) {
        // Walk alias targets; return LAKEHOUSE if all targets are lakehouse,
        // REGULAR if all are regular, or raise for mixed aliases (§8).
        throw new UnsupportedOperationException("implement in ClusterStateLakehouseIndexTypeResolver");
    }
}
```

### 4.4 Integration Point in Query Dispatch

The `IndexTypeResolver` is injected into the existing `QueryService` (or equivalent dispatch
layer). The check is a fast in-memory read of local cluster state — it adds no latency to the
critical query path.

```java
// Inside QueryService.execute() or equivalent dispatcher:

IndexTypeResolver.IndexType type = indexTypeResolver.resolve(targetIndex);

switch (type) {
    case REGULAR:
        // Existing path: Lucene shard fan-out
        return existingQueryEngine.execute(request);

    case LAKEHOUSE:
        // New path: Iceberg → SQL → DataFusion
        return lakehouseQueryRouter.route(request);

    case NOT_FOUND:
        throw new IndexNotFoundException(targetIndex);
}
```

---

## 5. Catalog Connection Management

### 5.1 Overview

Catalog connections are long-lived, pooled, and shared across queries. The
`CatalogConnectionManager` is a node-level singleton instantiated by `LakehousePlugin` at
startup. It holds one connection pool per distinct `(catalogType, credentialsRef)` pair so
that two lakehouse indices pointing to the same Glue account share one connection.

```
CatalogConnectionManager (singleton, node-scoped)
  ├── pool[GLUE, "lakehouse.aws.sales_catalog"]   → GlueCatalogClient (pooled)
  ├── pool[HIVE, "lakehouse.hive.analytics"]      → HiveMetastoreClient (pooled)
  └── pool[REST, "lakehouse.rest.streaming"]      → IcebergRestCatalogClient (pooled)
```

### 5.2 `CatalogConnectionManager` Interface

```java
package org.opensearch.lakehouse.catalog;

import org.apache.iceberg.catalog.Catalog;
import org.opensearch.lakehouse.index.LakehouseIndexMetadata;

/**
 * Manages pooled connections to Iceberg catalogs.
 * Resolves credentials from the OpenSearch keystore at connection time.
 * Handles credential rotation by rebuilding connections when the keystore entry changes.
 */
public interface CatalogConnectionManager extends java.io.Closeable {

    /**
     * Returns a ready-to-use Iceberg Catalog for the given lakehouse index.
     * Connections are cached by (catalogType, credentialsRef); multiple calls
     * with the same pair return the same underlying connection object.
     *
     * @param meta the lakehouse index metadata describing the catalog
     * @return an Iceberg Catalog client (thread-safe, reusable across calls)
     * @throws CatalogConnectionException if credentials are missing or the catalog is unreachable
     */
    Catalog getCatalog(LakehouseIndexMetadata meta) throws CatalogConnectionException;

    /**
     * Tests connectivity to the catalog referenced by the given metadata.
     * Called during PUT /_lakehouse to validate configuration before persisting.
     *
     * @param meta the metadata to validate
     * @throws CatalogConnectionException if the catalog cannot be reached
     */
    void validateConnection(LakehouseIndexMetadata meta) throws CatalogConnectionException;

    /**
     * Invalidates and rebuilds the connection for the given credentials reference.
     * Called when a keystore entry is updated (credential rotation).
     *
     * @param credentialsRef the keystore key whose credentials have changed
     */
    void rotateCredentials(String credentialsRef);

    /**
     * Returns health status of all active catalog connections.
     * Used by /_cluster/health and the observability stack.
     */
    java.util.Map<String, CatalogHealthStatus> getHealthStatus();
}
```

### 5.3 Supported Catalog Types

#### AWS Glue Catalog

```java
package org.opensearch.lakehouse.catalog;

import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Catalog;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

/**
 * Builds an Iceberg GlueCatalog using credentials from the OpenSearch keystore.
 *
 * Credential lookup: the keystore entry at credentialsRef must contain JSON:
 *   { "aws_access_key_id": "...", "aws_secret_access_key": "...", "aws_region": "us-east-1" }
 *   OR
 *   { "iam_role_arn": "arn:aws:iam::123456789012:role/LakehouseQueryRole" }
 *
 * For IAM role ARNs, the connection manager uses STS AssumeRole.
 */
public class GlueCatalogFactory implements CatalogFactory {

    @Override
    public Catalog create(String credentialsRef, KeystoreService keystore) {
        GlueCredentials creds = keystore.loadJson(credentialsRef, GlueCredentials.class);
        AwsCredentialsProvider provider = creds.hasRoleArn()
            ? StsAssumeRoleCredentialsProvider.create(creds.getRoleArn())
            : StaticCredentialsProvider.create(
                AwsBasicCredentials.create(creds.getAccessKeyId(), creds.getSecretAccessKey()));

        GlueCatalog catalog = new GlueCatalog();
        catalog.initialize("glue", Map.of(
            "warehouse", "s3://placeholder/",  // overridden per-table by s3BasePath
            "io-impl", "org.apache.iceberg.aws.s3.S3FileIO",
            "aws.region", creds.getRegion()
        ));
        return catalog;
    }
}
```

#### Hive Metastore

```java
package org.opensearch.lakehouse.catalog;

/**
 * Builds an Iceberg HiveCatalog connecting to a Hive Metastore Thrift service.
 *
 * Credential lookup: keystore entry must contain:
 *   { "metastore_uri": "thrift://hive-metastore:9083",
 *     "kerberos_principal": "hive/...",   // optional, for kerberized HMS
 *     "kerberos_keytab": "base64-encoded-keytab"  // optional
 *   }
 */
public class HiveCatalogFactory implements CatalogFactory {

    @Override
    public Catalog create(String credentialsRef, KeystoreService keystore) {
        HiveCredentials creds = keystore.loadJson(credentialsRef, HiveCredentials.class);
        HiveCatalog catalog = new HiveCatalog();
        catalog.initialize("hive", Map.of(
            "uri", creds.getMetastoreUri(),
            "warehouse", "s3://placeholder/"
        ));
        return catalog;
    }
}
```

#### Iceberg REST Catalog

```java
package org.opensearch.lakehouse.catalog;

/**
 * Builds an Iceberg RESTCatalog connecting to any Iceberg REST catalog endpoint
 * (e.g., Polaris, Nessie, Tabular).
 *
 * Credential lookup: keystore entry must contain:
 *   { "uri": "https://catalog.example.com/",
 *     "credential": "client_id:client_secret",  // OAuth2 client credentials
 *     "warehouse": "my_warehouse"                // optional, REST catalog-specific
 *   }
 */
public class RestCatalogFactory implements CatalogFactory {

    @Override
    public Catalog create(String credentialsRef, KeystoreService keystore) {
        RestCredentials creds = keystore.loadJson(credentialsRef, RestCredentials.class);
        RESTCatalog catalog = new RESTCatalog();
        catalog.initialize("rest", Map.of(
            "uri", creds.getUri(),
            "credential", creds.getCredential(),
            "warehouse", creds.getWarehouse()
        ));
        return catalog;
    }
}
```

### 5.4 Credential Storage in OpenSearch Keystore

Credentials are stored as JSON strings in the OpenSearch keystore under user-defined keys:

```bash
# Add AWS credentials for the sales catalog
bin/opensearch-keystore add-string lakehouse.aws.sales_catalog <<'EOF'
{
  "aws_access_key_id": "AKIA...",
  "aws_secret_access_key": "...",
  "aws_region": "us-east-1"
}
EOF

# Add an IAM role for cross-account access
bin/opensearch-keystore add-string lakehouse.aws.cross_account <<'EOF'
{
  "iam_role_arn": "arn:aws:iam::999999999999:role/DataLakeReadOnly",
  "aws_region": "us-west-2"
}
EOF
```

The keystore keys are referenced in the `credentialsRef` field of `LakehouseIndexMetadata`.
They are **never** returned in REST API responses.

---

## 6. Schema Resolution and Caching

### 6.1 Lazy Resolution

Lakehouse index creation does not read the Iceberg schema. Schema resolution is deferred to
the first query that targets the index. This avoids blocking the `PUT /_lakehouse` request on
catalog availability and keeps cluster state lean.

Resolution steps on first query:

```
1. CatalogConnectionManager.getCatalog(meta)
       → Iceberg Catalog client (from pool)

2. catalog.loadTable(TableIdentifier.of(meta.getDatabase(), meta.getTableName()))
       → org.apache.iceberg.Table

3. table.schema()
       → org.apache.iceberg.Schema (fields + types)

4. table.spec()
       → org.apache.iceberg.PartitionSpec (partition columns)

5. table.currentSnapshot()
       → org.apache.iceberg.Snapshot (snapshotId, timestamp, manifest list location)

6. SchemaCache.put(meta.getName(), resolvedSchema, ttl)
       → cached for subsequent queries
```

### 6.2 `SchemaCache` Interface

```java
package org.opensearch.lakehouse.schema;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;

import java.util.Optional;

/**
 * Node-local in-memory cache for resolved Iceberg schemas.
 * Backed by a Caffeine cache with per-entry TTL.
 *
 * Cache keys are OpenSearch lakehouse index names.
 * Cache values are resolved Iceberg Table objects (schema + partition spec).
 *
 * The cache is node-local — each coordinator node maintains its own.
 * Cache coherence across nodes is achieved via TTL expiry and explicit invalidation
 * propagated through the cluster state (partition spec cache field in metadata).
 */
public interface SchemaCache {

    /**
     * Returns the cached Iceberg Table for the given lakehouse index, if present and not expired.
     *
     * @param indexName the lakehouse index name
     * @return the cached Table, or empty if not present or expired
     */
    Optional<Table> get(String indexName);

    /**
     * Stores the resolved Iceberg Table in the cache with the index's configured TTL.
     *
     * @param indexName the lakehouse index name
     * @param table     the resolved Iceberg Table
     * @param ttlSeconds the TTL in seconds (from LakehouseIndexMetadata.schemaCacheTtlSeconds)
     */
    void put(String indexName, Table table, int ttlSeconds);

    /**
     * Removes the cache entry for the given index.
     * Called on: explicit /_refresh, schema evolution detection, index deletion.
     *
     * @param indexName the lakehouse index name to invalidate
     */
    void invalidate(String indexName);

    /**
     * Returns cache statistics for observability (hit rate, entry count, etc.).
     */
    SchemaCacheStats stats();
}
```

### 6.3 Cache Invalidation Triggers

| Trigger | Mechanism |
|---|---|
| TTL expiry | Caffeine expiry; next query triggers a fresh catalog read |
| `POST /_lakehouse/{name}/_refresh` | `SchemaCache.invalidate(name)` called by the REST handler on all nodes via a broadcast cluster state update |
| Schema evolution detected | If Iceberg table's `lastUpdatedMs` differs from the cached Table's metadata; invalidation happens inside `SchemaCache.get()` after a background staleness check |
| Lakehouse index deleted | `SchemaCache.invalidate(name)` called during the `DELETE` cluster state task |

### 6.4 Schema Evolution Handling

Iceberg supports backward-compatible (additive) schema evolution without breaking existing
queries:

| Change | Impact on cached schema | Action required |
|---|---|---|
| Add column | New column missing from cache until refresh | Queries referencing new column fail with `column not found` until cache refreshes via TTL or explicit refresh |
| Drop column | Old column present in cache | Query references old column → execution reads missing column → DataFusion returns null for that column |
| Rename column | Treated as drop + add | Old name present in cache; Iceberg field IDs maintain physical mapping |
| Change column type (compatible) | Old type in cache | May cause a type mismatch at execution; explicit refresh recommended |
| Change column type (incompatible) | Old type in cache | Execution error; explicit refresh required |

For production use, schema changes should be preceded by a `POST /_lakehouse/{name}/_refresh`
to flush stale cache state before queries resume.

---

## 7. Security Integration

Lakehouse indices participate in OpenSearch's security model identically to regular indices.
The Security plugin intercepts requests by index name — it does not distinguish between
regular and lakehouse indices. This means all existing security configuration (roles,
role mappings, FLS, DLS) applies to lakehouse index names without any additional setup.

### 7.1 Index-Level Permissions

Users must have `indices:data/read` on the lakehouse index name to query it.

```json
// Example role granting read access to the sales_orders lakehouse index
{
  "cluster_permissions": [],
  "index_permissions": [
    {
      "index_patterns": ["sales_orders", "sales_*"],
      "allowed_actions": ["indices:data/read", "indices:admin/get"]
    }
  ]
}
```

The permission check happens before the `IndexTypeResolver` is consulted — the Security plugin
intercepts at the transport layer by index name, so a user without `indices:data/read` on
`sales_orders` never reaches the lakehouse execution path.

### 7.2 Field-Level Security (FLS)

When a user's role has FLS rules on a lakehouse index, the restricted fields are intercepted
before the SQL plan is constructed. The `LakehouseSecurityFilter` examines the resolved
FLS configuration for the requesting user and removes restricted columns from the Iceberg
schema that is handed to `LakehouseCalciteSchema`.

```
User query: "source=sales_orders | fields customer_id, amount, ssn"

FLS rule for user's role:
  index: sales_orders
  excluded_fields: ["ssn", "credit_card_number"]

LakehouseSecurityFilter.applyFLS(schema, flsConfig)
  → removes "ssn" from the Iceberg schema fields visible to Calcite
  → Calcite planning proceeds with reduced schema
  → any reference to "ssn" in the query → column not found error
  → SQL plan never contains a projection of "ssn"
  → DataFusion never reads the "ssn" column from Parquet
```

The FLS filter operates at the **schema registration** level: the `LakehouseCalciteSchema`
that is registered into `UnifiedQueryContext` has already had restricted fields removed. This
means the SQL plan emitted by Component 2 physically cannot reference those fields — the
enforcement is at plan construction time, not at result filtering time.

```java
package org.opensearch.lakehouse.security;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Applies OpenSearch Security field-level security rules to an Iceberg schema.
 * Returns a new Schema with restricted fields removed.
 *
 * Called by LakehouseContextFactory before constructing LakehouseCalciteSchema.
 */
public class LakehouseSecurityFilter {

    /**
     * Returns a filtered schema with FLS-excluded fields removed.
     *
     * @param originalSchema the full Iceberg schema from the catalog
     * @param excludedFields set of field names the user cannot access
     * @return a new Iceberg Schema with excluded fields removed
     */
    public Schema applyFLS(Schema originalSchema, Set<String> excludedFields) {
        if (excludedFields.isEmpty()) return originalSchema;

        List<Types.NestedField> allowed = originalSchema.columns().stream()
            .filter(f -> !excludedFields.contains(f.name()))
            .collect(Collectors.toList());

        return new Schema(allowed);
    }

    /**
     * Injects a DLS filter predicate into the query's WHERE clause.
     * The DLS filter is expressed as an Iceberg Expression and appended to
     * the SQL plan's filter via SqlProducer (Component 2).
     *
     * @param dlsQuery    the DLS filter expression (e.g., "region:us-east-1")
     * @param indexName   the lakehouse index name
     * @return an Iceberg Expression equivalent of the DLS query
     */
    public org.apache.iceberg.expressions.Expression buildDLSFilter(
            String dlsQuery, String indexName) {
        // Translates the DLS query (OpenSearch query DSL) to an Iceberg Expression.
        // For simple term/range filters, this is a direct translation.
        // Complex query DSL expressions not expressible as Iceberg predicates
        // are applied as a post-scan filter in the SQL plan instead.
        throw new UnsupportedOperationException("implement in LakehouseSecurityFilter");
    }
}
```

### 7.3 Document-Level Security (DLS)

DLS rules on a lakehouse index specify which documents (rows) a user may see. For lakehouse
indices, DLS filters are injected as additional predicates in the SQL WHERE clause.

```
User query: "source=sales_orders | stats sum(amount) by region"

DLS rule for user's role:
  index: sales_orders
  query: { "term": { "region": "us-east-1" } }

Effective query plan:
  source=sales_orders
  | where region = 'us-east-1'   ← DLS filter injected
  | stats sum(amount) by region
```

The injected DLS filter is appended as an additional `AND` predicate in the SQL `WHERE` clause before
dispatch to DataFusion workers. Workers physically see only the rows matching the DLS
filter — the filter cannot be bypassed by the user.

### 7.4 Audit Logging

All lakehouse query executions are logged via the Security plugin's audit log infrastructure.
Audit events for lakehouse queries contain:

| Audit Field | Value |
|---|---|
| `audit_category` | `GRANTED_PRIVILEGES` |
| `audit_request_indices` | `["sales_orders"]` |
| `audit_request_privilege` | `indices:data/read` |
| `audit_trace_user` | requesting user |
| `audit_trace_task_name` | `ppl` or `sql` |
| `audit_request_body` | the query text (if `audit.request.resolve_indices` is enabled) |
| `@timestamp` | query execution time |

No additional audit integration is required — the Security plugin intercepts by index name
before the lakehouse path is entered.

---

## 8. Aliases and Index Patterns

### 8.1 Lakehouse Index Aliases

Lakehouse indices support OpenSearch aliases through the standard alias API. Alias metadata
is stored in cluster state alongside the `LakehouseIndicesMetadata`, using the same alias
storage mechanism as regular indices. Aliases are stored separately from the lakehouse
metadata itself — the alias → index resolution is handled by the existing `AliasMetadata`
infrastructure.

Create an alias pointing to a lakehouse index:

```
POST /_aliases
{
  "actions": [
    {
      "add": {
        "index": "sales_orders",
        "alias": "orders"
      }
    }
  ]
}
```

Query through the alias — the `IndexTypeResolver` resolves the alias to `sales_orders`,
detects it is a lakehouse index, and routes accordingly:

```
source=orders | where amount > 100
→ alias "orders" → "sales_orders" → LAKEHOUSE → DataFusion path
```

### 8.2 Mixed Aliases (Regular + Lakehouse)

An alias may span both regular and lakehouse indices. When a query targets such an alias, the
`LakehouseAliasResolver` splits the alias targets by type and routes each to the appropriate
engine, then merges results on the coordinator.

```
Alias "all_orders":
  → "orders_legacy"   (regular index, Lucene shards)
  → "sales_orders"    (lakehouse index, Iceberg/S3)

Query: "source=all_orders | stats count() by status"
  → LakehouseAliasResolver.split("all_orders")
      → regular_targets:  ["orders_legacy"]
      → lakehouse_targets: ["sales_orders"]
  → Execute in parallel:
      path A: OpenSearch Lucene query on "orders_legacy"
      path C: SQL/DataFusion on "sales_orders"
  → Merge partial aggregations on coordinator
  → Return combined result
```

Mixed alias queries with aggregations require coordinator-side partial aggregation merge.
This is handled by the existing distributed query merge infrastructure; no new code is needed
for simple aggregations (COUNT, SUM, MIN, MAX). Complex aggregations (PERCENTILE, HLL) may
require special merge logic.

### 8.3 Dashboards Index Patterns

OpenSearch Dashboards index patterns work with lakehouse indices by name. When a user creates
an index pattern in Dashboards:

```
Index pattern: sales_*
Matched indices: sales_orders, sales_returns, sales_customers
```

The `GET /_cat/indices?v` and `GET /_resolve/index/{pattern}` APIs are extended to include
lakehouse indices in their responses. The `LakehouseIndicesMetadata` custom is consulted
alongside regular `IndexMetadata` when resolving wildcard patterns.

```java
package org.opensearch.lakehouse.index;

import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.regex.Regex;

import java.util.ArrayList;
import java.util.List;

/**
 * Extends the standard index resolver to include lakehouse indices
 * when resolving wildcard patterns.
 *
 * Used by GET /_resolve/index/{pattern} and index pattern discovery
 * in OpenSearch Dashboards.
 */
public class LakehouseIndexResolver {

    private final Metadata clusterMetadata;

    public LakehouseIndexResolver(Metadata clusterMetadata) {
        this.clusterMetadata = clusterMetadata;
    }

    /**
     * Returns all lakehouse index names matching the given pattern.
     *
     * @param pattern index name or wildcard pattern (e.g., "sales_*", "*")
     * @return list of matching lakehouse index names
     */
    public List<String> resolvePattern(String pattern) {
        LakehouseIndicesMetadata meta =
            clusterMetadata.custom(LakehouseIndicesMetadata.TYPE);
        if (meta == null) return List.of();

        List<String> matches = new ArrayList<>();
        for (String name : meta.getIndices().keySet()) {
            if (Regex.simpleMatch(pattern, name)) {
                matches.add(name);
            }
        }
        return matches;
    }
}
```

### 8.4 Cross-Cluster Search

Lakehouse indices on remote clusters are accessible via cross-cluster search using the
standard `cluster:index_name` notation:

```
source=analytics_cluster:sales_orders | where amount > 1000
```

The local coordinator forwards the query to the remote cluster using the existing cross-cluster
transport. The remote cluster's `IndexTypeResolver` identifies `sales_orders` as a lakehouse
index and routes to its local DataFusion workers. Results are returned to the local coordinator
in Arrow IPC format and merged with any local results.

No changes are required to the cross-cluster transport layer — routing decisions are made
independently on each cluster.

---

## 9. Limitations and Constraints

### 9.1 Read-Only

Lakehouse indices are strictly read-only through this abstraction. There is no write path:

- `PUT /sales_orders/_doc/{id}` → `MethodNotAllowedException`
- `POST /sales_orders/_bulk` → `MethodNotAllowedException`
- `DELETE /sales_orders/_doc/{id}` → `MethodNotAllowedException`
- Index template auto-creation does not apply to lakehouse indices

Data mutations to the underlying Iceberg table are performed externally (Spark jobs, Flink
pipelines, Iceberg's Java API) and become visible to OpenSearch queries on the next schema
refresh or TTL expiry.

### 9.2 No Real-Time Data

Queries read from the most recent committed Iceberg snapshot at the time the query is planned.
Data written to Iceberg but not yet committed to a snapshot is invisible. Typical Iceberg
snapshot lag is minutes to hours depending on the upstream pipeline commit frequency.

Time travel (`AS OF TIMESTAMP` / `FOR SYSTEM_TIME AS OF`) is supported by passing the target
snapshot ID in the SQL plan metadata or stage configuration (see §3.3 of the architecture plan).
This is a Component 4 (Stage Splitter) concern, not a Component 0 concern.

### 9.3 Schema Evolution

| Change | Handling |
|---|---|
| Add column | Supported. Existing queries are unaffected; new column visible after cache refresh |
| Drop column | Supported by Iceberg (field ID–based). Queries referencing dropped column return null after cache refresh |
| Rename column | Treated as add + drop at the schema level. Iceberg field IDs preserve physical data mapping |
| Widen column type (e.g., INT → LONG) | Supported. New type visible after cache refresh |
| Narrow column type (e.g., DOUBLE → FLOAT) | Not recommended; may cause runtime type errors |
| Reorder columns | Transparent; Iceberg is column-position–independent |
| Partition spec change | Requires explicit `POST /_lakehouse/{name}/_refresh` to update the cached partition spec in cluster state |

### 9.4 No ISM Lifecycle Policies

Index State Management (ISM) policies that roll over, delete, or transition shards do not
apply to lakehouse indices. There are no shards to manage. Data lifecycle for lakehouse
tables is managed entirely by external tools (Iceberg's `ExpireSnapshots`, compaction jobs,
TTL tables in the Iceberg catalog).

### 9.5 Feature Compatibility Matrix

| OpenSearch Feature | Compatible with Lakehouse Index |
|---|---|
| PPL queries | Yes |
| SQL queries | Yes |
| OpenSearch query DSL (`_search`) | No — routed to Lucene; lakehouse indices have no Lucene shards |
| Aggregations via `_search` | No |
| Field mappings (`GET /_mapping`) | Partial — returns a synthetic mapping derived from Iceberg schema |
| Index aliases | Yes |
| Cross-cluster search | Yes |
| Dashboards index patterns | Yes |
| Security (index-level) | Yes |
| Security (FLS) | Yes |
| Security (DLS) | Yes |
| Audit logging | Yes |
| ISM policies | No |
| Index templates | No |
| Reindex API | No (source only; destination must be a regular index) |
| Snapshot / restore | No |
| Force merge | No |

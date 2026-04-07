# Component 3: Catalog + Metadata Service

## Table of Contents

1. [Overview and Responsibilities](#1-overview-and-responsibilities)
2. [Java Interfaces and Data Model](#2-java-interfaces-and-data-model)
3. [Iceberg Catalog Integration](#3-iceberg-catalog-integration)
4. [Partition Pruning Algorithm](#4-partition-pruning-algorithm)
5. [Schema Evolution Handling](#5-schema-evolution-handling)
6. [Time Travel Support](#6-time-travel-support)
7. [Caching Strategy](#7-caching-strategy)
8. [Integration with OpenSearch Cluster State](#8-integration-with-opensearch-cluster-state)
9. [Raw Parquet File Support](#9-raw-parquet-file-support)
10. [Table Registration](#10-table-registration)

---

## 1. Overview and Responsibilities

The Catalog + Metadata Service is the central metadata authority for the distributed lakehouse query engine. It runs as a component on the OpenSearch `datawarehouse` node type and is consulted by the Query Coordinator during query planning before any data is read.

### Core Responsibilities

- **Table discovery**: Enumerate databases and tables registered with the engine, resolving to physical storage locations.
- **Schema resolution**: Return authoritative column schemas, types, and field IDs for a given table at a given point in time.
- **Snapshot resolution**: Determine the correct Iceberg snapshot to read for a query, supporting both latest and historical snapshots.
- **File enumeration**: Walk the Iceberg metadata chain (snapshot → manifest list → manifest files → data files) and return the full set of `DataFileInfo` objects for a table snapshot.
- **Partition pruning**: Apply partition predicates extracted from the Calcite RelNode (or SQL WHERE clause) to eliminate data files before they are assigned to workers, reducing I/O dramatically.
- **Column statistics exposure**: Surface per-column min/max/null statistics from Iceberg manifest entries to the coordinator for further plan optimization (e.g., runtime filter construction).
- **Schema evolution awareness**: Correctly handle column additions, deletions, renames, and type promotions across snapshot history.
- **Raw Parquet support**: List S3 prefixes for tables not managed by Iceberg, inferring schema via Parquet footer inspection.
- **Metadata caching**: Maintain an in-process LRU cache for metadata objects (snapshot manifests, file lists) to avoid repeated S3 round-trips within and across queries.

### Position in Query Lifecycle

```
Client Query (SQL / PPL)
         |
         v
  Query Coordinator
         |
         |---> CatalogService.getTable(tableId)
         |         returns: LakehouseTable (schema, partitionSpec, currentSnapshot)
         |
         |---> CatalogService.getTableFiles(tableId, snapshotId, predicates)
         |         returns: FileManifest (pruned DataFileInfo list)
         |
         v
  File assignment -> Worker nodes
```

---

## 2. Java Interfaces and Data Model

### 2.1 CatalogService

The top-level service interface. Implementations exist for each supported catalog backend.

```java
package org.opensearch.lakehouse.catalog;

import java.util.List;
import java.util.Optional;

/**
 * Primary interface for metadata resolution. Implementations must be thread-safe.
 * All methods may throw CatalogException on connectivity or permission failures.
 */
public interface CatalogService {

    /**
     * Lists all database/namespace names visible in this catalog.
     *
     * @param catalogName the catalog to query; null implies the default catalog
     * @return ordered list of database names
     * @throws CatalogException if the catalog backend is unreachable
     */
    List<String> listDatabases(String catalogName) throws CatalogException;

    /**
     * Lists all table names in the given database.
     *
     * @param catalogName the catalog to query; null implies the default catalog
     * @param databaseName the database/namespace to list
     * @return ordered list of fully-qualified table identifiers
     * @throws CatalogException if the database does not exist or the backend is unreachable
     */
    List<TableIdentifier> listTables(String catalogName, String databaseName)
            throws CatalogException;

    /**
     * Returns full metadata for a table at its current (or pinned) snapshot.
     * Does NOT load the file manifest; use getTableFiles for that.
     *
     * @param tableId fully-qualified table identifier
     * @return LakehouseTable metadata, never null
     * @throws TableNotFoundException if the table does not exist
     * @throws CatalogException on backend failure
     */
    LakehouseTable getTable(TableIdentifier tableId) throws CatalogException;

    /**
     * Returns the schema for a table, optionally at a historical snapshot.
     * Handles schema evolution: returns the schema as of the given snapshotId.
     *
     * @param tableId fully-qualified table identifier
     * @param snapshotId snapshot to resolve; empty means current snapshot
     * @return TableSchema as of the requested snapshot
     * @throws TableNotFoundException if the table does not exist
     * @throws SnapshotNotFoundException if the snapshot does not exist
     * @throws CatalogException on backend failure
     */
    TableSchema getTableSchema(TableIdentifier tableId, Optional<Long> snapshotId)
            throws CatalogException;

    /**
     * Forces the catalog to reload metadata for the given table from the backend,
     * bypassing any caches. Use after external writes or on stale-data detection.
     *
     * @param tableId fully-qualified table identifier
     * @throws CatalogException on backend failure
     */
    void refreshTable(TableIdentifier tableId) throws CatalogException;

    /**
     * Returns the pruned file list for a table snapshot, applying partition predicates.
     * This is the hot path called per query; it uses the metadata cache aggressively.
     *
     * @param tableId fully-qualified table identifier
     * @param snapshotId snapshot to read; empty means current snapshot
     * @param predicates partition predicates extracted from the Calcite RelNode
     * @return FileManifest containing only files that pass partition pruning
     * @throws CatalogException on backend failure
     */
    FileManifest getTableFiles(
            TableIdentifier tableId,
            Optional<Long> snapshotId,
            List<PartitionPredicate> predicates)
            throws CatalogException;
}
```

### 2.2 LakehouseTable

```java
package org.opensearch.lakehouse.catalog;

import java.util.Map;

/**
 * Immutable descriptor for a lakehouse table. Returned by CatalogService.getTable().
 * Does not include the file manifest (fetched separately for lazy loading).
 */
public final class LakehouseTable {

    /** Fully-qualified identifier: catalog.database.table */
    private final TableIdentifier tableId;

    /** Schema at the current (or pinned) snapshot */
    private final TableSchema schema;

    /** Partition specification in effect for the current snapshot */
    private final PartitionSpec partitionSpec;

    /** Metadata for the snapshot that will be read, null if table is empty */
    private final SnapshotInfo currentSnapshot;

    /** Iceberg/Parquet table properties (e.g., write.format.default) */
    private final Map<String, String> properties;

    /** Physical storage format */
    private final TableFormat format;

    /** S3 URI of the Iceberg table root (contains metadata/ directory) or raw prefix */
    private final String warehouseLocation;

    public LakehouseTable(
            TableIdentifier tableId,
            TableSchema schema,
            PartitionSpec partitionSpec,
            SnapshotInfo currentSnapshot,
            Map<String, String> properties,
            TableFormat format,
            String warehouseLocation) { ... }

    // Accessors omitted for brevity; all fields are final with standard getters.
}

public enum TableFormat {
    ICEBERG,
    PARQUET_RAW
}
```

### 2.3 TableSchema

```java
package org.opensearch.lakehouse.catalog;

import java.util.List;

/**
 * Represents the schema of a table at a specific point in its history.
 * Field IDs are Iceberg-assigned and stable across schema evolutions.
 */
public final class TableSchema {

    private final int schemaId;
    private final List<ColumnDefinition> columns;

    public TableSchema(int schemaId, List<ColumnDefinition> columns) { ... }

    public int getSchemaId() { return schemaId; }
    public List<ColumnDefinition> getColumns() { return columns; }

    /**
     * Returns a ColumnDefinition by its stable Iceberg field ID, or empty if not present.
     * Use this (not column index) for schema-evolution-safe access.
     */
    public Optional<ColumnDefinition> findByFieldId(int fieldId) { ... }

    /**
     * Returns a ColumnDefinition by name in the current schema, or empty if not present.
     */
    public Optional<ColumnDefinition> findByName(String name) { ... }
}

/**
 * Metadata for a single column.
 */
public final class ColumnDefinition {

    /** Name of the column in the current schema */
    private final String name;

    /**
     * Calcite-compatible type descriptor. Maps to
     * org.apache.calcite.rel.type.RelDataType (e.g., INTEGER, DECIMAL, VARCHAR).
     * Used by SqlProducer to generate correct SQL type casts.
     */
    private final org.apache.calcite.rel.type.RelDataType type;

    /** Whether the column can hold NULL values */
    private final boolean nullable;

    /** Optional human-readable description */
    private final String comment;

    /**
     * Stable Iceberg field ID. Does NOT change on column rename or promotion.
     * This is the key used to correlate statistics and partition transforms.
     */
    private final int fieldId;

    public ColumnDefinition(String name, org.apache.calcite.rel.type.RelDataType type,
                            boolean nullable, String comment, int fieldId) { ... }

    // Standard getters.
}
```

### 2.4 PartitionSpec

```java
package org.opensearch.lakehouse.catalog;

import java.util.List;

/**
 * Describes how a table's rows are partitioned.
 * An Iceberg table may have evolved through multiple partition specs;
 * this represents the spec in effect for a given snapshot.
 */
public final class PartitionSpec {

    /** Iceberg spec ID; monotonically increasing on partition evolution */
    private final int specId;

    /** Ordered list of partition fields (multi-level partitioning is supported) */
    private final List<PartitionField> fields;

    public PartitionSpec(int specId, List<PartitionField> fields) { ... }

    public int getSpecId() { return specId; }
    public List<PartitionField> getFields() { return fields; }

    /** Returns true if this spec has no partition fields (unpartitioned table) */
    public boolean isUnpartitioned() { return fields.isEmpty(); }
}

/**
 * A single dimension of partitioning.
 */
public final class PartitionField {

    /** Field ID of the source column in TableSchema */
    private final int sourceColumnId;

    /** Transform applied to the source column value to derive the partition value */
    private final PartitionTransform transform;

    /**
     * For BUCKET and TRUNCATE transforms, the transform parameter
     * (number of buckets, or truncation width). Ignored for other transforms.
     */
    private final int transformParam;

    /** Iceberg-assigned partition field ID (distinct from sourceColumnId) */
    private final int partitionFieldId;

    /** Name of the partition column as it appears in partition metadata */
    private final String name;

    public PartitionField(int sourceColumnId, PartitionTransform transform,
                          int transformParam, int partitionFieldId, String name) { ... }
}
```

### 2.5 PartitionTransform

```java
package org.opensearch.lakehouse.catalog;

/**
 * Iceberg partition transform functions.
 * Each enum value corresponds to a transform in the Iceberg spec §4.1.
 */
public enum PartitionTransform {

    /** Partition by the raw column value. Works for any type. */
    IDENTITY,

    /**
     * Partition by the year extracted from a timestamp/date column.
     * Partition value: integer year (e.g., 2024).
     */
    YEAR,

    /**
     * Partition by year-month from a timestamp/date column.
     * Partition value: months since epoch (year*12 + month - 1).
     */
    MONTH,

    /**
     * Partition by calendar date from a timestamp column.
     * Partition value: days since epoch (1970-01-01 = 0).
     */
    DAY,

    /**
     * Partition by hour from a timestamp column.
     * Partition value: hours since epoch.
     */
    HOUR,

    /**
     * Hash the source value into N buckets.
     * transformParam = number of buckets.
     * Partition value: murmur3_32(value) mod N.
     */
    BUCKET,

    /**
     * Truncate string or integer to a fixed width/magnitude.
     * For integers: floor(value / width) * width.
     * For strings: value.substring(0, width).
     * transformParam = width.
     */
    TRUNCATE,

    /**
     * Marks a partition field as dropped in a later spec.
     * Files written with a VOID field always have null for that partition value.
     */
    VOID
}
```

### 2.6 SnapshotInfo

```java
package org.opensearch.lakehouse.catalog;

import java.util.Map;

/**
 * Summary metadata for a single Iceberg snapshot.
 * Does not contain file-level data; that is loaded lazily via FileManifest.
 */
public final class SnapshotInfo {

    /** Iceberg snapshot ID (globally unique long) */
    private final long snapshotId;

    /** Wall-clock timestamp when this snapshot was committed (ms since epoch) */
    private final long timestampMs;

    /**
     * The write operation that produced this snapshot.
     * Typical values: "append", "overwrite", "delete", "replace".
     */
    private final String operation;

    /**
     * Free-form metadata written by the committing process.
     * Common keys: "added-files-size", "total-records", "spark.app.id".
     */
    private final Map<String, String> summary;

    /**
     * S3 URI of the manifest list (.avro) file for this snapshot.
     * Each entry in the manifest list points to a manifest file,
     * which in turn lists individual data files.
     */
    private final String manifestListPath;

    /** Snapshot ID of the parent, or -1 if this is the first snapshot */
    private final long parentSnapshotId;

    /** Schema ID in effect when this snapshot was committed */
    private final int schemaId;

    public SnapshotInfo(long snapshotId, long timestampMs, String operation,
                        Map<String, String> summary, String manifestListPath,
                        long parentSnapshotId, int schemaId) { ... }
}
```

### 2.7 FileManifest

```java
package org.opensearch.lakehouse.catalog;

import java.util.List;

/**
 * The complete (or pruned) list of data files to read for a query.
 * Produced by CatalogService.getTableFiles() after partition pruning.
 */
public final class FileManifest {

    private final TableIdentifier tableId;
    private final long snapshotId;

    /** Files remaining after partition pruning; may be empty for selective queries */
    private final List<DataFileInfo> files;

    /** Total files before pruning, for observability */
    private final int totalFilesBeforePruning;

    /** Total files after pruning */
    private final int totalFilesAfterPruning;

    public FileManifest(TableIdentifier tableId, long snapshotId,
                        List<DataFileInfo> files,
                        int totalFilesBeforePruning,
                        int totalFilesAfterPruning) { ... }

    public List<DataFileInfo> getFiles() { return files; }
    public int getTotalFilesBeforePruning() { return totalFilesBeforePruning; }
    public int getTotalFilesAfterPruning() { return totalFilesAfterPruning; }
}
```

### 2.8 DataFileInfo

```java
package org.opensearch.lakehouse.catalog;

import java.util.List;
import java.util.Map;

/**
 * Metadata for a single data file. Populated from Iceberg manifest entries.
 * All fields are derived from the Iceberg DataFile struct (spec §4.1.2).
 */
public final class DataFileInfo {

    /** Fully-qualified S3 URI of the data file */
    private final String filePath;

    /** Physical encoding of the file */
    private final FileFormat format;

    /** Number of logical records in the file */
    private final long recordCount;

    /** Compressed on-disk size in bytes */
    private final long fileSizeBytes;

    /**
     * Partition values for this file, keyed by partition field ID.
     * Values are typed objects (Long, String, Integer) matching the partition transform output.
     * For IDENTITY(timestamp) the value is a long epoch-micros.
     */
    private final Map<Integer, Object> partitionValues;

    /**
     * Per-column statistics from the manifest entry.
     * Keyed by Iceberg field ID (matches ColumnDefinition.fieldId).
     */
    private final Map<Integer, ColumnStatistics> columnStats;

    /**
     * Sort order fields for this file (Iceberg sort order struct).
     * Empty if the file was not written in sorted order.
     */
    private final List<SortField> sortOrder;

    /**
     * Iceberg content type: DATA (0), POSITION_DELETES (1), EQUALITY_DELETES (2).
     */
    private final int contentType;

    public DataFileInfo(String filePath, FileFormat format, long recordCount,
                        long fileSizeBytes, Map<Integer, Object> partitionValues,
                        Map<Integer, ColumnStatistics> columnStats,
                        List<SortField> sortOrder, int contentType) { ... }
}

public enum FileFormat {
    PARQUET,
    ORC,
    AVRO
}
```

### 2.9 ColumnStatistics

```java
package org.opensearch.lakehouse.catalog;

/**
 * Column-level statistics stored in Iceberg manifest entries.
 * All statistics are optional (null = not available for this column/file).
 *
 * min/maxValue are typed as Object and will be the Java type matching
 * the column's Iceberg type:
 *   INTEGER -> Integer, LONG -> Long, FLOAT -> Float, DOUBLE -> Double,
 *   STRING -> CharBuffer (UTF-8 truncated), BINARY -> ByteBuffer,
 *   TIMESTAMP -> Long (micros since epoch), DATE -> Integer (days since epoch).
 */
public final class ColumnStatistics {

    /** Iceberg field ID of the column these stats belong to */
    private final int columnId;

    /** Inclusive lower bound for non-null values in this file, or null */
    private final Object minValue;

    /** Inclusive upper bound for non-null values in this file, or null */
    private final Object maxValue;

    /** Count of null values in this column for this file, or null if unknown */
    private final Long nullCount;

    /** Total count of values (including nulls) in this column, or null if unknown */
    private final Long valueCount;

    /** Count of NaN floating-point values, or null if not applicable / unknown */
    private final Long nanCount;

    public ColumnStatistics(int columnId, Object minValue, Object maxValue,
                            Long nullCount, Long valueCount, Long nanCount) { ... }

    /** Returns true if this column has sufficient stats to apply a range predicate */
    public boolean hasRangeBounds() {
        return minValue != null && maxValue != null;
    }
}
```

### 2.10 PartitionPruner

```java
package org.opensearch.lakehouse.catalog;

import java.util.List;

/**
 * Stateless utility that filters a list of DataFileInfo objects based on
 * partition predicates extracted from the Calcite RelNode (query plan).
 *
 * Implementations must be deterministic and thread-safe.
 * The standard implementation is IcebergPartitionPruner.
 */
public interface PartitionPruner {

    /**
     * Given the full list of data files for a snapshot and a set of partition
     * predicates from the query plan, returns only files that MIGHT contain
     * rows satisfying the predicates.
     *
     * Conservative: may return files that turn out to be empty for the predicate
     * (false positives acceptable). Must NOT drop files that contain matching rows
     * (false negatives are query correctness bugs).
     *
     * @param files       all data files for the table snapshot
     * @param spec        partition spec in effect for these files
     * @param schema      table schema (needed to resolve column types for stat comparison)
     * @param predicates  list of predicates; empty means "no pruning, return all files"
     * @return            filtered list; may be the same list object if no pruning occurred
     */
    List<DataFileInfo> pruneFiles(
            List<DataFileInfo> files,
            PartitionSpec spec,
            TableSchema schema,
            List<PartitionPredicate> predicates);
}

/**
 * A single predicate on a partition or data column, extracted from the Calcite RelNode.
 */
public final class PartitionPredicate {

    public enum Operator { EQ, NEQ, LT, LTE, GT, GTE, IN, IS_NULL, IS_NOT_NULL }

    private final int fieldId;         // Iceberg field ID of the referenced column
    private final Operator operator;
    private final List<Object> values; // one value for most ops; multiple for IN

    public PartitionPredicate(int fieldId, Operator operator, List<Object> values) { ... }
}
```

### 2.11 CatalogConfig

```java
package org.opensearch.lakehouse.catalog;

import java.util.Map;

/**
 * Configuration for a catalog backend. Stored in the OpenSearch cluster state
 * under the "lakehouse.catalogs" key. Serialized/deserialized as JSON.
 */
public final class CatalogConfig {

    public enum CatalogType {
        HIVE,       // HMS-compatible Thrift catalog
        GLUE,       // AWS Glue Data Catalog
        REST,       // Iceberg REST Catalog (spec v1/v2)
        NESSIE,     // Project Nessie (git-like catalog)
        HADOOP,     // Hadoop file-system catalog (path-based)
        RAW_S3      // No Iceberg; raw Parquet prefix listing
    }

    /** User-visible name for this catalog (used in SQL: catalog.db.table) */
    private final String catalogName;

    private final CatalogType catalogType;

    /**
     * Root storage path for the catalog warehouse.
     * Examples:
     *   s3://my-bucket/warehouse/
     *   s3://my-bucket/raw-data/events/
     */
    private final String warehousePath;

    /**
     * Catalog-type-specific properties. Common keys:
     *
     * HIVE:  "uri" (thrift URI), "warehouse"
     * GLUE:  "glue.region", "glue.account-id", "glue.endpoint"
     * REST:  "uri" (REST endpoint), "credential", "token", "oauth2-server-uri"
     * NESSIE: "uri", "ref" (branch/tag), "authentication.type"
     */
    private final Map<String, String> properties;

    /** Whether to use instance profile / IAM role credentials for S3 access */
    private final boolean useInstanceProfile;

    public CatalogConfig(String catalogName, CatalogType catalogType,
                         String warehousePath, Map<String, String> properties,
                         boolean useInstanceProfile) { ... }
}
```

---

## 3. Iceberg Catalog Integration

### 3.1 Supported Catalog Backends

The service supports the following Iceberg catalog implementations via the Apache Iceberg Java library. Each is wrapped in a thin adapter that implements `CatalogService`.

| CatalogType | Iceberg Class | Authentication |
|-------------|---------------|----------------|
| `HIVE` | `org.apache.iceberg.hive.HiveCatalog` | Kerberos / no-auth |
| `GLUE` | `org.apache.iceberg.aws.glue.GlueCatalog` | IAM role / instance profile |
| `REST` | `org.apache.iceberg.rest.RESTCatalog` | Bearer token / OAuth2 |
| `HADOOP` | `org.apache.iceberg.hadoop.HadoopCatalog` | S3 credentials |

Each adapter is instantiated via `CatalogFactory`:

```java
public class CatalogFactory {
    public static CatalogService create(CatalogConfig config, S3FileIO fileIO) {
        org.apache.iceberg.catalog.Catalog icebergCatalog = switch (config.getCatalogType()) {
            case HIVE    -> buildHiveCatalog(config);
            case GLUE    -> buildGlueCatalog(config);
            case REST    -> buildRESTCatalog(config);
            case HADOOP  -> buildHadoopCatalog(config);
            case RAW_S3  -> null; // handled by RawParquetCatalogService
            default      -> throw new IllegalArgumentException("Unknown catalog type");
        };
        return new IcebergCatalogService(config, icebergCatalog, fileIO);
    }
}
```

### 3.2 Metadata Resolution Chain

Iceberg tables are resolved by walking a chain of metadata files, all stored on S3. The entry point is the `metadata.json` file, whose path is recorded in the catalog backend.

```
metadata.json (TableMetadata)
  └── snapshots[]
        └── manifest-list-<uuid>.avro  (ManifestFile list)
              └── manifest-<uuid>.avro  (ManifestEntry list)
                    └── DataFile  (file path, stats, partition values)
```

**Step-by-step resolution in `IcebergCatalogService.getTableFiles()`:**

```
1. Resolve TableMetadata
   - Call icebergCatalog.loadTable(tableIdentifier)
   - Iceberg library fetches s3://.../metadata/v<N>.metadata.json
   - Parse into org.apache.iceberg.TableMetadata

2. Select Snapshot
   - If snapshotId is provided: tableMetadata.snapshot(snapshotId)
   - Else: tableMetadata.currentSnapshot()
   - If null (empty table): return empty FileManifest

3. Read Manifest List
   - snapshot.manifestListLocation() -> S3 URI of .avro manifest list
   - Use ManifestFiles.read(manifestList, fileIO) to iterate ManifestFile entries
   - Each ManifestFile has: path, partitionSpecId, addedFilesCount, existingFilesCount,
     deletedFilesCount, partitionFieldSummaries (for partition pruning at manifest level)

4. First-level pruning: Manifest List
   - For each ManifestFile, check partitionFieldSummaries against predicates
   - Drop manifests where the summary proves no matching rows exist
   - This avoids reading manifest .avro files that can't contribute rows

5. Read Manifest Files (in parallel, one thread per manifest)
   - For each retained ManifestFile: ManifestReader<DataFile> reader =
         ManifestFiles.read(manifestFile, fileIO, tableMetadata.specsById())
   - Iterate DataFile entries (status: ADDED or EXISTING; skip DELETED)

6. Convert DataFile -> DataFileInfo
   - Map all Iceberg DataFile fields to our DataFileInfo model
   - Extract ColumnStatistics from lowerBounds/upperBounds/nullValueCounts

7. Second-level pruning: File-level partition values
   - Apply PartitionPruner.pruneFiles() on the full DataFileInfo list

8. Return FileManifest with pruning stats
```

### 3.3 Snapshot Isolation

Each query reads a single, immutable snapshot. This guarantees:

- **Consistency**: No partial visibility of concurrent writes.
- **Repeatability**: Re-executing the same query with the same snapshot ID yields identical results.

Snapshot selection rules:

```java
Snapshot resolveSnapshot(TableMetadata meta, Optional<Long> requestedSnapshotId,
                          Optional<Long> asOfTimestampMs) {
    if (requestedSnapshotId.isPresent()) {
        // Explicit snapshot: fail if not found
        Snapshot s = meta.snapshot(requestedSnapshotId.get());
        if (s == null) throw new SnapshotNotFoundException(...);
        return s;
    }
    if (asOfTimestampMs.isPresent()) {
        // Time travel: find latest snapshot at or before the timestamp
        return SnapshotUtil.snapshotIdAsOfTime(meta, asOfTimestampMs.get());
    }
    // Default: current snapshot
    return meta.currentSnapshot();
}
```

**Concurrent write safety**: The Iceberg metadata.json is immutable once written. A query that pins a snapshotId will always see the same files regardless of concurrent commits. No locking is required in the catalog service.

---

## 4. Partition Pruning Algorithm

Partition pruning operates at two levels: the manifest list level (coarse) and the data file level (fine-grained). Both are implemented in `IcebergPartitionPruner`.

### 4.1 Predicate Extraction from Calcite RelNode

The Query Coordinator extracts partition-relevant predicates from the Calcite `LogicalFilter` nodes in the RelNode tree before calling `getTableFiles()`. Only predicates on columns that appear in the `PartitionSpec` as IDENTITY, YEAR, MONTH, DAY, HOUR, BUCKET, or TRUNCATE transforms are passed to the pruner.

```java
List<PartitionPredicate> extractPartitionPredicates(
        org.apache.calcite.rel.RelNode plan,
        PartitionSpec spec,
        TableSchema schema) {
    // Walk Calcite RelNode tree, find LogicalFilter nodes,
    // collect simple binary comparisons (RexCall with =, <, >, <=, >=, BETWEEN)
    // involving partition source columns. Complex expressions (OR across
    // different columns, subqueries) are dropped (conservative: no pruning).
}
```

### 4.2 Pruning Pseudocode

```
function pruneFiles(files, spec, schema, predicates):
    if predicates is empty:
        return files  // no pruning possible

    result = []

    for each file in files:
        keep = true

        for each predicate in predicates:
            column = schema.findByFieldId(predicate.fieldId)
            partField = spec.findBySourceColumnId(predicate.fieldId)

            if partField is null:
                // Column is not a partition column; cannot prune on partition value.
                // Fall through to column stats check.
                if file.columnStats contains predicate.fieldId:
                    stats = file.columnStats[predicate.fieldId]
                    if stats.hasRangeBounds():
                        if canEliminateByStats(predicate, stats):
                            keep = false
                            break
                continue

            // Column IS a partition column
            partitionValue = file.partitionValues[partField.partitionFieldId]

            if partitionValue is null:
                // Null partition value: file was written under a VOID or
                // earlier partition spec; conservatively retain.
                continue

            transformedPredicate = applyTransformToPredicate(
                predicate, partField.transform, partField.transformParam)

            if transformedPredicate is null:
                // Transform makes predicate non-evaluable (e.g., BUCKET with EQ).
                // For BUCKET(N, col) = v: we can only prune if we know the bucket.
                // Compute expected bucket and compare.
                if partField.transform == BUCKET and predicate.operator == EQ:
                    expectedBucket = murmur3Hash(predicate.values[0]) % partField.transformParam
                    if partitionValue != expectedBucket:
                        keep = false
                        break
                continue

            if not satisfies(partitionValue, transformedPredicate):
                keep = false
                break

        if keep:
            result.append(file)

    return result


function canEliminateByStats(predicate, stats):
    // Apply standard range-based elimination:
    // GT/GTE: eliminate if predicate value > stats.maxValue
    // LT/LTE: eliminate if predicate value < stats.minValue
    // EQ:     eliminate if predicate value < stats.minValue OR > stats.maxValue
    // IS_NULL: eliminate if stats.nullCount == 0
    match predicate.operator:
        case GT:  return predicate.values[0] >= stats.maxValue
        case GTE: return predicate.values[0] > stats.maxValue
        case LT:  return predicate.values[0] <= stats.minValue
        case LTE: return predicate.values[0] < stats.minValue
        case EQ:  return predicate.values[0] < stats.minValue
                      or predicate.values[0] > stats.maxValue
        case IS_NULL: return stats.nullCount == 0
        default: return false


function applyTransformToPredicate(predicate, transform, param):
    // For YEAR/MONTH/DAY/HOUR: if the predicate is on the original timestamp
    // column, convert the predicate value to the partition domain.
    // e.g., for YEAR transform: EQ(ts, '2024-03-15') -> EQ(partitionYear, 2024)
    //        GT(ts, '2024-01-01') -> GTE(partitionYear, 2024)
    // For TRUNCATE: floor the predicate value to the truncation boundary.
    // For IDENTITY: predicate passes through unchanged.
    ...
```

### 4.3 Multi-Spec Tables

When a table has undergone partition evolution, files written under old partition specs have different `partField.partitionFieldId` assignments. Each `DataFileInfo` records which spec it was written under (via its partition values map keys). The pruner uses `spec.findBySourceColumnId()` across all historical specs loaded from `TableMetadata.specsById()`.

---

## 5. Schema Evolution Handling

Iceberg supports the following non-breaking schema changes:

| Change Type | Handling |
|-------------|----------|
| Add column | New column absent in old files -> readers return null |
| Drop column | Column absent in new schema -> not projected; old files still have it |
| Rename column | Field ID unchanged; name lookup in `TableSchema.findByName()` returns new name |
| Widen type | e.g., INT -> LONG; Parquet readers handle implicit widening |
| Reorder columns | Field IDs are projection key; column order in file is irrelevant |

The catalog service resolves the schema for each file using the **field ID → column name** mapping from the snapshot's schema. Workers receive `DataFileInfo` with `columnStats` keyed by field ID, not column index, ensuring correctness across all schema versions.

**Schema at query time vs. schema at write time:**

```
QuerySchema (from getTableSchema at query snapshot)
     |
     +--> fieldId 1: "event_id" LONG
     +--> fieldId 2: "user_id" INT  (was "uid" in old files)
     +--> fieldId 3: "ts" TIMESTAMP
     +--> fieldId 7: "country" STRING  (added later; absent in old files)

Old file written with schema {1: event_id, 2: uid, 3: ts}
  -> Worker projects using field IDs [1,2,3,7]
  -> fieldId 7 not in file -> worker supplies null column
  -> fieldId 2 maps to "uid" in file -> worker reads it as "user_id"
```

Workers must use **column projection by field ID** (Parquet `MessageType` with field ID annotations). The `DataFileInfo` does not embed per-file schema; workers use the query schema and rely on Parquet field-ID-based column matching.

---

## 6. Time Travel Support

Time travel allows queries to read a historical version of a table.

### 6.1 Query Syntax (handled upstream, delivered to catalog as options)

```sql
-- By snapshot ID
SELECT * FROM catalog.db.events FOR VERSION AS OF 8921763450182921;

-- By timestamp
SELECT * FROM catalog.db.events FOR TIMESTAMP AS OF '2024-11-01 00:00:00';
```

### 6.2 Catalog Resolution

Time travel parameters are passed to `CatalogService.getTable()` and `CatalogService.getTableFiles()` via `QueryOptions`:

```java
public final class QueryOptions {
    // Mutually exclusive; both null means "current snapshot"
    private final Long explicitSnapshotId;       // FOR VERSION AS OF
    private final Long asOfTimestampMs;           // FOR TIMESTAMP AS OF
}
```

For timestamp-based time travel:

```java
long resolveSnapshotIdForTimestamp(TableMetadata meta, long asOfTimestampMs) {
    // Iceberg SnapshotUtil.snapshotIdAsOfTime walks the snapshot log in reverse
    // chronological order and returns the latest snapshot whose timestampMs
    // is <= asOfTimestampMs.
    return SnapshotUtil.snapshotIdAsOfTime(meta, asOfTimestampMs);
    // Throws IllegalArgumentException if asOfTimestampMs predates all snapshots.
}
```

### 6.3 Snapshot Retention and Expiry

If a requested snapshot has been expired by Iceberg's `expireSnapshots` procedure, the manifest files may have been garbage-collected from S3. The catalog service detects this:

```java
try {
    Snapshot snap = meta.snapshot(snapshotId);
    if (snap == null) throw new SnapshotNotFoundException(snapshotId);
    // Attempt to access manifest list; if S3 returns 404, the snapshot is expired
    validateManifestListAccessible(snap.manifestListLocation());
} catch (NotFoundException e) {
    throw new SnapshotExpiredException(snapshotId, tableId);
}
```

### 6.4 Schema for Historical Snapshots

Each snapshot records `schemaId`. `getTableSchema(tableId, Optional.of(snapshotId))` returns the schema with that `schemaId`, not the current schema. This is critical for:
- Correct column type interpretation in historical files.
- Correct partition predicate evaluation (column may have been renamed since).

---

## 7. Caching Strategy

Metadata files on S3 are immutable once written (Iceberg's append-only design). This makes them safe to cache aggressively. The catalog service maintains a multi-tier in-process cache.

### 7.1 Cache Tiers

```
Tier 1: TableMetadata cache
  - Key: TableIdentifier
  - Value: org.apache.iceberg.TableMetadata (parsed metadata.json)
  - TTL: 60 seconds (refreshed on cache miss or explicit refreshTable() call)
  - Max entries: 1,000 tables

Tier 2: Manifest List cache
  - Key: manifestListPath (S3 URI string)
  - Value: List<ManifestFile> (parsed manifest list .avro)
  - TTL: forever (manifest lists are immutable; keyed by content-addressed path)
  - Max entries: 10,000 manifest lists

Tier 3: Manifest File cache
  - Key: manifestFilePath (S3 URI string)
  - Value: List<DataFileInfo> (parsed manifest .avro, converted to our model)
  - TTL: forever (manifest files are immutable)
  - Max entries: 100,000 manifest files
  - Eviction: LRU
```

### 7.2 Cache Implementation

```java
public class MetadataCache {

    // Tier 1: mutable, short TTL
    private final Cache<TableIdentifier, TableMetadata> tableMetadataCache =
        Caffeine.newBuilder()
            .maximumSize(1_000)
            .expireAfterWrite(60, TimeUnit.SECONDS)
            .recordStats()
            .build();

    // Tier 2: immutable, no TTL
    private final Cache<String, List<ManifestFile>> manifestListCache =
        Caffeine.newBuilder()
            .maximumSize(10_000)
            .recordStats()
            .build();

    // Tier 3: immutable, LRU eviction
    private final Cache<String, List<DataFileInfo>> manifestFileCache =
        Caffeine.newBuilder()
            .maximumSize(100_000)
            .recordStats()
            .build();
}
```

### 7.3 Cache Invalidation

| Trigger | Action |
|---------|--------|
| `refreshTable(tableId)` | Evict `tableMetadataCache[tableId]`; manifest caches unaffected (immutable) |
| External write detected (via S3 event or polling) | Same as `refreshTable()` |
| OpenSearch node shutdown | Cache is in-process; no persistence required; rebuilt on restart |
| Partition spec evolution detected | Evict table metadata; pruner uses fresh spec on next call |

**Staleness detection**: The Tier 1 cache TTL of 60 seconds is the maximum staleness window. For interactive queries where freshness matters, callers pass `forceRefresh=true` to `getTable()`, which bypasses the TTL check and calls `refreshTable()`.

### 7.4 Cache Metrics

Exposed via OpenSearch node stats API under `/_nodes/stats/lakehouse_catalog`:

```json
{
  "table_metadata_cache": { "hit_rate": 0.94, "size": 142, "evictions": 3 },
  "manifest_list_cache":  { "hit_rate": 0.99, "size": 8421, "evictions": 0 },
  "manifest_file_cache":  { "hit_rate": 0.97, "size": 72340, "evictions": 1820 }
}
```

---

## 8. Integration with OpenSearch Cluster State

The catalog service uses OpenSearch cluster state for two purposes:
1. Persisting catalog registrations (durable, replicated).
2. Coordinating table refresh across nodes (invalidation broadcasts).

### 8.1 Cluster State Schema

Catalog configurations and table registrations are stored in OpenSearch cluster state metadata under a custom `LakehouseMetadata` extension:

```
cluster_state.metadata.custom["lakehouse"]
  |
  +--> catalogs: Map<String, CatalogConfig>   // catalog name -> config
  +--> tables:   Map<TableIdentifier, ExternalTableRegistration>
```

`ExternalTableRegistration` stores:

```java
public final class ExternalTableRegistration {
    private final TableIdentifier tableId;
    private final String metadataLocation;   // explicit s3://.../.../metadata.json path
    private final CatalogConfig catalogRef;  // which catalog owns this table
    private final long registeredAtMs;
    private final String registeredBy;       // OpenSearch user principal
    private final Map<String, String> options; // e.g., "read.split.target-size"
}
```

### 8.2 Cluster State Updates

Writes to cluster state go through a `MasterOperation` (OpenSearch transport action) to ensure linearizability:

```
User REST call -> CatalogAdminAction (Transport) -> MasterOperation
    -> ClusterStateUpdateTask { priority = NORMAL }
    -> Publish to all nodes via cluster state publishing protocol
    -> Each node's CatalogService.onClusterStateChanged() evicts affected cache entries
```

### 8.3 Cross-Node Cache Invalidation

When a table is refreshed or re-registered, the master publishes a cluster state update. Each node's `CatalogService` implements `ClusterStateListener`:

```java
@Override
public void clusterChanged(ClusterChangedEvent event) {
    LakehouseMetadata prev = LakehouseMetadata.fromClusterState(event.previousState());
    LakehouseMetadata curr = LakehouseMetadata.fromClusterState(event.state());

    for (TableIdentifier changed : curr.changedTables(prev)) {
        metadataCache.invalidateTable(changed);
        logger.info("Invalidated metadata cache for table {}", changed);
    }
}
```

This ensures all coordinator and worker nodes pick up new table registrations within one cluster state round-trip (typically < 100ms on a healthy cluster).

---

## 9. Raw Parquet File Support

For tables not managed by Iceberg (raw files dropped into S3 prefixes), the engine provides `RawParquetCatalogService`, which implements the same `CatalogService` interface but bypasses Iceberg metadata entirely.

### 9.1 Registration

A raw Parquet table is registered with an S3 prefix and a schema hint:

```java
public final class RawParquetTableConfig {
    private final TableIdentifier tableId;
    private final String s3Prefix;           // e.g., s3://bucket/events/year=2024/
    private final boolean inferSchema;       // true: read Parquet footer from sample file
    private final TableSchema explicitSchema; // used if inferSchema=false
    private final String partitionPattern;   // e.g., "year={year}/month={month}/day={day}"
}
```

### 9.2 File Listing

`getTableFiles()` for a raw Parquet table:

```
1. List all objects under s3Prefix using S3 ListObjectsV2 (paginated)
   - Filter to .parquet and .snappy.parquet extensions
   - Recursively list subdirectories

2. (Optional) Parse partition values from S3 key path
   - Match key against partitionPattern using named capture groups
   - e.g., "events/year=2024/month=03/day=15/part-00000.parquet"
     -> partitionValues: {year=2024, month=03, day=15}

3. Apply partition pruning if partitionPattern is set and predicates match

4. For each retained file, construct DataFileInfo:
   - filePath: s3://bucket/key
   - format: PARQUET
   - recordCount: unknown (-1) -- no manifest stats
   - fileSizeBytes: from S3 object metadata
   - partitionValues: from path parsing (if available)
   - columnStats: null (no pre-computed stats; skipped for pruning)

5. Return FileManifest
```

### 9.3 Schema Inference

If `inferSchema=true`, the service reads the Parquet footer of one representative file:

```java
TableSchema inferSchemaFromParquetFooter(String s3Path, S3FileIO fileIO) {
    ParquetFileReader reader = ParquetFileReader.open(fileIO.newInputFile(s3Path));
    MessageType parquetSchema = reader.getFooter().getFileMetaData().getSchema();
    return ParquetSchemaConverter.toTableSchema(parquetSchema);
}
```

`ParquetSchemaConverter` maps Parquet primitive types to Calcite SQL types. Field IDs are synthesized from column index (no Iceberg field IDs available). Schema evolution is not supported for raw Parquet tables.

### 9.4 Limitations vs. Iceberg

| Feature | Iceberg | Raw Parquet |
|---------|---------|-------------|
| Column statistics for pruning | Yes (from manifest) | No |
| Schema evolution | Full support | Not supported |
| Time travel | Yes | No |
| Atomic writes | Yes | No (best-effort listing) |
| Delete files (row-level deletes) | Yes | No |

---

## 10. Table Registration

### 10.1 External Iceberg Table Registration

Users register Iceberg tables via a REST API or SQL `CREATE TABLE` statement. The engine does not own the table data; it only records how to find the Iceberg metadata.

**REST API:**

```
PUT /_lakehouse/catalogs/{catalogName}/tables/{database}/{tableName}
Content-Type: application/json

{
  "type": "ICEBERG",
  "metadata_location": "s3://my-bucket/warehouse/mydb/events/metadata/v3.metadata.json",
  "catalog_type": "REST",
  "catalog_properties": {
    "uri": "https://my-catalog.example.com/",
    "credential": "client:secret"
  }
}
```

**SQL DDL:**

```sql
CREATE EXTERNAL TABLE catalog.mydb.events
USING ICEBERG
LOCATION 's3://my-bucket/warehouse/mydb/events/'
CATALOG_TYPE 'REST'
CATALOG_PROPERTIES ('uri'='https://my-catalog.example.com/', 'credential'='client:secret');
```

**Processing flow:**

```
1. Validate: attempt to load TableMetadata from metadata_location or LOCATION
2. Extract initial schema, partition spec, current snapshot from metadata
3. Write ExternalTableRegistration to cluster state via MasterOperation
4. All nodes receive cluster state update and prime their Tier 1 cache
5. Table immediately available for queries
```

### 10.2 Registration for Catalog-Backed Tables (Hive/Glue/REST)

For tables that already exist in a Hive Metastore, Glue, or REST catalog, registration is at the **catalog** level, not the table level:

```
PUT /_lakehouse/catalogs/{catalogName}
{
  "type": "GLUE",
  "warehouse_path": "s3://my-bucket/warehouse/",
  "properties": {
    "glue.region": "us-east-1"
  }
}
```

Once a catalog is registered, all tables within it are accessible via `listDatabases()` / `listTables()` without further registration.

### 10.3 Raw Parquet Table Registration

```
PUT /_lakehouse/catalogs/{catalogName}/tables/{database}/{tableName}
{
  "type": "PARQUET_RAW",
  "s3_prefix": "s3://my-bucket/raw/events/",
  "infer_schema": true,
  "partition_pattern": "year={year}/month={month}/day={day}"
}
```

### 10.4 Table Deregistration

```
DELETE /_lakehouse/catalogs/{catalogName}/tables/{database}/{tableName}
```

This removes the `ExternalTableRegistration` from cluster state and evicts the table from all node caches. The underlying data is not affected.

### 10.5 Catalog Deregistration

```
DELETE /_lakehouse/catalogs/{catalogName}
```

Fails if any `ExternalTableRegistration` references this catalog. Must deregister tables first, or pass `?force=true` to cascade-delete all table registrations.

---

## Appendix A: Exception Hierarchy

```
CatalogException (checked)
  ├── TableNotFoundException
  ├── DatabaseNotFoundException
  ├── SnapshotNotFoundException
  ├── SnapshotExpiredException
  ├── SchemaNotFoundException
  ├── CatalogBackendException (wraps HMS/Glue/REST connectivity errors)
  └── MetadataCorruptionException (unexpected S3 content)
```

## Appendix B: Key Dependencies

| Library | Version | Purpose |
|---------|---------|---------|
| `org.apache.iceberg:iceberg-core` | 1.5.x | TableMetadata, Snapshot, ManifestFile APIs |
| `org.apache.iceberg:iceberg-aws` | 1.5.x | S3FileIO, GlueCatalog |
| `org.apache.iceberg:iceberg-hive-metastore` | 1.5.x | HiveCatalog |
| `com.github.ben-manes.caffeine:caffeine` | 3.x | LRU/TTL in-process cache |
| `org.apache.parquet:parquet-hadoop` | 1.13.x | Parquet footer schema inference |
| `org.apache.calcite:calcite-core` | 1.37.x | Calcite type system and RelNode tree |

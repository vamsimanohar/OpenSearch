# Component 10: Query Result Collector

## Table of Contents

1. [Overview and Responsibilities](#1-overview-and-responsibilities)
2. [Java Interfaces and Data Structures](#2-java-interfaces-and-data-structures)
3. [Result Flow: Worker to Client](#3-result-flow-worker-to-client)
4. [Streaming Results for Large Result Sets](#4-streaming-results-for-large-result-sets)
5. [Pagination with Cursor-Based Approach](#5-pagination-with-cursor-based-approach)
6. [Memory Management](#6-memory-management)
7. [Format Conversion: Arrow to Wire Formats](#7-format-conversion-arrow-to-wire-formats)
8. [Error Propagation and Partial Results](#8-error-propagation-and-partial-results)
9. [Result Size Limits and Guardrails](#9-result-size-limits-and-guardrails)
10. [Integration with OpenSearch REST Layer](#10-integration-with-opensearch-rest-layer)

---

## 1. Overview and Responsibilities

The Query Result Collector is the final stage of the distributed lakehouse query pipeline. After workers execute DataFusion physical plans and produce Apache Arrow `RecordBatch` streams, the Result Collector is responsible for gathering those batches, routing them back to the coordinator, converting them to the wire format the client expects, and delivering them — either as a single buffered response, a paginated cursor sequence, or a streaming chunked-HTTP body.

### Position in the System

```
  Worker Node 1                 Worker Node N
  ┌─────────────────┐           ┌─────────────────┐
  │ DataFusion exec │  . . .    │ DataFusion exec │
  │  RecordBatch[]  │           │  RecordBatch[]  │
  └────────┬────────┘           └────────┬────────┘
           │  Arrow Flight (binary stream)│
           └──────────────┬──────────────┘
                          │
                  Coordinator Node
               ┌──────────▼──────────┐
               │  QueryResultCollector│
               │  ┌───────────────┐  │
               │  │ ResultStream  │  │
               │  │ (lazy pull)   │  │
               │  └───────┬───────┘  │
               │          │          │
               │  ┌───────▼───────┐  │
               │  │PaginationMgr  │  │
               │  │ + ResultCache │  │
               │  └───────┬───────┘  │
               │          │          │
               │  ┌───────▼───────┐  │
               │  │ResultFormatter│  │
               │  │ JSON/JDBC/CSV │  │
               │  │ /ArrowFlight  │  │
               │  └───────┬───────┘  │
               └──────────┼──────────┘
                          │
              ┌───────────▼────────────┐
              │  REST / JDBC / Flight  │
              │        Client          │
              └────────────────────────┘
```

### Responsibilities

1. **Result aggregation** — open Arrow Flight streams from all leaf-stage workers for a completed or running query and merge them into a single logical result stream.
2. **Lazy pull / backpressure** — pull `RecordBatch` data from workers only as fast as the downstream client can consume it; never buffer the entire result set in coordinator heap.
3. **Format conversion** — convert Arrow columnar batches to JSON rows, JDBC `ResultSet`-compatible payloads, raw CSV, or pass Arrow IPC frames unchanged to Arrow Flight clients.
4. **Pagination** — materialize a result window on demand, persist a lightweight cursor, and allow clients to fetch subsequent pages without re-running the query.
5. **Result caching** — cache small result sets (≤ configurable byte threshold) in an LRU+TTL store so repeated fetches (e.g., JDBC driver retries) are served from memory.
6. **Error propagation** — detect mid-stream worker failures, surface partial-result signals to the client, and clean up allocated resources.
7. **Guardrails** — enforce row count and byte size limits; truncate or reject queries that would produce runaway results.
8. **REST integration** — expose results under `/_lakehouse/_sql` and `/_lakehouse/_ppl` endpoints using the same response envelope used by existing OpenSearch SQL endpoints.

---

## 2. Java Interfaces and Data Structures

All classes live under the package `org.opensearch.lakehouse.result` unless otherwise noted.

### 2.1 QueryResultCollector

The top-level entry point. Called by the coordinator after all required worker stages have reported task completion (or as soon as the first leaf-stage worker is ready for streaming queries).

```java
package org.opensearch.lakehouse.result;

import org.apache.arrow.vector.types.pojo.Schema;
import java.util.concurrent.CompletableFuture;

/**
 * Gathers Arrow RecordBatch streams from one or more workers for a single query
 * and exposes them through a unified ResultStream.
 *
 * Implementations must be thread-safe: a single collector instance may be
 * accessed by the HTTP response thread, a pagination-cursor fetch, and an
 * expiry background task concurrently.
 */
public interface QueryResultCollector extends AutoCloseable {

    /**
     * Begin collecting results for the given query.  The returned ResultStream
     * is lazy: no data is transferred until the caller invokes next().
     *
     * @param queryId    unique query identifier assigned by the coordinator
     * @param workerRefs ordered list of Arrow Flight endpoints that hold leaf-
     *                   stage output for this query; order determines merge order
     *                   for deterministic output
     * @param options    collection options (size limits, timeout, partial-ok flag)
     * @return           a not-yet-started ResultStream bound to this query
     * @throws ResultCollectionException if no workers are reachable or the query
     *                                   ID is unknown
     */
    ResultStream collectResults(
            String queryId,
            List<WorkerFlightEndpoint> workerRefs,
            CollectionOptions options) throws ResultCollectionException;

    /**
     * Retrieve the Arrow schema for a query without pulling any data rows.
     * Used by JDBC drivers during prepareStatement to expose column metadata.
     *
     * @param queryId unique query identifier
     * @return Arrow Schema describing output columns and their types
     */
    CompletableFuture<Schema> getSchema(String queryId);

    /**
     * Cancel an in-progress collection, releasing Arrow Flight streams and any
     * intermediate buffer allocations.
     *
     * @param queryId unique query identifier
     */
    void cancel(String queryId);

    @Override
    void close();
}
```

Supporting value types:

```java
/**
 * Identifies a single Arrow Flight endpoint on a worker node that holds output
 * data for one leaf stage of a query.
 */
@Value
public class WorkerFlightEndpoint {
    /** OpenSearch node ID (used for routing within the transport layer). */
    String nodeId;
    /** Arrow Flight ticket that the worker will honour. */
    FlightTicket ticket;
    /** Estimated output row count; -1 if unknown. */
    long estimatedRowCount;
    /** Estimated output bytes; -1 if unknown. */
    long estimatedBytes;
}

/** Tuning knobs supplied per-request. */
@Builder
@Value
public class CollectionOptions {
    /** Maximum rows to return before truncating (0 = use cluster default). */
    int maxRows;
    /** Maximum bytes to buffer in coordinator memory per batch fetch (default 64 MB). */
    int maxBatchBytes;
    /** If true, return whatever rows were collected when a worker fails mid-stream. */
    boolean allowPartialResults;
    /** Wall-clock deadline for the entire collection, in milliseconds from now. */
    long timeoutMs;
    /** If true, coordinator streams directly without materialising a cursor. */
    boolean streamingMode;
}
```

---

### 2.2 ResultStream

A pull-based iterator over `ArrowRecordBatch` objects. Lazily fetches from workers; never holds more than one batch in memory at a time on the coordinator (unless explicitly buffered by the caller).

```java
package org.opensearch.lakehouse.result;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Lazy, pull-based stream of Arrow RecordBatches for a single query result set.
 *
 * Lifecycle: created → (hasNext / next)* → close
 *
 * Thread safety: NOT thread-safe.  The owner is responsible for serialising
 * calls.  Callers that need concurrent access must synchronise externally.
 */
public interface ResultStream extends AutoCloseable {

    /**
     * Returns the Arrow schema of the result set.  Available immediately after
     * construction; does not require calling hasNext() first.
     *
     * @return Arrow Schema (column names, types, nullability)
     */
    Schema schema();

    /**
     * Returns true if at least one more RecordBatch is available.
     * May block briefly while waiting for the next batch to arrive over Flight.
     *
     * @return true if next() will return a non-empty batch
     * @throws ResultStreamException if the underlying Flight stream signals an error
     */
    boolean hasNext() throws ResultStreamException;

    /**
     * Returns the next RecordBatch.  The returned root is owned by the stream;
     * callers must not retain a reference past the next call to next() or close().
     * Use VectorSchemaRoot.slice() or copy explicitly if longer retention is needed.
     *
     * @return VectorSchemaRoot containing the next batch of rows
     * @throws NoSuchElementException  if hasNext() returned false
     * @throws ResultStreamException   if a worker error occurred mid-stream
     */
    VectorSchemaRoot next() throws ResultStreamException;

    /**
     * Returns the number of rows delivered so far across all batches.
     */
    long rowsDelivered();

    /**
     * Returns the number of bytes transferred (Arrow IPC wire size) so far.
     */
    long bytesTransferred();

    /**
     * Whether this stream has been terminated by a worker error.  When
     * allowPartialResults was true, hasNext() returns false after the error
     * and isPartial() returns true.
     */
    boolean isPartial();

    /**
     * The error that caused early termination, or empty if the stream completed
     * normally.
     */
    Optional<Throwable> streamError();

    /**
     * Releases all Arrow buffers and closes the underlying Flight stream.
     * Safe to call multiple times.
     */
    @Override
    void close();
}
```

---

### 2.3 ResultPage

A materialised snapshot of one page of results, returned by `PaginationManager.fetchPage()`. Supports both the lightweight `ExprValue`-based representation (for JSON/JDBC) and the raw Arrow batch (for Arrow Flight clients and CSV streaming).

```java
package org.opensearch.lakehouse.result;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine.Schema;

/**
 * One page of query results, as returned by PaginationManager.fetchPage().
 *
 * Either arrowBatch or rows is populated, depending on the formatter in use.
 * arrowBatch takes priority when present; rows is produced by the formatter
 * lazily only when first accessed.
 */
@Value
@Builder
public class ResultPage {

    /**
     * Arrow schema for the result set.  Present regardless of which data
     * representation is used.
     */
    Schema schema;

    /**
     * Rows as a list of ExprValue tuples; each element is one row.
     * Non-null when the page was produced for a JSON or JDBC client.
     * Null when arrowBatch is non-null (caller should use arrowBatch directly).
     */
    @Nullable List<ExprValue> rows;

    /**
     * Arrow columnar batch for this page.  Non-null for Arrow Flight and CSV
     * streaming paths.  Memory is owned by the caller after return from
     * fetchPage(); caller must close VectorSchemaRoot when done.
     */
    @Nullable VectorSchemaRoot arrowBatch;

    /**
     * Opaque cursor string that the client must pass to fetchPage() to obtain
     * the next page.  Null when this is the last page.
     */
    @Nullable String nextCursor;

    /**
     * 1-based index of the first row on this page within the full result set.
     */
    long pageStartRow;

    /**
     * Number of rows on this page.
     */
    int rowCount;

    /**
     * True if the result was truncated (hit a row/byte limit) and there may be
     * more data that was not returned.
     */
    boolean truncated;

    /**
     * True if at least one worker failed mid-stream and partial data was returned
     * instead of an error.
     */
    boolean partial;
}
```

---

### 2.4 ResultFormatter

Converts `ResultPage` or a raw `ResultStream` to a wire-format string or byte array for a specific client protocol.

```java
package org.opensearch.lakehouse.result.format;

import org.opensearch.lakehouse.result.ResultPage;
import org.opensearch.lakehouse.result.ResultStream;

/**
 * Converts query results to a client-specific wire format.
 *
 * Implementations must be stateless and thread-safe so a single instance can
 * be shared across concurrent requests.
 */
public interface ResultFormatter {

    /**
     * Format a materialised page into a complete response body.
     * Used for paginated requests and small result sets served from cache.
     *
     * @param page the result page to format
     * @return formatted response body as a String
     */
    String formatPage(ResultPage page);

    /**
     * Stream-format a ResultStream into the provided output, writing successive
     * chunks as data arrives.  The implementation is responsible for flushing
     * each chunk immediately so clients receive incremental data.
     *
     * @param stream      the live result stream to consume
     * @param output      destination for formatted chunks
     * @param chunkSizeHint suggested maximum bytes per flush (implementations
     *                      may ignore this)
     * @throws ResultFormatterException on serialisation or I/O errors
     */
    void formatStream(ResultStream stream, FormatterOutput output, int chunkSizeHint)
            throws ResultFormatterException;

    /**
     * Format a Throwable into a client-appropriate error response body.
     *
     * @param t the exception to format
     * @return formatted error body as a String
     */
    String formatError(Throwable t);

    /**
     * Returns the HTTP Content-Type value for responses produced by this formatter.
     */
    String contentType();
}

/** Sink for streaming formatter output. */
@FunctionalInterface
public interface FormatterOutput {
    /**
     * Write one chunk of formatted data.
     *
     * @param chunk  formatted bytes for this chunk
     * @param isFinal true if this is the last chunk
     * @throws IOException on write error
     */
    void write(byte[] chunk, boolean isFinal) throws IOException;
}
```

#### 2.4.1 JsonResultFormatter

```java
package org.opensearch.lakehouse.result.format;

/**
 * Formats results as the OpenSearch SQL JSON envelope used by /_sql and /_ppl.
 *
 * Single-page output:
 * {
 *   "schema": [{"name":"col","type":"keyword"}, ...],
 *   "datarows": [[val, ...], ...],
 *   "total": 1000,
 *   "size": 200,
 *   "cursor": "<base64-cursor>",   // omitted on last page
 *   "status": 200
 * }
 *
 * For streaming (chunked HTTP), each chunk is a self-contained JSON object
 * followed by a newline delimiter so clients can parse incrementally.
 */
public class JsonResultFormatter implements ResultFormatter {

    private final Style style;

    public enum Style { PRETTY, COMPACT }

    public JsonResultFormatter(Style style) { this.style = style; }

    @Override
    public String formatPage(ResultPage page) { ... }

    @Override
    public void formatStream(ResultStream stream, FormatterOutput output, int chunkSizeHint)
            throws ResultFormatterException { ... }

    @Override
    public String formatError(Throwable t) { ... }

    @Override
    public String contentType() { return "application/json; charset=UTF-8"; }
}
```

#### 2.4.2 JdbcResultFormatter

```java
package org.opensearch.lakehouse.result.format;

/**
 * Formats results in the JDBC-compatible envelope, matching the schema and
 * datarows layout produced by the existing JdbcResponseFormatter in the
 * opensearch-sql plugin.  Extends JsonResultFormatter to reuse JSON
 * serialisation; overrides buildJsonObject to produce JDBC-specific fields.
 *
 * Response shape:
 * {
 *   "schema": [{"name":"col","alias":null,"type":"keyword"}, ...],
 *   "datarows": [[v1, v2, ...], ...],
 *   "total": <long>,
 *   "size":  <int>,
 *   "status": 200,
 *   "cursor": "<base64>" // optional
 * }
 *
 * Error shape:
 * {
 *   "error": {"type":"...", "reason":"...", "details":"..."},
 *   "status": 400 | 503
 * }
 */
public class JdbcResultFormatter extends JsonResultFormatter {

    public JdbcResultFormatter(Style style) { super(style); }

    @Override
    public String formatPage(ResultPage page) { ... }

    @Override
    public String formatError(Throwable t) { ... }

    @Override
    public String contentType() { return "application/json; charset=UTF-8"; }
}
```

#### 2.4.3 ArrowFlightResultFormatter

```java
package org.opensearch.lakehouse.result.format;

import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Writes Arrow IPC RecordBatch messages directly into an Arrow Flight
 * ServerStreamListener.  No JSON serialisation overhead; intended for
 * high-throughput clients (Pandas, Spark, DuckDB) that speak Arrow natively.
 *
 * formatPage() is not used for streaming Arrow Flight; instead, callers use
 * the dedicated streamToFlight() method.  formatPage() is provided as a
 * fallback that serialises to Arrow IPC bytes wrapped in base64 JSON.
 */
public class ArrowFlightResultFormatter implements ResultFormatter {

    private final BufferAllocator allocator;

    public ArrowFlightResultFormatter(BufferAllocator allocator) {
        this.allocator = allocator;
    }

    /**
     * Streams all batches from a ResultStream into a Flight ServerStreamListener.
     * Applies backpressure by honouring listener.isReady() before each put.
     *
     * @param stream   source of Arrow RecordBatches
     * @param listener Arrow Flight listener for the client stream
     * @throws ResultFormatterException on Arrow serialisation or Flight errors
     */
    public void streamToFlight(ResultStream stream, ServerStreamListener listener)
            throws ResultFormatterException { ... }

    /** Serialises one page as Arrow IPC bytes (for cursor-based Arrow Flight). */
    @Override
    public String formatPage(ResultPage page) { ... }

    @Override
    public void formatStream(ResultStream stream, FormatterOutput output, int chunkSizeHint)
            throws ResultFormatterException { ... }

    @Override
    public String formatError(Throwable t) { ... }

    @Override
    public String contentType() { return "application/vnd.apache.arrow.stream"; }
}
```

#### 2.4.4 CsvResultFormatter

```java
package org.opensearch.lakehouse.result.format;

/**
 * Formats results as RFC 4180 CSV.
 *
 * First chunk is the header row (column names, comma-separated).
 * Subsequent chunks are data rows.  Values containing commas, double-quotes,
 * or newlines are wrapped in double-quotes; embedded double-quotes are escaped
 * as "".
 *
 * Sanitisation: leading = + - @ characters in cell values are prefixed with
 * a single-quote to prevent CSV injection (enabled by default; can be disabled
 * for trusted internal consumers).
 */
public class CsvResultFormatter implements ResultFormatter {

    private final String separator;
    private final boolean sanitize;
    private final boolean includeHeader;

    public CsvResultFormatter() { this(",", true, true); }

    public CsvResultFormatter(String separator, boolean sanitize, boolean includeHeader) {
        this.separator = separator;
        this.sanitize = sanitize;
        this.includeHeader = includeHeader;
    }

    @Override
    public String formatPage(ResultPage page) { ... }

    @Override
    public void formatStream(ResultStream stream, FormatterOutput output, int chunkSizeHint)
            throws ResultFormatterException { ... }

    @Override
    public String formatError(Throwable t) { ... }

    @Override
    public String contentType() { return "text/csv; charset=UTF-8"; }
}
```

---

### 2.5 PaginationManager

Manages the lifecycle of pagination cursors: their creation, storage, fetch, and expiry.

```java
package org.opensearch.lakehouse.result;

import java.time.Duration;

/**
 * Manages cursor-based pagination for query results.
 *
 * Cursors are lightweight tokens — they encode the query ID, the last-seen
 * Arrow Flight batch offset (or a stable row number for cached results), and a
 * HMAC to prevent forgery.  The bulk of result data is NOT stored in the cursor;
 * the coordinator re-opens the Flight stream at the recorded offset.
 *
 * Cursors are stored in an in-memory map with TTL eviction (default 5 minutes).
 * For multi-coordinator deployments the cursor store should be backed by a
 * distributed shard (e.g., an OpenSearch index); the default implementation
 * is single-node.
 */
public interface PaginationManager {

    /**
     * Create a new cursor for the first page of a query.
     *
     * @param queryId      unique query identifier
     * @param pageSize     number of rows per page
     * @param ttl          how long the cursor should remain valid
     * @param workerRefs   the worker Flight endpoints for this query (stored in
     *                     cursor state so subsequent fetches can reuse them)
     * @return opaque, URL-safe cursor string for use in the first fetchPage() call
     */
    String createCursor(
            String queryId,
            int pageSize,
            Duration ttl,
            List<WorkerFlightEndpoint> workerRefs);

    /**
     * Fetch one page of results using a cursor obtained from a previous call to
     * createCursor() or from ResultPage.nextCursor().
     *
     * Behaviour:
     *  - Opens (or resumes) the Arrow Flight stream at the recorded offset.
     *  - Pulls pageSize rows, converts them via the specified formatter, and
     *    advances the internal offset.
     *  - If the stream is exhausted, ResultPage.nextCursor() is null.
     *
     * @param cursorToken the opaque cursor string
     * @param formatter   formatter to use for this page
     * @return a fully populated ResultPage
     * @throws CursorNotFoundException   if the cursor has expired or is unknown
     * @throws CursorTamperedException   if HMAC validation fails
     * @throws ResultCollectionException on Flight or worker errors
     */
    ResultPage fetchPage(String cursorToken, ResultFormatter formatter)
            throws CursorNotFoundException, CursorTamperedException, ResultCollectionException;

    /**
     * Proactively expire a cursor and release any resources (Flight streams,
     * buffer allocations) associated with it.
     *
     * @param cursorToken the opaque cursor string
     */
    void expireCursor(String cursorToken);

    /**
     * Run a background sweep to expire all cursors past their TTL.
     * Should be called periodically (e.g., every 30 seconds) by a scheduler.
     */
    void expireAllStaleCursors();

    /**
     * Returns the number of active (non-expired) cursors currently managed.
     */
    int activeCursorCount();
}
```

Internal state per cursor (not exposed in the API):

```java
/** Internal record stored in the cursor map. */
@Value
class CursorState {
    String queryId;
    List<WorkerFlightEndpoint> workerRefs;
    /** Arrow batch index (0-based) of the last batch fully consumed. */
    long batchOffset;
    /** Row offset within that batch (for partial batch consumption). */
    int rowOffsetWithinBatch;
    int pageSize;
    Instant expiresAt;
    /** The open ResultStream, if one is currently held open; null otherwise. */
    @Nullable ResultStream openStream;
}
```

---

### 2.6 ResultCache

LRU cache for small result sets, keyed by query ID. Prevents redundant Flight round-trips for small queries (e.g., metadata queries, SHOW TABLES) that are frequently re-fetched by JDBC drivers.

```java
package org.opensearch.lakehouse.result;

import java.time.Duration;
import java.util.Optional;

/**
 * LRU cache for query result pages.
 *
 * Only results whose total byte size is below the per-entry size threshold
 * (default 4 MB) are eligible for caching.  The cache has a global byte
 * capacity (default 256 MB) and a per-entry TTL (default 60 seconds).
 *
 * Thread-safe.
 */
public interface ResultCache {

    /**
     * Store a complete (single-page) result set in the cache.
     *
     * @param queryId    unique query identifier (cache key)
     * @param page       the fully materialised result page
     * @param ttl        how long the entry should remain valid
     * @return true if the entry was accepted (size within threshold); false if
     *         it was too large and was not cached
     */
    boolean put(String queryId, ResultPage page, Duration ttl);

    /**
     * Retrieve a cached result set.
     *
     * @param queryId unique query identifier
     * @return the cached ResultPage, or empty if not present or expired
     */
    Optional<ResultPage> get(String queryId);

    /**
     * Evict a specific entry from the cache.
     *
     * @param queryId unique query identifier
     */
    void invalidate(String queryId);

    /**
     * Evict all entries whose TTL has passed.  Called periodically by a
     * background task.
     */
    void evictExpired();

    /** Returns the current number of cached entries. */
    int size();

    /** Returns the total bytes currently held in the cache. */
    long cachedBytes();

    /**
     * Returns cache hit statistics.
     *
     * @return CacheStats with hit count, miss count, eviction count
     */
    CacheStats stats();

    @Value
    class CacheStats {
        long hits;
        long misses;
        long evictions;
        long totalBytesEvicted;
    }
}
```

---

## 3. Result Flow: Worker to Client

### 3.1 Happy Path (Streaming)

```
Step 1  Coordinator calls QueryResultCollector.collectResults(queryId, workerRefs, opts)
        → returns a ResultStream (no data transferred yet)

Step 2  For each WorkerFlightEndpoint:
        Coordinator opens an Arrow Flight DoGet call using the worker's FlightTicket.
        The worker streams Arrow IPC RecordBatch messages over gRPC.

Step 3  ResultStream.hasNext() / next() drives consumption:
        - Fetches one RecordBatch at a time from the current worker Flight stream.
        - When one worker's stream is exhausted, moves to the next worker.
        - Applies row / byte count checks; raises ResultSizeLimitException when exceeded.

Step 4  ResultFormatter.formatStream() is called with the ResultStream:
        - For JSON: serialises each batch as JSON rows and writes them as HTTP chunks.
        - For Arrow Flight: passes VectorSchemaRoot directly to ServerStreamListener.
        - For CSV: writes header once, then data rows chunk by chunk.

Step 5  HTTP response is closed / gRPC stream is completed.
        ResultStream.close() releases Arrow buffers and Flight connections.
```

### 3.2 Sequence Diagram

```
Client          Coordinator                 Worker N
  │                  │                          │
  │  POST /_lakehouse/_sql                      │
  │─────────────────>│                          │
  │                  │ collectResults(queryId)  │
  │                  │─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─>│
  │                  │                          │
  │  HTTP 200 (chunked transfer-encoding)       │
  │<─────────────────│                          │
  │                  │  Flight DoGet(ticket)    │
  │                  │─────────────────────────>│
  │                  │  RecordBatch[0..n]       │
  │  JSON chunk 1   <│<─────────────────────────│
  │<─────────────────│                          │
  │  JSON chunk 2   <│<─── RecordBatch          │
  │<─────────────────│                          │
  │       ...        │        ...               │
  │  JSON chunk k   <│<─── EOS                  │
  │<─────────────────│                          │
  │  (stream closed) │                          │
```

---

## 4. Streaming Results for Large Result Sets

### 4.1 Chunked HTTP (REST clients)

For REST clients (`/_lakehouse/_sql`, `/_lakehouse/_ppl`), the coordinator uses OpenSearch's `RestChannel` with `chunkedResponse = true`:

```
HTTP/1.1 200 OK
Transfer-Encoding: chunked
Content-Type: application/json; charset=UTF-8

<chunk: {"schema":[...],"datarows":[\n>
<chunk:   [row0col0, row0col1, ...],\n>
<chunk:   [row1col0, row1col1, ...],\n>
...
<chunk: ],"total":1000000,"size":1000000,"status":200}>
0\r\n\r\n
```

Each HTTP chunk corresponds to one Arrow `RecordBatch` converted to JSON rows. The coordinator never buffers more than one batch (~64 MB default) at a time. Backpressure is implemented by checking `RestChannel.isWritable()` before fetching the next batch from the Flight stream.

### 4.2 Arrow Flight (native Arrow clients)

For native Arrow Flight clients, the coordinator acts as a Flight server and proxies batches with zero copy:

1. Client calls `DoGet(Ticket{queryId})` on the coordinator Flight endpoint.
2. Coordinator calls `QueryResultCollector.collectResults()` and obtains a `ResultStream`.
3. For each `VectorSchemaRoot` from `ResultStream.next()`, the coordinator calls `ServerStreamListener.putNext()` — passing the Arrow buffer reference directly without deserialization.
4. When the stream is exhausted, the coordinator calls `ServerStreamListener.completed()`.

This path achieves near-zero serialisation overhead: Arrow buffers produced by DataFusion on the worker are transmitted over the coordinator-to-worker Flight channel and then re-transmitted over the coordinator-to-client Flight channel, with only a reference copy on the coordinator heap.

### 4.3 Streaming Batch Size Tuning

| Setting | Default | Description |
|---|---|---|
| `lakehouse.result.batch.rows` | 10 000 | Target rows per Arrow batch |
| `lakehouse.result.batch.bytes` | 64 MB | Maximum bytes per Arrow batch |
| `lakehouse.result.http.chunk.bytes` | 1 MB | Target HTTP chunk size for JSON streaming |
| `lakehouse.result.flight.prefetch.batches` | 2 | Number of Arrow batches to prefetch from workers |

---

## 5. Pagination with Cursor-Based Approach

### 5.1 Design Principles

- Cursors are **stateless tokens**: they encode the information needed to resume a Flight stream at a specific offset. The coordinator does not store result data on disk between pages.
- Cursor tokens are **HMAC-signed** (HMAC-SHA256, coordinator-local key) to prevent forgery and detect tampering.
- Flight streams are **kept open** between pages for the same cursor (stored in `CursorState.openStream`) if the client fetches pages rapidly. Idle streams are closed after a configurable idle timeout (default 30 s) to release worker resources.
- For multi-coordinator clusters, cursors must encode enough state to allow any coordinator to re-open the worker Flight stream from scratch (i.e., `workerRefs` and `batchOffset` are embedded in the cursor payload, not stored server-side).

### 5.2 Cursor Encoding

```
cursor_payload = Base64URL(GZIP(JSON({
  "v":   1,                        // format version
  "qid": "<queryId>",
  "wrs": [                         // worker refs
    {"nid":"<nodeId>","tkt":"<base64 ticket>","est_rows":-1},
    ...
  ],
  "bo":  42,                       // batch offset (0-based)
  "ro":  0,                        // row offset within batch
  "ps":  200,                      // page size
  "exp": 1712345678                // UNIX epoch seconds
})))

cursor_token = cursor_payload + "." + Base64URL(HMAC-SHA256(key, cursor_payload))
```

### 5.3 Fetch-Next Flow

```
Client: POST /_lakehouse/_sql { "cursor": "<token>" }

Coordinator:
  1. Split token into payload + signature; verify HMAC. → CursorTamperedException on mismatch.
  2. Decode payload; check exp > now. → CursorNotFoundException if expired.
  3. Look up CursorState in in-memory map.
     a. If openStream is present and not closed, use it.
     b. Otherwise, call collectResults() with workerRefs, seek to batchOffset.
  4. Pull pageSize rows from the stream.
  5. Encode new cursor with updated batchOffset/rowOffset (or null if EOS).
  6. Format page via ResultFormatter and return HTTP response.
```

### 5.4 Pagination with JDBC

JDBC drivers typically call `Statement.setFetchSize(n)` to hint at page size. The coordinator's `JdbcResultFormatter` includes the `cursor` field in the response envelope. The JDBC driver sends subsequent fetches as cursor-continuation requests:

```
POST /_lakehouse/_sql
{ "cursor": "<token from previous response>" }
```

The driver translates the resulting `ResultPage` into JDBC `ResultSet` rows transparently.

---

## 6. Memory Management

### 6.1 Coordinator Memory Constraints

The coordinator must not buffer the entire result set in heap. The design enforces:

1. **One-batch-at-a-time** consumption: `ResultStream.next()` returns a single `VectorSchemaRoot`. The previous batch's buffers are released before the next batch is fetched (the `VectorSchemaRoot` reference is reused via `VectorSchemaRoot.clear()` + reload, or the previous root is closed before calling `next()` again).

2. **Circuit breaker integration**: Arrow buffer allocations go through a `BufferAllocator` that is wired to the OpenSearch `CircuitBreakerService`. If the `PARENT` breaker trips, the result collection is aborted with a `ResultCollectionException`, and an appropriate HTTP 503 is returned.

3. **Off-heap Arrow buffers**: `BufferAllocator` (Apache Arrow `RootAllocator`) allocates from direct (off-heap) memory. This keeps Arrow batches out of the JVM heap and reduces GC pressure. The JVM heap is used only for the formatter's string buffers.

### 6.2 Memory Budget per Request

```
Total coordinator memory for one query result collection:
  = max(
      1 Arrow RecordBatch (≤ lakehouse.result.batch.bytes, default 64 MB),
      prefetch_batches * batch_bytes            // for Arrow Flight path
    )
  + formatter overhead (JSON string buffer, ≤ 2 × batch_bytes in worst case)
  + cursor state (negligible)
```

### 6.3 Formatter Memory: Row-by-Row vs. Batch

- `JsonResultFormatter`: converts one `RecordBatch` at a time to a JSON string, flushes it as an HTTP chunk, then releases the string buffer. Peak heap usage is proportional to one batch, not the full result set.
- `CsvResultFormatter`: same pattern — one batch to CSV string, flush, release.
- `ArrowFlightResultFormatter`: passes Arrow buffer references directly; no copy on coordinator heap.
- `JdbcResultFormatter`: builds one `Object[][]` per page (at most `pageSize` rows × column count). `pageSize` defaults to 10 000 rows; at typical 200 bytes per row this is ≤ 2 MB per page.

### 6.4 ResultCache Memory

The `ResultCache` has a configurable total capacity:

```
lakehouse.result.cache.max.bytes   = 256 MB  (default)
lakehouse.result.cache.max.entries = 1000     (default)
lakehouse.result.cache.entry.max.bytes = 4 MB (per-entry threshold; larger results not cached)
lakehouse.result.cache.ttl.seconds = 60       (default TTL)
```

Eviction is LRU by access time, with an additional TTL sweep every 30 s on a background thread.

---

## 7. Format Conversion: Arrow RecordBatch to Wire Formats

### 7.1 Arrow → JSON

Each `VectorSchemaRoot` column is a typed Arrow vector. The conversion iterates column vectors and serialises each cell:

| Arrow Type | JSON representation |
|---|---|
| `Int8/16/32/64` | JSON number |
| `Float32/Float64` | JSON number; `NaN`/`Inf` → `null` with a warning |
| `Utf8 / LargeUtf8` | JSON string |
| `Boolean` | `true` / `false` |
| `Date32` | ISO-8601 string `"YYYY-MM-DD"` |
| `TimestampMicro(UTC)` | ISO-8601 string `"YYYY-MM-DDTHH:MM:SS.ffffffZ"` |
| `Struct` | Nested JSON object |
| `List` | JSON array |
| `Decimal128` | JSON string (to preserve precision) |
| `Null / missing` | `null` |

Conversion is done using Jackson's `JsonGenerator` in streaming mode; no intermediate `Map<String,Object>` is allocated per row.

### 7.2 Arrow → JDBC ResultSet

The `JdbcResultFormatter` builds `Object[][]` where each element is a Java type compatible with `ResultSet.getObject()`:

| Arrow Type | Java type |
|---|---|
| `Int32` | `Integer` |
| `Int64` | `Long` |
| `Float64` | `Double` |
| `Utf8` | `String` |
| `Boolean` | `Boolean` |
| `Date32` | `java.sql.Date` |
| `TimestampMicro` | `java.sql.Timestamp` |
| `Decimal128` | `java.math.BigDecimal` |
| `Struct / List` | `String` (JSON-encoded) |
| `Null` | `null` |

The JDBC driver accesses these values via the standard `ResultSet` interface mapped by the OpenSearch JDBC driver.

### 7.3 Arrow → CSV

Each row is serialised as a single comma-separated line. Nested types (`Struct`, `List`) are JSON-encoded and quoted. Empty strings and nulls are represented as empty fields. The sanitiser prepends `'` to cells whose first character is `=`, `+`, `-`, or `@`.

### 7.4 Arrow → Arrow (Flight passthrough)

No conversion. The coordinator calls `VectorSchemaRoot` re-use: it holds the root as received from the worker Flight stream and passes it directly to `ServerStreamListener.putNext()`. The Arrow IPC framing is performed by the Flight library, not by application code.

---

## 8. Error Propagation and Partial Results

### 8.1 Error Classification

| Error type | Source | Default behaviour |
|---|---|---|
| Worker crashes mid-stream | Flight `UNAVAILABLE` status | Abort query; return 503 to client |
| Worker returns partial data then fails | Flight EOS before expected row count | If `allowPartialResults=true`, deliver what was received with `"partial":true` flag; else abort |
| Arrow allocation failure on coordinator | `OutOfMemoryError` / breaker trip | Abort; return 503; release all buffers |
| Result size limit exceeded | Row/byte counter | Truncate stream; return 200 with `"truncated":true` flag |
| Cursor expired | TTL eviction | Return 400 `CursorNotFoundException` |
| Query cancelled by client | REST DELETE or Flight cancel | Abort stream; release Flight connections |

### 8.2 Partial Results Protocol

When `CollectionOptions.allowPartialResults = true` (default: `false`; can be set per-request via `"partial_results": true` in the request body):

1. `ResultStream` catches `ResultStreamException` caused by a worker Flight failure.
2. It sets `isPartial = true` and `streamError` to the caught exception.
3. `hasNext()` returns `false` immediately (stream terminates with whatever was collected so far).
4. The formatter adds `"partial": true` and `"warnings": [{"type":"WORKER_FAILURE","worker":"<nodeId>","message":"..."}]` to the response envelope.

### 8.3 Mid-Stream Error Response

For JSON / JDBC clients on chunked HTTP, a mid-stream error cannot change the HTTP status code (already sent as 200). Instead, the formatter appends a terminal error object as the last chunk:

```json
{"error":{"type":"WORKER_FAILURE","reason":"Worker node-3 disconnected","details":"..."},"partial":true,"rows_delivered":42000}
```

For Arrow Flight clients, the coordinator calls `ServerStreamListener.error(FlightStatusCode.INTERNAL, ...)` to signal the error via the gRPC status trailer.

---

## 9. Result Size Limits and Guardrails

### 9.1 Configurable Limits

| Setting | Default | Scope | Description |
|---|---|---|---|
| `lakehouse.result.max.rows` | 10 000 | Cluster | Maximum rows returned per non-streaming query |
| `lakehouse.result.max.bytes` | 100 MB | Cluster | Maximum uncompressed bytes per response |
| `lakehouse.result.stream.max.rows` | 100 000 000 | Cluster | Streaming mode hard cap |
| `lakehouse.result.stream.max.bytes` | 10 GB | Cluster | Streaming mode byte cap |
| `lakehouse.result.page.max.rows` | 10 000 | Cluster | Maximum rows per page in paginated mode |
| `lakehouse.result.cache.entry.max.bytes` | 4 MB | Cluster | Maximum result size eligible for caching |

Per-request overrides can be specified in the request body: `"limit": 500` (row limit), `"fetch_size": 200` (page size). Per-request values cannot exceed cluster defaults.

### 9.2 Truncation vs. Rejection

- **Truncation** (default): when `maxRows` is hit during streaming, the stream is terminated gracefully and `"truncated": true` is added to the response. The client receives a valid (though incomplete) result.
- **Rejection**: if the query planner can determine at planning time that the result set will exceed limits (via statistics), it may inject a `LIMIT` node into the physical plan before dispatch. This is a best-effort optimisation; runtime truncation always remains the safety net.

### 9.3 Memory Circuit Breaker Integration

The `ResultCollectorService` registers a named child allocator under the OpenSearch `REQUEST` circuit breaker:

```java
BufferAllocator requestAllocator = parentAllocator.newChildAllocator(
    "lakehouse-result-" + queryId,
    0L,
    options.getMaxBatchBytes()
);
```

If `requestAllocator.buffer(n)` throws `OutOfMemoryException` (Arrow's variant), the collector catches it, closes the stream, and returns an HTTP 503 with `"error": {"type":"CIRCUIT_BREAKER_OPEN"}`.

---

## 10. Integration with OpenSearch REST Layer

### 10.1 REST Endpoints

Two new REST endpoints are registered by the lakehouse plugin:

| Method | Path | Description |
|---|---|---|
| `POST` | `/_lakehouse/_sql` | Execute SQL query; returns JSON result |
| `POST` | `/_lakehouse/_ppl` | Execute PPL query; returns JSON result |
| `POST` | `/_lakehouse/_sql` | With `"cursor"` field: fetch next page |
| `DELETE` | `/_lakehouse/_sql/scroll/<cursorToken>` | Expire a cursor explicitly |
| `GET` | `/_lakehouse/_sql/schema/<queryId>` | Retrieve schema without data (for JDBC prepareStatement) |

These mirror the existing `/_plugins/_sql` and `/_plugins/_ppl` paths from the OpenSearch SQL plugin so existing clients can be redirected with minimal changes.

### 10.2 Request / Response Envelope

**SQL request:**
```json
{
  "query": "SELECT id, name FROM hive.sales.orders WHERE year = 2024",
  "fetch_size": 200,
  "format": "jdbc",
  "partial_results": false,
  "parameters": []
}
```

**Cursor-continuation request:**
```json
{
  "cursor": "<opaque cursor token>"
}
```

**Successful response (JSON format):**
```json
{
  "schema": [
    {"name": "id",   "type": "long"},
    {"name": "name", "type": "keyword"}
  ],
  "datarows": [
    [1, "Alice"],
    [2, "Bob"]
  ],
  "total": 1000000,
  "size": 200,
  "cursor": "<next page cursor, omitted on last page>",
  "status": 200,
  "partial": false,
  "truncated": false
}
```

**Error response:**
```json
{
  "error": {
    "type":    "SemanticCheckException",
    "reason":  "column 'foo' not found",
    "details": "..."
  },
  "status": 400
}
```

### 10.3 REST Handler Wiring

```java
package org.opensearch.lakehouse.rest;

/**
 * REST handler for /_lakehouse/_sql and /_lakehouse/_ppl.
 * Dispatches to LakehouseQueryCoordinator for plan/execute and then to
 * QueryResultCollector for result formatting and delivery.
 */
public class LakehouseSqlRestAction extends BaseRestHandler {

    private final LakehouseQueryCoordinator coordinator;
    private final QueryResultCollector resultCollector;
    private final PaginationManager paginationManager;
    private final ResultFormatterRegistry formatterRegistry;

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        return channel -> {
            String body = request.content().utf8ToString();
            LakehouseQueryRequest req = parseRequest(body);

            if (req.getCursor() != null) {
                // Cursor-based next-page fetch
                ResultFormatter formatter = formatterRegistry.get(req.getFormat());
                ResultPage page = paginationManager.fetchPage(req.getCursor(), formatter);
                sendPage(channel, page, formatter);
            } else {
                // New query
                String queryId = coordinator.submit(req);
                List<WorkerFlightEndpoint> workerRefs = coordinator.awaitFirstStageReady(queryId);
                CollectionOptions opts = buildOptions(req);
                ResultFormatter formatter = formatterRegistry.get(req.getFormat());

                if (opts.isStreamingMode()) {
                    // Chunked HTTP streaming path
                    ResultStream stream = resultCollector.collectResults(queryId, workerRefs, opts);
                    streamResponse(channel, stream, formatter, opts);
                } else {
                    // Paginated path: fetch first page, create cursor
                    String cursor = paginationManager.createCursor(
                        queryId, req.getFetchSize(), DEFAULT_CURSOR_TTL, workerRefs);
                    ResultPage page = paginationManager.fetchPage(cursor, formatter);
                    sendPage(channel, page, formatter);
                }
            }
        };
    }

    private void streamResponse(RestChannel channel, ResultStream stream,
                                ResultFormatter formatter, CollectionOptions opts) {
        channel.sendResponse(new StreamedRestResponse(channel, formatter.contentType(), output -> {
            formatter.formatStream(stream, output, HTTP_CHUNK_SIZE_BYTES);
        }));
    }

    private void sendPage(RestChannel channel, ResultPage page, ResultFormatter formatter) {
        String body = formatter.formatPage(page);
        channel.sendResponse(new BytesRestResponse(RestStatus.OK, formatter.contentType(),
            new BytesArray(body.getBytes(StandardCharsets.UTF_8))));
    }
}
```

### 10.4 Format Selection

The `format` request parameter (or `Content-Type` negotiation for Arrow Flight) selects the formatter:

| `format` value | Formatter | Content-Type |
|---|---|---|
| `json` (default) | `JsonResultFormatter(COMPACT)` | `application/json` |
| `jdbc` | `JdbcResultFormatter(COMPACT)` | `application/json` |
| `csv` | `CsvResultFormatter` | `text/csv` |
| `raw` | `CsvResultFormatter(sanitize=false)` | `text/plain` |
| `arrow` | `ArrowFlightResultFormatter` | `application/vnd.apache.arrow.stream` |

Arrow Flight clients bypass the REST handler entirely and connect to the coordinator's Flight port using `DoGet(Ticket{queryId})`, which routes to `ArrowFlightResultFormatter.streamToFlight()`.

### 10.5 Interaction with Existing OpenSearch SQL Plugin

The existing `/_plugins/_sql` endpoint and its `JdbcResponseFormatter` (in `opensearch-sql/protocol/`) remain unchanged. The lakehouse endpoints are additive and share the same response envelope shape to maximise compatibility with the existing OpenSearch JDBC driver (`org.opensearch.sql.jdbc`). The JDBC driver needs no modification to consume lakehouse results once its endpoint URL is pointed at `/_lakehouse/_sql`.

---

## Appendix A: Key Dependencies

| Dependency | Version | Purpose |
|---|---|---|
| `org.apache.arrow:arrow-vector` | 14.x | Arrow columnar format, `VectorSchemaRoot` |
| `org.apache.arrow:arrow-flight-core` | 14.x | Arrow Flight RPC (DoGet, ServerStreamListener) |
| `org.apache.arrow:arrow-memory-netty` | 14.x | Netty-backed off-heap allocator |
| `com.fasterxml.jackson.core:jackson-core` | 2.15.x | Streaming JSON generation |
| `org.opensearch.sql:protocol` | (project) | `ResponseFormatter`, `QueryResult`, `Cursor` |
| `org.opensearch:opensearch` | 2.x | `CircuitBreakerService`, `RestChannel`, `ThreadPool` |

---

## Appendix B: Threading Model

| Thread | Responsibility |
|---|---|
| OpenSearch HTTP transport thread | Receives REST request; delegates to `LakehouseSqlRestAction`; must not block |
| `lakehouse-result-fetch` thread pool | Blocking Arrow Flight `DoGet` calls to workers; one thread per active Flight stream |
| `lakehouse-format` thread pool | JSON/CSV serialisation; one thread per active response |
| `lakehouse-cursor-expiry` scheduler | Periodic `PaginationManager.expireAllStaleCursors()` and `ResultCache.evictExpired()` |
| Arrow Flight executor (from `FlightStreamPlugin`) | Handles incoming `DoGet` from external Arrow Flight clients |

The `ResultStream` is not thread-safe; each stream is owned by exactly one thread at a time (either the fetch thread during data retrieval or the format thread during serialisation). Thread handoff between fetch and format uses a bounded `ArrayBlockingQueue<VectorSchemaRoot>` inside the stream implementation.

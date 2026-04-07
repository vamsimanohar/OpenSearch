# Component 9: Shuffle (Arrow Flight Exchange)

## Table of Contents

1. [Overview and Responsibilities](#1-overview-and-responsibilities)
2. [Interfaces](#2-interfaces)
   - 2.1 [Rust Worker-Side Interfaces](#21-rust-worker-side-interfaces)
   - 2.2 [Java Coordinator-Side Interface](#22-java-coordinator-side-interface)
3. [Hash Shuffle Protocol](#3-hash-shuffle-protocol)
4. [Broadcast Shuffle Protocol](#4-broadcast-shuffle-protocol)
5. [Gather Shuffle Protocol](#5-gather-shuffle-protocol)
6. [Backpressure Mechanism](#6-backpressure-mechanism)
7. [Memory Management](#7-memory-management)
8. [Network Optimization](#8-network-optimization)
9. [Failure Handling](#9-failure-handling)
10. [Shuffle Cleanup](#10-shuffle-cleanup)
11. [Metrics](#11-metrics)

---

## 1. Overview and Responsibilities

The Shuffle component moves intermediate result data between pipeline stages across worker nodes. Each stage in a multi-stage query plan produces output that must be redistributed before downstream stages can consume it. The Shuffle component implements this redistribution using Apache Arrow Flight's `DoExchange` RPC, which allows bidirectional streaming of Arrow record batches between two endpoints.

### Position in the System

```
                        ┌─────────────────────────────────────┐
                        │           Coordinator (JVM)          │
                        │                                      │
                        │  ┌──────────────────────────────┐   │
                        │  │       ShuffleManager          │   │
                        │  │ (tracks shuffle state per     │   │
                        │  │  query; drives cleanup)       │   │
                        │  └──────────────────────────────┘   │
                        └─────────────────────────────────────┘
                                         │  gRPC task assignment
                    ┌────────────────────┼───────────────────────┐
                    ▼                    ▼                        ▼
          ┌─────────────────┐  ┌─────────────────┐   ┌─────────────────┐
          │  Worker A (Rust) │  │  Worker B (Rust) │   │  Worker C (Rust) │
          │                 │  │                 │   │                 │
          │ ┌─────────────┐ │  │ ┌─────────────┐ │   │ ┌─────────────┐ │
          │ │ShuffleService│ │  │ │ShuffleService│ │   │ │ShuffleService│ │
          │ │ (Flight srv) │ │  │ │ (Flight srv) │ │   │ │ (Flight srv) │ │
          │ └──────┬──────┘ │  │ └──────┬──────┘ │   │ └──────┬──────┘ │
          │        │        │  │        │        │   │        │        │
          │ ┌──────▼──────┐ │  │ ┌──────▼──────┐ │   │ ┌──────▼──────┐ │
          │ │ShuffleWriter│─┼──┼─▶ShuffleBuffer│ │   │ │ShuffleReader│ │
          │ └─────────────┘ │  │ └─────────────┘ │   │ └─────────────┘ │
          └─────────────────┘  └─────────────────┘   └─────────────────┘
                   Stage N producers           Stage N+1 consumers
```

### Exchange Types

| Type | Description | Partitioning |
|---|---|---|
| `HASH` | Each row is routed to exactly one downstream partition by hash of key columns | `murmur3(key_cols) % num_partitions` |
| `BROADCAST` | Every row is sent to every downstream partition | Full replication |
| `GATHER` | All rows are sent to a single downstream partition (typically partition 0) | Converge to one |

### Responsibilities

1. **Partition routing** — compute the target partition for every row in every output batch.
2. **Batch splitting and buffering** — break a single output batch into per-partition sub-batches; buffer until a flush threshold is reached or the stage completes.
3. **Arrow Flight DoExchange transport** — establish bidirectional Flight streams between sender and receiver workers.
4. **Backpressure** — slow down senders when receiver buffers are full.
5. **Memory management** — cap per-shuffle buffer memory; spill excess to local disk.
6. **Failure recovery** — retry failed Flight streams; surface unrecoverable errors to the coordinator.
7. **Cleanup** — release Arrow buffers and Flight connections after the downstream stage finishes consuming.

---

## 2. Interfaces

### 2.1 Rust Worker-Side Interfaces

All Rust interfaces live in the `lakehouse-worker` crate under `src/shuffle/`.

#### ShuffleService

The `ShuffleService` trait is implemented by the Arrow Flight server handler that accepts inbound `DoExchange` calls from upstream senders.

```rust
use arrow_flight::flight_service_server::FlightService;
use arrow::record_batch::RecordBatch;
use tonic::Streaming;

/// Unique identifier for a shuffle exchange within a query.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ShuffleDescriptor {
    pub query_id: String,   // UUID assigned by coordinator
    pub stage_id: u32,      // producing stage index
    pub partition_id: u32,  // partition index this receiver owns
}

/// Implemented by the worker's Flight server to accept incoming shuffle data.
///
/// A worker that owns partition P of downstream stage S+1 runs one
/// ShuffleService instance.  All upstream workers for stage S open a
/// DoExchange call to this service and stream their sub-batches for
/// partition P.
pub trait ShuffleService: Send + Sync + 'static {
    /// Called once per incoming DoExchange stream.
    ///
    /// `descriptor` identifies which shuffle this stream belongs to.
    /// `incoming` is the stream of record batches from one upstream sender.
    /// Returns a stream of acknowledgement messages (used for backpressure).
    fn accept_exchange(
        &self,
        descriptor: ShuffleDescriptor,
        incoming: Streaming<RecordBatch>,
    ) -> impl futures::Stream<Item = Result<FlightAck, tonic::Status>> + Send;

    /// Register a new shuffle before any data arrives.
    /// Called by the coordinator via a separate control RPC before stage N starts.
    fn register_shuffle(
        &self,
        descriptor: ShuffleDescriptor,
        num_senders: u32,
        schema: arrow::datatypes::SchemaRef,
    ) -> Result<(), ShuffleError>;

    /// Signal that the shuffle is complete and buffers can be released.
    fn close_shuffle(&self, descriptor: ShuffleDescriptor) -> Result<(), ShuffleError>;
}
```

#### ShuffleWriter

`ShuffleWriter` is used by stage execution to write output batches. It partitions each batch and flushes sub-batches to the appropriate receiver workers over Flight.

```rust
use arrow::record_batch::RecordBatch;

/// One ShuffleWriter per stage task (one writer per producing partition).
pub trait ShuffleWriter: Send + 'static {
    /// Partition `batch` and send sub-batches to each downstream partition.
    ///
    /// This method is non-blocking; sub-batches are accumulated in an
    /// internal buffer and flushed asynchronously.  Backpressure from
    /// receivers is applied here: the future will not resolve until the
    /// sender's in-flight byte count drops below the high-water mark.
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<(), ShuffleError>;

    /// Flush all buffered sub-batches and signal end-of-stream to each receiver.
    /// Must be called exactly once after the last write_batch.
    async fn finish(&mut self) -> Result<(), ShuffleError>;

    /// Return a snapshot of per-partition byte counts written so far.
    fn stats(&self) -> WriterStats;
}

#[derive(Debug)]
pub struct WriterStats {
    pub batches_written: u64,
    pub bytes_written: u64,
    pub partition_bytes: Vec<u64>,  // indexed by partition_id
}
```

#### HashPartitioner

`HashPartitioner` computes the target partition index for each row in a batch based on a set of key columns.

```rust
use arrow::record_batch::RecordBatch;
use arrow::array::UInt32Array;

/// Computes per-row partition assignments using murmur3 hashing.
pub struct HashPartitioner {
    key_column_indices: Vec<usize>,
    num_partitions: u32,
}

impl HashPartitioner {
    /// Create a new partitioner.
    ///
    /// `key_column_indices` are positions within the batch schema of the
    /// hash key columns.  `num_partitions` is the downstream fan-out.
    pub fn new(key_column_indices: Vec<usize>, num_partitions: u32) -> Self;

    /// Compute partition assignments for every row in `batch`.
    ///
    /// Returns a UInt32Array of length batch.num_rows() where each element
    /// is in [0, num_partitions).
    ///
    /// Algorithm:
    ///   for each row r:
    ///     hash = 0
    ///     for each key column c:
    ///         hash = murmur3_combine(hash, encode_nullable_scalar(batch[c][r]))
    ///     partition[r] = hash % num_partitions
    pub fn partition_batch(&self, batch: &RecordBatch) -> Result<UInt32Array, ShuffleError>;

    /// Split `batch` into per-partition sub-batches.
    ///
    /// Returns a Vec of length num_partitions; empty partitions produce
    /// a zero-row RecordBatch (schema preserved) so receivers can detect
    /// EOS from a sender without ambiguity.
    pub fn split_batch(
        &self,
        batch: &RecordBatch,
        assignments: &UInt32Array,
    ) -> Result<Vec<RecordBatch>, ShuffleError>;
}
```

**Hashing details:**

- Column values are serialized to bytes using a canonical, schema-aware encoding before hashing:
  - Integers: little-endian fixed-width bytes.
  - Strings/Binary: raw UTF-8/binary bytes without length prefix.
  - Decimals: 16-byte little-endian fixed-width.
  - `NULL`: a single sentinel byte `0x00` that cannot appear in valid non-null encodings.
- Multi-column keys are hashed incrementally: `h = murmur3_32(encode(col[i]), seed=h)` for each key column in declaration order.
- Final partition: `h.wrapping_abs() % num_partitions` (wrapping abs avoids i32::MIN overflow).

#### ShuffleReader

`ShuffleReader` is consumed by downstream stage execution to read all record batches for the partition it owns.

```rust
use arrow::record_batch::RecordBatch;

/// One ShuffleReader per downstream task.
/// Merges incoming streams from all upstream senders into a single ordered stream.
pub trait ShuffleReader: Send + 'static {
    /// Await the next available RecordBatch from any upstream sender.
    ///
    /// Returns None when all senders have sent EOS and all buffered batches
    /// have been consumed.
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, ShuffleError>;

    /// True when all senders have been registered and all have sent EOS.
    fn is_complete(&self) -> bool;

    /// Return accumulated read statistics.
    fn stats(&self) -> ReaderStats;
}

#[derive(Debug)]
pub struct ReaderStats {
    pub batches_received: u64,
    pub bytes_received: u64,
    pub senders_complete: u32,
    pub senders_total: u32,
}
```

#### ShuffleBuffer

`ShuffleBuffer` is the in-memory store for a single receiver partition. It is populated by the `ShuffleService` (one entry per sender stream) and drained by the `ShuffleReader`.

```rust
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use tokio::sync::Notify;

/// Bounded in-memory buffer for one receiver partition.
///
/// Multiple sender tasks push into this buffer concurrently.
/// One reader task drains it.
pub struct ShuffleBuffer {
    descriptor: ShuffleDescriptor,
    max_memory_bytes: usize,
    spill_path: Option<std::path::PathBuf>,

    // Internal state (wrapped in Arc<Mutex<...>>)
    inner: Arc<ShuffleBufferInner>,
}

struct ShuffleBufferInner {
    queue: std::collections::VecDeque<RecordBatch>,
    queued_bytes: usize,
    senders_done: u32,
    senders_total: u32,
    ready: Notify,         // notified when a batch is enqueued or all senders done
    drain_ready: Notify,   // notified when bytes fall below low-water mark
}

impl ShuffleBuffer {
    /// Push a batch from one sender.  Blocks (async) if the buffer is full
    /// (i.e., queued_bytes >= max_memory_bytes) until the reader drains enough.
    pub async fn push(&self, batch: RecordBatch) -> Result<(), ShuffleError>;

    /// Signal that one sender has finished.  When senders_done == senders_total
    /// and the queue is empty, next_batch() will return None.
    pub fn sender_done(&self);

    /// Pull the next batch.  Awaits until a batch is available or all senders
    /// have sent EOS and the queue is empty.
    pub async fn next_batch(&self) -> Result<Option<RecordBatch>, ShuffleError>;

    /// Current estimated memory usage in bytes.
    pub fn memory_bytes(&self) -> usize;
}
```

---

### 2.2 Java Coordinator-Side Interface

#### ShuffleManager

`ShuffleManager` lives on the coordinator JVM and tracks the lifecycle of every shuffle exchange in the system.

```java
package org.opensearch.lakehouse.shuffle;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Coordinator-side registry for shuffle exchanges.
 *
 * One ShuffleManager instance exists per coordinator node.
 * Thread-safe: all methods may be called from any thread.
 */
public interface ShuffleManager {

    /**
     * Register all exchanges for a query stage before any workers start.
     *
     * For a HASH exchange with N downstream partitions this registers N
     * ShuffleDescriptors and informs the N receiver workers via a control
     * RPC so they can pre-allocate buffers.
     *
     * @param queryId  unique query identifier
     * @param stageId  the producing stage
     * @param spec     exchange specification (type, key columns, fan-out)
     * @param workers  the full worker topology for this query
     * @return         a future that completes when all receivers have acknowledged
     */
    CompletableFuture<Void> registerExchange(
        String queryId,
        int stageId,
        ExchangeSpec spec,
        WorkerTopology workers
    );

    /**
     * Return the state of all exchanges for a query, keyed by stageId.
     */
    Map<Integer, ExchangeState> getExchangeStates(String queryId);

    /**
     * Release all buffers and connections associated with a query's shuffles.
     * Called after the query completes (success or failure).
     *
     * Sends a close_shuffle control RPC to every receiver worker.
     */
    CompletableFuture<Void> cleanupQuery(String queryId);
}
```

```java
/** Describes a single exchange edge between two stages. */
public record ExchangeSpec(
    ExchangeType type,            // HASH, BROADCAST, GATHER
    List<Integer> keyColumnIndices,  // empty for BROADCAST / GATHER
    int numPartitions,            // downstream fan-out
    SchemaRef schema              // Arrow schema of the shuffled data
) {}

public enum ExchangeType { HASH, BROADCAST, GATHER }

/** Runtime state of one exchange, returned by getExchangeStates(). */
public record ExchangeState(
    String queryId,
    int stageId,
    ExchangeType type,
    int numPartitions,
    long totalBytesSent,
    long totalBytesReceived,
    Map<Integer, PartitionState> partitions  // partition_id -> state
) {}

public enum PartitionState { PENDING, RECEIVING, COMPLETE, FAILED }
```

---

## 3. Hash Shuffle Protocol

### 3.1 Flight Descriptor Encoding

Every `DoExchange` call carries a `FlightDescriptor` whose `cmd` field is a serialized protobuf:

```protobuf
// shuffle_descriptor.proto
message ShuffleDescriptor {
  string query_id    = 1;
  uint32 stage_id    = 2;   // producing stage
  uint32 partition_id = 3;  // receiver partition index
  uint32 sender_id   = 4;   // ordinal of this sender worker (0-based)
}
```

The receiver uses `(query_id, stage_id, partition_id)` to look up the `ShuffleBuffer` to push into. `sender_id` is used to count how many senders have closed so the buffer knows when to signal EOS.

### 3.2 End-to-End Flow

```
Coordinator
  │
  ├─ 1. registerExchange(queryId, stageId, HASH, 4 partitions, workers)
  │       sends register_shuffle RPC to receiver workers W1..W4
  │
  ├─ 2. assign stage N tasks to senders S0..S3
  │       each task is told: "your exchange is HASH, downstream partitions at W0..W3"
  │
  └─ 3. assign stage N+1 tasks to receivers R0..R3
         each receiver is told: "read partition P from your local ShuffleBuffer"

Sender S0 (stage N):
  │
  ├─ 4. for each output RecordBatch B:
  │       a. assignments = HashPartitioner.partition_batch(B)
  │       b. sub_batches = HashPartitioner.split_batch(B, assignments)
  │       c. for each partition p in [0..3]:
  │            buffer[p].append(sub_batches[p])
  │            if buffer[p].size >= FLUSH_THRESHOLD:
  │              flush buffer[p] to receiver Wp via DoExchange
  │
  ├─ 5. ShuffleWriter.finish():
  │       flush all remaining buffers for all partitions
  │       close all DoExchange streams (sends EOS marker)
  │
  └─ done

Receiver W2 (owns partition 2):
  │
  ├─ 6. ShuffleService.accept_exchange() called for each sender S0..S3
  │       pushes incoming batches into ShuffleBuffer[partition=2]
  │
  ├─ 7. ShuffleBuffer.sender_done() called when each sender closes its stream
  │
  └─ 8. Stage N+1 task on W2 calls ShuffleReader.next_batch() in a loop
         drains ShuffleBuffer[partition=2] until None is returned
```

### 3.3 DoExchange Stream Lifecycle

```
Sender                                    Receiver
  │                                           │
  │──── DoExchange(descriptor) ──────────────▶│
  │                                           │ locate ShuffleBuffer by descriptor
  │◀─── initial ack ──────────────────────────│
  │                                           │
  │──── RecordBatch (sub-batch for p=2) ─────▶│  push into ShuffleBuffer
  │◀─── FlightAck (bytes_accepted) ───────────│
  │                                           │
  │──── RecordBatch ──────────────────────────▶│
  │◀─── FlightAck ─────────────────────────── │
  │     (if buffer full: ack withheld          │
  │      until drain below low-water mark)     │
  │                                           │
  │──── EOS ──────────────────────────────────▶│  sender_done()
  │                                           │
```

`FlightAck` is a one-byte metadata message sent upstream on the receiver-to-sender half of the `DoExchange` bidirectional stream:

```
FlightAck {
  bytes_accepted: uint64   // cumulative bytes the receiver has accepted so far
}
```

The sender's write loop blocks (awaits) if `(bytes_in_flight > HIGH_WATER_BYTES)`. `bytes_in_flight = bytes_sent - ack.bytes_accepted`.

### 3.4 Batch Splitting

```rust
// Pseudocode inside HashPartitioner::split_batch
fn split_batch(batch, assignments) -> Vec<RecordBatch> {
    // Build per-partition row index lists in a single pass
    let mut indices: Vec<Vec<u32>> = vec![vec![]; num_partitions];
    for (row, &p) in assignments.iter().enumerate() {
        indices[p as usize].push(row as u32);
    }

    // Take slices using Arrow's take kernel (zero-copy for fixed-width cols)
    indices.iter().map(|rows| {
        if rows.is_empty() {
            RecordBatch::new_empty(batch.schema())
        } else {
            arrow::compute::take(batch, rows).unwrap()
        }
    }).collect()
}
```

The `take` kernel produces new Arrow buffers backed by the original batch's memory via reference counting, avoiding data copies for fixed-width column types. Variable-width columns (strings, binaries) require a copy due to offset rewriting.

---

## 4. Broadcast Shuffle Protocol

In a broadcast exchange every row is sent to every downstream partition. The sender does **not** split the batch; it sends the full batch on all `num_partitions` Flight streams simultaneously.

```
Sender S0:
  for each output batch B:
    for each downstream partition p in [0..N-1]:
      DoExchange[p].send(B)  // same batch, N streams in parallel

  // All N streams are opened at stage start and held open until finish()
```

**Connection reuse:** because all downstream partitions are opened at the start of the stage and kept alive, no reconnect overhead occurs per-batch. The coordinator limits broadcast fan-out to a configurable maximum (default 64 workers) to avoid O(N²) connection counts.

**Memory:** the sender holds one `Arc<RecordBatch>` per in-flight batch; each partition stream holds a reference to the same allocation (zero copy on the sender side). The per-partition backpressure mechanism still applies independently.

---

## 5. Gather Shuffle Protocol

A gather exchange routes all rows to a single downstream partition (partition 0). It is the degenerate case of hash shuffle with `num_partitions = 1`. The receiver is the single worker that will execute the final aggregation or sort.

```
Sender Sk:
  for each output batch B:
    DoExchange[partition=0].send(B)

Receiver (W0, partition 0):
  merges streams from all S0..SM senders into one ordered sequence
  (order within the merged stream is arbitrary and expected by the consumer)
```

No `HashPartitioner` is needed. The `ShuffleWriter` for a GATHER exchange skips batch splitting and directly appends each batch to the single send buffer.

---

## 6. Backpressure Mechanism

Backpressure prevents fast senders from overwhelming slow receivers.

### 6.1 Receiver-Side Buffer High/Low Water Marks

| Parameter | Default | Description |
|---|---|---|
| `shuffle.buffer.max_bytes` | 256 MB per partition | Hard cap; triggers spill above this |
| `shuffle.backpressure.high_water_ratio` | 0.80 | Ack withheld when buffer > 80% full |
| `shuffle.backpressure.low_water_ratio` | 0.40 | Ack resumes when buffer drains to 40% |

When `queued_bytes >= high_water_bytes`:
1. `ShuffleBuffer.push()` suspends (awaits `drain_ready`).
2. The suspended coroutine holds the Flight send loop for that partition paused.
3. gRPC flow control naturally prevents the sender from writing more bytes into the network socket.

When `queued_bytes <= low_water_bytes`:
1. `drain_ready.notify_waiters()` is called by `next_batch()` after each batch is dequeued.
2. Suspended `push()` calls are unblocked.

### 6.2 Sender-Side Credit Tracking

The sender independently tracks per-partition in-flight bytes using `FlightAck` messages:

```rust
struct PartitionSendState {
    bytes_sent: u64,
    bytes_acked: u64,
}

const HIGH_WATER_BYTES: u64 = 64 * 1024 * 1024; // 64 MB per partition stream

async fn send_with_backpressure(state: &mut PartitionSendState, batch: RecordBatch, stream: &mut DoExchangeStream) {
    // Wait until in-flight bytes are below the high-water mark
    while state.bytes_sent - state.bytes_acked > HIGH_WATER_BYTES {
        let ack = stream.recv_ack().await?;
        state.bytes_acked = ack.bytes_accepted;
    }

    let byte_count = batch.get_array_memory_size() as u64;
    stream.send(batch).await?;
    state.bytes_sent += byte_count;
}
```

### 6.3 OSFlightServer Backpressure Integration

The existing `OSFlightServer` (see `OSFlightServer.java:61`) already configures a `ServerBackpressureThresholdInterceptor` with `DEFAULT_BACKPRESSURE_THRESHOLD = 10 MB`. For shuffle the threshold is tuned via:

```yaml
# opensearch.yml
arrow.flight.shuffle.backpressure_threshold_bytes: 67108864  # 64 MB
```

This setting is passed to `OSFlightServer.Builder.backpressureThreshold()` when the shuffle Flight server instance is constructed.

---

## 7. Memory Management

### 7.1 Buffer Lifecycle

```
register_shuffle()
  │
  └─ ShuffleBuffer allocated, max_memory_bytes reserved from worker allocator

push() calls from ShuffleService
  │
  └─ RecordBatch reference counted into queue; queued_bytes incremented

next_batch() calls from ShuffleReader
  │
  └─ RecordBatch popped from queue; reference count decremented when consumer drops it
     queued_bytes decremented; drain_ready notified

close_shuffle()
  │
  └─ remaining queue drained and dropped; allocator reservation released
```

### 7.2 Spill to Disk

When `queued_bytes >= max_memory_bytes` and the downstream consumer has stalled (e.g., waiting on another join input), the buffer spills the oldest batches to a local temp file using Arrow IPC format:

```rust
async fn maybe_spill(&mut self) {
    if self.queued_bytes < self.max_memory_bytes {
        return;
    }
    let spill_file = self.open_spill_file().await?;
    let mut writer = arrow::ipc::writer::FileWriter::try_new(spill_file, &self.schema)?;

    // Spill oldest half of the queue
    let spill_count = self.queue.len() / 2;
    for _ in 0..spill_count {
        let batch = self.queue.pop_front().unwrap();
        self.queued_bytes -= batch.get_array_memory_size();
        writer.write(&batch)?;
        self.spilled_bytes += batch.get_array_memory_size();
    }
    writer.finish()?;
    self.spill_segments.push(spill_file_path);
}
```

On `next_batch()`, spilled segments are read back before the in-memory queue is drained (FIFO order preserved):

```rust
async fn next_batch(&self) -> Option<RecordBatch> {
    if let Some(batch) = self.read_next_spill_batch().await {
        return Some(batch);
    }
    // fall through to in-memory queue
    ...
}
```

### 7.3 Memory Limits

| Scope | Limit | Enforcement |
|---|---|---|
| Per-partition buffer | `shuffle.buffer.max_bytes` (256 MB default) | Spill trigger |
| Per-query shuffle total | `shuffle.query.max_bytes` (2 GB default) | Reject new registrations beyond limit |
| Worker-wide shuffle pool | `shuffle.worker.max_bytes` (50% of heap) | Back-pressure all queries proportionally |

---

## 8. Network Optimization

### 8.1 Batch Size Tuning

The target size for a single Flight message is 64 KB–1 MB. The `ShuffleWriter` accumulates sub-batches for the same partition until the buffer reaches the flush threshold:

```rust
const FLUSH_THRESHOLD_BYTES: usize = 512 * 1024; // 512 KB default

async fn flush_if_needed(&mut self, partition: usize) {
    if self.send_buffers[partition].byte_size() >= FLUSH_THRESHOLD_BYTES {
        self.flush_partition(partition).await?;
    }
}
```

On `finish()`, all remaining sub-batches regardless of size are flushed.

The threshold is configurable:

```yaml
arrow.flight.shuffle.flush_threshold_bytes: 524288  # 512 KB
```

Very small batches are concatenated using `arrow::compute::concat_batches` before sending to avoid gRPC framing overhead.

### 8.2 Compression

Arrow IPC serialization within a Flight message supports optional compression. LZ4 frame compression is used for shuffle data:

```rust
use arrow::ipc::writer::IpcWriteOptions;
use arrow::ipc::CompressionType;

let options = IpcWriteOptions::default()
    .try_with_compression(Some(CompressionType::LZ4_FRAME))
    .expect("LZ4 compression not available");
```

LZ4 is chosen over Zstd for shuffle because:
- Compression is CPU-bound; LZ4 compresses at ~500 MB/s vs ~200 MB/s for Zstd level 3.
- Network bandwidth between OpenSearch nodes on modern clusters is typically 10–25 Gbps, making decompression throughput more important than compression ratio.
- For already-compressed columns (e.g., Parquet ZSTD pages decoded into Arrow buffers) LZ4 still avoids negative compression by detecting incompressible data faster.

Compression is disabled automatically when `shuffle.compression.enabled: false` or when the measured compression ratio falls below 1.05 (< 5% savings).

### 8.3 Connection Pooling and Reuse

The existing `OSFlightClient` (see `OSFlightClient.java`) builds one gRPC `NettyChannelBuilder` per remote address. For shuffle, connections are pooled at the worker level:

```
ConnectionPool (one per worker process)
  ├─ channel_cache: HashMap<InetSocketAddress, Arc<FlightChannel>>
  └─ per-channel: max 32 concurrent DoExchange streams (HTTP/2 streams)
```

Connections are shared across all queries and stages running on the same worker. A new Flight channel is created only when:
1. No channel exists to the target address.
2. The existing channel has been idle for more than `shuffle.connection.idle_timeout_ms` (default 30 000 ms).
3. The existing channel has failed (GOAWAY or RST_STREAM received).

The channel pool is guarded by a `DashMap<InetSocketAddress, Arc<FlightChannel>>` (concurrent hash map from the `dashmap` crate) on the Rust side, and by synchronized `HashMap` on the Java coordinator side.

---

## 9. Failure Handling

### 9.1 Sender Crashes

When a sender worker crashes mid-stream, the in-progress `DoExchange` stream is closed with a transport error. The receiver detects this when `accept_exchange()` returns an error:

1. `ShuffleBuffer.push()` returns `ShuffleError::SenderFailed { sender_id }`.
2. `ShuffleBuffer` records the failed sender.
3. If retry is enabled, the coordinator reassigns the sender task to another worker (up to `shuffle.sender.max_retries`, default 2).
4. On retry, the new sender opens a fresh `DoExchange` stream with the same `ShuffleDescriptor`. The receiver accepts it as a replacement for the failed sender.
5. If retries are exhausted, `ShuffleBuffer.sender_done_with_error()` is called, causing `next_batch()` to return `Err(ShuffleError::SenderFailed)` to the downstream stage, which propagates the failure to the coordinator.

### 9.2 Receiver Crashes

When a receiver worker crashes, the sender's open `DoExchange` streams to that receiver fail with a transport error. The sender's `write_batch()` returns `ShuffleError::ReceiverFailed { partition_id }`.

The coordinator is notified via the worker health monitor (Component 6). It then:
1. Marks all exchanges for affected partitions as `FAILED`.
2. Aborts the downstream stage tasks assigned to the failed worker.
3. Re-schedules the failed downstream tasks on a replacement worker.
4. Issues `register_shuffle` RPCs to the replacement worker.
5. Restarts all sending tasks for stage N (full restart, not partial) since the original sender data is gone.

### 9.3 Network Partition

A network partition appears as repeated `DEADLINE_EXCEEDED` or `UNAVAILABLE` errors on the Flight client. The sender retries with exponential backoff:

```
retry_delay = min(base_delay_ms * 2^attempt, max_delay_ms)
base_delay_ms = 100
max_delay_ms  = 10_000
max_attempts  = 5
```

If the partition heals within `max_attempts`, the stream resumes from the last acknowledged byte position (the receiver stores the cumulative `bytes_accepted` so the sender can resume after a reconnect if the receiver is still alive).

If all retries fail, the error is escalated to the coordinator as `ShuffleError::NetworkPartition`.

### 9.4 Slow Receiver (Straggler)

A receiver that processes batches very slowly will fill its buffer and apply backpressure to senders. If a sender is stalled for longer than `shuffle.stall_timeout_ms` (default 60 000 ms), it reports a stall event to the coordinator. The coordinator may:
- Spawn a speculative copy of the downstream task on a different worker (see Component 5 speculative execution).
- Leave the stalled query in place if memory is the bottleneck (stalling is preferable to OOM).

---

## 10. Shuffle Cleanup

After a query completes (success, failure, or cancellation), all shuffle resources must be released to prevent memory leaks across queries.

### 10.1 Normal Completion

```
Stage N+1 finishes all tasks
  │
  ├─ Each ShuffleReader exits its next_batch() loop
  ├─ ShuffleReader drops its reference to ShuffleBuffer
  │
Coordinator.ShuffleManager.cleanupQuery(queryId)
  │
  ├─ for each (stageId, partitionId) in exchange state:
  │     send close_shuffle(descriptor) RPC to receiver worker
  │
  └─ receiver worker:
       ShuffleBuffer.drain_and_drop()
         deletes spill files if any
         releases Arrow allocator reservation
       removes entry from shuffle registry
```

### 10.2 Failure / Cancellation Cleanup

On query cancellation or failure, the coordinator sends `close_shuffle` RPCs to all registered receiver workers immediately. Sender workers abort their in-progress `ShuffleWriter.finish()` calls. Any open `DoExchange` Flight streams are cancelled via `tonic::Status::cancelled`.

### 10.3 Leak Detection

The shuffle registry on each worker runs a background sweeper every `shuffle.cleanup.sweep_interval_ms` (default 60 000 ms). Any `ShuffleBuffer` registered for a `query_id` that has not been referenced for longer than `shuffle.cleanup.ttl_ms` (default 300 000 ms) is forcibly released and an error is logged. This protects against coordinator crashes that omit cleanup RPCs.

---

## 11. Metrics

Shuffle metrics are reported as a separate subsystem within the existing `FlightMetrics` framework (see `FlightMetrics.java`) via a dedicated `ShuffleMetrics` struct.

### 11.1 Rust Worker Metrics

```rust
pub struct ShuffleMetrics {
    // Volume
    pub bytes_sent_total: Counter,       // total bytes sent across all partitions
    pub bytes_received_total: Counter,   // total bytes received across all partitions
    pub batches_sent_total: Counter,
    pub batches_received_total: Counter,

    // Per-exchange type breakdown
    pub hash_bytes_sent: Counter,
    pub broadcast_bytes_sent: Counter,
    pub gather_bytes_sent: Counter,

    // Latency (nanoseconds, tracked as histograms)
    pub send_batch_duration_ns: Histogram,   // time for write_batch() to complete
    pub recv_batch_duration_ns: Histogram,   // time from push() to next_batch()
    pub end_to_end_latency_ns: Histogram,    // time from first send to last recv per exchange

    // Backpressure
    pub sender_stall_count: Counter,       // number of times sender was stalled
    pub sender_stall_duration_ns: Counter, // total nanoseconds spent stalled

    // Memory / spill
    pub buffer_bytes_current: Gauge,       // current in-memory buffer bytes across all partitions
    pub spill_bytes_total: Counter,        // total bytes written to disk spill files
    pub spill_file_count: Counter,

    // Failures
    pub sender_failure_count: Counter,
    pub receiver_failure_count: Counter,
    pub retry_count: Counter,
}
```

### 11.2 Java Coordinator Metrics (ShuffleManager)

```java
public class ShuffleManagerMetrics {
    // Counters
    long activeExchanges;           // currently registered exchanges
    long completedExchanges;        // total completed since process start
    long failedExchanges;           // total failed
    long totalBytesSentAllQueries;
    long totalBytesReceivedAllQueries;

    // Latency histograms (nanoseconds)
    HistogramSnapshot exchangeRegistrationLatencyNs;  // time for registerExchange() RPC fan-out
    HistogramSnapshot cleanupLatencyNs;               // time for cleanupQuery() to complete

    // Resource
    long activeSenderConnections;   // open DoExchange streams across all workers
    long activeReceiverBuffers;     // ShuffleBuffer instances alive on remote workers
}
```

### 11.3 Key Alerting Thresholds

| Metric | Warning | Critical |
|---|---|---|
| `sender_stall_duration_ns` / query | > 10 s | > 60 s |
| `spill_bytes_total` / query | > 1 GB | > 10 GB |
| `retry_count` / exchange | > 1 | > 3 |
| `buffer_bytes_current` | > 70% of `shuffle.worker.max_bytes` | > 90% |
| P99 `end_to_end_latency_ns` | > 5 s | > 30 s |

Metrics are exposed via the existing `FlightStatsRestHandler` endpoint (`GET /_plugins/arrow-flight/stats`) and additionally via OpenTelemetry when the telemetry plugin is loaded.

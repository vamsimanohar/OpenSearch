# Arrow Memory Transfer Options

## Context

Arrow Flight binds one `VectorSchemaRoot` via `start()` for the entire stream. `putNext()` reads from that root. To send multiple batches, data must be swapped into the same root object. Additionally, producers (e.g., DataFusion) and Flight have independent allocator trees — `transfer()` only works within the same `RootAllocator` tree.

On the receive side, `FlightStream` reuses one `VectorSchemaRoot` across `next()` calls. Consumers must handle data before advancing or copy it out.

### Validated Assumptions

| Method | Zero-copy | Cross-root | Unified budget |
|--------|-----------|------------|----------------|
| `transfer()` same root | ✅ | ❌ blocked | ✅ |
| `transfer()` different roots | ❌ | ❌ | ❌ |
| C Data Interface | ✅ | ✅ | ❌ |
| Shared root + `transfer()` | ✅ | ✅ (same root) | ✅ |

---

## Send Side (server → network)

| | Approach 1: Shared RootAllocator + transfer() | Approach 2: C Data Interface importInto() |
|--|---|---|
| **Description** | Create one `RootAllocator` at node startup, pass children to both Flight and DataFusion. `transfer()` moves buffer pointers from producer's root into the shared root bound to `start()`. Same allocator tree, so transfer is allowed. | Export producer's root via C Data Interface structs (ArrowArray/ArrowSchema), import directly into the shared root under Flight's allocator. No shared allocator needed — works across independent allocator trees. |
| **Zero-copy** | ✅ Pointer swap between vectors | ✅ Pointer handoff via C structs |
| **Unified budget** | ✅ One root governs all Arrow memory | ❌ Flight and DataFusion pools independent |
| **Plugin wiring** | Needs shared allocator created at node startup and injected into both plugins | None — localized to `FlightOutboundHandler` |
| **Per-batch overhead** | Minimal — one `transfer()` per vector | ~580 bytes C struct metadata + export/import calls |
| **Multi-batch** | ✅ Same shared root reused, buffers swapped each batch | ✅ Same shared root reused, imported into each batch |
| **Pipelining** | ✅ Each batch has independent buffers until transferred | ✅ Each batch has independent buffers until imported |
| **Complexity** | Architectural change — touches plugin lifecycle, Node.java | Localized — only `FlightOutboundHandler` changes |
| **Works today** | ❌ Requires new infrastructure | ✅ Can implement now |

### Send Side Code

**Approach 1: Shared RootAllocator + transfer()**
```java
// At node startup: create shared root, pass children to plugins
// In FlightOutboundHandler:
sharedRoot.clear();
for (int i = 0; i < producerRoot.getFieldVectors().size(); i++) {
    producerRoot.getFieldVectors().get(i)
        .makeTransferPair(sharedRoot.getFieldVectors().get(i))
        .transfer();
}
sharedRoot.setRowCount(producerRoot.getRowCount());
producerRoot.close();  // empty shell
putNext();
```

**Approach 2: C Data Interface importInto()**
```java
// In FlightOutboundHandler:
ArrowArray arrowArray = ArrowArray.allocateNew(producerAllocator);
ArrowSchema arrowSchema = ArrowSchema.allocateNew(producerAllocator);
Data.exportVectorSchemaRoot(producerAllocator, producerRoot, null, arrowArray, arrowSchema);

sharedRoot.clear();
Data.importIntoVectorSchemaRoot(flightAllocator, arrowArray, sharedRoot, null);
putNext();

arrowArray.close();
arrowSchema.close();
producerRoot.close();
```

---

## Receive Side (network → consumer)

| | Option A: Process Inline | Option B: Deep Copy | Option C: C Data Interface | Option D: Shared Root + transfer() |
|--|---|---|---|---|
| **Description** | Read `FlightStream`'s shared root, process immediately, call `next()`. The root gets overwritten on the next call — consumer must finish before advancing. Simplest path, no memory overhead. | Copy all vectors from Flight's root into a new root under the consumer's allocator via `splitAndTransfer()`. Consumer fully owns the copy and can hold it indefinitely. Full `memcpy` cost. | Export Flight's root via C Data Interface structs, import into a new root under consumer's allocator. Zero-copy pointer handoff across allocator trees. After export, Flight's root vectors are empty. | If Flight and consumer share the same `RootAllocator`, `transfer()` moves buffer pointers directly. Zero-copy + unified budget. Requires shared allocator infrastructure. |
| **Zero-copy** | ✅ No copy at all | ❌ Full memcpy | ✅ Pointer handoff | ✅ Pointer swap |
| **Hold multiple batches** | ❌ Must process before `next()` | ✅ Each copy is independent | ✅ Each import is independent | ✅ Each transfer is independent |
| **Cross-allocator** | N/A — uses Flight's root directly | ✅ Works across any trees | ✅ Works across any trees | ❌ Same root only |
| **Unified budget** | N/A | ❌ Two independent pools | ❌ Two independent pools | ✅ One budget governs all |
| **Memory cost** | None — no additional allocation | 2x momentarily (source + copy) | ~580 bytes C struct overhead per batch | Minimal — pointer move only |
| **FlightStream interaction** | Safe — root stays intact | Safe — root stays intact, copy is separate | ⚠️ Caution — export empties Flight's root vectors, may break subsequent `FlightStream` operations | ⚠️ Caution — transfer empties Flight's root vectors |
| **Use case fit** | Streaming aggregation, single-pass processing | General purpose, safest option | Performance-critical paths needing multiple batches | Performance-critical with unified memory management |
| **Works today** | ✅ | ✅ | ✅ | ❌ Requires shared allocator infrastructure |
| **Complexity** | None | Low | Medium — manage ArrowArray/ArrowSchema lifecycle | Architectural — touches plugin lifecycle |

### Receive Side Code

**Option A: Process Inline**
```java
while (flightStream.next()) {
    VectorSchemaRoot root = flightStream.getRoot();
    processBatch(root);  // must finish before next next() call
}
```

**Option B: Deep Copy**
```java
VectorSchemaRoot copy = VectorSchemaRoot.create(root.getSchema(), myAllocator);
for (int i = 0; i < root.getFieldVectors().size(); i++) {
    root.getFieldVectors().get(i)
        .makeTransferPair(copy.getFieldVectors().get(i))
        .splitAndTransfer(0, root.getRowCount());
}
copy.setRowCount(root.getRowCount());
```

**Option C: C Data Interface**
```java
ArrowArray arrowArray = ArrowArray.allocateNew(flightAllocator);
ArrowSchema arrowSchema = ArrowSchema.allocateNew(flightAllocator);
Data.exportVectorSchemaRoot(flightAllocator, root, null, arrowArray, arrowSchema);

VectorSchemaRoot myRoot = Data.importVectorSchemaRoot(myAllocator, arrowArray, arrowSchema, null);
arrowArray.close();
arrowSchema.close();
```

**Option D: Shared Root + transfer()**
```java
VectorSchemaRoot myRoot = VectorSchemaRoot.create(root.getSchema(), myAllocator);
for (int i = 0; i < root.getFieldVectors().size(); i++) {
    root.getFieldVectors().get(i)
        .makeTransferPair(myRoot.getFieldVectors().get(i))
        .transfer();
}
myRoot.setRowCount(root.getRowCount());
```

---

## Recommendation

**Send side**: Approach 2 (C Data Interface) for now — works today, no architectural changes. Migrate to Approach 1 when shared allocator infrastructure is built.

**Receive side**: Depends on use case:
- Single-pass processing → Option A (inline)
- Need to hold batches for merge (distributed query) → Option C (C Data Interface) for zero-copy, or Option B (deep copy) for simplicity
- Long-term → Option D with shared allocator

**Long-term**: Build shared `RootAllocator` at node startup, inject into Flight and DataFusion plugins. Enables `transfer()` everywhere (zero-copy + unified budget) and eliminates the need for C Data Interface workarounds between Java components.

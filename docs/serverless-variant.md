# Serverless Variant — Distributed Query Engine

This document covers the serverless (elastic, scale-to-zero) variant of the distributed query engine.
The server (fixed-nodes) variant is in `server-fixed-nodes-plan.md`.

## What Changes vs Fixed Nodes

```
                        SERVER          SERVERLESS
                        ------          ----------
Query Frontend          SAME            SAME
Substrait Producer      SAME            SAME
Stage Splitter          SAME            SAME
Partition Assigner      SAME            SAME
Stage Sequencer         SAME            SAME

Worker Binary           SAME            SAME (containerized)
DataFusion Engine       SAME            SAME
Custom UDFs             SAME            SAME
Parquet/S3 Reader       SAME            SAME

Worker Registry         Static config   Dynamic (pool manager)
Scaling                 Manual          Auto-scaling
Shuffle                 Arrow Flight    S3 or shuffle service
                        (direct P2P)    (persistent)
Worker Lifecycle        Always on       Spin up/down per query
Local Cache             Yes (SSD)       No (ephemeral)
Cost Model              Fixed           Per-query
```

~80% of the code is shared. The serverless variant is essentially the server variant
with a pool manager replacing the static worker list, and persistent shuffle replacing
direct Arrow Flight.

---

## Worker Pool Manager

Replaces the fixed worker registry from the server variant.

```
Pool Manager (Java)

  min_workers: 0
  max_workers: 100
  current_workers: 3
  pending_tasks: 47

  SCALE UP when:
    pending_tasks > current_workers * slots_per_worker
    -> spin up containers (K8s, ECS, Cloud Run)
    -> wait for health check
    -> add to worker registry

  SCALE DOWN when:
    worker idle for > 5 minutes
    AND current_workers > min_workers
    -> drain worker (finish current tasks)
    -> remove from registry
    -> terminate container

  SCALE TO ZERO when:
    no queries for > 10 minutes
    -> shut down all workers
    -> coordinator stays alive (or goes to sleep
       behind an API gateway that wakes it)
```

---

## Persistent Shuffle

Workers are ephemeral — can't rely on them being alive for peer-to-peer shuffle.

### Option A: S3-Based Shuffle
```
Stage 1 writes: s3://shuffle/{query_id}/{stage_id}/{partition}.arrow
Stage 2 reads from S3
```
- Simple but adds 1-3 seconds latency per stage boundary
- Good for large shuffles (data is already on S3)
- Cheap storage

### Option B: Redis / Shared Memory Shuffle Service
```
Stage 1 writes Arrow batches to Redis cluster
Stage 2 reads from Redis
```
- Lower latency (~10-50ms)
- Need to manage Redis cluster
- Memory cost for large shuffles

### Recommendation
Start with S3-based shuffle. Optimize to Redis later for latency-sensitive queries.

```
Stage 1 workers                Shuffle Storage              Stage 2 workers
+----------+     write         +----------------+    read   +----------+
| Worker 1 |------------------>|                |<----------| Worker A |
| Worker 2 |------------------>|  S3 or Redis   |<----------| Worker B |
| Worker 3 |------------------>|                |<----------| Worker C |
+----------+                   +----------------+           +----------+

Stage 1 and Stage 2 workers can be completely different sets of containers.
Stage 1 workers can be terminated before Stage 2 even starts.
```

---

## Cold Start Mitigation

- Keep 1-2 "warm" workers pre-provisioned
- Worker container image: keep small (<500MB)
  - Pre-install DataFusion + your UDFs
  - Don't include data — always read from S3
- Pre-warm S3 connections on worker startup
- Use container image caching on nodes (e.g., K8s image pull policy)

---

## Per-Query Billing / Metering

Track per query:
- CPU-seconds consumed
- Bytes scanned from S3
- Bytes shuffled
- Wall-clock time
- Peak memory usage

Coordinator logs:
```json
{
  "query_id": "q-12345",
  "user": "analytics-team",
  "stages": [
    {
      "stage_id": 1,
      "tasks": 5,
      "total_cpu_ms": 12500,
      "bytes_scanned": 1073741824,
      "bytes_shuffled": 52428800
    }
  ],
  "total_cost_units": 42
}
```

---

## Serverless-Specific Failure Modes

1. **Cold start timeout**: Query arrives, no workers available, takes 10-30s to spin up
   - Mitigation: warm pool, predictive scaling based on query patterns

2. **Worker terminated mid-task** (spot instance, preemption)
   - Mitigation: shuffle output is persisted, so just re-run the task on a new worker

3. **Shuffle storage full** (Redis OOM or S3 throttling)
   - Mitigation: spill from Redis to S3, spread across S3 prefixes

4. **Cascading scale-up** (100 queries arrive simultaneously)
   - Mitigation: query queue with admission control, max concurrent queries limit

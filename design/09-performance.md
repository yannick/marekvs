# 09 — Performance

"Performance is key." This document sets targets, names the hot paths, and
lists the levers — with the measurement plan that keeps us honest.

## Targets (single node, 16-byte keys / 100-byte values, local NVMe)

| Metric | Target | Rationale |
|---|---|---|
| GET (hot, cached) p50 / p99 | ≤ 100 µs / ≤ 1 ms | memtable/block-cache hit + one shard hop |
| SET p50 / p99 | ≤ 150 µs / ≤ 2 ms | ondaDB put is ~µs; budget is queueing + RESP |
| Throughput, pipelined GET/SET mix | ≥ 500 k ops/s/node | ondaDB raw does 2.7–3.6 M ops/s single-process (its `docs/performance.md`); server stack overhead budget ≈ 5–7× |
| Replication propagation (push) | ≤ 5 ms intra-cluster p99 | 2 ms linger + RTT |
| Remote first-read (fetch + subscribe) | ≤ 2 ms p99 | one ctl RTT + local commit |
| Bootstrap streaming | ≥ 64 MiB/s/stream sustained | matches cap in defaults table |

Verified per release by the benchmark plan below; regressions >10 % fail CI.

## Hot paths & how they stay fast

### RESP → storage → RESP

- Parser yields argument **slices** into the connection read buffer; no arg
  copies until the storage job needs owned bytes (single memcpy into the
  ondaDB txn arena — which ondaDB requires anyway).
- Shard routing by `pid % S` → **no locks on the data path**; per-key
  operations are serialized by the shard thread, giving free atomic RMW.
- Shard queues are bounded MPSC (crossbeam); a full queue applies backpressure
  to the connection task (stop reading — TCP does the rest).
- Replies: shard job returns owned `Bytes`; reply builder writes into a
  per-connection 64 KiB output buffer, vectored flush once per burst.
  Pipelines amortize syscalls on both directions.
- v1.1: zero-copy value pass-through — ondaDB pinned-block borrows end-to-end
  into `writev`, skipping the owned-Bytes copy for large GETs (needs careful
  lifetime plumbing across the shard/tokio boundary; explicitly deferred).

### Replication

- Commit hook does **one ring append** (per-shard producer segment, no CAS
  contention); sender tasks batch (256 ops / 256 KiB / 2 ms) and write
  postcard frames with the envelope+payload bytes **verbatim** — zero
  re-serialization.
- Apply path groups a `ReplBatch` into one ondaDB `Txn` per shard —
  group-commit WAL amortizes fsync.
- Merkle bucket scans run on shard threads at AE pace (≤ 512 partitions /
  5 s), bounded by dirty-marking to buckets that actually changed.

### ondaDB tuning (per CF config)

| Knob | Value | Why |
|---|---|---|
| `sync_mode` | `Interval` 128 ms | fsync-per-commit (`Full`) costs ~10× on writes; AP + N replicas make a ≤128 ms single-node window acceptable (data survives on peers). `None` is too loose for a database server. |
| `compression` | lz4 | cheap CPU, ~2× disk saving; zstd per-level is future tuning |
| `klog_value_threshold` | 512 B (default) | WiscKey keeps big values out of compaction |
| `enable_bloom_filter` | on, fpr 0.01 | point-read heavy workload |
| `block_cache_size` | ~50 % of container memory | main read accelerator; env-tunable |
| `write_buffer_size` | 128 MiB | fewer, larger L0 files under write bursts |
| `unified_memtable` | off | only two CFs (`data`, `meta`) — not needed |
| feature `unsafe-fastpath` | **benchmark, then decide** | mmap reads + arena memtable ≈ C-class perf; costs `forbid(unsafe)` purity. Ship default-safe, offer a `-fast` image variant if the delta is ≥ 20 %. |

### Memory

- No user data in process heaps beyond transient buffers — ondaDB block cache
  is the cache. The interest table (≤ ~120 MB) and connection buffers are the
  other budgeted consumers.
- Global allocator: mimalloc ([08-build-deploy.md](08-build-deploy.md#static-binary)) —
  musl's malloc measurably degrades multithreaded tail latency.
- `MEMORY USAGE` approximates from envelope + payload length.

### Cluster-level

- Interest replication moves reads next to readers after one fetch —
  steady-state remote-read ratio should approach zero for skewed workloads
  (measure: `fetch_rate / local_hit_rate` gauge).
- Fan-out writes cost `(N-1) + subscribers` frames per write, batched; the
  2 ms linger keeps frame counts low under load.
- Known v1 gaps (documented, future work): zone-aware HRW scoring
  (cross-zone traffic reduction), hot-key H1 offload
  ([00-overview.md](00-overview.md) risky assumption 5), read-only iterator
  offload of large SCANs to a dedicated thread.

## Measured findings (KeyDB comparison harness, 2026-07)

The `just bench` harness (bench/) surfaced three real characteristics:

1. **ondadb iterator construction is O(memtable)** — `Memtable::snapshot()`
   clones and sorts every live entry on every `new_iterator()` call. Point
   ops are unaffected (0.1 ms), but every prefix scan (SPOP, ZPOPMIN, SCARD,
   SMEMBERS, HGETALL, sweeper ticks) pays milliseconds once the memtable
   holds tens of thousands of records: measured 1.3 ms/SPOP at 2 k memtable
   entries → 5.1 ms at ~15 k, while a point SADD stays at 0.12 ms. Fix
   belongs in ondadb: lazy k-way merge over the (already sorted) skiplist
   shards instead of snapshot-collect-sort.
2. **Scan-shaped pops need early exits** — SPOP/SRANDMEMBER/ZPOPMIN now use
   limit-bounded scans (O(count) visible hits) instead of materializing the
   collection; ZPOPMAX keeps a bounded tail window.
3. **List blobs are quadratic under fixed-key append storms** —
   redis-benchmark pushes every request onto one `mylist`; N pushes cost
   O(N²) blob-rewrite bytes and stall the shard queue (head-of-line). The
   harness runs list tests at n/10; the design answer remains a per-element
   list representation (sequence CRDT, future work).

### Round 3 (2026-07-03): profile-driven point-op + list overhaul

Profiling (`sample` under redis-benchmark SET load) showed the eager
`check_type` gate (3–4 point reads per string op) as the top marekvs cost.
Fixes, with geo-mean vs KeyDB moving 0.29×→0.59× (P=1), 0.19×→0.38× (P=16):

1. **Lazy type gate + string fast paths** — a live string record shadows
   collections, so GET/INCR check the gate only on a miss; plain SET does
   zero reads before its write.
2. **Per-pid placement tables** — `View` precomputes owners/H1 per membership
   change; reads and pump fan-out do lookups instead of HRW re-scoring.
3. **Pipeline batcher** — consecutive parallel-safe commands with disjoint
   argument sets fan out across shards concurrently (per-key ordering kept by
   batch-cutting on arg overlap). GET P=16 reached 114k rps locally.
4. **Per-element lists** — position-keyed element records replaced the LWW
   blob: LPUSH/RPUSH/LPOP at 1.00× KeyDB parity in the harness (were
   0.01–0.17×), 40–80k rps locally.
5. **Replication push robustness** (found by the cluster test): the pump
   MUST NOT advance a peer cursor past entries whose partition has an empty
   owner set — that means the gossip view hasn't converged (peers still
   `Joining` at boot), and skipping silently demotes first-write convergence
   to anti-entropy latency. This was a latent v0.1 bug unmasked by faster
   startup. Ring buffering is now skipped only for statically-configured
   standalone nodes (no seeds, N=1), never gated on runtime connectivity.

Remaining known gaps: SPOP/ZPOPMIN at ~0.15× (per-op iterator construction +
tombstone walk; next lever is ondadb-side), MSET at 0.10× (one command = 10
distributed writes by design).

## Benchmark plan

1. **Micro**: `redis-benchmark` and `memtier_benchmark` against a single node
   (GET/SET/HSET/SADD/ZADD/XADD mixes, pipeline 1/16/64, value sizes
   100 B/1 KiB/16 KiB). Baseline vs Redis on the same hardware
   for context (not a win condition — they are RAM stores).
2. **Storage floor**: ondaDB's own `onda_bench` numbers on the target hardware
   set the ceiling; the gap between server ops/s and engine ops/s is the
   number to optimize (target ≤ 7×).
3. **Cluster**: 3/5/9-node k8s runs — replication propagation latency
   histogram (write at A, poll at B), fetch latency, staleness gauge under
   AE-only repair (kill pushes artificially), bootstrap duration per GiB.
4. **Sustained-write soak**: 24 h at 70 % target throughput watching
   compaction debt, ring occupancy, p99 drift.
5. Rig: the existing multi-engine harness precedent in `../bench` (Go) informs
   methodology; marekvs adds a `criterion` micro-suite for parser/envelope/
   merge code and a `k6`-style cluster driver.

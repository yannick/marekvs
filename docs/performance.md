---
title: Performance
description: Latency and throughput targets, storage costs, and benchmark results against KeyDB.
status: mixed
---

MareKVS persists writes through the [ondaDB](https://github.com/yannick/ondadb)
LSM engine and can store datasets larger than RAM. SSD storage reduces the need
to provision expensive memory for the full dataset. Caches and write buffers
still need RAM; their size affects latency and throughput.

This page separates design targets from measured results and describes the
storage and replication work performed on each request.

```note
The comparison results are a snapshot from the KeyDB benchmark harness
(`just bench` → `bench/report.md`). Each configuration was run once in Docker
on macOS. See [Benchmark methodology](#benchmark-methodology) for limitations.
```

## Targets

Single node, 16-byte keys / 100-byte values, local NVMe. These are design
targets, verified per release by the [benchmark plan](#benchmark-methodology);
regressions over 10 % fail CI.

| Metric | Target | Rationale |
|---|---|---|
| GET (hot, cached) p50 / p99 | ≤ 100 µs / ≤ 1 ms | memtable/block-cache hit + one shard hop |
| SET p50 / p99 | ≤ 150 µs / ≤ 2 ms | ondaDB put is ~µs; budget is queueing + RESP |
| Throughput, pipelined GET/SET mix | ≥ 500 k ops/s/node | ondaDB raw does 2.7–3.6 M ops/s single-process; server-stack overhead budget ≈ 5–7× |
| Replication propagation (push) | ≤ 5 ms intra-cluster p99 | 2 ms linger + RTT |
| Remote first-read (fetch + subscribe) | ≤ 2 ms p99 | one ctl RTT + local commit |
| Bootstrap streaming | ≥ 64 MiB/s/stream sustained | matches the cap in the defaults table |

## Request processing

### RESP → storage → RESP

- The parser yields argument **slices** into the connection read buffer; no
  argument copies until the storage job needs owned bytes (a single memcpy into
  the ondaDB txn arena, which ondaDB requires anyway).
- Shard routing by `pid % S` means **no locks on the data path** — per-key
  operations are serialized by the shard thread, giving free atomic RMW.
- Shard queues are bounded MPSC (crossbeam); a full queue applies backpressure
  to the connection task (stop reading — TCP does the rest).
- Replies: the shard job returns owned `Bytes`; the reply builder writes into a
  per-connection 64 KiB output buffer and does one vectored flush per burst.
  Pipelines amortize syscalls in both directions.

```planned Zero-copy value pass-through (v1.1)
ondaDB pinned-block borrows carried end-to-end into `writev`, skipping the
owned-`Bytes` copy for large GETs. It needs careful lifetime plumbing across the
shard/tokio boundary and is explicitly deferred.
```

### Replication

- The commit hook does **one ring append** (a per-shard producer segment, no CAS
  contention); sender tasks batch (256 ops / 256 KiB / 2 ms) and write postcard
  frames with the envelope+payload bytes **verbatim** — zero re-serialization.
- The apply path groups a `ReplBatch` into one ondaDB `Txn` per shard, so
  group-commit WAL amortizes fsync.
- Merkle bucket scans run on shard threads at anti-entropy pace (≤ 512 partitions
  / 5 s), bounded by dirty-marking to buckets that actually changed.

### ondaDB tuning (per column-family config)

| Knob | Value | Why |
|---|---|---|
| `sync_mode` | `Interval` 128 ms | fsync-per-commit (`Full`) costs ~10× on writes; AP + N replicas make a ≤128 ms single-node window acceptable (data survives on peers). `None` is too loose for a database server. |
| `compression` | lz4 | cheap CPU, ~2× disk saving; per-level zstd is future tuning |
| `klog_value_threshold` | 512 B | WiscKey keeps big values out of compaction |
| `enable_bloom_filter` | on, fpr 0.01 | point-read-heavy workload |
| `block_cache_size` | ~50 % of container memory | the main read accelerator; env-tunable |
| `write_buffer_size` | 128 MiB | fewer, larger L0 files under write bursts |
| `unified_memtable` | off | only two CFs (`data`, `meta`) — not needed |

### Memory

- No user data lives in process heaps beyond transient buffers — the ondaDB block
  cache *is* the cache. The interest table (≤ ~120 MB) and connection buffers are
  the other budgeted consumers.
- Global allocator: mimalloc — musl's malloc measurably degrades multithreaded
  tail latency.
- `MEMORY USAGE` approximates from envelope + payload length.

### Cluster-level

- Interest replication moves reads next to readers after one fetch, so the
  steady-state remote-read ratio approaches zero for skewed workloads.
- Fan-out writes cost `(N-1) + subscribers` frames per write, batched; the 2 ms
  linger keeps frame counts low under load.
- Known v1 gaps (documented, future work): zone-aware HRW scoring, hot-key H1
  offload, read-only iterator offload of large SCANs to a dedicated thread.

## Measured findings vs KeyDB

The `just bench` harness compares single-node MareKVS against
[KeyDB](https://docs.keydb.dev/) with identical `redis-benchmark` workloads.
MareKVS persists writes through ondaDB and processes replication metadata.
KeyDB runs with persistence disabled and holds the dataset in memory.
These configurations have different durability and memory requirements, which
should be considered alongside the throughput results.

Geometric mean of throughput ratios (marekvs ÷ KeyDB), from `bench/report.md`:

| Config | marekvs ÷ keydb |
|---|---|
| 100 B values, pipeline 1 | 0.59× |
| 100 B values, pipeline 16 | 0.38× |
| 1024 B values, pipeline 1 | 0.55× |

```warning Benchmark scope
On these single-node workloads, MareKVS reached 0.55–0.59× KeyDB throughput
without pipelining and 0.38× with pipelining. These results do not measure
multi-node scaling or quantify infrastructure cost savings.
```

Results vary by command:

- **GET, SADD, HSET, and PING:** approximately 1.00× KeyDB throughput at P=1
  within the Docker-on-macOS test environment.
- **SET:** approximately 0.40× at P=1, including LSM writes and merge processing.
- **SPOP and ZPOPMIN:** approximately 0.17× and 0.15× at P=1. Iterator construction
  accounts for much of the overhead; see the optimization history below.
- **MSET:** approximately 0.10×. Each benchmark command performs ten distributed writes.

## Optimization changelog

The following entries record profiling results and the changes measured after
each optimization.

### Round 3 (2026-07-03): profile-driven point-op + list overhaul

Profiling (`sample` under `redis-benchmark` SET load) showed the eager
`check_type` gate (3–4 point reads per string op) as the top marekvs cost.
Geometric mean vs KeyDB moved **0.29× → 0.59× (P=1)** and **0.19× → 0.38× (P=16)**.

1. **Lazy type gate + string fast paths** — a live string record shadows
   collections, so GET/INCR check the gate only on a miss; plain SET does zero
   reads before its write.
2. **Per-pid placement tables** — `View` precomputes owners/H1 per membership
   change; reads and pump fan-out do lookups instead of HRW re-scoring.
3. **Pipeline batcher** — consecutive parallel-safe commands with disjoint
   argument sets fan out across shards concurrently (per-key ordering kept by
   batch-cutting on argument overlap). GET P=16 reached 114 k rps locally.
4. **Per-element lists** — position-keyed element records replaced the LWW blob:
   LPUSH/RPUSH/LPOP reached ~1.00× KeyDB parity in the harness (were 0.01–0.17×),
   40–80 k rps locally.
5. **Replication push robustness** (found by the cluster test) — the pump must
   not advance a peer cursor past entries whose partition has an empty owner set;
   that means the gossip view has not converged yet, and skipping silently
   demoted first-write convergence to anti-entropy latency. Ring buffering is now
   skipped only for statically-configured standalone nodes (no seeds, N=1), never
   gated on runtime connectivity.

Remaining known gaps: SPOP/ZPOPMIN at ~0.15× (per-op iterator construction +
tombstone walk; the next lever is ondaDB-side), MSET at ~0.10× (one command = 10
distributed writes by design).

### Earlier findings surfaced by the harness

1. **ondaDB iterator construction is O(memtable).** `Memtable::snapshot()` clones
   and sorts every live entry on every `new_iterator()` call. Point ops are
   unaffected (0.1 ms), but every prefix scan (SPOP, ZPOPMIN, SCARD, SMEMBERS,
   HGETALL, sweeper ticks) pays milliseconds once the memtable holds tens of
   thousands of records: measured 1.3 ms/SPOP at 2 k memtable entries → 5.1 ms at
   ~15 k, while a point SADD stays at 0.12 ms. The fix belongs in ondaDB: a lazy
   k-way merge over the already-sorted skiplist shards.
2. **Scan-shaped pops need early exits.** SPOP/SRANDMEMBER/ZPOPMIN now use
   limit-bounded scans (O(count) visible hits) instead of materializing the whole
   collection; ZPOPMAX keeps a bounded tail window.
3. **List blobs were quadratic under fixed-key append storms.** `redis-benchmark`
   pushes every request onto one `mylist`; N pushes cost O(N²) blob-rewrite bytes
   and stall the shard queue (head-of-line). Round 3's per-element lists resolved
   this; the harness historically ran list tests at n/10 to keep runs bounded.

## Benchmark methodology

The harness (`bench/`) drives `redis-benchmark` from the host against both
engines in Docker on the same machine and network mode, so the macOS VM overhead
applies equally to both.

- KeyDB runs `--server-threads 4 --appendonly no` — its headline multithreading
  on, persistence off (its fastest sensible config).
- marekvs runs single-node (`MAREKVS_REPLICAS_N=1`) at its default
  `SyncMode::Interval` (128 ms fsync window) — every write still hits the LSM.
- Matrix: 14 command types × {100 B P=1, 100 B P=16, 1 KiB P=1}, 50 clients,
  100 k random-key keyspace, `FLUSHALL` between engines.

```caution Benchmark environment
Docker port mapping and the macOS VM hold absolute throughput well below
bare-metal Linux; only the *relative* comparison is meaningful here. There is one
run per config and no confidence intervals. For deployment sizing, run on Linux
bare metal, repeat ≥ 5×, and alternate engine order.
```

Beyond this comparison harness, the full plan (see design/09) adds: a `criterion`
micro-suite for parser/envelope/merge code; a storage-floor comparison against
ondaDB's own `onda_bench` (the gap to engine ops/s is the number to optimize,
target ≤ 7×); 3/5/9-node cluster runs measuring replication-propagation latency,
fetch latency, and the staleness gauge under AE-only repair; and a 24 h
sustained-write soak watching compaction debt, ring occupancy, and p99 drift.

## Where to go next

- The guarantees behind the numbers: [Consistency & anti-entropy](../consistency/).
- How faults are exercised: [Testing](../testing/).
- System components: [Architecture](../architecture/).

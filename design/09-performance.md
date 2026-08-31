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
  contention); sender tasks batch (256 ops / 1 MiB, pumped on notify or a
  50 ms tick) and write postcard frames with the envelope+payload bytes
  **verbatim** — zero re-serialization.
- Apply path groups a `ReplBatch` into one ondaDB `Txn` per shard —
  group-commit WAL amortizes fsync.
- Merkle roots are cached per partition and rescanned only when the commit
  hook dirtied the pid (or on a 10-min TTL — ondaDB's TTL purge bypasses
  the hook); quiescent partitions cost no scan per round, and
  `MAREKVS_AE_PARTITIONS_PER_ROUND` caps probes per round if set. Scans run
  on shard threads at AE pace, dirty-marked down to the buckets that
  actually changed. AE/bootstrap frames ride a dedicated incoming lane so
  digest scans never head-of-line-block read-through fetches.

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
| feature `unsafe-fastpath` | **benchmark, then decide** | mmap reads + arena memtable ≈ C-class perf; lifts the crate's `deny(unsafe_code)` (0.8.0 relaxed this from `forbid`, which made the default build fail to compile on Linux). Ship default-safe, offer a `-fast` image variant if the delta is ≥ 20 %. |

### ondaDB 0.9.0 adoption

Every 0.9.0 format feature sits behind a **one-way** manifest capability bit
that defaults off, so a database enabling nothing is byte-identical to 0.8.2.
Adopted so far:

| Capability | Why | Rollback boundary |
|---|---|---|
| `CAP_RANGE_DELETES` | the cold-partition purge drops `[pid, pid+1)` in one record instead of a scan plus a tombstone per key, and range deletes are structurally invisible to commit hooks so the local-only guarantee stops depending on a `suppress_commit_hook()` guard | once taken, the data directory can no longer be opened by ondaDB < 0.9.0 |
| `CAP_PERIODIC_AGE` | a size trigger never fires on a family that has stopped being written, so gc_grace tombstones and head-tombstone-shadowed collection elements accumulate indefinitely on an idle node. Needs a durable `SstMeta::last_compaction_time` to measure age against | same |

**Prefix-delta data blocks: measured, and left off.** `MAREKVS_PREFIX_DELTA_KEYS`
exists and works, but the default stays `0` and `CAP_PREFIX_DELTA` stays
unclaimed unless it is switched on.

The hypothesis was that marekvs would beat ondaDB's own 12.8%, because its keys
are far more prefix-redundant than the corpus ondaDB measured: every element
record of a collection repeats `[pid][tag][varint klen][userkey]` and differs
only in its suffix. **That was wrong.** On 100 hashes x 200 fields with
incompressible 32-byte values — a shape chosen to be maximally favourable —
the store went 730,862 -> 696,776 bytes, a saving of **4.66%**
(`tests/prefix_delta.rs`, run it with `--nocapture`).

The reason is that lz4 is already exploiting that redundancy inside each block.
Prefix-delta removes it again *before* compression, so it only collects what lz4
left behind. Under 5% is not worth a per-block CPU cost on every read plus a
one-way capability bit that removes the rollback path to ondaDB < 0.9.0.

Worth re-measuring if `compression` ever moves to `None`, where lz4 is not
doing that work.

**MultiGet: adopted, and the win is small.** MGET resolves each shard's keys
through `Txn::multi_get` (`store::read_lww_batch`) instead of one point read per
key. Measured storage-path-only, batch of 64, release build, 10 runs:
**~1.09x**, range 0.98-1.22x, batched never meaningfully slower.

That is far short of ondaDB's headline 2.4-3.4x, and the reason is structural:
that figure comes from batches whose keys share blocks, and marekvs hashes the
user key into the partition id, so one shard's keys scatter across the keyspace
and mostly land in *different* blocks. The per-block fetch-and-decompress saving
therefore never materialises; what is left is the shared snapshot and level-walk
setup, paid once instead of N times.

Kept rather than reverted because it is consistently non-negative rather than
merely on average positive, and because `read_lww_batch` is the primitive the
anti-entropy and bootstrap fetch paths would want. Re-measure with
`cargo test --release -p marekvs-engine --test mget_batch -- --ignored --nocapture`.

**PerfContext: `DEBUG PERFCTX <key> [<key>...]`.** Reports ondaDB's read-path
counters for reading those keys — bloom probes and negatives, memtable and
SSTable probes, index seeks, block-cache hits and misses, bytes decompressed,
vlog reads and cache hits, and `multiget_blocks_deduped`. One key uses the point
path, several the batched one.

This is the instrument for the L0-depth question left open by the 0.8.x
benchmarking: it attributes a slow read to a mechanism instead of inferring it
from wall time. The scope opens on the shard thread because PerfContext is
thread-affine and counts into whatever scope is open on the thread doing the
work.

**Background IO limits and the vlog value cache: knobs only, all default off.**
`MAREKVS_BACKGROUND_IO_BPS` / `_BURST_BYTES` / `MAREKVS_OBSOLETE_DELETE_BPS`
bound background bandwidth so flush and compaction cannot monopolise a shared
PVC; `MAREKVS_VLOG_VALUE_CACHE_BYTES` caches decoded vlog values, which is where
every value above `klog_value_threshold` (512 B) lives.

Both ship off, and deliberately **unmeasured here**, because ondaDB ships them
off too and did not validate either: 0.6's acceptance benchmark is one of
several it records as run under load it does not trust, and 0.5's arm was
S3-gated and never ran at all. Adopting an unvalidated default on the strength
of a plausible mechanism is how a regression gets shipped. Turn them on against
a measurement on the bench box — and use a bracketed A-B-A there, since that box
drifts +/-15% run to run and an ordered sweep has already produced one false
result in this project's history.

The block-cache `BlockDomain` fix that shipped alongside the vlog cache — a klog
block and a vlog frame at the same offset of the same file could alias — is
unconditional and arrived with the 0.9.0 upgrade itself, not with this knob.

Deliberately **not** adopted, with reasons, so nobody re-runs the analysis:

- **Merge operators.** `write_merged` is literally read-modify-write and marekvs
  is a CRDT store, so it looks like a perfect fit. It is not: `write_merged`
  returns *whether the stored bytes changed* and Redis replies depend on that
  (SADD's count, DEL's true/false), which a merge operand cannot answer without
  the read it was meant to remove. ondaDB's headline gain is contention (3.02x,
  zero retries) and marekvs has none by construction — one thread per shard.
  Uncontended it is 1.15x.
- **Pessimistic locking / prepared transactions.** No conflicts to eliminate and
  no coordinator; ondaDB itself reports the 3.3 throughput case as not made.
- **Tailing iterators.** Replication uses the commit hook and ring, not
  iteration, and it is explicitly not a change feed.

### Compaction and backpressure (ondaDB ≥ 0.8.0)

0.8.0 bounded compaction jobs (one source file plus the target files it
overlaps) and split `target_file_size` / `l1_base_bytes` out of
`write_buffer_size`. Before that, L1's capacity *was* `write_buffer_size`, so L1
held exactly one file spanning the keyspace and every push-down rewrote the
level below — work per job grew with the dataset. That is why the
`write_buffer_size = 128 MiB` above was never applied in code: under 0.7.x it
would have made the geometry worse, not better. It is now decoupled and safe to
raise; measure before doing so.

| Knob | Value | Why |
|---|---|---|
| `num_compaction_threads` | default 2, `MAREKVS_COMPACTION_THREADS` | jobs are range-locked and disjoint ones now run concurrently; 2 predates that change |
| `finish_compactions_on_close` | off, `MAREKVS_FINISH_COMPACTIONS_ON_CLOSE` | prompt shutdown inside the 60 s k8s grace, at the cost of a deeper L0 on the restarted pod |
| debt write-stop | 6 GiB / 4 GiB | marekvs refuses client writes below ondaDB's 8 GiB hard pacing ceiling, which blocks the commit *on a shard thread* — see `Engine::compaction_stopped` |

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

1. **ondadb iterator construction was O(memtable)** — `Memtable::snapshot()`
   cloned and sorted every live entry on every `new_iterator()` call. Point
   ops were unaffected (0.1 ms), but every prefix scan (SPOP, ZPOPMIN, SCARD,
   SMEMBERS, HGETALL, sweeper ticks) paid milliseconds once the memtable
   held tens of thousands of records: measured 1.3 ms/SPOP at 2 k memtable
   entries → 5.1 ms at ~15 k, while a point SADD stayed at 0.12 ms.

   **Fixed in ondaDB, as requested.** `Memtable::iter` is now lazy: a
   `LazyMemIter` k-way merge (binary heap) directly over the already-sorted
   shard skip lists, no snapshot-collect-sort (`ondadb/src/memtable.rs:13`).
   ondaDB 0.7.2 additionally binary-searches sorted levels when building an
   iterator, and marekvs now passes explicit key bounds (see design/02).
   **The SPOP/ZPOPMIN numbers above predate all three and need re-measuring**;
   the `pop_hints` workaround in `store.rs` should be re-justified or deleted
   on the new numbers.
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

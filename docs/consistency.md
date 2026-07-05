---
title: Consistency & anti-entropy
description: The two-layer anti-entropy model, the derived 15 s staleness bound, tombstone GC, TTL convergence, HLC discipline — and the canonical defaults table.
status: mixed
---

marekvs is eventually consistent, but *bounded*-eventually: the requirement is
that stale records never live for long. This page is the mechanism and the math
behind the **15 s worst-case, milliseconds-typical** staleness bound the
[overview](../overview/#published-guarantees) promises.

Anti-entropy (AE) runs in **two layers**, log-first with Merkle as a backstop.
Both run **only between home replicas** — interest replicas get their bound from
connection-scoped leases ([replication](../replication/#interest-subscriptions))
and never participate in AE.

## Layer 1 — sequence-cursor catch-up

<a name="layer-1"></a>
Every node persists `applied_seq[origin_node]` in the `meta` CF, updated as
`ReplBatch`es are applied (each batch carries the origin's `first_seq`). On
(re)connect to a peer, a node sends `ResumeFrom{origin: self, seq}`; if the
peer's [replication ring](../replication/#replication-ring--backpressure) still
holds that seq, it replays from there.

This is the **log-first resume**: it covers restarts and brief blips at
near-zero cost, and it is exact — no digests, no scanning. Merkle exchange only
runs when the log has already rolled past the gap.

## Layer 2 — per-partition Merkle exchange

<a name="layer-2"></a>
Per owned partition, a 2-level digest:

- **256 leaf buckets**: `bucket = xxh3_64(ikey) & 0xFF`.
- **Bucket digest** = XOR-fold of `xxh3(ikey ‖ hlc ‖ value_hash)` over the
  bucket's records. XOR is commutative, so no sorting is needed. Digests are
  **content-aware** (`value_hash = xxh3(stored bytes)`), not just version-aware.
- **Root** = xxh3 over the 256 bucket digests.

```note
**Why content-aware.** Merged CRDT records (PN counters, HLL) can hold
*different* slot sets under the *same* envelope version (symmetric max). An
`(ikey, hlc)`-only digest would call two divergent replicas identical and repair
would never fire — the chaos-suite clock-skew finding (design/10). Keying the
digest on `value_hash` too makes equal-version / different-content records repair
in both directions.
```

Buckets are **dirty-marked** by the commit hook (a bit set, nothing else) and
recomputed lazily by prefix scan at sync time on the shard thread. A bucket is
~1/256 of a partition (≈1/1M of the keyspace), so scan cost is bounded and the
hook never needs old values.

### Round protocol

Every `ae_round = 5 s` (plus jitter), each node walks its owned partitions in
rotation and, for each, picks a random other owner (pairwise, Dynamo-style):

```text
for each owned pid this round:
    peer = random other member of owners(pid)
    → MerkleRoot{pid, root}
    if roots differ:
        ← MerkleBuckets{pid, [256 × u64]}
        → per differing bucket: BucketKeys{pid, bucket, [(ikey_hash, hlc, value_hash)]}
        ↔ push/pull ReplOps for keys whose hlc or content differs or that are missing
```

Runs on the `bulk` connection, lz4-framed. Steady-state cost is one 12-byte root
per (pid, round-participation) — tens of KB/s per node even on a 50-node
cluster. Repair cost is proportional to the actual diff, which per-element keys
keep minimal.

## Staleness bound

<a name="staleness-bound"></a>
A committed write can be missing on some home only if its replication push
failed (peer down, ring overrun, or membership-view divergence). Then:

```text
staleness ≤ AE rotation delay + round exchange time
rotation delay ≤ ae_round × ceil(owned_pids / ae_partitions_per_round)
```

Owned pids shrink with cluster size (≈ `4096 × N / n`), so the bound is sized
for the worst supported small cluster:

- **n ≥ 24**: all owned pids fit in one round → bound ≈ `2 × ae_round + 1 s ≈ 11 s`.
- **n = 3** (floor): with a per-round cap the design restores ≤ 2 rounds for any
  cluster size, holding the bound.

**Published bound: 15 s worst case** (2 rounds + exchange + margin);
**typical: replication-push latency, single-digit milliseconds.** The single
knob to tighten it is `ae_round`, and cost scales linearly.

```planned
The `ae_partitions_per_round` **per-round cap / auto-scale**
(`max(512, ceil(owned_pids / 2))`) is a design target and is **not implemented**.
Today every round walks *all* owned pids, so the ≤ 2-round rotation bound holds
trivially and the published 15 s figure stands — the cap only matters as an
optimization at large partition counts per node.
```

For interest replicas, the bound is ≤ the home bound plus push latency while
connected; after a disconnect it is bounded by liveness detection plus
revalidation on the next read. The pathological ceiling (a wedged-open
connection) is the 60 s lease timer.

## Tombstone lifecycle & GC safe point

A delete is never a raw storage delete at write time — it is an envelope with
the tombstone flag set (and, for element removes, the observed dots). GC works
off the storage engine's per-key TTL:

- Every tombstone carries **ondaDB per-key TTL = `gc_grace = 1 h`**, so the
  engine purges it automatically at the safe point — no sweep needed.
- **Safety invariant.** A replica partitioned or down longer than `gc_grace`
  may hold data whose covering tombstone was already purged elsewhere; merging
  it back would resurrect the delete.

```planned
**The pull-only-until-synced rejoin rule is designed but not yet enforced.**

The intended enforcement: on rejoin, if `now − last_alive > gc_grace`, the
node's home partitions become **pull-only** — it receives AE repairs but never
pushes, until each partition completes a full Merkle sync against a current home
(its local data is a warm base; only the diff is pulled). Only then does it
regain push eligibility. This is the precise rule that prevents resurrection
across a long absence; today it is not wired into the rejoin path.
```

Interest replicas cannot resurrect by construction: they never push AE, and
lease-gated reads revalidate against homes.

## TTL convergence

- Deadlines are **absolute milliseconds**, set once at the origin, shipped in
  the envelope, never recomputed per hop. Every replica evaluates
  `now ≥ deadline` locally → identical convergence modulo NTP-level skew.
- An expiry is an **implicit tombstone** with `hlc = HLC(deadline, 0)`: any
  stale pre-expiry version loses the merge against expiry, so no expiry messages
  are needed and replicas can't disagree for longer than skew.
- `EXPIRE` / `PERSIST` / `EXPIREAT` are ordinary LWW envelope writes and
  replicate like any write.

```planned
`ttl_skew_grace` (design 5 s) — excluding expired records from Merkle digests
only after `deadline + ttl_skew_grace`, so skewed replicas don't ping-pong
repairs around the deadline — is **unimplemented**. Today expiry is materialized
by the sweep as an ordinary tombstone write, with no digest-exclusion grace.
```

## HLC discipline

The full layout is in the [data model](../data-model/#hybrid-logical-clock);
the rules that matter for convergence:

- One process-wide HLC (an atomic `u64`). Local event:
  `max(prev + 1, wall << 16)`. Receive: `max(local, remote) + 1`.
- **The receive rule runs at the replication apply point** (`apply_op` in
  marekvs-repl): every ingested record's HLC is observed before it is merged.
  This is load-bearing. Without it, a node with a lagging wall clock that reads a
  value and then overwrites it stamps the overwrite *below* the value it
  causally read, and the overwrite loses LWW everywhere.
- LWW total order is `(hlc, origin)`; equal pairs denote the identical write.
- A remote HLC more than `max_clock_drift = 5 s` ahead of local wall clock is
  clamped with a loud log. NTP is assumed on k8s nodes; the receive-max rule
  keeps causally-related updates ordered even under skew.

```note
The clock-skew failure above was found in practice on Apple containers
(per-container VMs with skewed clocks). Docker Compose shares one VM clock and
cannot reproduce it, so the apple-container cluster test (`just apple-test`) is
also the clock-skew regression test.
```

## Defaults table

<a name="defaults-table"></a>
This is the **single source of truth** for every tunable — other pages reference
it and never restate values.

The **Where set** column is the current-vs-planned oracle:

- `env VAR` — startup environment variable (restart to change) → **implemented**.
- `const (crate)` — compile-time constant (rebuild to change) → **implemented**.
- `manifest` — the k8s pod spec → **implemented**.
- `design` — a design target **not yet implemented**; the Notes column says what
  the code does instead. These rows are marked _Planned_ and collected in the
  callout below the table.

**Runtime** = adjustable on a live node without a restart. `CONFIG SET` applies
three live keys — `requirepass`, `lua-time-limit` (alias
`busy-reply-threshold`), and `loglevel` — and accepts-but-ignores everything
else. All runtime changes are ephemeral: the env is the source of truth again
after a restart (`CONFIG REWRITE` is a no-op; the k8s manifest is the durable
config).

| Parameter | Default | Where set | Notes |
|---|---|---|---|
| replicas N | 3 | env `MAREKVS_REPLICAS_N` | per-key homes; also the minimum node floor; must match cluster-wide |
| partitions P | 4096 | const (marekvs-core `PARTITIONS`) | fixed at cluster creation; u16 prefix, 12 bits used |
| shard threads | cores − 2, min 2 | env `MAREKVS_SHARDS` | storage/execution threads per node |
| gossip interval | 500 ms | const (marekvs-server) | chitchat |
| failure detection | ~5 s | chitchat defaults | phi-accrual |
| gossip dead-node grace | 1 h | const (marekvs-cluster) | chitchat `marked_for_deletion_grace_period` |
| ae_round | 5 s + 0–2 s jitter | const (marekvs-repl `AE_ROUND`) | jitter is uniform 0–2 s |
| ae_partitions_per_round | all owned pids | design | _Planned_ — per-round cap `max(512, owned/2)` unimplemented; every round walks all owned pids, so the ≤ 2-round rotation bound holds trivially |
| stranded-record AE | every 3rd round | const (marekvs-repl) | push-only roots for non-owned pids with local data (chaos finding, design/10) |
| **published staleness bound** | **15 s worst / ms typical** | derived | derivation above |
| merkle buckets / partition | 256 | const (marekvs-repl `BUCKETS`) | content-aware digests: (ikey, hlc, value_hash) |
| interest_lease | 60 s | const (marekvs-repl `INTEREST_LEASE`) | connection-scoped |
| interest renew interval | — | design (15 s) | _Planned_ — `InterestRenew` msg exists and is handled but never sent; leases refresh by re-fetch on expiry |
| read-through fetch timeout | 300 ms | const (marekvs-repl `FETCH_TIMEOUT`) | miss → serve local/empty, AE reconciles |
| peer heartbeat / timeout | — | design (1 s / 3 s) | _Planned_ — not implemented; peer liveness = TCP disconnect + gossip failure detection |
| interest_escalate | — | design (4096 keys/pid) | _Planned_ — whole-partition escalation unimplemented |
| interest_max_entries | — | design (1,000,000) | _Planned_ — no cap/LRU on the interest map; expired entries GC'd each AE round |
| replication ring | 128 MiB / 262,144 ops | const (marekvs-repl `RING_MAX_*`) | overrun → ring gap warning + AE backstop |
| repl batch | 256 ops / pump on notify or 50 ms tick | const (marekvs-repl `BATCH_MAX_OPS`) | _Planned_ — design byte cap (256 KiB) + 2 ms linger unimplemented |
| per-peer unacked window | — | design (4 MiB) | _Planned_ — `AckSeq` is received and ignored; no send-window flow control |
| ring high-water persist | 1 s | const (marekvs-repl) | restart resumes seq space +1,000,000 above persisted HW |
| mesh writer queue | 4096 msgs | const (marekvs-repl) | per-peer, per-lane |
| mesh reconnect backoff | 100 ms → 5 s | const (marekvs-repl) | exponential |
| gc_grace | 1 h | const (marekvs-engine `GC_GRACE`) | tombstone TTL; _Planned_ — the pull-only-until-synced rejoin rule is **not yet enforced** |
| ttl_skew_grace | — | design (5 s) | _Planned_ — expiry is materialized by the sweep as an ordinary tombstone write; digest-exclusion grace unimplemented |
| expiry sweep budget | 128 records | const (marekvs-engine) | incremental cursor walk between shard jobs |
| max_clock_drift | 5 s | const (marekvs-core `MAX_CLOCK_DRIFT_MS`) | remote HLC clamp + loud log |
| repair_delay | — | design (30 s + jitter) | _Planned_ — unimplemented; AE repairs fire on the next round |
| bootstrap chunking | 256 ops/chunk, sequential | const (marekvs-repl) | lz4 bulk lane; _Planned_ — design 8 streams / 64 MiB/s rate cap unimplemented |
| cold_purge_delay | — | design (15 m) | _Planned_ — unimplemented; data kept after losing ownership (feeds stranded-record AE) |
| terminationGracePeriodSeconds | 60 | manifest (k8s/statefulset.yaml) | drain typically completes in ~3 s |
| listen addresses | :6379 / :7373 / :7946 / :9121 | env `MAREKVS_{RESP,MESH,GOSSIP,METRICS}_ADDR` | RESP / mesh / gossip(UDP) / metrics+probes |
| node identity | hostname ordinal, else 0 | env `MAREKVS_NODE_ID` | `marekvs-3` → 3; StatefulSet needs no per-pod config |
| data dir | `.data/n0` | env `MAREKVS_DATA_DIR` | |
| seeds | empty | env `MAREKVS_SEEDS` | chitchat re-resolves DNS names continuously |
| advertise IP | 127.0.0.1 | env `MAREKVS_ADVERTISE_IP` | `auto` = self-detect the pod IP |
| cluster name | `marekvs` | env `MAREKVS_CLUSTER` | gossip cluster isolation |
| requirepass | off | env `MAREKVS_REQUIREPASS` | runtime via `CONFIG SET requirepass`; new connections need the new password, authenticated sessions stay |
| upstream Redis master | none | env `MAREKVS_REPLICAOF` | runtime via `REPLICAOF`/`SLAVEOF`; live-migration ingest, node stays writable |
| script time limit | 20 ms | env `MAREKVS_SCRIPT_TIME_LIMIT_MS` | runtime via `CONFIG SET lua-time-limit` (alias `busy-reply-threshold`); applies from the next EVAL |
| Lua allocator limit | 16 MiB | const (marekvs-engine) | per script VM |
| blocking-list poll | 50 ms | const (marekvs-engine `POLL_MS`) | BLPOP/BRPOP wakeup granularity |
| ondaDB sync_mode | Interval, 128 ms | ondadb default | durability window per node |
| log level | `info,chitchat=warn` | env `RUST_LOG` | runtime via `CONFIG SET loglevel`; Redis levels map to tracing, any other value is a raw filter spec |

```planned
**Planned parameters — designed, not yet in code.** Collected here so the table
above can stay factual. Each does something specific *today* (see the row's
Notes); the design target is what is missing.

- **`ae_partitions_per_round`** — per-round cap / auto-scale. Every round walks
  all owned pids instead.
- **interest renew interval** (15 s) — `InterestRenew` is handled but never
  sent; leases refresh by re-fetch on expiry.
- **peer heartbeat / timeout** (1 s / 3 s) — liveness is TCP disconnect + gossip
  failure detection.
- **`interest_escalate`** (4096 keys/pid) — no whole-partition escalation.
- **`interest_max_entries`** (1,000,000) — no cap/LRU on the interest map.
- **per-peer unacked window / `AckSeq` flow control** (4 MiB) — `AckSeq` is
  received and ignored; no send-window backpressure.
- **repl batch byte cap (256 KiB) + 2 ms linger** — batching is by op count
  (256) and a 50 ms tick.
- **`ttl_skew_grace`** (5 s) — no digest-exclusion grace around deadlines.
- **`repair_delay`** (30 s + jitter) — repairs fire on the next AE round.
- **bootstrap 8 streams / 64 MiB/s rate cap** — chunking is 256 ops/chunk,
  sequential.
- **`cold_purge_delay`** (15 m) — cold data is kept after ownership loss (feeds
  stranded-record AE).
- **`gc_grace` pull-only-until-synced rejoin rule** — the resurrection-prevention
  gate is not yet enforced on rejoin.
```

## Where to go next

- How writes are pushed before AE ever runs: [Replication](../replication/).
- Join, leave, and failure detection: [Cluster membership](../membership/).
- The guarantees this all backs: [Overview](../overview/#published-guarantees).

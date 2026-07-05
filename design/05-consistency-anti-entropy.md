# 05 — Consistency, Anti-Entropy & the Staleness Bound

The requirement: *"we need to make 100% sure that we don't have stale records
for a longer period of time."* This document is the mechanism and the math.

Two layers, log-first with Merkle as backstop. Both run **only between home
replicas** — interest replicas get their bound from connection-scoped leases
([04-replication.md](04-replication.md#interest-subscriptions)) and never
participate in anti-entropy (AE).

## Layer 1 — sequence-cursor catch-up

Every node persists `applied_seq[origin_node]` in the `meta` CF, updated as
`ReplBatch`es are applied (each batch carries the origin's `first_seq`).
On (re)connect to a peer, a node sends `ResumeFrom{origin: self, seq}`; if the
peer's replication ring still holds that seq, it replays from there. This
covers restarts and brief blips at near-zero cost, and is exact (no digests).

## Layer 2 — per-partition Merkle exchange

Per owned partition: a 2-level digest.

- **256 leaf buckets**: `bucket = xxh3_64(ikey) & 0xFF`.
- **Bucket digest** = XOR-fold of `xxh3(ikey ‖ hlc ‖ value_hash)` over the
  bucket's records — commutative, so no sorting needed. Digests are
  **content-aware** (`value_hash = xxh3(stored bytes)`), not just
  version-aware: merged CRDT records (PN counters, HLL) can hold *different*
  slot sets under the *same* envelope version (symmetric max), so an
  `(ikey, hlc)`-only digest calls two divergent replicas identical and
  repair never fires (chaos clock-skew finding, design/10).
- **Root** = xxh3 over the 256 bucket digests.

Buckets are **dirty-marked** by the commit hook (a bit set, nothing else) and
recomputed lazily by prefix scan at sync time on the shard thread. Scan cost is
bounded (a bucket is ~1/256 of a partition ≈ 1/1M of the keyspace) and avoids
needing old values inside the hook.

### Round protocol

Every `ae_round = 5 s` (±20 % jitter), each node walks its owned partitions in
rotation, ≤ `ae_partitions_per_round = 512` per round:

```
for each owned pid this round:
    peer = random other member of owners(pid)        # pairwise, Dynamo-style
    → MerkleRoot{pid, root}
    if roots differ:
        ← MerkleBuckets{pid, [256 × u64]}
        → per differing bucket: BucketKeys{pid, bucket, [(ikey_hash, hlc, value_hash)]}
        ↔ push/pull ReplOps for keys whose hlc or content differs or that are missing
```

Runs on the `bulk` connection, lz4-framed. Steady-state cost: one 12-byte root
per (pid, round-participation) — with 4096 partitions × 3 owners spread over a
50-node cluster, tens of KB/s per node. Repair cost is proportional to the
actual diff (per-element keys keep diffs minimal).

## Staleness bound

<a name="staleness-bound"></a>
A committed write can be missing on some home only if its replication push
failed (peer down, ring overrun, membership-view divergence). Then:

```
staleness ≤ AE rotation delay + round exchange time
rotation delay ≤ ae_round × ceil(owned_pids / ae_partitions_per_round)
```

Defaults: a 3-node cluster owns ≤ 4096 pids each → `ceil(4096/512) = 8` rounds
worst case... but owned pids shrink with cluster size (≈ 4096×3/n). We size the
published bound for the worst supported small cluster:

- **n ≥ 24**: all owned pids fit in one round → bound ≈ `2 × ae_round + 1 s ≈ 11 s`.
- **n = 3** (floor): 4096 owned pids → 8 rounds → 40 s. Unacceptable; therefore
  `ae_partitions_per_round` **auto-scales**: `max(512, ceil(owned_pids / 2))`,
  restoring ≤ 2 rounds for any cluster size.

**Published bound: 15 s worst case** (2 rounds + exchange + margin);
**typical: replication push latency, single-digit milliseconds.** The single
knob to tighten it is `ae_round`; cost scales linearly.

Interest replicas: ≤ home bound + push latency while connected; after a
disconnect, ≤ 3 s heartbeat detection + revalidation on next read. The
pathological ceiling (wedged-open connection) is the 60 s lease timer
(risky assumption 4, [00-overview.md](00-overview.md)).

## Tombstone lifecycle & GC safe point

Deletes are never raw storage deletes at write time — a delete is an envelope
with the tombstone flag (and, for element removes, observed dots). GC:

- Every tombstone carries **ondaDB per-key TTL = `gc_grace` = 1 h** → the
  storage engine purges it automatically at the safe point; no sweep needed.
- **Safety invariant**: a replica partitioned/down longer than `gc_grace` may
  hold data whose covering tombstone was already purged elsewhere; merging it
  back would resurrect deletes. Enforcement on rejoin: if
  `now − last_alive > gc_grace` (last_alive persisted in `meta` and observed
  via gossip), the node's home partitions become **pull-only**: it
  participates in AE receiving repairs but never pushes, until each partition
  completes a full Merkle sync against a current home (its local data serves
  as a warm base — the diff is pulled, not re-streamed). Only then does it
  regain push eligibility. *Pull-only-until-synced is the precise rule that
  prevents resurrection.*
- Interest replicas cannot resurrect by construction: they never push AE, and
  lease-gated reads revalidate against homes.

## TTL convergence

- Deadlines are **absolute ms**, set once at origin, shipped in the envelope,
  never recomputed per hop. Every replica evaluates `now ≥ deadline` locally →
  convergence identical modulo NTP-level skew.
- Expiry is an **implicit tombstone** with `hlc = HLC(deadline, 0)`: any stale
  pre-expiry version loses the merge against expiry — no expiry messages
  needed, replicas can't disagree for longer than skew.
- Expired records are excluded from Merkle digests only after
  `deadline + ttl_skew_grace = 5 s`, so skewed replicas don't ping-pong
  repairs around the deadline.
- EXPIRE/PERSIST/EXPIREAT are ordinary LWW envelope writes and replicate like
  any write.

## HLC discipline (summary; layout in [02](02-data-model.md#hybrid-logical-clock))

- One process-wide HLC (atomic u64). Local event: `max(prev+1, wall<<16)`.
  Receive: `max(local, remote) + 1`.
- **The receive rule runs at the replication apply point** (`apply_op` in
  marekvs-repl): every ingested record's HLC is observed before it is merged.
  This is load-bearing, not bookkeeping: without it, a node with a lagging
  wall clock that reads a value and then overwrites it stamps the overwrite
  *below* the value it causally read — the overwrite loses LWW everywhere.
  Found in practice on Apple containers (per-container VMs with skewed
  clocks); Docker Compose shares one VM clock and cannot reproduce it. The
  apple-container cluster test (`just apple-test`) is therefore also our
  clock-skew regression test.
- Remote HLC > 5 s ahead of local wall clock → clamp + loud log
  (`max_clock_drift`). NTP assumed on k8s nodes; HLC's receive-max rule keeps
  causally-related updates ordered even under skew.
- LWW total order `(hlc, origin)`; equal pairs denote the identical write.

## Defaults table

<a name="defaults-table"></a>
Single source of truth — other documents reference, never restate, these.

**Where set** says what changing the value takes: `env VAR` = startup
environment variable (restart to change), `const (crate)` = compile-time
constant (rebuild to change), `manifest` = the k8s pod spec, `design` = a
design target **not yet implemented** (the Notes column says what the code
does instead). **Runtime** = adjustable on a live node without a restart.

`CONFIG SET` is *accepted and ignored* (Redis-client compat; config is
env-driven). The only knob changeable at runtime is the upstream master via
the `REPLICAOF`/`SLAVEOF` command.

| Parameter | Default | Where set | Runtime | Notes |
|---|---|---|---|---|
| replicas N | 3 | env `MAREKVS_REPLICAS_N` | no | per-key homes; also the minimum node floor; must match cluster-wide |
| partitions P | 4096 | const (marekvs-core `PARTITIONS`) | no | fixed at cluster creation; u16 prefix, 12 bits used |
| shard threads | cores − 2, min 2 | env `MAREKVS_SHARDS` | no | storage/execution threads per node |
| gossip interval | 500 ms | const (marekvs-server) | no | chitchat |
| failure detection | ~5 s | chitchat defaults | no | phi-accrual |
| gossip dead-node grace | 1 h | const (marekvs-cluster) | no | chitchat `marked_for_deletion_grace_period` |
| ae_round | 5 s + 0–2 s jitter | const (marekvs-repl `AE_ROUND`) | no | jitter is uniform 0–2 s, not ±20 % |
| ae_partitions_per_round | all owned pids | design | — | per-round cap (max(512, owned/2)) unimplemented; every round walks all owned pids, so the ≤ 2-round rotation bound holds trivially |
| stranded-record AE | every 3rd round | const (marekvs-repl) | no | push-only roots for non-owned pids with local data (chaos finding, design/10) |
| **published staleness bound** | **15 s worst / ms typical** | derived | — | derivation above |
| merkle buckets / partition | 256 | const (marekvs-repl `BUCKETS`) | no | content-aware digests: (ikey, hlc, value_hash) |
| interest_lease | 60 s | const (marekvs-repl `INTEREST_LEASE`) | no | connection-scoped |
| interest renew interval | — | design (15 s) | — | `InterestRenew` msg exists and is handled but never sent; leases refresh by re-fetch on expiry |
| read-through fetch timeout | 300 ms | const (marekvs-repl `FETCH_TIMEOUT`) | no | miss → serve local/empty, AE reconciles |
| peer heartbeat / timeout | — | design (1 s / 3 s) | — | not implemented; peer liveness = TCP disconnect + gossip failure detection |
| interest_escalate | — | design (4096 keys/pid) | — | whole-partition escalation unimplemented |
| interest_max_entries | — | design (1,000,000) | — | no cap/LRU on the interest map; expired entries GC'd each AE round |
| replication ring | 128 MiB / 262,144 ops | const (marekvs-repl `RING_MAX_*`) | no | overrun → ring gap warning + AE backstop |
| repl batch | 256 ops / pump on notify or 50 ms tick | const (marekvs-repl `BATCH_MAX_OPS`) | no | design byte cap (256 KiB) + 2 ms linger unimplemented |
| per-peer unacked window | — | design (4 MiB) | — | `AckSeq` is received and ignored; no send-window flow control |
| ring high-water persist | 1 s | const (marekvs-repl) | no | restart resumes seq space +1,000,000 above persisted HW |
| mesh writer queue | 4096 msgs | const (marekvs-repl) | no | per-peer, per-lane |
| mesh reconnect backoff | 100 ms → 5 s | const (marekvs-repl) | no | exponential |
| gc_grace | 1 h | const (marekvs-engine `GC_GRACE`) | no | tombstone TTL; the pull-only-until-synced rejoin rule is **not yet enforced** |
| ttl_skew_grace | — | design (5 s) | — | expiry is materialized by the sweep as an ordinary tombstone write; digest-exclusion grace unimplemented |
| expiry sweep budget | 128 records | const (marekvs-engine) | no | incremental cursor walk between shard jobs |
| max_clock_drift | 5 s | const (marekvs-core `MAX_CLOCK_DRIFT_MS`) | no | remote HLC clamp + loud log |
| repair_delay | — | design (30 s + jitter) | — | unimplemented; AE repairs fire on the next round |
| bootstrap chunking | 256 ops/chunk, sequential | const (marekvs-repl) | no | lz4 bulk lane; design 8 streams / 64 MiB/s rate cap unimplemented |
| cold_purge_delay | — | design (15 m) | — | unimplemented; data kept after losing ownership (feeds stranded-record AE) |
| terminationGracePeriodSeconds | 60 | manifest (k8s/statefulset.yaml) | k8s edit | drain typically completes in ~3 s |
| listen addresses | :6379 / :7373 / :7946 / :9121 | env `MAREKVS_{RESP,MESH,GOSSIP,METRICS}_ADDR` | no | RESP / mesh / gossip(UDP) / metrics+probes |
| node identity | hostname ordinal, else 0 | env `MAREKVS_NODE_ID` | no | `marekvs-3` → 3; StatefulSet needs no per-pod config |
| data dir | `.data/n0` | env `MAREKVS_DATA_DIR` | no | |
| seeds | empty | env `MAREKVS_SEEDS` | no | chitchat re-resolves DNS names continuously |
| advertise IP | 127.0.0.1 | env `MAREKVS_ADVERTISE_IP` | no | `auto` = self-detect the pod IP |
| cluster name | `marekvs` | env `MAREKVS_CLUSTER` | no | gossip cluster isolation |
| requirepass | off | env `MAREKVS_REQUIREPASS` | no | `CONFIG SET requirepass` is ignored like all CONFIG SET |
| upstream Redis master | none | env `MAREKVS_REPLICAOF` | **yes** — `REPLICAOF`/`SLAVEOF` cmd | live-migration ingest (design/03); node stays writable |
| script time limit | 20 ms | env `MAREKVS_SCRIPT_TIME_LIMIT_MS` | no | read per EVAL, but the process env is fixed at exec |
| Lua allocator limit | 16 MiB | const (marekvs-engine) | no | per script VM |
| blocking-list poll | 50 ms | const (marekvs-engine `POLL_MS`) | no | BLPOP/BRPOP wakeup granularity |
| ondaDB sync_mode | Interval, 128 ms | ondadb default | no | durability window per node |
| log level | `info` | env `RUST_LOG` | no | |

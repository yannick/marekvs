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
- **Bucket digest** = xxh3 over the sorted `(ikey, hlc)` stream of the bucket.
  Only key + version are digested — value bytes don't matter; version equality
  implies value equality (envelopes are written once at origin).
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
        → per differing bucket: BucketKeys{pid, bucket, [(ikey_hash, hlc)]}
        ↔ push/pull ReplOps for keys whose hlc differs or that are missing
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

| Parameter | Default | Notes |
|---|---|---|
| replicas N | 3 | per-key homes; also the minimum node floor |
| partitions P | 4096 | fixed at cluster creation; u16 prefix, 12 bits used |
| gossip interval | 500 ms | chitchat |
| failure detection | ~5 s | phi-accrual (chitchat defaults) |
| ae_round | 5 s | ±20 % jitter |
| ae_partitions_per_round | max(512, owned/2) | keeps rotation ≤ 2 rounds |
| **published staleness bound** | **15 s worst / ms typical** | derivation above |
| merkle buckets / partition | 256 | dirty-marked, scan-on-sync |
| interest_lease | 60 s | connection-scoped |
| interest renew interval | 15 s | batched, only actually-read keys |
| peer heartbeat / timeout | 1 s / 3 s | ctl connection, application-level |
| interest_escalate | 4096 keys/partition | → whole-partition subscription |
| interest_max_entries | 1,000,000 | ~120 MB, LRU-evict |
| replication ring | 128 MiB / 262,144 ops | overrun → dirty-pair + AE |
| repl batch | 256 ops / 256 KiB / 2 ms linger | |
| per-peer unacked window | 4 MiB | |
| gc_grace | 1 h | down longer ⇒ pull-only until synced |
| ttl_skew_grace | 5 s | |
| max_clock_drift | 5 s | clamp + log |
| repair_delay | 30 s + jitter | absorbs quick pod restarts |
| bootstrap concurrency / rate | 8 streams / 64 MiB/s | lz4, per node each direction |
| cold_purge_delay | 15 m | after losing ownership |
| terminationGracePeriodSeconds | 300 | pod spec |
| ondaDB sync_mode | Interval 128 ms | durability window per node |

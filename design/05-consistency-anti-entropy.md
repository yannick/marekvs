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

Per-partition **roots are cached**: a root is rescanned only when the commit
hook has dirtied the pid since the last scan, or after a 10-min TTL (ondaDB's
TTL backstop purges expired records *without* a commit hook, so a purely
dirty-driven cache could hold a stale root forever on a quiescent pid).
Quiescent partitions therefore cost no scan per round;
`marekvs_ae_digest_scans_total` counts real scans. An empty partition's root
is the documented `0` sentinel (it was previously a nonzero xxh3-of-zeros
constant, so the stranded-AE "no local data" skip never fired).

### Round protocol

Every `ae_round = 5 s` (±20 % jitter), each node walks its owned partitions in
rotation, ≤ `ae_partitions_per_round` per round
(`MAREKVS_AE_PARTITIONS_PER_ROUND`, default 0 = all owned; a cap rotates a
per-round probe cursor):

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
- **n = 3** (floor): 4096 owned pids → 8 rounds → 40 s at a 512 cap.
  Unacceptable; as implemented the default (cap = 0) probes **all** owned pids
  every round, so the ≤ 2-round bound holds trivially at any cluster size —
  cached roots keep the per-round cost proportional to *dirty* partitions, not
  owned partitions. Setting `MAREKVS_AE_PARTITIONS_PER_ROUND` trades rotation
  delay for a hard per-round ceiling
  (`rotation ≤ ae_round × ceil(owned_pids / cap)`).

**Published bound: 15 s worst case** (2 rounds + exchange + margin);
**typical: replication push latency, single-digit milliseconds.** The single
knob to tighten it is `ae_round`; cost scales linearly.

Interest replicas: ≤ home bound + push latency while connected; after a
disconnect, ≤ 3 s heartbeat detection + revalidation on next read. The
formerly pathological wedged-open connection is now closed by the mesh
heartbeat (ping every 1 s, close after 3 s without inbound bytes —
`MAREKVS_MESH_PING_INTERVAL_MS` / `MAREKVS_MESH_IDLE_TIMEOUT_MS`), bounding
detection at ~3 s; the 60 s lease timer remains the absolute backstop
(risky assumption 4, [00-overview.md](00-overview.md)).

## Tombstone lifecycle & GC safe point

Deletes are never raw storage deletes at write time — a delete is an envelope
with the tombstone flag (and, for element removes, observed dots). GC:

- Every tombstone carries **ondaDB per-key TTL = `gc_grace` = 1 h**
  (`MAREKVS_GC_GRACE_SECS`, must be uniform cluster-wide) → the
  storage engine purges it automatically at the safe point; no sweep needed.
- **Safety invariant**: a replica partitioned/down longer than `gc_grace` may
  hold data whose covering tombstone was already purged elsewhere; merging it
  back would resurrect deletes. Enforcement on rejoin (implemented): an
  `alive:last` heartbeat is written to `meta` every `min(gc_grace/4, 30 s)`
  while Active/Leaving; a node whose downtime exceeds `gc_grace` stays
  **Joining** (held by the join gate) and Merkle-syncs each data-bearing home
  partition against its **pre-outage co-owner** — placement computed with
  itself included — never a current-view owner, which after a long outage can
  be an empty crash-era owner whose want-set would destroy valid data. Stale
  extras (records only the rejoiner holds, HLC before its death) are
  **dropped** when the co-owner's `RequestKeys` enumerates them
  (commit-hook-suppressed delete, so the drops don't replicate) instead of
  served; every other requester is refused without deletion while rejoining,
  and outbound stranded-AE is suppressed. A sole survivor (no Active peers
  anywhere) stands down and serves — there is no one to sync against.
  `MerkleRootMatch{pid}` confirms each partition; its local data serves as a
  warm base — the diff is pulled, not re-streamed. Only then does the node
  regain push eligibility. *Pull-only-until-synced is the precise rule that
  prevents resurrection.* Metrics: `marekvs_rejoin_active`,
  `marekvs_rejoin_dropped_records_total`.
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

`CONFIG SET` applies **three live keys** — `requirepass`, `lua-time-limit`
(alias `busy-reply-threshold`), and `loglevel` (a Redis level *or* a raw
tracing filter spec like `info,chitchat=debug`) — and accepts-but-ignores
everything else (Redis-client compat; config is env-driven). The upstream
master is changeable via the `REPLICAOF`/`SLAVEOF` command. All runtime
changes are **ephemeral**: the env is the source of truth again after a
restart (`CONFIG REWRITE` is a no-op — the k8s manifest is the durable
config).

| Parameter | Default | Where set | Runtime | Notes |
|---|---|---|---|---|
| replicas N | 3 | env `MAREKVS_REPLICAS_N` | no | per-key homes; also the minimum node floor; must match cluster-wide |
| partitions P | 4096 | const (marekvs-core `PARTITIONS`) | no | fixed at cluster creation; u16 prefix, 12 bits used |
| shard threads | cores − 2, min 2 | env `MAREKVS_SHARDS` | no | storage/execution threads per node |
| gossip interval | 500 ms | const (marekvs-server) | no | chitchat |
| failure detection | ~5 s | chitchat defaults | no | phi-accrual |
| gossip dead-node grace | 1 h | const (marekvs-cluster) | no | chitchat `marked_for_deletion_grace_period` |
| ae_round | 5 s + 0–2 s jitter | const (marekvs-repl `AE_ROUND`) | no | jitter is uniform 0–2 s, not ±20 % |
| ae_partitions_per_round | 0 = all owned pids | env `MAREKVS_AE_PARTITIONS_PER_ROUND` | no | default walks all owned pids (≤ 2-round rotation bound holds trivially); a cap rotates a per-round probe cursor |
| ae root cache TTL | 10 m | const (marekvs-repl `AE_ROOT_CACHE_TTL`) | no | roots rescanned only when the commit hook dirtied the pid or on TTL (ondaDB TTL purge bypasses the hook); `marekvs_ae_digest_scans_total` counts real scans |
| stranded-record AE | every 3rd round | const (marekvs-repl) | no | push-only roots for non-owned pids with local data (chaos finding, design/10); suppressed during a gc_grace rejoin |
| **published staleness bound** | **15 s worst / ms typical** | derived | — | derivation above |
| merkle buckets / partition | 256 | const (marekvs-repl `BUCKETS`) | no | content-aware digests: (ikey, hlc, value_hash) |
| interest_lease | 60 s | const (marekvs-repl `INTEREST_LEASE`) | no | connection-scoped |
| interest renew interval | — | design (15 s) | — | `InterestRenew` msg exists and is handled but never sent; leases refresh by re-fetch on expiry |
| read-through fetch timeout | 300 ms | const (marekvs-repl `FETCH_TIMEOUT`) | no | miss → serve local/empty, AE reconciles |
| peer heartbeat / timeout | 1 s / 3 s | env `MAREKVS_MESH_PING_INTERVAL_MS` / `MAREKVS_MESH_IDLE_TIMEOUT_MS` | no | every ctl+bulk connection pings; closed after the idle timeout without inbound bytes (`marekvs_mesh_conn_timeouts_total`); disconnect deregisters the peer entry, so `connected_peers` is truthful |
| interest_escalate | — | design (4096 keys/pid) | — | whole-partition escalation unimplemented |
| interest_max_entries | 1,000,000 | env `MAREKVS_INTEREST_MAX_ENTRIES` | no | reject-at-cap (refresh always allowed); a rejected registration degrades to worst-case-lease (60 s) staleness; `marekvs_interest_entries` / `marekvs_interest_rejected_total` |
| replication ring | 128 MiB / 262,144 ops | const (marekvs-repl `RING_MAX_*`) | no | overrun → ring gap warning + AE backstop; the ring is also the flow-control retransmit buffer |
| repl batch | 256 ops / 1 MiB payload / pump on notify or 50 ms tick | const (marekvs-repl `BATCH_MAX_OPS`, `BATCH_MAX_BYTES`) | no | byte cap keeps frames under proto MAX_FRAME (oversized frames previously failed encode silently); design 2 ms linger unimplemented |
| per-peer unacked window | 4 MiB | env `MAREKVS_REPL_WINDOW_BYTES` | no | `AckSeq` (= `ReplBatch.last_seq`) drains it; a full window stalls only that peer's lane (`marekvs_repl_window_stalls_total`, warn after 5 s); cursor advances only on successful send |
| ring high-water persist | 1 s | const (marekvs-repl) | no | restart resumes seq space +1,000,000 above persisted HW |
| mesh writer queue | 4096 msgs | const (marekvs-repl) | no | per-peer, per-lane |
| mesh reconnect backoff | 100 ms → 5 s | const (marekvs-repl) | no | exponential |
| gc_grace | 1 h | env `MAREKVS_GC_GRACE_SECS` | no | tombstone TTL; must be uniform cluster-wide; pull-only-until-synced rejoin rule **enforced** (above); `alive:last` heartbeat every min(gc_grace/4, 30 s) |
| ttl_skew_grace | — | design (5 s) | — | expiry is materialized by the sweep as an ordinary tombstone write; digest-exclusion grace unimplemented |
| expiry sweep budget | 128 records | const (marekvs-engine) | no | incremental cursor walk between shard jobs |
| max_clock_drift | 5 s | const (marekvs-core `MAX_CLOCK_DRIFT_MS`) | no | remote HLC clamp + loud log |
| repair_delay | — | design (30 s + jitter) | — | unimplemented; AE repairs fire on the next round |
| bootstrap chunking | 256 ops/chunk, sequential | const (marekvs-repl) | no | lz4 bulk lane; donors refuse non-owned pids and dedup duplicate (peer, pid) streams within a 20 s window; design 8 concurrent streams unimplemented |
| bootstrap rate cap | 64 MiB/s | env `MAREKVS_BOOTSTRAP_RATE_MB` | no | donor-side stream pacing; 0 = unlimited; `marekvs_bootstrap_bytes_sent_total` |
| join gate timeout | 0 = wait forever | env `MAREKVS_JOIN_TIMEOUT_SECS` | no | operator escape hatch: forces Active with incomplete bootstrap (loud log + `marekvs_join_gate_timeouts_total`); gate progress visible via `marekvs_join_gate_pending_pids` |
| disk high/low water | 90 % / 85 % | env `MAREKVS_DISK_HIGH_WATER_PCT` / `MAREKVS_DISK_LOW_WATER_PCT` | no | client writes (incl. DEL/EXPIRE/FLUSHALL — LSM deletes grow disk) get MISCONF at high-water; peer replication/AE/bootstrap and the REPLICAOF apply session are exempt; `marekvs_disk_write_stopped` |
| disk min-avail floor | 1024 MiB | env `MAREKVS_DISK_MIN_AVAIL_MB` | no | write stop engages only when used% ≥ high-water AND available < floor (shared-fs false positives); releases at low-water or 2× the floor; statvfs + ondaDB DbStats polled every 2 s (`marekvs_disk_total_bytes` / `_avail_bytes`, `marekvs_db_total_bytes`) |
| cold_purge_delay | — | design (15 m) | — | unimplemented; data kept after losing ownership (feeds stranded-record AE) |
| terminationGracePeriodSeconds | 60 | manifest (k8s/statefulset.yaml) | k8s edit | drain typically completes in ~3 s |
| listen addresses | :6379 / :7373 / :7946 / :9121 | env `MAREKVS_{RESP,MESH,GOSSIP,METRICS}_ADDR` | no | RESP / mesh / gossip(UDP) / metrics+probes |
| node identity | hostname ordinal, else 0 | env `MAREKVS_NODE_ID` | no | `marekvs-3` → 3; StatefulSet needs no per-pod config |
| data dir | `.data/n0` | env `MAREKVS_DATA_DIR` | no | |
| seeds | empty | env `MAREKVS_SEEDS` | no | chitchat re-resolves DNS names continuously |
| advertise IP | 127.0.0.1 | env `MAREKVS_ADVERTISE_IP` | no | `auto` = self-detect the pod IP |
| cluster name | `marekvs` | env `MAREKVS_CLUSTER` | no | gossip cluster isolation |
| requirepass | off | env `MAREKVS_REQUIREPASS` | **yes** — `CONFIG SET requirepass` | new connections need the new password; authenticated sessions stay (Redis semantics) |
| upstream Redis master | none | env `MAREKVS_REPLICAOF` | **yes** — `REPLICAOF`/`SLAVEOF` cmd | live-migration ingest (design/03); node stays writable |
| script time limit | 20 ms | env `MAREKVS_SCRIPT_TIME_LIMIT_MS` | **yes** — `CONFIG SET lua-time-limit` | alias `busy-reply-threshold`; applies from the next EVAL |
| Lua allocator limit | 16 MiB | const (marekvs-engine) | no | per script VM |
| blocking-list poll | 50 ms | const (marekvs-engine `POLL_MS`) | no | BLPOP/BRPOP wakeup granularity |
| ondaDB sync_mode | Interval, 128 ms | ondadb default | no | durability window per node |
| log level | `info,chitchat=warn` | env `RUST_LOG` | **yes** — `CONFIG SET loglevel` | Redis levels map to tracing (`debug`→trace, `verbose`→debug, `notice`→info, `warning`→warn, `nothing`→off); any other value is a raw tracing filter spec |

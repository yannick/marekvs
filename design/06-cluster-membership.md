# 06 — Cluster Membership & Node Lifecycle

## Gossip

- **chitchat** (Quickwit's SWIM-flavored gossip crate) over UDP :7946.
  `gossip_interval = 500 ms`; phi-accrual failure detector marks a node `Down`
  after ~5 s.
- Seeds: the Kubernetes **headless service** DNS
  (`marekvs-headless.<ns>.svc`), resolved at boot and re-resolved whenever a
  node finds itself isolated. Kubernetes is used **only** for seeding — the
  membership source of truth is gossip, so the data plane never depends on the
  apiserver.
- **Persisted fallback seeds** (chaos finding): every node stores its
  peers' gossip addresses in its meta CF (`peers:last`, updated on every
  view change) and merges them into the seed list at boot. In environments
  with neither stable IPs nor DNS registration — Apple containers hand out
  a fresh IP on every restart — a revived node's configured seeds are all
  stale; the persisted survivor addresses (unchanged, since the survivors
  did not restart) let it rejoin, and gossip propagates its new address to
  everyone else. On k8s this is a no-op safety net behind DNS.
- Node KV payload gossiped: `state`, `mesh_addr` (ip:7373), `epoch`
  (incarnation, bumped every boot), `pubsub_summary` (channel list or bloom,
  versioned), `has_patterns`, `last_alive` heartbeat stamp.

**Node identity:** `NodeId: u16` = StatefulSet pod ordinal. Stable across
restarts; the PVC keeps the data, so most "crashes" resolve by cursor resume,
not re-replication. Epoch disambiguates a restarted incarnation from a stale
gossip echo.

## Node state machine

State is published in gossip; placement reads it.

```
             bootstrap done                 drain signal (preStop)
  Joining ────────────────────► Active ─────────────────────────► Leaving
     ▲ │                          ▲                                  │
     │ │ restart w/ data:         │ gossip re-sees node              │ all pids handed
     │ │ fast-path (below)        │ (same epoch rules)               │ off or grace
     │ ▼                          │                                  ▼ period expires
   (boot)                        Down ◄── failure detector          Left (process exits)
```

| State | In placement? | Serves clients? | Notes |
|---|---|---|---|
| Joining | no | no (readiness gate) | bootstrapping future-owned partitions |
| Active | yes | yes | steady state |
| Leaving | as source only (excluded from new top-N) | yes | draining; still a valid data source |
| Down | no | — | failure-detected; repair scheduled |
| Left | no | — | clean exit |

### Restart fast-path

<a name="restart-fast-path"></a>
On boot with existing data: if `meta` records a clean shutdown, the gossip view
shows ownership unchanged since `last_alive`, **and**
`now − last_alive ≤ gc_grace`, the node skips bootstrap: cursor-resume
(`ResumeFrom` to each peer) closes the gap, then `Active`. `last_alive` is the
`alive:last` meta heartbeat, written every `min(gc_grace/4, 30 s)` — only
while Active/Leaving, so a crash mid-rejoin keeps measuring downtime from the
pre-death timestamp. If `now − last_alive > gc_grace` → **pull-only until
synced** ([05](05-consistency-anti-entropy.md#tombstone-lifecycle--gc-safe-point)):
the node stays Joining (held by the join gate) and Merkle-syncs each
data-bearing home partition against its **pre-outage co-owner** — placement
computed with itself included — never a current-view owner, which can be an
empty crash-era owner whose want-set would destroy valid data. Stale extras
only the rejoiner holds are dropped when the co-owner requests them; all
other requesters are refused without deletion; `MerkleRootMatch{pid}` marks
each partition done. A sole survivor (no Active peers) stands down and
serves.

## Join / bootstrap

A joining node computes the partitions it will own once Active (placement
with itself included) and holds phase **Joining** — invisible to HRW,
`/ready` 503 — until every one of them is bootstrapped: the **join gate**.
(The old "simplified v1" join was a fixed 2 s sleep before `Active`; a
scale-up node went Active with an empty store, HRW immediately routed ~1/n
of partitions to it, and its reads *and* other nodes' read-throughs served
nils until AE.) Per partition:

```
NeedPid ── BootstrapReq{pid} ──► donor streams a shard-consistent closure
Streaming: BootstrapChunk × n    256-op chunks, lz4 bulk lane, paced at
                                 MAREKVS_BOOTSTRAP_RATE_MB (64 MiB/s, 0=∞)
Streaming ── BootstrapDone{pid} ──► pid leaves the gate's pending set
```

- Chunks apply through the normal **merge** path, one batch per shard
  closure (idempotent — a crashed bootstrap restarts safely; a concurrent
  client write to the joining node can't be clobbered).
- **Crash resume**: unfinished pids are persisted under the meta key
  `join:pending` and re-requested at boot even though locally non-empty.
- **Retries** use a capped per-pid backoff (5/10/20 s), deferred whenever
  *any* chunk applied since the last sweep (`BootstrapDone` counts as
  progress): a donor streams its request queue sequentially, so "no chunk
  yet" usually means "still queued", not "lost" — naive fixed-interval
  retries were measured re-streaming every partition ~6×.
- **Donors refuse** `BootstrapReq` for pids they don't own (a joiner on a
  partial early gossip view can pick the wrong donor; streaming an empty
  copy + Done would mark the pid bootstrapped forever) and dedup duplicate
  (peer, pid) streams within a 20 s window (9× stream amplification
  measured without it). No reply = the joiner's backoff re-requests from
  the right owner once views converge.
- `/metrics` and `/ready` serve **before** the gate — a long bootstrap is
  observable (`marekvs_join_gate_pending_pids`,
  `marekvs_bootstraps_completed_total`,
  `marekvs_bootstrap_bytes_sent_total`), not a black box.
- `MAREKVS_JOIN_TIMEOUT_SECS` (default 0 = wait forever) is an operator
  escape hatch: on expiry the node goes Active with incomplete bootstrap
  (loud log + `marekvs_join_gate_timeouts_total`). Waiting forever is the
  safe default — a node that cannot finish bootstrap must stay unready
  rather than serve empty reads.
- **Post-join residual**: writes that landed on donors during the join, and
  read-through races, heal within the ~15 s AE bound — documented AP
  staleness, not a gate condition.
- Preferred source: current `H1(pid)`; any home works.
- When **all** future-owned pids are done, the node flips
  `Joining → Active` in one gossip update — a single atomic ownership change
  per node, no per-partition placement churn. Previous owners that drop out of
  `owners(pid)` demote those partitions to cold.

## Planned leave (Kubernetes drain)

preStop hook → node sets `Leaving`:

1. New placement (excluding it from top-N) makes new owners bootstrap-pull
   from it — it is the cheapest source, having everything.
2. The node drains its replication backlog: exit waits (bounded by
   `terminationGracePeriodSeconds`) until every peer has **acked** — not
   merely been sent — everything up to the ring head. (`HandoffAck` is
   removed from the wire: a per-pid handoff ack added nothing over
   acked-drain plus AE, and was never consumed.)
3. When the backlog reaches zero — or the grace period expires — the node
   sets `Left` and exits. On expiry, any remaining pids fall back to
   crash-repair from surviving homes; no data is lost while N−1 other homes
   live.
4. Throughout `Leaving` the node still serves clients and replication
   (the Service deselects it via readiness only at the very end).

## Crash

1. Phi-accrual marks `Down` (~5 s); placement recomputes.
2. Each newly promoted owner schedules repair of its gained pids after
   `repair_delay = 30 s + jitter(pid_hash % 30 s)`. The floor absorbs quick
   pod restarts (kubelet restart is usually < 30 s; the returning node needs
   only cursor catch-up). Jitter spreads source load.
3. Repair = the bootstrap protocol against a surviving home, warm-started by
   Merkle diff when the new owner already holds cold data for the pid.
4. HRW scatters a dead node's ~P/n partitions evenly across survivors, so
   repair fan-in is inherently balanced; the 8-stream / 64 MiB/s caps bound
   the rest.

## Minimum node floor

`min_nodes = N = 3.` Below the floor placement returns < N distinct owners
(the top-N dedupes); the system **keeps serving** (AP) but some partitions run
under-replicated — one more failure risks data loss on them.

Signals, not stoppage:

- gauges `marekvs_underreplicated_partitions`, `marekvs_effective_rf_min`;
- `INFO replication` → `cluster_degraded:1`;
- readiness stays green (degraded ≠ down);
- a PodDisruptionBudget with `minAvailable: 3` blocks *voluntary* evictions
  below the floor ([07-kubernetes.md](07-kubernetes.md)).

## Membership-view divergence (the AP fine print)

Two nodes may briefly compute different `owners(pid)` / `H1(pid)` during
propagation (≤ a few gossip rounds ≈ 1–2 s):

- **Two H1s**: both fan out to subscribers → harmless duplicates (idempotent
  merges).
- **Zero H1s** (each thinks the other is it): subscriber pushes miss →
  repaired by AE; this path is what actually consumes the 15 s staleness
  bound.
- **Fetch misroute**: `Fetch` hits a non-owner → it answers from cold data or
  redirects to its own current H1 view; client-visible effect is one extra
  hop.

Churn tests target exactly these windows
([10-testing.md](10-testing.md#103-membership-churn--jepsen)).

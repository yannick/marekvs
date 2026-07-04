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
(`ResumeFrom` to each peer) closes the gap, then `Active`. If
`now − last_alive > gc_grace` → **pull-only until synced**
([05](05-consistency-anti-entropy.md#tombstone-lifecycle--gc-safe-point)).

## Join / bootstrap

A joining node computes the partitions it will own once Active
(placement with itself included), then per partition:

```
NeedPid ── BootstrapReq{pid} ──► source picks MVCC snapshot at seq S
                                 (ondaDB checkpoint / snapshot iterator)
Streaming: BootstrapChunk × n    4 MiB chunks, lz4, ≤ 64 MiB/s per node,
                                 ≤ 8 concurrent partition streams each way
Streaming ── BootstrapDone{S} ──► Tailing: ReplOps with seq > S from
                                  the source's ring (standard cursor)
Tailing ── lag < 1000 ops ──► Synced
```

- Chunks apply through the normal **merge** path (idempotent — a crashed
  bootstrap restarts safely; a concurrent client write to the joining node
  can't be clobbered).
- Preferred source: current `H1(pid)`; any home works.
- When **all** future-owned pids are `Synced`, the node flips
  `Joining → Active` in one gossip update — a single atomic ownership change
  per node, no per-partition placement churn. Previous owners that drop out of
  `owners(pid)` demote those partitions to cold.

## Planned leave (Kubernetes drain)

preStop hook → node sets `Leaving`:

1. New placement (excluding it from top-N) makes new owners bootstrap-pull
   from it — it is the cheapest source, having everything.
2. Each new owner sends `HandoffAck{pid}` once Synced.
3. When all owed pids are acked — or `terminationGracePeriodSeconds = 300`
   expires — the node sets `Left` and exits. On expiry, the remaining pids
   fall back to crash-repair from surviving homes; no data is lost while N−1
   other homes live.
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

---
title: Cluster membership
description: Gossip, failure detection, the node state machine, join/leave/crash handling, the minimum-node floor, and the AP fine print of a divergent membership view.
status: mixed
---

Membership is the source of truth for [placement](../replication/#topology):
every node computes `owners(pid)` from the same gossiped view. The view itself
is maintained by gossip, not by Kubernetes — the data plane never depends on the
apiserver. This page covers how nodes discover each other, how the cluster
decides a node is alive or dead, and what happens as nodes join, leave, and
crash.

Every tunable named here lives in the
[defaults table](../consistency/#defaults-table).

## Gossip

marekvs uses **chitchat** (Quickwit's SWIM-flavored gossip crate) over UDP
`:7946`:

- `gossip_interval = 500 ms`; a phi-accrual failure detector marks a node
  `Down` after ~5 s.
- **Seeds** are the Kubernetes headless-service DNS (`marekvs-headless.<ns>.svc`),
  resolved at boot and re-resolved whenever a node finds itself isolated.
  Kubernetes is used **only** for seeding.
- The gossiped node payload is `state`, `mesh_addr` (ip:7373), `epoch`
  (incarnation, bumped every boot), `pubsub_summary`, `has_patterns`, and a
  `last_alive` heartbeat stamp.

**Node identity** is `NodeId: u16` = the StatefulSet pod ordinal, stable across
restarts. The PVC keeps the data, so most "crashes" resolve by cursor resume,
not re-replication. The epoch disambiguates a restarted incarnation from a stale
gossip echo.

### Persisted fallback seeds

```note
**A chaos finding.** Every node stores its peers' gossip addresses in its meta
CF (`peers:last`, updated on every view change) and merges them into the seed
list at boot. Apple containers hand out a **fresh IP on every restart**, so a
revived node's configured seeds are all stale — but the persisted survivor
addresses are unchanged (the survivors did not restart), which lets the revived
node rejoin; gossip then propagates its new address to everyone else. On
Kubernetes this is a no-op safety net behind DNS.
```

## Node state machine

State is published in gossip; placement reads it.

```text
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
On boot with existing data, a node **skips bootstrap** if `meta` records a clean
shutdown, the gossip view shows ownership unchanged since `last_alive`, **and**
`now − last_alive ≤ gc_grace`. It then closes the gap with cursor-resume
(`ResumeFrom` to each peer) and goes `Active`. This is why a kubelet restart
usually costs only a catch-up, not a re-replication.

If `now − last_alive > gc_grace`, the node must instead come back **pull-only
until synced** — see the [tombstone GC safe
point](../consistency/#tombstone-lifecycle--gc-safe-point) (that rejoin rule is
itself a planned enforcement, flagged there).

## Join / bootstrap

A joining node computes the partitions it will own once Active (placement with
itself included), then per partition:

```text
NeedPid ── BootstrapReq{pid} ──► source picks MVCC snapshot at seq S
                                 (ondaDB checkpoint / snapshot iterator)
Streaming: BootstrapChunk × n    4 MiB chunks, lz4, ≤ 64 MiB/s per node,
                                 ≤ 8 concurrent partition streams each way
Streaming ── BootstrapDone{S} ──► Tailing: ReplOps with seq > S from
                                  the source's ring (standard cursor)
Tailing ── lag < 1000 ops ──► Synced
```

- Chunks apply through the normal **merge** path (idempotent — a crashed
  bootstrap restarts safely, and a concurrent client write to the joining node
  can't be clobbered).
- Preferred source is the current `H1(pid)`; any home works.
- When **all** future-owned pids are `Synced`, the node flips `Joining → Active`
  in one gossip update — a single atomic ownership change, no per-partition
  churn. Previous owners that drop out of `owners(pid)` demote those partitions
  to cold.

```note
The `64 MiB/s` rate cap and `8`-concurrent-stream limit above are the design
targets; the current bootstrap streams **256 ops/chunk, sequentially** — the
rate cap and stream parallelism are not yet implemented (see the
[defaults table](../consistency/#defaults-table)).
```

## Planned leave (Kubernetes drain)

A preStop hook (a `/drain` call) sets the node to `Leaving`:

1. New placement (excluding it from top-N) makes the new owners bootstrap-pull
   from it — it is the cheapest source, having everything.
2. Each new owner sends `HandoffAck{pid}` once Synced.
3. When all owed pids are acked — or `terminationGracePeriodSeconds` expires —
   the node sets `Left` and exits. On expiry, the remaining pids fall back to
   crash-repair from surviving homes; no data is lost while `N−1` other homes
   live.
4. Throughout `Leaving` the node still serves clients and replication; the
   Service deselects it via readiness only at the very end. Drain typically
   completes in ~3 s.

## Crash handling

1. Phi-accrual marks the node `Down` (~5 s); placement recomputes.
2. Each newly promoted owner schedules repair of its gained pids after
   `repair_delay = 30 s + jitter` (planned; today AE simply repairs on the next
   round). The delay is meant to absorb quick pod restarts — a returning node
   needs only cursor catch-up — and the jitter spreads source load.
3. Repair is the bootstrap protocol against a surviving home, warm-started by
   Merkle diff when the new owner already holds cold data for the pid.
4. HRW scatters a dead node's ~`P/n` partitions evenly across survivors, so
   repair fan-in is inherently balanced.

```planned
**Peer heartbeat / timeout (1 s / 3 s) is designed but unimplemented.** There is
no dedicated mesh liveness probe today. Peer liveness = **TCP disconnect** on the
ctl/bulk connections **plus gossip phi-accrual failure detection**. The designed
1 s heartbeat with a 3 s dead threshold would tighten interest-lease invalidation
and give faster mesh-level failure signals than the ~5 s gossip detector.
```

## Minimum node floor

`min_nodes = N = 3`. Below the floor, placement returns fewer than `N` distinct
owners (the top-N dedupes), and the system **keeps serving** (AP) — but some
partitions run under-replicated, so one more failure risks data loss on them.

These are signals, not stoppage:

- gauges `marekvs_underreplicated_partitions`, `marekvs_effective_rf_min`;
- `INFO replication` → `cluster_degraded:1`;
- readiness stays green (degraded ≠ down);
- a PodDisruptionBudget with `minAvailable: 3` blocks *voluntary* evictions
  below the floor.

## Membership-view divergence

<a name="divergence"></a>
Two nodes may briefly compute different `owners(pid)` / `H1(pid)` during
propagation (≤ a few gossip rounds, ≈ 1–2 s). This is the AP fine print, and
every case is self-healing:

| Case | Effect | Heal |
|---|---|---|
| **Two H1s** | both fan out to subscribers | harmless duplicates (idempotent merges) |
| **Zero H1s** (each thinks the other is it) | subscriber pushes miss | repaired by AE — this path is what actually consumes the 15 s [staleness bound](../consistency/#staleness-bound) |
| **Fetch misroute** | a `Fetch` hits a non-owner | it answers from cold data or redirects to its own current H1 view; one extra hop, client-visible only as latency |

The churn tests target exactly these windows.

## Where to go next

- How ownership drives the write path: [Replication](../replication/#topology).
- The bound that heals divergence: [Consistency & anti-entropy](../consistency/).
- Running it on Kubernetes: [Quickstart](../quickstart/).

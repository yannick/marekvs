# Production-grade assessment — what to build, what to skip

Scope: the open items from todo.md sections *Replication & consistency*,
*CRDT*, *Cluster/membership*, *k8s operator*, triaged for one goal: **the
most stable and reliable cluster system**, explicitly NOT Redis feature
parity. Honest = each verdict names the failure it prevents, or the reason
it's safe to skip.

## Tier 1 — implement before calling it production-grade

These close silent data-loss / wrong-read / resource-exhaustion holes. A
cluster without them *works* until the exact day it doesn't, and the failure
is invisible when it happens.

| # | Item | Failure it closes | Effort |
|---|---|---|---|
| 1 | **Join gate: no Active before bootstrap-complete** | Scale-up serves **empty reads cluster-wide**: a joining node goes Active after a fixed 2 s sleep (`main.rs:270-273`), HRW immediately routes ~1/n of partitions to it, and both its own reads *and* other nodes' read-throughs hit its empty store until AE fills it. Worst window of any item on this list. | M |
| 2 | **Replication flow control (use AckSeq)** | Verified in code: `pump_peers` advances the cursor, then `send_ctl` does `try_send` — a slow peer with a full writer queue **drops the batch after the cursor moved**. Writes silently demote to AE latency (up to 15 s) with only a debug-level trace. Advance the cursor on ack, bound the unacked window, re-send on reconnect. Also gives drain a real "peer applied it" signal (subsumes the HandoffAck item). | M |
| 3 | **gc_grace pull-only rejoin rule** | A node down/partitioned > 1 h rejoins and **resurrects deletes** whose tombstones were already purged elsewhere. Classic LSM-cluster bug (Cassandra's oldest rule). Persist `last_alive`; if exceeded, home partitions go pull-only until a full Merkle sync per partition. | M |
| 4 | **Peer heartbeat/timeout on the mesh (1 s / 3 s)** | A wedged-but-open TCP connection (conntrack blackhole — we *built the chaos harness that creates these*) stalls replication to that peer and serves interest reads stale up to the 60 s lease. Gossip phi-accrual detects dead *nodes*, not dead *connections*. `Ping`/`Pong` already exist in the proto; only the loop is missing. | S |
| 5 | **Interest-map hard cap** | Unbounded `HashMap` growth: any client scanning many unique keys through non-home nodes inflates the interest table without limit — an OOM you cause from a redis-cli. A hard cap + evict-oldest is enough; skip the whole-partition escalation optimization. | S |
| 6 | **Disk-usage gauge + write-stop guard** | Disk-full is *the* unrecoverable LSM failure: ondadb write errors wedge the node mid-compaction. A gauge (also the prerequisite for the operator's disk autoscale signal) plus refusing writes above a high-water mark turns a corruption scenario into a clean error. | S–M |

## Tier 2 — build next; needed at scale, not on day one

| # | Item | Why / when it bites | Effort |
|---|---|---|---|
| 7 | **Incremental/dirty AE digests + per-round cap** | Today every AE round recomputes bucket digests by **full partition scan** — the whole keyspace re-hashed every ~5 s. Invisible in chaos tests, linear cost in data size; at tens of GB it eats the I/O budget. The design's dirty-bucket marking was never implemented. | M |
| 8 | **Bootstrap rate limiting** | Unthrottled partition streaming saturates the donor's disk/network during scale events — exactly when you least want p99 collapse. A token bucket on the bulk lane; skip the 8-stream concurrency, sequential is fine. | S |
| 9 | **Cold purge after ownership loss** | Disk leak: every scale event strands data forever on ex-owners. Purge only after N consecutive clean stranded-AE rounds (the stranded-record AE exists precisely because this data can be the last copy — the gate matters more than the timer). | M |
| 10 | **Mesh peer GC** | Departed nodes are redialed forever; long-lived clusters accumulate dial loops and log spam. Drop the loop when the view says permanently gone. | S |
| 11 | **Zone-aware HRW** | HRW is topology-blind: with RF=2, both homes of a partition can land in one zone — pod spread constraints spread *pods*, not *partition replicas*. A zone outage then takes both copies. **Must-have if you deploy multi-zone; irrelevant single-zone.** | M–L |
| 12 | **HINCRBY → PN counter** | Same lost-increment bug INCR had before v1.1, still present for hash fields. The counter machinery exists; reuse it. (Leave INCRBYFLOAT as LWW — float PN counters are not associative; document instead.) | S–M |
| 13 | **List position node-salting** | The honest fix for concurrent-push collisions is a sequence CRDT (RGA) — a large project not worth it here. But embedding node-id bits in position allocation makes cross-node collisions structurally impossible for the common case at trivial cost. Do that; skip RGA. | S |
| 14 | **Operator: surface reconcile errors to CR status** | Today scrape failures, PVC-reclaim failures, and reconcile errors are warn-logs or discarded (`main.rs:82-102,245,283-313`). An operator whose failures are invisible is worse than no operator. | S |
| 15 | **Operator: health-gated rollouts** | `spec.image` change does a plain rolling update gated only on pod readiness — readiness says "serving", not "underreplicated == 0". Reuse the scale-down gate between pods. | M |
| 16 | **Operator: leader election** | Cheap insurance (kube Lease). Today the failure needs someone to scale the Deployment to 2 by hand — real but self-inflicted. Do it when touching the operator anyway. | S |

## Tier 3 — consciously skip (and why that's honest, not lazy)

- **Interest renew interval** — the current refresh-by-refetch is *correct*,
  just adds a periodic latency blip on interest reads. Perf polish.
- **repair_delay (30 s damping)** — its motivation (avoid re-replication on
  pod bounce) is already covered by PVC resume + cursor resume +
  content-aware digests. Dead design weight; remove from the docs instead.
- **ttl_skew_grace** — transient repair ping-pong around TTL deadlines under
  clock skew; no data is wrong, convergence still holds. Cosmetic traffic.
- **repl batch byte cap / 2 ms linger** — throughput tuning, benchmark-driven
  work, not reliability.
- **MVS.SESSION tokens / cross-connection RYW** — a *semantics feature*.
  The AP contract is documented; changing it is scope creep.
- **Zero-H1/dual-H1 divergence window** — inherent to gossip-membership AP;
  the pump already defers on unconverged views and AE bounds the damage at
  15 s. The "fix" is epoch-fenced views ≈ consensus — a different system.
  Accept, document, and add a metric for view-divergence duration.
- **Runtime REPLICAS_N** — the rolling-restart procedure works and the
  operator can automate it later. A live `CLUSTER SETRF` is a coordination
  feature with real risk for near-zero operational win.
- **Hot-key H1 offload** — real at extreme skew, invisible before it; wait
  for evidence from the ops/s + key-heat metrics.
- **RGA sequence CRDT, stream consumer groups, remaining Redis-compat
  commands** — feature parity, explicitly out of scope.
- **ORSWOT >255-remove cap** — astronomically unlikely; document. But DO
  spend one day extending `merge_laws` property tests with concurrent
  remove-race interleavings — cheap confidence on risky assumption 3.
- **kubectl scale subresource, Flux manifests, placeholder values** —
  deployment hygiene; batch into a release checklist, not engineering work.

## What stays risky even after all of Tier 1+2 (accepted, by design)

1. **AP semantics**: cross-connection read-your-writes does not exist;
   divergence windows up to the 15 s AE bound under partitions. This is the
   product, not a bug.
2. **ondaDB commit-hook contract** (exactly-once, commit order) is
   load-bearing and only regression-tested — marekvs cannot verify it at
   runtime.
3. **Membership divergence** can still produce brief zero-H1 windows; we
   bound and observe it, we don't eliminate it.
4. **Non-home stranded data on a >gc_grace rejoiner** (added with #3's
   implementation): while rejoining, such data is refused-but-kept
   (last-copy safety); after the node turns Active, stranded-AE offers
   resume and can still resurrect purged deletes from *non-home*
   partitions. The home-partition path — the one HRW actually reads — is
   fully closed; extending the extras-drop to non-home data would risk
   destroying validly-unshipped last copies.
5. **Sole-survivor rejoin is AP**: a node down >gc_grace that rejoins while
   every peer is merely *unreachable* (not gone) will conclude sole
   survivor after the settle window and serve its data, including
   potentially-resurrectable records. Inherent to gossip membership.
6. **Writes-during-join staleness**: writes committed on donors while a
   node bootstraps are healed by regular AE (≤ ~15 s) after it turns
   Active. An eager per-pid AE kick at Active was tried and reverted — the
   probe storm starved read-through fetches and *created* an empty-reads
   window (join_empty_reads measured 254/792 nils with it in place).

## Implementation status (2026-07-05)

All Tier-1 items (#1–#6) plus Tier-2 #7 (AE cost) and #8 (bootstrap rate
limit) are implemented on the branch stack
`mesh-heartbeat → repl-flow-control → join-gate → interest-cap →
disk-guard → gc-grace-rejoin → incremental-ae`, each with unit tests and a
chaos scenario (`blackhole_conn`, `backpressure_no_drop`,
`join_empty_reads`, `gc_grace_rejoin`, `interest_flood`, `disk_guard`)
verified fails-before / passes-after where the pre-fix behavior was
reproducible. One wire change (ReplBatch.last_seq, MerkleRootMatch,
HandoffAck removed) — upgrade the whole cluster together.

## Suggested order

1. **Join gate (#1)** and **flow control (#2)** first — both are silent
   wrong-data windows in the *happy path* of routine scaling and load.
2. **gc_grace rejoin (#3)** + **heartbeats (#4)** — the partition-recovery
   story. Chaos harness already has the faults to regression-test both
   (wipe_replace, partition, freeze).
3. **Caps and gauges (#5, #6)** — memory + disk exhaustion guards; small.
4. Tier 2 in roughly listed order; **#7 (AE cost)** before any large-data
   production deployment, **#11 (zones)** before any multi-zone one.

Every Tier 1 item is verifiable with the existing chaos harness — that's
the bar for "done": a scenario that fails before the fix and passes after.

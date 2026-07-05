---
title: Testing
description: The tiered test strategy — merge-law property tests, storage integration against real ondaDB, and an implemented Jepsen-style chaos harness — plus the bugs it caught.
status: mixed
---

marekvs earns its guarantees with tests, ordered by cost, each tier gating the
next. The cluster tiers exist mainly to attack the
[riskiest assumptions](../architecture/) behind the AP design. This page covers
what is implemented today — including a real Jepsen-style chaos suite and the six
production bugs it caught — and, clearly marked, the privileged fault injection
that is still a forward plan.

## Merge-law property tests

The convergence claim rests on the merge functions being **commutative,
associative, and idempotent** for every record type. The pure merge core
(`marekvs-core`, no I/O) is property-tested with proptest in
`crates/marekvs-core/tests/merge_laws.rs`.

- **Generators**: arbitrary interleavings of ops (SET/DEL/EXPIRE, SADD/SREM,
  HSET/HDEL, ZADD/ZREM, XADD/XDEL) from *k* simulated nodes with skewed HLCs.
- **Properties**: `merge(a,b) == merge(b,a)`; `merge(merge(a,b),c) ==
  merge(a,merge(b,c))`; `merge(a,a) == a`; applying any permutation of the same
  op multiset to any replica yields identical final state.
- **Targeted cases**: dot-covered add vs concurrent fresh add (ORSWOT-lite bias),
  whole-collection tombstone vs concurrent element add, expiry as implicit
  tombstone vs stale pre-expiry write, HLC clamp on a drifted remote.

Also property-tested: envelope and internal-key codecs round-trip, and the
score-index key order matches f64 order (including ±0, subnormals, infinities).

## Storage integration (against real ondaDB)

These run against a real ondaDB instance, not a mock.

- **Commit-hook contract** — hooks fire exactly once per committed batch, in
  publish order, with the full op list. Hammered with concurrent committers
  across shards, asserting the ring sees a gap-free, ordered seq stream. This
  test is the canary for ondaDB upgrades.
- Shard-thread RMW atomicity (INCR storms on one key); TTL sweeper vs lazy expiry
  vs compaction GC; prefix-scan boundary discipline (the iterator stops at prefix
  end); crash-restart WAL replay with `SyncMode::Interval` (bounded loss window,
  no corruption — `kill -9` loops).
- **RESP conformance** — the redis-py / ioredis protocol subsets plus a
  golden-file suite for RESP2/RESP3 framing (HELLO switching, map/set/push
  frames, downgrades).

## Membership churn & chaos (Jepsen-style)

```success Implemented
The chaos suite is a real bash harness under `tests/chaos/` (`just chaos-docker`
/ `just chaos-apple`). It ports the Jepsen acceptance algorithms directly
(`checker.clj` counter :828, set :324) rather than driving Clojure Jepsen. Since
v1.1's PN counters made counters exact, the harness now *asserts* increments
rather than merely documenting lost ones.
```

**History model.** Every op is recorded as acked / failed / indeterminate, with
single-writer logs per workload. Counter reads are windowed Jepsen-style:
`lower` (acked at read invoke) `<= value <= upper` (acked + indeterminate at
completion), checked mid-run and on the final converged read of every node. The
set checker classifies acked-but-absent = LOST, never-attempted-but-present =
PHANTOM, multiplicity > 1 = DUPLICATE (all fail); indeterminate adds that landed
are "recovered" (legal).

**Nemeses.**

- SIGKILL crash + revive (data preserved); SIGSTOP freeze/thaw (Jepsen
  hammer-time); graceful SIGTERM churn (the k8s rollout path).
- **True network partitions** on the docker backend — every node joins a *mesh*
  net (gossip/replication, advertised) and an *edge* net (client ports); a
  partition disconnects only the mesh, so clients keep writing to **both** sides
  of the split. This is a genuine split-brain, not a single-node isolation.
- wipe-replace (data destroyed → fresh bootstrap via anti-entropy); membership
  churn (a node joins mid-load, takes ownership, then leaves).

**Invariants after every scenario.** Jepsen counter/set acceptance on every node,
total convergence, and `marekvs_cluster_underreplicated_partitions` back to 0 —
the operator's scale-safety signal must recover from every fault.

Scenarios live in `tests/chaos/chaos_test.sh`: `crash_restart`,
`partition_divergence`, `partition_no_resurrect` (SREM/DEL on the majority side
while an island holds the record — resurrection is the classic AP bug),
`freeze_thaw`, `rolling_churn`, `wipe_replace`, `membership_churn`, and a **bank
test** (atomic same-hash-tag Lua transfers; the total must be conserved on every
node's final read through graceful churn).

### Bugs the suite found

Six real bugs, each tied to a specific design mechanism. These are the war
stories from the harness's first days of existence.

1. **ondaDB read-your-own-writes violation** (fixed in ondaDB `de50da9`).
   `visible_seq` advances gap-free, so during a concurrent commit a thread's own
   completed commit could sit above the watermark — a `get()` right after `put()`
   on the same thread returned the previous value. INCR built on the stale state
   and the PN merge silently swallowed the increment (~2–6 % of acked increments
   lost under load, no faults needed). Fix: a per-thread commit floor;
   ReadCommitted reads use `max(visible_seq, own_floor)`.
2. **Replication-ring seq reset on restart.** Consumers persist "applied up to S
   per origin" and resume with `ResumeFrom{S}`; a restarted origin re-numbered
   from 1, every stale cursor looked caught-up, and the pump shipped nothing
   until seqs passed S — every write the node accepted after restart stranded
   locally. Fix: a persisted high-water mark plus a restart jump.
3. **Owners-only anti-entropy blind spot.** SIGKILL destroys the in-memory ring's
   unshipped entries; the record survives in ondaDB on the origin, but Merkle AE
   ran only among owners — who agreed with each other — so the strand was
   permanent and `underreplicated_partitions` read 0 throughout. Fix: non-owned
   data-bearing pids join the Merkle exchange every few rounds, push-only
   (`no_backfill`) so a non-owner never accumulates partition data.
4. **Fixed-sleep drain race.** SIGTERM slept 3 s and exited regardless of the ring
   backlog; a write acked in the last moment left with the process. Fix: drain
   until all peer cursors reach the ring head (bounded).
5. **Ring misattribution under clock skew** (found by the debug-image clock-bump
   scenario). The commit hook attributed each ring entry to the record
   *envelope's* origin. A merged CRDT record (PN counter, HLL) keeps the version
   winner's origin — so once a node held a future-stamped counter from a
   clock-bumped peer, every subsequent *local* increment it committed was
   attributed to that peer, and the pump's `origin == self` home-push rule
   dropped them all. Replication stalled for the full skew (~100 s), losing
   hundreds of acked increments cluster-wide. Fix: attribute ring entries by the
   *commit context* (`store::set_apply_origin`), not the envelope.
6. **Version-only AE digest miss** (same scenario). The Merkle bucket digest and
   diff keyed on `(ikey, hlc)`. Two replicas can hold the *same* counter version
   (envelope version = symmetric max) with *different* slot sets, so the digests
   matched and anti-entropy — the backstop that should have caught bug 5 — never
   fired. Fix: content-aware digests and diffs (add a value hash);
   equal-version-different-content records now repair in both directions.

## Just recipes

| Recipe | What it runs |
|---|---|
| `just test` | the Rust unit + property + storage-integration suites |
| `just chaos-docker` | the full chaos suite on the Docker backend (true partitions) |
| `just chaos-apple` | the chaos suite on Apple containers (per-VM clocks) |
| `just chaos-debug` | grudge partitions + netem packet faults (Docker, privileged — see below) |
| `just chaos-clock` | clock bump/strobe scenarios (Apple, privileged — see below) |
| `just grudge-test` | pure unit self-test of the grudge topology builders (no cluster) |
| `just ci` | the gating suite; the privileged debug suites are **not** included |

## Debug-image faults (opt-in)

Three fault classes need tooling *inside* the node container — `iptables`, `tc`,
a settable clock — that the `FROM scratch` production image deliberately lacks. A
separate `Dockerfile.debug` (the *same* binary over alpine + iproute2/iptables/
coreutils) carries them, gated behind `CHAOS_DEBUG=1` with least-privilege caps
(`NET_ADMIN`, plus `SYS_TIME` on Apple). The production image is never touched and
never runs privileged.

```success Implemented and runnable today
The debug binary is built from a second final stage over the *same* build stage,
so the marekvs binary is byte-identical to production. These run now:

- **Grudge partitions + packet faults** — `just chaos-debug` (Docker). Jepsen's
  grudge topologies (`bridge`, `majorities-ring`) applied as symmetric iptables
  DROP rules on the mesh subnet — arbitrary split-brain shapes while both sides
  accept writes — plus `tc`-netem `slow_peer` (overrun the ring, force the
  gap → Merkle repair path; verified converging through 126 ring-gap warnings)
  and `lossy_writes` (loss + corruption under load). The topology builders in
  `tests/chaos/grudge.py` are pure and unit-tested via `just grudge-test`.
- **Clock faults** — `just chaos-clock` (Apple). Per-VM clocks make Apple the
  only place single-node skew is meaningful (a static-musl binary defeats
  libfaketime, and Docker shares the host VM's `CLOCK_REALTIME`). `clock_bump`
  and `clock_strobe` skew one node's wall clock; **bugs 5 and 6 above were caught
  here.** An offset check (`assert_skewed`) fails the run if the skew was a no-op,
  so the test can never pass vacuously.
```

```planned Not yet ported
A few Jepsen refinements remain future work: exact bridge/ring solutions for
N > 5 nodes, netem reorder/duplicate scenarios, and Jepsen's exponential
bump/strobe offset distributions (the suite uses fixed ±10 / ±100 s offsets and a
±4 s strobe). The phased roadmap lives in `tests/chaos/DEBUG-PLAN.md`.
```

```note Docker cannot skew a single clock, by construction
Every Docker container shares the host VM's wall clock, and time namespaces
virtualize only `CLOCK_MONOTONIC`, not the `CLOCK_REALTIME` the HLC reads. So
clock faults are Apple-only — this is stated, not worked around.
```

## Kubernetes chaos & continuous verification

Beyond the local harness, the plan runs Chaos Mesh (or Litmus) on a kind/k3s
cluster nightly: pod-kill under load, rolling restart with zero client errors on
the Service, a 3→9→3 scale soak, a wedged-connection pathology (iptables DROP on
an established ctl connection, asserting the 60 s lease timer as the staleness
ceiling), apiserver-outage survival, and disk-pressure backpressure. A per-node
staleness gauge (worst AE round age) turns the headline guarantee into a
monitored SLO, alerting at 2× the bound, and `cargo fuzz` targets the RESP parser
and peer-frame decoder — both consume untrusted bytes.

## Where to go next

- The guarantees these tests defend: [Consistency & anti-entropy](../consistency/).
- The numbers behind the point ops: [Performance](../performance/).
- The moving parts under test: [Architecture](../architecture/).

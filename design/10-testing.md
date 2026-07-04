# 10 — Testing Strategy

Ordered by cost; each tier gates the next. The cluster tiers exist mainly to
attack the [riskiest assumptions](00-overview.md#riskiest-assumptions-tracked-with-tests).

## 10.1 Merge-law property tests (proptest) <a name="101"></a>

The convergence claim rests on merge functions being **commutative,
associative, idempotent** for every record type. Property-test the pure merge
core (`marekvs-core`, no I/O):

- generators: arbitrary interleavings of ops (SET/DEL/EXPIRE, SADD/SREM,
  HSET/HDEL, ZADD/ZREM, XADD/XDEL) from k simulated nodes with skewed HLCs;
- properties: `merge(a,b) == merge(b,a)`; `merge(merge(a,b),c) ==
  merge(a,merge(b,c))`; `merge(a,a) == a`; applying any permutation of the
  same op multiset to any replica yields identical final state;
- targeted cases: dot-covered add vs concurrent fresh add (ORSWOT-lite bias —
  assumption 3), whole-collection tombstone vs concurrent element add, expiry
  as implicit tombstone vs stale pre-expiry write, HLC clamp on drifted
  remote.

Also: envelope + internal-key codecs round-trip; score-index key order matches
f64 order (including ±0, subnormals, infinities).

## 10.2 Storage integration (against real ondaDB) <a name="102"></a>

- **Commit-hook contract** (assumption 2): hooks fire exactly once per
  committed batch, in publish order, with the full op list — hammer with
  concurrent committers across shards and assert the ring sees a gap-free,
  ordered seq stream. This test is the canary for ondaDB upgrades.
- Shard-thread RMW atomicity (INCR storms on one key), TTL sweeper vs lazy
  expiry vs compaction GC, prefix-scan boundary discipline (iterator stops at
  prefix end), crash-restart WAL replay with `SyncMode::Interval` (bounded
  loss window, no corruption — kill -9 loops).
- RESP conformance: run the redis-py / ioredis test suites' protocol subsets +
  a golden-file suite for RESP2/RESP3 framing (HELLO switching, map/set/push
  frames, downgrades).

## 10.3 Membership churn & Jepsen <a name="103"></a>

Status: **implemented** as a bash harness (`tests/chaos/`, `just
chaos-docker` / `just chaos-apple`) that ports the Jepsen acceptance
algorithms directly (checker.clj: counter :828, set :324) rather than
driving Clojure Jepsen. The original plan said the counter workload
"documents (not asserts) lost increments" — v1.1's PN counters made
counters exact, so the harness now *asserts* them.

- **History model**: every op is acked / failed / indeterminate; single-
  writer logs per workload. Counter reads are windowed Jepsen-style:
  `lower` (acked at read invoke) `<= value <= upper` (acked+indeterminate
  at completion), checked mid-run and on the final converged read of every
  node. Set checker: acked-but-absent = LOST, never-attempted-but-present
  = PHANTOM, multiplicity > 1 = DUPLICATE (all fail); indeterminate adds
  that landed are "recovered" (legal).
- **Nemeses**: SIGKILL crash + revive (data preserved); SIGSTOP freeze/
  thaw (Jepsen hammer-time); graceful SIGTERM churn (the k8s rollout
  path); **true partitions** on the docker backend — every node joins a
  "mesh" net (gossip/replication, advertised) and an "edge" net (client
  ports), and a partition disconnects only the mesh, so clients write to
  BOTH sides of the split; wipe-replace (data destroyed → fresh bootstrap
  via AE); membership churn (node joins mid-load, takes ownership, leaves).
- **Clock faults**: the apple-container backend runs one lightweight VM
  per node with an independent clock — the environment that originally
  caught the missing HLC receive rule. Freeze/thaw doubles as a clock-jump
  fault from the process's perspective. (Jepsen's bump/strobe need
  clock_settime inside the node; FROM-scratch images have no exec, so
  deliberate skew injection is future work.)
- **Scenarios** (`tests/chaos/chaos_test.sh`): crash_restart,
  partition_divergence, partition_no_resurrect (SREM/DEL on the majority
  side while an island holds the record — resurrection is the classic AP
  bug), freeze_thaw, rolling_churn, wipe_replace, membership_churn, and a
  bank test (atomic same-hash-tag Lua transfers; total conserved on every
  node's final read through graceful churn).
- **Invariants after every scenario**: Jepsen counter/set acceptance on
  every node, total convergence, and
  `marekvs_cluster_underreplicated_partitions` back to 0 (the operator's
  scale-safety signal must recover from every fault).

### Bugs the suite found (first two days of existence)

1. **ondadb: read-your-own-writes violation** (fixed in ondadb
   `de50da9`). `visible_seq` advances gap-free, so during a concurrent
   commit a thread's own completed commit sat above the watermark — a
   get() right after put() on the same thread returned the previous
   value. INCR built on the stale state and the PN merge silently
   swallowed the increment (~2-6% of acked increments lost under load,
   no faults needed). Fix: per-thread commit floor;
   ReadCommitted reads use `max(visible_seq, own_floor)`.
2. **Ring seq space reset on restart.** Consumers persist "applied up to
   S per origin" and resume with ResumeFrom{S}; a restarted origin
   re-numbered from 1, every stale cursor looked caught-up, and the pump
   silently shipped nothing until seqs passed S — every write the node
   accepted after restart stranded locally. Fix: persisted high-water
   mark + restart jump.
3. **Owners-only AE blind spot.** SIGKILL destroys the in-memory ring's
   unshipped entries; the record survives in ondadb on the origin, but
   Merkle AE ran only among owners — who AGREED with each other — so the
   strand was permanent and `underreplicated_partitions` read 0
   throughout. Fix: non-owned data-bearing pids join the Merkle exchange
   every few rounds, push-only (`no_backfill`) so a non-owner never
   accumulates partition data.
4. **Fixed-sleep drain.** SIGTERM slept 3 s and exited regardless of the
   ring backlog; a write acked in the last moment left with the process.
   Fix: drain until all peer cursors reach the ring head (bounded).
5. **Ring misattribution under clock skew** (found by the debug-image
   clock-bump scenario). The commit hook attributed each ring entry to the
   record ENVELOPE's origin. A merged CRDT record (PN counter, HLL) keeps
   the version *winner's* origin — so once a node held a future-stamped
   counter from a clock-bumped peer, every subsequent LOCAL increment it
   committed was attributed to that peer, and the pump's `origin == self`
   home-push rule dropped them all. Replication to a peer stalled for the
   full skew (~100 s), losing 100s of acked increments cluster-wide. Fix:
   attribute ring entries by the *commit context* (which batch is being
   applied on the shard thread; `store::set_apply_origin`), not the
   envelope.
6. **Version-only AE digests miss CRDT divergence** (same scenario). The
   Merkle bucket digest and diff keyed on (ikey, hlc). Two replicas can
   hold the SAME counter version (envelope version = symmetric max) with
   DIFFERENT slot sets, so the digests matched and anti-entropy — the
   backstop that should have caught bug 5 — never fired. Fix: digests and
   diffs are content-aware (add a value hash); equal-version-different-
   content records repair in both directions.

### Debug-image faults (privileged, opt-in)

Three faults need tooling inside the node container — `iptables`, `tc`,
a settable clock — that the `FROM scratch` production image omits. A
separate `Dockerfile.debug` (same binary over alpine + iproute2/iptables/
coreutils) carries them; `CHAOS_DEBUG=1` selects it and adds `NET_ADMIN`
(+ `CAP_SYS_TIME` on apple). Plan: `tests/chaos/DEBUG-PLAN.md`.

- **Grudge partitions** (`just chaos-debug`, docker): Jepsen bridge and
  majorities-ring topologies (`tests/chaos/grudge.py`, self-tested pure
  builders) applied as symmetric iptables DROP rules on the mesh subnet —
  arbitrary split-brain shapes, not just single-node isolation.
- **tc-netem packet faults**: `slow_peer` delays one node's mesh nic hard
  enough to overrun the replication ring and force the gap→AE repair path
  (verified: 126 ring-gap warnings, data still converged); `lossy_writes`
  runs 25 % loss + 5 % corruption under load.
- **Clock bump/strobe** (`just chaos-clock`, apple): per-VM clocks let one
  node's wall clock be skewed with `date -s` (CAP_SYS_TIME). Bugs 5 and 6
  came from here. `assert_skewed` fails the run if the skew was a no-op, so
  the test can never pass vacuously. Docker can't do single-node skew (one
  shared VM clock; time namespaces virtualize only CLOCK_MONOTONIC), so
  clock faults are apple-only by construction.

Still not ported: bridge/ring for N>5 exact solutions, netem
reorder/duplicate scenarios, and Jepsen's exponential bump/strobe offset
distributions (we use fixed ±10/±100 s and a ±4 s strobe).

## 10.4 Kubernetes chaos <a name="104"></a>

Chaos Mesh (or Litmus) on a kind/k3s cluster in CI-nightly:

- pod-kill under load (crash repair + PDB floor), rolling restart
  (drain/handoff, zero client errors expected on the Service),
  scale 3→9→3 soak;
- **wedged-connection pathology** (assumption 4): iptables DROP (not RST) on
  an established ctl connection — assert staleness ceiling is the 60 s lease
  timer, alert fires, and heartbeat timeout recovers within 3 s once traffic
  resumes;
- apiserver outage: cluster keeps serving (gossip-only dependence), new pods
  simply can't seed until DNS returns;
- disk pressure: cold-partition purge kicks in; ondaDB `Busy`/stall behavior
  surfaces as backpressure, not errors.

## 10.5 Continuous verification

- Staleness gauge exported per node (worst AE round age) — alert at 2× bound;
  this turns the headline guarantee into a monitored SLO rather than a
  design-doc promise.
- Nightly bench regression suite ([09-performance.md](09-performance.md#benchmark-plan)).
- Fuzzing: `cargo fuzz` targets for the RESP parser and the peer-frame decoder
  (both consume untrusted bytes).

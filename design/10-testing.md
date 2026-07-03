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

A Jepsen checkout exists at `../jepsen`; write a marekvs test harness
(Clojure) with:

- **Workloads**: register (per-key LWW linearizability is *expected to fail* —
  the checker instead validates **eventual convergence within the staleness
  bound** and session guarantees per connection: read-your-writes, monotonic
  reads); OR-set workload (concurrent SADD/SREM across nodes → final member
  set matches the ORSWOT-lite model checker); counter workload documents
  (not asserts) lost increments.
- **Nemeses**: network partitions (majority/minority/bridge), partition +
  heal, node kill/restart (PVC preserved), node kill + data wipe (bootstrap
  path), clock skew ±(1 s, 10 s) (HLC clamp), **membership churn** — rolling
  join/leave during load (assumption 1: dual-H1 / zero-H1 windows), slow-peer
  (tc netem) to force ring overrun → dirty-pair → Merkle repair.
- **Invariants checked after heal**: all homes byte-identical per partition
  within 15 s (the published bound — measured, not assumed); no tombstone
  resurrection after `gc_grace` scenarios (down node rejoins pull-only); no
  duplicate/lost stream ids.

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

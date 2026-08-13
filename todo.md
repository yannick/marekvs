# marekvs — unimplemented / deferred work

Deep scan of code + design docs (2026-07-05). Sources: literal code markers,
stubbed/no-op handlers, `design/*.md` "future work"/"v1.1"/"risky assumption"
callouts, and the design-target rows of the
[defaults table](design/05-consistency-anti-entropy.md#defaults-table).
The codebase has **zero literal TODO/FIXME comments** — everything below is
implicit (design-promised but absent, or stub/no-op in code).

## Replication & consistency (design targets from the defaults table)

- [x] **Per-peer flow control / unacked window (4 MiB)** — implemented
      2026-07-05: per-peer `PeerFlow{sent, acked, inflight}`; `AckSeq`
      (= new `ReplBatch.last_seq`) drains a 4 MiB window
      (`MAREKVS_REPL_WINDOW_BYTES`); window-full stalls only that peer's
      lane (`marekvs_repl_window_stalls_total`); the ring is the retransmit
      buffer; SIGTERM drain now means shipped AND acked.
- [x] **Peer heartbeat / timeout (1 s / 3 s)** — implemented 2026-07-05:
      every ctl+bulk connection pings each `MAREKVS_MESH_PING_INTERVAL_MS`
      and closes after `MAREKVS_MESH_IDLE_TIMEOUT_MS` without inbound bytes
      (`marekvs_mesh_conn_timeouts_total`); wedged-connection staleness is
      now bounded at ~3 s detection instead of the 60 s lease
      (risky assumption 4, `design/00-overview.md`).
- [ ] **Interest renew interval (15 s)** — `InterestRenew` is defined and
      handled (`crates/marekvs-repl/src/lib.rs:604`) but never *sent*;
      leases currently refresh by re-fetch on expiry.
- [ ] **interest_escalate (4096 keys/pid → whole-partition sub)** — still
      unimplemented.
- [x] **interest_max_entries (1 M)** — implemented 2026-07-05 as a hard cap
      (`MAREKVS_INTEREST_MAX_ENTRIES`): reject-at-cap (refresh always
      allowed), rejected registrations degrade to worst-case-lease (60 s)
      staleness; `marekvs_interest_entries` / `_rejected_total`.
- [x] **pull-only-until-synced rejoin rule for `gc_grace`** — enforced
      2026-07-05 (`design/05` §Tombstone lifecycle, `design/06`): gc_grace
      env-tunable (`MAREKVS_GC_GRACE_SECS`); a node down longer stays
      Joining and Merkle-syncs each home partition against its pre-outage
      CO-OWNER, dropping stale extras instead of serving them
      (`marekvs_rejoin_active`, `marekvs_rejoin_dropped_records_total`;
      chaos scenario `gc_grace_rejoin`).
- [ ] **ttl_skew_grace (5 s)** — expiry is materialized by the sweep as an
      ordinary tombstone; the digest-exclusion grace around the deadline is
      unimplemented (skewed replicas may ping-pong repairs briefly).
- [ ] **repair_delay (30 s + jitter)** — AE repairs fire on the next round;
      no damping to absorb quick pod restarts.
- [x] **ae_partitions_per_round cap** — implemented 2026-07-05:
      `MAREKVS_AE_PARTITIONS_PER_ROUND` (0 = all, rotating per-round probe
      cursor); per-pid Merkle roots are also cached (recomputed only when
      dirty or on a 10-min TTL — `marekvs_ae_digest_scans_total`), so
      quiescent partitions cost no scan either way.
- [ ] **repl batch 2 ms linger** — batches are pumped on notify or a 50 ms
      tick; the byte cap now exists (1 MiB payload, 2026-07-05 — oversized
      frames previously failed encode silently) but the linger does not.
- [ ] **Bootstrap concurrency (8 streams)** — streaming is still sequential
      256-op chunks per donor; the **rate cap is done** 2026-07-05
      (`MAREKVS_BOOTSTRAP_RATE_MB`, 64 MiB/s, 0 = unlimited). Chunking of
      `FetchCollectionResp` also still flagged simple-v1
      (`crates/marekvs-proto/src/lib.rs:78`).
- [x] **cold_purge_delay (15 m)** — implemented (T2-9): a partition this node
      no longer owns is dropped locally after `MAREKVS_COLD_PURGE_SECS`, but
      only once >=3 stranded-AE exchanges returned MerkleRootMatch, the view
      shows a full Active owner set, and no rejoin is active. Deletes are
      local-only (hook suppressed) so they never replicate.
- [x] **HandoffAck** — resolved 2026-07-05 by removing it from the wire
      (it was never consumed): planned leave now drains the ring until
      every peer has *acked* the head, grace-expiry still falls back to
      crash repair (`design/06-cluster-membership.md`). Wire break —
      whole-cluster upgrade, no mixed-version mesh.
- [x] **Mesh peer GC** — implemented (T2-10): a node absent from the view for
      `MAREKVS_MESH_PEER_GC_SECS` (5 m) has its dial loops torn down and its
      flow/interest state dropped; a returning node re-dials and re-inits via
      ResumeFrom.
- [ ] **MVS.SESSION HLC watermark tokens** for cross-connection
      read-your-writes (`design/04-replication.md:206`, v1.1 optional).

## CRDT / data-model semantic gaps (documented, unproven or lossy)

- [x] **List position collisions**: concurrent cross-node pushes could land on
      the same position → one push lost. — fixed (T2-13): positions are salted
      with the node id on a 1024-wide stride, so both pushes survive. Requires
      `MAREKVS_NODE_ID < 1024`, enforced at boot. True sequence CRDT (RGA)
      remains future work (`design/02-data-model.md`).
- [ ] **HINCRBY / INCRBYFLOAT stay LWW** — no PN-counter semantics for hash
      fields or floats (`design/02:299,302`). **T2-12 attempted; the written
      plan is incomplete.** Two findings from the attempt:
      1. The envelope's rtype field is 3 bits and all 8 values are taken, so
         `CounterField` needs the field widened to bits 2..5. That widening is
         backwards-compatible for reads (bit 5 was always 0) but not forwards —
         hence the feature gate, which is why P0 landed first.
      2. The plan's step 3 ("merge_values dispatches on rtype → counter merge")
         is NOT sufficient. Hash fields are OR-elements: the payload is an
         `ElementState` and `element_value()` returns `live.first()` — a single
         dot. Two concurrent HINCRBYs on different nodes produce two live dots,
         both survive the OR merge, and the reader shows only one — the
         lost-increment bug relocates from LWW to dot-selection rather than
         being fixed. Routing straight to `merge_counters` instead would lose
         remove-observation and break HDEL.
      The design needs to say how counter state folds across live dots (fold at
      read, or collapse on merge) and how repeated same-node increments cover
      their own prior dot. Prerequisite P0 is done.
- [ ] **ORSWOT-lite add-wins races** "believed acceptable" but unproven
      (risky assumption 3, `design/00:136`); >255-way concurrent remove
      history can resurrect a stale add (`design/02:144`).
- [ ] **Stream consumer-group state is LWW by design and the commands are
      absent**: XGROUP/XREADGROUP/XACK/XPENDING/XCLAIM not in dispatch
      (`crates/marekvs-engine/src/cmd/mod.rs:180-186` has only raw entry
      ops; `crates/marekvs-engine/src/cmd/stream.rs:7`).

## Redis compat (stubs / explicit unsupported)

- [ ] **Wire missing v1/v1.1 commands into dispatch** — design/03 lists these
      as v1 (or v1.1) but they have NO handler in
      `crates/marekvs-engine/src/cmd/mod.rs` (→ "ERR unknown command"):
      - zset v1: ZRANDMEMBER, ZREMRANGEBYRANK, ZREMRANGEBYLEX, ZMPOP,
        ZRANGESTORE, ZLEXCOUNT; v1.1: ZUNION/ZINTER/ZDIFF (+STORE/CARD),
        BZPOPMIN/BZPOPMAX/BZMPOP (design/03:72-75)
      - list v1: LMPOP; planned: BLMPOP (design/03:85,87)
      - stream v1: XSETID, XINFO STREAM; v1.1: XAUTOCLAIM (design/03:102)
      - generic v1: COPY, OBJECT ENCODING/REFCOUNT/IDLETIME/FREQ (promised
        as static/stub answers — no handler at all); v1.1: DUMP, RESTORE,
        MOVE, SORT (design/03:38-40)
      - set v1.1: SADDEX (design/03:62)
- [ ] **DEBUG is a silent no-op stub** for every subcommand except
      COUNTERSTATE/SLEEP — DEBUG OBJECT (design/03:142, v1) returns OK with
      no data (`crates/marekvs-engine/src/cmd/server.rs` `debug()`
      fallthrough).
- [ ] WATCH → error (no CAS in AP) — `crates/marekvs-engine/src/lib.rs:382`.
- [ ] ZRANGEBYLEX / BYLEX → "not supported" — `cmd/zset.rs:541`.
- [ ] WAIT, FAILOVER, CLUSTER *, FUNCTION, ACL beyond AUTH → absent
      (`design/03-redis-api.md:145`).
- [ ] SLOWLOG, LATENCY, MEMORY USAGE/STATS — listed v1.1, not in dispatch.
- [ ] CLIENT NO-EVICT/NO-TOUCH/SETINFO are accepted no-ops
      (`cmd/server.rs:152`); unknown CONFIG SET keys accepted-and-ignored
      by design (`cmd/server.rs:318`).
- [ ] TYPE reports HLL keys as `string` but GET → WRONGTYPE (documented
      divergence, `design/02:205`).

## Cluster / membership

- [x] **Join sequence is "simplified v1"** — replaced 2026-07-05 by the
      join gate (`design/06` §Join / bootstrap): a node stays Joining until
      every future-owned pid is bootstrapped (crash-resume via
      `join:pending`, progress-gated retries, donor-side refusal + dedup);
      `MAREKVS_JOIN_TIMEOUT_SECS` (0 = wait forever) is the operator escape
      hatch; `/metrics` and `/ready` observable while Joining
      (`marekvs_join_gate_pending_pids`; chaos scenario
      `join_empty_reads`).
- [ ] **Zero-H1 / dual-H1 window during view divergence** consumes the full
      15 s bound; only healed by AE (risky assumption 1, `design/00:130`,
      `design/06:129`).
- [ ] **Below-floor operation** (< REPLICAS_N+1 nodes) runs under-replicated
      with no spare (`design/06:116`) — operator gates this, plain
      manifests don't.
- [ ] **Runtime REPLICAS_N change** — needs a coordinated cluster-wide
      epoch-gossiped change (CLUSTER SETRF-style); today: rolling restart +
      full AE cycle (`k8s/README.md` caveats).
- [ ] **Hot-key H1 offload** — a single mega-hot key lands on one H1
      (risky assumption 5, `design/00:143`, `design/09:77`).
- [ ] **Zone-aware HRW placement (T2-11)** — topology-blind v1; appears in
      three docs (`design/07:115`, `design/09:77`, `design/12:147`). One epic,
      and the plan sequences it last: it needs `Member.zone` gossiped as
      chitchat KV, a zone-spread greedy pick over HRW-sorted candidates, and
      **both** `View::with_tables` and `Cluster::future_owned_pids` routed
      through the same zone-aware path — they compute ownership separately
      today, so changing only one would make the join gate and the placement
      tables disagree. Gate on `MAREKVS_ZONE_AWARE` with a regression-freeze
      test (unset ⇒ byte-identical placement). Only worth building if you will
      actually deploy multi-zone.

## Operator / k8s (ops)

- [x] **Leader election** — implemented (T2-16): coordination.k8s.io Lease
      `marekvs-operator-leader` (15 s duration / 10 s renew), only the holder
      runs the controller stream, loss of the lease exits the process.
- [ ] **Disk-fill autoscale signal** — the server-side prerequisite exists
      as of 2026-07-05 (`marekvs_db_total_bytes`,
      `marekvs_disk_total_bytes`/`_avail_bytes`, `marekvs_disk_write_stopped`
      + MISCONF write-stop at `MAREKVS_DISK_HIGH_WATER_PCT`); remaining work
      is the operator consuming it (`design/12:142`).
- [x] **Health-gated version rollouts** — implemented (T2-15): the controller
      walks `rollingUpdate.partition` down one ordinal at a time, gated on the
      same check as scale-down (all pods ready AND underreplicated == 0), so a
      rollout cannot open a single-copy window.
- [ ] **`kubectl scale` subresource** on the CRD (`design/12:148`,
      `k8s/operator/crd.yaml:189`).
- [x] **Silent operator error paths** — implemented (T2-14): status
      `conditions` (MetricsAvailable / ReconcileSucceeded / PvcReclaim /
      RolloutHealthy) with k8s lastTransitionTime semantics; scrape reports
      scraped/eligible and pod-list errors; PVC reclaim failures are reported
      and retried instead of discarded.
- [ ] **Flux ImagePolicy/ImageRepository manifests** are docs-only
      (`k8s/README.md:34-48`) — not shipped in `k8s/`.
- [ ] Placeholders requiring per-cluster edits: storage size + memory
      request (`k8s/README.md:263`), `storageClassName`
      (`statefulset.yaml:116`, `example-cluster.yaml:13`), operator RBAC
      namespace (`k8s/operator/rbac.yaml:46`).

## Performance (design/09 backlog)

- [ ] Zero-copy value pass-through for large GETs (v1.1, `design/09:35`).
- [ ] zstd per-level compression tuning (currently lz4 only, `design/09:53`).
- [ ] `unsafe-fastpath` feature (mmap reads + arena memtable): benchmark,
      ship a `-fast` variant only if ≥ 20 % (`design/09:59`).
- [x] ondaDB iterator construction is O(memtable); lazy k-way merge belongs
      in ondaDB (`design/09:93`). — landed in ondaDB (`LazyMemIter`), picked up
      by the 0.7.8 upgrade. marekvs also now uses `new_iterator_bounded`.
- [ ] Re-benchmark SPOP/ZPOPMIN on ondaDB 0.7.8 and decide whether the
      `pop_hints` pop-cursor workaround (`store.rs`) still earns its keep.
- [ ] Known bench gaps vs KeyDB: SPOP/ZPOPMIN ~0.15×, MSET ~0.10×
      (`design/09:129`) — measured pre-0.7.8, stale.
- [ ] `proto_crdt::oneof_race_converges_identically_both_orders` is **flaky at
      ~50 %** ("oneof winner depends on order"): the oneof tie-break is not
      order-independent. Pre-existing and unrelated to the storage engine —
      measured 4/8 failures on `defc648` against ondaDB 0.2.0, 5/8 on the same
      commit against 0.7.8, 4/8 on the 0.7.8 upgrade branch.
- [x] Commit-hook delivery is not seq-ordered (pre-existing; see
      `tests/commit_hook_contract.rs`). `Ring::read_after` assumed a sorted
      buffer, so an out-of-order op could be skipped and left to anti-entropy.
      — fixed: `Ring::push` now allocates its own monotonic seq under the
      buffer lock, so the ring is sorted by construction and no longer depends
      on ondaDB's hook ordering.
- [ ] LINSERT/LREM/LTRIM O(n) rebuilds (`design/02:261`).
- [ ] mimalloc vs jemalloc decision still open (`design/08:41`).
- [ ] Interest table exact-key memory (blooms rejected for now,
      `design/04:169`).

## Testing / CI

- [ ] **`net_dup` injector never written** (planned in
      `tests/chaos/DEBUG-PLAN.md:84`); **`net_reorder` written but no
      scenario calls it** (`tests/chaos/lib.sh:291`).
- [ ] Not ported from Jepsen: bridge/ring exact solutions for N>5, netem
      reorder/duplicate scenarios, exponential bump/strobe offset
      distributions (`design/10:147`).
- [ ] Debug scenarios (bridge_partition, majority_ring, slow_peer,
      lossy_writes, clock_bump_skew, clock_strobe) are opt-in — not in
      `just ci` (`tests/chaos/chaos_test.sh:21`); clock faults apple-only.
- [ ] Apple `settimeofday` fallback for `date -s` failures never
      implemented — `clock_bump` warns and continues
      (`tests/chaos/lib.sh:322`); `assert_skewed` guards against vacuous
      passes but injection remains flaky-tolerant.
- [ ] Kubernetes chaos (Chaos Mesh/Litmus on kind/k3s) planned as CI
      nightly (`design/10:151`).
- [ ] Continuous verification: staleness-gauge SLO alerting, nightly bench
      regression, cargo-fuzz targets for the RESP parser + peer-frame
      decoder (`design/10:167`).
- [ ] `partition_divergence` / `partition_no_resurrect` skipped on the
      apple backend (no runtime net detach) — docker-only coverage.

## Build / deploy

- [ ] ondaDB consumed as sibling path dependency with git fallback; wants a
      canonical remote/versioned release flow (`design/01:154`,
      `design/08:18`).
- [ ] Bench suite (bench/, uncommitted): validation + real run + commit
      still pending (see plan `create-a-design-in-gentle-corbato`).

## Explicit accepted risks (documented, revisit periodically)

- Read-after-write across connections unsupported by design
  (`design/00:46`); AP semantics during scale events (`k8s/README.md`).
- ondaDB commit-hook contract is load-bearing (risky assumption 2,
  `design/00:133`). Exactly-once and whole-batch delivery are now pinned by
  `crates/marekvs-engine/tests/commit_hook_contract.rs`; **commit-order
  delivery was measured false** and is tracked as an open item above. It
  remains a contract, not an invariant marekvs can check at runtime.
- Old list `'l'` blobs from pre-v1.1 are not read or migrated
  (`design/02:218`) — recreate lists after upgrade.

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
- [ ] **cold_purge_delay (15 m)** — data kept forever after losing
      ownership (currently *feeds* stranded-record AE; purge needs care).
- [x] **HandoffAck** — resolved 2026-07-05 by removing it from the wire
      (it was never consumed): planned leave now drains the ring until
      every peer has *acked* the head, grace-expiry still falls back to
      crash repair (`design/06-cluster-membership.md`). Wire break —
      whole-cluster upgrade, no mixed-version mesh.
- [ ] **Mesh peer GC** — disconnected peers are redialed until process exit;
      view-driven GC is future work (`crates/marekvs-repl/src/mesh.rs:175`).
- [ ] **MVS.SESSION HLC watermark tokens** for cross-connection
      read-your-writes (`design/04-replication.md:206`, v1.1 optional).

## CRDT / data-model semantic gaps (documented, unproven or lossy)

- [ ] **List position collisions**: concurrent cross-node pushes can land on
      the same position → one push lost; true sequence CRDT (RGA) is future
      work (`design/02-data-model.md:266`).
- [ ] **HINCRBY / INCRBYFLOAT stay LWW** — no PN-counter semantics for hash
      fields or floats (`design/02:299,302`).
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
- [ ] **Zone-aware HRW placement** — topology-blind v1; appears in three
      docs (`design/07:115`, `design/09:77`, `design/12:147`). One epic.

## Operator / k8s (ops)

- [ ] **Leader election** — controller must run 1 replica; two would fight
      over the same field manager (`design/12:130,141`,
      `k8s/operator/deployment.yaml:8`).
- [ ] **Disk-fill autoscale signal** — the server-side prerequisite exists
      as of 2026-07-05 (`marekvs_db_total_bytes`,
      `marekvs_disk_total_bytes`/`_avail_bytes`, `marekvs_disk_write_stopped`
      + MISCONF write-stop at `MAREKVS_DISK_HIGH_WATER_PCT`); remaining work
      is the operator consuming it (`design/12:142`).
- [ ] **Health-gated version rollouts** — `spec.image` change is a plain
      StatefulSet rolling update today (`design/12:144`).
- [ ] **`kubectl scale` subresource** on the CRD (`design/12:148`,
      `k8s/operator/crd.yaml:189`).
- [ ] **Silent operator error paths**: metrics scrape failures swallowed
      (`crates/marekvs-operator/src/main.rs:82-102`), PVC reclaim delete
      result discarded (`main.rs:245`), reconcile errors only warn-logged,
      never surfaced on CR status (`main.rs:283-313`).
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
- [ ] ondaDB iterator construction is O(memtable); lazy k-way merge belongs
      in ondaDB (`design/09:93`).
- [ ] Known bench gaps vs KeyDB: SPOP/ZPOPMIN ~0.15×, MSET ~0.10×
      (`design/09:129`).
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
- ondaDB commit-hook contract (exactly-once, commit order) is load-bearing
  (risky assumption 2, `design/00:133`) — regression-tested but a contract,
  not an invariant marekvs can check.
- Old list `'l'` blobs from pre-v1.1 are not read or migrated
  (`design/02:218`) — recreate lists after upgrade.

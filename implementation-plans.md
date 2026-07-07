# Implementation plans — Tier 1 & Tier 2 (production-assessment.md)

Each plan: problem → current code → design → steps → compatibility →
observability → test plan (chaos scenario = definition of done) → effort/risk.

**Cross-cutting fact that shaped every plan**: the mesh wire format is
postcard-encoded `PeerMsg` (`crates/marekvs-proto/src/lib.rs:167-192`) —
enum variant *indices* are the wire contract. Adding/reordering variants
breaks mixed-version clusters (old node → decode error → connection flap).
Deliberately, **every Tier 1 plan uses only existing messages**
(`Ping`/`Pong`, `AckSeq`, `ResumeFrom`, `Bootstrap*`, `no_backfill` all
already exist on the wire). One flag-day proto change is scheduled as P0
to make all *future* evolution rolling-upgrade-safe.

---

## P0 — proto v2: version negotiation (prerequisite, do first)

**Problem.** `Hello { node, kind }` carries no version. Any future enum
change (Tier 2 needs some) turns a rolling upgrade into connection-flap
chaos between mixed versions.

**Design.** One breaking change, now, while there are no production
deployments to break:
- `Hello { node, kind, proto: u16, features: u32 }` (features = bitset).
- Receiver stores the peer's `proto`/`features` on the `PeerHandle`;
  senders gate any post-v2 message kind on the peer's announced bits.
- Rule going forward: new `PeerMsg` variants are appended, never reordered;
  never sent unless the peer's feature bit is set.

**Steps.**
1. `crates/marekvs-proto/src/lib.rs`: extend `Hello`; add
   `pub const PROTO_VERSION: u16 = 2;` and a `Features` bitset newtype.
2. `crates/marekvs-repl/src/mesh.rs`: send extended Hello on dial/accept;
   record `(proto, features)` in the peers map; expose
   `Mesh::peer_features(node)`.
3. `crates/marekvs-repl/src/lib.rs:520` (`PeerMsg::Hello` no-op arm):
   store the capability info.

**Compatibility.** Flag-day: v2 nodes cannot mesh with v1 nodes (Hello body
layout changed). Documented as "restart the whole cluster once"; acceptable
pre-production. After this, upgrades are rolling.

**Test.** Unit: postcard round-trip of new Hello; integration: 2-node
cluster forms, features visible in logs. Effort: **S (½ day)**. Risk: low.

---

# Tier 1

## T1-1 — Join gate: no Active before bootstrap-complete

**Problem.** `main.rs:270-273` sleeps 2 s then unconditionally
`set_phase(Active)`. The moment phase=Active, `View::owner_candidates()`
(`cluster/lib.rs:106-112`) includes the node, HRW routes ~1/n of partitions
to it, and — because the read path serves *local* for homes
(`repl/lib.rs:898`) — the whole cluster returns empty reads for those keys
until AE backfills. Routine scale-up = temporary data loss from the
client's view.

**What already exists (use, don't rebuild):**
- Joining nodes are excluded from placement: `owner_candidates()` filters
  `phase.owns_data()` (Active|Leaving). While Joining we receive nothing
  and serve nothing as home — the gate just has to *hold* Joining until safe.
- `Cluster::future_owned_pids()` (`cluster/lib.rs:319-328`) computes
  exactly the pids we'll own once Active.
- Bootstrap machinery: `request_bootstraps()` (`repl/lib.rs:245-269`)
  already asks an existing owner per empty future-owned pid;
  `BootstrapDone { pid, as_of_seq }` already arrives (`repl/lib.rs:684`).
- k8s startup probe already budgets 6 min for exactly this
  (`k8s/statefulset.yaml:93` comment "up to 6 min for long bootstraps" —
  currently aspirational, this plan makes it true).

**Design.** Track per-pid bootstrap state on `Repl`; the server flips
Active only when every future-owned pid is `Ready`.

```rust
// repl/lib.rs
pub struct BootstrapTracker {
    // pid -> state; only pids that were EMPTY and have a live source count
    pending: Mutex<HashMap<Pid, BootstrapState>>,   // Requested{source, at} | Done
    gate_open: AtomicBool,                          // all clear at least once
}
```

Rules per future-owned pid, evaluated in `request_bootstraps()`:
1. Partition non-empty locally (PVC resume / restart) → `Ready`
   immediately — cursor resume + AE cover staleness, same as today.
2. Empty + no other owner exists (fresh cluster, n=1, or we're the first
   node) → `Ready` (there is nothing to fetch; matches today's behavior).
3. Empty + source available → `Requested{source, at=Instant}`; `Ready` on
   `BootstrapDone{pid}`.
4. Source disconnects or `at` older than 30 s without progress → re-request
   from the next owner (view may have changed); the view-watcher loop
   (`repl/lib.rs:220-240`) already re-runs `request_bootstraps` on every
   view change, so retry = clearing the stale `Requested` entry.

Server side (`main.rs`): replace the fixed sleep with:
```rust
// join gate: wait for view convergence, then for bootstrap completion
cluster.wait_first_view().await;                  // new: first non-empty view or 2s
repl.bootstrap_gate().await;                      // resolves when all future-owned pids Ready
cluster.set_phase(NodePhase::Active).await;
```
`bootstrap_gate()` = watch channel notified whenever a pid flips Ready;
also re-evaluated on view change (future_owned_pids can shrink/grow while
Joining — recompute the pending set each evaluation, don't freeze it).

**Edge cases.**
- **Fresh single node / fresh cluster**: rule 2 makes the gate instant —
  no regression for `just run` and tests.
- **All sources down**: node stays Joining (correct — going Active would
  serve empty); `/ready` stays 503; k8s startup probe eventually kills it
  → retry. Log loudly every 10 s with the stuck pid count.
- **Leaving nodes as sources**: acceptable (they own data until gone).
- **Ownership changes mid-join**: recompute; a pid no longer future-owned
  drops from pending.
- **The 2 s "give gossip a moment"**: subsumed by wait_first_view (first
  view rebuild with >1 member, or 2 s timeout for single-node).

**Observability.** Gauges `bootstrap_pending_partitions`,
`bootstrap_state` info in `/ready` 503 body; INFO cluster section line.

**Test plan.**
- Unit: tracker state transitions incl. re-request on source loss.
- Chaos (new scenario `join_gate`): 3-node cluster, write 5k keys, add
  node 3 (docker `node_run 3`), IMMEDIATELY hammer reads for all keys via
  node 3's edge address and via node 0 — **zero empty reads allowed**
  (today this fails); assert node 3 reaches Active and `pending==0`.
- Chaos: `join_gate_source_crash` — crash the bootstrap source mid-join;
  assert node re-requests from another owner and still converges; reads
  stay correct throughout.

**Effort: M (2–3 days).** Risk: deadlock-shaped bugs (gate never opens) —
mitigated by the stuck-log + startup-probe kill; rule-2 fallback keeps
single-node/dev flows instant.

## T1-2 — Replication flow control (consume AckSeq)

**Problem.** `pump_peers` (`repl/lib.rs:335-423`): cursor is advanced
*then* the batch goes out via `send_ctl` = `try_send`
(`mesh.rs:68-73`) — full writer queue (4096) ⇒ batch silently dropped,
cursor already moved, write demoted to AE (≤15 s) with a debug-level log.
Slow peer also means unbounded effective memory (queue of up-to-256-op
batches) before drops start.

**Key simplification: ops are idempotent.** Every applied op goes through
merge (`apply_op_from` → merge_values / LWW) — re-delivery is harmless.
So the protocol needs *at-least-once with a window*, not exactly-once:
no sequence-ack bookkeeping beyond two cursors.

**Design.** Per-peer cursor pair replaces the single cursor:

```rust
struct PeerCursor {
    sent: u64,       // last seq shipped (written to the writer queue)
    acked: u64,      // last seq the peer confirmed applied (AckSeq)
    inflight: u64,   // ops between acked..sent (derived, kept for cheap checks)
    last_ack_at: Instant,
}
const MAX_INFLIGHT_OPS: u64 = 8_192;          // ~32 batches
```

pump_peers changes:
1. Read from `sent` as today, but stop when
   `sent - acked >= MAX_INFLIGHT_OPS` (window closed → skip peer this pump).
2. `send_ctl` result is now checked: on `false` (queue full / disconnected)
   **do not advance `sent`** — retry next pump. (This alone fixes the
   verified drop-after-advance bug.)
3. `PeerMsg::AckSeq { origin, seq }` handler (`repl/lib.rs:545`, currently
   `{}`): `acked = max(acked, seq)`, `last_ack_at = now`.
4. Stall handling: in the 50 ms pump tick, if `inflight > 0` and
   `last_ack_at > 10 s` ago → reset `sent = acked` (retransmit window;
   safe by idempotence) and log warn. Covers lost acks and half-dead
   connections (T1-4 will usually close them first).
5. Reconnect: `ResumeFrom { seq }` handler (`repl/lib.rs:544`) already sets
   the cursor — now sets both `sent = acked = seq`.
6. Ring overrun: `read_after` gap (`repl/lib.rs:348-351`) — unchanged
   semantics (AE backstop), but now ALSO bump a counter metric and set
   `sent = acked = last` (explicit skip, no repeat warn spam).

Receiver side already sends AckSeq after applying each batch
(`repl/lib.rs:539-544`) — zero changes, zero wire changes.

**Drain integration (subsumes HandoffAck item).**
`pending_backlog()` (`repl/lib.rs:428-437`) currently measures
`last_seq - cursor` where cursor meant "written to socket". Change it to
use `acked` — SIGTERM drain now waits for **peer-applied**, not
peer-buffered. `PeerMsg::HandoffAck` stays ignored (dead variant; do not
remove — wire indices).

**Backpressure endgame.** When a peer's window stays closed, the ring keeps
filling; ring overrun evicts oldest → gap → AE for that peer. That is the
correct AP behavior (never block local writes on a slow peer) — the
difference from today: it happens *visibly* (metrics below) and only after
a real window, not silently on a queue blip.

**Observability.** Per-peer gauges: `repl_peer_inflight_ops{peer}`,
`repl_peer_ack_lag_seconds{peer}`; counters `repl_window_stalls_total`,
`repl_ring_gaps_total`, `repl_retransmits_total`.

**Test plan.**
- Unit: window arithmetic; sent-not-advanced on failed send; retransmit
  reset; ResumeFrom resets both.
- Chaos `slow_peer` (exists, debug image, tc-netem delay): assert with
  netem 500 ms delay that a counter workload loses **zero increments
  without an AE round** — i.e. `repl_ring_gaps_total == 0` and convergence
  happens at push latency, not AE latency. Today this passes only via AE.
- Chaos new `stalled_peer`: `freeze` (SIGSTOP) a node for 30 s under write
  load — assert sender's inflight caps at MAX_INFLIGHT_OPS, memory stable,
  retransmit fires after thaw, converges without AE gaps.

**Effort: M (2–3 days), pairs naturally with T1-4** (same files, same
failure domain). Risk: retransmit loops if acked regresses — guard with
`max()`; interest fan-out entries share the same batch path and are also
idempotent.

## T1-3 — gc_grace pull-only rejoin (delete-resurrection fence)

**Problem.** A node down/partitioned longer than `GC_GRACE` (1 h,
`store.rs:19`) holds records whose covering tombstones were purged
elsewhere (ondadb TTL purge). Its AE pushes resurrect deletes cluster-wide.
Documented in design/05 §Tombstone lifecycle; enforcement absent.

**Design.** Three parts: detect, fence, reconcile.

*Detect.* Persist `meta:last_alive` (u64 ms) every 30 s from the existing
ring-hw persist loop (`repl/lib.rs:299-320` — piggyback, zero new tasks).
On boot: `outage = now_ms - last_alive`. If `outage > GC_GRACE * 0.8`
(safety margin for clock slop) → enter **quarantine**: `Repl.quarantined:
Mutex<HashSet<Pid>>` initialized to all pids with local data
(scan: partition roots ≠ 0 — reuse `ae::partition_root`).

*Fence (prevents cluster-wide resurrection).* While `pid ∈ quarantined`:
- AE responder: when diffing (`diff_bucket` path, `repl/lib.rs:629-666`),
  never include local-only / local-newer records in `push` for that pid —
  i.e. force the `no_backfill` behavior in the *outbound* direction
  (`BucketKeys.no_backfill` already exists on the wire — set it on our
  outgoing side; receiving repairs stays enabled, that's the "pull").
- pump_peers: entries for quarantined pids with `origin == self` and
  `hlc < boot_hlc` never occur (ring is empty at boot) — **new client
  writes flow normally**: a fresh envelope with a post-boot HLC cannot be
  a resurrection candidate. No client-visible write impact.

*Reconcile (removes the local anomaly).* Local reads on a quarantined
node could still serve a resurrected value. During the first full Merkle
sync per pid (initiate proactively at boot, ≤ 64 pids in flight):
- Run the existing bucket exchange against a current home.
- For records that are **local-only** and `hlc < last_alive`: the peer
  purged the tombstone that covered them → write a local tombstone with
  `hlc = HLC(last_alive, 0)` (loses to any legitimately newer write,
  beats the stale record). This is the precise inverse of resurrection.
- Records local-only with `hlc ≥ last_alive` are post-outage writes → keep
  and (after unfencing) push normally.
- When a pid's sync round completes with zero diffs → remove from
  `quarantined`; persist progress in `meta:quarantine:<pid>` so a restart
  mid-reconcile doesn't reopen the hole.

**Edge cases.**
- First boot ever (no `last_alive`): not quarantined.
- Whole-cluster outage (all nodes exceed gc_grace): every node quarantines
  and none can be the "current home"… deadlock. Rule: if ALL owners of a
  pid are quarantined, the pid unfences after one *mutual* sync (they're
  equally stale; tombstone purge happened nowhere or everywhere — no
  asymmetry to fence). Detect via gossiped `quarantined=true` node state
  (chitchat KV — additive, gossip-compatible).
- Clock skew: `last_alive` compares wall clocks across restarts of the
  SAME node — immune to peer skew.

**Observability.** Gauge `quarantined_partitions`; log on enter/exit;
`/ready` stays 200 (node serves; only AE-push is fenced) but INFO shows
quarantine state.

**Test plan.**
- Unit: local-only + old ⇒ tombstoned; local-only + new ⇒ kept.
- Chaos `resurrection_fence` (docker, needs shortened grace — add
  `MAREKVS_GC_GRACE_MS` env override for tests): write k, DEL k everywhere,
  partition node 2 away, wait > grace (tombstone purged on 0/1 — force
  compaction or lower ondadb TTL slack in test), heal. Assert: k stays
  deleted on ALL nodes (today: k resurrects), node 2's counter of
  quarantine-tombstones > 0, quarantine exits.
- Chaos: whole-cluster stop > grace, restart all — assert mutual-sync
  unfence, no permanent quarantine.

**Effort: M–L (3–5 days; the reconcile pass is the bulk).** Risk: the
`hlc < last_alive` classification wrongly tombstones a record whose only
copy was local *and legitimate* (peer lost it) — mitigated because AE
would have replicated it before the outage (15 s bound ≪ 1 h grace);
document as accepted residual.

## T1-4 — Mesh heartbeats (1 s ping / 3 s timeout)

**Problem.** A wedged-open TCP connection (conntrack blackhole — our own
chaos `partition` creates these) is invisible: gossip phi-accrual watches
*nodes* (UDP), not this TCP connection. Consequences: repl to that peer
stalls silently until kernel timeouts (minutes–hours); interest leases
serve stale up to 60 s.

**Design.** Entirely inside `mesh.rs`; `Ping{nonce}`/`Pong{nonce}` already
exist on the wire (`proto/lib.rs:153-156`).

Per ctl connection (dialer AND acceptor side):
- Writer task: every 1 s, if nothing else was sent in the last 1 s, send
  `Ping{nonce: monotonic}` (piggyback rule keeps busy links ping-free).
- Reader task: track `last_rx: Instant` updated on ANY inbound frame.
  A `tokio::select!` timeout arm: if `last_rx > 3 s` → log warn, drop the
  connection (abort both halves).
- `Ping` handler: reply `Pong{nonce}` (currently `Pong` is ignored at the
  repl layer — keep that; `last_rx` update is the signal, RTT tracking is
  optional gravy).
- Connection drop → existing machinery takes over: reconnect loop with
  backoff (`mesh.rs:139-168`), `peer_events` fires disconnected →
  interest leases die (`repl/lib.rs:285-291`), `ResumeFrom` on reconnect
  → flow-control cursors reset (T1-2).

Bulk lane: **no heartbeat** (long silent periods are normal between
bootstraps; a wedged bulk lane only delays bootstrap/AE-repair, and the
ctl-lane detection will usually reap the same peer). Revisit if chaos
shows wedged-bulk problems.

**Constants.** `PING_INTERVAL=1 s`, `PEER_TIMEOUT=3 s` (design/05 table
values); env overrides `MAREKVS_PEER_TIMEOUT_MS` for chaos tuning.

**Observability.** Counter `mesh_peer_timeouts_total{peer}`; existing
`mesh_peers` gauge already reflects the drop.

**Test plan.**
- Chaos: extend `partition_divergence` — after `partition` (iptables DROP,
  which wedges established connections), assert BOTH sides log a peer
  timeout within 5 s and `mesh_peers` drops, *before* heal; today
  detection relies on TCP giving up.
- Chaos `lossy_writes` (exists): with 30 % loss, assert connections do
  NOT flap (3 s tolerates 1 s pings over lossy links; if flapping shows
  up, timeout to 5 s — note in scenario).
- Unit: piggyback suppression (busy connection sends no pings).

**Effort: S (1 day). Do in the same PR as T1-2** — shared files, and
flow-control stall handling assumes dead connections get reaped. Risk:
false-positive reaps under CPU starvation (the freeze/thaw chaos scenario
covers exactly this — a SIGSTOPped node's peers should reap it, and they
now will: that's correct, not a false positive).

## T1-5 — Interest-map hard cap

**Problem.** `InterestMap = HashMap<Pid, HashMap<key, HashMap<NodeId,
Instant>>>` (`repl/lib.rs:37`) grows per unique key read through a
non-home. No cap ⇒ a `redis-cli --scan` through one node OOMs its homes.
GC only removes *expired* leases (`gc_interest`, each AE round).

**Design (cap + refuse, no LRU).** LRU across a 3-deep nested map costs
more complexity than the feature is worth. At the cap, new registrations
are refused — the subscriber peer simply doesn't get pushes and re-fetches
on its own lease expiry (`leases` map on ITS side, `repl/lib.rs:907`) —
strictly correct, mildly slower, only for the keys past the cap.

1. `interest_entries: AtomicUsize` maintained at the (pid,key,node) leaf
   grain: increment on new leaf insert (`repl/lib.rs:796` and the
   `register_interest_ops` path), decrement in `gc_interest` and the
   disconnect sweep (`repl/lib.rs:285-291`).
2. `MAX = env MAREKVS_INTEREST_MAX (default 1_000_000)` (≈120 MB as
   designed). On insert when full: skip insert, bump
   `interest_evictions_total` (name it `interest_refusals_total`), debug
   log (rate-limited).
3. Renewal of an EXISTING leaf (same pid/key/node) always allowed —
   steady-state working sets keep working at the cap; only *new* keys are
   refused. This is why refuse beats evict: eviction would churn the hot
   set, refusal degrades only the overflow.

**Skip** (explicitly): whole-partition escalation (design/04's
interest_escalate) — optimization, not safety; revisit with evidence.

**Observability.** Gauge `interest_entries`, counter
`interest_refusals_total`.

**Test plan.** Unit: cap boundary (insert at MAX refused, renewal at MAX
allowed, GC frees capacity). Integration: set `MAREKVS_INTEREST_MAX=100`,
read 200 keys through a non-home, assert map ≤100, all 200 reads still
correct (values served), refusals counted.

**Effort: S (½–1 day).** Risk: counter drift between map and atomic —
add a debug assertion recomputing the true count in `gc_interest`.

## T1-6 — Disk-usage gauge + write-stop guard

**Problem.** No disk metrics at all; ondadb at 100 % disk fails writes
mid-compaction — the one unrecoverable LSM failure. Also the prerequisite
for the operator's disk autoscale signal (T2-16).

**Design.**
*Gauges* (sampled every 15 s by a task in `main.rs`, values pushed into
`engine.metrics`):
- `disk_total_bytes`, `disk_available_bytes` — `statvfs` on
  `MAREKVS_DATA_DIR` (nix crate or libc; no new heavy deps).
- `data_dir_bytes` — recursive size of the data dir (walk; at ondadb's
  file counts this is cheap; every 60 s, separate slower cadence).

*Write-stop*:
- `engine.read_only: AtomicBool` (new field, pattern like
  `script_time_limit_ms`). Sampler sets it when
  `available < max(MAREKVS_DISK_MIN_FREE_BYTES (default 1 GiB),
  5 % of total)`; clears with hysteresis at 2× the threshold.
- `Engine::dispatch` (engine/lib.rs, next to the NOAUTH gate at :322):
  if `read_only` and the command has the `write` flag
  (`command_docs::find(name)` flags, already exist — `CW`/`CWF` sets):
  reply `-MISCONF disk almost full, writes disabled (marekvs)` (Redis uses
  MISCONF for this class). DEL/EXPIRE allowed? **No exceptions** — a
  tombstone is also a write; simplicity and predictability win. Frees come
  from TTL purge + compaction, which continue.
- Replication ingest (apply_op) **stays enabled**: stopping it would
  desync the node while peers still count it as a home. The 5 % headroom
  is sized to absorb repl inflow while operators react — document this
  explicitly in the runbook: the guard buys time, it is not a fix.

**Observability.** The three gauges + `writes_rejected_disk_total` counter
+ INFO persistence section line `read_only:1`.

**Test plan.** Unit: flag gating in dispatch (write vs read commands).
Integration: set `MAREKVS_DISK_MIN_FREE_BYTES` above the actual free space
→ writes rejected with MISCONF, reads fine, gauge values sane; clear
threshold → writes resume. (Real disk-full chaos is a follow-up: a small
loopback/tmpfs volume in the docker harness — note in design/10.)

**Effort: S–M (1–2 days).** Risk: statvfs on unusual filesystems (bind
mounts report the host fs — fine, that IS the constraint that matters).

---

# Tier 2

## T2-7 — Incremental AE digests + per-round cap

**Problem.** Every AE round recomputes bucket digests by full partition
scan (`ae::bucket_digests` → `scan_prefix` over the whole pid;
`spawn_ae` calls `partition_root` per owned pid per 5 s round). Cost is
linear in data size *every 5 seconds* — the design's "dirty-marked,
recompute lazily" was never built, and even that design re-scans the
partition per dirty bucket. At tens of GB this eats the I/O budget.

**Design — maintain digests at write time (XOR delta), checkpoint, rescan
only on unclean boot.** The digest is an XOR-fold of
`entry_hash(ikey, hlc, vhash)` — XOR is its own inverse, so an update is
`digest ^= old_contribution ^ new_contribution`. The merge write path
(`store::write_merged` and LWW writes in `store.rs`) *already reads the
old record* before writing — the old contribution is free at exactly the
right place. (This is why the digest must stay content-aware — which it
now is, post clock-skew fix.)

1. `DigestTable`: `Box<[[AtomicU64; 256]]>` indexed by pid — 4096×256×8 =
   8 MiB resident. Owned by `Store`, updated inside the shard thread at
   every write site that changes a stored record (write_merged, del paths,
   expiry sweep, RepairOps/Bootstrap applies — they all funnel through the
   same store write helpers; audit checklist in the PR).
2. Checkpoint: every 10 s per shard, serialize dirty pids' rows to
   `meta:digest:<pid>` + a clean-shutdown marker. On boot: marker present
   → load; absent (crash) → background full rescan per pid (prioritize
   owned pids; AE for a pid waits until its row is rebuilt).
3. `ae::partition_root`/`bucket_digests` become table reads (no scan).
   `bucket_entries` (the per-bucket key listing for diff) still scans the
   partition — that's fine: it runs only for *differing* buckets, i.e.
   proportional to actual divergence.
4. Per-round cap: `ae_partitions_per_round = max(512, owned/2)` from the
   design table — now nearly moot (roots are table lookups) but keeps
   MerkleRoot message volume bounded on huge clusters; trivial rotation
   index in `spawn_ae`.

**Correctness invariant.** Digest row must change iff stored bytes change,
atomically with the write (same shard thread — single-threaded per shard,
no races). ondadb TTL purge deletes records *without* the shard thread —
**gap**: purged records leave stale contributions. Handle: expiry is
already materialized as a tombstone write by `sweep_expired` (updates the
digest via the normal path) and real ondadb purge only removes
already-tombstoned records whose contribution was replaced by the
tombstone's… but the tombstone record itself gets TTL-purged eventually →
its contribution must be removed. Solution: `sweep_expired`'s cursor walk
also handles tombstones due for purge: XOR out, then delete through the
store (explicitly, instead of relying on ondadb's background TTL) for
digest-tracked CFs. This makes marekvs the owner of record deletion —
verify ondadb TTL can be disabled per-CF or set as backstop-only
(ONDA_TTL_SLACK already adds slack for exactly this reason,
`store.rs:20-21`).
Safety net either way: a slow background verifier (1 pid/min full rescan,
compare to table, alert + self-heal on mismatch —
`ae_digest_mismatch_total` metric; this catches any missed write site).

**Test plan.** Unit: XOR-delta equivalence vs full scan after randomized
op sequences (property test). Chaos: full default suite (AE behavior
unchanged observably) + new assertion in `check_replication_healed` that
`ae_digest_mismatch_total == 0`. Perf: bench AE CPU before/after with 1 M
keys — target: round cost independent of keyspace size.

**Effort: L (1–1.5 weeks — the write-site audit and purge ownership are
the bulk).** Risk: a missed write site silently rots digests → the
verifier is non-optional, ship it in the same PR.

## T2-8 — Bootstrap rate limiting

**Problem.** `stream_partition` (`repl/lib.rs:828-877`) streams 256-op
chunks in a tight loop per request; N joiners × unthrottled reads
saturate the donor's disk exactly during scale events.

**Design.** Token bucket per node (not per stream — the donor's disk is
the shared resource): `MAREKVS_BOOTSTRAP_RATE_MB` (default 64) shared by
all outbound bootstrap streams; count serialized chunk bytes, `sleep`
when exhausted (bulk lane is async, blocking a stream is free).
Concurrency cap: max 4 concurrent `stream_partition` tasks per donor
(semaphore); further BootstrapReqs queue FIFO. Joiner side needs nothing
(T1-1's tracker already tolerates slow bootstraps; its 30 s re-request
timeout must count *progress*, not just time — count received chunks,
reset timer per chunk).

**Test plan.** Integration: 1 GiB dataset, join a node with rate=8MB —
assert wall time ≈ size/rate ±20 % and donor p99 read latency under
concurrent load stays < 5× baseline (measure in bench harness, not chaos).
Chaos `wipe_replace` (exists): still passes, just slower — set test-env
rate high to keep CI fast.

**Effort: S (1 day).** Risk: none notable; pure throttling.

## T2-9 — Cold purge after ownership loss

**Problem.** Data is kept forever on ex-owners (disk leak across scale
events). But stranded-record AE (`repl/lib.rs:466-489`) deliberately uses
that data as a safety net for un-shipped writes — purging must not race it.

**Design.**
1. Track loss: in the view watcher, diff `owned_pids` per epoch; on loss,
   `meta:cold:<pid> = now_ms` (persisted — restarts must not reset the
   clock). On (re)gain: delete the marker.
2. Purge conditions, ALL required, evaluated by a slow task (1/min):
   - marker older than `cold_purge_delay` (15 m, env-overridable), AND
   - ≥ 3 *completed* stranded-AE exchanges for that pid since the marker
     (track `meta:cold_ae:<pid> = count` — bump when the stranded-AE
     round's MerkleRoot for the pid got a response and any resulting
     repairs were pushed... simplest observable: roots matched, or diff
     was pushed and a follow-up root matched), AND
   - current view has ≥ replicas_n Active owners for the pid (never purge
     into a degraded cluster), AND
   - node not quarantined (T1-3) for the pid.
3. Purge = ranged delete of `partition_prefix(pid)` on the shard thread,
   chunked (bounded per job to not starve the shard), digests (T2-7)
   updated through the same path.

**Wire note.** Needs the stranded-AE responder to answer roots for
non-owned pids — it already does (responder side doesn't check ownership;
verify + test). "Roots matched" is observable locally: our root for the
pid equals the response-implied state when no BucketKeys follow-up
arrives. Simpler proxy if that's fiddly: require the pid's local root to
equal the root received from an owner in the last exchange — direct
equality, no protocol change.

**Test plan.** Chaos `cold_purge` (new): 3 nodes, load data, scale-down…
docker harness: stop node 2, wait for re-replication (underrep back to 0),
restart node 2 with SAME data but ownership moved (shrink via
REPLICAS_N? — simpler: membership_churn variant where HRW moves pids),
assert: pids purge only after delay + matched roots, disk usage drops,
full `check_converged` still passes, and — the safety case — a write
stranded on the ex-owner (crash before ship) is NOT purged before
stranded-AE recovers it (this is chaos scenario `partition_no_resurrect`'s
machinery, extend it).

**Effort: M (2–3 days).** Risk: purging a last copy — the 3-clean-rounds +
healthy-owners conditions are the fence; keep the delay generous by
default.

## T2-10 — Mesh peer GC

**Problem.** `maintain_peer` loops redial forever (`mesh.rs:175`:
"v1: redial until process exit"); departed nodes accumulate dial loops +
log spam on long-lived clusters.

**Design.** The view watcher (`repl/lib.rs:220-240`) owns the `dialed`
map already. Add: when a node id present in `dialed` is absent from the
current view for > 5 min (chitchat's dead-node grace is 1 h; use our own
timer keyed on first-absence), call new `mesh.forget_peer(node)`:
- cancellation token per maintain loop (store `CancellationToken` in the
  peers map alongside handles) → loop exits;
- drop `PeerHandle` entries (ctl+bulk);
- repl side: drop `cursors` entry and interest sub-entries for the node.
If the node reappears (same id, new addr — the apple-container pattern),
the existing `dialed.get(&m.node) != Some(&m.mesh_addr)` re-dial logic
recreates everything; verify cursors re-init via ResumeFrom (they do).

**Test plan.** Chaos `membership_churn` (exists): add assertion — after a
node is gone > grace (shrink grace via env for the test), the survivors'
logs stop showing dial attempts and `mesh_peers` reflects only live peers.
Unit: token cancels loop.

**Effort: S (1 day).**

## T2-11 — Zone-aware HRW placement

**Problem.** `owners_for` ranks purely by HRW score; with RF=2 both homes
of a partition can share a zone. Pod topology spread constraints spread
*pods*, not *partition replicas*. A zone loss can take both copies of some
partitions. Matters iff deployed multi-zone.

**Design.**
1. Zone label: `MAREKVS_ZONE` env (k8s downward API from
   `topology.kubernetes.io/zone`); gossiped as chitchat node-state KV
   (additive — gossip KV is string map, no wire break). `Member` gains
   `zone: Option<String>`.
2. Placement: `owners_for` becomes zone-spread HRW — sort candidates by
   HRW score (unchanged), then greedy pick: highest score whose zone is
   not yet used; when zones exhausted (or unlabeled), fall back to pure
   score order. Deterministic, stable, same inputs ⇒ same outputs on every
   node (zones are gossiped state like phase — same convergence class as
   today's membership divergence, no new failure mode).
3. **Gating**: `MAREKVS_ZONE_AWARE=1`, must be uniform cluster-wide (same
   deployment rule as REPLICAS_N). Flipping it on a live cluster reshuffles
   ownership → mass data movement handled by the existing join/AE machinery
   (bounded by T2-8's rate limit — do T2-8 first), `underreplicated` gauge
   tracks progress. Document the flip as a maintenance operation.
4. `h1` selection unchanged (top-ranked Active owner).

**Test plan.** Unit: property tests — (a) with ≥ RF zones, owners span RF
distinct zones for every pid; (b) removing one zone's nodes changes only
affected placements (HRW minimal-disruption preserved within zone groups);
(c) unlabeled nodes degrade to today's behavior byte-for-byte
(`MAREKVS_ZONE_AWARE` unset ⇒ identical placement — regression-freeze
test). Chaos: docker harness gains `MAREKVS_ZONE` per node (zones a,a,b,b);
new scenario `zone_loss`: kill both zone-a nodes at once with RF=2 —
assert zero lost acked writes after heal (today this loses whatever was
homed entirely in zone a).

**Effort: M–L (3–5 days incl. harness work).** Risk: mixed
zone-aware/unaware nodes during the flip disagree on placement — same
class as REPLICAS_N mismatch; enforce via a gossiped config-hash check
that logs ERROR on mismatch (cheap, worth adding for REPLICAS_N too).

## T2-12 — HINCRBY as PN counter

**Problem.** HINCRBY is LWW-on-result (`design/02:299`): concurrent
increments on different nodes lose updates. INCR was fixed in v1.1 with
`CounterState`; hash fields weren't.

**Design.** Reuse `CounterState` verbatim inside hash-field element
records.
1. `RecordType`: hash fields today use the element rtype for hashes; add
   `CounterField` (new rtype byte — data-format change, see compat).
2. Write path: HINCRBY on a field — read element; if absent → new
   CounterField with base=0 + self slot; if CounterField → bump own slot
   (same logic as `cmd/string.rs` incr); if plain HashField → Redis
   semantics: parse as int, CONVERT to CounterField with base=(parsed,
   current hlc/origin) — mirrors what INCR does to a SET value; if
   unparsable → `ERR hash value is not an integer`.
3. Merge: `merge_values` dispatches on rtype — add CounterField arm
   delegating to the existing counter merge (pointwise max + base LWW).
   HSET over a counter field = plain HashField write with newer HLC → LWW
   replaces it (matches SET-over-INCR semantics).
4. Read path: HGET/HGETALL/HRANDFIELD render CounterField as the resolved
   integer string (one helper, used everywhere elements are read).
5. DEBUG COUNTERSTATE extended to accept `key field`.

**Compatibility.** New rtype byte on the wire *inside record payloads* —
old nodes receiving a CounterField via repl/AE would fail/mis-merge.
Gate: only write CounterField when all mesh peers announce the feature bit
(P0); until then HINCRBY keeps LWW behavior (with a startup log). Flag-day
alternative acceptable pre-production.

**Test plan.** Unit: merge laws for CounterField (extend
`merge_laws.rs` — commutativity/associativity/idempotence property
tests). Chaos: `counter` workload variant `hash_counter` — concurrent
HINCRBY across 3 nodes under partition/heal; checker = same windowed
bounds as the existing counter checker. INCRBYFLOAT: **stays LWW,
documented** (float addition is non-associative; a PN-float would drift).

**Effort: M (2–3 days).** Risk: rtype dispatch missed in some read path →
renders binary garbage; grep-audit every `RecordType::` match when adding
the variant (compiler helps: make the match exhaustive, no `_` arms in
element readers).

## T2-13 — List position node-salting

**Problem.** Concurrent LPUSH/RPUSH on different nodes allocate
`head-1`/`tail+1` from the same observed head/tail (`cmd/list.rs:4-6`) —
same position, LWW keeps one, the other push is lost (`design/02:266`).
Full fix (RGA) is a big project; not worth it. Cheap structural fix:
make cross-node collisions impossible.

**Design.** Positions are u64. Allocate with a stride that embeds the
node id: `pos = base ± (stride × k) | node_id`, stride = 1024 (10 bits,
supports node ids 0-1023 — enforce at boot: `node_id < 1024`, we already
practically require u16 ids; document the new bound).
- LPUSH: `pos = (head & !1023) - 1024 | node_id` — strictly less than any
  existing position from any node built on the same observed head, and
  distinct across nodes by construction (low 10 bits differ).
- RPUSH symmetric. Two nodes pushing concurrently from the same observed
  head land at the same stride slot but different low bits → **both
  survive**, order between them = node-id order (deterministic, converges;
  arbitrary but consistent everywhere — same guarantee Redis gives for
  concurrent pushes from different clients anyway).
- LINSERT midpoint math: `mid = (a+b)/2` may collide cross-node — apply
  the same masking (`mid & !1023 | node_id`; if the masked slot equals a
  or b → trigger the existing O(n) rebuild path, which already handles
  position exhaustion).
- Rebuild (`LINSERT/LREM/LTRIM` renumbering) allocates salted positions
  too — a rebuild concurrent with a remote push then cannot collide with
  it (this closes the rebuild-race noted in design/02).

**Headroom.** 2^63/1024 ≈ 9×10^15 pushes per direction before exhaustion
→ rebuild; non-issue.

**Compatibility.** Data-compatible: existing positions remain valid;
new positions interleave correctly (comparison is plain u64 order). Mixed
versions during rollout: old nodes still allocate unsalted → collision
window persists until all nodes upgrade — no gating needed (strict
improvement, no corruption either way).

**Test plan.** Unit: two simulated nodes pushing from identical observed
state → distinct positions, deterministic order. Chaos: new `list_push`
workload — concurrent LPUSH/RPUSH of tagged values from 3 nodes,
partition/heal, checker: every acked push present exactly once (set-full
checker on list members), order consistent across all nodes (today: lost
pushes expected; after: zero).

**Effort: S–M (1–2 days).** Risk: off-by-one in masking around
LIST_CENTER; property-test the allocator against a naive model.

## T2-14 — Operator: surface errors to CR status

**Problem.** Scrape failures return empty metrics silently
(`operator/main.rs:82-102`), PVC-reclaim deletes are `let _ =` discarded
(`main.rs:245`), reconcile errors only reach the operator's own log
(`main.rs:283-313`). An operator whose failures are invisible is worse
than none.

**Design.**
1. `MarekvsClusterStatus` gains `conditions: Vec<Condition>` (standard
   k8s condition type: `type/status/reason/message/lastTransitionTime`) —
   types: `MetricsAvailable`, `ReconcileSucceeded`, `PvcReclaim`.
2. `scrape()`: return `Result` with per-pod failure detail; reconcile sets
   `MetricsAvailable=False, reason=ScrapeFailed, message=<n>/<m> pods` —
   autoscaler already treats NoMetrics safely, now it's visible.
3. PVC reclaim: log error + `PvcReclaim=False` + retry next reconcile
   (natural — reclaim re-evaluates each pass; just stop discarding).
4. `error_policy`: also write `ReconcileSucceeded=False` with the error
   string (guard against status-write-fails-too loops: best-effort,
   rate-limited).
5. Emit k8s Events (`Recorder`) for transitions — `kubectl describe mkv`
   becomes the debugging entry point.

**Test plan.** Extend operator unit tests (`resources.rs` pattern):
condition transitions for scrape-fail/succeed; manual kind run:
`kubectl describe` shows conditions. **Effort: S (1 day).**

## T2-15 — Operator: health-gated rollouts

**Problem.** `spec.image` change → new StatefulSet template → k8s rolling
update gated only on pod *readiness*; readiness = "serving", not
"cluster fully replicated". With RF=2, restarting pod B while pod A's
partitions are still underreplicated leaves a 1-copy window.

**Design.** Use the StatefulSet `updateStrategy.rollingUpdate.partition`
knob, operator-driven (standard canary-walk pattern):
1. When the generated pod template hash changes, set
   `partition = replicas` (nothing updates), then walk down: decrement by
   1 only when the same gate as scale-down passes (all pods ready AND
   `underreplicated == 0` AND the just-updated pod, if any, is back and
   Active).
2. Surface progress in status: `phase: Updating`, `updatedReplicas x/y`
   (from STS status), condition `RolloutHealthy`.
3. Stuck gate (> stabilization window without progress): `phase: Blocked`
   + reason — same UX as blocked scale-down.
4. `reconcile` requeue drives the walk (Action::requeue 15 s during
   rollouts).

**Test plan.** Operator unit tests: partition-walk state machine against
faked pod/metric states (gate holds, gate blocks, resumes). kind/e2e
(manual, pre-release): 3-node cluster under write load, `spec.image` bump
→ zero underreplicated>0 samples during the whole rollout.
**Effort: M (2–3 days).**

## T2-16 — Operator: leader election

**Problem.** Two controller replicas would fight (same field manager,
no election); today's mitigation is "run 1 replica" by convention
(`k8s/operator/deployment.yaml:8`).

**Design.** Kubernetes Lease-based election, ~100 lines hand-rolled (the
kube-rs ecosystem's `kube-leader-election` crate is fine too — prefer the
crate if its deps are clean):
- On start: acquire/renew `coordination.k8s.io/Lease`
  `marekvs-operator-leader` (15 s duration, 10 s renew, 2 s retry).
- Only the holder runs the controller stream; non-holders idle-watch the
  lease. On loss (renew fail): **exit the process** (fail-fast beats
  split-brain; Deployment restarts it as a follower).
- RBAC: add lease get/create/update to `k8s/operator/rbac.yaml`.
- `deployment.yaml`: replicas stays 1 in the example (election makes 2+
  *safe*, not required); drop the "not leader-elected yet" comment.

**Test plan.** kind: run 2 replicas, kill the leader, follower takes over
< 20 s, exactly one reconciler active throughout (log marker).
**Effort: S–M (1–2 days).**

---

# Sequencing & bundles

Dependency/affinity-ordered; each bundle is one reviewable PR series with
its chaos scenario(s) as the merge gate.

| Order | Bundle | Items | Why together |
|---|---|---|---|
| 1 | proto v2 | P0 | everything later gates features on it; flag-day is cheapest now |
| 2 | mesh reliability | T1-2 + T1-4 (+ HandoffAck subsumption) | same files, same failure domain, chaos `slow_peer`/`stalled_peer`/`freeze_thaw` gate both |
| 3 | join gate | T1-1 | independent; biggest correctness win; chaos `join_gate` |
| 4 | resource guards | T1-5 + T1-6 | small, independent, ship fast |
| 5 | resurrection fence | T1-3 | needs test-env grace overrides; heaviest Tier 1 item |
| 6 | AE at scale | T2-7 | largest single change; isolate |
| 7 | scale-event hygiene | T2-8 + T2-9 + T2-10 | all about join/leave churn; chaos `membership_churn`/`wipe_replace`/`cold_purge` |
| 8 | operator hardening | T2-14 + T2-16 + T2-15 | one operator PR series (status first — the others report through it) |
| 9 | data-type upgrades | T2-12 + T2-13 | independent of cluster work; T2-12 needs P0 feature bits |
| 10 | multi-zone | T2-11 | last: needs T2-8 (rate-limited reshuffle) and evidence you'll deploy multi-zone |

Rough total: Tier 1 ≈ 2–3 weeks, Tier 2 ≈ 4–5 weeks of focused work.

**Definition of done, globally**: every bundle lands with (a) its chaos
scenario failing on main / passing on the branch, (b) metrics named in the
plan present in `/metrics`, (c) the defaults table row(s) in design/05
updated from `design` → real, (d) todo.md item checked off.

# Idle expiry and cold-purge CPU Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove repeated idle scanning and range-mask reconstruction while preserving TTL visibility, replication safety, and bounded foreground latency.

**Architecture:** First fix range-fragment reuse in ondaDB, then make marekvs cold cleanup avoid repeated empty-range deletes. Replace independent whole-database expiry walks with generation-checked, partition-bounded discovery and deadline scheduling. Correct INFO reporting and verify the complete change in a disposable three-node workload before changing Annotix.

**Tech Stack:** Rust 2021, marekvs workspace, ondaDB 0.9.0 sibling checkout, existing crossbeam/parking_lot/Tokio primitives, Docker test harness.

**Spec:** [CPU investigation](../../investigations/2026-09-14-annotix-idle-cpu.md), the user's additional shard-loop investigation, and the correctness contracts below. Existing expiry and replication contracts in `design/05-consistency-anti-entropy.md` remain authoritative.

## Execution status (2026-09-14)

Tasks 2–6 are implemented and independently reviewed. The ondaDB
change is published in PR #2 (`8afa06618ba4320df33b5a8c004adf2b85c003e8`),
with default and unsafe-fastpath full suites passing. The final pin is
`0f4ebc67434551c9d3d4cba899829234760f4215` (PR #3), adding only the
Linux clock portability lint annotation validated on Rust 1.97.1. Tasks 4/5 additionally
require the nonce protocol amendment below. Task 1's disposable process and
Docker fixtures are available. The final Docker 1/2/10-shard matrix passed idle
structural assertions, TTL behavior and autonomous restart repair; see
[release validation](../../investigations/2026-09-14-idle-cpu-results.md).
Release publication is tracked in Task 7. The detailed checklist retains the original
acceptance contract; the final results report records any deviations explicitly.
Live Annotix rollout remains outside this release task.

## Global constraints

- Rust floor: 1.89. No new runtime dependencies, storage-format bits, Redis commands or record encodings. Review established that safe cleanup needs feature-negotiated internal proof request/reply messages; see the execution amendment below.
- Two repositories: `marekvs` and `../ondadb`. Keep their changes independently testable and reviewable. Create isolated execution worktrees; preserve all unrelated working files.
- Do not restart, flush, compact, reconfigure, load-test or replace the live Annotix stack during implementation. Use synthetic databases and disposable containers. Production rollout is a separate explicit operation.
- Preserve the budget-record exemption, deterministic expiry tombstone clocks, RGA dead anchors, head/delete-clock visibility, and passive ondaDB TTL backstop.
- Never infer absence from an incomplete/erroring scan. Never suppress active expiry based on `INFO expires`.
- No blocking shard submission or recursive storage writes from a commit callback. Replication-hook suppression must not suppress local maintenance invalidation.
- Do not solve this by simply increasing the 100 ms interval, reducing shard count, disabling expiry, or discarding historical range sequences.

## Evidence corrections and acceptance contract

1. `cmd/server.rs` currently formats `expires=0,avg_ttl=0` literally. The cited INFO result does **not** prove that these stores contain no TTLs. Even accurate key-level INFO would not count all per-member expirations.
2. The sweeper parses every internal key before its ownership check; envelope decoding is inside that check. All shards still pay iterator construction and traversal for the complete data CF.
3. `marekvs_db_range_deletes` is cumulative since open; `marekvs_db_range_fragments` reports catalogued SST fragments, not live memtable spans. Neither alone measures the current in-memory mask.
4. The isolated release experiment confirms expensive **iterator construction**, including on an empty database. Step cost may contribute, but it is not necessary to reproduce the fault. With 18,117 spans over 466 intervals, construction averaged 75.7 ms before a synthetic flush and 0.292 ms afterward.
5. Reported zero SST gauges do not establish that no files exist. The additional investigation reports similar on-disk shapes. Inventory read-only files and record both observations; do not make the remedy depend on an all-memory assumption.

Success requires all of the following:

- After initial discovery, a stable TTL-free store opens **zero expiry iterators** over a deterministic 60-second interval, regardless of shard count.
- Expiry reads only partitions assigned to the executing shard. Empty discovery is bounded by a partition count as well as a record count.
- A completed TTL-free proof cannot survive a relevant concurrent write unnoticed; restart/import/replication/repair cannot introduce a missed TTL.
- Due TTL work makes progress under continuously arriving jobs as well as when idle.
- Repeated reads of an unchanged range set reuse fragment storage and perform no re-fragmentation. Range mutations invalidate subsequent reads without changing existing iterator snapshots.
- Repeated eligible purges of an empty partition do not increment range-delete counts. New data must receive fresh cleanup safety evidence before a new purge.
- The representative three-node idle workload drops aggregate CPU by at least 90% relative to its pre-fix baseline and averages below 5% of one core per node over 60 seconds after warm-up. Measure on the same host/runtime, outside profiling; use structural counters as the CI oracle rather than flaky CPU assertions.

## File and ownership map

| Repository / files | Responsibility |
|---|---|
| marekvs `crates/marekvs-engine/src/store.rs`, new `src/store/expiry.rs` | Store-owned maintenance invalidation, expiry discovery and scheduling |
| marekvs `crates/marekvs-engine/src/cmd/server.rs`, `cmd/generic.rs` | Accurate key-level expiry statistics using existing visibility logic |
| marekvs `crates/marekvs-engine/src/metrics.rs` | Expiry work counters and live range-state measurements |
| marekvs `crates/marekvs-repl/src/lib.rs` | Cold-purge eligibility, generation-bound evidence, empty-range check |
| marekvs `crates/marekvs-engine/tests/expiry_maintenance.rs`, `tests/info_expiry.rs`; repl inline tests | Behavioral and race regressions |
| ondaDB `src/range_tombstone.rs`, `src/column_family.rs`, `src/unified.rs` | Immutable cached fragment snapshots and integration |
| ondaDB `tests/range_delete.rs`, new `tests/range_mask_reuse.rs` | MVCC, invalidation, bounds and allocation regressions |
| marekvs new `tests/idle_maintenance/` | Reproducible workload, metrics collection and CPU report |
| marekvs `docs/testing.md`, `design/05-consistency-anti-entropy.md`, investigation | Operating limits, evidence and rollout guidance |

Dependency order: **1 → 2 → 3 → 4 → 5 → 6 → 7**. Each task ends with its focused tests and a separate commit. Do not begin later tasks with failing earlier gates.

## Task 1 — Preserve the reproduction and add honest observability

- [ ] Add `tests/idle_maintenance/README.md` and a synthetic fixture generator based on the recorded experiment: zero ranges; 1,024 unique ranges; 8,331 deletions across 24 partitions; 18,117 deletions across 466 partitions. Include a live non-expiring key and a completely empty variant. Construct data through APIs, never private production volumes.
- [ ] Add test-only counters at expiry iterator creation, visited records, partition transitions and range fragmentation. Reset/read counters within an isolated fixture; do not share global counters across parallel tests without isolation.
- [ ] Expose process metrics `marekvs_expiry_passes_total`, `marekvs_expiry_iterators_total`, `marekvs_expiry_records_total`, `marekvs_expiry_scan_errors_total`, `marekvs_expiry_tombstones_total`, and `marekvs_expiry_tick_seconds`. Use fixed labels only. Record only successful tombstone writes as emitted mutations.
- [ ] Add ondaDB statistics for current memtable range spans and cached fragment bytes, distinct from cumulative range deletes and catalogued fragments. Carry these through marekvs metrics using the existing DB statistics collection path. Instrument cache builds/hits in test/debug performance counters.
- [ ] Save an unchanged-baseline 60-second report: CPU, thread distribution, connected clients, command deltas, expiry iterator counts, range spans, range deletes, SST inventory and compaction activity. Sweep/iterator counters must distinguish discovery from due work.
- [ ] Verify baseline reproduction fails the acceptance contract: iterator count increases indefinitely without writes or TTLs. Preserve the failure as an ignored/manual performance baseline until the behavioral regression exists in Task 5.
- [ ] Commit the harness and instrumentation, with no behavior change.

Reproduction must use both per-CF memtables and unified WAL/memtable configurations. Existing databases and default configurations remain readable.

## Task 2 — Cache immutable range-fragment snapshots in ondaDB

**Interfaces:** add an internal `RangeTombstoneSet::fragment_snapshot() -> Arc<[Fragment]>`; keep the existing clipped `fragments(lower, upper)` behavior for flush/compaction callers. Change `FragCursor` to own shared fragment storage plus its own cursor/range window. Expose no new public wire or on-disk format.

- [x] Add a failing test: construct a range set, create/drop 100 iterators without mutations, and assert fragment construction happens once, shared backing allocation is reused, and every iterator has independent cursor state.
- [x] Add mutation/snapshot tests: hold an old iterator, add an overlapping range, create a new iterator; old traversal remains stable and the new iterator honors the new sequence. Check fixed read snapshots at before/equal/after tombstone sequences and point reinsertions after a range deletion.
- [x] Add bounds tests for inclusive/exclusive endpoints, reverse traversal, seek direction changes, custom comparator, overlapping sources, and a range extending outside an SST's point-key bounds.
- [x] Store an optional cached `Arc<[Fragment]>` alongside the protected span set. Invalidate it **under the same write lock** that changes spans; build/publish the first snapshot under exclusive access after rechecking the cache. This prevents concurrent shard scans from independently rebuilding the same cold cache or publishing stale results. Existing iterators keep their immutable Arcs.
- [x] Build the full snapshot once per range-set generation, then select overlapping fragments through binary search and a per-iterator window. Do not cache a vector for every arbitrary query bound and do not clone the complete vector on each iterator. Account one resident cache plus snapshots retained by live readers; expose retained-memory behavior in the benchmark.
- [x] Adapt `iterator_range_mask` to consume shared snapshots. Adapt unified mode without concatenating and sorting all cached fragments per read: retain per-source fragment lists, clip to the CF-prefixed interval, and translate keys consistently. Keep transaction overlay ranges isolated from the committed cache.
- [x] Preserve every sequence required by MVCC; do not deduplicate repeated deletions by keeping only their newest sequence. Ensure cache-only computation cannot swallow read/storage errors.
- [x] Run `cargo test --test range_delete --test range_mask_reuse` in ondaDB, then its normal full CI/test gate and feature variants covering unified and ordinary memtables. Rerun the reproduction: unchanged-iterator fragmentation count must stay constant after warm-up.
- [x] Commit in ondaDB. Do not optimize `fragment_spans` into a new sweep-line algorithm in this change: caching removes repeated work with a smaller correctness surface. Measure cold-build time separately and retain it as a known cost.

## Task 3 — Integrate the dependency change reproducibly

- [x] Add the reviewed ondaDB commit to the dependency provenance used by marekvs; update `Cargo.lock` against the canonical Git source once that commit is available there. Keep a local path patch only for development. Do not claim the lockfile is reproducible while it references an unpublished sibling-only change.
- [x] Validate that marekvs with the local patch and a clean checkout without the patch use the same reviewed ondaDB revision and pass the existing scan/TTL/merge suites.
- [x] Record pre/post **warm and cold** iterator timings and cache memory on the four fixtures. Ensure the optimization does not replace repeated CPU work with unbounded per-query cache growth.
- [x] Commit the marekvs dependency integration separately from behavior changes.

Publishing the dependency is an execution handoff if not already authorized. Implementation and local verification can proceed with the path patch; remote publication must not be invented as completed.

## Task 4 — Stop duplicate cold purges and bind safety evidence to data state

**Interfaces:** extract a testable `purge_if_eligible` path returning `PurgeOutcome::{Purged, Empty, StaleEvidence, Ineligible}`. Keep ownership age persisted. Bind clean-AE counts to an in-process data generation; discard old clean evidence on restart, while retaining the conservative ownership-loss timestamp.

- [x] Add a failing replica test: mark a cold partition safe, populate it, purge it, then evaluate cleanup ten more times. Assert the first pass emits one range delete and subsequent passes emit none.
- [x] Add tests for an initially empty partition; an incomplete empty probe; new data after a successful purge; writes after the last clean-AE exchange; ownership loss/regain/loss; restart; degraded owners. Errors must never be treated as proof of emptiness or permission to delete.
- [x] Introduce store-owned `partition_generation(pid) -> u64` invalidation shared with Task 5. Increment the affected partition after every successfully committed data mutation, including replication/AE/bootstrap and direct batch paths. Hook ordering is not commit ordering: do not assign an older sequence over a newer generation. Range operations need explicit invalidation because ondaDB omits range deletes from point commit hooks.
- [x] Bind a clean-AE exchange to the generation used for its root: capture before root computation, require the same generation after computing and when processing the peer's match. A cached root must carry the generation at which it was computed; sampling a new generation around an old cached root is insufficient. Count the match only for the same cold ownership epoch and data generation. Restart clears runtime proof so it cannot reuse a reset generation with old persisted counts.
- [x] Inside the owning shard job, recheck generation and cleanup eligibility, then use `new_iterator_bounded` on `[pid, pid+1)` to probe for live data. If complete and empty, return `Empty` without issuing a range delete. If nonempty, issue the existing local-only range deletion and clear clean evidence after success. Recheck owner-view epoch/health before deletion; a view change requires another eligibility evaluation.
- [x] Establish a serialization boundary covering the final proof check, empty probe and range deletion against all same-partition data writers. Use the owning shard when the ingress audit proves every writer uses it; otherwise route bypassing writers through that boundary. A post-commit callback alone cannot close the gap between data becoming visible and invalidation, or prevent a write between the final check and deletion. Add a barrier-controlled race test for both gaps.
- [x] Do not introduce a permanent “purged” flag. Later data or ownership changes must naturally require fresh proof. Keep cold age metadata separate from proof resets, so frequent writes do not accidentally defeat the retention safety window.
- [x] Retain best-effort excise after a successful nonempty purge; do not force flush/compact a live store to hide the root cause. Add `cold_purge_empty_skips_total` and `cold_purge_stale_proofs_total`.
- [x] Run existing repl tests plus the new cases; prove remote replicas retain records, same-key later data is not incorrectly purged, and repeated empty passes leave the range-delete count unchanged. Commit.

## Task 5 — Replace perpetual whole-store expiry walks with safe discovery

**State:** fixed-size per-partition generation counters (4,096 entries), dirty bits, and shard-local discovery records. Each partition is `Unknown`, `Scanning`, `NoTtl { generation }`, or `Due { generation, deadline_ms }`. State is advisory and rebuilt on restart; it is not a persisted TTL index.

**Interfaces:** new `store/expiry.rs` owns `ExpiryScheduler`, with `next_wait(now_ms) -> Duration`, `invalidate(pid)`, and `poll(ctx, now_ms, record_budget, partition_budget, elapsed_budget) -> Result<ExpiryProgress, ScanIncomplete>`. Use existing configured shard ownership and no new runtime dependencies. Use an injected clock/counters in unit tests. `ExpiryProgress` reports `records_visited: usize`, `partitions_completed: usize`, `iterator_opens: usize`, `tombstones_written: usize` and `has_more_work: bool`. `ScanIncomplete` retains the failed partition ID and storage error for retry/metrics; budget exhaustion is ordinary progress, not an error.

- [x] Before coding the scheduler, inventory **every** data ingestion path: `put_raw`, `write_merged`, `put_many_lww`, replication batches, AE, bootstrap, restore, partition ingest/move/import, range deletes and physical derived-index deletes. Identify bypasses of the point commit hook. Route invalidation through one store-owned observer composed with the replication observer; never install two competing hooks or perform metadata writes in the callback.
- [x] Start every partition `Unknown`, including after restart. A data mutation sets its dirty bit and advances its generation without blocking. Conservative invalidation for non-TTL writes is acceptable; no false-negative invalidation is acceptable.
- [x] Discover only `pid % shard_count == shard_index`. For each partition use declared byte bounds `[pid, pid+1)` and a cursor within those bounds. Process at most 128 surfaced records and 8 partition transitions per poll, with a 2 ms elapsed-work check between steps. Construction/one underlying iterator step is not preemptible; document this and record its actual duration.
- [x] A multi-tick discovery pass captures its generation. If any mutation occurs before completion, discard its absence/deadline proof and rescan; a write behind the cursor must not be missed. Publish `NoTtl` only after a complete error-free scan with matching generation. Keep `Unknown` on errors and retry with bounded backoff; never clear dirtiness unconditionally after a concurrent commit. Apply the writer serialization contract from Task 4 to proof publication, or demonstrate an equivalent ordering protocol that closes the visible-commit/before-callback gap. Add a deterministic race test that pauses there.
- [x] For non-budget, live envelopes, remember the earliest positive TTL deadline during a complete scan. Keep future TTLs scheduled even when none are due now. At a due deadline, rescan that partition and apply the existing deterministic expiry behavior. TTL extension, PERSIST, collection replacement, member TTLs and merges invalidate the old proof. Native GC TTLs and budget payload deadlines are not interchangeable with envelope TTLs.
- [x] Park partitions proven `NoTtl` until mutation. For unfinished discovery or dirty partitions, schedule the next bounded poll within 100 ms; rotate partitions fairly so one busy partition cannot monopolize discovery. Otherwise cap waiting at 1 second to observe dirty bits and wall-clock changes without opening an iterator. Use wall clock for expiry decisions and a monotonic clock for elapsed-work limits. Forward clock jumps are noticed within that cap; backward jumps must not expire records early.
- [x] Change `shard_loop` to perform a bounded due/discovery poll after a job when its maintenance deadline has arrived, as well as after a receive timeout. Never let a continuously nonempty command queue starve expiration; never run unbounded catch-up work after a pause.
- [x] Add deterministic tests using the injected clock: no-TTL store performs discovery once and zero additional iterator opens over 60 seconds; 1/2/10 shards never visit another shard's partition; empty databases honor the partition budget; writes during discovery and replica TTL arrivals invalidate absence; future TTL, PERSIST, expired overwrite and backward/forward clock changes behave correctly; busy queues still expire due records; read failure leaves the partition armed.
- [x] Reuse `head_del`, JSON, per-member TTL, RGA-anchor and budget tests as visibility regressions. Add restart/replay and native-TTL filtering cases: discovery must not claim that a vanished expired record was actively tombstoned, and must preserve the existing passive expiry/replication contract.
- [x] Run `cargo test -p marekvs-engine --test expiry_maintenance`, the relevant existing suites, then full workspace tests. Commit.

If an ingestion route cannot participate in invalidation, that route must leave affected partitions `Unknown` and trigger rediscovery. Do not enable a no-TTL skip for partitions whose writes cannot be observed. This is a correctness gate, not an optional optimization.

## Task 6 — Make INFO expiry reporting truthful

- [x] Add `tests/info_expiry.rs`: one persistent string, one future-TTL string, one future-TTL collection head, an already-expired key, and a persistent collection with only a per-member TTL. Assert `keys` counts live keys, `expires` counts only the two live key-level TTLs, and `avg_ttl` is computed from positive remaining key TTLs with a deterministic test clock/tolerance.
- [x] Replace `keyspace_count` with a shared visibility-aware `keyspace_stats` result containing `{ keys, expires, avg_ttl_ms }`. Reuse generic key enumeration/type and head-clock rules; avoid double-counting physical record families for one logical key.
- [x] Preserve INFO section filtering. Do not turn metrics scraping or expiry scheduling into repeated INFO scans; this remains explicit command work. Document that a multi-shard result is an observed aggregate, not a global snapshot.
- [x] Explain that `INFO expires=0` does not rule out per-member TTLs, and cannot act as the maintenance scheduler's safety proof. Run INFO/generic/type regression tests and commit.

## Task 7 — End-to-end acceptance, docs and rollout handoff

- [x] Run `just ci` on the final marekvs tree and the full ondaDB gate on its reviewed revision. Keep a TTL-heavy suite, range-deletion MVCC suite and cold-purge re-arrival tests as separate reported results.
- [x] In disposable Docker nodes, run the same no-TTL, key-TTL, member-TTL, mixed, restart and range-heavy fixtures before/after. Use fixed shard counts 1/2/10, persistent volumes scoped to the test, RF=2 across three nodes, and a stable ownership view.
- [x] Warm caches and complete initial discovery, then collect a 60-second no-client interval. Assert zero expiry iterator growth for stable no-TTL partitions and no range-delete growth for empty cold partitions. Record CPU mean/peak, mask builds/hits/bytes, discovery/due time, and replication repair counters. Observe at least three cold-purge ticks to catch duplicate cleanup.
- [x] Repeat under normal client traffic: report throughput and p50/p95/p99 latency; require no more than 5% throughput loss or p99 increase against the same-host baseline. Separate first-use fragmentation and restart discovery latency from steady state. If noisy, extend measurement instead of weakening correctness assertions.
- [x] Verify convergence after TTL deadlines, replica disconnect/reconnect, new writes into a previously purged partition, and a node restart. No lost last-copy data, stale mask reuse, skipped TTL or range resurrection is acceptable.
- [x] Update `docs/testing.md`, the defaults/design documentation, and the investigation with actual results and any unmet acceptance criteria. Record the resolved `expires=0` misconception and the distinction between iterator build work and visited-record budget.
- [ ] Produce reviewed, separate ondaDB and marekvs commits/PR descriptions with dependency ordering. Finish implementation locally before requesting any publication or live rollout authorization not already granted.
- [ ] Live Annotix handoff: identify the exact image digest and dependency revision; retain existing volumes; use its existing rollout scripts only after explicit authorization. Recheck readiness, replication health, TTL behavior and the 60-second idle CPU window. A rolling restart without the code change is not the remedy.

## Execution amendment — cleanup response correlation

Independent implementation review found that even equal-content Merkle buckets
can be delayed from an earlier ownership period. Assigning the current epoch
when a response arrives cannot prove that the exchange belongs to that epoch.
The implementation therefore adds feature-negotiated cold-proof request/reply
messages with unique request IDs. Pending evidence records the local root,
data generation, ownership epoch and peer at request time; receipt and deletion
recheck that provenance. Legacy root matches and bucket messages remain normal
anti-entropy traffic and cannot authorize cleanup. Mixed-version peers continue
replication; destructive cleanup waits for an owner supporting correlated proofs.
This is an internal protocol extension, with no Redis API or storage-format change.

Review also requires a fairness regression with at least eight repeatedly dirty
partitions, elapsed-budget checks between expiry commits, and rejection of
out-of-range peer partition IDs before indexing generation arrays.

## Acceptance execution notes

The process fixture isolates the reported live-memtable, persistent-key worst
case for 1/2/10 shards. The ondaDB 80-case timing matrix and deterministic engine
suites cover the additional empty, range-shape and TTL cases. Baseline binaries
do not have the new maintenance counters; red/green tests provide the structural
comparison. Docker reports retain counter snapshots and warm for 180 seconds;
repeated-empty cleanup is enforced deterministically in CI, rather than inferred
from an unexported purge-tick count. Throughput/p99 comparisons improved in every
shard configuration, with background-host-load limitations stated in the report.
The initial Docker isolation transport error was reproduced and corrected, then
the complete fixed matrix reran successfully. Live rollout remains unperformed.

## Plan review checklist

- [x] Every successful data ingress invalidates expiry proofs, even when replication forwarding is suppressed.
- [x] Empty/error, future/already-expired, key/member/budget TTLs are distinct in tests.
- [x] Cached fragments preserve comparator ordering, source boundaries, MVCC stacks and independent cursors.
- [x] Cold proof is invalidated by new data and restart; empty cleanup adds no new tombstone.
- [x] No runtime/production action is claimed completed by this planning document.

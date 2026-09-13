# DIFF.* commands and the `marekvs-diff` crate

Reviewed implementation plan, 2026-09-13.

Implementation is present on `codex/diff-commands`. Final validation status is
recorded below; a checked implementation item does not imply Docker chaos ran.

**Goal:** compare canonical structured documents, discover moved and edited content, produce independently acceptable suggestions, record replicated decisions, and apply decided two- or three-way graphs to immutable snapshots.

**Architecture:** a synchronous, storage-independent algorithm crate plus an engine adapter. JSON records retain per-node identities in working copies. Immutable snapshots identify visible content; scan-local identity bindings are separate. CPU work runs on a dedicated bounded pool; storage reads/writes remain shard operations. Artifacts use existing record types and replication.

**Reference:** annotix revision 2, `/Volumes/HOME/code/storage-engines/annotix/docs/2026-09-13-annotix-design.md`, especially §§5.1–5.7 and §6. The reviewed decisions below explicitly correct contradictions in that proposal. No converter, editor, Word add-in, AI service, watcher, or indexer is included.

**Dependencies:** existing workspace Rust dependencies only; Rust 1.89 floor. No ondaDB protocol/storage change. Work is on `codex/diff-commands`; unrelated pre-existing untracked files must remain untouched. No release, push, or tagging.

## Review decisions


These decisions resolve defects found by reviewing the original plan against the code and annotix revision 2. They are part of the implementation contract. The original pseudocode has been replaced with executable task boundaries and validation requirements.

- **Buildable dependency order:** A.1–A.2 include `budget`, cancellation, and tokenization prerequisites. Export only implemented modules. Implement S2 before the showcase/classification acceptance tests; do not substitute a no-op matcher and call the showcase complete. Add `records` as an explicit A.2b task, tested against the core materializer's visibility and RGA ordering.
- **Strict model boundary:** require a `doc` root, reject unknown fields and leaf/container field mismatches, scalar-only attributes with signed-64-bit integers (the core JSON codec otherwise rounds large unsigned integers to f64), no empty formatting-attribute runs, checked offset conversions before narrowing, exact three-field formatting tuples, valid formatting attributes, sorted nonoverlapping merged runs. Preserve hard-break `\n` tokens and verbatim code whitespace. NFC remains the converter's contract (no Unicode-normalization dependency is available); do not claim server-side NFC validation. Enforce depth, nodes, bytes and leaf-token limits during parsing, including standalone use.
- **Matching correctness:** always pair compatible roots. An I0 pair must not stop traversal of edited descendants. Reject duplicate/ambiguous Eids as evidence rather than overwriting an index entry. Bulk subtree pairing must respect already-established pairs. I2 near equality never implies equal shape; do not pair near subtrees positionally. S3's content-equality Myers cannot pair edited text: use explicit bounded contextual substitution within matched-parent gaps, with deterministic kind-compatible pairing. Emit attribute changes on leaves as well as containers.
- **Showcase calibration correction:** the original eight-token cutoff produces about 0.33 shingle Jaccard for the showcase word replacement, making the required move+modify impossible at the specified score. Use character 4-grams through 16 tokens and normalize per-kind active scoring features (leaf: text/format/size with contextual evidence; container: child/context/format/size). Freeze regression fixtures before measuring; report these choices in the measurements rather than claiming the original defaults passed.
- **Canonical identity:** encode all digests as fixed-width lowercase hex strings on the JSON wire; use domain-separated, length-delimited hash inputs and sorted deterministic iteration. Change IDs include the complete operation payload and graph input context, preventing left/right edits to one node from sharing an ID. Graph IDs must cover every semantic input including identity evidence when used, normalized semantic options, algorithm version, and ordered three-way inputs. Exclude deadlines, timings and request counters from immutable graph bytes. Budget-dependent output requires deterministic budgets in the identity; cancellation must publish nothing.
- **Lossless text edits:** token indices alone cannot describe partial-token character edits or code whitespace. Specify explicit code-point spans alongside token ranges, retain separators, and validate overlap/bounds. Word and character modes must reproduce exact target text and formatting. Three-way formatting reprojects through the merged token mapping; replacing the whole run list twice is not a merge.
- **Planner authority:** validate graph/base identity and all references before applying. Compute dependencies from operations, not untrusted hint arrays. Distinguish static conflicts from conflicts conditional on accepted escape moves. Structural cycles and deleted destinations are unresolved selections, so the subset law applies to structurally valid selections (not merely those passing hint checks). Placement must not reverse multiple operations sharing a fallback anchor. Test descendant escapes, inserted containers, rejected predecessors, and opposing moves explicitly.
- **Admission:** distinguish per-input bytes from total in-flight bytes. Add `MAREKVS_DIFF_INFLIGHT_BYTES`, default at least `3 * MAX_BYTES * CONCURRENCY` (checked arithmetic), so default COMPARE/MERGE3 can be admitted. Account separately for bounded tree/graph/candidate/cache allocation overhead. Worker jobs own admission guards until execution ends, even if callers disconnect or time out; cancel on drop and shutdown, use a bounded queue, and release memory only after the job releases it.
- **Immutable guard coverage:** inventory every mutation path, including all JSON writers, APPEND/SETRANGE/GETDEL/GETEX, collection writers, multi-key destinations, Lua and protocol-specific replacement paths. Validate all keys before multi-key writes. Replication remains outside this convention.
- **IMPORT paths and forks:** moving an array container requires transcribing every descendant record under the new Eid prefix; a marker-only move loses its content. Measure delta size against moved subtree records, not an arbitrary six-record ceiling. `FROM prev` must initialize/replace a distinct destination fully. Raw uploads have no I0 evidence; verify carried identity on a subsequent branch-to-branch comparison. Derived Eid HLCs must occupy a low reserved range to preserve ordinary insert ordering in snapshot forks; origin zero alone does not guarantee ordering.
- **Graph allocation:** charge emitted anchors/serialized bytes during construction; the change count alone does not bound quadratic predecessor lists.
- **Publication and mutation races:** publication must validate destination content/type, write the complete artifact before its visible head, and recover/verify partial artifacts. APPLY looks up an existing request result before inspecting current decisions or snapshots, and returns it immediately after validating the graph association. APPLY rechecks the decision revision in its publication closure and binds a request token to graph plus decision revision. IMPORT rechecks its captured source/destination revision before writing; concurrent edits must not be silently overwritten. Define retry/mismatch errors and test races deterministically.
- **Decision records:** use one scalar JSON-encoded tuple per change, not an unescaped pipe-delimited string (principals may contain pipes). Validate every pair before any write; duplicate IDs are errors. A concurrent MERGE3 retry must not reset existing decisions. Missing decisions mean pending.
- **Verification commands:** run `cargo test -p marekvs-diff canonical` and `cargo test -p marekvs-diff hash` separately; Cargo takes one test filter. Run `just chaos diff_negotiation` (the `chaos-docker` recipe takes no arguments). Measure actual emitted candidate pairs against independently computed true shingle Jaccard, with fixed seeds and a declared sample distribution; the LSH probability formula alone is not measured recall.

Execution progress is recorded separately; unchecked tasks are not implemented. Existing unrelated untracked files are preserved. No release, push, or tagging is part of this work.


## Execution order and gates

A builds the standalone core. B measures its correspondence choices. C introduces storage and command integration. D adds decisions, application, merge and import. Do not substitute placeholders for an unfinished stage. Keep a task unchecked until its behavior and checks pass. The session ledger is `.superpowers/sdd/2026-09-13-diff-commands/progress.md`.

Baseline `just test` passed before implementation. The standalone gate requires the moved-and-edited showcase without Eids to produce exactly one move and one modify; accepting each alone must succeed. Engine work follows the standalone correctness/review gate. Performance claims must cite measured data, not the theoretical LSH curve.

## Phase A — standalone algorithms

### A.1–A.2: Model, budgets, canonical serialization and hashes

- [x] Register `crates/marekvs-diff` in the workspace and expose implemented modules only.
- [x] Implement arena `Tree`, `Node`, `Kind`, scalar `Attrs`, Unicode-scalar `Run`/`Text`, and fixed-width hexadecimal `Sid`/`Lid` wire types.
- [x] Perform iterative depth/node/byte/token preflight before recursive parsing. Require a doc root; reject malformed/unknown fields, noncanonical whitespace/runs, empty marks, and integers not exactly representable by storage.
- [x] Compute versioned, length-delimited canonical content serialization; derive snapshot and logical IDs independently of source Eids. Compute exact/content subtree hashes and token weights in postorder.
- [x] Add cancellation/deadline and deterministic work/resource budgets. Validate options, normalized finite weights, theta and 128-slot banding before comparison.

Files: `Cargo.toml`, crate manifest, `src/{lib,model,canonical,hash,budget}.rs`.
Checks: model boundary, canonical identity, Unicode hard breaks, whitespace-preserving code, distinct position IDs, exact/content formatting hashes, numeric-storage and empty-mark regressions.

### A.2b: Record adapter and identity binding

- [x] Materialize visibility-gated `NodeIn` records using the core JSON/RGA reader. Retain dead array anchors for ordering and exclude them from semantic content.
- [x] Bind semantic child ordinals to source Eids through the materializer's array index; validate binding snapshot/paths. Keep binding separate from semantic tree cache entries.

File: `src/records.rs`. Checks: semantic roundtrip, equal SIDs with different Eids, dead anchors and orphan visibility.

### A.3–A.4 and A.9: Correspondence pipeline

- [x] Implement a safe one-to-one Matching table and layer accounting; pair roots and unambiguous I0 evidence first.
- [x] Match unique exact/content subtrees without overwriting preassigned descendants; continue traversal beneath edited I0 parents.
- [x] Generate deterministic capped near candidates, fixed-seed minhash/LSH, child overlap and bounded neighbour rescue; assign mutual best pairs. Stream and charge signature work before allocation, with cancellation inside hashing.
- [x] Implement bounded patience/Myers sibling alignment and explicit kind-compatible substitution in unmatched contextual gaps. Do not infer identical descendant shape from near matching.
- [x] Apply the measured short-text cutoff and active-feature scoring correction described above.

Files: `src/{matching,exact,near,align}.rs`. Checks: root replacement, ambiguous Eids, content-only formatting, repeated clauses, crowded buckets, short cross-parent moves, unequal child counts, signature cancellation and deterministic budget fallbacks.

### A.5–A.8: Classification, text, graph and planner

- [x] Classify minimal LIS moves, maximal unmatched subtree deletes, inserted shells, and independent text/format/attribute changes (including leaf attributes).
- [x] Implement lossless word/character/sentence edits in base code-point coordinates. Charge expansion/frontier memory before allocation; use exact coarse replacements when work is exhausted.
- [x] Define stable tagged graph wire types, complete-payload change IDs, graph-content IDs, predecessor chains, dependencies, conditional conflict hints, and explicit alternative groups.
- [x] Bound graph bytes, anchor counts, ancestor work and change counts during construction; exclude wall-clock metadata from immutable graph content.
- [x] Validate base/version/references and derived dependencies; resolve surviving nodes, escape moves, placement and ancestry before applying leaf operations. Preserve target order when several operations share a fallback anchor.
- [x] Implement bounded planning, exact formatting reprojection, keywise attribute composition, malformed payload rejection and deterministic output mapping.

Files: `src/{classify,textdiff,graph,plan}.rs`. Checks: independent move/edit; rotation; insertion dependencies; descendant escape; rejected anchor; opposing moves; foreign base/unknown refs; malformed Unicode wire refs; disjoint/overlapping edits; format boundaries and budgets.

### A.10: Three-way merge

- [x] Compare base→left and base→right. Preserve side attribution and hash ordered inputs into merge identity.
- [x] Collapse equal operations and identical insertions only at the same resolved parent/anchor; remap duplicate inserted descendant identities.
- [x] Compose independent text, attribute and formatting changes; preserve separately selectable operations. Detect overlap, competing zero-width insertion, same-key attribute conflicts and ambiguous intra-token formatting boundaries.
- [x] Validate combined structural changes. Report cycles/competing alternatives explicitly while retaining acceptance-dependent deletion/escape conflicts as conditional hints.

File: `src/merge3.rs`. Checks: unchanged/equal sides, independent edits, differing-parent insertions, partial-equal edit lists, competing formatting carried by modifications, opposing ancestor moves.

### A.11: Laws, adversarial corpus and CLI

- [x] Add `diffcli compare`, `apply --accept`, and `merge3` using the same bounded library APIs.
- [x] Add golden showcase, rotation, formatting, attributes, repeated-clause and rewrite fixtures; oversized/deep input tests.
- [x] Run 256 cases each for all/none/self/deterministic roundtrip, Unicode text, and structural edit scripts including valid-subset determinism.
- [x] Finish independent review and its scoped regression verification.

Files: `src/bin/diffcli.rs`, `tests/{laws,corpus}.rs`, `tests/corpus/`.

Gate:

```sh
cargo test -p marekvs-diff
cargo clippy -p marekvs-diff --all-targets -- -D warnings
```

## Phase B — measure before tuning

### B.1: Candidate recall and assignment accuracy

- [x] Add an explicit `harness = false` benchmark with reproducible synthetic inputs, 1,000 samples per attainable Jaccard bin, separately for short and long text.
- [x] Compute true Jaccard independently from raw shingles. Measure actual candidate emission and ground-truth assignment, including false assignments.
- [x] Save methodology and release output in `2026-09-13-diff-commands-measurements.md`; initial recall in the 0.5–0.6 bin exceeds 85% for both distributions.
- [x] Rerun after final resource-bound changes; preserve observed failures and limitations.

```sh
cargo bench -p marekvs-diff --bench recall -- --report
```

### B.2: Scan/decode measurement

This depends on C.3, so execute as C.3b. Measure physical/live records, decoded bytes, shard scan time and pool decode time on representative documents. Record hardware and profile. Keep limits provisional unless supported by these measurements.

## Phase C — storage adapter and basic commands

### C.1: Key grammar, command plumbing and immutable guard

- [x] Implement strict one-tag branch/snapshot/graph/decision/result key parsing and full-key routing. Require matching tags for all referenced keys; reject malformed digests and empty/extra brace groups.
- [x] Guard all client mutations of snapshot, graph and result namespaces, including direct JSON/string/generic handlers, multi-key writes, Lua dispatch and destination replacement paths. Prevalidate all affected keys before any write.
- [x] Register commands in dispatch, write-stop classification, command metadata and key extraction. Keep CPU-heavy commands out of the pipeline parallel whitelist and reject unsupported script execution before side effects.
- [x] Verify ordinary reads and documented FLUSH exemptions; replication remains outside the client guard.

Files: engine `cmd/diff/{mod,keys}.rs`, `cmd/mod.rs`, `cmd/command_docs.rs`, `lib.rs`, relevant mutation handlers; tests `tests/diff.rs`.

### C.2: Deterministic record primitives

- [x] Add `element_add_with_dot`: fresh envelope, caller-supplied deterministic dot, unchanged merge semantics. Test byte-identical payloads under different envelopes and idempotent merge.
- [x] Add path-aware JSON decomposition without changing existing call sites. The callback receives containing array path and ordinal.
- [x] Derive collision-checked snapshot Eids/dots deterministically. Reserve low identity clocks compatible with later ordinary inserts into forks; never observe derived values into the HLC.

Files: core `merge.rs`, `json.rs`; core unit/merge tests.

### C.3–C.4: SNAPSHOT and FORK

- [x] Scan through existing head/TTL/parent/RGA gates with physical/live/byte bounds enforced during scanning. Capture all same-tag inputs in one shard closure.
- [x] Build semantic trees and bindings on the admitted worker lane. Snapshot keys suppress I0 binding.
- [x] Write canonical JSON records using deterministic payload identities and fresh destination-safe envelopes; publish complete artifacts and verify/repair partial publication. Readers verify expected SID, not only head presence.
- [x] FORK preserves live Eids and dead array anchors, refreshes map dots/envelopes, preserves destination delete clocks, drops source TTL, and requires REPLACE when overwriting.
- [x] Test expiry/shadowing, deleted/reused destinations, partial artifact recovery, two-node payload identity, fork-after-tombstone, and insertion into snapshot forks.
- [x] C.3b: record scan/decode measurements described in B.2.

Files: engine `cmd/diff/{snapshot,fork}.rs` and shared helpers.

### C.5–C.6: Pool, admission, cache and metrics

- [x] Dedicated bounded thread/queue pool with request and total-byte admission before shard work. Separate per-input MAX_BYTES from INFLIGHT_BYTES; defaults admit a three-input request.
- [x] Workers own guards through allocation lifetime. Disconnect/deadline cancellation must not release capacity while work continues. Bound queue waiting and avoid blocking Tokio sends.
- [x] Cache immutable semantic trees by SID, excluding bindings, with conservative byte accounting and LRU eviction.
- [x] Add operation/result, stage duration, records, in-flight bytes, cache hits, queue wait and rejection metrics; read configuration through existing conventions with checked arithmetic.
- [x] Test default admission, exhaustion, cancellation, caller drop, capacity restoration, cache isolation and eviction.

Files: engine `cmd/diff/{pool,cache}.rs`, `metrics.rs`, `lib.rs`.

### C.7: COMPARE, HASH and STATS

- [x] COMPARE parses LEVEL/THETA, admits work, ensures locality, captures both inputs atomically on the shard, compares on the pool, and publishes source snapshots plus immutable graph.
- [x] Validate existing artifact content and repair incomplete snapshots; concurrent equal publishers store equal payloads. Graph events follow successful publication.
- [x] Return `[full graph key, graph JSON]`. HASH returns exact/content/weight for an input/path; STATS exposes counters.
- [x] Test branch I0, snapshot-unbound comparisons, deterministic repeated publication, cross-tag rejection, limits/busy/timeout, partial replication and competing publishers.

Files: engine `cmd/diff/compare.rs`, tests.

### C.8: Design and operator documentation

- [x] Finish `design/19-diff.md`, `docs/diff.md`, design/doc navigation and README feature entry.
- [x] Add verified configuration defaults to design/05, the existing defaults source of truth. Document snapshot-local freshness, guard scope, scan limits and measured calibration.

## Phase D — decisions, application, merge, import

### D.1: DECIDE and DECISIONS

- [x] Syntax: `DIFF.DECIDE graph [BY principal] id accept|reject|pending ...`. Validate graph, every ID/state, duplicates and complete arity before any writes.
- [x] Persist each decision atomically as a scalar JSON-encoded tuple; principal strings need no delimiter escaping. Missing entries are pending. Publish decision events after writes.
- [x] DECISIONS returns the decision view, canonical revision digest, and all unresolved groups from authoritative planner validation.
- [x] Test independent concurrent decisions, same-change LWW attribution, arbitrary principals, invalid-batch atomicity and dynamic unresolved groups.

### D.2: APPLY

- [x] Syntax: `DIFF.APPLY graph REQUEST token [DECISIONS revision]`.
- [x] Look up a stored request result first; verify graph association and return it even if current decisions/snapshots changed. Recheck within publication for concurrent local callers.
- [x] Capture graph, complete required snapshots and decisions. Reject a requested stale revision. Plan/apply on the pool; unresolved selections write no result.
- [x] Recheck decisions before publishing the result snapshot and immutable request record with base→result mapping. Bind request identity to graph/revision; define conflicting token reuse explicitly.
- [x] Test retries after decision changes and missing snapshots, stale requested revisions, a deterministic capture-to-publication decision race, unresolved groups, equal result SIDs on two nodes and result metadata semantics.

### D.3: MERGE3

- [x] Capture/admit three inputs as one request, run core merge, and publish all snapshots and the merge graph with origin/conflict metadata.
- [x] Initialize non-conflicting suggestions accepted and alternatives pending without overwriting existing human decisions, including publication from a lagging replica. Prefer defaults in immutable graph metadata over mutable initialization that could win LWW.
- [x] Test repeated MERGE3 after DECIDE and concurrent initialization, plus three-way graph application.

### D.4: IMPORT

- [x] Syntax: `DIFF.IMPORT branch tree [FROM previous] [THETA value]`, default theta 0.7.
- [x] Validate tree and same-tag source/destination before work. Capture source/destination revisions, compare, and recheck both before publication; stale input must not overwrite concurrent changes.
- [x] Persist both source/upload snapshots and graph. Apply identity-preserving deltas: unchanged records untouched, edits cover observed map dots, moved containers migrate complete descendant prefixes, deletions tombstone, new content receives fresh Eids.
- [x] FROM a distinct source initializes/replaces the destination fully; equal semantic upload is a no-op only if the destination already has the required state.
- [x] Test repeated zero-write import, subtree moves, fresh destination, unrelated existing destination, stale races, attrs/format-only changes and later branch I0 evidence. Delta bounds are proportional to affected subtree records.

Files for D.1–D.4: engine `cmd/diff/{decide,apply}.rs`, dispatch/classification/metadata, `tests/diff.rs`.

### D.5–D.6: Integration, chaos and docs

- [x] Add `diff_negotiation` to the existing chaos scenario runner: concurrent conflicting decisions across a partition, convergence to the same unresolved groups, rejection of one alternative, then equal applied SIDs and documents on healed replicas.
- [x] Include engine-level payload-identity and partial-publication tests; a semantic JSON comparison alone does not prove byte identity.
- [x] Finish command syntax, replies, error table, configuration, script/pipeline limitations, import concurrency, request-token and merge-default semantics in docs.

```sh
cargo test -p marekvs-engine --test diff
just ci
just chaos diff_negotiation
```

## Completion criteria

- All Phase A tests and independent review regressions pass; CLI uses the same implementations.
- Final measured recall meets the declared gate on the documented corpus, and scan/decode limits have evidence or remain explicitly provisional.
- All ten DIFF verbs are registered, documented and covered by engine integration tests; no placeholder handlers.
- Immutable payloads are deterministic and incomplete publication cannot masquerade as a valid DIFF input.
- Decision/application/import race tests and the negotiation chaos scenario pass.
- `just ci` passes. Report any unavailable infrastructure honestly; do not mark an unrun check passed.


## Final review and validation

Independent core and engine reviews produced scoped regressions for bounded
character/signature work, graph metadata identity, immutable publication,
merge formatting, cumulative decision-record budgets and concurrent JSON/string
type visibility. These fixes are included. Final endpoint review also verifies
raw root accounting, malformed live decision rejection, and a deterministic
APPLY race that returns DIFFSTALE without writing a request result. Decision batches precompute merged
record sizes before writing, and immutable string publication verifies the
stored bytes before reporting success.

- Standalone corpus, Unicode and structural property tests pass; final recall
  reproduces the recorded results. Scan/decode defaults remain provisional.
- Engine suites cover storage, identity-preserving import, runtime admission,
  immutable guards, decisions, request retry and record-level replication.
- `diff_negotiation` is implemented; shell syntax and the strict oracle were
  verified against fixtures. `just chaos diff_negotiation` was attempted but
  stopped before cluster startup because the Docker daemon was unavailable at
  `/Users/yannick/.docker/run/docker.sock`. Its live convergence check is unrun.
- `just ci` passed: formatting, workspace Clippy with warnings denied, all
  workspace unit/integration/doc tests, and the grudge topology self-test.

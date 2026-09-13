# 19 — Structured document comparison

`marekvs-diff` compares canonical document trees and produces suggestions that
can be accepted independently. The storage adapter exposes these algorithms
through `DIFF.*`, using ordinary replicated records and one hash tag per
document history. The implementation plan and measured calibration are in
`docs/superpowers/plans/2026-09-13-diff-commands*.md`. The operator-facing
[command reference](../docs/diff.md) documents all ten verbs and reply shapes.

## Canonical trees

Nodes have a kind (`doc`, `sec`, `par`, `sen`, `list`, `li`, `tbl`, `row`,
`cell`, `code`), scalar attributes `a`, and ordered children `c`. Sentence
and code leaves have text `x` and optional formatting runs `f`. Runs are
`[start, end, attributes]` in Unicode scalar coordinates. They are sorted,
nonoverlapping, contain at least one mark, and adjacent equal runs are merged. Supported marks are
boolean `b`, `i`, `u`, `s`, `code`, and string `link`.

The parser rejects unknown fields, malformed nodes/runs, excessive depth,
node counts, bytes, and leaf tokens. Integer attributes are signed 64-bit;
larger numeric IDs must be strings to avoid the storage codec rounding them. Sentences have trimmed single-space
whitespace, with explicit newline hard breaks. Code preserves whitespace.
NFC normalization is the converter's contract; the server does not claim to
validate NFC. Omitted empty children/attributes/formatting have one semantic
representation.

A versioned, length-delimited serialization in document order determines the
128-bit snapshot ID, `sid`. Envelope timestamps, source array IDs, and
invisible records do not affect it. Logical node IDs derive from the snapshot
ID and ordinal path. Snapshot and graph digests use 32 lowercase hexadecimal characters on the
wire; change IDs use the `c:` prefix followed by the same digest width.

## Correspondence and changes

The comparison stages are:

1. Pair compatible roots and unambiguous source array identities (I0).
2. Match unique exact/content subtrees (I1), respecting existing pairs.
3. Generate bounded near candidates using fixed-seed minhash/LSH, matching
   child overlap, and neighbouring context; choose mutual best pairs (I2).
4. Align remaining siblings with bounded patience/Myers and explicit
   kind-compatible contextual substitutions (I3).
5. Classify insert, delete, move, text modification, formatting, and
   attribute changes. A longest increasing subsequence minimizes sibling
   moves. Moving and editing a sentence yields separate suggestions.

Character 4-grams are used through 16 tokens; longer leaves use three-word
shingles. Scoring normalizes the applicable features by node kind. These
choices correct the original proposal's inability to match its short
moved-and-edited showcase. The measurement document records the synthetic
distribution and observed recall; it is not a claim about arbitrary documents.

Text edits carry executable base code-point spans, preserving separators and
partial-word edits exactly. Token projections are display/merge aids, not the
sole executable coordinates. A work-budget fallback is a lossless coarse
replacement. Cancellation detected before publication prevents publishing a
graph; synchronous shard publication is not interrupted halfway through writes.

Graph and change IDs hash complete deterministic content with domain
separation. The graph identity includes algorithm/options, source identities,
operations, evidence statistics, and deterministic fallback outcomes. Thus
binding-dependent comparisons cannot write different bytes under one graph
ID. Deadlines and wall-clock timings are not graph content. Three-way graph
identity also includes the ordered left and right inputs. The engine seals the
complete stored MERGE3 wrapper under `marekvs-diff/stored-merge/v2`, clearing
the embedded GID before hashing its serialization. This binds outer base/left/
right SIDs, origins, conflicts and the executable graph. Reads recompute that
seal, so altered defaults or outer inputs cannot pass validation using an
unchanged inner graph. Two-way storage uses the core graph digest.

## Planning and three-way merge

The planner verifies the base, all references, inserted-parent dependencies,
explicit alternative conflicts, and the resulting ancestry. Stored conditional
conflict hints do not replace structural validation. An accepted move can
escape an accepted ancestor deletion; rejecting that move leaves the node
inside the deleted subtree. Insertions contain node shells, with child
insertions depending on their parents. Placement uses predecessor fallback
chains and stable target order, including shared fallback anchors.

Accepted leaf operations use base coordinates. Three-way merging deduplicates
equal changes, combines independent text/attribute/format changes, and reports
competing edits and structural cycles as unresolved groups. Formatting is
projected through text edits; ambiguous intra-token boundaries conflict.

The core laws are all-accepted round trip, none-accepted identity, empty
self-diff, and deterministic valid selections. Structurally invalid selections
return errors naming their changes. Tests include the independently acceptable
move/word-change showcase, generated structural scripts, Unicode, duplicates,
limits, and malformed graphs.

## Storage contract

All keys in one call share exactly one nonempty hash tag:

| Key | Contents |
|---|---|
| `doc:{tenant/document}:b:branch` | Mutable JSON working copy |
| `doc:{tenant/document}:s:sid` | Immutable semantic snapshot |
| `diff:{tenant/document}:g:gid` | Immutable graph string |
| `diff:{tenant/document}:d:gid` | Per-change decision JSON document |
| `diff:{tenant/document}:r:request` | Immutable application result string |

Snapshot scans use the JSON reader's head-clock, expiry, parent-type, and
RGA visibility rules. Source Eids belong to that scan's binding, not a cache
entry keyed only by semantic `sid`. A comparison of stored snapshot keys
carries no I0 evidence.

Snapshot record payloads use deterministic element identities and OR-add
dots with fresh envelopes. Derived identity clocks occupy a low reserved range so later ordinary inserts
in snapshot forks retain their normal ordering. Identity values never advance
the HLC. Publication
must tolerate partial replication: DIFF readers verify semantic completeness,
and retrying publication repairs incomplete artifacts. A visible head alone
is not proof of completeness. Forks preserve live array identities and dead
anchors, with fresh destination clocks and no inherited TTL.

Immutable-key guards cover client mutation routes. They are a convention at
the command boundary, not protection against replicated records or an
operator's `FLUSHALL`.

Decisions store one scalar encoded tuple per change so state, principal, and
time cannot tear under LWW merge. Batch validation checks the full projected
merged CRDT byte size before writing, bounding cumulative decisions as well
as individual requests. The decision revision hashes the canonical
whole decision view. Missing decisions in two-way graphs are pending. In
MERGE3 graphs, nonconflicting suggestions default to accepted and conflict
alternatives default to pending. Defaults come from the sealed immutable
wrapper, not mutable initialization writes: explicit decisions, including an
explicit pending state, survive repeated or lagging merge publication.

APPLY first returns an existing result for its request token after checking
the graph association. Otherwise it plans against captured decisions, rechecks
the revision before publication, and writes the result snapshot and request
record. Request-token identity is scoped to the history tag and graph; a
recorded retry returns before current decisions or inputs are inspected. A
fresh token applies the current decision revision. A revision mismatch cannot
silently apply stale decisions. Different
request tokens may point to the same semantic result snapshot.

IMPORT preserves identity and writes deltas. Moving a container transcribes
all descendant paths under its new element identity; a marker-only insertion
would lose the subtree. A distinct FROM destination is initialized/replaced,
and publication rechecks captured state to avoid overwriting concurrent edits.

## Execution and limits

CPU work runs on a dedicated bounded pool. Admission precedes storage reads;
per-input byte limits and total in-flight capacity are distinct. Jobs retain
admission ownership until their allocations are released even when a client
disconnects. Parsing, candidates, edit work, graph anchors, output, cache, and
record scans each have bounds. Snapshot and graph publication remain shard
operations; semantic comparison does not run on shard threads. Deserialization
and digest verification of bounded stored graph strings remain shard work.
Request deadlines are cooperative checks, not preemption of a storage closure.
The command future cancels on drop, while independent admission references
in shard closures and worker jobs retain capacity through allocation lifetime.

Admission rejects immediately at capacity. The worker queue has CONCURRENCY
slots and its own deadline; at most twice that count of requests is admitted.
Input reservations, bounded algorithm allocation, and semantic-cache bytes are
distinct, so the in-flight gauge is not a total process-memory limit.

All DIFF verbs are excluded from Lua execution and the parallel pipeline
whitelist. The seven publishing/mutating verbs obey disk and compaction write
stops. Ordinary reads remain available; immutable client guards exempt
FLUSHDB/FLUSHALL and internal replication. APPLY request-token checks are local
shard serialization, not a cross-partition or disconnected-node lock.

The defaults table in design/05 is the operator source of truth. Snapshots
reflect what the executing node observed, not a cluster-wide freshness fence.

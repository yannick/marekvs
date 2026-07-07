---
title: JSON documents
description: JSON.* — RedisJSON-compatible documents stored as per-path CRDTs, so concurrent editors on different nodes merge instead of clobbering each other.
status: stable
---

The `JSON.*` family stores JSON documents with the full RedisJSON v2 command
surface — but unlike a module bolted onto a single-node store, marekvs
decomposes every document into **one record per JSON path**. Concurrent
writers on different nodes merge structurally: edits to different fields
both survive, concurrent array appends all land without interleaving, and
deletes never resurrect.

```text
JSON.SET doc $ '{"title":"Plan","tags":[],"meta":{"owner":"ada"}}'
JSON.SET doc $.title '"CRDT plan"'
JSON.ARRAPPEND doc .tags '"marekvs"' '"crdt"'
JSON.NUMINCRBY doc .meta.rev 1
JSON.GET doc $.tags
JSON.DEL doc $.meta
```

## Command reference

| Command | Notes |
|---|---|
| `JSON.SET key path value [NX\|XX]` | Root creates the doc; non-root paths update every match or create one new object key (static paths only). Subtree replace is atomic on the owning shard. |
| `JSON.GET key [INDENT i] [NEWLINE n] [SPACE s] [path ...]` | `$…` paths reply arrays of matches; legacy paths reply the bare value; multiple paths reply an object keyed by path. |
| `JSON.MGET key [key ...] path` | Per-key values, `nil` for missing docs. |
| `JSON.MSET key path value [key path value ...]` | **Not atomic across keys** (documented AP gap). |
| `JSON.DEL / JSON.FORGET key [path]` | Root deletes the doc; paths delete every matched subtree. |
| `JSON.TYPE key [path]` | `object array string integer number boolean null`. |
| `JSON.NUMINCRBY / JSON.NUMMULTBY key path value` | LWW under concurrent writers (like `HSET` on the same field). Integral results normalize to integers. |
| `JSON.STRAPPEND key [path] '"str"'` / `JSON.STRLEN key [path]` | String ops. |
| `JSON.ARRAPPEND key path v [v ...]` | RGA sequence: concurrent appends from different nodes all survive, each run contiguous. |
| `JSON.ARRINDEX key path v [start [stop]]` | First occurrence by JSON equality. |
| `JSON.ARRINSERT key path index v [v ...]` | Inserts before `index` (negative = from the end). |
| `JSON.ARRLEN / JSON.ARRPOP / JSON.ARRTRIM` | Pop clamps out-of-range indexes; trim keeps the inclusive range. |
| `JSON.OBJKEYS / JSON.OBJLEN key [path]` | Keys are reported in lexicographic order. |
| `JSON.TOGGLE key path` | Boolean flip. |
| `JSON.CLEAR key [path]` | Empties containers, zeroes numbers. |
| `JSON.MERGE key path value` | RFC 7386 merge-patch; decomposes into per-field deltas (a merge touching one field replicates one field). |
| `JSON.RESP key [path]` | RESP-structured view. |
| `JSON.DEBUG MEMORY key [path]` | Stored-record bytes under the path (includes CRDT metadata). |

`TYPE` reports `ReJSON-RL` (RedisJSON's module type name) so type-sniffing
clients recognize docs; `OBJECT ENCODING` reports `json`. `EXPIRE`/`TTL`/
`PERSIST`/`RENAME`/`COPY` work like any other collection type.

## How it works — the document CRDT

Most JSON stores treat a document as one value. In an AP, leaderless store
that is fatal: two nodes that each change one field of the same document
would race whole blobs, and last-writer-wins would silently discard one
side's edit entirely. marekvs avoids the race by never storing the blob.

### One record per path

`JSON.SET doc $ '{"title":"Plan","tags":["a"],"meta":{"owner":"ada"}}'`
writes **six** records, not one:

```text
head record            type = json, delete clock
""                     Obj                 ← the root
"title"                Str("Plan")
"tags"                 Arr
"tags"/elem(e1)        left=HEAD, Str("a") ← array element, stable id e1
"meta"                 Obj
"meta"."owner"         Str("ada")
```

Each record has its own version (a hybrid logical clock + writer id) and its
own merge rule. A deep edit like `JSON.SET doc $.meta.owner '"bo"'` ships
exactly one small record; replication, anti-entropy (Merkle digests), and
conflict resolution all happen at field granularity. Two nodes editing
different fields never even touch the same record.

### Object fields: observed-remove maps

Every map entry carries a tiny lattice of **dots** — `(clock, writer)` pairs
identifying the writes it has absorbed — the same machinery marekvs already
uses for set members and hash fields (an ORSWOT). The rules:

- a **write** covers the dots it *observed* and adds one fresh dot;
- a **delete** covers the dots it observed — and nothing else.

That asymmetry is the whole trick. Walk through the classic race — node A
deletes `owner` while node B concurrently overwrites it:

```text
                A                            B
      sees owner@dot₁            sees owner@dot₁
      JSON.DEL  $.owner          JSON.SET $.owner '"bo"'
      → covers {dot₁}            → covers {dot₁}, adds dot₂
```

After the records merge (in either order): `dot₁` is covered by both sides —
dead. `dot₂` was never observed by A's delete, so it survives. Converged
result on every node: `owner = "bo"`. The delete removed exactly what it
saw; it cannot reach into the future. If instead both sides *write*, both
dots survive and the higher `(clock, writer)` pair is the visible value —
plain last-writer-wins, but with the loser retained in the lattice so merge
order can never matter.

### Arrays: RGA sequences

Array indexes are unstable under concurrency — "insert at position 3" means
different things on nodes with different local views. So elements are never
addressed by index internally. Each element gets a **permanent id** (its
creation clock + writer) and stores a pointer to its **left neighbor** at
insert time. An array is a linked forest:

```text
HEAD ◄── e1("a") ◄── e2("b")        node A appends: e3 anchored on e2
                                    node B appends: e4 anchored on e2
```

Materialization walks from HEAD; when two elements share an anchor (e3 and
e4 above), the one with the **higher clock sorts first, together with
everything chained behind it**. That tie-break is what makes concurrent
*runs* of appends stay contiguous — if node A appended `x1,x2,x3` while
node B appended `y1,y2`, the healed array is `…,y1,y2,x1,x2,x3` (or the
reverse), never an interleaving like `y1,x1,y2,…`. This is the RGA
(Replicated Growable Array) construction from the sequence-CRDT literature.

Commands still speak indexes — `JSON.ARRINSERT doc $.tags 2 '"x"'` resolves
index 2 against the *local* materialized array, then emits a delta anchored
on the element id it found there. Replicas apply it to the same element no
matter what their local index for it is.

Deleting an element tombstones it **but keeps its record**: other elements
may name it as their left anchor, so a dead element still orders its
neighbors — it just renders nothing. These anchors are the one permanent
cost of the model (~35 bytes per deleted element, reported by `JSON.DEBUG
MEMORY`; a rewrite of the array — root `JSON.SET`, `RENAME`, `COPY` —
re-decomposes fresh and sheds them).

### Subtree deletes and the visibility rule

`JSON.DEL doc $.meta` covers the observed dots of `meta` **and every record
underneath it** (one prefix scan — a child's storage key literally starts
with its parent's). On read, a record is visible only if its own state is
live *and* its parent materializes as the right container type. A stale
leaf under a deleted or retyped branch is an orphan: skipped, by the same
deterministic rule, on every replica.

One consequence worth internalizing: **a leaf write does not re-assert its
ancestors**. If node A deletes `$.meta` while node B writes
`$.meta.fresh`, the delete wins — B's leaf survives in storage but its
branch is gone, so it does not render. Re-creating the branch itself
(`JSON.SET doc $.meta '{…}'`) is what resurrects it. The alternative —
every write re-adding its whole ancestor chain — would multiply write
amplification by path depth and make subtree deletion nearly impossible to
express.

### Why this converges

Every record kind is a join-semilattice: dot lattices union, LWW registers
take the max version, tombstones absorb. Merging is commutative,
associative, and idempotent, so any node can apply any record in any order
— replication delivery order, anti-entropy repair, and bootstrap all
produce the same bytes. This is not aspirational: the laws are
property-tested (`crates/marekvs-core/tests/json_laws.rs` folds random
multi-writer histories in every rotation/reversal and asserts identical
stored bytes *and* identical materialized documents), backed by dual-node
merge simulations and a network-partition chaos scenario.

## Multi-writer semantics

Documents converge to the identical bytes on every node, whatever the
delivery order. The merge rules:

- **Different paths** — both edits survive (per-path records).
- **Same path** — last-writer-wins by hybrid logical clock, like plain `SET`.
- **Update racing a delete of the same value** — the update wins (add-wins:
  the delete only covers what it observed).
- **Edit inside a subtree racing `JSON.DEL` of the subtree** — the delete
  wins. Re-creating the subtree root itself (e.g. `JSON.SET doc $.meta {…}`)
  is what resurrects the branch, not a leaf write inside it.
- **Concurrent `ARRAPPEND` runs** — all elements survive; each writer's run
  stays contiguous (no interleaving); the later run sorts first.
- **Concurrent `ARRPOP` of the same element** — both callers get the value;
  the element is removed once.

```note
marekvs is AP: replicas may briefly disagree while replication catches up,
and the rules above decide the converged result — not an error. If your
workload needs a conflict to be *rejected* rather than merged, JSON is the
wrong primitive; use [budgets](../budget/) for guarded quantities.
```

## Differences from RedisJSON

- `JSON.MSET` is not atomic across keys.
- Object keys come back in lexicographic order, not insertion order.
- Numbers are `i64` / IEEE `f64` (no arbitrary precision); integral results
  of `NUMINCRBY`/`NUMMULTBY` print as integers.
- `$…` paths use strict RFC 9535 JSONPath; RedisJSON's non-standard filter
  laxities are not reproduced.
- `JSON.DEBUG MEMORY` counts stored record bytes (CRDT metadata included),
  not allocator bytes.
- Deleted array elements leave small permanent ordering anchors (~35 bytes
  each) so replicas can never disagree about element order; `JSON.DEBUG
  MEMORY` includes them.
- RESP3 replies mirror the RESP2 shapes.

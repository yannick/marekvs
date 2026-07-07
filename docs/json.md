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

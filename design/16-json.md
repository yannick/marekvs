# 16 — JSON Documents (`JSON.*`)

A RedisJSON-compatible command surface over a **per-path CRDT document
model**: every JSON path is its own storage record, so concurrent editors on
different nodes merge structurally instead of clobbering whole documents.
This is what makes `JSON.*` viable in an AP store — the reason the family
was originally parked as "module territory" (design/03) was the assumption a
document is one blob; decomposed per-path it rides the existing element
machinery (per-element keys, ORSWOT dots, Merkle anti-entropy) unchanged.

Design basis: the JSON-CRDT survey in `crdt-research/research-marekvs1.md §3`
(DSON/Melda delta-state JSON CRDTs) and the RGA design in `research2.md §3.2`.

## Data model

A document under user key `K` is a head-gated collection
(`head::CTYPE_JSON = 8`) whose elements live under `ikey::Tag::Json = b'j'`:

```text
head        [pid][b'M'][klen][K]                ctype = 8, del_hlc
json node   [pid][b'j'][klen][K][path-bytes]    root record = empty path
field seg   [0x01][varint len][field bytes]
array seg   [0x02][hlc u64 BE][origin u16 BE]   = the element's stable Eid
```

A node's key is a strict byte-prefix of every descendant's key: subtree =
prefix scan, and an ordered scan visits parents before children (single-pass
materialization).

**Two record kinds**, discriminated by the LAST path segment:

| Kind | Last segment | Envelope rtype | Payload | Merge |
|---|---|---|---|---|
| map entry / root | field (or empty) | `HashField` (reuse) | ORSWOT dot lattice; value bytes = one `JVal` | observed-remove, add-wins |
| array element | Eid | `List` (reuse) | `[left-Eid 10B][JVal]` | LWW by `(hlc, origin)`; tombstone flag |

`JVal` payload codec: `NULL/FALSE/TRUE/INT(i64 BE)/FLT(f64 BE)/STR/OBJ/ARR`
type byte. Containers carry no data — their children are child records.

No new `RecordType` (the 3-bit field is full) and no new merge routing:
`write_merged`, replication (commit hook), anti-entropy, bootstrap, TTL and
DEL all treat JSON records as the element kinds they reuse.

## Writes

- **Path assignment** (`JSON.SET $.a.b v`): `merge::element_set` — covers
  the observed dots of that path's record and installs one fresh add in a
  single record. Replaced subtrees get their descendants covered
  (`element_remove` of observed dots / array tombstones), then a fresh
  decomposition is written.
- **Array edits**: every element has a stable id `Eid = (hlc, origin)`
  (`Hlc::now()` is strictly monotone per process → cluster-unique) and a
  `left` anchor. RGA order: walk from the head sentinel, siblings sharing an
  anchor sort by `(hlc, origin)` **descending** — concurrent runs stay
  contiguous. Deletes tombstone the element **with its payload preserved**:
  a dead element still anchors ordering.
- **Stable addressing**: index paths (`$.a[3]`) resolve against the LOCAL
  materialized doc and ship deltas by record path / Eid, never by index —
  replicas apply to the same element even when their local indexes differ.
- Every handler runs its per-key work in one shard closure (node-locally
  atomic) and calls `ensure_local` first (cluster read-through; writes must
  observe the dots they cover).

## Reads

`JSON.GET` = prefix scan → `build_doc` (marekvs-core/src/json.rs):
tree rebuild with a deterministic **visibility rule** — a record renders iff
it is visible against the head delete clock, its OR state is live, and its
parent's winning value is the matching container type. Stale records under
retyped/deleted branches are orphans and skip identically on every replica.
No materialized-doc cache in v1 (correctness first; the scan is bounded by
doc size).

## Convergence semantics (the contract)

- Concurrent writes to **different paths**: both survive.
- Concurrent writes to the **same path**: LWW visibility among live dots
  (both retained in the lattice up to the dot caps).
- **Delete vs concurrent update of the same record**: observed-remove — the
  update's fresh dot survives (add-wins), the observed dots die.
- **Subtree delete vs concurrent edit inside the subtree**: the delete wins.
  A leaf write does not re-assert its ancestors' presence; only re-creating
  the branch node itself resurrects the branch. (Ancestor re-assertion would
  amplify every write by path depth and hollow out DEL.)
- **Concurrent ARRAPPEND on the same array**: all elements survive; each
  writer's run stays contiguous; later-HLC run materializes first.
- **Concurrent ARRPOP of the same element**: both callers receive it; the
  element dies once (idempotent tombstone).

Property-tested in `marekvs-core/tests/json_laws.rs` (permutation
independence of the record set, RGA order, and the materialized document;
run non-interleaving) plus dual-engine merge simulations in
`marekvs-engine/tests/json.rs` and the `json_convergence` chaos scenario.

## Tombstones and GC

Array-element tombstones are **exempt from `gc_grace` physical GC**
(`store::onda_ttl_for_keyed`): dropping one would dangle other elements'
left anchors and reorder the array. They are retained until the doc's
records are rewritten (RENAME/COPY re-decompose fresh; root JSON.SET covers
and supersedes). Map-entry tombstones keep the normal `gc_grace` window —
same resurrection story as hash fields. Owner-driven compaction (drop a
tombstone once nothing anchors on it, re-anchor otherwise) is future work.

Cost: ~35 bytes per deleted array element, bounded by delete count.
`JSON.DEBUG MEMORY` reports stored-record bytes including dead anchors.

## Path dialects

`$…` = RFC 9535 JSONPath (`serde_json_path`), multi-match, per-match array
replies. Anything else = legacy static path (`.a.b[3]`, `a["x"][-1]`),
single-match, bare replies. Static `$`-paths (names/indexes only) are also
write-capable (create-on-SET of a new object key). Legacy paths resolve
manually against the materialized doc — no query engine involved.

## Compatibility gaps vs RedisJSON (documented in docs/json.md)

AP semantics: LWW per path, add-wins vs delete-wins rules above, double-pop.
`JSON.MSET` is not atomic across keys. `OBJKEYS`/`GET` order object keys
lexicographically, not by insertion. Numbers are i64/f64. JSONPath dialect
is strict RFC 9535. `JSON.DEBUG MEMORY` counts stored record bytes.
Integral float results of `NUMINCRBY`/`NUMMULTBY` normalize to integers.
RESP3 replies mirror RESP2 shapes.

## Files

- `crates/marekvs-core/src/json.rs` — codecs, RGA, decompose/materialize
- `crates/marekvs-core/src/merge.rs::element_set` — cover + fresh add
- `crates/marekvs-engine/src/cmd/json/{mod,doc,path}.rs` — handlers, write
  helpers, path dialects
- `crates/marekvs-core/tests/json_laws.rs`, `crates/marekvs-engine/tests/json.rs`

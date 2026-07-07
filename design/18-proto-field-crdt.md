# 18 — Field-level CRDT merge for protobuf values (`PROTO.*`)

design/17 shipped protobuf typed values as **whole-message LWW**:
`PROTO.SETFIELD` decoded the stored message, mutated one field, re-encoded,
and wrote the whole message back — so two nodes concurrently setting
*different* fields of the same message clobbered each other. This design
closes that gap by **decomposing a protobuf value into one record per field
path**, reusing the JSON per-path CRDT machinery (design/16) unchanged, keyed
by **field numbers** so records survive schema field renames.

Design basis: the "structured element addressing" note in
`crdt-research/research3.md` — a protobuf field-number path as the element
address, giving field-granular deltas and Merkle repair.

## Data model

A decomposed value under user key `K` is the same head-gated collection as
before (`head::CTYPE_PROTO = 9`) whose field records live under a new tag
`ikey::Tag::ProtoField = b'p'`:

```text
head        [pid][b'M'][klen][K]                ctype = 9, del_hlc, fmt=2 tail
proto node  [pid][b'p'][klen][K][path-bytes]    root record = empty path
field seg   [0x01][varint len][varint field_number]     message field (NUMBER)
elem seg    [0x02][hlc u64 BE][origin u16 BE]           repeated element Eid
mapkey seg  [0x03][varint len][kind u8][canonical key]  map entry (bool/i*/u*/str)
```

A node's key is a strict byte-prefix of every descendant's key: subtree =
prefix scan, parents before children, exactly like `Tag::Json`. The codecs
live in `marekvs-core/src/pdoc.rs` (`PSeg`/`MKey`/`PVal`/`PArrElem`,
`encode_path`/`split_last`, and the descriptor-free `recompose_tree` used by
RENAME/COPY) — no prost dependency in core.

**Two record kinds**, discriminated by the LAST path segment (same rule as
JSON):

| Kind | Last segment | Envelope rtype | Payload | Merge |
|---|---|---|---|---|
| singular field / map entry / container marker / root | field / map-key / empty | `HashField` (reuse) | ORSWOT dot lattice; value bytes = one `PVal` | observed-remove, add-wins |
| repeated element | Eid | `List` (reuse) | `[left-Eid 10B][PVal]` | LWW by `(hlc, origin)`; tombstone flag keeps the anchor |

`PVal` leaf codec: `Bool/I32/I64/U32/U64/F32/F64` (bit-exact via `to_bits`,
NaN-safe) `/Str/Bytes/Enum(i32)` plus the container markers `Msg/List/Map`
(carry no data — children are child records). **No new `RecordType` and no
change to `merge_values`** — decomposed proto rides the existing element
machinery exactly like JSON.

`decompose_msg`/`build_msg` (the prost-reflect walkers in
`marekvs-engine/src/proto/fields.rs`) map a `DynamicMessage` to/from records.
Only **present** fields produce records: absence of a record is absence of the
field, matching wire encoding. Setting a proto3 non-presence scalar to its
default value removes the record (documented wire-format parity).

## Head format and upgrade-on-write

The proto head tail gains a format byte (`protohead.rs`):

- **`fmt=1`** — the legacy whole-message tail `[1][ver][schema][type][msg]`.
  Read-only; still decodes.
- **`fmt=2`** — the decomposed tail `[2][ver][schema][type]` (no message
  bytes; the value lives in the `'p'` records).

New `PROTO.SET`/`SETJSON` always write `fmt=2`. An existing `fmt=1` value is
**upgraded on the next write**: the first `PROTO.SETFIELD`/`CLEARFIELD`
transcribes the stored message into records, then applies the edit and
restamps the head `fmt=2` (fresh HLC, delete clock + TTL preserved).

**Upgrade stamping.** Transcription is stamped with the ORIGINAL head version:
every kind-A record carries the dot `(head.hlc, head.origin)`, and
repeated-element ids are derived deterministically as
`fnv1a64(head.hlc ‖ array_path ‖ ordinal)`. Two nodes upgrading from the same
`fmt=1` state therefore write **byte-identical records** (idempotent under
merge — no duplication). The upgrader's own edit uses a fresh `Hlc::now()` dot
`> head.hlc`, so a concurrent edit always beats the transcription: the
headline different-fields-survive property holds across the upgrade boundary.

## Semantics

**Materialization winners.** `build_msg` renders records against the head's
LWW-winning schema version's descriptor. Deterministic skip rules: unknown
field numbers, kind-mismatched records, and orphans under a missing/retyped
parent are skipped (the data stays stored and reappears if the head later
moves to a defining version).

**oneof.** The materialize-time winner is the live member with the highest
`(max live dot, field_number)`; losing members are skipped like JSON's
retyped-parent orphans. A local `SETFIELD`/`CLEARFIELD` on a member also
observed-removes the stored sibling-member records. proto3 `optional`
(a synthetic single-member oneof) is excluded from exclusion — member count
is the version-independent detector.

**RENAME/COPY** materialize + re-decompose with fresh element ids
(`recompose_tree`, descriptor-free — works even if the schema was deleted):
kind-A records are re-added in ascending original-dot order under fresh
monotone HLCs (so oneof winners and same-path LWW cannot flip), repeated runs
are re-chained cleanly, tombstones dropped.

## Documented anomalies

1. **Skewed-upgrade repeated duplication.** Upgrades from *different* observed
   legacy states union their repeated-field runs (converged, no loss, visually
   duplicated). Any whole-field write or root `PROTO.SET` self-heals it. The
   common case — both nodes upgrading from the *same* replicated `fmt=1` state
   — does not duplicate (identical derived ids).
2. **`PROTO.GET` byte instability for maps.** A `fmt=2` value with map fields
   re-encodes in `HashMap` iteration order, so the wire bytes are not stable
   across calls (spec-legal — map field order is not significant). Tests
   compare decoded messages, not bytes.
3. **Accretion past `max_value`.** A decomposed value can grow past the
   `max_value` limit via repeated `SETFIELD`s (each leaf is bounded, the whole
   is not) — the same property JSON documents have.

## Mixed-version clusters (upgrade ordering)

Old binaries cannot read `'p'` records, and their `fmt=1` head writes can
LWW-shadow a `fmt=2` value. **Upgrade the whole cluster before the first
decomposed write.** (A config gate on `fmt=2` writes is the escape hatch if
rolling support is later required — out of scope by decision.)

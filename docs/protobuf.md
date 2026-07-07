---
title: Protobuf values
description: PROTO.* — a server-side protobuf schema registry with prefix bindings, validated typed values, field access and JSON projection.
status: stable
---

The `PROTO.*` family turns marekvs into a **schema-aware store**: upload a
`.proto` schema once (plain text — the server compiles it), bind it to a key
prefix, and every value written under that prefix is **validated protobuf**.
The server can read single fields, patch fields atomically, and project any
value to canonical protobuf-JSON — no client-side codegen required.

## Five-minute tour

```sh
# 1. upload a schema (server-compiled; imports resolve from the registry)
redis-cli PROTO.SCHEMA SET orders SOURCE '
  syntax = "proto3";
  package shop.v1;
  message Order {
    string id = 1;
    uint64 total_cents = 2;
    repeated string tags = 3;
  }'
# → (integer) 1        ← schema version

# 2. bind it to a prefix — clients never pass the type again
redis-cli PROTO.BIND "order:" shop.v1.Order

# 3. write values (validated against shop.v1.Order)
redis-cli PROTO.SETJSON order:1001 '{"id":"1001","totalCents":"995","tags":["rush"]}'
redis-cli PROTO.SET     order:1002 "$RAW_PROTOBUF_BYTES"

# 4. read whole, by field, or as JSON
redis-cli PROTO.GET      order:1001            # raw protobuf bytes
redis-cli PROTO.GETJSON  order:1001            # canonical protobuf-JSON
redis-cli PROTO.GETFIELD order:1001 total_cents  # (integer) 995
redis-cli PROTO.GETFIELD order:1001 tags.0       # "rush"

# 5. patch fields atomically (decode → set → re-encode on the key's shard)
redis-cli PROTO.SETFIELD order:1001 total_cents 1495 tags '["rush","gift"]'
redis-cli PROTO.CLEARFIELD order:1001 tags.1
```

`TYPE` reports `proto`, and `OBJECT ENCODING` reports the message type:

```sh
redis-cli TYPE order:1001              # proto
redis-cli OBJECT ENCODING order:1001   # "shop.v1.Order"
```

## The schema registry

| Command | What it does |
|---|---|
| `PROTO.SCHEMA SET name SOURCE text` | Compile `.proto` source server-side (pure Rust, [protox]); returns the new **version**. |
| `PROTO.SCHEMA SET name DESCRIPTOR fds` | Upload a compiled, self-contained `FileDescriptorSet` (e.g. from `protoc --descriptor_set_out --include_imports`). |
| `PROTO.SCHEMA COMPILE …` | Dry run: compiles, returns the type names, stores nothing. |
| `PROTO.SCHEMA GET name [VERSION v] [SOURCE\|DESCRIPTOR]` | Metadata map, or the raw source / descriptor bytes. |
| `PROTO.SCHEMA LIST` / `TYPES name` | Registry index / fq type names. |
| `PROTO.SCHEMA DEL name` | Removes the schema from the index. **Old versions are retained** — values written against them keep decoding forever. |

[protox]: https://github.com/andrewhickman/protox

Schemas are versioned: every `SET` stores an immutable version record and
bumps the latest pointer. Values remember the exact `(schema, version)` they
were validated against.

Source imports resolve **from the registry**: upload `common.proto`'s schema
under the name `common` (or `common.proto`), and any later source can
`import "common.proto";`. The `google/protobuf/*` well-known types are
bundled — `import "google/protobuf/timestamp.proto";` just works.

The registry itself is stored as hidden replicated records (the same
mechanism as `SCRIPT LOAD`): it survives restarts, replicates like data, and
nodes that never saw an upload fetch it on demand.

## Prefix bindings

```sh
PROTO.BIND "user:"        shop.v1.Customer
PROTO.BIND "user:order:"  shop.v1.Order       # longest prefix wins
PROTO.BINDINGS MATCH "user:*"
PROTO.UNBIND "user:order:"
```

Typed commands resolve the message type in this order: explicit `TYPE`
argument → **longest**-prefix binding → `-NOBINDING` error. If the type name
is unique across the registry you can omit `SCHEMA`; ambiguity is an error.

Bindings are cached per node for ~2 s — after `PROTO.BIND` on one node,
other nodes may use the old binding for up to that long (AP semantics).

## Typed values

| Command | Notes |
|---|---|
| `PROTO.SET key value [TYPE t] [NX\|XX] [EX\|PX\|EXAT\|PXAT\|KEEPTTL]` | Validates the bytes; stores the message decomposed into per-field CRDT records. |
| `PROTO.GET key` | Message bytes, materialized from the field records (works without the schema for legacy values; needs it for decomposed ones). |
| `PROTO.INFO key` | `{schema, version, type, format, [records,] bytes}` — `format` is `whole` (legacy) or `fields` (decomposed). |
| `PROTO.GETJSON` / `PROTO.SETJSON` | Canonical protobuf-JSON in/out (64-bit ints as strings, bytes base64, enums by name). |
| `PROTO.GETFIELD key path…` | Scalars as native RESP types, enums as names, message/repeated/map as JSON; unset → nil. |
| `PROTO.SETFIELD key path value…` | Per-field CRDT writes on the key's shard; concurrent edits to different fields merge. |
| `PROTO.CLEARFIELD key path…` | Reset a field, remove a list element, delete a map key. |

Field paths are dot-separated: field names or numbers; a number after a
repeated field is an index, after a map field it's the key
(`items.0.note`, `scores.alice`, `by_id.5.name`; max 32 segments).

## Typed hash fields & set members

```sh
PROTO.HSET  order:byday f1 <bytes> f2 <bytes>   # validate → ordinary HSET
PROTO.SADD  order:pending <bytes>               # validate → ordinary SADD
PROTO.HGETJSON  order:byday f1
PROTO.HGETFIELD order:byday f1 total_cents
```

Elements are stored as **raw proto bytes in ordinary hashes/sets** — plain
`HGET`, `HGETALL`, `SMEMBERS`, `SCARD` etc. all work unchanged, and the
CRDT merge behavior of hashes/sets is untouched. The optional `TYPE t`
clause goes immediately after the key.

## Errors

| Code | Meaning |
|---|---|
| `-NOSCHEMA` | Unknown schema / version / type name. |
| `-SCHEMAERR` | Compile or upload failed (parse error, missing import, size limits, ambiguous type). |
| `-PROTOVALIDATE` | Value bytes / JSON do not decode as the resolved type. |
| `-NOBINDING` | No `TYPE` argument and no binding covers the key. |
| `-PROTOPATH` | Bad field path (unknown field, scalar descent, >32 segments, bad index/key). |

## Field-level merge

A protobuf value is stored **decomposed into one record per field path**
(keyed by field *number*, so records survive field renames), the same per-path
CRDT model as JSON documents. Two nodes that concurrently `PROTO.SETFIELD`
*different* fields of the same key both keep their edit after the partition
heals — the data-loss case whole-message LWW could not handle. Concurrent
edits to the *same* field are last-writer-wins by HLC; concurrent appends to
the same `repeated` field converge with both runs present and contiguous
(RGA); map entries are add-wins per key; `oneof` races resolve to a
deterministic winner on every node.

### Two clients, two nodes — both edits survive

```sh
# one-time setup (any node)
redis-cli -p 6379 PROTO.SCHEMA SET acme/user.proto SOURCE '
  syntax = "proto3"; package acme;
  message User { string name = 1; int32 age = 2; repeated string tags = 3; }'
redis-cli -p 6379 PROTO.BIND "user:" acme.User
redis-cli -p 6379 PROTO.SETJSON user:1 '{"name":"seed","age":1}'

# client A, connected to node 1 — concurrently with client B on node 2:
redis-cli -p 6380 PROTO.SETFIELD user:1 name ada    # client A
redis-cli -p 6381 PROTO.SETFIELD user:1 age 42      # client B

# after replication (or partition heal), on EVERY node:
redis-cli -p 6379 PROTO.GETJSON user:1
# {"name":"ada","age":42}
```

Under whole-message storage one of those writes would have silently erased
the other (decode → mutate → re-encode races the full message). With
field-level merge each `SETFIELD` ships one field record, so the writes
commute. The same holds for repeated fields — two clients appending
concurrently keep both elements:

```sh
redis-cli -p 6380 PROTO.SETFIELD user:1 tags.0 alpha   # client A appends
redis-cli -p 6381 PROTO.SETFIELD user:1 tags.0 beta    # client B appends, concurrently
# converged: BOTH elements present on every node, in the same deterministic
# order everywhere (the later hybrid-logical-clock write sorts first);
# multi-element runs from one client stay contiguous
```

`PROTO.INFO` reports `format: fields` for a decomposed value (and
`records`, the field-record count).

**Upgrade on write.** Values written before this feature are stored
whole-message (`format: whole`). They stay readable, and the **first**
`PROTO.SETFIELD`/`CLEARFIELD` upgrades them in place to the decomposed form
(TTL and delete clock preserved). No migration job, no opt-in flag.

> **Mixed-version clusters:** upgrade *every* node before the first field-level
> write. Old binaries cannot read the decomposed records, and a `PROTO.SET`
> from an old node can shadow a decomposed value. Roll the whole cluster first.

## Semantics & caveats (AP store)

- **`PROTO.GET` bytes are not stable for messages with maps.** A decomposed
  value re-encodes map fields in hash order (spec-legal — map field order is
  insignificant); compare decoded messages, not raw bytes.
- **A decomposed value can grow past `max_value` via `SETFIELD`** (each leaf is
  bounded, the accumulated message is not) — the same property JSON has.
- **Old values always decode.** Values pin their schema version; versions
  survive `PROTO.SCHEMA DEL`.
- Concurrent `PROTO.SCHEMA SET` of the *same name* on both sides of a
  partition: the latest pointer is last-writer-wins (version numbers can
  collide across the partition). Do schema administration from one place.
- Rebinding a prefix changes how stored **collection elements** are
  interpreted at read time; the stored bytes never change. Binding changes
  reach other nodes within a few seconds (each node re-reads the table at
  most every 2 s and a background warmer pulls bindings **and** compiled
  descriptors onto every node) — so typed writes keep working on every node
  even through a network partition, serving the last table it saw.
- `PROTO.*` cannot be called from Lua scripts in v1.
- Limits (env-tunable): source ≤ 1 MiB, descriptor set ≤ 4 MiB, value
  ≤ 4 MiB, ≤ 64 files per compile, import depth ≤ 16.

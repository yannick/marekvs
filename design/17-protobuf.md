# 17 — Protobuf schema registry & typed values (`PROTO.*`)

Clients store **schema-validated protobuf messages** in marekvs: upload
`.proto` source (compiled server-side with [protox], pure Rust) or a
precompiled `FileDescriptorSet`; bind schemas to key prefixes so clients
don't pass the type on every call; then write values that the server
validates, reads field-by-field, and projects to canonical protobuf-JSON —
all without any client-side codegen.

[protox]: https://github.com/andrewhickman/protox

## Command surface

| Command | Reply | Notes |
|---|---|---|
| `PROTO.SCHEMA SET name SOURCE text` | `:version` | server-compiles; imports BFS-resolved from the registry (`google/protobuf/*` bundled) |
| `PROTO.SCHEMA SET name DESCRIPTOR fds` | `:version` | uploads a compiled, **self-contained** FileDescriptorSet |
| `PROTO.SCHEMA COMPILE name SOURCE\|DESCRIPTOR …` | array of types | dry run — same pipeline, nothing stored |
| `PROTO.SCHEMA GET name [VERSION v] [SOURCE\|DESCRIPTOR]` | map / bulk | map `{name, version, kind, types, imports}`; the flags return raw source / fds |
| `PROTO.SCHEMA LIST` | map name→version | latest versions |
| `PROTO.SCHEMA TYPES name [VERSION v]` | array | fq message type names |
| `PROTO.SCHEMA DEL name` | `:0/1` | tombstones latest + index entry; **version records are retained** so stored values keep decoding |
| `PROTO.BIND prefix fq-type [SCHEMA name] [VERSION v]` | `+OK` | SCHEMA omitted → registry-wide type search; ambiguity errors |
| `PROTO.UNBIND prefix` | `:0/1` | |
| `PROTO.BINDINGS [MATCH glob]` | map | prefix → `{schema, version, type}` |
| `PROTO.SET key value [TYPE t] [NX\|XX] [EX\|PX\|EXAT\|PXAT\|KEEPTTL]` | `+OK` / nil | validates the bytes against the resolved type |
| `PROTO.GET key` | bulk | raw message bytes — no schema needed |
| `PROTO.INFO key` | map | `{schema, version, type, bytes}` from the stored header |
| `PROTO.GETJSON key` / `PROTO.SETJSON key json [TYPE t] [ttl opts]` | bulk / `+OK` | canonical protobuf-JSON (prost-reflect serde) |
| `PROTO.GETFIELD key path [path…]` | native / array | scalars → Int/Double/Bool/Bulk, enum → name, message/repeated/map → JSON; unset → nil |
| `PROTO.SETFIELD key path value [path value…]` | `+OK` | atomic RMW on the owning shard thread |
| `PROTO.CLEARFIELD key path [path…]` | `:n` | field reset / list-element removal / map-key removal |
| `PROTO.HSET key [TYPE t] field value…` | `:added` | validates every value, then **delegates** to `HSET` — elements stay raw bytes |
| `PROTO.SADD key [TYPE t] member…` | `:added` | same, delegating to `SADD` |
| `PROTO.HGETJSON key field [TYPE t]` / `PROTO.HGETFIELD key field path [TYPE t]` | bulk / native | decode one hash element at read time |

**Type resolution** for typed commands: explicit `TYPE` arg → longest-prefix
binding → `-NOBINDING`. Field paths are dot-separated segments — field names
or field **numbers**; a numeric segment after a repeated field is an index,
after a map field a key; ≤ 32 segments.

Errors (raw codes, `BUDGETEXHAUSTED` precedent): `-NOSCHEMA`, `-SCHEMAERR`
(parse/compile/limits/ambiguity), `-PROTOVALIDATE` (value fails decode),
`-NOBINDING`, `-PROTOPATH`, plus standard `-WRONGTYPE`.

## Value storage: head-only LWW record

A proto value is a collection head (`head::CTYPE_PROTO = 9`) with **no
element records**. The head payload is the standard
`[ctype u8][del_hlc u64 BE]` prefix followed by the `protohead` tail codec
(`marekvs-core/src/protohead.rs`, plain bytes — core has no protobuf
dependency):

```text
[fmt u8 = 1][schema_version u32 BE]
[varint nlen][schema utf-8][varint tlen][fq type utf-8][message bytes…]
```

Consequences, all inherited from existing machinery:

- **Whole-message LWW** via the ordinary head merge — one record, one
  ReplOp; replication, anti-entropy, bootstrap and TTL need no changes.
  Field-level CRDT merge (field-number paths, crdt-research/research3.md
  "structured element addressing") is deliberate future work.
- Values embed `(schema, version)` → every read decodes against the **exact
  version it was written with**, and `PROTO.SCHEMA DEL` keeps version
  records, so old values never go dark.
- `TYPE` → `proto`; `OBJECT ENCODING` → the fq message type name read from
  the tail; `EXPIRE`/`PERSIST` re-stamp the full head payload (tail
  preserved); `RENAME`/`COPY` carry the head verbatim with a fresh envelope;
  `DEL` is the standard head tombstone; plain `SET` shadows the proto value
  (standard Redis overwrite semantics).
- `PROTO.SET` carries the previous delete clock forward
  (`ensure_head` precedent) so stale pre-delete records arriving later via
  replication cannot resurrect.

`PROTO.SETFIELD`/`CLEARFIELD` are decode → mutate → re-encode → LWW write,
executed **on the key's shard thread** (node-local atomic RMW). The
descriptor pool is resolved asynchronously *before* the shard job; the job
re-checks the stored `(schema, version, type)` and retries when a concurrent
type change slipped in between.

## Registry storage: hidden replicated system records

Exactly the `SCRIPT LOAD` precedent (`\x00script:*`): ordinary data-CF
records under a `\x00` prefix — replicated like data, healed by
anti-entropy/read-through, filtered from `SCAN`/`KEYS`/`RANDOMKEY`/`DBSIZE`.
NOT the meta CF (node-local, never replicated).

```text
\x00proto:s:<name>             String: postcard(SchemaRecord) — latest
\x00proto:v:<name>:<%08x ver>  String: postcard(SchemaRecord) — immutable per version
\x00proto:idx                  Hash: name → latest version (ascii)
\x00proto:bind                 Hash: prefix → postcard(BindingRecord)
```

`SchemaRecord { version, kind (source|descriptor), source, fds, imports,
types, created_ms }` — `fds` is always a **self-contained** compiled
FileDescriptorSet (imports inlined at compile time).
`BindingRecord { schema, version (0 = track latest), type_name }`.

Versioning is a monotonic u32 per name: `SET` reads the latest record and
writes `latest + 1`. Concurrent same-name `SCHEMA SET` on both sides of a
partition is **LWW on the latest pointer** — a documented admin caveat, not
a data-safety issue (each value pins the exact version it validated
against).

Registry loads call `engine.ensure_local` on the exact hidden key first
(EVALSHA-fallback pattern), so a node that never saw the upload fetches it
from a home replica on demand.

## Compilation & source imports

`PROTO.SCHEMA SET … SOURCE` extracts top-level `import "x";` names
lexically, BFS-fetches them **from the registry** (an import name resolves
to the schema of the same name, with or without the `.proto` suffix), and
compiles with protox using a chained resolver: protox's bundled
`GoogleFileResolver` (well-known types) → the in-memory registry map.
Descriptor-upload dependencies contribute every file of their own
self-contained set. A missing import fails with
`-SCHEMAERR import 'x' not found (upload it first or use DESCRIPTOR)`.

protox compilation is CPU-bound and always runs in
`tokio::task::spawn_blocking` — never on shard threads.

## Caching & limits

- **DescriptorPool LRU** per `(schema, version)` — entries are immutable
  (per-version records never change) so there is no invalidation, only
  capacity eviction. `DescriptorPool` is Arc'd internally: clones are cheap,
  Send + Sync.
- **Binding table**: node-locally cached for `MAREKVS_PROTO_BIND_TTL_MS`
  (default 2000 ms), refreshed immediately after local BIND/UNBIND. Remote
  staleness ≤ TTL is a documented AP caveat.
- DoS bounds (env-tunable): `MAREKVS_PROTO_MAX_SOURCE` (1 MiB),
  `MAREKVS_PROTO_MAX_FDS` (4 MiB), `MAREKVS_PROTO_MAX_VALUE` (4 MiB — keeps
  records under the 8 MiB MAX_FRAME), `MAREKVS_PROTO_MAX_FILES` (64),
  `MAREKVS_PROTO_MAX_DEPTH` (16), `MAREKVS_PROTO_POOL_CACHE` (128).

## Collection elements

`PROTO.HSET`/`PROTO.SADD` validate and then **delegate verbatim** to the
ordinary hash/set handlers: element payloads are raw proto bytes, plain
`HGET`/`HGETALL`/`SMEMBERS` return them unchanged, and the OR-set/ORSWOT
merge machinery is untouched. The element's type is *not* stored per
element — `PROTO.HGETJSON`/`HGETFIELD` resolve it at read time (explicit
`TYPE` > binding), so **rebinding a prefix changes interpretation of stored
elements, not their bytes** (documented caveat).

## Cluster & scripting

Every typed-value handler calls `engine.ensure_local(&key)` before its
shard job (read-through for cluster-remote keys, commit 7b92499 rule).
`PROTO.*` is **excluded from Lua `script_safe`** in v1: handlers consult the
hidden registry (a different partition) and may `spawn_blocking`, both of
which would suspend inside the script driver's poll-once executor.

Write commands (`PROTO.BIND/UNBIND/SET/SETFIELD/CLEARFIELD/SETJSON/HSET/
SADD`) are classified in `Engine::is_write_command` (disk high-water guard)
and `parallel_safe` (pipeline batcher).

## Documented caveats

- Whole-message LWW: concurrent `PROTO.SET`/`SETFIELD` on both sides of a
  partition converge to the higher `(hlc, origin)` message; a SETFIELD does
  not merge field-wise with a concurrent SETFIELD of a *different* field.
- Same-name concurrent `PROTO.SCHEMA SET` is LWW on the latest pointer.
- Binding staleness ≤ 2 s (TTL) on nodes other than where BIND ran.
- Rebinding a prefix reinterprets stored hash/set elements on read.

---
title: Data model
description: How keys, envelopes, hybrid logical clocks, and convergent merges give marekvs its eventual consistency.
status: implemented
---

Everything marekvs stores lives in the ondaDB `data` column family as
`internal key → envelope + payload`. This page defines those byte layouts and
the merge rules that make replication converge — the machinery behind the
[published guarantees](../overview/#published-guarantees).

## Partitioning

```text
pid: u16 = (xxh3_64(userkey) >> 52) as u16     // top 12 bits → 0..4095
```

`P = 4096` fixed partitions, chosen at cluster creation and never changed. Every
internal key begins with the big-endian `pid`, so one partition is one
contiguous ondaDB key range — which makes bootstrap streaming, Merkle digesting,
and handoff cheap prefix operations. For collections, `pid` derives from the
**user key only** (not the field or member), so a whole collection lives in one
partition and on one shard thread.

Redis **hash tags** are implemented and match Redis exactly: if a key contains
`{...}`, only the bytes inside the first non-empty brace pair are hashed — so
`rate:{user1}:count` and `rate:{user1}:window` land on the same partition (and
therefore the same shard thread). This is what lets a multi-key `MULTI` block or
a Lua script co-locate its keys.

## Internal key layouts

All fields are concatenated with integers big-endian, so memcmp order is key
order. `klen` is a varint length of the user key, so an element key can never
collide with a user key that happens to embed another.

```text
string          [pid:u16] [b's'] [userkey]
collection head [pid:u16] [b'M'] [klen] [userkey]
hash field      [pid:u16] [b'h'] [klen] [userkey] [field]
set member      [pid:u16] [b'S'] [klen] [userkey] [member]
zset member     [pid:u16] [b'z'] [klen] [userkey] [member]
zset score idx  [pid:u16] [b'Z'] [klen] [userkey] [score_be:u64] [member]
list element    [pid:u16] [b'q'] [klen] [userkey] [pos:u64]
hll register    [pid:u16] [b'H'] [klen] [userkey] [bucket:u16]
stream entry    [pid:u16] [b'x'] [klen] [userkey] [id_ms:u64] [id_seq:u64]
```

Per-element keys (hash fields, set/zset members, list elements, HLL registers,
stream entries) pay off three ways: a mutation touches `O(element)` in the LSM,
not `O(collection)`; every replicated op is naturally a **delta**, so one `HSET`
ships one field; and `HGETALL` / `SMEMBERS` / `ZRANGE` become prefix scans that
LSM iterators handle well.

```note
The tag `b'l'` (a whole-list LWW blob) is **retired**. Lists are now per-element
records under `b'q'` — see [Lists](#lists) below. The old tag stays reserved, and
old blobs are neither read nor migrated.
```

The **zset score index** is a second key per member, maintained in the same
ondaDB transaction as the member key: the member key holds the score (the LWW
source of truth) and the index key is derived, carrying no payload. The score is
encoded as an order-preserving `u64`, so `ZRANGEBYSCORE` is a prefix scan over
`b'Z'` keys.

## Envelope

A fixed **19-byte header** is prefixed to every stored value:

```text
offset size field
0      1    flags:  bit0 tombstone
                    bit1 collection-head
                    bits 2..4 record type (0 string, 1 hash-field, 2 set-member,
                              3 zset-member, 4 list, 5 stream-entry,
                              6 counter, 7 hll-register)
1      8    hlc:    u64 big-endian = [phys_ms:48 | logical:16]
9      2    origin: NodeId (u16)
11     8    ttl_deadline_ms: u64 absolute wall-clock ms; 0 = no TTL
19     …    payload
```

The origin node writes the envelope **once**, and replication ships it
byte-for-byte with no re-stamping, so every replica agrees on the record's
identity. `(hlc, origin)` is the record **version** — and, for element adds, its
**dot**.

A collection **head key** carries its own small payload: the collection type
(hash / set / zset / stream / list / HLL), a whole-collection delete clock, and,
for streams, the last id and configuration.

## Hybrid logical clock

marekvs timestamps every write with a Kulkarni **hybrid logical clock** packed
into a `u64`: 48 bits of physical milliseconds since the Unix epoch, plus a
16-bit logical counter (65k events per millisecond per node before it borrows a
millisecond).

- **Local event:** `hlc = max(hlc_prev + 1, wall_ms << 16)`.
- **On receive:** `hlc = max(hlc_local, hlc_remote) + 1`.
- A remote HLC more than `max_clock_drift` (5 s) ahead of local wall clock is
  clamped and logged loudly — NTP is assumed on the nodes.
- The total order for last-writer-wins is `(hlc, origin)`; the `u16` origin
  breaks exact ties deterministically.

There is one HLC per process (an atomic `u64` behind a CAS loop), shared by every
shard.

## Merge rules

The apply path — whether the write is a local command or an incoming replication
op — always reads the current envelope and **merges**. A blind overwrite never
happens on the replication path. Every merge is commutative, associative, and
idempotent, and those laws are enforced by property tests in
`crates/marekvs-core/tests/merge_laws.rs`.

### LWW registers

Strings, hash-field values, zset scores, list elements, and collection heads are
last-writer-wins: higher `(hlc, origin)` wins, equal versions are the same write
(a no-op). A tombstone is just a version with the tombstone flag set, so deletes
and writes race symmetrically. `EXPIRE` / `PERSIST` are LWW writes of the
envelope with an unchanged payload.

### Observed-remove elements

Set members, hash fields, and zset members use **ORSWOT** semantics at
per-element granularity. Each element record carries **two capped dot lattices**:

```text
element payload:
  [nlive u8] nlive × [origin u16][hlc u64][vlen varint][value]   # live adds
  [ncov  u8] ncov  × [origin u16][hlc u64]                       # covered (removed) dots
```

A local add contributes `(dot, value)` to `live`; a remove contributes the dots
it *observed* to `covered`. Merge is the lattice join —
`covered' = cov_a ∪ cov_b`, `live' = (live_a ∪ live_b) \ covered'` — and the
element is dead when `live'` is empty. A lagging replica pushing an old add
carries the old dot, which `covered` still holds, so **deletes never resurrect**;
a concurrent add the remove never observed survives.

The caps keep the top-4 live dots and the top-255 covered dots (top-N-of-union is
itself commutative, associative, and idempotent, so the caps preserve the merge
laws).

```note
This two-lattice design superseded an earlier single-dot sketch that property
testing proved **non-associative**: a remove covering a dot the local record
never held lost its covering information on merge. The test suite caught it.
```

### Whole-collection delete

`DEL myhash` writes one LWW tombstone on the **head key** with a delete clock. An
element is live only if its version is newer than that clock — so a whole
collection dies in one op, with no per-element tombstone fan-out. Stale elements
are filtered on read and reclaimed by the expiry sweeper and anti-entropy.

### Counters (PN counters)

`INCR` / `DECR` / `INCRBY` / `DECRBY` on a string key produce a **PN-counter**
record, not a plain string:

- a **base register** `(base_hlc, base_origin, base_value)` set by the last
  explicit `SET` (or `(0,0,0)` for a fresh counter), merged LWW;
- **per-node delta slots** `(pos, neg)` — each node only grows its own slot, so
  slots merge by pointwise max. The value is `base + Σpos − Σneg`.

When two nodes share a base version, their slots join and **concurrent increments
on different nodes all survive**. A different base version (from an explicit
`SET`) wins wholesale — matching Redis's "SET resets the counter". Reads
materialize the counter as a decimal string, so `GET` / `STRLEN` / `TYPE` behave
normally; `APPEND` / `SETRANGE` / `GETSET` / `INCRBYFLOAT` freeze it back into a
plain string.

```note
`HINCRBY` is last-writer-wins on the result, and `INCRBYFLOAT` is LWW — counter
semantics inside hash fields and float slots are future work. Same-node
concurrency is always exact thanks to shard serialization.
```

### HyperLogLog

`PFADD` / `PFCOUNT` / `PFMERGE` store a HyperLogLog **decomposed into one record
per touched register** under a head-gated collection, merged by a one-byte
max-lattice: higher rank wins with its own envelope; equal ranks resolve to the
*lower* envelope version, so a duplicate `PFADD` is a strict no-op — no write, no
replication, no anti-entropy churn. Sparse cardinalities materialize only the
registers they touch; `DEL` writes one head tombstone whose delete clock gates
old registers, so resurrection safety is identical to sets.

```warning
The parameters `P = 14` (m = 16384) and the `xxh3_64` element hash are a **frozen
wire format**: every node must map an element to the identical `(bucket, rank)`
forever. Changing either in a rolling upgrade silently corrupts every estimate.
`TYPE` reports HLL keys as `string` for tooling compatibility, but `GET` on one
is a `WRONGTYPE` error (a documented divergence — Redis exposes the raw sketch
bytes; marekvs has no single blob).
```

### Streams

`XADD` generates the entry id at the origin: the millisecond comes from the HLC's
physical clock, and the sequence embeds the origin node id
(`seq = (origin << 20) | local_counter`), so ids are cluster-unique without
coordination. Merge is a union by id (entries are immutable — a duplicate id is
the same entry). `XDEL` / `XTRIM` write per-entry LWW tombstones.

```planned Consumer groups
`XGROUP` / `XREADGROUP` / `XACK` and the rest of the consumer-group surface are
**not implemented**. Streams currently expose only the raw entry operations:
`XADD`, `XLEN`, `XRANGE`, `XREVRANGE`, `XREAD`, `XDEL`, `XTRIM`.
```

### Lists

Lists are **per-element position-keyed LWW registers**: a list is a **collection
head** (like a set or zset) plus one **element record per position**.

```text
list element  [pid][b'q'][klen][userkey][pos:u64 BE]   payload = raw value bytes
```

- **Positions.** `pos` is an unsigned `u64` whose big-endian key suffix makes
  memcmp order equal list order. The first element lands at `LIST_CENTER = 1<<63`;
  `LPUSH` allocates `head − 1`, `RPUSH` allocates `tail + 1`, leaving ~2⁶³ of
  headroom each way. While only the ends are touched, push and pop are `O(1)`
  point writes and `LRANGE` is `O(range)`.
- **Elements are plain LWW registers**, so every push/pop replicates as one delta
  through the shared write path — no list-specific merge code, no whole-value
  shipping.
- **The head gates the collection**: TTL and the whole-collection delete clock
  ride the head exactly like a hash or set, and `DEL` is one head tombstone.
- **Interior ops rebuild.** `LSET` overwrites one position in place (`O(index)`).
  `LINSERT` / `LREM` / `LTRIM` cannot open room between adjacent integer
  positions, so they read the live values, tombstone the old positions, and
  rewrite the sequence compacted from `LIST_CENTER` (`O(n)`, and rare).

```warning
Two nodes pushing concurrently can allocate the **same** position; the element
records then merge LWW and **one push is lost — but only that one colliding
element**, not the whole push set. This is a bounded, per-collision loss, and
strictly better than the retired whole-list blob (which dropped an entire
concurrent push set). Single-node order is exact (shard serialization); ordering
across concurrent cross-node writers is best-effort. A true sequence CRDT (RGA)
remains future work.
```

## TTL representation

A Redis TTL is stored as the envelope's absolute `ttl_deadline_ms`, set once at
the origin and shipped verbatim. It is evaluated locally at read time: once
`now >= deadline`, the record is treated as a tombstone whose clock is the
deadline, so a stale pre-expiry value loses the merge against expiry and **TTL
converges cluster-wide without any expiry message**. The record also gets an
ondaDB per-key TTL of `deadline + gc_grace` as a compaction backstop.

Because hash fields, set members, and zset members are separate records with
their own envelopes, per-member TTL (`EXPIREMEMBER`, a KeyDB extension) falls out
naturally — each element carries its own deadline.

## What a `TYPE` check reads

Type resolves from the first matching key: a string key reports `string`; a head
key reports its collection type. Writing a string where a hash exists (or vice
versa) returns `WRONGTYPE` from this lookup — one extra point-read per command on
the same shard, usually served from the memtable or block cache.

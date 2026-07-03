# 02 — Data Model: Keys, Envelopes, Clocks, Merges

Everything marekvs stores lives in the ondaDB `data` column family as
`internal key → envelope + payload`. This document defines those byte layouts
and the merge rules that make replication convergent.

## Partitioning

```
pid: u16 = (xxh3_64(userkey) >> 52) as u16     // top 12 bits → 0..4095
```

`P = 4096` fixed partitions, chosen at cluster creation and never changed.
Every internal key begins with the big-endian `pid`, so one partition is one
contiguous ondaDB key range — this is what makes bootstrap streaming, Merkle
digesting, cold-partition purging, and handoff cheap prefix operations.

For collections, `pid` derives from the **user key only** (not field/member),
so a whole collection lives in one partition and one shard thread.

## Internal key layouts

All fields concatenated, integers big-endian (memcmp-ordered). `klen` is a
varint length of the user key (needed so element keys can't collide with a
user key that happens to embed another).

```
string          [pid:u16] [b's'] [userkey]
collection head [pid:u16] [b'M'] [klen] [userkey]
hash field      [pid:u16] [b'h'] [klen] [userkey] [field]
set member      [pid:u16] [b'S'] [klen] [userkey] [member]
zset member     [pid:u16] [b'z'] [klen] [userkey] [member]
zset score idx  [pid:u16] [b'Z'] [klen] [userkey] [score_be:u64] [member]
list element    [pid:u16] [b'q'] [klen] [userkey] [pos:u64]
list blob       [pid:u16] [b'l'] [userkey]              (RETIRED — see §Lists)
stream entry    [pid:u16] [b'x'] [klen] [userkey] [id_ms:u64] [id_seq:u64]
stream head     [pid:u16] [b'M'] [klen] [userkey]              (type in envelope)
```

Rationale for **per-element keys** (hash fields, set/zset members, stream
entries):

- a mutation touches O(element), not O(collection), in the LSM;
- every replicated op is naturally a **delta** — no diff computation, no
  full-value shipping for one `HSET`;
- Merkle anti-entropy digests at element granularity, so repair ships only
  divergent elements;
- `HGETALL`/`SMEMBERS`/`ZRANGE` become prefix scans, which LSM iterators do
  well (ondaDB has no range-bounded iterator: seek to prefix, walk, stop when
  the prefix no longer matches).

The **zset score index** is a second key per member maintained transactionally
with the member key (one ondaDB `Txn`): member key holds the score (source of
truth, LWW); index key is derived and carries no payload. Score encoded as
order-preserving u64 (IEEE-754 with sign-flip trick). ZADD = delete old index
key + write member + write new index key in one commit. ZRANGEBYSCORE = prefix
scan over `'Z'` keys.

## Envelope

A fixed **19-byte header** prefixed to every stored value:

```
offset size field
0      1    flags:  bit0 tombstone
                    bit1 collection-head
                    bits 2..4 type: 0 string, 1 hash-field, 2 set-member,
                                    3 zset-member, 4 list, 5 stream-entry
                    bits 5..7 reserved
1      8    hlc: u64 big-endian = [phys_ms:48 | logical:16]
9      2    origin: NodeId (u16)
11     8    ttl_deadline_ms: u64 absolute wall-clock ms; 0 = no TTL
19     …    payload
```

- The envelope is written by the origin node **once**; replication ships it
  byte-for-byte (no re-stamping) so all replicas agree on the record identity.
- `(hlc, origin)` is the record **version** and, for element adds, its **dot**.
- Payloads: string/field/list/stream = the raw value bytes; set/zset member
  adds = empty (zset member payload = 8-byte score); tombstones = observed-dot
  list (below).

Head-key payload (`flags.collection-head = 1`):

```
[ctype: u8]                 1 hash, 2 set, 3 zset, 4 stream, 5 list
[del_hlc: u64]              whole-collection tombstone clock (0 = never deleted)
[stream state, ctype=4]     last_id (u64,u64), max_len config, group state blob
```

## Hybrid logical clock

Kulkarni HLC packed in a u64: 48-bit physical milliseconds since Unix epoch
(good past year 10,000), 16-bit logical counter (65k events/ms/node before
borrowing a millisecond).

Rules:

- **send/local event**: `hlc = max(hlc_prev + 1, wall_ms << 16)`;
- **receive**: `hlc = max(hlc_local, hlc_remote) + 1` (logical bump);
- remote HLC more than `max_clock_drift = 5 s` ahead of local wall clock →
  clamp to `local_wall + drift` and log loudly (NTP is assumed on k8s nodes);
- total order for LWW: `(hlc, origin)` — origin u16 breaks exact ties.

One HLC instance per process (atomic u64, CAS loop), shared by all shards.

## Merge rules (the heart of convergence)

The apply path (local command or incoming replication op) always reads the
current envelope for the internal key and merges. **Blind overwrite never
happens on the replication path.** All merges are commutative, associative,
idempotent (verified by property tests, [10-testing.md](10-testing.md#101)).

### LWW registers — strings, hash-field values, zset scores, heads, lists

Higher `(hlc, origin)` wins; equal = same write = no-op. A tombstone is just a
version whose flag bit is set — deletes and writes race symmetrically.
TTL changes (EXPIRE/PERSIST) are LWW writes of the envelope with unchanged
payload.

### Observed-remove elements — set members, hash fields, zset members

The **dot** of an add is the add's own `(origin, hlc)`. Every element record
carries **two capped dot lattices** (this supersedes the earlier
single-dot-plus-tombstone sketch, which property testing proved
non-associative — a remove covering a dot the local record never held lost
its covering information on merge):

```
element payload:
  [nlive u8] nlive × [origin u16][hlc u64][vlen varint][value]   # live adds, dot-desc
  [ncov  u8] ncov  × [origin u16][hlc u64]                        # covered (removed) dots
```

- A local add contributes `(dot, value)` to `live`; a remove
  (SREM/HDEL/ZREM) contributes its *observed* dots to `covered`.
- **Merge is the lattice join**: `covered' = cov_a ∪ cov_b`,
  `live' = (live_a ∪ live_b) \ covered'`. The element is dead when `live'`
  is empty (envelope tombstone flag mirrors this). The visible value is the
  max-dot live entry.
- Caps: `live` keeps the top-4 dots, `covered` the top-255 (top-N-of-union is
  itself commutative/associative/idempotent, so the caps preserve the merge
  laws). A >255-way concurrent remove history per element could in theory
  resurrect a stale add — accepted and documented.

This is ORSWOT semantics at per-element granularity without version vectors
(cf. Riak DT). A lagging replica pushing an old add carries the old dot,
which `covered` still holds → **no resurrection**; a concurrent add whose dot
the remove never observed survives. The merge laws (commutativity,
associativity, idempotence, permutation independence, canonical fixed point)
are enforced by 512-case property tests in
`crates/marekvs-core/tests/merge_laws.rs`.

### Whole-collection delete

`DEL myhash` writes one LWW tombstone on the **head key** with `del_hlc`.
An element is live only if its version > head `del_hlc`. This kills the whole
collection with one op — no per-element tombstone fan-out. Elements older than
`del_hlc` are filtered on read and purged by the expiry sweeper / anti-entropy
(they compare dead against the head clock).

### HyperLogLog — per-register max lattice

`PFADD/PFCOUNT/PFMERGE` store a HyperLogLog **decomposed into one record per
touched register**, under a head-gated collection (ctype `CTYPE_HLL = 6`):

```
head       [pid][b'M'][klen][userkey]              ctype = 6
register   [pid][b'H'][klen][userkey][bucket u16]  payload = rank u8
```

`RecordType::HllRegister = 7` merges as a one-byte monotone lattice:
**higher rank wins with its envelope; equal ranks resolve to the LOWER
envelope version.** The min-version tie rule is what makes a duplicate
`PFADD` a strict no-op — no write, no replication, no anti-entropy digest
churn (a max-version rule would re-stamp the record on every duplicate).
Property-tested (commutative/associative/idempotent + duplicate-add-noop)
in `crates/marekvs-core/tests/merge_laws.rs`.

Why per-register instead of one 12 KiB sketch blob (the same rationale as
per-element collections):

- `PFADD` ships a ~30-byte register delta, never the sketch; a no-op add
  ships nothing;
- anti-entropy digests per register, so repair transfers only divergent
  registers;
- **deletion needs no epochs**: `DEL` writes one head tombstone and the head
  delete clock gates old registers — the resurrection-safety story is
  identical to sets (a blob design would need an epoch field to stop a
  lagging replica's old sketch from register-joining into a re-created one);
- small cardinalities are naturally sparse: registers materialize only when
  touched.

Costs (accepted): a fully dense HLL is ~16 k records (≈ 500 KiB raw before
LSM compression, vs 12 KiB packed), and `PFCOUNT` is a prefix scan of the
populated registers (µs sparse, ~ms dense).

**Frozen parameters — treat like a wire format**: `P = 14` (m = 16384) and
the element hash `xxh3_64`. Every node must map an element to the identical
`(bucket, rank)` forever; changing either in a rolling upgrade silently
corrupts every estimate. Estimator: classic Flajolet HLL with the
linear-counting small-range correction (σ ≈ 0.81 %; no large-range
correction needed with 64-bit hashes). `TYPE` reports HLL keys as `string`
for Redis tooling compatibility; `GET` on them is WRONGTYPE (documented
divergence — Redis exposes the raw sketch bytes, we have no single blob).

### Streams

`XADD` at origin generates the entry id: `ms = hlc.phys_ms`,
`seq = (origin << 20) | local_counter` — origin embedded in the sequence half
guarantees cluster-wide id uniqueness without coordination. Merge = union by
id (entries are immutable; duplicate id = same entry). `XDEL`/`XTRIM` write
per-entry LWW tombstones; consumer-group state (PEL, last-delivered) lives in
the head payload as LWW — cross-node consumer groups get last-writer semantics,
documented in [03-redis-api.md](03-redis-api.md#streams).

### Lists (per-element, v1.2)

> **Breaking storage change (pre-1.0, no migration):** lists were a single LWW
> blob (`'l'` key) through v1.1; they are now a head-gated collection of
> position-keyed element records (`'q'` keys). Old `'l'` blobs are not read or
> migrated — recreate lists after upgrading. The `'l'` tag stays reserved.

The blob made every mutation O(list) and redis-benchmark list workloads O(N²)
(design/09 finding #3). A list is now a **collection head** (ctype 5, like
set/zset) plus one **element record per position**:

```
list element  [pid][b'q'][klen][userkey][pos:u64 BE]   payload = raw value bytes
```

- **Position allocation.** `pos` is an unsigned u64; the big-endian key suffix
  makes memcmp order equal list order. The first element lands at
  `CENTER = 1<<63`; **LPUSH** allocates `head_pos - 1`, **RPUSH**
  `tail_pos + 1` — ~2^63 headroom each way. As long as only the ends are
  touched, live positions stay a contiguous range `[head, tail]`, so push/pop
  are O(1) point writes and LRANGE is O(range).
- **Elements are plain LWW registers** (`RecordType::List` reused per element).
  Payload is the raw value; merge is ordinary last-writer-wins (higher
  `(hlc, origin)`), so every mutation replicates as one delta through
  `write_merged` — no list-specific merge code, no whole-value shipping.
- **Head gates the collection.** TTL and the whole-collection delete clock ride
  the head exactly like a hash/set; an element is live iff it is not a
  tombstone, not expired, and `env.hlc > head.del_hlc` (`store::visible`). DEL
  is one head tombstone. TYPE/EXISTS/RENAME/EXPIRE go through the shared
  head-collection machinery (`ctype 5 → tag 'q'`).
- **Head/tail discovery.** Each shard caches `(head_pos, tail_pos)` per list in
  its in-memory `pop_hints` map (namespaced by the `'q'` collection prefix).
  On a miss — fresh process, post-rebuild, or when a cheap head-verify shows
  the cached head is no longer live (DEL/expiry/remote pop) — a prefix scan
  recovers the live min/max position. **The hint is a pure optimization:** all
  reads (LLEN/LRANGE/LINDEX/LPOS) walk the records, so a wrong hint costs at
  most a rescan, never a wrong answer. A list element arriving via
  replication/anti-entropy/bootstrap goes straight through `write_merged`,
  which does not maintain the hint, so the replication apply path invalidates
  the hint for that key (analogous to the zset score-index rebuild) to keep the
  node-local derived state in sync.
- **Interior ops rebuild.** LSET overwrites one position in place (O(index)).
  LINSERT/LREM/LTRIM cannot open room between adjacent integer positions, so
  they **rebuild**: read the live values, tombstone every old position, rewrite
  the new sequence compacted from CENTER (O(n), documented; these are rare).
  Position exhaustion at the u64 edge triggers the same recenter rebuild.
- **Concurrency caveat (weaker than a sequence CRDT, stronger than the blob):**
  two nodes pushing concurrently can allocate the **same** position; the
  element records then merge LWW and **one push is lost — but only that one
  colliding element**, not the whole push set the blob would have dropped.
  Ordering across concurrent cross-node writers is best-effort; single-node
  order is exact (shard serialization). A true sequence CRDT (RGA) remains
  future work. Blocking ops (BLPOP…) poll the same primitives on local state.

### Counters (v1.1: PN counters — stable increments)

INCR/DECR/INCRBY/DECRBY on string keys produce `RecordType::Counter` records
(`marekvs_core::counter`), a hybrid register/CRDT:

- **base register** `(base_hlc, base_origin, base_value)` — LWW, established
  by the last explicit SET (or `(0,0,0)` for a fresh counter, so first-touch
  increments from different nodes share a base and join);
- **per-node delta slots** `(pos, neg)` — each node only grows its own slot,
  so slots merge by **pointwise max** (PN-counter join). Value =
  base + Σpos − Σneg.

Merge: equal base versions → join slots (**concurrent increments on
different nodes all survive**); different base versions → higher base wins
wholesale (Redis "SET resets the counter": increments racing an explicit SET
drop by design). Counter vs plain string/tombstone on the same key is LWW by
envelope version, which makes SET/DEL/expiry reset naturally. Reads
materialize counters as decimal strings, so GET/STRLEN/TYPE behave normally;
APPEND/SETRANGE/GETSET/INCRBYFLOAT freeze the counter into a plain string.
EXPIRE/PERSIST re-encode the counter (TTL changes don't freeze it). RENAME
freezes the value at the destination.

Payload: `[base_hlc u64][base_origin u16][base i64][n u8] n × [node u16][pos
u64][neg u64]`, slots sorted by node id (canonical). Merge laws + a
no-lost-increments property are enforced in
`crates/marekvs-core/tests/merge_laws.rs`; the cluster test drives 60
concurrent INCRs across 3 nodes and asserts exact convergence.

**HINCRBY remains LWW-on-result** (hash fields are OR elements; counter
semantics inside element records is future work). Same-node concurrency is
exact everywhere (shard serialization); INCRBYFLOAT remains LWW (f64 slots
would trade exactness for convergence).

## TTL representation

- Redis TTL = envelope `ttl_deadline_ms`, absolute, set once at origin,
  shipped verbatim, evaluated locally at read time (`now >= deadline` → treat
  as tombstone with `hlc = HLC(deadline, 0)` — a stale pre-expiry value loses
  the merge against expiry, so expiry converges without any expiry message).
- Additionally the record gets ondaDB per-key TTL = `deadline + gc_grace` as a
  compaction backstop.
- Expired records leave Merkle digests only after `deadline + ttl_skew_grace`
  (5 s) so clock-skewed replicas don't ping-pong repairs
  ([05-consistency-anti-entropy.md](05-consistency-anti-entropy.md)).
- Field-level TTL (HEXPIRE, Redis 7.4) falls out for free: fields are separate
  records with their own envelopes.


## Value size & encoding notes

- Max value size 256 MiB (Redis compatible); ondaDB WiscKey moves
  values ≥ 512 B (`klog_value_threshold`) to the vlog automatically — large
  values do not bloat the LSM merge path.

## What a `TYPE` check reads

Type is resolved from the first matching key: string key → string; head key →
ctype. Writing a string where a hash exists (or vice versa) returns WRONGTYPE
based on this lookup — one extra point-read per command on the same shard,
usually served from the memtable/block cache.

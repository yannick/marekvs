---
title: Redis API reference
description: The Redis command surface marekvs actually implements — taken from the dispatch table, not a wish list.
status: mixed
---

marekvs speaks RESP2 and RESP3. The tables below list the commands that are
actually wired into the command dispatcher (`crates/marekvs-engine/src/cmd/`),
so if a command is here, it runs. Commands that some tools assume exist but that
marekvs does **not** implement are called out explicitly in
[Not implemented](#not-implemented).

```note
Read guarantees are **per connection**: a connection sees its own writes and
never reads backward in time. They are *not* cross-client — two clients on two
nodes can briefly observe different values. See [Consistency](../consistency/).
```

## Connection & server

| Command | Notes |
|---|---|
| `PING` `ECHO` | Liveness / echo. |
| `HELLO` | Protocol handshake; selects RESP2/RESP3. |
| `AUTH` | Single-password auth when `MAREKVS_REQUIREPASS` is set. |
| `QUIT` `RESET` | Close / reset the connection state. |
| `SELECT` | Accepted (single logical keyspace). |
| `CLIENT` `COMMAND` `CONFIG` `INFO` `DBSIZE` | Introspection / runtime config. |
| `FLUSHALL` `FLUSHDB` | Clear data. |
| `TIME` | Server time. |
| `REPLICAOF` / `SLAVEOF` | Follow a real Redis master (live migration). |
| `SHUTDOWN` `DEBUG` | Lifecycle / debug helpers. |
| `CLUSTER INFO/MYID/KEYSLOT/SLOTS/SHARDS/NODES` | Read-only topology for cluster-aware client routing. See [Cluster protocol](../cluster-protocol/). |

A few settings are live-reconfigurable via `CONFIG SET`: `requirepass`,
`lua-time-limit`, and `loglevel`.

## Generic / keyspace

| Command | Notes |
|---|---|
| `DEL` `UNLINK` `EXISTS` `TYPE` `TOUCH` | Standard key ops. |
| `TTL` `PTTL` `EXPIRETIME` `PEXPIRETIME` | Read expiry. |
| `EXPIRE` `PEXPIRE` `EXPIREAT` `PEXPIREAT` `PERSIST` | Set / clear expiry. |
| `KEYS` `SCAN` `RANDOMKEY` | Enumerate keys. |
| `RENAME` `RENAMENX` | Rename keys. |
| `COPY` | Copy a key within DB 0; supports `REPLACE` and accepts `DB 0`. |
| `OBJECT` | Compatibility introspection: `ENCODING`, `REFCOUNT`, `IDLETIME`, `FREQ`, `HELP`. |
| `EXPIREMEMBER` `EXPIREMEMBERAT` `PEXPIREMEMBERAT` | KeyDB-style per-member TTL. |
| `TTL key member` | Per-member TTL read (KeyDB extension). |

## Strings

| Command | Notes |
|---|---|
| `GET` `SET` `SETNX` `SETEX` `PSETEX` | Get / set with options. |
| `GETSET` `GETDEL` `GETEX` | Get-and-mutate. |
| `APPEND` `STRLEN` `SETRANGE` `GETRANGE` / `SUBSTR` | Substring ops. |
| `INCR` `DECR` `INCRBY` `DECRBY` `INCRBYFLOAT` | Counters (see below). |
| `MGET` `MSET` `MSETNX` | Multi-key get / set. |

```success
`INCR` / `DECR` / `INCRBY` / `DECRBY` are backed by **PN-counters**: concurrent
increments on different nodes are all preserved, never lost. An explicit `SET`
resets the counter. See [counters](../data-model/#counters).
```

## Hashes

| Command | Notes |
|---|---|
| `HSET` `HMSET` `HSETNX` `HGET` `HMGET` `HGETALL` | Field get / set. |
| `HGETDEL` | Return and delete one or more fields using `FIELDS n field ...`. |
| `HDEL` `HEXISTS` `HLEN` `HKEYS` `HVALS` `HSTRLEN` | Field inspection. |
| `HEXPIRE` `HPEXPIRE` `HEXPIREAT` `HPEXPIREAT` | Set field-level TTLs using `FIELDS n field ...`; supports `NX`/`XX`/`GT`/`LT`. |
| `HTTL` `HPTTL` `HEXPIRETIME` `HPEXPIRETIME` `HPERSIST` | Read or clear field-level TTL metadata. |
| `HGETEX` | Return fields and optionally set `EX`/`PX`/`EXAT`/`PXAT` or `PERSIST`. |
| `HSETEX` | Set field/value pairs using `FVS n field value ...`; supports `FNX`/`FXX`, expiry options, and `KEEPTTL`. |
| `HINCRBY` `HINCRBYFLOAT` | Field arithmetic. |
| `HRANDFIELD` `HSCAN` | Sample / iterate. |

## Sets

| Command | Notes |
|---|---|
| `SADD` `SREM` `SCARD` `SISMEMBER` `SMISMEMBER` | Membership. |
| `SMEMBERS` `SPOP` `SRANDMEMBER` `SSCAN` | Read / sample / iterate. |
| `SMOVE` | Move a member between sets. |
| `SUNION` `SINTER` `SDIFF` (+ `STORE`) `SINTERCARD` | Set algebra. |

Concurrent `SADD`s on different nodes both survive (ORSWOT merge).

## Sorted sets

| Command | Notes |
|---|---|
| `ZADD` `ZINCRBY` `ZREM` | Add / mutate. |
| `ZSCORE` `ZMSCORE` `ZCARD` `ZRANK` `ZREVRANK` `ZCOUNT` `ZLEXCOUNT` | Inspect. |
| `ZRANGE` `ZRANGEBYSCORE` `ZREVRANGE` `ZREVRANGEBYSCORE` `ZRANGEBYLEX` `ZREVRANGEBYLEX` | Range queries. |
| `ZRANDMEMBER` | Return one or more members, optionally `WITHSCORES`. |
| `ZRANGESTORE` | Store a `ZRANGE` result in a destination key. |
| `ZPOPMIN` `ZPOPMAX` `BZPOPMIN` `BZPOPMAX` `ZMPOP` `BZMPOP` | Pop commands, including blocking variants. |
| `ZREMRANGEBYSCORE` `ZREMRANGEBYRANK` `ZREMRANGEBYLEX` | Range deletion. |
| `ZUNION` `ZINTER` `ZDIFF` | Return sorted-set algebra results; supports `WEIGHTS`, `AGGREGATE`, and `WITHSCORES`. |
| `ZUNIONSTORE` `ZINTERSTORE` `ZDIFFSTORE` `ZINTERCARD` | Store or count sorted-set algebra results. |
| `ZSCAN` | Iterate members and scores. |

```note
Lexicographical sorted-set operations are implemented as scans over live members
(`O(N)`). `ZRANDMEMBER` follows marekvs' deterministic sampling style rather
than making distribution guarantees.
```

## Lists

| Command | Notes |
|---|---|
| `LPUSH` `RPUSH` `LPUSHX` `RPUSHX` | Push. |
| `LPOP` `RPOP` `LLEN` `LRANGE` `LINDEX` | Pop / read. |
| `LSET` `LREM` `LTRIM` `LINSERT` `LPOS` | Mutate / search. |
| `LMOVE` `RPOPLPUSH` | Move between lists. |
| `LMPOP` | Pop multiple elements from the first non-empty list. |
| `BLPOP` `BRPOP` `BLMOVE` `BRPOPLPUSH` `BLMPOP` | Blocking variants. |

```note
Lists are per-element position-keyed LWW registers. Blocking commands poll at
~50 ms and also wake on a replicated push. Concurrent cross-node pushes can
collide on a position — see the [list caveat](../data-model/#lists).
```

## Streams

| Command | Notes |
|---|---|
| `XADD` `XLEN` `XDEL` `XTRIM` | Append / size / trim. |
| `XRANGE` `XREVRANGE` `XREAD` | Read entries. |
| `XSETID` | Set last-generated-id metadata, with optional `ENTRIESADDED` and `MAXDELETEDID`. |
| `XINFO STREAM` | Return stream metadata, length, first/last entry, and placeholder radix/group fields. |

```warning
**Consumer groups are not implemented** — `XGROUP`, `XREADGROUP`, `XACK`,
`XCLAIM`, `XAUTOCLAIM`, `XPENDING`, and `XINFO GROUPS`/`XINFO CONSUMERS` are
absent. Streams provide raw, at-least-once entry operations plus basic stream
metadata.
```

## HyperLogLog

| Command | Notes |
|---|---|
| `PFADD` `PFCOUNT` `PFMERGE` | Convergent (per-register max) cardinality. |

## Pub/Sub

| Command | Notes |
|---|---|
| `SUBSCRIBE` `UNSUBSCRIBE` `PSUBSCRIBE` `PUNSUBSCRIBE` | Channel / pattern. |
| `PUBLISH` `PUBSUB` | Publish / introspect. |

## Scripting

| Command | Notes |
|---|---|
| `EVAL` `EVALSHA` `SCRIPT` | Lua 5.4; keys must co-locate. See [Lua scripting](../lua-scripting/). |

## Transactions

| Command | Notes |
|---|---|
| `MULTI` `EXEC` `DISCARD` | Queue and run sequentially. |

```warning
Transactions are a **convenience batch, not ACID**: queued commands run
sequentially with no atomicity beyond per-key shard serialization. `WATCH` /
`UNWATCH` are **rejected** with an error — marekvs is AP and has no transactional
compare-and-swap.
```

## Distributed budgets (marekvs extension)

Not a Redis family — `BG.*` is marekvs-native: escrow-based shared budgets
with a hard **never-overspend** invariant that holds through partitions,
crashes, and split-brain (fail-closed). Full guide: [Distributed
budgets](../budget/).

| Command | Notes |
|---|---|
| `BG.CREATE` `BG.TOPUP` | Create / fund a budget (central actor; `SEQ`-idempotent; `MODE WINDOW` = self-refilling rate limit). |
| `BG.RESERVE` | Reserve an amount → token + deadline; forwards to a peer with escrow headroom; fails closed (`-BUDGETEXHAUSTED`). |
| `BG.COMMIT` `BG.RELEASE` `BG.DRAW` | Report spend / return / draw incrementally against a token (routed to its issuing node). |
| `BG.INFO` | Node-local ledger view. |
| `BG.RECLAIM` | Admin: fence a permanently dead node and redistribute its unconsumed escrow. |

`TYPE` reports `budget`; `EXPIRE`, `RENAME`, and `COPY` are rejected on budget
keys; `DEL` starts a fresh generation (outstanding tokens die with the old one).

## JSON documents (RedisJSON-compatible)

Full RedisJSON v2 surface over a **per-path CRDT** document model —
concurrent editors on different nodes merge structurally instead of
last-writer-wins on whole documents. Full guide: [JSON documents](../json/).

| Command | Notes |
|---|---|
| `JSON.SET` `JSON.GET` `JSON.MGET` `JSON.MSET` | Both path dialects (`$…` JSONPath, legacy `.a.b[3]`); `MSET` not atomic across keys. |
| `JSON.DEL` `JSON.FORGET` `JSON.TYPE` `JSON.CLEAR` | Subtree delete wins over concurrent interior edits; add-wins per record. |
| `JSON.NUMINCRBY` `JSON.NUMMULTBY` | LWW under concurrency (like `HINCRBY`). |
| `JSON.STRAPPEND` `JSON.STRLEN` `JSON.TOGGLE` | Scalar ops. |
| `JSON.ARRAPPEND` `JSON.ARRINDEX` `JSON.ARRINSERT` `JSON.ARRLEN` `JSON.ARRPOP` `JSON.ARRTRIM` | Arrays are RGA sequences: concurrent appends all survive, runs never interleave. |
| `JSON.OBJKEYS` `JSON.OBJLEN` | Lexicographic key order. |
| `JSON.MERGE` | RFC 7386 merge-patch, decomposed into per-field deltas. |
| `JSON.RESP` `JSON.DEBUG` | RESP view; MEMORY = stored-record bytes. |

`TYPE` reports `ReJSON-RL` (module-compat); `OBJECT ENCODING` reports `json`;
TTL and RENAME/COPY behave like other collection types.

## Not implemented

These are **not** in the dispatch table, even though some clients or older design
notes assume them. Calling one returns an error:

- **Keyspace:** `SORT`, `DUMP`, `RESTORE`, `MOVE`
- **Stream:** consumer-group commands (`XGROUP`, `XREADGROUP`, `XACK`,
  `XCLAIM`, `XAUTOCLAIM`, `XPENDING`, `XINFO GROUPS`, `XINFO CONSUMERS`)
- **Cluster / HA:** `WAIT`, `FAILOVER`, `FUNCTION`. `CLUSTER SETSLOT`/`FORGET`/
  `MEET` are also absent — topology is gossip+HRW managed, not client-mutated.
  (Read-only `CLUSTER` topology commands *are* implemented — see
  [Cluster protocol](../cluster-protocol/).)
- **Modules / extras:** `GEO*`, bitfield/bit ops, `CLIENT TRACKING`
  (client-side caching)
- **Transport:** no TLS.

## Cross-cutting semantics

- **Protocols:** RESP2 and RESP3, negotiated with `HELLO`; inline commands are
  supported.
- **Atomicity:** per-key only. A key's operations are serialized on one shard
  thread; there is no multi-key or cross-node atomicity.
- **Type errors:** operating on the wrong structure returns `WRONGTYPE`.
- **Expiry:** absolute deadlines decided at the origin, converging cluster-wide.
- **Counters:** exact under concurrency (PN-counters).
- **Blocking commands:** implemented via ~50 ms polling that also wakes on a
  replicated push.
- **Cluster routing:** no `MOVED`/`ASK` redirects, no `CROSSSLOT` errors — any
  node serves any key, cluster-aware clients just get to skip the extra hop.
  See [Cluster protocol](../cluster-protocol/).

## Where to go next

- The structures behind these commands: [Data model](../data-model/).
- What "eventually consistent" means for reads: [Consistency](../consistency/).
- Scripting details and caveats: [Lua scripting](../lua-scripting/).
- Client-side slot routing: [Cluster protocol](../cluster-protocol/).

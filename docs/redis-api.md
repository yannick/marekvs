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
| `HDEL` `HEXISTS` `HLEN` `HKEYS` `HVALS` `HSTRLEN` | Field inspection. |
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
| `ZSCORE` `ZMSCORE` `ZCARD` `ZRANK` `ZREVRANK` `ZCOUNT` | Inspect. |
| `ZRANGE` `ZRANGEBYSCORE` `ZREVRANGE` `ZREVRANGEBYSCORE` | Range queries. |
| `ZPOPMIN` `ZPOPMAX` `ZREMRANGEBYSCORE` `ZSCAN` | Pop / trim / iterate. |

## Lists

| Command | Notes |
|---|---|
| `LPUSH` `RPUSH` `LPUSHX` `RPUSHX` | Push. |
| `LPOP` `RPOP` `LLEN` `LRANGE` `LINDEX` | Pop / read. |
| `LSET` `LREM` `LTRIM` `LINSERT` `LPOS` | Mutate / search. |
| `LMOVE` `RPOPLPUSH` | Move between lists. |
| `BLPOP` `BRPOP` `BLMOVE` `BRPOPLPUSH` | Blocking variants. |

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

```warning
**Consumer groups are not implemented** — `XGROUP`, `XREADGROUP`, `XACK`,
`XCLAIM`, `XAUTOCLAIM`, `XPENDING`, `XSETID`, and `XINFO` are absent. Streams
provide raw, at-least-once entry operations only.
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

## Not implemented

These are **not** in the dispatch table, even though some clients or older design
notes assume them. Calling one returns an error:

- **Keyspace:** `COPY`, `OBJECT`, `SORT`, `DUMP`, `RESTORE`, `MOVE`
- **Sorted set:** `ZMPOP`, `ZRANDMEMBER`, `ZRANGESTORE`, `ZREMRANGEBYRANK`,
  `ZREMRANGEBYLEX`, `ZLEXCOUNT`
- **List:** `LMPOP`
- **Stream:** `XSETID`, `XINFO`, consumer-group commands
- **Cluster / HA:** `WAIT`, `FAILOVER`, `CLUSTER`, `FUNCTION` (no Redis Cluster
  protocol, no `MOVED`/`ASK`)
- **Modules / extras:** `GEO*`, `JSON*`, bitfield/bit ops, `CLIENT TRACKING`
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

## Where to go next

- The structures behind these commands: [Data model](../data-model/).
- What "eventually consistent" means for reads: [Consistency](../consistency/).
- Scripting details and caveats: [Lua scripting](../lua-scripting/).

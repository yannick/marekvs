# 03 — Redis API Surface


- **v1** — implemented in the first release
- **v1.1** — fast follow
- **✗** — not planned (out of scope for an AP disk store, or module territory)

## Protocol

- **RESP2 and RESP3**, negotiated via `HELLO`. Parser: incremental state
  machine (arrays + inline commands).
- RESP3 native frames: maps (`%`), sets (`~`), doubles (`,`), null (`_`),
  push (`>`), verbatim strings (`=`), big numbers. RESP2 downgrades: map →
  2n array, set → array, double → bulk string, null → `$-1`/`*-1`.
- Inline commands, `AUTH` (single password), `RESET`, pipelining supported.
- No TLS (constraint), no client-side caching (`CLIENT TRACKING` → error).

## Command coverage matrix

### Strings (`string_family.cc` inventory)

| Commands | Tier |
|---|---|
| GET, SET (EX/PX/EXAT/PXAT/NX/XX/KEEPTTL/GET), SETNX, SETEX, PSETEX, GETSET, GETDEL, GETEX | v1 |
| MGET, MSET, MSETNX*, APPEND, STRLEN, SETRANGE, GETRANGE/SUBSTR | v1 |
| INCR, DECR, INCRBY, DECRBY | v1.1: **PN counters** — concurrent cross-node increments all survive; SET/DEL resets ([02](02-data-model.md#counters-v11-pn-counters--stable-increments)) |
| INCRBYFLOAT | v1 (LWW caveat below) |
| CL.THROTTLE | ✗ |

*MSETNX is atomic per shard only; cross-shard MSETNX is best-effort (documented).

### Generic / keyspace (`generic_family.cc`)

| Commands | Tier |
|---|---|
| DEL, UNLINK, EXISTS, TYPE, TTL, PTTL, EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, EXPIRETIME, PEXPIRETIME, PERSIST | v1 |
| SCAN (MATCH/COUNT/TYPE), KEYS, RANDOMKEY, TOUCH | v1 |
| RENAME, RENAMENX, COPY | v1 (copy+tombstone, not atomic across shards) |
| DUMP, RESTORE, MOVE, SORT | v1.1 (DUMP format is marekvs-native, not RDB) |
| OBJECT ENCODING/REFCOUNT/IDLETIME/FREQ | v1 (static/stub answers) |
| STICK, conditional DELEX/IFEQ variants | ✗ |

SCAN cursors encode `(pid, ondadb-key-offset)`; guaranteed-terminating,
at-least-once semantics like Redis. KEYS does a full prefix walk — documented
as expensive.

### Hashes (`hset_family.cc`)

| Commands | Tier |
|---|---|
| HSET, HSETNX, HMSET, HGET, HMGET, HGETALL, HDEL, HEXISTS, HLEN, HKEYS, HVALS, HSTRLEN, HRANDFIELD, HSCAN | v1 |
| HINCRBY, HINCRBYFLOAT | v1 (counter caveat) |
| HEXPIRE, HPEXPIRE, HTTL, HPTTL, HPERSIST, HEXPIRETIME, HPEXPIRETIME, HGETEX, HSETEX | v1.1 (field TTL is native in our model) |
| **EXPIREMEMBER / EXPIREMEMBERAT / PEXPIREMEMBERAT** (KeyDB extension) + `TTL key member` | ✓ — per-member TTL on hash fields, set members, zset members. TTL rides the element envelope (deadline absolute, evaluated locally → converges cluster-wide); expiry becomes an observed-remove via the sweeper |

### Sets (`set_family.cc`)

| Commands | Tier |
|---|---|
| SADD, SREM, SCARD*, SISMEMBER, SMISMEMBER, SMEMBERS, SRANDMEMBER, SPOP, SSCAN, SMOVE | v1 |
| SUNION, SINTER, SDIFF (+STORE, SINTERCARD) | v1 |
| SADDEX | v1.1 |

*SCARD is a prefix-scan count in v1 (cached count in head key is a v1.1
optimization — counts are approximate under concurrent merges anyway).

### Sorted sets (`zset_family.cc`)

| Commands | Tier |
|---|---|
| ZADD (NX/XX/GT/LT/CH/INCR), ZSCORE, ZMSCORE, ZCARD, ZINCRBY, ZREM | v1 |
| ZRANGE (BYSCORE/BYLEX/REV/LIMIT), ZRANGEBYSCORE, ZREVRANGE*, ZRANGESTORE, ZRANK, ZREVRANK, ZCOUNT, ZLEXCOUNT | v1 |
| ZPOPMIN/MAX, ZRANDMEMBER, ZSCAN, ZREMRANGEBY(RANK/SCORE/LEX), ZMPOP | v1 |
| ZUNION/ZINTER/ZDIFF (+STORE/CARD) | v1.1 |
| BZPOPMIN/MAX, BZMPOP | v1.1 (blocking infra shared with lists) |

ZRANK/ZREMRANGEBYRANK require an ordinal walk of the score index (O(rank));
documented — no order-statistic tree over an LSM in v1.

### Lists (`list_family.cc`)

| Commands | Tier |
|---|---|
| LPUSH, RPUSH, LPUSHX, RPUSHX, LPOP, RPOP, LLEN, LRANGE, LINDEX, LSET, LREM, LTRIM, LINSERT, LPOS | v1 |
| LMOVE, RPOPLPUSH, LMPOP | v1 |
| BLPOP, BRPOP, BLMOVE, BRPOPLPUSH, BLMPOP | v1.1 ✓ (50 ms polling; wakes on replicated pushes too) |

**Consistency caveat (prominent in user docs):** lists are position-keyed
per-element LWW registers (v1.2), not a sequence CRDT. Single-node order is
exact (shard serialization). Across nodes, only concurrent pushes that land on
the **same allocated position** collide and lose one element — a bounded,
per-collision loss, not the whole-push-set loss of the retired v1 blob. Push
counts returned by LPUSH/RPUSH assume local contiguity, so they can be
approximate while concurrent cross-node pushes are in flight; LLEN always
reports the true live count ([02-data-model.md](02-data-model.md#lists-per-element-v12)).

### Streams (`stream_family.cc`)

| Commands | Tier |
|---|---|
| XADD, XLEN, XRANGE, XREVRANGE, XREAD, XDEL, XTRIM, XSETID, XINFO STREAM | v1 |
| XGROUP, XREADGROUP, XACK, XPENDING, XCLAIM, XAUTOCLAIM, XINFO GROUPS/CONSUMERS | v1.1 |

XADD auto-ids embed the origin node in the sequence half — ids are unique
cluster-wide and streams merge as a union
([02-data-model.md](02-data-model.md#streams)). **Caveat:** consumer-group
state is LWW; concurrent XREADGROUP on different nodes can double-deliver.
Streams are at-least-once cross-node in v1.

### HyperLogLog

| Commands | Tier |
|---|---|
| PFADD, PFCOUNT (multi-key), PFMERGE | ✓ — per-register records, cluster-convergent by construction ([02](02-data-model.md#hyperloglog--per-register-max-lattice)) |
| PFDEBUG, PFSELFTEST | ✗ (Redis internals) |

### Pub/Sub

| Commands | Tier |
|---|---|
| SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PUBLISH, PUBSUB CHANNELS/NUMSUB/NUMPAT | v1 |
| SPUBLISH/SSUBSCRIBE (sharded) | ✗ |

Cluster-wide delivery via the filtered mesh
([04-replication.md](04-replication.md#pubsub)); fire-and-forget, matching
Redis semantics (Redis pub/sub is already lossy on disconnect).

### Keyspace notifications

`notify-keyspace-events` with the full flag matrix (K/E/g/$/l/s/h/z/x/t/d/m/A)
— we generate events in the
command layer at the **origin node only** (envelope origin == self), routed
through the pub/sub mesh. One event per logical write cluster-wide.

### Server / management

| Commands | Tier |
|---|---|
| PING, ECHO, HELLO, AUTH, SELECT*, TIME, DBSIZE, FLUSHDB, FLUSHALL | v1 |
| INFO (server, clients, memory, persistence, stats, replication†, keyspace) | v1 |
| CLIENT (ID/GETNAME/SETNAME/LIST/KILL/INFO), COMMAND (DOCS/COUNT/INFO), CONFIG GET/SET‡, SHUTDOWN, DEBUG SLEEP/OBJECT | v1 |
| SLOWLOG, LATENCY, MEMORY USAGE/STATS | v1.1 |
| MULTI/EXEC/DISCARD | v1.1 ✓ — per-connection queue, sequential execution; **no atomicity across keys** (queued commands are ordinary commands). WATCH → error (no CAS in AP) |
| EVAL/EVALSHA, SCRIPT (LOAD/EXISTS/FLUSH) | v1.1 ✓ — sandboxed Lua, effects replication ([design/11](11-lua-scripting.md)) |
| REPLICAOF/SLAVEOF | v1.1 ✓ — live-migration ingest from an upstream Redis master; node stays writable (AP) |
| CLUSTER INFO/MYID/KEYSLOT/SLOTS/SHARDS/NODES | ✓ — read-only topology for cluster-aware clients ([15](15-cluster-protocol.md)); no MOVED/CROSSSLOT — any node serves any key |
| WAIT, FAILOVER, CLUSTER SETSLOT/FORGET/MEET (topology is gossip-managed), FUNCTION, ACL beyond AUTH | ✗ |

*SELECT 0 only (single logical database; SELECT n>0 → error, like many
Redis-compatible stores). †`INFO replication` reports marekvs cluster health:
`cluster_degraded`, `underreplicated_partitions`, effective RF, staleness gauge.
‡`CONFIG SET` applies three keys live — `requirepass`, `lua-time-limit`
(alias `busy-reply-threshold`), and `loglevel` (Redis level or a raw tracing
filter spec, e.g. `info,chitchat=debug`) — and accepts-but-ignores every
other key (config is env-driven; see the
[defaults table](05-consistency-anti-entropy.md#defaults-table)). Runtime
changes are ephemeral: the env re-applies on restart, and `CONFIG REWRITE`
is a no-op. `CONFIG GET` answers the three live keys plus common client
probes (`maxmemory`, `appendonly`, `save`, `databases`, …).

### Not planned (module territory / conflicts with AP-on-disk)

GEO*, BF./CF./CMS./TOPK.*, BITCOUNT/BITPOS/BITOP/
BITFIELD/SETBIT/GETBIT (bitops need byte-addressable in-place mutation —
possible later as string overlay, not v1), OBJECT tiering commands.

## Cross-cutting semantics & caveats

| Area | Behavior |
|---|---|
| Atomicity | per key (shard-serialized). Multi-key commands are not atomic across shards; Redis-on-one-box atomicity is **not** preserved for cross-shard MSET/SINTERSTORE — documented. |
| Counters | **exact across nodes** (v1.1 PN counters) for INCR/DECR/INCRBY/DECRBY; increments racing an explicit SET are dropped (SET-resets semantics). INCRBYFLOAT and HINCRBY remain LWW. |
| Read guarantees | read-your-writes + monotonic reads per connection; nothing across connections ([00-overview.md](00-overview.md#published-guarantees-what-we-tell-users)). |
| WRONGTYPE | checked against head/string key ([02-data-model.md](02-data-model.md#what-a-type-check-reads)). |
| Expiry | active sweeper + lazy check; `expired` notifications fire on the sweeping node only. |
| Limits | 256 MiB max value; 512 MB max bulk in protocol; TTL ≤ 8 years; GETRANGE/SETRANGE indices are signed 32-bit. |
| Blocking commands | v1.1; implemented via local wakeup + interest subscription on the key (a remote push wakes local blockers). Cross-node BLPOP is racy-by-design (two nodes may both pop under partition; documented — AP). |

## marekvs extensions

| Family | Commands | Notes |
|---|---|---|
| Budgets (`BG.*`) | `BG.CREATE`, `BG.TOPUP`, `BG.RESERVE`, `BG.COMMIT`, `BG.RELEASE`, `BG.DRAW`, `BG.INFO`, `BG.RECLAIM` | escrow-based distributed budgets with a hard never-overspend invariant; fail-closed under partition. Not a Redis command family — see [13-budget.md](13-budget.md) for the protocol, reply shapes, and AP caveats. `TYPE` reports `budget`; EXPIRE/RENAME/COPY are rejected on budget keys. |
| Member TTL | `EXPIREMEMBER`, `EXPIREMEMBERAT`, `PEXPIREMEMBERAT`, 3-arg `TTL`/`PTTL` | KeyDB-compatible per-member expiry (pre-existing). |

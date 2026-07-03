# 11 — Lua Scripting (EVAL/EVALSHA): Design & Distributed Caveats

Status: **implemented** (Phases 0 + 1; Phase 2 open). The hard part is not
embedding Lua — it is that Redis scripts *assume a single-threaded,
single-master world*, and every one of those assumptions must be either
honored locally, mapped onto our model, or loudly rejected.

> **Implementation note — deviation from the bridge plan below.** Instead of
> refactoring every command into a sync `core_<cmd>` function, the shipped
> bridge drives the *existing async handlers* to completion on the shard
> thread with a poll-once executor: `Store::run` detects it is already on
> the target shard thread (a `CURRENT_SHARD_CTX` thread-local) and runs the
> job inline, so every `redis.call` future resolves on its first poll. A
> future that returns `Pending` (remote fetch, blocking op, key on another
> shard) is a clean error, never a hang. This got the full command surface
> (minus an explicit denylist: MSET/MGET, blocking, pub/sub, EVAL
> recursion) for free, with no per-command refactor. See
> `crates/marekvs-engine/src/cmd/script.rs`.

## What Redis scripts assume vs. what marekvs is

| Redis script assumption | marekvs reality |
|---|---|
| Whole script is atomic vs. the entire keyspace | Atomicity exists per key (shard-thread serialization); cross-key = nothing |
| One master; replicas replay effects | Multi-master; every node accepts writes; convergence via merges |
| Script sees THE state | Script sees THIS NODE's state (bounded-stale) |
| `SCRIPT LOAD` visible cluster-wide (single node) | Each node has its own cache |
| Long script blocks everything (and that's "fine") | A long script would block 1/S of the keyspace on one node |

## The five caveats, and their solutions

### Caveat 1 — Cross-key atomicity is physically impossible cluster-wide

In an AP system, "atomic script over keys" can only ever mean **node-local**
atomicity; another node can always write the same keys mid-script, and the
results merge afterward like any concurrent writes. No implementation choice
changes this — it is the CAP position marekvs took on day one.

What we CAN honestly provide is exactly what Redis Cluster provides:
**full atomicity for scripts whose keys co-locate**, plus explicit
non-atomic execution otherwise.

**Solution — the shard-thread trick.** Our per-key serialization comes from
routing all operations for a partition through one shard thread. A script
whose declared KEYS all map to the **same pid** can execute *entirely inside
one shard job*: every `redis.call` runs synchronously against `ShardCtx`,
and no other operation on those keys can interleave — that is Redis-grade
atomicity, delivered by machinery we already have, with zero locks.

The classic high-value scripts (rate limiter, unlock-if-token-matches,
compare-and-swap, sliding window) are single-key → they get full atomicity
automatically.

**Prerequisite — hash tags.** Same-pid multi-key scripts need users to be
able to co-locate keys. Implement Redis Cluster hash tags: `pid_of` hashes
only the `{...}` substring when present (`rate:{user1}:count` and
`rate:{user1}:window` share a pid). This is a **storage-breaking change**
for existing keys containing braces (pre-1.0: no migration, documented) and
independently improves MULTI and multi-key commands. Ship it first.

Scripts whose keys span pids: rejected by default with a clear error
(`CROSSSLOT`-equivalent), executable non-atomically via an explicit opt-in
(Phase 2, `#!lua flags=allow-multi-shard` style shebang, mirroring Redis 7
script flags) — each `redis.call` then dispatches like a normal command.

### Caveat 2 — Never replicate the script; replicate its effects

Re-executing a script per replica diverges the cluster: `TIME`,
`math.random`, `SRANDMEMBER`, and — worse — *reads of node-local state*
make script execution non-deterministic across nodes by construction.

**Solution — we already have effects replication, for free.** Our
replication ships committed storage records from the ondadb commit hook;
a script's writes are just writes. The script executes exactly once, on the
receiving node; its effects propagate as ordinary records and merge under
the ordinary rules (LWW / OR-set / PN counter / register-max). This also
means we can *allow* `math.random` and `TIME` without Redis's historical
determinism restrictions — `redis.replicate_commands()` becomes a no-op
returning true.

Corollary worth advertising: **a PN-counter-based rate limiter in Lua is
cross-node exact** (concurrent INCRs merge), while a GET/SET token bucket
races as LWW — identical to the non-script command semantics. Scripts add
no *new* distributed anomalies; they inherit the per-type merge semantics.

### Caveat 3 — Reads inside scripts see node-local, bounded-stale state

A script reading a non-home key needs the read-through fetch — but the
atomic path runs on a *synchronous* shard thread that cannot await the
async fetch.

**Solution — pre-fetch declared keys, forbid undeclared access.**
Before entering the shard job, `ensure_local` runs for every declared KEY
(async, off the shard thread). Inside the script, `redis.call` on a key
that was not declared in KEYS is an **error**. Redis historically tolerated
undeclared access and Redis Cluster broke that habit for the same
structural reason; we enforce it strictly since every access goes through
our bridge anyway. Freshness guarantee inside a script = the read path
guarantee (lease-fresh interest copy or home read), then frozen for the
duration of the atomic execution.

### Caveat 4 — A long script on a shard thread is head-of-line blocking

The atomic path borrows a shard thread; an infinite loop would freeze 1/S
of this node's keyspace.

**Solutions, layered:**
- **Instruction budget**: a Lua debug hook every N instructions checks a
  deadline (default 20 ms atomic path, `MAREKVS_SCRIPT_TIME_LIMIT` to
  raise) and aborts with an error. Writes already made STICK — same as
  Redis semantics after the write barrier.
- **Memory budget** via the Lua allocator limit (default 16 MiB).
- **`SCRIPT KILL`** sets a flag the debug hook observes (works only until
  the first write, as in Redis).
- Blocking commands (`BLPOP`, …) and connection/state commands
  (`SUBSCRIBE`, `MULTI`, `EVAL` recursion) are rejected inside scripts.
- The non-atomic path (Phase 2) runs on the tokio blocking pool with a
  larger budget — it holds no shard thread between calls.

### Caveat 5 — Script cache visibility across the cluster

`SCRIPT LOAD` on node A, `EVALSHA` on node B → `NOSCRIPT`. Redis Cluster
has the identical problem and every client library already handles it
(retry with `EVAL`).

**Solution — two layers:**
1. Keep the standard contract: node-local cache, `NOSCRIPT` on miss.
   Clients recover automatically.
2. **Scripts as replicated records**: `SCRIPT LOAD` also writes the source
   under an internal system key (`\x00sys:script:<sha1>` — a reserved
   prefix hidden from SCAN/KEYS/DBSIZE/TYPE). It then replicates and
   anti-entropies like any record; an `EVALSHA` cache miss falls back to
   the system keyspace before erroring. Eventual, not immediate — the
   `NOSCRIPT` path remains the correctness story; the system record makes
   it rare.

## Non-solutions, called out explicitly

- **Global script lock / serializing scripts cluster-wide** — would require
  consensus, which we rejected by design; and it would still be wrong the
  moment a partition happens.
- **Redlock and other multi-node lock patterns implemented in Lua** — they
  are unsound on ANY asynchronously-replicated Redis (Kleppmann's
  analysis) and doubly so here. The docs must say: marekvs scripts give
  per-node/per-shard atomicity; if you need a distributed lock with fencing,
  you need a CP system.
- **Replicating the script for re-execution** — divergence machine, see
  caveat 2.

## Runtime choice

**mlua with vendored Lua 5.4** (statically linked; the musl builder image
already carries a C toolchain). LuaJIT would match Redis's 5.1 semantics
and speed more closely but is a known pain on aarch64-musl — keep it as an
opt-in cargo feature later. 5.1→5.4 compat shims to provide: a `bit`
library facade (Redis scripts call `bit.band` etc.), `cjson`,
`redis.sha1hex`, `redis.error_reply`/`status_reply`, `redis.call/pcall`,
`KEYS`/`ARGV`. Sandbox: no `os`, `io`, `package`, `load`, `dofile`;
`Lua::new_with` restricted stdlib + memory limit.

Value conversion must follow Redis's documented Lua↔RESP conversion table
exactly (nil↔Null, number→integer truncation, table array, `ok`/`err`
fields, RESP3 extensions behind `redis.setresp`); conformance-test by
diffing script outputs against a real redis-server.

## The command bridge (main refactor)

Atomic path needs `redis.call` to execute **synchronously on ShardCtx**.
Today each command's logic lives inside an async handler that closes over a
`run_key` job — the closure body *is* the sync core. Refactor pattern (no
behavior change): extract `fn core_<cmd>(ctx: &ShardCtx, args) -> Reply`
per data command; async handlers call them through `run_key`; the script
bridge calls them directly on the borrowed ctx. Scope v1 to the ~40 point
data commands (strings incl. counters, hash, set, zset point ops, DEL/
EXISTS/EXPIRE/TTL/EXPIREMEMBER, PFADD); scans/pops later; anything
unbridged errors with "command not supported in scripts".

Per-shard Lua instance reuse (thread-local pool) + per-sha compiled
bytecode cache keep the hot path allocation-free.

## Phasing

| Phase | Contents | Status |
|---|---|---|
| 0 | Hash tags in `pid_of` (+ docs, placement tests) | ✅ done — `{tag}` content is the placement hash input (Redis Cluster rule) |
| 1 | mlua sandbox, SCRIPT LOAD/EXISTS/FLUSH, EVAL/EVALSHA **atomic same-pid path**, inline shard bridge, budgets, system-key script replication | ✅ done — smoke covers conversion table, sandbox, budget abort, CROSSSLOT, undeclared-key, bombardment atomicity; cluster test covers effects replication, exact script counters, `math.random` divergence trap, cross-node EVALSHA |
| 2 | `allow-multi-shard` non-atomic path (async bridge over normal dispatch), SCRIPT KILL | open |
| 3 (maybe) | FUNCTION/FCALL, LuaJIT feature flag | open |

## Test plan (the distributed bits)

1. **Atomicity under fire**: N connections bombard `INCR`+`GET` on a key
   while a script does read-modify-write loops on it — script must never
   observe interleaving (single-shard path).
2. **Cross-node convergence of effects**: script writes on node A appear on
   B/C via replication; a counter-based limiter incremented concurrently
   via scripts on all three nodes converges to the exact total.
3. **Divergence trap**: a `math.random`-writing script executed on one node
   must produce ONE cluster-wide value (proof that effects, not scripts,
   replicate).
4. **Budget**: `while true do end` aborts within the limit; shard latency
   for unrelated keys stays bounded during it.
5. **Sandbox**: `os`, `io`, `require` are nil; memory bomb hits the
   allocator limit.
6. **NOSCRIPT + replicated cache**: EVALSHA on a node that never saw LOAD —
   errors before AE delivers the system record, succeeds after.
7. **Conformance**: run a corpus of public Redis scripts (rate limiters,
   locks, sliding windows) against marekvs and redis-server, diff replies.

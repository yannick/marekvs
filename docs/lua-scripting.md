---
title: Lua scripting
description: EVAL/EVALSHA on marekvs — Redis-grade atomicity per node, and the distributed caveats that come with an AP store.
status: mixed
---

marekvs runs Lua scripts with `EVAL`, `EVALSHA`, and `SCRIPT`, backed by
**mlua** with a vendored **Lua 5.4**. Scripts get Redis-grade atomicity — but
only within a single partition on a single node, because marekvs is AP and has
no cross-node locking. This page is about that boundary.

## The one rule: keys must co-locate

Every key a script touches, declared in `KEYS`, must live in the **same
partition**. Use Redis **hash tags** to guarantee that:

```lua
-- all three keys hash to the same partition via the {order:42} tag
redis.call('HSET', KEYS[1], 'status', 'paid')     -- {order:42}:meta
redis.call('INCR', KEYS[2])                        -- {order:42}:version
redis.call('SADD', KEYS[3], ARGV[1])               -- {order:42}:events
return redis.call('HGET', KEYS[1], 'status')
```

```sh
redis-cli EVAL "$(cat pay.lua)" 3 \
  '{order:42}:meta' '{order:42}:version' '{order:42}:events' shipped
```

Because a partition is served by exactly one shard thread on the node handling
the request, the whole script runs with no interleaving — a true atomic block.

```warning
If the declared keys span more than one partition, the script is **rejected**
with an error. Scripts are **not** a distributed lock primitive: the atomicity
is node-local, and two nodes can each run a co-located script concurrently
against their own copy. Design for convergence, not mutual exclusion.
```

## Effects-only replication

A script never replicates as a script. marekvs replicates only its **effects** —
the individual writes it performed, each stamped with an HLC and merged like any
other write.

That has a useful consequence: **non-deterministic calls are safe.**
`math.random`, `redis.call('TIME', ...)`, and similar are allowed, because the
resulting concrete writes are what propagate — not the code that produced them.
In classic Redis, effects-vs-verbatim replication is a footgun; here there is
only one mode, and it is the safe one.

## EVALSHA and script distribution

`SCRIPT LOAD` (and `EVAL`) store the script body as a hidden, **replicated**
system record under an internal key (`\x00script:<sha>`). Because that record
replicates like ordinary data, a script loaded on one node becomes resolvable on
the others, so `EVALSHA` against a node that never saw the original `EVAL`
usually self-heals rather than returning `NOSCRIPT`.

```sh
sha=$(redis-cli SCRIPT LOAD "return redis.call('GET', KEYS[1])")
redis-cli EVALSHA "$sha" 1 '{cache}:user:7'
```

## Budgets

| Limit | Default | Controls |
|---|---|---|
| Wall-clock deadline | 20 ms | `MAREKVS_SCRIPT_TIME_LIMIT_MS` / `CONFIG SET lua-time-limit` |
| Lua allocator cap | 16 MiB | Per-invocation memory ceiling |

A script that exceeds its deadline or memory budget is aborted. Keep scripts
short — they run on the shard thread and hold up other work on that shard while
executing.

## The redis.call bridge

Inside a script, `redis.call(...)` dispatches back through the same command
engine, executing against the owning shard thread. Only co-located keys are
reachable, so every bridged call stays within the script's partition.

## Roadmap

```planned
Scripting shipped in phases. Phases 0 and 1 (the EVAL/EVALSHA/SCRIPT bridge,
hash-tag co-location, effects-only replication, and script distribution) are
implemented. Later-phase work — broader script-management surface and additional
guardrails — is designed but not yet wired.
```

## Where to go next

- Why co-location works: [partitioning & hash tags](../data-model/#partitioning).
- The atomicity model scripts rely on: [shard threads](../architecture/#the-shard-thread-storage-model).
- Command coverage: [Redis API reference](../redis-api/).

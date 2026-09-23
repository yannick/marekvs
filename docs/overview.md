---
title: Overview
description: SSD-backed storage with JSON and Protobuf CRDTs, distributed budgets, Redis compatibility, and convergent replication.
status: mixed
---

**MareKVS** is a distributed key-value database written in Rust with a
**Redis-compatible API**. It stores data on SSD through the
[ondaDB](https://github.com/yannick/ondadb) LSM engine. The full dataset does not
need to fit in RAM, reducing the memory capacity required as data grows.
RAM supports the block cache, write buffers, and database operations.

Connect with `redis-cli` or a RESP driver. Any node can serve any key.
Writes replicate asynchronously, and hybrid logical clocks and CRDT merge
rules reconcile concurrent updates. The database prioritizes availability
during network partitions and provides eventual consistency.

## Features

- **JSON document CRDTs:** use the RedisJSON command surface while storing each
  path separately, so concurrent edits to different fields survive.
- **Protobuf field CRDTs:** register schemas, validate typed values, and merge
  concurrent updates at field level instead of replacing the whole message.
- **Distributed budgets:** reserve capacity from any node with escrow accounting
  that prevents overspending during partitions and crashes.
- **Redis protocol support:** RESP2 and RESP3 on port `6379`, with strings,
  hashes, sets, sorted sets, lists, streams, pub/sub, and HyperLogLog.
- **Convergent replication:** deterministic merge rules reconcile concurrent
  writes according to each data type’s semantics.
- **Demand-driven replication:** a node that reads a remote key caches it and
  subscribes to updates.
- **Kubernetes deployment:** gossip membership, StatefulSets, and a cluster
  operator support discovery and scaling.
- **Container packaging:** a static binary in a `FROM scratch` image.

## Document review over the Redis protocol

The experimental [DIFF extension](../diff/) compares structured documents,
separates moves from text edits, and lets reviewers accept changes individually.
Two-way comparisons and three-way merges produce immutable suggestion graphs;
decisions replicate as attributed records. Applying a valid selection creates
a snapshot that can be forked for further editing.

Use it for version review or reconciling edited branches. Start with the
[complete review example](../diff/#try-a-complete-review), or browse the
[command reference](../redis-api/#document-comparison-marekvs-extension).

## Design goals

1. **Client compatibility:** support existing Redis clients for the implemented commands.
2. **Storage capacity:** persist datasets larger than RAM through ondaDB.
3. **Local reads:** cache remote keys where applications read them.
4. **Asynchronous replication:** avoid synchronous cross-node round trips on writes.
5. **Replica repair:** limit divergence through background anti-entropy.
6. **Cluster scaling:** redistribute data as nodes join and leave.
7. **Command performance:** serialize per-key read-modify-write operations on shard threads.
8. **Small deployments:** ship a static binary without a base operating system image.

## Compatibility limits

- **Consistency:** reads and writes do not use quorums. Two clients on different
  nodes may observe different values until replicas converge.
- **Redis Cluster:** `MOVED` / `ASK` redirects and client-side slot routing are unsupported.
- **Access control:** authentication uses a single password. TLS and ACLs are unsupported.
- **Persistence format:** Redis RDB/AOF files are unsupported. Recovery uses
  ondaDB storage, replication, and anti-entropy.

## Published guarantees

These guarantees apply **per connection** unless stated otherwise, under the
operating conditions documented in [Consistency & anti-entropy](../consistency/).

| Guarantee | What it means |
|---|---|
| Read-your-writes | A connection always observes its own earlier writes. |
| Monotonic reads | A connection's reads never move backward in time. |
| Convergence | With no new writes, every replica reaches the same value. |
| Exact counters | Concurrent `INCR`/`DECR` across nodes are never lost (an explicit `SET` resets). |
| Bounded staleness | Cross-node divergence heals within seconds; **15 s worst case**, milliseconds typical. |
| No resurrection | Tombstones prevent deleted values from returning during the repair window. |
| TTL convergence | Expiry is decided once at the origin and converges cluster-wide. |
| Durability | ondaDB WAL; a crash may lose only the last fsync window on that one node. |

```note
The staleness bound depends on repair intervals and operating conditions.
See [Consistency & anti-entropy](../consistency/) for the derivation and the
assumptions checked by the chaos test suite.
```

## Cluster architecture

```text
          Redis clients (redis-cli, any RESP driver)
                          │  :6379
        ┌─────────────────┴─────────────────┐
        │        Kubernetes Service          │
        └──┬──────────────┬──────────────┬───┘
           │              │              │
      ┌────┴───┐     ┌────┴───┐     ┌────┴───┐
      │ pod 0  │     │ pod 1  │     │ pod 2  │
      │ RESP   │     │ RESP   │     │ RESP   │   command engine
      │ ondaDB │ ⇄   │ ondaDB │ ⇄   │ ondaDB │   :7373 replication mesh
      └────────┘     └────────┘     └────────┘
           └──────── chitchat gossip ─────────┘  :7946/udp
                                                  :9121 metrics + health
```

Each node runs five subsystems: a RESP frontend, a shard-threaded command
engine, disk-native storage, a replication engine, and the gossip cluster layer.
The [architecture](../architecture/) page walks through each one.

## Glossary

| Term | Meaning |
|---|---|
| **Partition (pid)** | One of 4096 fixed hash partitions; the unit of placement. |
| **Home replicas** | The `N` nodes that own a partition by rendezvous hashing. |
| **Interest replica** | A node caching a key it read but does not own. |
| **Envelope** | The 19-byte per-record header (flags, HLC, origin, TTL). |
| **HLC** | Hybrid logical clock; a packed `[physical ms | logical]` timestamp. |
| **Tombstone** | A delete marker retained for `gc_grace` to prevent resurrection. |

## Where to go next

- Run a local node with the [Quickstart](../quickstart/).
- Read the [Architecture](../architecture/) and [Data model](../data-model/).
- Look up supported commands in the [Redis API reference](../redis-api/).

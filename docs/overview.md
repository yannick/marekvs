---
title: Overview
description: What marekvs is, what it guarantees, and what it deliberately is not.
status: mixed
---

**marekvs** is a distributed key-value database with a **Redis-compatible API**,
written in Rust. It is **AP by design** — available and partition-tolerant,
eventually consistent, and coordination-free — and **disk-native**: it stores
everything in the [ondaDB](https://github.com/yannick/ondadb) LSM engine rather
than keeping the dataset in RAM.

You talk to it with `redis-cli` or any RESP driver. Underneath, there is no
leader, no quorum, and no consensus protocol on the data path. Writes are
fire-and-forget and converge through hybrid logical clocks and CRDT-style
merges. Any node can serve any key.

## What it is

- **A Redis protocol front-end** — RESP2 and RESP3 over `:6379`, with strings,
  hashes, sets, sorted sets, lists, streams, pub/sub, and HyperLogLog.
- **A convergent replicated store** — concurrent writes on different nodes merge
  deterministically instead of one clobbering the other.
- **Demand-driven replication** — a node that reads a remote key caches it and
  subscribes to its updates, so hot data spreads to where it is used.
- **Kubernetes-native** — gossip membership, a StatefulSet, and an operator that
  scales the cluster without losing data.
- **Tiny** — a static binary in a `FROM scratch` container image.

## Goals

1. **Redis-compatible API** — drop-in for the command subset we implement.
2. **Disk-native** — durability and datasets larger than RAM, via ondaDB.
3. **Dynamic, interest-based replication** — replicate what is actually read.
4. **Fire-and-forget writes** — no synchronous cross-node round-trips on the hot path.
5. **Bounded staleness** — divergence heals within seconds, not "eventually."
6. **Kubernetes elasticity** — nodes join and leave; the cluster rebalances safely.
7. **Performance first** — per-key lock-free RMW on shard threads.
8. **Minimal images** — an OS-less static binary.

## Non-goals

marekvs deliberately does **not** try to be everything Redis is:

- **Not linearizable / not CP.** There are no quorum reads or writes and no Raft
  for data. Two clients on two nodes can briefly read different values.
- **Not the Redis Cluster protocol.** No `MOVED` / `ASK` redirects, no slot map.
- **No TLS and no ACLs** beyond `AUTH` with a single password.
- **No RDB/AOF file compatibility.** Recovery is via replication and
  anti-entropy, not by loading a Redis dump.

## Published guarantees

These are the promises the rest of the docs stand behind. They hold **per
connection** unless stated otherwise.

| Guarantee | What it means |
|---|---|
| Read-your-writes | A connection always observes its own earlier writes. |
| Monotonic reads | A connection's reads never move backward in time. |
| Convergence | With no new writes, every replica reaches the same value. |
| Exact counters | Concurrent `INCR`/`DECR` across nodes are never lost (an explicit `SET` resets). |
| Bounded staleness | Cross-node divergence heals within seconds; **15 s worst case**, milliseconds typical. |
| No resurrection | A deleted key stays deleted — tombstones outlive the repair window. |
| TTL convergence | Expiry is decided once at the origin and converges cluster-wide. |
| Durability | ondaDB WAL; a crash may lose only the last fsync window on that one node. |

```note
"Bounded staleness" is a real, derived number — not a hope. The derivation lives
in [Consistency & anti-entropy](../consistency/), and the assumptions behind it are
tracked by the chaos test suite.
```

## System shape at a glance

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

Each node runs five subsystems — a RESP frontend, a shard-threaded command
engine, disk-native storage, a replication engine, and the gossip cluster layer.
The [architecture](../architecture/) page walks through each one.

## Glossary

| Term | Meaning |
|---|---|
| **Partition (pid)** | One of 4096 fixed hash partitions; the unit of placement. |
| **Home replicas** | The `N` nodes that own a partition by rendezvous hashing. |
| **Interest replica** | A node caching a key it read but does not own. |
| **Envelope** | The 19-byte per-record header (flags, HLC, origin, TTL). |
| **HLC** | Hybrid logical clock — a packed `[physical ms | logical]` timestamp. |
| **Tombstone** | A delete marker retained for `gc_grace` to prevent resurrection. |

## Where to go next

- New here? Continue to the [Quickstart](../quickstart/).
- Want the internals? Start with the [Architecture](../architecture/), then the
  [Data model](../data-model/).
- Looking for a command? Jump to the [Redis API reference](../redis-api/).

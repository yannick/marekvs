# 00 — Overview

## What marekvs is

marekvs is a distributed key-value database server that speaks the Redis
protocol (RESP2/RESP3) and persists everything to disk through the
[ondaDB](../../ondadb) LSM storage engine. There is no in-memory dataset:
ondaDB's memtable, block cache, and OS page cache are the only memory tiers.

It is designed as an **AP system** in CAP terms: every node accepts reads and
writes at all times, replication is asynchronous, and convergence is guaranteed
by merge semantics plus anti-entropy — never by coordination.

## Goals

1. **Redis-compatible API** — clients use standard Redis libraries unchanged.
   Command semantics and data structures follow Redis behavior
   (see [03-redis-api.md](03-redis-api.md)).
2. **Disk-native** — dataset size is bounded by disk, not RAM. ondaDB provides
   MVCC transactions, per-key TTL, WAL durability, and a post-commit change
   feed ([01-architecture.md](01-architecture.md#storage-layer)).
3. **Dynamic, interest-based replication** — each key has N home replicas
   (default 3, configurable, minimum 1). Additionally, any node that serves a
   key caches it locally and subscribes to its updates, so hot keys migrate to
   where they are read ([04-replication.md](04-replication.md)).
4. **Fire-and-forget writes** — a write is acknowledged after the local ondaDB
   commit; replication to homes and subscribers is asynchronous with no
   confirmation wait.
5. **Bounded staleness** — despite being eventually consistent, the system
   guarantees that a divergent (stale) record on any home replica is repaired
   within a hard bound (default **15 s worst case, milliseconds typical**);
   see the derivation in
   [05-consistency-anti-entropy.md](05-consistency-anti-entropy.md#staleness-bound).
6. **Kubernetes-native elasticity** — above a minimum node count (= replica
   factor N), nodes join and leave freely; membership is gossip-based with
   Kubernetes DNS used only for seeding
   ([06-cluster-membership.md](06-cluster-membership.md)).
7. **Performance first** — zero-copy hot paths, no TLS/encryption, batched
   replication, per-element storage keys so collection updates are O(element).
8. **Minimal images** — statically linked binary in a `FROM scratch` container
   ([08-build-deploy.md](08-build-deploy.md)).

## Non-goals

- **Linearizability / CP behavior.** No quorum reads/writes, no Raft for data.
  Clients needing read-after-write across connections should not use marekvs v1.
- **Redis Cluster protocol.** No MOVED/ASK redirects; any node serves any key
  behind one Kubernetes Service.
- **Encryption** (in transit or at rest), ACL beyond simple AUTH, Lua scripting,
  modules, RDB/AOF file compatibility.
- **List CRDTs.** Lists are LWW whole-value in v1 (see
  [02-data-model.md](02-data-model.md#lists)).

## Published guarantees (what we tell users)

| Guarantee | Scope | Mechanism |
|---|---|---|
| Read-your-writes | per client connection | writes commit locally before ack; connection is pinned to one node |
| Monotonic reads | per client connection | all remote data merges via HLC max-wins; a node's version of a key never regresses |
| Convergence | global | commutative, idempotent, associative merges (LWW + observed-remove + PN counters) |
| Exact counters | global (v1.1) | INCR family = PN counters; concurrent increments never lost (SET resets) |
| Bounded staleness | home replicas | sequence-cursor resume + Merkle anti-entropy every `ae_round` (5 s); bound ≈ 15 s worst case |
| Bounded staleness | interest replicas | connection-scoped leases: ≤ home bound + push latency while connected; ≤ 3 s heartbeat timeout + revalidation after disconnect (pathological worst case: 60 s lease timer, see risky assumption 4) |
| No deleted-data resurrection | global | tombstones retained `gc_grace` (1 h); nodes down longer re-sync pull-only |
| TTL convergence | global | absolute deadlines set once at origin, evaluated locally everywhere |
| Durability | per node | ondaDB WAL, `SyncMode::Interval` (128 ms fsync window) by default; a crash may lose the last window on that node only — surviving replicas retain the data |

Cross-client consistency is explicitly **not** guaranteed: two clients on two
pods may observe divergent values inside the staleness bound.

## System shape at a glance

```
                       ┌──────────────── Kubernetes Service (one endpoint) ───────────────┐
 redis clients ───────►│  pod 0          pod 1          pod 2          pod N              │
                       │ ┌─────────┐   ┌─────────┐   ┌─────────┐                          │
                       │ │ RESP    │   │ RESP    │   │ RESP    │   any node serves        │
                       │ │ frontend│   │ frontend│   │ frontend│   any key                │
                       │ ├─────────┤   ├─────────┤   ├─────────┤                          │
                       │ │ command │   │ command │   │ command │                          │
                       │ │ engine  │   │ engine  │   │ engine  │                          │
                       │ ├─────────┤   ├─────────┤   ├─────────┤     peer mesh            │
                       │ │ ondaDB  │◄─►│ ondaDB  │◄─►│ ondaDB  │  (repl + fetch +         │
                       │ │ + repl  │   │ + repl  │   │ + repl  │   anti-entropy +         │
                       │ └─────────┘   └─────────┘   └─────────┘   pub/sub)               │
                       │      ▲             ▲             ▲                               │
                       │      └───────── chitchat gossip (membership) ─────────┘          │
                       └──────────────────────────────────────────────────────────────────┘
```

## Glossary

| Term | Meaning |
|---|---|
| **Partition** (`pid`) | One of 4096 fixed key buckets: `pid = xxh3_64(userkey) >> 52`. Every internal storage key is prefixed with its pid, making a partition a contiguous ondaDB key range. |
| **Home replicas** `H(p)` | The N nodes durably responsible for partition p, chosen by rendezvous hashing over the gossip membership view. |
| **Primary home** `H1(p)` | The highest-rendezvous-score alive home. Coordinates interest fan-out and serves fetches. Not a consistency primary — any home accepts writes. |
| **Interest replica** | A non-home node that cached a key on demand and holds a live lease subscribing it to updates. |
| **Envelope** | The 19-byte metadata header prefixed to every stored value: flags, HLC, origin node, TTL deadline. |
| **HLC** | Hybrid logical clock, packed u64 `[48-bit physical ms | 16-bit logical]`. Total order for LWW is `(hlc, origin)`. |
| **Dot** | The unique identity of a set-member/hash-field add: `(origin, hlc)`. Basis of observed-remove semantics. |
| **Head key** | The per-collection storage key holding type, whole-collection tombstone clock, and collection TTL. |
| **Replication ring** | Bounded in-process buffer fed by ondaDB commit hooks; per-peer sender cursors read from it. |
| **gc_grace** | Tombstone retention window (1 h). Nodes offline longer must re-sync pull-only. |

## Literature

- **Dynamo** (DeCandia et al., SOSP'07) — placement, anti-entropy with Merkle
  trees, hinted-repair philosophy (we bound hints with a ring instead of disk
  queues).
- **Anna** (Wu et al., ICDE'18) — coordination-free multi-master KV with
  gossip-fed replication; validates the any-node-writes model.
- **ORSWOT** (Riak DT; Bieniusa et al.) — observed-remove sets without
  per-element vector clocks; our dot-based merge is a per-element
  simplification ("ORSWOT-lite").
- **HLC** (Kulkarni et al., 2014) — hybrid logical clocks; timestamps close to
  wall time with logical-clock causality.
- **PNUTS** (Cooper et al., VLDB'08) — per-record mastery; considered and
  rejected (adds a forwarding hop and failover machinery; conflicts with
  fire-and-forget writes).


## Riskiest assumptions (tracked, with tests)

Test pointers refer to [10-testing.md](10-testing.md).

1. **Envelope-origin echo suppression suffices for loop-freedom.** During
   membership-view divergence two nodes may briefly both act as H1 (harmless
   duplicates) or neither (repaired by anti-entropy — this path consumes the
   15 s bound). → membership-churn Jepsen tests (§10.3).
2. **ondaDB commit-hook contract**: fires exactly once per committed batch, in
   commit order, with the full op list. Cursor-resume replication depends on
   it. → integration test against ondaDB (§10.2).
3. **ORSWOT-lite bias**: dot-based observed-remove without causal context is
   add-wins in races where a remove propagated through an intermediary that saw
   a newer add. Believed acceptable for Redis set semantics. → merge-law
   property tests (§10.1).
4. **Connection-scoped leases as the interest staleness bound** assume
   application heartbeats detect peer death within 3 s. A wedged-but-open
   connection (conntrack blackhole) can serve stale up to the 60 s lease timer.
   Documented as the pathological worst case. → chaos test (§10.4).
5. **HRW balances load only for uniform key traffic.** A single mega-hot key
   still lands on one H1 for interest fan-out; escalation helps readers, not
   the writer. Acceptable v1; per-key H1 offload is future work.

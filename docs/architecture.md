---
title: Architecture
description: The five subsystems inside one marekvs node, the shard-thread storage model, and how the crates fit together.
status: mixed
---

Every marekvs node is a single Rust process, built from a small cargo workspace.
Inside that process are five subsystems — a RESP frontend, a command engine, the
storage layer, a replication engine, and the cluster layer — plus a health and
metrics endpoint. Nodes are symmetric: there is no separate coordinator process,
no leader, and no consensus service. Coordination happens only through gossip and
asynchronous replication.

This page is the map. The [data model](../data-model/) covers byte layouts and
merge rules; [replication](../replication/) covers how writes propagate.

## Process anatomy

```text
┌────────────────────────────────────────────────────────────────────┐
│ marekvs process                                                     │
│                                                                     │
│  ┌───────────────┐   ┌────────────────┐   ┌────────────────────┐    │
│  │ RESP frontend │──►│ command engine │──►│ storage layer      │    │
│  │ (tokio)       │◄──│ (dispatch,     │◄──│ (ondaDB + shard    │    │
│  │ :6379         │   │  arg parsing,  │   │  threads, TTL      │    │
│  └───────────────┘   │  type checks)  │   │  sweeper)          │    │
│                      └───────┬────────┘   └─────────┬──────────┘    │
│                              │              commit hooks            │
│                      ┌───────▼────────┐   ┌─────────▼──────────┐    │
│                      │ cluster layer  │   │ replication engine │    │
│                      │ (chitchat      │◄─►│ (ring, per-peer    │    │
│                      │  gossip,       │   │  senders, apply,   │    │
│                      │  placement,    │   │  interest table,   │    │
│                      │  lifecycle)    │   │  anti-entropy)     │    │
│                      │ :7946/udp      │   │ :7373/tcp ctl+bulk │    │
│                      └────────────────┘   └────────────────────┘    │
└────────────────────────────────────────────────────────────────────┘
```

Each node binds four ports:

| Port | Protocol | Purpose |
|---|---|---|
| `6379` | TCP | Redis client protocol (RESP2/RESP3) |
| `7373` | TCP | Peer replication mesh — ctl + bulk connections per peer |
| `7946` | UDP | chitchat gossip |
| `9121` | HTTP | Prometheus metrics + health / readiness / drain probes |

```note
There is no Raft, Paxos, quorum, or leader election on the data path — by
design. marekvs is AP and coordination-free. Any node can serve any key.
```

## The five subsystems

### RESP frontend

- A tokio TCP listener with one task per client connection.
- `RespParser` (in `marekvs-resp`) is an incremental request parser: raw socket
  bytes are fed in, complete commands are pulled out. It understands RESP
  multi-bulk arrays (`*N\r\n` then N bulk strings) and bare inline commands for
  telnet / health-check compatibility. It is pure protocol logic — no I/O, no
  async, std only.
- `ReplyBuf` is a RESP3-aware serializer that applies the standard RESP2
  downgrades automatically (map → flat array, set → array, `_` null → `$-1`,
  double → bulk string). The protocol version is negotiated per connection via
  `HELLO`.

### Command engine

`marekvs-engine` holds the `Engine`, the per-connection `Session`, and the
command families under `marekvs-engine::cmd` (`string`, `list`, `set`, `hash`,
`zset`, `stream`, `generic`, `pubsub`, `server`, `script`). Each handler parses
its arguments, derives the partition (`pid`) from the key, submits a storage job
to the shard thread that owns that partition, and builds the reply. Multi-key
commands group their keys per shard and run per-shard batches.

### Storage layer

ondaDB is an external LSM engine — a synchronous, thread-safe library
(`DB: Clone + Send + Sync`, no async), consumed as a dependency and **not** part
of this repo. marekvs is disk-native: there is no in-RAM dataset. All ondaDB
access is funneled through the [shard threads](#the-shard-thread-storage-model);
marekvs never calls ondaDB from a tokio worker.

Two ondaDB column families back everything:

- `data` — all user records (envelope + payload); key layouts are in the
  [data model](../data-model/).
- `meta` — node-local state: replication cursors, partition sync state, node
  epoch and liveness stamps.

Each shard thread also runs an incremental **expiry sweeper** that walks `data`,
checks the envelope TTL deadline, and deletes expired records — which emits
tombstone envelopes into replication and `expired` keyspace notifications.

### Replication engine

`marekvs-repl` is fed by ondaDB **commit hooks** installed on the `data` column
family. A commit hook does no I/O — it only enqueues committed ops into a bounded
in-memory ring. From there: per-peer sender tasks (tokio) hold cursors into the
ring and fan writes out; the apply path receives, merges, and writes back through
the shard threads; an interest table tracks per-key leases and read-through of
remote keys; an anti-entropy driver runs Merkle repair rounds; and a bootstrap
streamer ships whole partitions on join. See [replication](../replication/).

### Cluster layer

`marekvs-cluster` wraps chitchat (Quickwit's SWIM-flavored gossip) plus
placement. Gossip carries node id, phase (`Joining` / `Active` / `Leaving`), mesh
address, and epoch. Placement is HRW (rendezvous) hashing: `owners_for(pid)` is a
pure function of the current membership view, recomputed on every view change.
See [membership](../membership/).

## The shard-thread storage model

marekvs serializes all work on a key onto a single OS thread, which makes every
read-modify-write on that key atomic **without locks**.

At startup the store spins up **S shard threads**, where

```text
S = available_parallelism() − 2, floored at 2      (override: MAREKVS_SHARDS)
```

Each shard thread owns an MPSC job queue and exclusive access to a `ShardCtx`
(the `data` and `meta` column-family handles). A key is routed to its shard by

```text
shard = pid % S
```

Because a key's partition maps to exactly one shard thread, every operation on
one key is serialized on one thread. That is what makes `INCR`, `HINCRBY`,
`LPUSH`, `SADD`, and CRDT merges atomic without a per-key mutex — the thread
assignment *is* the lock. Cross-shard commands (e.g. `MSET` across partitions)
run as fan-out jobs and make no cross-key atomicity promise, consistent both with
Redis and with an AP design.

```note
When a command handler is already running on the shard thread that owns its key,
the store executes the job inline instead of round-tripping through the queue —
the same-shard fast path that lets scripting and co-located multi-key work run
synchronously.
```

## Thread & task model

Three distinct executors coexist in the process; keeping them separate is what
keeps ondaDB stalls off the client path.

| Executor | Count | Work |
|---|---|---|
| tokio multi-thread runtime | ~cores | client connections, peer mesh I/O, gossip, HTTP, timers |
| shard threads (std) | `available_parallelism() − 2`, min 2 | all ondaDB reads/writes/iterators, expiry sweeping, Merkle scans |
| ondaDB internal | ondaDB-managed | background LSM flush + compaction, WAL |

Two rules follow:

1. **Never block tokio on ondaDB.** All storage access goes through the shard
   queues. ondaDB calls are usually microseconds, but a compaction stall can
   block a write for milliseconds — that must land on a shard thread.
2. **Commit hooks only enqueue.** The hook runs on the shard thread that
   committed, pushes into the replication ring, and returns. Replies are built on
   the tokio side from bytes the shard job already returned.

## Crate layout

The workspace is eight crates. `ondadb` is an *external* dependency (a sibling
`../ondadb` checkout or the git source), consumed by `marekvs-engine` (storage)
and `marekvs-repl` (checkpoint / bootstrap).

| Crate | Kind | Responsibility |
|---|---|---|
| `marekvs-core` | lib (leaf) | partitioning, HLC, envelopes, internal keys, merge rules — pure, I/O-free, property-tested |
| `marekvs-resp` | lib (leaf) | RESP2/RESP3 parser + reply builder — pure protocol, std only |
| `marekvs-proto` | lib (leaf) | peer wire messages (`PeerMsg`, `ReplBatch`, `ReplOp`) via `postcard` |
| `marekvs-engine` | lib | shard-threaded storage over ondaDB, command families, pub/sub |
| `marekvs-cluster` | lib | chitchat gossip membership + HRW placement + lifecycle |
| `marekvs-repl` | lib | replication ring, peer mesh, interest leases, Merkle anti-entropy, bootstrap |
| `marekvs-server` | bin | process wiring: config, HTTP probes (`http.rs`), Redis-master follow (`redisrepl.rs`) |
| `marekvs-operator` | bin | Kubernetes controller for `MarekvsCluster` resources |

Dependency direction:

```text
server ─► engine ─► resp
   │        └────► core ◄────────────┐
   ├────► repl ─► {core, proto, engine, cluster}
   └────► cluster ─► core

core, resp, proto   leaves — no marekvs deps
operator            standalone binary: drives the cluster via the Kubernetes
                    API and pod :9121 metrics; does not link the data-path crates
```

`core`, `resp`, and `proto` never depend on anything else in the workspace. Key
external crates: `tokio`, `chitchat` (gossip), `postcard` (wire encoding),
`xxhash-rust` (xxh3), `crossbeam-channel`, `mlua` (Lua), `prometheus`.

## Startup sequence

`marekvs-server` wires the process together in order (see `main.rs`):

1. Load config from the environment; derive `NodeId` from the pod ordinal
   (`MAREKVS_NODE_ID`, or parsed from `HOSTNAME` on a StatefulSet).
2. Open ondaDB at `MAREKVS_DATA_DIR`; create or open the `data` + `meta` column
   families; install the commit hook on `data`. (ondaDB replays its own WAL
   internally during open.)
3. Start the `S` shard threads.
4. Bring up the cluster layer in the `Joining` phase: load persisted fallback
   seeds, join gossip.
5. Bind the peer mesh listener on `:7373` and connect to known peers.
6. Bootstrap any owed partitions, then transition to `Active`.
7. Start the HTTP probe / metrics server on `:9121`, then accept client
   connections on `:6379` — the RESP listener opens only once the node is
   `Active`, and the readiness probe gates traffic the same way.

```note
The `:9121` endpoint serves `GET /metrics` (Prometheus text), `GET /ready`
(200 while the phase is `Active` / `Leaving`, else 503), `GET /alive`, and
`GET /drain` (sets the phase to `Leaving`, used as the preStop hook). It is plain
HTTP — the production image is `FROM scratch`, so probes and the operator speak
minimal HTTP/1.1 with no TLS.
```

## Where to go next

- How records are laid out and merged: [Data model](../data-model/).
- How writes propagate: [Replication](../replication/).
- How to configure and deploy a node: [Build & deploy](../build-deploy/).

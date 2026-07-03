# 01 — Process Architecture & Runtime Model

## Process anatomy

Every marekvs pod runs one process with five subsystems:

```
┌────────────────────────────────────────────────────────────────────┐
│ marekvs process                                                    │
│                                                                    │
│  ┌──────────────┐    ┌───────────────┐    ┌────────────────────┐   │
│  │ RESP frontend │──►│ command engine │──►│ storage layer      │   │
│  │ (tokio)       │◄──│ (dispatch,     │◄──│ (ondaDB + shard    │   │
│  │ :6379         │    │  arg parsing,  │    │  threads, TTL      │   │
│  └──────────────┘    │  type checks)  │    │  sweeper)          │   │
│                      └───────┬───────┘    └─────────┬──────────┘   │
│                              │              commit hooks           │
│                      ┌───────▼───────┐    ┌─────────▼──────────┐   │
│                      │ cluster layer  │    │ replication engine │   │
│                      │ (chitchat      │◄──►│ (ring, per-peer    │   │
│                      │  gossip,       │    │  senders, apply,   │   │
│                      │  placement,    │    │  interest table,   │   │
│                      │  lifecycle)    │    │  anti-entropy)     │   │
│                      │ :7946/udp      │    │ :7373/tcp (ctl+bulk)│  │
│                      └───────────────┘    └────────────────────┘   │
└────────────────────────────────────────────────────────────────────┘
```

Ports: `6379` RESP (clients), `7373` peer mesh (ctl + bulk TCP), `7946/udp`
chitchat gossip, `9121` Prometheus metrics + health HTTP.

### RESP frontend

- tokio TCP listener; one task per client connection.
- Incremental RESP parser `RedisParser`
  (`src/facade/redis_parser.cc`): a state machine consuming buffers as they
  arrive, supporting RESP arrays and inline commands, yielding argument slices
  without copies where possible.
- Reply builder hierarchy `reply_builder.cc`: a
  `ReplyBuilder` trait with RESP2/RESP3 implementations. RESP3 emits native
  MAP/SET/PUSH/double/null frames; RESP2 applies the standard downgrades
  (MAP → flat array, SET → array, `_\r\n` → `$-1`). Version negotiated via
  `HELLO` per connection.
- Output is written through a vectored, buffered writer; pipelined commands
  are executed back-to-back and flushed once per readable burst.

### Command engine

- A static command table built at startup, one entry per command:
  `CommandDef { name, arity, flags, first_key, last_key, key_step, handler }` —
  `CommandId` (see
  `src/server/command_registry.h`), which gives us key extraction, read/write
  classification, and DENYOOM-style flags for free.
- Handlers are grouped in family modules:
  `cmd/string.rs`, `cmd/list.rs`, `cmd/set.rs`, `cmd/hash.rs`, `cmd/zset.rs`,
  `cmd/stream.rs`, `cmd/generic.rs`, `cmd/pubsub.rs`, `cmd/server.rs`.
- Each handler: parse args → derive `pid` from the key → submit a storage job
  to the owning shard thread → build the reply. Multi-key commands
  (MSET, SUNIONSTORE, …) group per shard and run per-shard batches.

### Storage layer

ondaDB is a synchronous, thread-safe library (`DB: Clone + Send + Sync`, no
async). We do **not** call it from tokio worker threads. Instead:

- **S shard threads** (default: `num_cpus - 2`, min 2). Each shard thread owns
  an MPSC job queue. A storage job is a closure over `(&DB, &Arc<ColumnFamily>)`
  returning a reply payload; the tokio side awaits a oneshot.
- Keys are routed to shard threads by `pid % S`, so all operations on one key
  are serialized on one thread — this gives per-key atomic read-modify-write
  (INCR, HINCRBY, LPUSH) **without locks**, using plain ondaDB
  `ReadCommitted` auto-commit or a short `Txn` per command.
- Cross-shard commands (MSET across pids, SINTERSTORE) run as fan-out jobs and
  do not need atomicity across keys (Redis makes no such promise for
  multi-key commands under concurrency either, and we are AP anyway).
- **Column families**:
  - `data` — all user records (envelope + payload), key layouts in
    [02-data-model.md](02-data-model.md).
  - `meta` — node-local state: `applied_seq[origin]` cursors, partition sync
    states, node epoch/last-alive stamp.
  - CF config: `sync_mode = Interval` (128 ms), lz4 compression, bloom filters
    on (defaults per [09-performance.md](09-performance.md)).
- **TTL**: ondaDB expires lazily (read + compaction). Redis needs active
  expiry for keyspace notifications and SCAN hygiene, so each shard thread
  runs an **expiry sweeper**: an incremental cursor walking `data` with an
  ondaDB iterator, checking envelope `ttl_deadline_ms`, deleting expired
  records (which emits tombstone envelopes → replication + `expired`
  notifications). Budget: ≤ 1 ms per 100 ms tick per shard.
- **ondaDB TTL is also set on every record that has a Redis TTL** (deadline +
  `gc_grace`) as a backstop so even unswept garbage vanishes in compaction.

### Replication engine

Fed by ondaDB **commit hooks** (`CommitHookFn(seq, &[CommitOp])`, installed on
the `data` CF). The hook only enqueues into the replication ring — never does
I/O. Full design in [04-replication.md](04-replication.md) and
[05-consistency-anti-entropy.md](05-consistency-anti-entropy.md). Components:

- bounded replication ring (128 MiB / 262,144 ops),
- per-peer sender tasks (tokio) with cursors into the ring,
- apply path (receiver → merge → ondaDB batch via shard threads),
- interest table (per-key leases + partition escalation),
- anti-entropy driver (Merkle rounds),
- bootstrap streamer (ondaDB checkpoint + chunked transfer).

### Cluster layer

chitchat gossip (Quickwit's SWIM-flavored crate) carries: node id, state
(Joining/Active/Leaving/Down), mesh address, epoch, pub/sub channel summary.
Placement (`owners(pid)`, `H1(pid)`) is a pure function of the current view,
cached as a flat `[pid] → [NodeId; N]` table, recomputed on view change.
Details in [06-cluster-membership.md](06-cluster-membership.md).

## Thread & task model

| Executor | Count | Work |
|---|---|---|
| tokio multi-thread runtime | default (= cores) | client connections, peer mesh I/O, gossip, timers |
| shard threads (std) | `num_cpus - 2` | all ondaDB reads/writes/iterators, expiry sweeping, Merkle bucket scans |
| ondaDB internal | 2 flush + 2 compaction | background LSM maintenance (ondaDB-managed) |

Rules:

1. **Never block tokio on ondaDB.** All storage access goes through shard
   queues. (ondaDB calls are microseconds typically, but compaction stalls and
   `l0_queue_stall_threshold` gating can block writes for milliseconds.)
2. **Commit hooks only enqueue.** The hook runs on the shard thread that
   committed; it pushes `(seq, ops)` into the ring (a lock-free SPMC-read
   structure with one writer per shard, see 04) and returns.
3. **Replies are built on tokio side** from owned bytes returned by the shard
   job. For large values (vlog-resident), the shard job copies once out of the
   pinned block; zero-copy pass-through to the socket is a v1.1 optimization
   ([09-performance.md](09-performance.md)).

## Crate layout (cargo workspace)

```
marekvs/
  Cargo.toml                 # workspace
  crates/
    marekvs-server/          # bin: main, config, wiring
    marekvs-resp/            # RESP2/3 parser + reply builder (no deps on rest)
    marekvs-core/            # envelope, HLC, internal keys, merge rules, partitioning
    marekvs-engine/          # command table + family handlers, shard threads, expiry
    marekvs-cluster/         # chitchat integration, placement, lifecycle state machine
    marekvs-repl/            # ring, senders, apply, interest, anti-entropy, bootstrap
    marekvs-proto/           # peer wire messages (postcard), framing
  design/                    # these documents
  deploy/                    # k8s manifests (07)
  Dockerfile
```

Dependency direction: `server → engine → core`, `server → repl → {core, proto,
cluster}`, `resp` and `proto` are leaves. `ondadb = { path = "../ondadb" }`
is consumed by `marekvs-engine` (and `marekvs-repl` for checkpoint/bootstrap).

Key external crates: `tokio`, `chitchat`, `postcard` + `serde`, `xxhash-rust`
(xxh3), `crossbeam` (ring + channels), `parking_lot`, `smallvec`, `bytes`,
`tracing`, `metrics`/`prometheus`.

## Startup sequence

1. Load config (env + optional file); derive `NodeId` from pod ordinal
   ([07-kubernetes.md](07-kubernetes.md)).
2. `DB::open` on the PVC path; create/open `data` + `meta` CFs; install commit
   hook on `data`.
3. Start shard threads; replay nothing (ondaDB WAL replay is internal to open).
4. Start gossip in `Joining` (or fast-path to `Active` if `meta` shows a clean
   recent shutdown and ownership unchanged — see
   [06-cluster-membership.md](06-cluster-membership.md#restart-fast-path)).
5. Start peer mesh listener; establish ctl/bulk connections to known peers.
6. Bootstrap owed partitions if joining; flip `Active`.
7. Open RESP listener **only after** entering `Active` (readiness probe gates
   traffic the same way).

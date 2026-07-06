# 04 — Replication: Placement, Write/Read Paths, Interest Subscriptions

## Topology model

Keys hash into `P = 4096` fixed partitions ([02-data-model.md](02-data-model.md#partitioning)).
Three replica roles:

- **Home replicas `H(p)`** — the N nodes (default 3) durably responsible for
  partition p. Unconditional, permanent until placement changes.
- **Primary home `H1(p)`** — highest-scoring alive home; coordinates interest
  fan-out and serves fetches. *Not* a consistency primary: any home (indeed any
  node) accepts writes.
- **Interest replicas** — nodes that cached keys on demand; lease-based,
  evictable, never sources for anti-entropy.

## Placement: rendezvous hashing

```
score(node, pid) = xxh3_64(node_id_bytes || pid_le_bytes)
owners(pid)      = top-N alive nodes by score, states ∈ {Active, Leaving}
H1(pid)          = highest-score owner with state == Active
```

**Why HRW, not a virtual-node ring:** at the target scale (3–50 nodes),
recomputing 4096 × nodes scores on membership change is trivial (≈200k xxh3
calls, cached as a flat `[pid] → [NodeId; N]` table). HRW needs no ring
metadata or token management, and gives minimal, evenly-scattered disruption:
a joining node steals ~P/n partitions spread across all nodes; a dead node's
partitions scatter evenly to all survivors (built-in thundering-herd
spreading). Weighted HRW (`score = -w / ln(u)`) covers heterogeneous nodes if
ever needed. Rings win only at hundreds of nodes.

Placement is a **pure function of the gossip membership view**. Views may
disagree transiently (AP); consequences are duplicate replicas or a briefly
missed home — both healed by anti-entropy within the staleness bound
([05-consistency-anti-entropy.md](05-consistency-anti-entropy.md)).

Ownership changes on membership events are described in
[06-cluster-membership.md](06-cluster-membership.md). A node that loses
ownership of a partition demotes it to **cold** (kept on disk, servable as a
fetch source, purged after `cold_purge_delay = 15 m` or under disk pressure).

## Write path

`SET k v` arriving at node X (X may or may not be a home for `pid(k)`):

```
client ──► X: stamp HLC → envelope → ondaDB commit (one Txn per command) ──► ack client
                                   │
                        commit hook (seq, ops)
                                   ▼
                        replication ring (bounded)
                                   ▼
                 per-peer sender cursors, fan-out rule:
                   • → every node in owners(pid) except self and except op.origin
                   • → interest subscribers of the key/partition, iff self == H1(pid)
```

1. **Local commit first, always.** Ack after the local ondaDB commit — this is
   the fire-and-forget contract and gives per-connection read-your-writes.
2. The **commit hook** pushes `(seq, ops)` into the ring and returns
   (no I/O on the commit path).
3. The fan-out rule creates a two-hop DAG: `origin → homes → interest
   subscribers`. Homes never forward to homes; only H1 forwards to
   subscribers. The envelope's `origin` field suppresses echo — a receiver's
   own commit hook fires for applied remote ops, but its senders skip the op's
   origin and, since a non-H1 home forwards to nobody else, no loops form.
4. If X is not a home for the key, applying its own write makes X an interest
   replica: the push to H1 carries an `IMPLICIT_SUB` flag registering X's
   lease (write-implies-subscribe, mirroring fetch-implies-subscribe).
5. **Duplicates are harmless** — every merge is idempotent
   ([02-data-model.md](02-data-model.md#merge-rules-the-heart-of-convergence)).

### Crash-safety of the push path (chaos findings)

The ring is in-memory and its seq numbers are meaningful to consumers
(they persist "applied up to S per origin" and reconnect with
`ResumeFrom{S}`). Three mechanisms keep acked writes from stranding on
their origin — each one closes a hole the chaos suite actually caught:

1. **Seq space survives restarts.** The ring high-water mark is persisted
   (~1 s cadence); a restart resumes at `hw + 1e6`. Without this, a
   restarted origin re-numbers from 1, every stale consumer cursor looks
   "caught up" (`cursor >= last_seq`), and the pump silently ships nothing
   the node accepts until seqs pass the stale cursor again.
2. **Boot re-offer.** After the view settles (and again on every view
   epoch change), a node pushes every record it holds for partitions it
   does NOT own to a current owner. This heals strands created by SIGKILL
   (unshipped ring entries die with the process) and by ownership moves —
   which owners-only Merkle AE can never repair, because the owners AGREE
   with each other and the gauge reads 0.
3. **Backlog-aware drain.** SIGTERM waits (bounded) for every peer's
   **acked** seq — shipped *and* acknowledged, not merely queued on a
   socket — to reach the ring head before exiting, instead of a fixed
   grace sleep. The last-moment ack window otherwise leaves with the
   process.
4. **Commit-context attribution, not envelope origin.** Each ring entry's
   origin (used by the fan-out rule to suppress echo) is the origin of the
   *batch being applied on the shard thread* — a thread-local set around
   the apply job — not the record envelope's origin. A merged CRDT record
   keeps the version winner's origin, so envelope-based attribution made a
   node that held a clock-skewed peer's future-stamped counter attribute
   its OWN later increments to that peer, and the `origin == self` home
   push dropped them for the duration of the skew. See design/10 finding 5.

### Anti-entropy digests are content-aware

The Merkle bucket digest and diff key on `(ikey, hlc, value_hash)`, not
just `(ikey, hlc)`. Merged CRDT records (PN counters, HLL registers) can
carry the **same** envelope version (version = symmetric max) with
**different** payloads on two replicas; a version-only digest calls them
equal and AE never repairs the divergence. The value hash makes
equal-version/different-content records repair in both directions — the
backstop that guarantees convergence even when the push path mis-fires
(design/10 finding 6).

### Wire format

Postcard-encoded after the frame header ([framing](#transport)):

```rust
struct ReplBatch {
    origin: NodeId,        // u16
    first_seq: u64,        // origin's ondaDB seq of first op (cursor resume)
    last_seq: u64,         // highest ring seq this batch COVERS on the sender,
                           // including entries filtered out for this peer —
                           // this is the value the receiver acks and persists
    ops: Vec<ReplOp>,      // ≤256 ops / ≤1 MiB payload, whichever first
}
struct ReplOp {
    ikey: Bytes,           // full internal key (pid + tag + userkey [+ elem])
    env_and_payload: Bytes, // 19-byte envelope + payload, verbatim ondaDB value
}
```

No separate delta encoding exists or is needed: per-element keys make every
hash/set/zset mutation a single-element op by construction; strings ship the
full value (a string *is* the delta); a collection DEL is one head-key
tombstone.

### Replication ring & backpressure

- One bounded ring per process: **128 MiB or 262,144 ops**, whichever first.
  Shard threads write (one producer segment per shard, sequenced by ondaDB
  seq); per-peer sender tasks hold read cursors.
- Receiver acks `ReplBatch` with `AckSeq{origin, seq = last_seq}`.
  `last_seq` is the highest ring seq the batch *covers* on the sender —
  including entries filtered out for that peer; acking an op count would
  never match the sender's cursor under interest-filtered traffic, so
  windows would never drain and `ResumeFrom` would rewind too far. Acks
  drain the per-peer unacked window — **4 MiB**
  (`MAREKVS_REPL_WINDOW_BYTES`) — and never gate client acks. A full window
  stalls **only that peer's lane** (skipped pump passes counted by
  `marekvs_repl_window_stalls_total`, warn log after 5 s); the cursor
  advances only on successful send, and the ring is the retransmit buffer —
  no separate resend queue.
- **Slow/dead peer:** when a peer's cursor falls off the ring tail, the
  sender (a) drops the cursor, (b) marks every partition shared with that peer
  **dirty-pair(peer, pid)**, (c) stops streaming to it. Recovery: the peer
  reports `last_applied_seq[origin]` on reconnect (`ResumeFrom`); if still in
  the ring → replay; else → Merkle anti-entropy on the dirty pairs
  ([05](05-consistency-anti-entropy.md)). **No unbounded disk hint queues** —
  the ring plus a tight anti-entropy period is the whole lag story.

### Apply path (receiver)

For each op: route to the shard thread for `pid`, read current envelope for
`ikey`, run the merge rule, write only if the incoming version wins — one
ondaDB `Txn` per `ReplBatch` per shard. Applied ops re-enter the local commit
hook; the DAG rule (above) prevents echo.

## Interest subscriptions

State held **in memory** by each home node:

```rust
interest:      HashMap<Pid, HashMap<Bytes /*userkey*/, SmallVec<[(NodeId, Instant); 4]>>>
part_interest: HashMap<Pid, HashMap<NodeId, Instant>>   // escalated whole-partition subs
```

**Exact keys, not blooms**: bloom false positives would fan writes to
uninterested nodes, and blooms can't expire entries. Metadata is bounded by:

- **Escalation**: one subscriber holding > `interest_escalate = 4096`
  key-leases in a partition is converted to a partition-level subscription
  (it becomes a shadow replica fed every op for that pid); per-key entries drop.
- **Global cap** `interest_max_entries = 1,000,000` (~120 MB,
  `MAREKVS_INTEREST_MAX_ENTRIES`) — a hard cap, since a client scanning
  unique keys through non-home nodes can otherwise inflate the map without
  limit (an OOM you can cause from redis-cli). Policy at cap: **reject** new
  registrations, always allow refreshing an existing leaf. Rejection is safe
  in the AP model: the subscriber's own lease timer expires independently,
  so a rejected registration degrades to worst-case-lease (60 s) staleness.
  Gauges: `marekvs_interest_entries`, `marekvs_interest_rejected_total`.

**Lifecycle:**

| Event | Behavior |
|---|---|
| Create | first remote GET: `FetchResp` carries value **and** lease (`interest_lease = 60 s`). Fetch implies subscribe — one RTT. Collections subscribe at collection granularity (head + all elements, one lease). |
| Renew | subscriber batches keys it actually served since last renew → `InterestRenew{pid, keys}` every 15 s, re-arming to 60 s. Unread keys expire on both sides. |
| Connection scope | a lease is valid only while the subscriber holds a live ctl connection to the current H1(pid): heartbeat 1 s, dead after 3 s. On connection loss / H1 change / restart, the subscriber marks all affected leases invalid immediately. |
| Lease-expired read | cached value stays on disk; an in-memory lease table gates freshness. Read of an expired-lease key → `Check{ikey, hlc}` to H1 → `Fresh` (re-arm) or `Newer{env,payload}` (merge, re-arm). Cheaper than refetch for large values. |
| Subscriber restart | lease table is memory-only → every non-home local key is lease-expired → lazy revalidation on first read. No resubscription storm. Home-side leases die by heartbeat timeout. |

## Read path

`GET k` at node X, `p = pid(k)`:

1. **X ∈ owners(p)** → serve locally (envelope decode, TTL check). May be
   behind an in-flight push — that's AP, bounded by anti-entropy.
2. **X caches k, lease valid** → serve locally.
3. **X caches k, lease expired/invalid** → `Check` to H1(p), merge if newer,
   serve, re-arm.
4. **X lacks k** → `FetchCollection{userkey}` to H1(p), fallback to each
   remaining home in rank order (`FETCH_TIMEOUT = 300 ms` per owner).
   Response committed locally **via merge** (a concurrent local write can't
   be regressed), lease registered, value served. The collection fetch
   streams all element keys of that user key, so one RTT hydrates the whole
   collection.

   **Soft failure, by design:** if every owner is unreachable, the command
   serves the local (possibly empty) view instead of erroring — a
   non-owner may transiently report an existing key as absent, bounded by
   the anti-entropy staleness window ([05](05-consistency-anti-entropy.md)).
   An earlier revision of this spec returned `-UNAVAILABLE partition p
   unreachable` here; the implementation deliberately fails soft to keep
   reads available under partition (AP), at the cost of freshness honesty
   in exactly this window.

**Freshness honesty:** per-connection read-your-writes and monotonic reads
hold (local-commit-first + HLC max-wins merges). Nothing across connections.
Optional v1.1: `MVS.SESSION` HLC watermark tokens for cross-connection session
guarantees.

## Pub/Sub

<a name="pubsub"></a>
Cluster size (3–50) → **filtered full-mesh fan-out**, no broadcast tree
(a tree buys nothing below hundreds of nodes; ctl connections already exist).

- Each node gossips its subscription summary via a chitchat KV: exact channel
  list while ≤ 1024 channels (postcard, versioned), else a 64 KiB blocked
  bloom filter (rebuilt on change). A separate `has_patterns` flag marks nodes
  with PSUBSCRIBE clients — they receive every publish (pattern matching is
  local, glob semantics as Redis).
- `PUBLISH ch msg` at X: deliver to local subscribers → send
  `Publish{channel, payload}` on ctl only to peers whose summary matches or
  that set `has_patterns`. At-most-once, fire-and-forget — exactly Redis
  pub/sub semantics.
- Local delivery uses a sharded channel registry `ChannelStore`, RCU-swapped 
  subscriber maps, 16 shards  adapted to tokio broadcast senders.
- Keyspace notifications ride the same mesh, generated at the origin node only
  ([03-redis-api.md](03-redis-api.md#keyspace-notifications)).

## Transport

<a name="transport"></a>
- **TCP + tokio**, `TCP_NODELAY`, 4 MiB socket buffers. **QUIC rejected**:
  quinn mandates TLS (no-encryption constraint), burns CPU in a userspace
  stack, and its HOL-blocking win is captured by the two-connection split.
- **Two connections per peer pair**, both dialed by the lower NodeId:
  - `ctl` — latency-sensitive: ReplBatch, Ack, Fetch/Check, Interest, Publish,
    heartbeats.
  - `bulk` — bootstrap chunks and Merkle exchanges (lz4-compressed frames).
    Separating bulk removes head-of-line blocking without multiplexing
    machinery.
- **Heartbeat**: every connection (ctl *and* bulk) pings its peer each
  `MAREKVS_MESH_PING_INTERVAL_MS` (1 s) and is closed after
  `MAREKVS_MESH_IDLE_TIMEOUT_MS` (3 s) without inbound bytes; Ping/Pong is
  answered in the mesh reader on the same connection. TCP alone cannot
  detect a wedged-but-open connection (conntrack blackhole), and gossip
  phi-accrual detects dead *nodes*, not dead *connections*. Idle closes are
  counted (`marekvs_mesh_conn_timeouts_total`), and a disconnect removes the
  peer's registry entry — `connected_peers` is truthful.
- **Incoming lanes**: received frames are demuxed onto two channels — a
  latency lane (Repl/Ack/Fetch/Check/Interest/Publish) and a heavy lane for
  all AE + bootstrap messages, so digest scans and partition streams never
  head-of-line-block read-through fetches.
- **Framing**: `[len: u32 LE][postcard body]`, max frame 8 MiB. The body is
  a single `PeerMsg` enum; postcard's varint discriminant replaces the
  manual msg-type byte (one enum = one registry, still compact).
- **Codec**: postcard (serde) for message bodies — compact varints, evolvable
  via `#[serde(default)]`. Exception: `ReplOp.env_and_payload` and fetch
  payloads are raw bytes copied verbatim from/to ondaDB values — zero
  re-encode on the hot path. (rkyv rejected: schema-evolution pain; bincode:
  larger wire size.)

**Message registry (`PeerMsg` variants, in wire order):**

```
Hello          ReplBatch        AckSeq            ResumeFrom
Fetch          FetchResp        FetchCollection   FetchCollectionResp
Check          CheckResp        InterestRenew     MerkleRoot
MerkleRootMatch  MerkleBuckets  BucketKeys        RepairOps
RequestKeys    BootstrapReq     BootstrapChunk    BootstrapDone
Publish        Ping             Pong              BudgetReserve
BudgetReserveResp  BudgetClose  BudgetCloseResp
```

The `Budget*` messages are the forwarded-grant / issuer-routed-close RPCs of
the `BG.*` escrow protocol ([13-budget.md](13-budget.md)); the enum is
append-only (postcard discriminants are positional).

`HandoffAck` is **removed** — planned-leave completion is drain-based
([06](06-cluster-membership.md)). `ReplBatch.last_seq` and
`MerkleRootMatch{pid}` (the gc_grace-rejoin per-partition completion signal)
were added in the same change. Postcard discriminants are positional, so any
registry change is a **wire break: whole-cluster upgrade required, no
mixed-version mesh.**

All tunables referenced here are collected in the
[defaults table](05-consistency-anti-entropy.md#defaults-table).

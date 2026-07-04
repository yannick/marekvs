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
3. **Backlog-aware drain.** SIGTERM waits (bounded) for all peer cursors
   to reach the ring head before exiting, instead of a fixed grace sleep —
   the last-moment ack window otherwise leaves with the process.

### Wire format

Postcard-encoded after the frame header ([framing](#transport)):

```rust
struct ReplBatch {
    origin: NodeId,        // u16
    first_seq: u64,        // origin's ondaDB seq of first op (cursor resume)
    ops: Vec<ReplOp>,      // ≤256 ops / ≤256 KiB / 2 ms linger, whichever first
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
- Receiver acks `ReplBatch` with `AckSeq{origin, seq}`; acks advance the
  sender's persisted cursor floor — they never gate client acks.
  Per-peer unacked window: **4 MiB** (sender pauses that peer past it).
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
- **Global cap** `interest_max_entries = 1,000,000` (~120 MB), LRU-evicted.
  Eviction is safe: the subscriber's own lease timer expires independently, so
  it revalidates at most one lease period late.

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
4. **X lacks k** → `Fetch{ikey}` to H1(p); fallback next-ranked home, then any
   home (2 retries, 50 ms timeout each; all homes unreachable → error
   `-UNAVAILABLE partition p unreachable`). Response committed locally **via
   merge** (a concurrent local write can't be regressed), lease registered,
   value served. Collection fetch = `FetchCollection` streaming all element
   keys of that user key.

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
- **Framing**: `[len: u32 LE][msg_type: u8][flags: u8][body…]`, max frame
  8 MiB.
- **Codec**: postcard (serde) for message bodies — compact varints, evolvable
  via `#[serde(default)]`. Exception: `ReplOp.env_and_payload` and fetch
  payloads are raw bytes copied verbatim from/to ondaDB values — zero
  re-encode on the hot path. (rkyv rejected: schema-evolution pain; bincode:
  larger wire size.)

**Message registry (u8):**

```
01 ReplBatch   02 AckSeq        03 Fetch          04 FetchResp
05 FetchCollection  06 Check    07 CheckResp      08 InterestRenew
09 MerkleRoot  0A MerkleBuckets 0B BucketKeys     0C BootstrapReq
0D BootstrapChunk   0E BootstrapDone  0F HandoffAck  10 Publish
11 Ping/Pong   12 ResumeFrom
```

All tunables referenced here are collected in the
[defaults table](05-consistency-anti-entropy.md#defaults-table).

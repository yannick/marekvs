---
title: Replication
description: How writes fan out, how nodes own partitions, and how interest-based caching spreads hot data — the fire-and-forget push path and its crash-safety.
status: mixed
---

marekvs replicates by **pushing**, not by quorum. A write commits locally, acks
the client, and is then fanned out to the nodes that own its partition and to
the nodes that have subscribed to it. There is no coordinator on the data path
and no synchronous cross-node round-trip. Divergence — from a dropped push, a
ring overrun, or a transient membership disagreement — is healed by
[anti-entropy](../consistency/) within the staleness bound.

This page covers placement, the write and read paths, interest subscriptions,
the wire format, and the transport. Every tunable named here lives in the
single-source-of-truth [defaults table](../consistency/#defaults-table).

## Topology

Keys hash into `P = 4096` fixed partitions (the unit of placement — see the
[data model](../data-model/#partitioning)). Each partition has three replica
roles:

| Role | Meaning |
|---|---|
| **Home replicas `H(p)`** | The `N` nodes durably responsible for partition `p`. Permanent until placement changes. |
| **Primary home `H1(p)`** | The highest-scoring alive home. Coordinates interest fan-out and serves fetches — *not* a consistency primary; any node accepts writes. |
| **Interest replicas** | Nodes caching keys they read on demand. Lease-based, evictable, never a source for anti-entropy. |

Home replicas are chosen by **HRW (highest-random-weight, a.k.a. rendezvous)
hashing**, not a token ring:

```text
score(node, pid) = xxh3_64(node_id_bytes ‖ pid_le_bytes)
owners(pid)      = top-N alive nodes by score, states ∈ {Active, Leaving}
H1(pid)          = highest-score owner with state == Active
```

`N` is set by `MAREKVS_REPLICAS_N` (default `3`) and must match cluster-wide.
Placement is a **pure function of the gossip membership view**: every node
computes the same `[pid] → [NodeId; N]` table from the same view, with no ring
metadata or token management to keep in sync.

```note
**Why HRW, not a virtual-node ring.** At the target scale of 3–50 nodes,
recomputing 4096 × nodes scores on a membership change is trivial (≈200k xxh3
calls, cached as a flat table). HRW gives minimal, evenly-scattered disruption:
a joining node steals ~`P/n` partitions spread across all nodes, and a dead
node's partitions scatter evenly to all survivors — built-in thundering-herd
spreading. Rings only win at hundreds of nodes.
```

Views may disagree transiently — that is the AP contract. The consequences are
duplicate replicas or a briefly missed home, both healed by anti-entropy. See
[membership-view divergence](../membership/#divergence) for the fine print.

## Write path

`SET k v` arriving at node `X` (which may or may not be a home for `pid(k)`):

```text
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

1. **Local commit first, always.** The client is acked after the local ondaDB
   commit. This is the fire-and-forget contract, and it gives per-connection
   read-your-writes.
2. The **commit hook** pushes `(seq, ops)` into the ring and returns — no I/O
   on the commit path.
3. The fan-out rule creates a two-hop DAG: `origin → homes → interest
   subscribers`. Homes never forward to homes; only H1 forwards to subscribers.
4. If `X` is not a home for the key, applying its own write makes `X` an
   interest replica: the push to H1 carries an `IMPLICIT_SUB` flag registering
   `X`'s lease (write-implies-subscribe, mirroring fetch-implies-subscribe).
5. **Duplicates are harmless** — every merge is idempotent, so a value arriving
   twice (push plus anti-entropy) converges to the same result.

### Echo suppression

Applied remote ops re-enter the local commit hook, so without a stop rule they
would loop. The envelope carries an **`origin`** field, and each sender skips
the op's origin; since a non-H1 home forwards to nobody, no cycles form.

```caution
Echo suppression attributes each ring entry to the origin of the **batch being
applied on the shard thread** — a thread-local set around the apply job — not to
the record envelope's `origin`. A merged CRDT record keeps the version winner's
origin, so envelope-based attribution once made a node holding a clock-skewed
peer's future-stamped counter attribute its *own* later increments to that peer;
the `origin == self` home push then dropped them for the duration of the skew.
Commit-context attribution closes that hole (chaos suite, design/10 finding 5).
```

### Crash-safety of the push path

The ring is in-memory and its seq numbers are meaningful to consumers — peers
persist "applied up to `S` per origin" and reconnect with `ResumeFrom{S}`. Four
mechanisms, each closing a hole the chaos suite actually caught, keep acked
writes from stranding on their origin:

1. **Seq space survives restarts.** The ring high-water mark is persisted
   (~1 s cadence); a restart resumes at `hw + 1_000_000`. Without this, a
   restarted origin re-numbers from 1, every stale consumer cursor looks
   "caught up" (`cursor >= last_seq`), and the pump silently ships nothing until
   seqs pass the stale cursor again.
2. **Boot re-offer.** After the view settles (and on every view-epoch change), a
   node pushes every record it holds for partitions it does *not* own to a
   current owner. This heals strands from SIGKILL (unshipped ring entries die
   with the process) and from ownership moves — which owners-only Merkle AE can
   never repair, because the owners agree with each other and the gauge reads 0.
3. **Backlog-aware drain.** SIGTERM waits (bounded) for all peer cursors to
   reach the ring head before exiting, instead of a fixed grace sleep — the
   last-moment ack window otherwise leaves with the process.
4. **Commit-context attribution.** See the caution above — echo attribution uses
   the applied batch's origin, not the merged record's envelope origin.

### Content-aware anti-entropy digests

The Merkle bucket digest and diff key on `(ikey, hlc, value_hash)`, not just
`(ikey, hlc)`. Merged CRDT records (PN counters, HLL registers) can carry the
**same** envelope version (version = symmetric max) with **different** payloads
on two replicas; a version-only digest calls them equal and AE never repairs the
divergence. The value hash makes equal-version / different-content records
repair in both directions — the backstop that guarantees convergence even when
the push path mis-fires (design/10 finding 6). The digest layout lives in
[Consistency](../consistency/#layer-2).

## Wire format

Replication ops are postcard-encoded after the frame header (see
[transport](#transport)):

```rust
struct ReplBatch {
    origin: NodeId,         // u16
    first_seq: u64,         // origin's ondaDB seq of first op (cursor resume)
    ops: Vec<ReplOp>,       // batched; see batch policy below
}
struct ReplOp {
    ikey: Bytes,            // full internal key (pid + tag + userkey [+ elem])
    env_and_payload: Bytes, // 19-byte envelope + payload, verbatim ondaDB value
}
```

No separate delta encoding exists or is needed: per-element keys make every
hash/set/zset mutation a single-element op by construction; a string ships its
full value (a string *is* the delta); a collection `DEL` is one head-key
tombstone.

**Message registry (u8 opcodes):**

```text
01 ReplBatch   02 AckSeq        03 Fetch          04 FetchResp
05 FetchCollection  06 Check    07 CheckResp      08 InterestRenew
09 MerkleRoot  0A MerkleBuckets 0B BucketKeys     0C BootstrapReq
0D BootstrapChunk   0E BootstrapDone  0F HandoffAck  10 Publish
11 Ping/Pong   12 ResumeFrom
```

## Replication ring & backpressure

One bounded ring per process: **128 MiB or 262,144 ops, whichever comes first**.
Shard threads write (one producer segment per shard, sequenced by ondaDB seq);
per-peer sender tasks hold read cursors.

Today the ring batches **256 ops per `ReplBatch`**, pumping on notify or a 50 ms
tick. When a peer's cursor falls off the ring tail (overrun), the sender drops
the cursor, marks the shared partitions dirty, and stops streaming to that peer;
recovery is `ResumeFrom` replay if the seq is still in the ring, else Merkle
anti-entropy on the dirty pairs. **There are no unbounded disk hint queues** —
the ring plus a tight anti-entropy period is the whole lag story.

```planned
**Byte-cap and flow control are designed but not yet implemented.**

- The **256 KiB batch byte cap + 2 ms linger** are design targets; the code
  caps by op count (256) and time (50 ms tick) only.
- **Per-peer unacked-window / `AckSeq` flow control** (designed 4 MiB send
  window): `AckSeq` frames are received and ignored today — there is no
  send-window backpressure. Acks are meant to advance the sender's persisted
  cursor floor without ever gating client acks.
```

### Apply path (receiver)

For each op: route to the shard thread for `pid`, read the current envelope for
`ikey`, run the merge rule, and write only if the incoming version wins — one
ondaDB `Txn` per `ReplBatch` per shard. Applied ops re-enter the local commit
hook; the DAG rule above prevents echo.

## Interest subscriptions

A node that reads a remote key **caches it and subscribes** to its updates, so
hot data spreads to where it is used. Home nodes hold the subscription state in
memory:

```rust
interest:      HashMap<Pid, HashMap<Bytes /*userkey*/, SmallVec<[(NodeId, Instant); 4]>>>
part_interest: HashMap<Pid, HashMap<NodeId, Instant>>   // escalated whole-partition subs
```

Exact keys are tracked, not bloom filters: false positives would fan writes to
uninterested nodes, and blooms can't expire entries.

**Lifecycle (implemented):**

| Event | Behavior |
|---|---|
| Create | First remote `GET`: `FetchResp` carries the value **and** a lease (`interest_lease = 60 s`). Fetch implies subscribe — one RTT. Collections subscribe at collection granularity (head + all elements, one lease). |
| Lease-expired read | The cached value stays on disk; an in-memory lease table gates freshness. A read of an expired-lease key sends `Check{ikey, hlc}` to H1 → `Fresh` (re-arm) or `Newer{env, payload}` (merge, re-arm). Cheaper than a refetch for large values. |
| Subscriber restart | The lease table is memory-only, so every non-home local key is lease-expired → lazy revalidation on first read. No resubscription storm. |

```planned
**Renew, escalation, and the entry cap are designed but not wired up.**

- **Interest renew interval** (design 15 s): the `InterestRenew` message exists
  and is handled, but it is never sent — leases refresh by re-fetch on expiry
  instead.
- **Whole-partition escalation** (`interest_escalate`, design 4096 key-leases
  per partition): converting a heavy subscriber into a partition-level shadow
  replica is unimplemented.
- **`interest_max_entries`** (design 1,000,000, LRU-evicted): there is no cap or
  LRU on the interest map today; expired entries are GC'd each AE round.
```

## Read path

`GET k` at node `X`, with `p = pid(k)`:

1. **`X ∈ owners(p)`** → serve locally (envelope decode, TTL check). May be
   behind an in-flight push — that is AP, bounded by anti-entropy.
2. **`X` caches `k`, lease valid** → serve locally.
3. **`X` caches `k`, lease expired/invalid** → `Check` to H1(p), merge if newer,
   serve, re-arm.
4. **`X` lacks `k`** → `FetchCollection` to H1(p), falling back to each
   remaining home in rank order with a ~300 ms fetch timeout per owner
   (`FETCH_TIMEOUT`). The response is committed locally **via merge** (a
   concurrent local write can't be regressed), the lease is registered, and the
   value served. The collection fetch streams all element keys of that user key.

   **Soft failure:** if every owner is unreachable, the command does not error —
   it serves the local (possibly empty) view and lets anti-entropy reconcile.
   During such a partition a non-owner can transiently report an existing key
   as absent (e.g. `EXISTS` → 0), bounded by the
   [staleness window](../consistency/).

**Freshness honesty:** per-connection read-your-writes and monotonic reads hold
(local-commit-first plus HLC max-wins merges). Nothing is promised across
connections.

## Pub/Sub

<a name="pubsub"></a>
At cluster sizes of 3–50 nodes, pub/sub is a **filtered full-mesh fan-out** —
no broadcast tree (a tree buys nothing below hundreds of nodes, and ctl
connections already exist).

- Each node gossips its subscription summary via a chitchat KV: the exact
  channel list while ≤ 1024 channels (postcard, versioned), else a 64 KiB
  blocked bloom filter. A separate `has_patterns` flag marks nodes with
  `PSUBSCRIBE` clients — they receive every publish (pattern matching is local,
  glob semantics as Redis).
- `PUBLISH ch msg` at `X`: deliver to local subscribers, then send
  `Publish{channel, payload}` on the ctl connection only to peers whose summary
  matches or that set `has_patterns`. At-most-once, fire-and-forget — exactly
  Redis pub/sub semantics.
- Local delivery uses a sharded `ChannelStore` (16 shards, RCU-swapped
  subscriber maps) over tokio broadcast senders.
- Keyspace notifications ride the same mesh, generated at the origin node only.

## Transport

<a name="transport"></a>
The mesh is **TCP + tokio**, with `TCP_NODELAY` and 4 MiB socket buffers. There
are **two connections per peer pair**, both dialed by the lower `NodeId`:

| Lane | Carries |
|---|---|
| `ctl` | Latency-sensitive traffic: `ReplBatch`, `AckSeq`, `Fetch`/`Check`, interest, `Publish`, heartbeats. |
| `bulk` | Bootstrap chunks and Merkle exchanges (lz4-compressed frames). |

Splitting `bulk` off removes head-of-line blocking without multiplexing
machinery. Framing is `[len: u32 LE][msg_type: u8][flags: u8][body…]`, max
frame 8 MiB. Bodies are postcard (serde) — compact varints, evolvable via
`#[serde(default)]` — except `ReplOp.env_and_payload` and fetch payloads, which
are raw bytes copied verbatim from/to ondaDB values (zero re-encode on the hot
path).

```note
**QUIC was rejected.** quinn mandates TLS (against the no-encryption
constraint), burns CPU in a userspace stack, and its head-of-line-blocking win
is already captured by the two-connection ctl/bulk split.
```

## Where to go next

- The math behind convergence: [Consistency & anti-entropy](../consistency/).
- How nodes join, leave, and are declared dead: [Cluster membership](../membership/).
- Every tunable on this page: [defaults table](../consistency/#defaults-table).

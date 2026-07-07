# 15 — Redis Cluster Protocol (client-side routing)

Status: **accepted** · Implements: CRC16 slot mapping + read-only `CLUSTER`
command family · Deliberately excluded: `MOVED`/`ASK` redirects (phase 2),
`CROSSSLOT` errors (never — see §Forgiving mode).

## Why

Today a client connects to any node (in k8s: whatever pod the Service picks);
when that node does not own the key, the engine serves it anyway via the
read-through / mesh forwarding path. With N nodes, roughly `(N-1)/N` of all
operations pay a second intra-cluster hop. For blob-cache workloads
(sccache: compiled objects, hundreds of KB to MB per value) that doubles
intra-cluster bandwidth and adds an RTT per operation.

Redis Cluster solves this on the **client** side: cluster-aware clients
(redis-rs `ClusterClient`, lettuce, jedis, go-redis) compute
`slot = CRC16(key) % 16384` locally, fetch a slot→node map from any node via
`CLUSTER SLOTS` / `CLUSTER SHARDS`, and send each command directly to the
owner. To benefit, marekvs only has to (a) place keys where CRC16 says they
are, and (b) answer the topology queries. Nothing about the internal
architecture (HRW, gossip, anti-entropy) changes.

Precedent: Dragonfly's "emulated cluster" mode does exactly this — reports
cluster topology for client routing while every node can serve every key.

## Slot mapping

Redis Cluster clients hash keys themselves; the server cannot pick the hash.
So the partition function changes from xxh3 to the Redis slot function:

```text
slot: u16 = crc16_xmodem(hash_slice(key)) % 16384      // Redis-identical
pid:  u16 = slot >> 2                                   // 4 slots per pid
```

- `hash_slice` (the `{...}` hash-tag rule) is already Redis-identical and is
  unchanged.
- `PARTITIONS` stays 4096; each pid is exactly the contiguous slot range
  `[pid*4, pid*4 + 3]`. All pid-keyed machinery (internal key prefix, Merkle
  anti-entropy, shard routing, HRW placement, join/leave handoff) is
  untouched — only the key→pid map changes.
- CRC16 is the XMODEM variant (poly `0x1021`, init `0`), table-driven,
  Redis-identical (`crc16("123456789") == 0x31C3`).

### Compatibility — this is a data-format break

`pid` is the first two bytes of every internal storage key (design/02). Keys
written under the xxh3 mapping are invisible under the CRC16 mapping. There
is no dual-hash mode:

- **New clusters**: nothing to do.
- **Existing clusters**: migrate through the existing upstream-replication
  path (`MAREKVS_REPLICAOF` / `REPLICAOF host port`, design/03): point a
  fresh CRC16 cluster at the old cluster as an upstream, let it sync, cut
  clients over. This is the same live-migration path already used for
  Redis→marekvs moves.

Rejected alternative: a per-cluster hash-mode flag. `pid_of` is a pure
function called on every hot-path operation from `marekvs-core` (no config
access); threading a mode through every caller buys nothing for a pre-1.0
store with a working migration path.

## Topology surface — read-only `CLUSTER` family

New dispatch family (all read-only, no topology mutation — placement remains
gossip+HRW, design/06):

| Subcommand | Reply |
|---|---|
| `CLUSTER KEYSLOT key` | integer slot, Redis-identical (great for tests) |
| `CLUSTER MYID` | 40-hex node id |
| `CLUSTER INFO` | bulk string: `cluster_enabled:1`, `cluster_state:ok`, `cluster_slots_assigned:16384`, `cluster_known_nodes`, `cluster_size`, `cluster_current_epoch` (view epoch), `cluster_my_epoch` (boot generation) |
| `CLUSTER SLOTS` | array of `[start, end, [master ip, port, id], [replica…]…]`, adjacent pids with identical owner lists merged into one range |
| `CLUSTER SHARDS` | Redis-7 map-shaped view of the same ranges |
| `CLUSTER NODES` | bulk string, one Redis-format line per member with its H1 slot ranges |

Mapping marekvs concepts onto the Redis wire model:

- **Master of a slot range** = `H1(pid)` (top-ranked Active HRW owner).
  **Replicas** = the remaining HRW owners. Every node is "master" for the
  ranges it homes — there are no whole-node replicas in marekvs, and clients
  don't care: they route writes to the range's master, which is exactly
  where we want the first hop to land.
- **Node id**: 40-hex, deterministic from the ordinal
  (`format!("{:040x}", node_id)`) — stable across restarts, which is what
  clients expect of a node identity. The per-boot incarnation lives in
  `cluster_my_epoch`/config-epoch (chitchat generation), not the id.
- **Client address**: nodes now gossip a third KV, `resp_addr` (advertise IP
  + RESP port), alongside `mesh_addr` and `state`. Members that don't gossip
  it (older binaries during a rolling upgrade) are simply omitted from
  `CLUSTER SLOTS` — clients treat those slots as uncovered and fall back to
  any connected node, where read-through serves as today. Fail-safe, not
  fail-closed.
- `INFO` already reports `cluster_enabled:1` in `# Cluster` (installed hook).

The engine gets the data through a new `Topology` provider hook
(`Engine::set_cluster_topology`), same trait-object indirection as the
existing `cluster_info`/`replicaof` hooks — `marekvs-engine` stays free of a
`marekvs-cluster` dependency. The server installs a closure that snapshots
`Cluster::view()` into plain `Topology`/`TopologyNode` structs.

## Forgiving mode (deliberate deviations from real Redis Cluster)

marekvs can serve any key from any node; Redis Cluster cannot. We keep the
superset behavior:

- **No `CROSSSLOT` errors, ever.** Multi-key commands spanning slots keep
  working (read-through). Cluster clients never send them cross-slot anyway;
  non-cluster clients keep full functionality.
- **No `MOVED` in v1.** A client with a stale slot map degrades to today's
  behavior (extra hop), never to an error. The cost: after a topology change,
  a cluster client keeps paying the forwarding hop until its own refresh
  (redis-rs refreshes on `MOVED`, on connection errors, and periodically).
- **Phase 2 — `MOVED` for self-identified cluster clients.** Mark a session
  cluster-aware when it issues `CLUSTER SLOTS`/`SHARDS`/`NODES`; for such
  sessions reply `MOVED <slot> <ip:port>` when the first key's pid is not
  locally owned, which triggers the client's map refresh. Non-cluster
  sessions never see redirects. Not in this change because (a) it needs
  per-command first-key extraction, and (b) the parallel batching path
  (`dispatch_data`) is session-less — redirect gating there needs its own
  design pass. `cluster_state`, slot coverage and correctness do not depend
  on it.

## Testing

- `marekvs-core`: CRC16 vector (`0x31C3`), Redis-known slots
  (`foo`→12182, `bar`→5061, `hello`→866), hash-tag colocation unchanged,
  `pid == slot >> 2`.
- `marekvs-engine`: pure formatter tests for range merging, `CLUSTER NODES`
  lines and `CLUSTER INFO` against a hand-built `Topology` (no store, no
  network).
- `tests/cluster_protocol.sh`: 3-node local cluster; asserts `CLUSTER
  KEYSLOT foo` == 12182, full 16384-slot coverage of the reported topology,
  MYID shape, and — the load-bearing check — that a key written anywhere is
  readable by querying the reported slot master DIRECTLY (no `-c`; since
  marekvs never sends `MOVED`, `redis-cli -c` would silently degenerate to
  read-through and prove nothing about the map).

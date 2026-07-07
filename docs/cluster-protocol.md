---
title: Cluster protocol
description: CRC16 slot mapping and the read-only CLUSTER command family — client-side routing without adopting Redis Cluster's failure model.
status: implemented
---

marekvs speaks enough of the Redis Cluster wire protocol for cluster-aware
clients (redis-rs `ClusterClient`, lettuce, jedis, go-redis) to route commands
straight to the node that owns a key, cutting out the extra intra-cluster hop
you'd otherwise pay on every command sent to a node that doesn't hold the key.

```note
This is client-side routing only. marekvs does **not** adopt Redis Cluster's
failure model: there are no `MOVED`/`ASK` redirects and no `CROSSSLOT`
errors. Every node can still serve every key — cluster-aware clients just get
to skip the hop when their slot map is fresh.
```

## Why bother

With N nodes, a client connecting to whatever node the Kubernetes Service
happens to pick will miss the right node roughly `(N-1)/N` of the time; today
that's fine because the engine forwards the request through the mesh
(read-through), but it costs an extra round trip and doubles intra-cluster
bandwidth for large values. Redis Cluster solves this on the client: the
client hashes the key itself, fetches a slot→node map once, and sends future
commands directly to the owner. marekvs only has to place keys where that
hash says they are and answer the topology queries — everything about the
underlying architecture (HRW placement, gossip, anti-entropy) is unchanged.

## Slot mapping

```text
slot: u16 = crc16_xmodem(key) % 16384   // Redis-identical
pid:  u16 = slot >> 2                   // 4 slots per pid
```

The slot function is CRC16/XMODEM (poly `0x1021`, init `0`) over the same
hash-tag-aware key slice Redis Cluster clients use
(`crc16("123456789") == 0x31C3`) — see [Data model](../data-model/#partitioning)
for how `pid` continues to drive internal key layout, anti-entropy, and
placement.

## Compatibility

`pid` is encoded in the first two bytes of every internal storage key, so this
is a data-format change, not an additive one — keys written under the old
`xxh3`-based mapping aren't reachable under CRC16 mapping. There's no dual-hash
mode:

- **New clusters:** nothing to do.
- **Existing clusters:** migrate through the ordinary upstream-replication
  path (`REPLICAOF` / `MAREKVS_REPLICAOF`, [Redis API](../redis-api/)): stand
  up a fresh CRC16 cluster, point it at the old one as an upstream, let it
  sync, then cut clients over. It's the same live-migration path already used
  for moving off real Redis.

## The `CLUSTER` command family

All read-only — topology mutation (`SETSLOT`, `FORGET`, `MEET`) isn't
supported; placement is managed by gossip and HRW, not by clients.

| Command | Reply |
|---|---|
| `CLUSTER KEYSLOT key` | Integer slot, Redis-identical — handy for testing your client's hashing. |
| `CLUSTER MYID` | 40-hex node id, stable across restarts. |
| `CLUSTER INFO` | `cluster_enabled:1`, `cluster_state:ok`, `cluster_slots_assigned:16384`, plus known-node count, cluster size, and epoch fields. |
| `CLUSTER SLOTS` | Array of `[start, end, [master ip, port, id], [replica…]…]`; adjacent slot ranges with the same owner set are merged. |
| `CLUSTER SHARDS` | The same ranges in the Redis 7 map-shaped view. |
| `CLUSTER NODES` | Bulk string, one Redis-format line per member with its slot ranges. |

A few mapping notes if you're comparing against real Redis Cluster:

- **"Master" of a slot range** is the top-ranked HRW owner for that range's
  `pid`; the remaining HRW owners show up as "replicas". Every node is a
  master for the ranges it owns — marekvs has no whole-node replicas, and
  clients don't need to know that; they just route writes to the reported
  master, which is exactly where you want the first hop to land.
- **Client address** comes from a `resp_addr` gossip value nodes advertise
  alongside their mesh address. A node mid-rolling-upgrade that hasn't started
  gossiping it yet is simply left out of `CLUSTER SLOTS` — clients treat that
  range as uncovered and fall back to whatever node they're already
  connected to, which still serves the key via read-through. Fail-safe, not
  fail-closed.

## What's deliberately different from real Redis Cluster

```warning
marekvs can serve any key from any node; real Redis Cluster cannot. We keep
that superset behavior rather than emulate Redis Cluster's stricter one.
```

- **No `CROSSSLOT` errors, ever.** Multi-key commands spanning slots keep
  working via read-through. Cluster-aware clients don't send cross-slot
  commands anyway; non-cluster clients keep full functionality.
- **No `MOVED` redirects yet.** A client with a stale slot map just pays the
  extra forwarding hop — never an error — until its own refresh logic kicks
  in (most cluster clients refresh on connection errors and periodically).
  Redirect-driven refresh for self-identified cluster clients is planned but
  not implemented.

## Where to go next

- The rest of the command surface: [Redis API reference](../redis-api/).
- How `pid` drives internal key layout: [Data model](../data-model/#partitioning).
- Full protocol writeup, rejected alternatives, and test plan:
  [design/15-cluster-protocol.md](https://github.com/yannick/marekvs/blob/main/design/15-cluster-protocol.md).

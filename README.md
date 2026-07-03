# marekvs

A distributed key-value database server with a Redis-compatible API, written
in Rust. AP by design (eventually consistent, coordination-free), disk-native
via the [ondaDB](../ondadb) LSM engine — no in-memory dataset.

- **Redis protocol**: RESP2 + RESP3, strings / hashes / sets / sorted sets /
  lists / streams / pub-sub ([coverage matrix](design/03-redis-api.md))
- **Convergent replication**: hybrid logical clocks + per-element ORSWOT
  merges; concurrent `SADD`s on different nodes both survive, deletes never
  resurrect ([data model](design/02-data-model.md))
- **Dynamic replication**: any node serves any key; a node that reads a
  remote key caches it and subscribes to its updates
  ([replication](design/04-replication.md))
- **Bounded staleness**: sequence-cursor resume + Merkle anti-entropy repair
  divergence within seconds ([anti-entropy](design/05-consistency-anti-entropy.md))
- **Kubernetes-native**: gossip membership (chitchat) with DNS-seeded
  discovery; nodes come and go ([membership](design/06-cluster-membership.md),
  [k8s](design/07-kubernetes.md))
- **Lua scripting**: EVAL/EVALSHA with Redis-grade atomicity for scripts
  whose keys co-locate (hash tags `{...}`); script *effects* replicate, never
  the script ([scripting](design/11-lua-scripting.md))
- **OS-less images**: static binary in a `FROM scratch` container

Full design documentation lives in [design/](design/README.md).

## Quickstart

Everything runs through [just](https://github.com/casey/just):

```sh
just build          # debug build
just test           # unit + property tests (merge laws!)
just test-smoke     # end-to-end single node via redis-cli
just run            # single local node on :6379

just run-cluster    # local 3-node cluster on :6379/:6380/:6381

just docker-build   # FROM-scratch image (needs ../ondadb sibling checkout)
                    # plain cargo builds fall back to the ondadb git dep
                    # (github.com/yannick/ondadb)
just docker-test    # 3-node compose cluster + convergence tests
just apple-build    # same image via Apple's `container` CLI
just apple-test     # 3-node apple-container cluster + convergence tests

just ci             # fmt-check + clippy + tests

just k8s-apply      # example Kubernetes deployment (see k8s/README.md
just k8s-status     # for safe dynamic scale-up/down without data loss)

just bench          # benchmark vs KeyDB (both in docker) → bench/report.md
just bench-report   # re-render the report from accumulated results
```

Try it:

```sh
just run &
redis-cli set greeting hello
redis-cli get greeting
redis-cli sadd tags rust distributed redis
redis-cli smembers tags
```

## Configuration (environment)

| Variable | Default | Meaning |
|---|---|---|
| `MAREKVS_DATA_DIR` | `.data/n0` | ondaDB directory (PVC in k8s) |
| `MAREKVS_NODE_ID` | hostname ordinal | u16 node id (StatefulSet ordinal) |
| `MAREKVS_RESP_ADDR` | `0.0.0.0:6379` | client listener |
| `MAREKVS_MESH_ADDR` | `0.0.0.0:7373` | peer replication listener |
| `MAREKVS_GOSSIP_ADDR` | `0.0.0.0:7946` | chitchat UDP |
| `MAREKVS_ADVERTISE_IP` | `127.0.0.1` | IP/hostname peers should use |
| `MAREKVS_SEEDS` | — | comma-separated gossip seeds (`host:7946`) |
| `MAREKVS_REPLICAS_N` | `3` | home replicas per partition |
| `MAREKVS_REQUIREPASS` | — | optional AUTH password |

## Workspace layout

| Crate | Contents |
|---|---|
| `marekvs-core` | partitioning, HLC, envelopes, key layouts, merge rules (pure, property-tested) |
| `marekvs-resp` | RESP2/3 parser + reply builder |
| `marekvs-proto` | peer wire messages (postcard) |
| `marekvs-engine` | shard-threaded storage over ondaDB, command families, pub/sub |
| `marekvs-cluster` | chitchat gossip, HRW placement |
| `marekvs-repl` | replication ring, peer mesh, interest leases, Merkle anti-entropy, bootstrap |
| `marekvs-server` | binary: config + wiring |

## Consistency notes (read before production use)

marekvs is AP: reads on one connection see that connection's writes and never
go backward, but two clients on two nodes can briefly observe different
values. Lists are LWW whole-value in v1 (concurrent cross-node pushes race).
Since v1.1, INCR/DECR/INCRBY/DECRBY are PN counters: concurrent increments
across nodes are never lost (an explicit SET resets). Lua scripts are atomic
per node/shard when all KEYS share a partition — they are NOT a distributed
lock primitive; see [11-lua-scripting.md](design/11-lua-scripting.md). See
[00-overview.md](design/00-overview.md#published-guarantees-what-we-tell-users).

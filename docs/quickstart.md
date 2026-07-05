---
title: Quickstart
description: Run a 1-node or 3-node marekvs cluster from the published images — Docker and Apple containers.
status: implemented
---

Get a marekvs endpoint on `:6379` in one command, then scale it to a real
3-node cluster. Everything below uses the published images from the GitHub
Container Registry — no build required.

```note
The images are `ghcr.io/yannick/marekvs` (production, `FROM scratch`) and
`ghcr.io/yannick/marekvs:debug` (the fault-injection suite only). Swap in your
own owner/org if you run a fork.
```

Every node exposes the same four ports:

| Port | Purpose |
|---|---|
| `6379` | Redis client protocol (RESP2/RESP3) |
| `7373` | Peer replication mesh |
| `7946/udp` | chitchat gossip |
| `9121` | Health probes + Prometheus metrics |

A node is configured entirely through environment variables. The handful that
matter for bootstrapping a cluster:

| Variable | Purpose |
|---|---|
| `MAREKVS_NODE_ID` | Stable `u16` node id (use the pod/container ordinal). |
| `MAREKVS_REPLICAS_N` | Home replicas per partition (`1` for a single node). |
| `MAREKVS_ADVERTISE_IP` | IP peers should dial; `auto` self-detects. |
| `MAREKVS_SEEDS` | Comma-separated gossip seeds, `host:7946`. |
| `MAREKVS_DATA_DIR` | ondaDB data directory (mount a volume for durability). |

The [full environment reference](../build-deploy/#configuration) lists them all.

## Single node — Docker

```sh
docker run -d --name marekvs \
  -p 6379:6379 -p 9121:9121 \
  -e MAREKVS_NODE_ID=0 \
  -e MAREKVS_REPLICAS_N=1 \
  -e MAREKVS_DATA_DIR=/data \
  -v marekvs-data:/data \
  ghcr.io/yannick/marekvs:latest
```

Then talk to it like any Redis server:

```sh
redis-cli -p 6379 ping                        # PONG
redis-cli -p 6379 set greeting hello          # OK
redis-cli -p 6379 get greeting                # "hello"
redis-cli -p 6379 sadd tags rust distributed  # (integer) 2
curl -s localhost:9121/ready                  # readiness probe
```

`MAREKVS_REPLICAS_N=1` tells the node not to expect peers, so it is home for
every partition on its own.

## Three nodes — Docker Compose

Save this as `compose.yaml`. The three nodes share a fixed subnet so each can
advertise a stable IP and seed off the others; `MAREKVS_REPLICAS_N=2` keeps two
home copies of every partition.

```yaml
name: marekvs
x-common: &common
  image: ghcr.io/yannick/marekvs:latest
  networks: [mkv]
  environment: &env
    MAREKVS_REPLICAS_N: "2"
    MAREKVS_DATA_DIR: /data
    MAREKVS_SEEDS: 172.28.5.10:7946,172.28.5.11:7946,172.28.5.12:7946

services:
  marekvs-0:
    <<: *common
    environment: { <<: *env, MAREKVS_NODE_ID: "0", MAREKVS_ADVERTISE_IP: 172.28.5.10 }
    networks: { mkv: { ipv4_address: 172.28.5.10 } }
    ports: ["16379:6379"]
  marekvs-1:
    <<: *common
    environment: { <<: *env, MAREKVS_NODE_ID: "1", MAREKVS_ADVERTISE_IP: 172.28.5.11 }
    networks: { mkv: { ipv4_address: 172.28.5.11 } }
    ports: ["16380:6379"]
  marekvs-2:
    <<: *common
    environment: { <<: *env, MAREKVS_NODE_ID: "2", MAREKVS_ADVERTISE_IP: 172.28.5.12 }
    networks: { mkv: { ipv4_address: 172.28.5.12 } }
    ports: ["16381:6379"]

networks:
  mkv:
    ipam: { config: [{ subnet: 172.28.5.0/24 }] }
```

```sh
docker compose up -d

# write on one node, read it back on another — replication is automatic
redis-cli -p 16379 set city zurich
redis-cli -p 16381 get city          # "zurich"
```

```tip
Working from a checkout of the repo? `just docker-up` builds the image locally
and starts this exact cluster (it uses `deploy/compose.yaml`), and `just
docker-test` runs convergence checks against it.
```

## Single node — Apple containers

Apple's `container` CLI gives each container its own IP rather than publishing
host ports, so start the node, read its IP, and connect to it directly.

```sh
container system start

container run -d --name marekvs \
  -e MAREKVS_NODE_ID=0 \
  -e MAREKVS_REPLICAS_N=1 \
  -e MAREKVS_ADVERTISE_IP=auto \
  -e MAREKVS_DATA_DIR=/data \
  ghcr.io/yannick/marekvs:latest

IP=$(container inspect marekvs \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["status"]["networks"][0]["ipv4Address"].split("/")[0])')

redis-cli -h "$IP" -p 6379 ping      # PONG
redis-cli -h "$IP" -p 6379 set greeting hello
```

## Three nodes — Apple containers

`MAREKVS_ADVERTISE_IP=auto` lets every node self-detect its address, so only the
seed needs to be known ahead of time: start node 0, then seed nodes 1 and 2 off
its IP.

```sh
container system start

# helper: print a container's IP
ip() { container inspect "$1" | python3 -c \
  'import json,sys; print(json.load(sys.stdin)[0]["status"]["networks"][0]["ipv4Address"].split("/")[0])'; }

run() { # <ordinal> <seeds>
  container run -d --name "mkv-$1" \
    -e MAREKVS_NODE_ID="$1" \
    -e MAREKVS_REPLICAS_N=2 \
    -e MAREKVS_ADVERTISE_IP=auto \
    -e MAREKVS_SEEDS="$2" \
    -e MAREKVS_DATA_DIR=/data \
    ghcr.io/yannick/marekvs:latest
}

run 0 ""; sleep 2
IP0=$(ip mkv-0)
run 1 "$IP0:7946"
run 2 "$IP0:7946"

redis-cli -h "$IP0" set city zurich
redis-cli -h "$(ip mkv-2)" get city   # "zurich"
```

```tip
From a repo checkout, `just apple-up` runs exactly this flow (via
`tests/apple_cluster.sh`) and `just apple-test` checks convergence.
```

## From source

If you have the repo (and an `../ondadb` sibling checkout), everything runs
through [`just`](https://github.com/casey/just):

```sh
just run            # single local node on :6379
just run-cluster    # local 3-node cluster on :6379 / :6380 / :6381
just test-smoke     # end-to-end redis-cli checks against one node
```

## Next steps

- Understand the guarantees you just relied on: [Consistency](../consistency/).
- Deploy it for real: [Kubernetes](../kubernetes/) and the [operator](../operator/).
- See the whole command surface: [Redis API reference](../redis-api/).

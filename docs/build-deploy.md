---
title: Build & deploy
description: How the marekvs image is built — static musl binary, FROM scratch, multi-arch CI — plus the complete MAREKVS_* configuration reference.
status: implemented
---

marekvs builds to a single static binary and ships in a `FROM scratch`
container: no OS, no shell, no libc in the image — just the binary. This page
covers the build (workspace, static link, release profile, Dockerfile), the CI
image pipeline, and the full environment-variable reference.

marekvs is version **0.2.0**. It is proprietary software — all rights reserved.

## Cargo workspace

The [root `Cargo.toml`](https://github.com/yannick/marekvs/blob/main/Cargo.toml)
is a workspace of eight crates — the pure core (`marekvs-core`), the RESP and
peer-wire codecs, the engine, cluster and replication crates, and the two
binaries `marekvs-server` and `marekvs-operator`. ondaDB is a `git` dependency,
overridden by a `[patch]` to a sibling `../ondadb` checkout when you have one
(the `just` recipes generate it).

The release profile is tuned for a small, fast binary:

```toml
[profile.release]
lto = "thin"
codegen-units = 1
panic = "abort"
strip = "symbols"

[profile.release.package."*"]
opt-level = 3
```

`panic = "abort"` drops the unwinding tables, `strip = "symbols"` removes the
symbol table, and thin LTO with `codegen-units = 1` trims dead code across the
whole graph — all of which shrink the shipped binary.

## Static binary

The image builds on `rust:1-alpine`, whose host target is musl, so
`cargo build --release` produces a **fully static** binary with no dynamic libc
— exactly what a `FROM scratch` runtime needs. The dependency tree is pure Rust
or vendored-static, so the static link is straightforward on both `amd64` and
`arm64`.

```note title="Allocator"
Design note [08-build-deploy.md](https://github.com/yannick/marekvs/blob/main/design/08-build-deploy.md)
calls for linking **mimalloc** as the `#[global_allocator]` to avoid musl's slow
multithreaded malloc. That swap is not wired into the workspace yet — the current
binary uses the default system (musl) allocator. Treat mimalloc as an intended
optimization, benchmarked in the performance notes, not as a shipped fact.
```

## Dockerfile — multi-stage, FROM scratch

The [`Dockerfile`](https://github.com/yannick/marekvs/blob/main/Dockerfile)
compiles once and copies both binaries into a scratch image:

```dockerfile
# ---- build stage ----------------------------------------------------------
FROM rust:1-alpine AS build
RUN apk add --no-cache musl-dev build-base
WORKDIR /src
COPY ondadb/ ondadb/
COPY marekvs/ marekvs/
WORKDIR /src/marekvs
RUN cargo build --release -p marekvs-server -p marekvs-operator \
 && cp target/release/marekvs-server /marekvs \
 && cp target/release/marekvs-operator /marekvs-operator

# ---- runtime stage --------------------------------------------------------
FROM scratch
COPY --from=build /marekvs /marekvs
COPY --from=build /marekvs-operator /marekvs-operator
EXPOSE 6379 7373 7946/udp 9121
ENTRYPOINT ["/marekvs"]
```

Notes on the shape:

- The **same image carries both binaries.** The default entrypoint is
  `/marekvs` (the server); operator Deployments set
  `command: ["/marekvs-operator"]`.
- **`FROM scratch`, not distroless.** distroless still ships a filesystem tree
  (ca-certs, tzdata, `/etc`); marekvs needs neither TLS roots (no encryption)
  nor tzdata (all times are UTC ms). Expected image size after strip is
  **8–15 MiB** — essentially the binary and nothing else.
- **No `USER` is baked in.** Docker named volumes mount root-owned and scratch
  has no way to `chown`; Kubernetes sets `runAsUser` / `fsGroup` via
  `securityContext` instead (see [Kubernetes](../kubernetes/)).
- **Debugging** a scratch container uses ephemeral containers
  (`kubectl debug --image=busybox --target=marekvs`) — never bake tools into the
  production image.

The build context must be the **parent** directory (so the `../ondadb` path
dependency resolves); `just docker-build` stages a clean context for you.

## CI image pipeline

The [`docker` workflow](https://github.com/yannick/marekvs/blob/main/.github/workflows/docker.yml)
publishes to `ghcr.io/yannick/marekvs` on every push to `main` and on `v*` tags.

The multi-arch build deliberately **avoids QEMU** — an emulated musl Rust build
is far too slow. Instead each platform compiles on a **native runner**
(`ubuntu-24.04` for `amd64`, `ubuntu-24.04-arm` for `arm64`), pushes its image
**by digest**, and a `merge` job stitches the per-arch digests into manifest
lists. Each build job checks out both `marekvs` and `ondadb` side by side and
writes the same cargo `[patch]` the `Justfile` generates locally.

The production manifest list is tagged:

| Tag | Source | When |
|---|---|---|
| `latest` | raw | default branch (`main`) |
| `sha-<sha>` | `type=sha` | every build |
| `vX.Y.Z` | `type=semver` | on `v*` tags |
| `<branch>-<sha7>-<unix-ts>` | raw, Flux-sortable | default branch — the trailing timestamp makes tags numerically sortable for Flux image automation |

A **separate `:debug` image** is built from
[`Dockerfile.debug`](https://github.com/yannick/marekvs/blob/main/Dockerfile.debug)
in the same run: the *identical* build stage over a small `alpine:3.20` userland
with `iptables` (grudge partitions), `tc`/`iproute2` (netem packet faults) and
GNU `coreutils` (clock faults). It exists **only for the chaos harness**
(`CHAOS_DEBUG=1`, run with `NET_ADMIN`/`SYS_TIME`) and must never run in
production. Its tags mirror the production set with a `debug` prefix
(`debug`, `debug-sha-<sha>`, `debug-<version>`, `debug-<branch>-<sha7>-<ts>`).

```tip
For Flux image automation, filter on the sortable tag and extract the trailing
timestamp — see the `ImagePolicy` example in
[`k8s/README.md`](https://github.com/yannick/marekvs/blob/main/k8s/README.md).
```

## Configuration

A node is configured entirely through environment variables (12-factor,
Kubernetes-friendly). Every variable read by `marekvs-server`, with its real
default:

| Variable | Default | Meaning |
|---|---|---|
| `MAREKVS_DATA_DIR` | `.data/n0` | ondaDB data directory (mount a PVC/volume for durability). |
| `MAREKVS_NODE_ID` | hostname ordinal, else `0` | Stable `u16` node id. Unset in a StatefulSet — parsed from the pod hostname (`marekvs-3` → `3`). |
| `MAREKVS_RESP_ADDR` | `0.0.0.0:6379` | Redis client (RESP2/RESP3) listener. |
| `MAREKVS_MESH_ADDR` | `0.0.0.0:7373` | Peer replication-mesh listener. |
| `MAREKVS_GOSSIP_ADDR` | `0.0.0.0:7946` | chitchat gossip (UDP) listener. |
| `MAREKVS_METRICS_ADDR` | `0.0.0.0:9121` | Health-probe + Prometheus-metrics listener. |
| `MAREKVS_ADVERTISE_IP` | `127.0.0.1` | IP or hostname peers should dial; `auto` self-detects the primary-interface IP. |
| `MAREKVS_SEEDS` | *(empty)* | Comma-separated gossip seeds, `host:7946`. One DNS name suffices — chitchat re-resolves it. Empty + `REPLICAS_N=1` = standalone. |
| `MAREKVS_REPLICAS_N` | `3` | Home replicas per partition. `1` for a single node. |
| `MAREKVS_REQUIREPASS` | *(empty — auth off)* | `AUTH` password. Live-settable via `CONFIG SET requirepass`. |
| `MAREKVS_SHARDS` | auto (`available_parallelism`) | Shard-thread count; defaults to the number of CPU cores. |
| `MAREKVS_SCRIPT_TIME_LIMIT_MS` | `20` | `EVAL`/`EVALSHA` wall-clock budget in ms. Live-settable via `CONFIG SET lua-time-limit`. |
| `MAREKVS_REPLICAOF` | *(unset)* | `host:port` of an upstream Redis to replicate from at boot (the `REPLICAOF` live-migration path). |

```note
A few settings are **live-reconfigurable** at runtime via `CONFIG SET`, without
a restart: `requirepass`, `lua-time-limit` (alias `busy-reply-threshold`,
backing `MAREKVS_SCRIPT_TIME_LIMIT_MS`), and `loglevel` (which reloads the
`RUST_LOG` filter). The environment values are re-applied on the next restart.
```

```note title="Licensing"
marekvs is **proprietary — Copyright © 2026 Yannick Koechlin, all rights
reserved**. It is not open-source licensed: `Cargo.toml` carries no `license`
field and there is no `LICENSE` file at the repo root.
```

## Where to go next

- Get a node running from these images in one command: [Quickstart](../quickstart/).
- Deploy the image on Kubernetes: [Kubernetes](../kubernetes/) and the
  [operator](../operator/).

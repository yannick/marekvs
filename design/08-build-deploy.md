# 08 — Build & Container Images

Requirement: minimal Docker images containing **no OS** — just the binary.

## Cargo workspace

Layout in [01-architecture.md](01-architecture.md#crate-layout-cargo-workspace).
ondaDB is a path dependency:

```toml
# crates/marekvs-engine/Cargo.toml
[dependencies]
ondadb = { path = "../../../ondadb" }                     # dev checkout layout
# release builds may enable: features = ["unsafe-fastpath"]  (see 09)
```

For CI, the ondadb checkout is vendored as a git submodule (or a git
dependency once it has a canonical remote) so the workspace builds outside the
`storage-engines` directory tree.

Release profile:

```toml
[profile.release]
lto = "thin"
codegen-units = 1
panic = "abort"
strip = "symbols"
```

## Static binary

Target `x86_64-unknown-linux-musl` (and `aarch64-unknown-linux-musl` for
multi-arch). Everything in the dependency tree is pure Rust (ondaDB is
`#![forbid(unsafe_code)]` by default; lz4/zstd compression crates have pure-Rust
or vendored-static builds), so a fully static musl link is straightforward.

Note: musl's default allocator is slow under multithreaded load — we link
**mimalloc** (`#[global_allocator]`, pure-Rust build mode) to avoid the musl
malloc penalty. Alternative: jemalloc via `tikv-jemallocator` (needs cc, still
static). This is a known hot decision; benchmarked in
[09-performance.md](09-performance.md).

## Dockerfile (multi-stage, FROM scratch)

```dockerfile
# ---- build stage ----------------------------------------------------------
FROM rust:1.86-alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /src
# cache dependency graph
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY vendor/ondadb/ vendor/ondadb/
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --target x86_64-unknown-linux-musl -p marekvs-server \
 && cp target/x86_64-unknown-linux-musl/release/marekvs-server /marekvs

# ---- runtime stage --------------------------------------------------------
FROM scratch
COPY --from=build /marekvs /marekvs
# no shell, no libc, no OS — the binary is the image
USER 65534:65534
EXPOSE 6379 7373 7946/udp 9121
ENTRYPOINT ["/marekvs"]
```

- **`FROM scratch`**, not distroless: distroless still ships a filesystem tree
  (ca-certs, tzdata, /etc). We need neither TLS roots (no encryption) nor
  tzdata (all times UTC ms). Target image size: **binary + ~0**, expected
  8–15 MiB after strip.
- Health probes are HTTP against the process itself (:9121) — no shell needed
  in the image.
- Debugging a scratch container: `kubectl debug --image=busybox
  --target=marekvs` (ephemeral containers), never bake tools into the image.
- Multi-arch via `docker buildx` matrix (amd64 + arm64 musl targets), manifest
  list pushed once.

## CI pipeline (outline)

1. `cargo fmt --check`, `clippy -D warnings`, `cargo test --workspace`
   (includes merge-law property tests, [10-testing.md](10-testing.md)).
2. Release build for both musl targets; `cargo deny` for licenses/advisories.
3. Integration tests: 3-node docker-compose cluster, redis-cli smoke suite +
   convergence checks.
4. buildx bake → push image (tag = git sha + semver on tags).
5. (nightly) bench job — [09-performance.md](09-performance.md) targets, fail
   on >10 % regression.

## Configuration

Everything via environment (12-factor, k8s-friendly), one optional TOML file
for the long tail. Precedence: env > file > defaults
([05 defaults table](05-consistency-anti-entropy.md#defaults-table)).

```
MAREKVS_DATA_DIR=/data
MAREKVS_SEED_DNS=marekvs-headless.ns.svc
MAREKVS_REPLICAS_N=3
MAREKVS_AE_ROUND_MS=5000
MAREKVS_SYNC_MODE=interval           # none|interval|full
MAREKVS_SHARD_THREADS=auto
MAREKVS_MAXMEMORY_CACHE=auto         # ondaDB block cache sizing
```

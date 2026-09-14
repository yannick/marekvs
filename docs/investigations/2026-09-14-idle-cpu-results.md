# Idle maintenance fix: release validation

The final process fixture seeds 13,765 persistent keys and 18,117 range deletions
across 466 partitions in live memtables. Each run warms for 60 seconds and then
measures process CPU for 60 seconds, excluding setup and shutdown. CPU is percent
of one core. Measurements used the same macOS host and release toolchain;
background Docker/build activity means these are diagnostic measurements, not
controlled capacity claims.

| Shards | Baseline CPU | Fixed CPU | Fixed idle iterator growth |
| --- | ---: | ---: | ---: |
| 1 | 29.198% | 0.382% | 0 |
| 2 | 70.442% | 0.341% | 0 |
| 10 | 337.494% | 0.370% | 0 |

Baseline: marekvs `bd405d5`, ondaDB `bcd2de8`. Fixed behavior: marekvs
`c821053`, with annotation-only lint follow-up `e943840`, and ondaDB
`8afa06618ba4320df33b5a8c004adf2b85c003e8`. The final release dependency is
`0f4ebc67434551c9d3d4cba899829234760f4215` (ondaDB PR #3), which adds
only a Linux portability lint annotation to the reviewed cache implementation.
It is pinned in Cargo.toml and the canonical Git-source Cargo.lock. Exact Linux
Rust/Clippy 1.97.1 passes both default and unsafe-fastpath all-target lint gates.

## Regression gates

- Final marekvs `just ci` against the published dependency: 605 passed, zero
  failed, three existing ignored tests; formatting, Clippy and grudge self-test
  passed. New scheduler, cleanup, nonce correlation and INFO tests run in CI.
- ondaDB default suite: 1,052 passed, zero failed, 32 ignored. Unsafe-fastpath:
  1,047 passed, zero failed, 32 ignored. Both Clippy gates passed. A pre-existing
  one-second flush timing check needed a focused retry and a full rerun with
  four test threads under concurrent benchmark load; its timeout was unchanged.
- Documentation generator: 19 pages and landing page built successfully.
- Independent implementation reviews approved range snapshot reuse, expiry
  invalidation/scheduling, cold cleanup serialization/proofs and INFO reporting.

ondaDB's 80-case cold/warm benchmark and 20 structural cases passed. For the
empty ordinary-memtable 18,117-range fixture, warm iterator construction went
from about 117 ms to 0.604 microseconds; the first cold build still cost about
56 ms. These timings also had concurrent host load. Cache tests, rather than
wall-clock thresholds, enforce one shared build per unchanged range generation,
independent iterator positions, MVCC preservation and retained-memory accounting.

## Docker acceptance

The first final Docker matrix passed all fixed idle structural assertions:
zero expiry iterator and range-delete growth, no connected clients, and stable
ownership across the 60-second sample. The reproduction harness uses
three RF=2 nodes, unique persistent test volumes, separate client/peer networks,
180 seconds of warm-up and a 60-second no-client sample. It records individual
SET/GET latency and probes autonomous restart repair while the home is isolated
from peers before its first GET. The fixed invocation additionally requires
zero idle expiry iterator and range-delete growth with stable ownership.

| Shards | Baseline aggregate CPU | Fixed aggregate CPU | Baseline ops/s | Fixed ops/s | Baseline p99 ms | Fixed p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 101.389% | 7.516% | 423.93 | 2,760.65 | 17.220 | 1.436 |
| 2 | 209.257% | 7.560% | 142.34 | 2,864.21 | 87.474 | 1.368 |
| 10 | 560.767% | 4.407% | 25.23 | 2,982.03 | 522.029 | 1.408 |

CPU is the sum of the three node means; throughput and p99 are medians of three
runs of 1,000 SET/GET pairs, timing each command separately. Both matrices ran
three shard configurations concurrently on the same Docker runtime; background
build activity overlapped the baseline. The measurements meet the directional
throughput/p99 comparison but do not establish a controlled capacity benchmark.
The earlier pair-amortized latency reports were excluded.

Baseline image: `sha256:886bb1948d39f2573864d74b51c11aefdf64f8cc4a53ba3e17c80b869357652d`.
Fixed acceptance image: `sha256:02fd6487b55c2428801532695abb512f45d5a21a193dce540a085db171f28b74`.
The first matrix's two-/ten-shard isolated restart probes timed out because
Docker stopped routing the published host port after peer-network removal.
A controlled reproduction showed the unchanged host port failing while localhost
PING/GET inside the node's network namespace succeeded; reconnecting the network
restored host access. The corrected harness uses a tracked redis-cli helper in
that namespace, with the peer network disconnected before the first GET.

The complete corrected matrix passed for **all three shard counts**, including
TTL expiry, persistent-member retention, seed-data retention and autonomous
restart repair. All idle structural assertions passed again; every disposable
container, helper, volume and network was removed successfully.

| Shards | Repeated fixed aggregate CPU | Ops/s | p99 ms | Behavior and structural checks |
| --- | ---: | ---: | ---: | --- |
| 1 | 6.899% | 2,901.74 | 1.478 | passed |
| 2 | 7.210% | 2,738.78 | 1.500 | passed |
| 10 | 6.535% | 3,041.14 | 1.400 | passed |

The baseline two-shard run failed an immediate asynchronous member read, and the
baseline ten-shard probe encountered the same Docker transport issue. Those
failures were retained, not counted as passing baseline behavior checks. The
corrected harness waits for asynchronous member convergence and verifies local
repair without relying on host-port routing.

No live Annotix deployment or volume was changed. Publishing v0.3.3 does not
perform a live rollout.

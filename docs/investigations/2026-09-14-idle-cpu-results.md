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
`8afa06618ba4320df33b5a8c004adf2b85c003e8`. The release dependency is pinned
in both Cargo.toml and the canonical Git-source Cargo.lock.

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

Final disposable cluster results are pending. The reproduction harness uses
three RF=2 nodes, unique persistent test volumes, separate client/peer networks,
180 seconds of warm-up and a 60-second no-client sample. It records individual
SET/GET latency and probes autonomous restart repair while the home is isolated
from peers before its first GET. The fixed invocation additionally requires
zero idle expiry iterator and range-delete growth with stable ownership.

No live Annotix deployment or volume was changed. Publishing v0.3.3 does not
perform a live rollout.

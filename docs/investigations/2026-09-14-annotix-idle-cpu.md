# Annotix marekvs idle CPU investigation — 2026-09-14

## Finding

The evidence points to repeated range-tombstone fragmentation during idle expiry
scans, amplified by repeated cold-partition purges. This is storage maintenance
CPU, rather than active Annotix requests or DIFF computation.

No production container was restarted, reconfigured or modified during this
investigation. Temporary profiling containers attached to the hottest process;
short syscall tracing adds overhead, so the CPU measurements below also include
samples taken before tracing and an independent sampling profile. Temporary
synthetic databases were created outside the repository and removed afterward.

## Live evidence

The Annotix Docker Compose stack contains three marekvs nodes sharing node 0's
network namespace. Each has ten `mkv-shard-*` threads. All run image ID
`sha256:886bb1948d39f2573864d74b51c11aefdf64f8cc4a53ba3e17c80b869357652d`.

| Node | First Docker CPU sample | Data-CF range-delete count | Connected clients | Active DIFF requests |
|---|---:|---:|---:|---:|
| 0 | 9.94% | 0 | 0 | 0 |
| 1 | 58.79% | 8,331 | 0 | 0 |
| 2 | 394.92% | 18,117 | 0 | 0 |

Docker CPU uses 100% per logical core. A later node-2 sample reached 651.83%;
it was not a steady-state average. `perf stat` independently measured 4.3 CPU
cores over three seconds. A five-second 49 Hz user-space sampling profile
collected 934 samples, all attributed to the ten `mkv-shard-*` threads.
Compaction, WAL-sync, and DIFF workers were effectively idle.

Over a 39–40 second metrics interval, no Redis command counters changed and no
replicated operation counters advanced. Background anti-entropy continued.
All nodes reported zero SSTable bytes, zero L0 tables, zero open SST readers,
and zero compaction debt: the observed storage state was memory/WAL resident.

Complete retained logs showed repeated cold purges:

- Node 1: 10,504 logged purges over 24 distinct partition IDs; up to 466 purges
  for one partition.
- Node 2: 19,426 logged purges over 466 distinct partition IDs; up to 484 purges
  for one partition.

Retained-log totals cover container history and need not equal the current
process counters. During the short investigation window the range-delete
counters were stable; already accumulated tombstones were sufficient to keep
CPU high.

## Source trace

1. `crates/marekvs-repl/src/lib.rs:1525`, `spawn_cold_purge`, checks cold
   partitions every 60 seconds. After a successful purge it increments a metric
   but does not record that the current cold generation has already been purged
   or consume its cleanup eligibility. Already-empty ranges can be deleted again.
2. `crates/marekvs-engine/src/store.rs:877`, `delete_partition_range`, writes
   an ondaDB range tombstone unconditionally for the partition interval.
3. `crates/marekvs-engine/src/store.rs:669`, `shard_loop`, runs an expiry sweep
   after each 100 ms idle receive timeout, independently on every shard.
4. `sweep_expired` constructs an **unbounded data-CF iterator** before processing
   at most 128 visible records. It filters shard ownership only after iteration;
   every shard pays whole-database iterator construction. The record budget does
   not bound that construction cost or invisible records skipped by the iterator.
5. In the sibling ondaDB checkout, `src/column_family.rs:2257`,
   `iterator_range_mask`, fragments the current in-memory range sets for every
   newly constructed iterator.
6. `src/range_tombstone.rs:328`, `fragment_spans`, iterates every distinct
   boundary interval and scans every span for covering ranges. The work scales
   approximately with **range-span count × distinct boundaries**, plus sorting
   sequence stacks. Unchanged tombstones incur this cost on every new iterator.

The production binary is stripped: the sampling profile identifies hot threads
but does not directly symbolize their inner Rust functions. The source trace,
range-count correlation and isolated reproduction support the specific
fragmentation explanation.

## Isolated reproduction

A temporary release-mode Rust program linked the clean local ondaDB 0.9.0
checkout. It issued only range deletions into an otherwise empty database,
then timed ten iterations of `begin → new_iterator → seek_to_first → drop`.
No client data, replica traffic or DIFF algorithms were involved.

| Range deletes | Distinct partition intervals | Mean empty-iterator time |
|---:|---:|---:|
| 0 | 0 | 8 µs |
| 1,024 | 1,024 | 6,562 µs |
| 8,192 | 1,024 | 52,167 µs |
| 18,432 | 1,024 | 220,908 µs |

A second run approximated the live counts and distinct-partition counts:

| Range deletes | Distinct intervals | Before flush | After explicit memtable flush |
|---:|---:|---:|---:|
| 8,331 | 24 | 2,851 µs | 5 µs |
| 18,117 | 466 | 75,710 µs | 292 µs |

The explicit flush was performed **only in the synthetic databases**. It moves
fragmentation out of repeated iterator construction, explaining the large
reduction. These macOS release measurements isolate the cost; they are not an
exact prediction of Linux VM CPU utilization.

Temporary reproduction: `/tmp/marekvs-idle-probe/src/main.rs`.
Results: `/tmp/marekvs-idle-probe.log`, `/tmp/marekvs-idle-probe-shaped.log`.
Profiles: `/tmp/annotix-mkv-profile.txt`, `/tmp/annotix-mkv-perf.txt`.
Metrics snapshots: `/tmp/annotix-mkv-metrics-{0,1,2}-{a,b}.txt`.

## Fix direction

- Make cold cleanup idempotent for a cold ownership/data generation. Re-arm it
  when ownership changes or newly arriving data requires cleanup; a blanket
  permanent “already purged” flag would be unsafe.
- Reuse immutable range-fragment snapshots until range state changes, or use a
  more efficient fragmentation algorithm. Preserve MVCC sequence visibility.
- Avoid ten independent unbounded expiry scans of the same database. Bound
  iterator construction as well as visible-record processing, and scan only
  the owning shard's partition ranges.
- Consider flushing range-heavy memtables as a controlled mitigation. A restart
  alone is not a durable fix because WAL replay can restore the same tombstones.

No fix or operational mitigation has been applied to the Annotix stack.

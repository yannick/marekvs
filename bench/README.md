# marekvs vs KeyDB benchmark suite

Compares single-node marekvs against [KeyDB](https://docs.keydb.dev/)
(the multithreaded Redis fork) with identical workloads driven by
`redis-benchmark` from the host.

## Run it

```sh
just bench            # build image, start both engines, run matrix, report
just bench-report     # re-render bench/report.md from bench/results.csv
just bench-down       # stop the engine containers
```

Knobs (env): `BENCH_REQUESTS` (default 100000), `BENCH_CLIENTS` (50),
`BENCH_THREADS` (4 — redis-benchmark client threads).

## Methodology

- **Both engines run in Docker** on the same machine and network mode, so
  the Docker-on-macOS VM overhead applies to both equally. The benchmark
  client runs on the host against mapped ports.
- KeyDB runs with `--server-threads 4 --appendonly no` (its headline
  multithreading on, persistence off — its fastest sensible config).
- marekvs runs single-node (`MAREKVS_REPLICAS_N=1`), its default
  `SyncMode::Interval` (128 ms fsync window) — every write hits the LSM.
- Workload matrix: 14 command types × {100 B P=1, 100 B P=16, 1 KiB P=1},
  50 clients, keyspace of 100 k random keys, FLUSHALL between engines.
- One warm-up consequence of `-t ... -r`: LPOP/SPOP phases consume data
  pushed by earlier LPUSH/SADD phases — same ordering for both engines.

## Read the numbers with these caveats

1. **This is disk vs RAM.** marekvs persists every write into ondadb
   (WAL + memtable + compaction); KeyDB holds everything in memory with
   persistence off. KeyDB "should" win — the interesting result is the
   margin, especially for reads (marekvs serves hot reads from memtable +
   block cache).
2. **marekvs pays the merge tax.** Every write carries a 19-byte envelope
   and goes through convergent-merge logic so that multi-node replication
   works; KeyDB does none of that in single-node mode.
3. **Lists differ structurally**: marekvs lists are whole-value blobs
   (design/02) — LPUSH/LPOP rewrite the list; expect KeyDB to dominate
   long-list workloads. `lrange_100` on marekvs decodes one blob.
   **Beware large request counts**: redis-benchmark's list tests push every
   request onto ONE fixed key (`mylist`), so an N-request LPUSH test builds
   an N-element list — on marekvs that's O(N²) total blob-rewrite bytes,
   and every other key sharing that shard thread queues behind it
   (head-of-line blocking). The harness FLUSHALLs between configs to stop
   cross-config compounding, but within one config the quadratic cost is
   real marekvs behavior — that's the finding, not a harness artifact.
   Keep `BENCH_REQUESTS` ≲ 20–50k unless you want to measure exactly that.
4. Docker port mapping + the macOS VM cap absolute numbers well below
   bare-metal Linux; only the *relative* comparison is meaningful here.
5. Single run per config, no confidence intervals — this is a smoke-level
   comparison harness, not a paper. For serious numbers: run on Linux
   bare metal, repeat ≥ 5×, alternate engine order (`bench/results.csv`
   is append-only, so repeated runs accumulate and the report averages
   nothing — wipe it between campaigns).

## Files

| File | Purpose |
|---|---|
| `run_workloads.sh` | one engine → CSV rows on stdout |
| `report.py` | results.csv → report.md (side-by-side + ratios) |
| `results.csv` | appended raw results (gitignored) |
| `report.md` | last rendered report (gitignored) |

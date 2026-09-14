# Idle maintenance reproduction

Run only against disposable synthetic data. The fixture creates and removes its
own temporary database; it never opens an existing deployment's volume.

```sh
cargo run --release -p marekvs-engine --example idle_maintenance -- 10 18117 466 13765 60 60
```

Arguments are shard count, range-delete count, distinct partition ranges, live
persistent key count, warm-up seconds and sample seconds. Output contains
Prometheus snapshots bracketing the sample and process CPU time (100% = one core).
Warm-up, seeding and process shutdown are excluded from measured CPU.

Compare the same release toolchain, host, arguments and ondaDB provenance before
and after the fix. Test shards 1/2/10; range fixtures 0/0, 1024/1024, 8331/24 and
18117/466; and keys 0 and 13765. Use at least a 60-second sample. No TTLs are
installed here: key/member TTL, restart and cold-purge safety belong to the
behavioral regression suites, which run under the ordinary CI test command.

The process fixture isolates expiry maintenance and database worker cost. It
is not a three-node networking or client-throughput benchmark. Record that
separately using disposable containers, stable RF=2 ownership and no live
Annotix data. ondaDB's range-cache benchmark covers ordinary and unified
memtables, cold/warm iterator construction and retained cache memory.

## Disposable Docker acceptance

```sh
python3 tests/idle_maintenance/cluster.py --image marekvs:idle-baseline --shards 10 --output /tmp/idle-baseline.json
python3 tests/idle_maintenance/cluster.py --image marekvs:idle-fixed --expect-fixed --shards 10 --output /tmp/idle-fixed.json
```

The script assigns unique peer and client networks, containers, volumes and dynamic loopback
ports, and removes only those resources when finished. It seeds one owner before
joining two peers (RF=2), uses shortened cold-retention settings **only in these
test containers**, warms up for at least 180 seconds, then records CPU and
metrics for 60 seconds without client traffic. After sampling it measures three
SET/GET throughput/latency runs and verifies key/member expiry and restart repair.
Repeat with `--shards 1` and `--shards 2`. Reports retain the image digest,
configuration, metric snapshots, raw CPU samples, behavior outcome and logs.

Compare aggregate mean CPU and per-node means, plus the median throughput and
p99 across the three runs. Latencies measure individual SET and GET commands. Check the structural expiry/range counters alongside
CPU: low CPU alone cannot show that a scheduler still expires records correctly.
`--expect-fixed` waits for discovery to settle and requires zero growth in expiry
iterator and range-delete counters during the idle sample. Restart repair is
checked with the restarted home disconnected from the peer network before its
first GET, while the separate client network remains reachable.

The script records failure in its JSON report and exits unsuccessfully; it does
not replace behavioral CI tests with a timing threshold.

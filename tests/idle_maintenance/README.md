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

# ondaDB 0.9.0 Feature Adoption Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Adopt the ondaDB 0.9.0 features that pay off for marekvs — range-delete partition purge with excise, periodic compaction, prefix-delta blocks, MultiGet, PerfContext, background IO limiting, and the vlog value cache — each behind its own commit and each individually revertible.

**Architecture:** ondaDB 0.9.0 gates everything that changes a stored byte behind a one-way manifest capability bit that defaults off; a database enabling nothing is byte-identical to 0.8.2. So each phase below is independent: it turns on one capability or one option, proves it with a test, and ships. Phases 1–3 (Tier 1) change what marekvs *does*; phases 4–7 (Tier 2) change what it *measures* or how fast it reads. No phase depends on a later one.

**Tech Stack:** Rust 2021 (toolchain floor 1.89), ondaDB 0.9.0 (path-patched sibling checkout), tokio, prometheus, tempfile + `#[tokio::test]` for integration tests.

**Prerequisite:** ondaDB 0.9.0 must be pushed to `github.com/yannick/ondadb` before ANY of this can ship. `.github/workflows/docker.yml` checks that repo out at `ref: main` and `Dockerfile` builds without `--locked`, so CI currently re-resolves silently to 0.8.2. Commit `b02a32b` (the lock bump) is already held back for this reason. **Verify `git ls-remote --tags https://github.com/yannick/ondadb | grep v0.9.0` returns a hit before starting.**

---

## Scope note

These are seven largely independent changes. They are in one document because they share one dependency upgrade and one rollback story, but **each phase produces working, testable software on its own** and can be split into its own plan if you prefer. If you only do one, do Phase 1.

## File Structure

| File | Responsibility | Phases |
|---|---|---|
| `crates/marekvs-engine/src/store.rs` | ondaDB open + options; add capability enabling, new env knobs, `delete_partition_range` primitive | 1, 2, 5, 6, 7 |
| `crates/marekvs-repl/src/lib.rs` | `purge_partition` rewrite; excise pass; new gauges into the stats task | 1, 2 |
| `crates/marekvs-engine/src/metrics.rs` | New gauges for range deletes, excise, periodic compaction | 1, 2 |
| `crates/marekvs-engine/src/cmd/string.rs` | MGET batched read | 4 |
| `crates/marekvs-engine/tests/range_purge.rs` | **New.** Range-delete purge correctness | 1 |
| `crates/marekvs-engine/tests/periodic_compaction.rs` | **New.** Idle reclamation | 2 |
| `crates/marekvs-engine/tests/prefix_delta.rs` | **New.** Space measurement | 3 |
| `README.md`, `design/09-performance.md` | Operator surface + tuning record | all |

---

## Phase 1 — Range-delete partition purge (Tier 1, highest value)

**Why.** `purge_partition` (`crates/marekvs-repl/src/lib.rs:1602`) runs on every rebalance. Today it scans `partition_prefix(pid)` and issues one `del_raw` per key in 512-key chunks with a 20 ms sleep between chunks. Internal keys are `[pid:u16 BE][tag]…` (`crates/marekvs-core/src/ikey.rs:1-14`), so a partition is exactly the half-open interval `[pid, pid+1)` — one `delete_range` replaces the whole loop.

**The safety win matters as much as the speed.** That function must not replicate; it wraps its deletes in `store::suppress_commit_hook()`. Lose that guard in a refactor and you delete the partition cluster-wide from the owners you just handed it to. Range deletes are *structurally* invisible to commit hooks (`ondadb/src/txn.rs:1459`: "a range delete has two keys and no value, so v1 does not surface it to hooks"), so the guarantee stops depending on being remembered.

**Design decision you must not skip:** `purge_partition` currently returns a *record count* feeding `marekvs_cold_purged_records_total`. A range delete has no count, and counting first would reinstate the scan this phase removes. **Change the metric rather than keep the scan:** `cold_purged_records_total` stays but stops advancing, and a new `marekvs_cold_purged_partitions_total` counts partitions. Report reclaimed bytes from `CfStats::excised_bytes` instead — that is the number an operator actually wants.

### Task 1.1: Enable `CAP_RANGE_DELETES` at open

**Files:**
- Modify: `crates/marekvs-engine/src/store.rs` (in `Store::open`, after `DB::open`)
- Test: `crates/marekvs-engine/tests/range_purge.rs` (create)

- [ ] **Step 1: Write the failing test**

```rust
//! Range-delete partition purge (ondaDB 0.9.0 feature 1.2).

use std::sync::Arc;

use marekvs_core::ikey;
use marekvs_engine::store::{put_raw, Store, StoreConfig};
use ondadb::SyncMode;

fn store(dir: &tempfile::TempDir) -> Arc<Store> {
    Store::open(&StoreConfig {
        data_dir: dir.path().to_string_lossy().into_owned(),
        node_id: 1,
        shard_threads: 2,
        sync_mode: SyncMode::Interval,
    })
    .unwrap()
}

/// Range deletes change stored bytes, so ondaDB gates them behind a one-way
/// capability bit. Opening must enable it, or `delete_range` fails at runtime
/// on a database that has never had it turned on.
#[tokio::test]
async fn open_enables_the_range_delete_capability() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir);
    assert_ne!(
        store.db.format_capabilities() & ondadb::format::CAP_RANGE_DELETES,
        0,
        "CAP_RANGE_DELETES must be enabled at open"
    );
}
```

- [ ] **Step 2: Run it and watch it fail**

Run: `cargo test -p marekvs-engine --test range_purge open_enables_the_range_delete_capability`
Expected: FAIL — capability mask is `0`.

- [ ] **Step 3: Enable the capability in `Store::open`**

In `crates/marekvs-engine/src/store.rs`, immediately after `let db = DB::open(opts)?;`:

```rust
        // Range deletes (ondaDB 1.2) back `purge_partition`: a rebalance drops a
        // whole `[pid, pid+1)` interval in one record instead of one tombstone
        // per key. The bit is ONE-WAY and changes stored bytes, so a database
        // that has taken it can no longer be read by ondaDB < 0.9.0 — that is
        // the rollback boundary for this feature, and it is why it is enabled
        // explicitly here rather than inferred from a config field.
        db.enable_format_capabilities(ondadb::format::CAP_RANGE_DELETES)?;
```

- [ ] **Step 4: Run it and watch it pass**

Run: `cargo test -p marekvs-engine --test range_purge open_enables_the_range_delete_capability`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/marekvs-engine/src/store.rs crates/marekvs-engine/tests/range_purge.rs
git commit -m "feat: enable ondaDB range-delete capability at open"
```

### Task 1.2: A `delete_partition_range` primitive in the store layer

**Files:**
- Modify: `crates/marekvs-engine/src/store.rs` (next to `del_raw`, ~line 759)
- Test: `crates/marekvs-engine/tests/range_purge.rs`

- [ ] **Step 1: Write the failing test**

Append to `crates/marekvs-engine/tests/range_purge.rs`:

```rust
/// One range delete removes every record of one partition and touches no
/// neighbouring partition. The bound arithmetic is the whole risk here: an
/// end bound of `pid` instead of `pid + 1` deletes nothing, and a 4-byte
/// encoding of `pid + 1` sorts below every 2-byte key and also deletes nothing.
#[tokio::test]
async fn range_delete_drops_exactly_one_partition() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir);

    // Three adjacent partitions, one string record each, written directly so
    // the test does not depend on key routing.
    let pids: [ikey::Pid; 3] = [41, 42, 43];
    for pid in pids {
        let s = store.clone();
        s.run(0, move |ctx| {
            let mut k = pid.to_be_bytes().to_vec();
            k.push(b's');
            k.extend_from_slice(b"key");
            put_raw(ctx, &k, b"value");
        })
        .await;
    }

    let s = store.clone();
    s.run(0, |ctx| marekvs_engine::store::delete_partition_range(ctx, 42))
        .await
        .expect("range delete");

    for (pid, expect_present) in [(41, true), (42, false), (43, true)] {
        let s = store.clone();
        let present = s
            .run(0, move |ctx| {
                let mut found = false;
                let _ = marekvs_engine::store::scan_prefix(
                    ctx,
                    &(pid as ikey::Pid).to_be_bytes(),
                    |_, _| {
                        found = true;
                        false
                    },
                );
                found
            })
            .await;
        assert_eq!(present, expect_present, "partition {pid}");
    }
}
```

- [ ] **Step 2: Run it and watch it fail**

Run: `cargo test -p marekvs-engine --test range_purge range_delete_drops_exactly_one_partition`
Expected: FAIL to compile — `delete_partition_range` not found.

- [ ] **Step 3: Implement the primitive**

In `crates/marekvs-engine/src/store.rs`, after `del_raw`:

```rust
/// Drop every record of one partition with a single range delete.
///
/// Internal keys lead with the partition id big-endian (`ikey` module docs), so
/// a partition is exactly the half-open interval `[pid, pid + 1)` and one
/// record at one sequence replaces a scan plus a tombstone per key.
///
/// **This never reaches the commit hook.** ondaDB does not surface range
/// deletes to hooks at all (a range delete is two keys and no value; `CommitOp`
/// is one key and one value), which is what makes it safe for the caller that
/// needs a purely LOCAL drop — dropping this node's copy of a partition it no
/// longer owns must not replicate tombstones to the new owners. The scan-based
/// predecessor depended on a `suppress_commit_hook()` guard for that; here it
/// is a property of the operation.
pub fn delete_partition_range(ctx: &ShardCtx, pid: Pid) -> anyhow::Result<()> {
    // `Pid` is u16 and `ikey::PARTITIONS` is 4096, so `pid + 1` cannot overflow
    // a u16 in practice — but compute in u32 and encode 2 bytes anyway, because
    // a 4-byte end bound would sort BELOW every 2-byte key and silently delete
    // nothing at all.
    let end: u16 = u16::try_from(u32::from(pid) + 1)
        .map_err(|_| anyhow::anyhow!("partition {pid} has no representable upper bound"))?;
    ctx.db
        .delete_range(&ctx.data, &pid.to_be_bytes(), &end.to_be_bytes())
        .map_err(|e| anyhow::anyhow!("range delete for partition {pid} failed: {e:?}"))
}
```

- [ ] **Step 4: Run it and watch it pass**

Run: `cargo test -p marekvs-engine --test range_purge`
Expected: both tests PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/marekvs-engine/src/store.rs crates/marekvs-engine/tests/range_purge.rs
git commit -m "feat: delete_partition_range store primitive"
```

### Task 1.3: A purged partition must not resurrect on re-bootstrap

This is the correctness question the feature turns on, and it needs its own test: a partition can be purged and then handed *back* to this node, and the re-bootstrapped records must be visible. A range delete masks at its own sequence, so later writes are unaffected — but that is a property to pin, not to assume.

**Files:**
- Test: `crates/marekvs-engine/tests/range_purge.rs`

- [ ] **Step 1: Write the test**

```rust
/// A partition purged by range delete and then re-populated (the rebalance
/// give-back path) must show the NEW records. A range delete masks at its own
/// sequence; anything written after it is above the span and survives. If this
/// ever fails, `purge_partition` cannot use range deletes at all.
#[tokio::test]
async fn records_written_after_a_purge_survive() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir);
    let key = {
        let mut k = 7u16.to_be_bytes().to_vec();
        k.push(b's');
        k.extend_from_slice(b"back");
        k
    };

    let (s, k) = (store.clone(), key.clone());
    s.run(0, move |ctx| put_raw(ctx, &k, b"before")).await;
    let s = store.clone();
    s.run(0, |ctx| marekvs_engine::store::delete_partition_range(ctx, 7))
        .await
        .unwrap();
    let (s, k) = (store.clone(), key.clone());
    s.run(0, move |ctx| put_raw(ctx, &k, b"after")).await;

    let (s, k) = (store.clone(), key.clone());
    let got = s
        .run(0, move |ctx| marekvs_engine::store::get_raw(ctx, &k))
        .await;
    assert_eq!(got.as_deref(), Some(&b"after"[..]));
}
```

- [ ] **Step 2: Run it**

Run: `cargo test -p marekvs-engine --test range_purge records_written_after_a_purge_survive`
Expected: PASS (no implementation needed — this pins existing engine behaviour).

- [ ] **Step 3: Commit**

```bash
git add crates/marekvs-engine/tests/range_purge.rs
git commit -m "test: purged partitions accept new records"
```

### Task 1.4: Rewrite `purge_partition`

**Files:**
- Modify: `crates/marekvs-repl/src/lib.rs:1602-1628` (`purge_partition`) and its caller at `:1573`
- Modify: `crates/marekvs-engine/src/metrics.rs`

- [ ] **Step 1: Add the partition counter**

In `crates/marekvs-engine/src/metrics.rs`, beside `cold_purged_records_total` (field ~line 47, constructor ~line 315):

```rust
    /// Partitions dropped by cold purge. Replaces the per-record count as the
    /// purge signal: a range delete retires a whole `[pid, pid+1)` interval in
    /// one record, so there is no record count to report without reinstating
    /// the scan the range delete exists to remove.
    pub cold_purged_partitions_total: IntCounter,
```

```rust
            cold_purged_partitions_total: counter!(
                registry,
                "marekvs_cold_purged_partitions_total",
                "Partitions whose local copy was dropped by cold purge"
            ),
```

- [ ] **Step 2: Replace the body**

In `crates/marekvs-repl/src/lib.rs`, replace `purge_partition` entirely:

```rust
    /// Physically drop this node's records for `pid` with a single range delete.
    ///
    /// Deliberately a *local* drop: this node is discarding its own copy of a
    /// partition it no longer owns, not deleting the records cluster-wide.
    /// ondaDB never surfaces a range delete to a commit hook, so — unlike the
    /// scan-and-tombstone predecessor, which needed an explicit
    /// `suppress_commit_hook()` guard — nothing here can reach the replication
    /// ring even by accident.
    ///
    /// Returns immediately: there is no chunking and no inter-chunk yield,
    /// because there is no per-record work to spread out. The commit does take
    /// ondaDB's database-wide commit lock (range commits are atomic against
    /// every isolation level), which is why this is only ever called for a
    /// whole cold partition and never on a client path.
    async fn purge_partition(&self, pid: Pid) -> anyhow::Result<()> {
        self.store
            .run(pid, move |ctx| store::delete_partition_range(ctx, pid))
            .await
    }
```

- [ ] **Step 3: Update the caller at `:1573`**

```rust
                    match self.purge_partition(pid).await {
                        Ok(()) => {
                            self.engine.metrics.cold_purged_partitions_total.inc();
                            tracing::info!(
                                pid,
                                rounds,
                                "cold purge: dropped local copy of an un-owned partition"
                            );
                        }
                        Err(e) => {
                            tracing::warn!(pid, error = %e, "cold purge range delete failed");
                        }
                    }
```

- [ ] **Step 4: Build and test**

Run: `cargo test --workspace --no-fail-fast`
Expected: 29 binaries ok, 446+ passing. If `window_mode_refills_each_period` fails, re-run it alone — it is a known timing flake, not a regression.

- [ ] **Step 5: Commit**

```bash
git add crates/marekvs-repl/src/lib.rs crates/marekvs-engine/src/metrics.rs
git commit -m "perf: purge a cold partition with one range delete"
```

### Task 1.5: Excise pass + reclamation gauges

`delete_range` masks logically; `excise_covered` is what returns the *space*, by retiring whole SSTables the span covers without reading them.

**Files:**
- Modify: `crates/marekvs-repl/src/lib.rs` (`purge_partition`, and `update_disk_guard` ~line 870)
- Modify: `crates/marekvs-engine/src/metrics.rs`

- [ ] **Step 1: Add the gauges**

```rust
    /// Range-delete records committed to the data CF, and the durable
    /// fragments they cost after flush and compaction have clipped them. A
    /// fragment count growing without bound means purges are outrunning
    /// compaction's ability to retire them.
    pub db_range_deletes: IntGauge,
    pub db_range_fragments: IntGauge,
    /// Tables delete-only excise has retired, and the bytes they held — space
    /// reclaimed WITHOUT reading or rewriting the data.
    pub db_excised_tables: IntGauge,
    pub db_excised_bytes: IntGauge,
```

Register them beside `db_l0_files` with `gauge!`, help text matching the doc comments.

- [ ] **Step 2: Publish them in `update_disk_guard`**

Beside the existing `let cf = self.store.data.stats();` block:

```rust
        m.db_range_deletes.set(cf.range_deletes as i64);
        m.db_range_fragments.set(cf.range_fragments as i64);
        m.db_excised_tables.set(cf.excised_tables as i64);
        m.db_excised_bytes.set(cf.excised_bytes as i64);
```

- [ ] **Step 3: Run excise after a purge**

Append to `purge_partition`, after the range delete succeeds:

```rust
        // Reclaim eagerly: a purge is exactly the case excise is for — a whole
        // interval proven deleted, so any table lying entirely inside it can be
        // unlinked by catalog edit without reading a byte. Best-effort: excise
        // declines whenever a table is busy (an overlapping compaction, an
        // in-flight part move, a foreign mount) and that is a skip, not an
        // error, so a failure here must not fail the purge that already
        // succeeded.
        // `excise_covered` hangs off DB, not ColumnFamily (ColumnFamily has no
        // `db()` accessor — verified), so capture both handles. `DB` is Clone.
        let db = self.store.db.clone();
        let data = self.store.data.clone();
        let excised = tokio::task::spawn_blocking(move || db.excise_covered(&data))
            .await
            .unwrap_or(Ok(0));
        match excised {
            Ok(n) if n > 0 => tracing::info!(pid, tables = n, "excise retired tables after purge"),
            Ok(_) => {}
            Err(e) => tracing::warn!(pid, error = ?e, "excise pass failed after purge"),
        }
```

- [ ] **Step 4: Test**

Run: `cargo test --workspace --no-fail-fast`

- [ ] **Step 5: Commit**

```bash
git add crates/marekvs-repl/src/lib.rs crates/marekvs-engine/src/metrics.rs
git commit -m "feat: excise pass and reclamation gauges after a cold purge"
```

---

## Phase 2 — Periodic compaction (Tier 1)

**Why.** Deleting a collection writes a *head tombstone* carrying `del_hlc` (`crates/marekvs-engine/src/cmd/generic.rs:39-52`) — O(1) writes, but every element record stays physically on disk, only logically shadowed. gc_grace tombstones behave the same. Both are reclaimed only when compaction happens to run, and a size trigger never fires on an idle family. `periodic_compaction_interval` revisits tables by age so an idle family reclaims.

### Task 2.1: Env knob + capability

**Files:**
- Modify: `crates/marekvs-engine/src/store.rs`
- Test: `crates/marekvs-engine/tests/periodic_compaction.rs` (create)

- [ ] **Step 1: Write the failing test**

```rust
//! Periodic compaction (ondaDB 0.9.0 feature 0.3): an idle family must still
//! reclaim expired TTL entries, tombstones and shadowed versions.

use std::sync::Arc;
use std::time::Duration;

use marekvs_engine::store::{Store, StoreConfig};
use ondadb::SyncMode;

fn store(dir: &tempfile::TempDir) -> Arc<Store> {
    Store::open(&StoreConfig {
        data_dir: dir.path().to_string_lossy().into_owned(),
        node_id: 1,
        shard_threads: 2,
        sync_mode: SyncMode::Interval,
    })
    .unwrap()
}

/// The interval is durable per column family, so a reopen must restore it —
/// otherwise idle reclamation silently stops after the first restart.
#[tokio::test]
async fn periodic_interval_is_configured_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    std::env::set_var("MAREKVS_PERIODIC_COMPACTION_SECS", "3600");
    {
        let store = store(&dir);
        let cfg = store.db.column_family_config("data").unwrap();
        assert_eq!(cfg.periodic_compaction_interval, Duration::from_secs(3600));
    }
    let store = store(&dir);
    let cfg = store.db.column_family_config("data").unwrap();
    assert_eq!(cfg.periodic_compaction_interval, Duration::from_secs(3600));
    std::env::remove_var("MAREKVS_PERIODIC_COMPACTION_SECS");
}
```

> **Note:** `std::env::set_var` is process-global. Keep every periodic-compaction env assertion in this one test, exactly as `store::env_knob_tests` does, so parallel tests cannot race it.

- [ ] **Step 2: Run and watch it fail**

Run: `cargo test -p marekvs-engine --test periodic_compaction`
Expected: FAIL — interval is `Duration::ZERO`.

- [ ] **Step 3: Implement**

In `Store::open`, extend the capability call from Phase 1 and the CF config:

```rust
        db.enable_format_capabilities(
            ondadb::format::CAP_RANGE_DELETES | ondadb::format::CAP_PERIODIC_AGE,
        )?;
```

```rust
        // Idle families never reclaim on a size trigger, and marekvs has two
        // sources of dead-but-resident data that only compaction removes:
        // gc_grace tombstones, and collection elements shadowed by a head
        // tombstone's del_hlc (design/02) which are logically dead but
        // physically present. A 24 h revisit bounds how long they stay.
        // 0 disables (ondaDB's default).
        let periodic = Duration::from_secs(env_u64("MAREKVS_PERIODIC_COMPACTION_SECS", 86_400));
```

and inside `cf_config`: `periodic_compaction_interval: periodic,`

Add `env_u64` next to `env_usize` if absent — mirror `env_usize`, returning `u64`, with `0` meaningful (do NOT `.filter(|&v| v > 0)`, since 0 disables).

- [ ] **Step 4: Run and watch it pass**

Run: `cargo test -p marekvs-engine --test periodic_compaction`

- [ ] **Step 5: Commit**

```bash
git add crates/marekvs-engine/src/store.rs crates/marekvs-engine/tests/periodic_compaction.rs
git commit -m "feat: periodic compaction so idle families reclaim"
```

### Task 2.2: Expose `periodic_compactions`

**Files:** `crates/marekvs-engine/src/metrics.rs`, `crates/marekvs-repl/src/lib.rs`

- [ ] **Step 1:** Add gauge `db_periodic_compactions` — "Compactions the age trigger picked rather than a capacity trigger". Publish `cf.periodic_compactions` in `update_disk_guard`.
- [ ] **Step 2:** Run `cargo test --workspace --no-fail-fast`.
- [ ] **Step 3:** Commit: `feat: export the periodic-compaction counter`.

---

## Phase 3 — Prefix-delta data blocks (Tier 1, measure before keeping)

**Why.** marekvs keys are extraordinarily prefix-redundant: every hash field record repeats `[pid][b'h'][varint klen][userkey]` and differs only in the field suffix, so a 1000-field hash stores that prefix 1000 times. Space-for-CPU, per family, opt-in.

**This phase ends in a measurement, and the measurement decides whether the default is on or off.** Do not skip to enabling it.

### Task 3.1: Knob + capability, default OFF

- [ ] **Step 1:** Add `MAREKVS_PREFIX_DELTA_KEYS` (default `false`) via `env_bool`; set `enable_prefix_delta_keys` in `cf_config`; add `CAP_PREFIX_DELTA` to the capability mask **only when the knob is on** — it is one-way, so do not burn it by default.
- [ ] **Step 2:** Test in `crates/marekvs-engine/tests/prefix_delta.rs` that the knob reaches the effective durable CF config and survives a reopen (same shape as Task 2.1).
- [ ] **Step 3:** Commit: `feat: optional prefix-delta data blocks`.

### Task 3.2: Measure it

- [ ] **Step 1:** Write `crates/marekvs-engine/tests/prefix_delta.rs::prefix_delta_shrinks_a_hash_heavy_store` — build two stores in separate tempdirs, one with the knob on, write 200 hashes × 200 fields each with 32-byte values, `db.close()` both, sum file sizes under each dir, assert the delta store is strictly smaller. Print both sizes with `--nocapture` so the ratio is recorded.
- [ ] **Step 2:** Run `cargo test -p marekvs-engine --test prefix_delta -- --nocapture` and **write the measured ratio into `design/09-performance.md`**.
- [ ] **Step 3:** Decide the default from the number, and say so in the commit message. Under ~5% saving, leave it off and record that.
- [ ] **Step 4:** Commit: `perf: measure prefix-delta blocks on a hash-heavy store`.

---

## Phase 4 — MultiGet for MGET (Tier 2)

**Why.** `mget` (`crates/marekvs-engine/src/cmd/string.rs:433`) already groups keys by shard, but the inner loop is N sequential `read_string` calls (`:463-471`). `Txn::multi_get` resolves them in one snapshot-consistent pass.

**Honest expectation:** marekvs hashes the user key into the pid, so one shard's keys scatter across the keyspace and the "one block fetch per distinct block" component will NOT fully materialise. The win is the shared snapshot and level-walk setup. Expect well under ondaDB's 2.4–3.4x. **If the benchmark shows no improvement, revert this phase** — it is not worth the complexity for zero gain.

### Task 4.1: Batched read helper

- [ ] **Step 1:** Write a failing test in `crates/marekvs-engine/tests/mget_batch.rs`: 64 keys set, MGET returns all 64 values in order, with 8 absent keys interleaved returning `Null`. Assert ordering explicitly — the reply index mapping is the easiest thing to break.
- [ ] **Step 2:** Run it against the current implementation; it should PASS (this is a characterisation test — it locks in behaviour *before* the rewrite).
- [ ] **Step 3:** Add `store::read_strings_batch(ctx, keys: &[Vec<u8>]) -> Vec<Option<Vec<u8>>>` using `ctx.db.begin()` + `txn.multi_get(&ctx.data, &refs)`, decoding each envelope exactly as `read_string` does. Reuse `read_string`'s envelope/TTL/tombstone logic — extract it into a shared `fn decode_string_value(raw: &[u8]) -> Option<Vec<u8>>` rather than duplicating it. **DRY matters here:** a second copy of the tombstone check is a correctness bug waiting to happen.
- [ ] **Step 4:** Rewrite the `mget` inner closure to call it. Run the characterisation test — it must still pass unchanged.
- [ ] **Step 5:** Commit: `perf: batch MGET reads through ondaDB multi_get`.

### Task 4.2: Prove it is faster (or revert)

- [ ] **Step 1:** Using the `kvload` harness pattern from the 0.8.x benchmarking (or `redis-benchmark -t mget`), measure MGET of 64 keys before and after on a settled 5M-key store.
- [ ] **Step 2:** Record the number in `design/09-performance.md`. If it is inside run-to-run noise, `git revert` Task 4.1 and record *that* — a rewrite that buys nothing is a liability.

---

## Phase 5 — PerfContext (Tier 2, diagnostic)

**Why.** This is the instrument that settles the open L0-depth question from the 0.8.x benchmarking: attribute read cost to bloom probes vs SSTable probes vs block-cache misses instead of inferring it from wall time.

### Task 5.1: A DEBUG subcommand

- [ ] **Step 1:** Add `DEBUG PERFCTX <key>` to `crates/marekvs-engine/src/cmd/server.rs::debug`, following the existing `DEBUG` arm conventions. Wrap one `read_string` in `ondadb::perf::enter()` / `Scope::finish()` and return the counters as a RESP map (bloom probes, memtable probes, SSTable probes, block-cache hits/misses, bytes decompressed, vlog reads).
- [ ] **Step 2:** Test in `crates/marekvs-engine/tests/perfctx.rs`: after writing and reading a key, `DEBUG PERFCTX` reports a non-zero SSTable-or-memtable probe count. Assert a *mechanism fired*, not an exact number — the numbers are machine-dependent.
- [ ] **Step 3:** Commit: `feat: DEBUG PERFCTX exposes ondaDB read-path counters`.
- [ ] **Step 4:** Document in `design/09-performance.md` under the ondaDB tuning section: this is how a read-latency claim gets attributed to a mechanism.

---

## Phase 6 — Background IO limiter (Tier 2, measure first)

**Why.** Bounds background bandwidth so flush and compaction cannot monopolise the device — relevant for k8s shared PVCs. **ondaDB ships this default-off and explicitly did not validate its benchmark.** Treat it as unproven.

### Task 6.1: Knobs, default off

- [ ] **Step 1:** Add `MAREKVS_BACKGROUND_IO_BPS` (→ `background_io_bytes_per_second`), `MAREKVS_BACKGROUND_IO_BURST_BYTES`, `MAREKVS_OBSOLETE_DELETE_BPS`, all defaulting to `0` = off. Log them in the existing `"ondaDB options"` tracing line.
- [ ] **Step 2:** Test that a non-zero value reaches `Options` (assert via the effective config if ondaDB exposes it; otherwise assert `env_u64` parsing directly and note the limitation in a comment rather than pretending to test more).
- [ ] **Step 3:** Commit: `feat: optional background IO rate limits`.

### Task 6.2: Decide with data

- [ ] **Step 1:** On the bench box, run a sustained ingest with the limiter off and then set to ~50% of measured device throughput; compare client p99 from `kvload` and `marekvs_db_compaction_debt_bytes`.
- [ ] **Step 2:** Record in `design/09-performance.md`. Keep the default at `0` unless p99 improves beyond the noise band — and note that the box used previously drifts ±15%, so use a bracketed A-B-A, not an ordered sweep.

---

## Phase 7 — vlog value cache (Tier 2)

**Why.** `klog_value_threshold` is 512 B, so every larger Redis string lives in the vlog and is re-read per access.

### Task 7.1: Knob, default off

- [ ] **Step 1:** Add `MAREKVS_VLOG_VALUE_CACHE_BYTES` → `max_cached_vlog_value_bytes`, default `0` (ondaDB's default = off).
- [ ] **Step 2:** Test it reaches the effective durable CF config and survives a reopen.
- [ ] **Step 3:** Commit: `feat: optional vlog value cache`.

### Task 7.2: Measure

- [ ] **Step 1:** Benchmark repeated GETs of 4 KiB values (above the klog threshold, so they live in the vlog) with the cache off and at 256 MiB.
- [ ] **Step 2:** Record the result; enable by default only if it clearly wins. Note in the commit that the block-cache `BlockDomain` aliasing fix rode in with the 0.9.0 upgrade regardless and is unrelated to this knob.

---

## Final task: documentation sweep

- [ ] **Step 1:** Add every new env var to the README storage-engine table with defaults, matching the existing row style.
- [ ] **Step 2:** Add a `design/09-performance.md` subsection "ondaDB 0.9.0 adoption" recording, for each phase, what was enabled, what was measured, and what was left off and why. **A phase that was measured and rejected must be recorded** — otherwise the next person re-runs the same experiment.
- [ ] **Step 3:** Note the one-way capability bits (`CAP_RANGE_DELETES`, `CAP_PERIODIC_AGE`, and `CAP_PREFIX_DELTA` if enabled) and the rollback boundary they create: once taken, the database can no longer be opened by ondaDB < 0.9.0.
- [ ] **Step 4:** Commit: `docs: record the ondaDB 0.9.0 adoption decisions`.

---

## Rollback

Phases 4–7 are pure code/config and revert cleanly with `git revert`.

Phases 1–3 enable **one-way capability bits**. Reverting the marekvs commit stops *writing* the new records, but a database that has taken a bit cannot be reopened by ondaDB < 0.9.0. The rollback unit is therefore the ondaDB version, not the marekvs commit — which is exactly why Phase 3 keeps `CAP_PREFIX_DELTA` unclaimed until its knob is deliberately switched on.

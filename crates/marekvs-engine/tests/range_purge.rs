//! Range-delete partition purge (ondaDB 0.9.0 feature 1.2).
//!
//! Internal keys lead with the partition id big-endian (`ikey` module docs), so
//! a partition is exactly the half-open interval `[pid, pid + 1)`. That is what
//! lets a rebalance drop this node's copy of an un-owned partition with one
//! record at one sequence instead of a scan plus a tombstone per key.

use std::sync::Arc;

use marekvs_core::ikey;
use marekvs_engine::store::{delete_partition_range, get_raw, put_raw, Store, StoreConfig};
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

/// A raw string-record key inside `pid`, bypassing key routing so the test
/// controls which partition it lands in.
fn key_in(pid: ikey::Pid, name: &[u8]) -> Vec<u8> {
    let mut k = pid.to_be_bytes().to_vec();
    k.push(b's');
    k.extend_from_slice(name);
    k
}

/// Range deletes change stored bytes, so ondaDB gates them behind a one-way
/// capability bit. Opening must enable it, or `delete_range` fails at runtime on
/// a database that has never had it turned on.
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

/// One range delete removes every record of one partition and touches no
/// neighbouring partition. The bound arithmetic is the whole risk: an end bound
/// of `pid` instead of `pid + 1` deletes nothing, and a 4-byte encoding of
/// `pid + 1` sorts below every 2-byte key and also deletes nothing.
#[tokio::test]
async fn range_delete_drops_exactly_one_partition() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir);

    for pid in [41u16, 42, 43] {
        let s = store.clone();
        s.run(0, move |ctx| put_raw(ctx, &key_in(pid, b"key"), b"value"))
            .await;
    }

    let s = store.clone();
    s.run(0, |ctx| delete_partition_range(ctx, 42))
        .await
        .expect("range delete");

    for (pid, expect_present) in [(41u16, true), (42, false), (43, true)] {
        let s = store.clone();
        let present = s
            .run(0, move |ctx| get_raw(ctx, &key_in(pid, b"key")).is_some())
            .await;
        assert_eq!(present, expect_present, "partition {pid}");
    }
}

/// A partition purged by range delete and then re-populated (the rebalance
/// give-back path) must show the NEW records. A range delete masks at its own
/// sequence; anything written after it is above the span and survives. If this
/// ever fails, `purge_partition` cannot use range deletes at all.
#[tokio::test]
async fn records_written_after_a_purge_survive() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir);

    let s = store.clone();
    s.run(0, |ctx| put_raw(ctx, &key_in(7, b"back"), b"before"))
        .await;
    let s = store.clone();
    s.run(0, |ctx| delete_partition_range(ctx, 7))
        .await
        .unwrap();
    let s = store.clone();
    s.run(0, |ctx| put_raw(ctx, &key_in(7, b"back"), b"after"))
        .await;

    let s = store.clone();
    let got = s.run(0, |ctx| get_raw(ctx, &key_in(7, b"back"))).await;
    assert_eq!(got.as_deref(), Some(&b"after"[..]));
}

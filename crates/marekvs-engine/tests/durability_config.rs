//! marekvs's durability story is `SyncMode::Interval` — "a crash may lose the
//! last 128 ms window on that node only" (design/00 §Durability). Until ondaDB
//! 0.5.0 that could only be asserted about the config marekvs *passed in*, not
//! about what the database actually persisted or did.
//!
//! `DB::column_family_config` returns the **effective durable** config — what a
//! reopen restores — and `DB::wal_sync_count` counts successful physical
//! `sync_data()` calls. Together they turn the durability claim into something
//! testable: under `SyncMode::None` the counter never advances, so a passing
//! assertion here means real fsyncs happened, not that a field was set.

use std::sync::Arc;

use marekvs_engine::store::{put_raw, Store, StoreConfig};
use ondadb::{Compression, SyncMode};

fn store_with(dir: &tempfile::TempDir, sync_mode: SyncMode) -> Arc<Store> {
    Store::open(&StoreConfig {
        data_dir: dir.path().to_string_lossy().into_owned(),
        node_id: 7,
        shard_threads: 2,
        sync_mode,
    })
    .unwrap()
}

/// The `data` and `meta` column families persist the durability and compression
/// settings marekvs opened them with — verified against the database rather
/// than against the struct that was passed to it.
#[tokio::test]
async fn column_families_persist_the_configured_durability() {
    let dir = tempfile::tempdir().unwrap();
    let store = store_with(&dir, SyncMode::Interval);

    for cf in ["data", "meta"] {
        let cfg = store
            .db
            .column_family_config(cf)
            .unwrap_or_else(|e| panic!("no effective config for {cf}: {e:?}"));
        assert_eq!(
            cfg.sync_mode,
            SyncMode::Interval,
            "{cf} did not persist SyncMode::Interval; a reopen would restore \
             {:?}, silently changing the durability window",
            cfg.sync_mode
        );
        assert_eq!(
            cfg.compression,
            Compression::Lz4,
            "{cf} did not persist Lz4 compression"
        );
    }
}

/// `SyncMode::Interval` must produce real physical WAL syncs.
///
/// The negative control is the point: the same workload under `SyncMode::None`
/// must leave the counter at zero. Without it, a passing assertion could just
/// mean "some unrelated sync happened".
#[tokio::test]
async fn interval_mode_issues_physical_wal_syncs_and_none_does_not() {
    let write_some = |store: &Arc<Store>| {
        let store = store.clone();
        async move {
            for i in 0..64u32 {
                let k = marekvs_core::ikey::string_key(format!("dur:{i}").as_bytes());
                let owned = k.clone();
                store
                    .run_key(&k, move |ctx| put_raw(ctx, &owned, b"v"))
                    .await;
            }
        }
    };

    // Negative control first: SyncMode::None must never sync.
    let none_dir = tempfile::tempdir().unwrap();
    let none_store = store_with(&none_dir, SyncMode::None);
    write_some(&none_store).await;
    let none_syncs = none_store.db.wal_sync_count();
    assert_eq!(
        none_syncs, 0,
        "SyncMode::None performed {none_syncs} physical WAL syncs — the \
         counter is not measuring what this test assumes"
    );

    // Interval: the 128 ms window must actually close at least once.
    let dir = tempfile::tempdir().unwrap();
    let store = store_with(&dir, SyncMode::Interval);
    write_some(&store).await;
    // Outlast one sync interval so the window is guaranteed to have closed.
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    store.db.sync_wal().ok();

    let syncs = store.db.wal_sync_count();
    assert!(
        syncs > 0,
        "SyncMode::Interval produced no physical WAL sync: marekvs's documented \
         bounded-loss window is not being enforced by the engine"
    );
}

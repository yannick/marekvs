//! Periodic compaction (ondaDB 0.9.0 feature 0.3).
//!
//! marekvs has two sources of dead-but-resident data that only compaction
//! removes: gc_grace tombstones, and collection elements shadowed by a head
//! tombstone's `del_hlc` (design/02) — deleting a collection is O(1) writes
//! precisely because the elements are left in place and masked. Neither is
//! reclaimed on an idle family, because a size trigger never fires when nothing
//! is being written. `periodic_compaction_interval` revisits tables by age so
//! it does.

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

/// The interval is part of the durable column-family config, so a reopen must
/// restore it — otherwise idle reclamation silently stops after the first
/// restart, which is exactly when nobody would look for it.
///
/// Both column families are asserted: `meta` is small, but a family that never
/// reclaims its tombstones is a slow leak either way.
#[tokio::test]
async fn periodic_interval_is_configured_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();

    {
        let store = store(&dir);
        for cf in ["data", "meta"] {
            let cfg = store.db.column_family_config(cf).unwrap();
            assert_eq!(
                cfg.periodic_compaction_interval,
                Duration::from_secs(86_400),
                "{cf} did not take the default periodic interval"
            );
        }
    }

    let store = store(&dir);
    for cf in ["data", "meta"] {
        let cfg = store.db.column_family_config(cf).unwrap();
        assert_eq!(
            cfg.periodic_compaction_interval,
            Duration::from_secs(86_400),
            "{cf} lost its periodic interval across a reopen"
        );
    }
}

/// Periodic compaction rewrites tables by age, which is a format-visible
/// behaviour gated on `CAP_PERIODIC_AGE` (it needs a durable
/// `SstMeta::last_compaction_time`). Without the bit the interval is a setting
/// with nothing behind it.
#[tokio::test]
async fn open_enables_the_periodic_age_capability() {
    let dir = tempfile::tempdir().unwrap();
    let store = store(&dir);
    assert_ne!(
        store.db.format_capabilities() & ondadb::format::CAP_PERIODIC_AGE,
        0,
        "CAP_PERIODIC_AGE must be enabled at open"
    );
}

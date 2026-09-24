//! 0.3.5 moved the ondaDB database from `<data_dir>` into `<data_dir>/db`, so
//! that ondaDB can swap it by rename when `<data_dir>` is a volume mount root.
//! A 0.3.4 data directory must come up with every record intact and the move
//! must happen exactly once. (A clean close flushes the memtable, so this test
//! moves tables only; a WAL left by a crash is exercised by the release's
//! kill -9 upgrade trial against real binaries.)

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use marekvs_engine::store::{layout::DB_SUBDIR, Store, StoreConfig};
use ondadb::SyncMode;

fn open(dir: &Path) -> Arc<Store> {
    Store::open(&StoreConfig {
        data_dir: dir.to_string_lossy().into_owned(),
        node_id: 3,
        shard_threads: 2,
        sync_mode: SyncMode::Full,
    })
    .unwrap()
}

fn key(i: u32) -> Vec<u8> {
    format!("layout:{i}").into_bytes()
}

/// Turn a 0.3.5 data directory back into the flat layout 0.3.4 wrote.
fn flatten(root: &Path) {
    let db = root.join(DB_SUBDIR);
    for e in std::fs::read_dir(&db).unwrap() {
        let e = e.unwrap();
        std::fs::rename(e.path(), root.join(e.file_name())).unwrap();
    }
    std::fs::remove_dir(&db).unwrap();
}

#[test]
fn new_data_directory_puts_the_database_in_db() {
    let dir = tempfile::tempdir().unwrap();
    drop(open(dir.path()));
    assert!(dir.path().join(DB_SUBDIR).join("MANIFEST").is_file());
    assert!(!dir.path().join("MANIFEST").exists());
}

#[test]
fn flat_0_3_4_layout_is_moved_with_every_record() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = open(dir.path());
        for i in 0..500 {
            store
                .db
                .put(&store.data, &key(i), b"flushed", Duration::ZERO)
                .unwrap();
        }
        store.db.flush_memtable(&store.data).unwrap();
        // Unflushed at close: `Store::drop` flushes them into a second table.
        for i in 500..600 {
            store
                .db
                .put(&store.data, &key(i), b"late", Duration::ZERO)
                .unwrap();
        }
    }
    flatten(dir.path());
    assert!(dir.path().join("MANIFEST").is_file(), "fixture is flat");

    let store = open(dir.path());
    assert!(!dir.path().join("MANIFEST").exists());
    assert!(dir.path().join(DB_SUBDIR).join("MANIFEST").is_file());
    for i in 0..600 {
        let want: &[u8] = if i < 500 { b"flushed" } else { b"late" };
        assert_eq!(store.db.get(&store.data, &key(i)).unwrap(), want, "key {i}");
    }
    let epoch = store.epoch;
    assert!(
        !store.epoch_fresh,
        "the moved database kept its epoch record"
    );
    drop(store);

    // Reopen: already migrated, nothing moves, same epoch.
    let store = open(dir.path());
    assert_eq!(store.epoch, epoch);
    assert_eq!(store.db.get(&store.data, &key(599)).unwrap(), b"late");
}

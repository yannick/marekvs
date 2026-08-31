//! `DEBUG PERFCTX` — ondaDB read-path counters (0.9.0 feature 0.10).
//!
//! The point of the feature is attribution: without it, a read-latency claim is
//! inferred from wall time and could be caused by bloom misses, block-cache
//! misses, vlog reads or L0 depth with no way to tell which. These assertions
//! therefore check that a MECHANISM fired, never an exact count — the numbers
//! are machine- and state-dependent, and a test pinning them would be pinning
//! this machine.

use std::sync::Arc;

use marekvs_engine::cmd::{server, string as string_cmd};
use marekvs_engine::reply::Reply;
use marekvs_engine::store::{Store, StoreConfig};
use marekvs_engine::Engine;
use ondadb::SyncMode;

fn engine() -> (tempfile::TempDir, Arc<Engine>) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&StoreConfig {
        data_dir: dir.path().to_string_lossy().into_owned(),
        node_id: 1,
        shard_threads: 2,
        sync_mode: SyncMode::Interval,
    })
    .unwrap();
    (dir, Engine::new(store))
}

fn a(parts: &[&[u8]]) -> Vec<Vec<u8>> {
    parts.iter().map(|p| p.to_vec()).collect()
}

/// Pull one counter out of the flat `[name, value, ...]` reply.
fn counter(reply: &Reply, name: &str) -> i64 {
    let Reply::Array(items) = reply else {
        panic!("DEBUG PERFCTX must reply with an array, got {reply:?}");
    };
    for pair in items.chunks(2) {
        if let [Reply::Bulk(k), Reply::Int(v)] = pair {
            if k == name.as_bytes() {
                return *v;
            }
        }
    }
    panic!("no counter named {name} in {reply:?}");
}

/// Reading a key that is still in the memtable must show a memtable probe and
/// no SSTable work — the cheapest possible read, and the baseline every other
/// attribution is read against.
#[tokio::test]
async fn perfctx_attributes_a_memtable_read() {
    let (_d, e) = engine();
    string_cmd::set(&e, &a(&[b"SET", b"hot", b"value"])).await;

    let r = server::debug(&e, &a(&[b"DEBUG", b"PERFCTX", b"hot"])).await;
    assert_eq!(counter(&r, "keys"), 1);
    assert!(
        counter(&r, "memtable_probes") > 0,
        "a just-written key must be found in the memtable: {r:?}"
    );
}

/// The counters must survive a flush and then show SSTable-side work, which is
/// what makes them useful for the question they exist to answer (why is a cold
/// read slow?).
#[tokio::test]
async fn perfctx_attributes_a_read_after_flush() {
    let (_d, e) = engine();
    for i in 0..200 {
        let k = format!("k{i:04}");
        string_cmd::set(&e, &a(&[b"SET", k.as_bytes(), b"value"])).await;
    }
    e.store.db.flush_memtable(&e.store.data).unwrap();

    let r = server::debug(&e, &a(&[b"DEBUG", b"PERFCTX", b"k0100"])).await;
    let probes = counter(&r, "sstable_probes") + counter(&r, "memtable_probes");
    assert!(probes > 0, "a read must probe something: {r:?}");
    // After a flush the key lives in an SSTable, so the filter for that table
    // must have been consulted. This is the assertion that would catch the
    // counters being wired up but never incremented.
    assert!(
        counter(&r, "bloom_probes") > 0,
        "a post-flush read must consult at least one bloom filter: {r:?}"
    );
}

/// A batch reports `multiget_blocks_deduped`: for each distinct block the batch
/// touches, the number of its target keys minus one.
///
/// **This is not evidence about production, and must not be read as such.** The
/// corpus here is 200 records, which is a block or two in total, so a batch
/// dedups against them almost whatever its keys are — the observed value is 3
/// out of a possible 4. It says the counter is wired up and moves; it says
/// nothing about whether a real MGET's keys share blocks.
///
/// The production question — why design/09 records MultiGet at only ~1.09x —
/// needs a corpus large enough for blocks to be selective, because marekvs
/// hashes the user key into the partition id and a batch's keys therefore
/// scatter across the keyspace. That measurement belongs on the bench box, and
/// `DEBUG PERFCTX` is now the instrument for it.
#[tokio::test]
async fn perfctx_reports_multiget_block_dedup() {
    let (_d, e) = engine();
    for i in 0..200 {
        let k = format!("k{i:04}");
        string_cmd::set(&e, &a(&[b"SET", k.as_bytes(), b"value"])).await;
    }
    e.store.db.flush_memtable(&e.store.data).unwrap();

    let r = server::debug(
        &e,
        &a(&[
            b"DEBUG", b"PERFCTX", b"k0001", b"k0002", b"k0003", b"k0004", b"k0005",
        ]),
    )
    .await;
    assert_eq!(counter(&r, "keys"), 5);
    // Print it: this is the evidence behind design/09's claim that MGET's
    // MultiGet win is small here. `counter` panics if the field is absent, so
    // reading it is itself the "is it reported" assertion.
    let deduped = counter(&r, "multiget_blocks_deduped");
    let probes = counter(&r, "sstable_probes") + counter(&r, "memtable_probes");
    println!("multiget over 5 adjacent keys: deduped={deduped}, probes={probes}");
    assert!(probes > 0, "the batch must probe something: {r:?}");
}

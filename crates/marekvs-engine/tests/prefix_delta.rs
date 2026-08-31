//! Prefix-delta data blocks (ondaDB 0.9.0 feature 2.1).
//!
//! marekvs internal keys are unusually redundant for this. Every element record
//! of one collection repeats `[pid][tag][varint klen][userkey]` and differs only
//! in its suffix (`ikey` module docs), so a hash with N fields writes that
//! prefix N times. Prefix-delta stores each data-block user key as the bytes it
//! does not share with its predecessor.
//!
//! This is a space-for-CPU trade behind a ONE-WAY capability bit, so the
//! decision to enable it by default has to be made on a measurement, not on the
//! shape of the key layout looking promising.

use std::sync::Arc;

use marekvs_core::ikey;
use marekvs_engine::store::{put_raw, Store, StoreConfig};
use ondadb::SyncMode;

fn store_at(dir: &std::path::Path) -> Arc<Store> {
    Store::open(&StoreConfig {
        data_dir: dir.to_string_lossy().into_owned(),
        node_id: 1,
        shard_threads: 2,
        sync_mode: SyncMode::Interval,
    })
    .unwrap()
}

/// Total bytes of every file under `dir`, recursively.
fn dir_bytes(dir: &std::path::Path) -> u64 {
    let mut total = 0;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in entries.flatten() {
            let Ok(ft) = e.file_type() else { continue };
            if ft.is_dir() {
                stack.push(e.path());
            } else if let Ok(m) = e.metadata() {
                total += m.len();
            }
        }
    }
    total
}

/// Write a hash-shaped corpus: `hashes` collections of `fields` members each,
/// which is the key shape prefix-delta is supposed to help most.
async fn populate(store: &Arc<Store>, hashes: usize, fields: usize) {
    for h in 0..hashes {
        let s = store.clone();
        s.run(0, move |ctx| {
            let userkey = format!("user:profile:{h:06}");
            for f in 0..fields {
                let field = format!("field-name-{f:06}");
                let k = ikey::hash_field_key(userkey.as_bytes(), field.as_bytes());
                // Incompressible values, deterministically derived. A constant
                // value (`[b'v'; 32]`) compresses to almost nothing under lz4
                // and shrinks the denominator this measurement is a percentage
                // OF, which flatters prefix-delta's share of the store.
                let mut seed = (h as u64) << 32 | f as u64 | 1;
                let mut value = [0u8; 32];
                for b in value.iter_mut() {
                    seed ^= seed >> 12;
                    seed ^= seed << 25;
                    seed ^= seed >> 27;
                    *b = (seed.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 24) as u8;
                }
                put_raw(ctx, &k, &value);
            }
        })
        .await;
    }
}

/// Measure, and print the number. The assertion is only that enabling it does
/// not make the store BIGGER — the actual ratio is the output, and it is what
/// decides whether the default should move. A test that asserted a specific
/// percentage would be pinning the corpus, not the feature.
///
/// The off-by-default case is asserted here too rather than in its own test:
/// `MAREKVS_PREFIX_DELTA_KEYS` is a process-global, so two tests in this binary
/// toggling it would race each other under cargo's thread-per-test.
#[tokio::test]
async fn prefix_delta_shrinks_a_hash_heavy_store() {
    const HASHES: usize = 100;
    const FIELDS: usize = 200;

    let plain_dir = tempfile::tempdir().unwrap();
    let delta_dir = tempfile::tempdir().unwrap();

    // `enable_prefix_delta_keys` is read from the environment inside
    // `Store::open`, and env vars are process-global — so the two stores are
    // built one after the other, never concurrently, and the variable is
    // cleared before the second.
    std::env::remove_var("MAREKVS_PREFIX_DELTA_KEYS");
    {
        let store = store_at(plain_dir.path());
        // Off by default, and the capability must stay UNCLAIMED while it is.
        // CAP_PREFIX_DELTA is one-way: burning it on every deployment for a
        // feature nobody switched on would remove the rollback path to
        // ondaDB < 0.9.0 for no benefit at all.
        assert_eq!(
            store.db.format_capabilities() & ondadb::format::CAP_PREFIX_DELTA,
            0,
            "CAP_PREFIX_DELTA must stay unclaimed while the knob is off"
        );
        populate(&store, HASHES, FIELDS).await;
        store.db.close().unwrap();
    }
    let plain = dir_bytes(plain_dir.path());

    std::env::set_var("MAREKVS_PREFIX_DELTA_KEYS", "1");
    let delta = {
        let store = store_at(delta_dir.path());
        assert_ne!(
            store.db.format_capabilities() & ondadb::format::CAP_PREFIX_DELTA,
            0,
            "the knob must claim CAP_PREFIX_DELTA"
        );
        populate(&store, HASHES, FIELDS).await;
        store.db.close().unwrap();
        dir_bytes(delta_dir.path())
    };
    std::env::remove_var("MAREKVS_PREFIX_DELTA_KEYS");

    let saved = plain as f64 - delta as f64;
    println!(
        "prefix-delta on {HASHES}x{FIELDS} hash fields: \
         plain={plain} bytes, delta={delta} bytes, saved={saved:.0} ({:.2}%)",
        saved / plain as f64 * 100.0
    );

    assert!(
        delta <= plain,
        "prefix-delta made the store larger: {delta} > {plain}"
    );
}

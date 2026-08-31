//! MGET batching (ondaDB 0.9.0 feature 0.4, `Txn::multi_get`).
//!
//! Written as a CHARACTERISATION test: it passes against the sequential
//! implementation first, so that when the inner loop is replaced by one
//! batched pass it is pinning behaviour rather than describing the new code.
//!
//! Ordering is the thing most likely to break. `mget` groups keys by shard and
//! rejoins the per-shard replies by their original index, so a batched read that
//! returns results in storage order rather than request order would produce a
//! reply that is subtly, silently wrong.

use std::sync::Arc;

use marekvs_engine::cmd::{hash, string as string_cmd};
use marekvs_engine::reply::Reply;
use marekvs_engine::store::{Store, StoreConfig};
use marekvs_engine::Engine;
use ondadb::SyncMode;

fn engine() -> (tempfile::TempDir, Arc<Engine>) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&StoreConfig {
        data_dir: dir.path().to_string_lossy().into_owned(),
        node_id: 1,
        shard_threads: 4,
        sync_mode: SyncMode::Interval,
    })
    .unwrap();
    (dir, Engine::new(store))
}

fn a(parts: &[&[u8]]) -> Vec<Vec<u8>> {
    parts.iter().map(|p| p.to_vec()).collect()
}

/// MGET over a mix of present, absent and wrong-type keys returns one reply per
/// requested key, in request order.
///
/// 64 keys is past any plausible batch threshold and spreads over all four
/// shards, so the per-shard grouping and the index rejoin are both exercised.
#[tokio::test]
async fn mget_preserves_order_across_hits_misses_and_wrong_types() {
    let (_d, e) = engine();

    // Every third key absent; every seventh a hash, which MGET reports as nil
    // rather than WRONGTYPE (matching Redis, and matching what marekvs did
    // before batching).
    let names: Vec<String> = (0..64).map(|i| format!("k{i:03}")).collect();
    for (i, name) in names.iter().enumerate() {
        if i % 3 == 0 {
            continue;
        }
        if i % 7 == 0 {
            let args = a(&[b"HSET", name.as_bytes(), b"f", b"v"]);
            hash::hset(&e, &args, false).await;
        } else {
            let val = format!("value-of-{name}");
            let args = a(&[b"SET", name.as_bytes(), val.as_bytes()]);
            string_cmd::set(&e, &args).await;
        }
    }

    let mut args: Vec<Vec<u8>> = vec![b"MGET".to_vec()];
    args.extend(names.iter().map(|n| n.as_bytes().to_vec()));
    let reply = string_cmd::mget(&e, &args).await;

    let Reply::Array(items) = reply else {
        panic!("MGET must reply with an array, got {reply:?}");
    };
    assert_eq!(items.len(), names.len(), "one reply per requested key");

    for (i, (name, item)) in names.iter().zip(items.iter()).enumerate() {
        if i % 3 == 0 || i % 7 == 0 {
            assert!(
                matches!(item, Reply::Null),
                "index {i} ({name}) should be nil, got {item:?}"
            );
        } else {
            let want = format!("value-of-{name}").into_bytes();
            assert!(
                matches!(item, Reply::Bulk(v) if *v == want),
                "index {i} ({name}) should be its own value, got {item:?}"
            );
        }
    }
}

/// A key repeated in one MGET must yield its value at every position. A batched
/// read that de-duplicates keys internally has to put the result back at each
/// requesting index, not just the first.
#[tokio::test]
async fn mget_handles_a_repeated_key() {
    let (_d, e) = engine();
    string_cmd::set(&e, &a(&[b"SET", b"dup", b"one"])).await;

    let reply = string_cmd::mget(&e, &a(&[b"MGET", b"dup", b"missing", b"dup", b"dup"])).await;
    let Reply::Array(items) = reply else {
        panic!("expected an array");
    };
    assert_eq!(items.len(), 4);
    for i in [0usize, 2, 3] {
        assert!(
            matches!(&items[i], Reply::Bulk(v) if v.as_slice() == b"one"),
            "index {i} should be the repeated key's value, got {:?}",
            items[i]
        );
    }
    assert!(matches!(items[1], Reply::Null));
}

/// Measure the batched read against the sequential one it replaced.
///
/// Ignored by default: it is a measurement, not an assertion about behaviour,
/// and a timing test in the normal suite is a flake waiting to happen. Run it
/// deliberately, in release:
///
/// ```text
/// cargo test --release -p marekvs-engine --test mget_batch -- --ignored --nocapture
/// ```
///
/// Compares `store::read_lww_batch` against N x `store::read_lww` on identical
/// key sets — the two storage paths, with no command dispatch on either side,
/// so the number attributes to the change and nothing else.
///
/// Expect a modest win rather than ondaDB's headline 2.4-3.4x. That figure comes
/// from batches whose keys share blocks; marekvs hashes the user key into the
/// partition id, so one shard's keys scatter across the keyspace and mostly land
/// in different blocks. What is left is the shared snapshot and level-walk
/// setup, paid once instead of N times.
#[tokio::test]
#[ignore]
async fn measure_batched_versus_sequential_reads() {
    use marekvs_core::ikey;
    use marekvs_engine::store::{read_lww, read_lww_batch};
    use std::time::Instant;

    const KEYS: usize = 50_000;
    const BATCH: usize = 64;
    const ROUNDS: usize = 2000;

    let (_d, e) = engine();
    for i in 0..KEYS {
        let k = format!("bench:{i:08}");
        let v = format!("value-{i:08}-padding-padding-padding");
        string_cmd::set(&e, &a(&[b"SET", k.as_bytes(), v.as_bytes()])).await;
    }

    let batches: Vec<Vec<Vec<u8>>> = (0..ROUNDS)
        .map(|r| {
            (0..BATCH)
                .map(|j| {
                    ikey::string_key(
                        format!("bench:{:08}", (r * 7919 + j * 104_729) % KEYS).as_bytes(),
                    )
                })
                .collect()
        })
        .collect();

    // Both arms run on ONE shard thread, on the same keys, warmed first — so
    // the only difference between them is batched versus sequential.
    let store = e.store.clone();
    let bs = batches.clone();
    let (seq, batched) = store
        .run(0, move |ctx| {
            for b in &bs {
                for k in b {
                    std::hint::black_box(read_lww(ctx, k, 0));
                }
                std::hint::black_box(read_lww_batch(ctx, b, 0));
            }

            let t0 = Instant::now();
            for b in &bs {
                for k in b {
                    std::hint::black_box(read_lww(ctx, k, 0));
                }
            }
            let seq = t0.elapsed();

            let t1 = Instant::now();
            for b in &bs {
                std::hint::black_box(read_lww_batch(ctx, b, 0));
            }
            let batched = t1.elapsed();
            (seq, batched)
        })
        .await;

    let ops = (ROUNDS * BATCH) as f64;
    let seq_us = seq.as_secs_f64() * 1e6 / ops;
    let batch_us = batched.as_secs_f64() * 1e6 / ops;
    println!(
        "read path over {ops} lookups (batch={BATCH}): \
         sequential {seq_us:.3} µs/op, batched {batch_us:.3} µs/op, \
         speedup {:.2}x",
        seq_us / batch_us
    );
}

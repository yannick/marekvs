//! HINCRBY as a PN counter (T2-12), end to end through the store.
//!
//! `design/02:299` documented hash fields as LWW-on-result: two nodes each
//! doing `HINCRBY h f 1` concurrently both wrote "1", the later envelope won,
//! and one increment vanished. The fix makes the field's *value* a
//! `CounterState` while the field itself stays an OR-element, so HDEL and
//! whole-hash DEL are unaffected.
//!
//! These tests drive the real `write_merged` path on two stores and then
//! exchange records the way replication does, so they cover the parts the
//! pure merge-law tests in `marekvs-core` cannot: the write path's
//! observed-dot bookkeeping, the read path's rendering, and the interaction
//! with head/delete clocks.

use std::sync::Arc;

use marekvs_core::envelope::head;
use marekvs_core::ikey;
use marekvs_engine::cmd::hash::incr_counter_field_for_test as incr;
use marekvs_engine::store::{ensure_head, get_raw, read_element, write_merged, Store, StoreConfig};

fn store(dir: &tempfile::TempDir, node_id: u16) -> Arc<Store> {
    Store::open(&StoreConfig {
        data_dir: dir.path().to_string_lossy().into_owned(),
        node_id,
        shard_threads: 2,
        ..StoreConfig::default()
    })
    .unwrap()
}

/// Copy the stored record for `field` from `from` into `to`, exactly as the
/// replication apply path does (merge, not overwrite).
async fn replicate(from: &Arc<Store>, to: &Arc<Store>, key: &[u8], field: &[u8]) {
    let ik = ikey::hash_field_key(key, field);
    let rec = {
        let ik = ik.clone();
        from.run_key(key, move |ctx| get_raw(ctx, &ik)).await
    };
    let Some(rec) = rec else { return };
    let ik2 = ik.clone();
    to.run_key(key, move |ctx| {
        write_merged(ctx, &ik2, &rec);
    })
    .await;
}

async fn read(s: &Arc<Store>, key: &[u8], field: &[u8]) -> Option<String> {
    let ik = ikey::hash_field_key(key, field);
    s.run_key(key, move |ctx| read_element(ctx, &ik, 0))
        .await
        .map(|v| String::from_utf8_lossy(&v).into_owned())
}

async fn bump(s: &Arc<Store>, key: &[u8], field: &[u8], delta: i64) -> Result<i64, String> {
    let (k, f) = (key.to_vec(), field.to_vec());
    s.run_key(key, move |ctx| {
        ensure_head(ctx, &k, head::CTYPE_HASH);
        incr(ctx, &k, &f, delta, 0).map_err(|e| e.to_string())
    })
    .await
}

/// The bug in one test: concurrent increments on two nodes must both count.
#[tokio::test]
async fn concurrent_hincrby_on_two_nodes_keeps_both_increments() {
    let (d1, d2) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (a, b) = (store(&d1, 1), store(&d2, 2));
    let (key, field) = (b"h".to_vec(), b"f".to_vec());

    assert_eq!(bump(&a, &key, &field, 1).await, Ok(1));
    assert_eq!(bump(&b, &key, &field, 1).await, Ok(1));

    replicate(&a, &b, &key, &field).await;
    replicate(&b, &a, &key, &field).await;

    assert_eq!(read(&a, &key, &field).await.as_deref(), Some("2"));
    assert_eq!(read(&b, &key, &field).await.as_deref(), Some("2"));
}

/// Many rounds of independent local increments, synced periodically: the
/// total must be exact, not merely "converged to something".
#[tokio::test]
async fn interleaved_increments_total_exactly() {
    let (d1, d2) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (a, b) = (store(&d1, 1), store(&d2, 2));
    let (key, field) = (b"h".to_vec(), b"hits".to_vec());

    let mut expected = 0i64;
    for round in 0..8 {
        for _ in 0..3 {
            bump(&a, &key, &field, 1).await.unwrap();
            bump(&b, &key, &field, 2).await.unwrap();
            expected += 3;
        }
        if round % 2 == 0 {
            replicate(&a, &b, &key, &field).await;
            replicate(&b, &a, &key, &field).await;
        }
    }
    replicate(&a, &b, &key, &field).await;
    replicate(&b, &a, &key, &field).await;

    assert_eq!(
        read(&a, &key, &field).await.as_deref(),
        Some(&*expected.to_string())
    );
    assert_eq!(
        read(&b, &key, &field).await.as_deref(),
        Some(&*expected.to_string())
    );
}

/// Decrements share the machinery — negative deltas go to the neg slots.
#[tokio::test]
async fn concurrent_increment_and_decrement_both_apply() {
    let (d1, d2) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (a, b) = (store(&d1, 1), store(&d2, 2));
    let (key, field) = (b"h".to_vec(), b"bal".to_vec());

    bump(&a, &key, &field, 100).await.unwrap();
    replicate(&a, &b, &key, &field).await;

    bump(&a, &key, &field, -30).await.unwrap();
    bump(&b, &key, &field, -20).await.unwrap();
    replicate(&a, &b, &key, &field).await;
    replicate(&b, &a, &key, &field).await;

    assert_eq!(read(&a, &key, &field).await.as_deref(), Some("50"));
    assert_eq!(read(&b, &key, &field).await.as_deref(), Some("50"));
}

/// A field written by HSET first, then incremented, adopts the parsed value
/// as the counter base — the same conversion INCR does to a string.
#[tokio::test]
async fn incrementing_a_plain_field_adopts_its_value_as_the_base() {
    let d = tempfile::tempdir().unwrap();
    let s = store(&d, 1);
    let (key, field) = (b"h".to_vec(), b"n".to_vec());

    {
        let (k, f) = (key.clone(), field.clone());
        s.run_key(&key, move |ctx| {
            ensure_head(ctx, &k, head::CTYPE_HASH);
            let rec = marekvs_core::merge::element_add(
                marekvs_core::envelope::RecordType::HashField,
                ctx.hlc.now(),
                ctx.node_id,
                b"41",
            );
            write_merged(ctx, &ikey::hash_field_key(&k, &f), &rec);
        })
        .await;
    }
    assert_eq!(read(&s, &key, &field).await.as_deref(), Some("41"));
    assert_eq!(bump(&s, &key, &field, 1).await, Ok(42));
    assert_eq!(read(&s, &key, &field).await.as_deref(), Some("42"));
}

/// A non-numeric field is still a Redis error, not a silent reset.
#[tokio::test]
async fn incrementing_a_non_integer_field_errors() {
    let d = tempfile::tempdir().unwrap();
    let s = store(&d, 1);
    let (key, field) = (b"h".to_vec(), b"s".to_vec());

    {
        let (k, f) = (key.clone(), field.clone());
        s.run_key(&key, move |ctx| {
            ensure_head(ctx, &k, head::CTYPE_HASH);
            let rec = marekvs_core::merge::element_add(
                marekvs_core::envelope::RecordType::HashField,
                ctx.hlc.now(),
                ctx.node_id,
                b"hello",
            );
            write_merged(ctx, &ikey::hash_field_key(&k, &f), &rec);
        })
        .await;
    }
    let err = bump(&s, &key, &field, 1).await.unwrap_err();
    assert!(err.contains("not an integer"), "got {err}");
    // and the field is untouched
    assert_eq!(read(&s, &key, &field).await.as_deref(), Some("hello"));
}

/// Repeated increments must not grow the record: each write covers the dots
/// it observed. A counter hammered a thousand times must stay small.
#[tokio::test]
async fn repeated_increments_do_not_grow_the_record() {
    let d = tempfile::tempdir().unwrap();
    let s = store(&d, 1);
    let (key, field) = (b"h".to_vec(), b"c".to_vec());

    for _ in 0..200 {
        bump(&s, &key, &field, 1).await.unwrap();
    }
    assert_eq!(read(&s, &key, &field).await.as_deref(), Some("200"));

    let ik = ikey::hash_field_key(&key, &field);
    let size = s
        .run_key(&key, move |ctx| get_raw(ctx, &ik).map(|v| v.len()))
        .await
        .unwrap();
    // One live entry for this node, no covered dots: the size is a constant,
    // not a function of how often the counter was hit. A per-increment dot
    // would have put this at ~2 KB (the MAX_TOMB_DOTS cap).
    assert!(
        size < 80,
        "record grew to {size} bytes over 200 increments — dots are accumulating"
    );
}

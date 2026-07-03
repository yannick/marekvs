//! Per-element list representation (design/02 §Lists): position-keyed LWW
//! element records with a per-shard head/tail hint. Exercises push/pop order,
//! index/range walks, the interior rebuild ops, and — the point of the
//! hint-as-optimization design — correctness after a reopen drops the
//! in-memory hint and every position must be recovered by a scan.

use std::sync::Arc;

use marekvs_engine::cmd::list;
use marekvs_engine::reply::Reply;
use marekvs_engine::store::{Store, StoreConfig};
use marekvs_engine::Engine;

fn engine_in(dir: &std::path::Path) -> Arc<Engine> {
    let store = Store::open(&StoreConfig {
        data_dir: dir.to_string_lossy().into_owned(),
        node_id: 3,
        shard_threads: 2,
        ..StoreConfig::default()
    })
    .unwrap();
    Engine::new(store)
}

fn a(parts: &[&[u8]]) -> Vec<Vec<u8>> {
    parts.iter().map(|p| p.to_vec()).collect()
}

fn int(r: Reply) -> i64 {
    match r {
        Reply::Int(n) => n,
        other => panic!("expected Int, got {other:?}"),
    }
}

fn bulk(r: Reply) -> Option<Vec<u8>> {
    match r {
        Reply::Bulk(v) => Some(v),
        Reply::Null => None,
        other => panic!("expected Bulk|Null, got {other:?}"),
    }
}

fn arr(r: Reply) -> Vec<Vec<u8>> {
    match r {
        Reply::Array(items) => items
            .into_iter()
            .map(|i| match i {
                Reply::Bulk(v) => v,
                other => panic!("expected Bulk in array, got {other:?}"),
            })
            .collect(),
        Reply::NullArray => Vec::new(),
        other => panic!("expected Array, got {other:?}"),
    }
}

fn s(v: &[u8]) -> String {
    String::from_utf8_lossy(v).into_owned()
}

fn strs(v: Vec<Vec<u8>>) -> Vec<String> {
    v.iter().map(|x| s(x)).collect()
}

// LPUSH prepends each value; RPUSH appends. Verify the resulting order via
// LRANGE over the position-keyed records.
#[tokio::test]
async fn push_order_and_range() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine_in(dir.path());

    assert_eq!(
        int(list::push(&e, &a(&[b"RPUSH", b"l", b"a", b"b", b"c"]), false, false).await),
        3
    );
    // LPUSH x y z  →  z y x a b c
    assert_eq!(
        int(list::push(&e, &a(&[b"LPUSH", b"l", b"x", b"y", b"z"]), true, false).await),
        6
    );

    let r = arr(list::lrange(&e, &a(&[b"LRANGE", b"l", b"0", b"-1"])).await);
    assert_eq!(strs(r), ["z", "y", "x", "a", "b", "c"]);

    assert_eq!(int(list::llen(&e, &a(&[b"LLEN", b"l"])).await), 6);

    // Bounded (non-negative) range takes the fast walk path.
    let mid = arr(list::lrange(&e, &a(&[b"LRANGE", b"l", b"1", b"3"])).await);
    assert_eq!(strs(mid), ["y", "x", "a"]);
    // Negative bounds resolve against the length.
    let tail = arr(list::lrange(&e, &a(&[b"LRANGE", b"l", b"-2", b"-1"])).await);
    assert_eq!(strs(tail), ["b", "c"]);
    // Empty selection.
    assert!(arr(list::lrange(&e, &a(&[b"LRANGE", b"l", b"5", b"2"])).await).is_empty());
}

#[tokio::test]
async fn pop_both_ends() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine_in(dir.path());
    list::push(
        &e,
        &a(&[b"RPUSH", b"q", b"a", b"b", b"c", b"d"]),
        false,
        false,
    )
    .await;

    assert_eq!(
        bulk(list::pop(&e, &a(&[b"LPOP", b"q"]), true).await).map(|v| s(&v)),
        Some("a".into())
    );
    assert_eq!(
        bulk(list::pop(&e, &a(&[b"RPOP", b"q"]), false).await).map(|v| s(&v)),
        Some("d".into())
    );
    // LPOP with a count.
    let two = arr(list::pop(&e, &a(&[b"LPOP", b"q", b"2"]), true).await);
    assert_eq!(strs(two), ["b", "c"]);
    // Now empty: single pop → Null, TYPE none.
    assert_eq!(bulk(list::pop(&e, &a(&[b"LPOP", b"q"]), true).await), None);
    assert_eq!(int(list::llen(&e, &a(&[b"LLEN", b"q"])).await), 0);
}

// Reopen drops the in-memory position hint; the elements must still be found
// (min/max recovered by scan) and pops/pushes continue correctly.
#[tokio::test]
async fn survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let e = engine_in(dir.path());
        list::push(&e, &a(&[b"RPUSH", b"k", b"1", b"2", b"3"]), false, false).await;
        list::pop(&e, &a(&[b"LPOP", b"k"]), true).await; // drop "1"
                                                         // give the shard threads a moment to flush is unnecessary: writes are
                                                         // synchronous through run_key before the await returns.
    }
    // Fresh process image over the same data dir: no hint in memory.
    let e = engine_in(dir.path());
    let r = arr(list::lrange(&e, &a(&[b"LRANGE", b"k", b"0", b"-1"])).await);
    assert_eq!(strs(r), ["2", "3"]);
    assert_eq!(int(list::llen(&e, &a(&[b"LLEN", b"k"])).await), 2);
    // Push after reopen keeps ordering (tail recovered by scan).
    list::push(&e, &a(&[b"RPUSH", b"k", b"4"]), false, false).await;
    list::push(&e, &a(&[b"LPUSH", b"k", b"0"]), true, false).await;
    let r = arr(list::lrange(&e, &a(&[b"LRANGE", b"k", b"0", b"-1"])).await);
    assert_eq!(strs(r), ["0", "2", "3", "4"]);
}

// DEL tombstones the head; the list reads empty and can be recreated. A push
// after DEL is gated above the delete clock and starts a fresh sequence.
#[tokio::test]
async fn del_gates_then_recreate() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine_in(dir.path());
    list::push(&e, &a(&[b"RPUSH", b"d", b"a", b"b", b"c"]), false, false).await;

    // DEL via the generic path (head tombstone).
    let deleted = e
        .store
        .run_key(b"d", move |ctx| {
            marekvs_engine::cmd::generic::del_key(ctx, b"d")
        })
        .await;
    assert!(deleted);

    assert_eq!(int(list::llen(&e, &a(&[b"LLEN", b"d"])).await), 0);
    assert!(arr(list::lrange(&e, &a(&[b"LRANGE", b"d", b"0", b"-1"])).await).is_empty());

    // Recreate: fresh elements are visible over the carried-forward clock.
    assert_eq!(
        int(list::push(&e, &a(&[b"RPUSH", b"d", b"x", b"y"]), false, false).await),
        2
    );
    let r = arr(list::lrange(&e, &a(&[b"LRANGE", b"d", b"0", b"-1"])).await);
    assert_eq!(strs(r), ["x", "y"]);
}

#[tokio::test]
async fn lindex_lset_lpos() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine_in(dir.path());
    list::push(
        &e,
        &a(&[b"RPUSH", b"l", b"a", b"b", b"c", b"b", b"e"]),
        false,
        false,
    )
    .await;

    assert_eq!(
        bulk(list::lindex(&e, &a(&[b"LINDEX", b"l", b"0"])).await).map(|v| s(&v)),
        Some("a".into())
    );
    assert_eq!(
        bulk(list::lindex(&e, &a(&[b"LINDEX", b"l", b"-1"])).await).map(|v| s(&v)),
        Some("e".into())
    );
    assert_eq!(
        bulk(list::lindex(&e, &a(&[b"LINDEX", b"l", b"10"])).await),
        None
    );

    // LSET keeps positions stable (in-place overwrite).
    assert_eq!(
        list::lset(&e, &a(&[b"LSET", b"l", b"2", b"C"])).await,
        Reply::Simple("OK")
    );
    assert_eq!(
        bulk(list::lindex(&e, &a(&[b"LINDEX", b"l", b"2"])).await).map(|v| s(&v)),
        Some("C".into())
    );

    // LPOS: first "b" is at index 1; all matches; from tail.
    assert_eq!(int(list::lpos(&e, &a(&[b"LPOS", b"l", b"b"])).await), 1);
    let all = match list::lpos(&e, &a(&[b"LPOS", b"l", b"b", b"COUNT", b"0"])).await {
        Reply::Array(v) => v.into_iter().map(int).collect::<Vec<_>>(),
        o => panic!("{o:?}"),
    };
    assert_eq!(all, vec![1, 3]);
    assert_eq!(
        int(list::lpos(&e, &a(&[b"LPOS", b"l", b"b", b"RANK", b"-1"])).await),
        3
    );
}

#[tokio::test]
async fn interior_rebuild_ops() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine_in(dir.path());

    // LREM
    list::push(
        &e,
        &a(&[b"RPUSH", b"r", b"a", b"b", b"a", b"c", b"a"]),
        false,
        false,
    )
    .await;
    assert_eq!(
        int(list::lrem(&e, &a(&[b"LREM", b"r", b"2", b"a"])).await),
        2
    );
    assert_eq!(
        strs(arr(
            list::lrange(&e, &a(&[b"LRANGE", b"r", b"0", b"-1"])).await
        )),
        ["b", "c", "a"]
    );
    // After rebuild, pushes still extend both ends correctly.
    list::push(&e, &a(&[b"LPUSH", b"r", b"HEAD"]), true, false).await;
    list::push(&e, &a(&[b"RPUSH", b"r", b"TAIL"]), false, false).await;
    assert_eq!(
        strs(arr(
            list::lrange(&e, &a(&[b"LRANGE", b"r", b"0", b"-1"])).await
        )),
        ["HEAD", "b", "c", "a", "TAIL"]
    );

    // LTRIM
    list::push(
        &e,
        &a(&[b"RPUSH", b"t", b"0", b"1", b"2", b"3", b"4"]),
        false,
        false,
    )
    .await;
    assert_eq!(
        list::ltrim(&e, &a(&[b"LTRIM", b"t", b"1", b"3"])).await,
        Reply::Simple("OK")
    );
    assert_eq!(
        strs(arr(
            list::lrange(&e, &a(&[b"LRANGE", b"t", b"0", b"-1"])).await
        )),
        ["1", "2", "3"]
    );

    // LINSERT
    list::push(&e, &a(&[b"RPUSH", b"i", b"a", b"c"]), false, false).await;
    assert_eq!(
        int(list::linsert(&e, &a(&[b"LINSERT", b"i", b"BEFORE", b"c", b"b"])).await),
        3
    );
    assert_eq!(
        strs(arr(
            list::lrange(&e, &a(&[b"LRANGE", b"i", b"0", b"-1"])).await
        )),
        ["a", "b", "c"]
    );
    assert_eq!(
        int(list::linsert(&e, &a(&[b"LINSERT", b"i", b"AFTER", b"c", b"d"])).await),
        4
    );
    assert_eq!(
        strs(arr(
            list::lrange(&e, &a(&[b"LRANGE", b"i", b"0", b"-1"])).await
        )),
        ["a", "b", "c", "d"]
    );
    // Missing pivot → -1.
    assert_eq!(
        int(list::linsert(&e, &a(&[b"LINSERT", b"i", b"BEFORE", b"zzz", b"q"])).await),
        -1
    );
}

#[tokio::test]
async fn move_ops() {
    let dir = tempfile::tempdir().unwrap();
    let e = engine_in(dir.path());
    list::push(&e, &a(&[b"RPUSH", b"src", b"a", b"b", b"c"]), false, false).await;

    // RPOPLPUSH src dst: move "c" to head of dst.
    assert_eq!(
        bulk(list::rpoplpush(&e, &a(&[b"RPOPLPUSH", b"src", b"dst"])).await).map(|v| s(&v)),
        Some("c".into())
    );
    assert_eq!(
        strs(arr(
            list::lrange(&e, &a(&[b"LRANGE", b"dst", b"0", b"-1"])).await
        )),
        ["c"]
    );
    assert_eq!(
        strs(arr(
            list::lrange(&e, &a(&[b"LRANGE", b"src", b"0", b"-1"])).await
        )),
        ["a", "b"]
    );

    // Same-key rotation: RPOPLPUSH src src moves tail to head.
    assert_eq!(
        bulk(list::rpoplpush(&e, &a(&[b"RPOPLPUSH", b"src", b"src"])).await).map(|v| s(&v)),
        Some("b".into())
    );
    assert_eq!(
        strs(arr(
            list::lrange(&e, &a(&[b"LRANGE", b"src", b"0", b"-1"])).await
        )),
        ["b", "a"]
    );
}

//! PROTO.* field-level CRDT convergence (design/18): two independent engines
//! edit the same decomposed value concurrently, exchange records in both
//! delivery orders, and must converge with every non-conflicting edit
//! surviving — the data-loss case whole-message LWW could not handle.

use std::sync::Arc;

use marekvs_core::envelope::{head, Envelope};
use marekvs_core::{ikey, protohead};
use marekvs_engine::cmd::{generic, proto as proto_cmd};
use marekvs_engine::reply::Reply;
use marekvs_engine::store::{self, Store, StoreConfig};
use marekvs_engine::Engine;

const REC_SRC: &str = r#"
    syntax = "proto3";
    package t.v1;
    message Rec {
        string a = 1;
        int32 b = 2;
        repeated string tags = 3;
        map<string, int32> m = 4;
        oneof choice { string label = 5; int32 code = 6; }
    }
"#;

fn a(parts: &[&[u8]]) -> Vec<Vec<u8>> {
    parts.iter().map(|p| p.to_vec()).collect()
}

fn open_engine(dir: &tempfile::TempDir, node_id: u16) -> Arc<Engine> {
    let store = Store::open(&StoreConfig {
        data_dir: dir.path().to_string_lossy().into_owned(),
        node_id,
        shard_threads: 2,
        ..StoreConfig::default()
    })
    .unwrap();
    Engine::new(store)
}

/// A fresh engine with the `recs` schema registered and `rec:` bound.
async fn setup(node: u16) -> (tempfile::TempDir, Arc<Engine>) {
    let dir = tempfile::tempdir().unwrap();
    let e = open_engine(&dir, node);
    let r = proto_cmd::schema(
        &e,
        &a(&[
            b"PROTO.SCHEMA",
            b"SET",
            b"recs",
            b"SOURCE",
            REC_SRC.as_bytes(),
        ]),
    )
    .await;
    assert_eq!(r, Reply::Int(1));
    let r = proto_cmd::bind(&e, &a(&[b"PROTO.BIND", b"rec:", b"t.v1.Rec"])).await;
    assert_eq!(r, Reply::Simple("OK"));
    (dir, e)
}

/// Encode a `t.v1.Rec` message directly (for fmt=1 legacy fixtures).
fn rec_bytes(build: impl FnOnce(&mut prost_reflect::DynamicMessage)) -> Vec<u8> {
    use prost::Message;
    let out = marekvs_engine::proto::compile::compile_source(
        "recs",
        REC_SRC,
        Default::default(),
        &marekvs_engine::proto::ProtoLimits::from_env(),
    )
    .unwrap();
    let pool = marekvs_engine::proto::compile::pool_from_fds(&out.fds).unwrap();
    let desc = pool.get_message_by_name("t.v1.Rec").unwrap();
    let mut m = prost_reflect::DynamicMessage::new(desc);
    build(&mut m);
    m.encode_to_vec()
}

/// Seed a pre-design/18 whole-message (fmt=1) head directly, with a delete
/// clock and TTL deadline, so upgrade-on-write preservation can be checked.
async fn seed_fmt1(e: &Arc<Engine>, key: &[u8], msg: &[u8], del_hlc: u64, ttl_deadline_ms: u64) {
    let (k, msg) = (key.to_vec(), msg.to_vec());
    e.store
        .run_key(&k.clone(), move |ctx| {
            let mut payload = head::encode(head::CTYPE_PROTO, del_hlc);
            payload.extend_from_slice(&protohead::encode("recs", 1, "t.v1.Rec", &msg));
            let env = Envelope::head(ctx.hlc.now(), ctx.node_id).with_ttl(ttl_deadline_ms);
            store::write_merged(ctx, &ikey::head_key(&k), &env.encode_with(&payload));
        })
        .await;
}

/// Copy every proto record of `key` (head + `'p'` field records) from `src`
/// to `dst`, applying them through `write_merged` in the given delivery order.
async fn replicate_proto(src: &Arc<Engine>, dst: &Arc<Engine>, key: &[u8], reverse: bool) {
    let k = key.to_vec();
    let mut records: Vec<(Vec<u8>, Vec<u8>)> = src
        .store
        .run_key(&k.clone(), move |ctx| {
            let mut out = Vec::new();
            if let Some(h) = store::get_raw(ctx, &ikey::head_key(&k)) {
                out.push((ikey::head_key(&k), h));
            }
            store::scan_prefix(
                ctx,
                &ikey::collection_prefix(ikey::Tag::ProtoField, &k),
                |ik, v| {
                    out.push((ik.to_vec(), v.to_vec()));
                    true
                },
            );
            out
        })
        .await;
    if reverse {
        records.reverse();
    }
    let key = key.to_vec();
    dst.store
        .run_key(&key, move |ctx| {
            for (ik, v) in records {
                store::write_merged(ctx, &ik, &v);
            }
        })
        .await;
}

/// Full three-way exchange: both replicas end up having seen every record.
async fn exchange(e1: &Arc<Engine>, e2: &Arc<Engine>, key: &[u8]) {
    replicate_proto(e1, e2, key, false).await;
    replicate_proto(e2, e1, key, true).await;
    replicate_proto(e1, e2, key, false).await;
}

async fn gf(e: &Arc<Engine>, key: &[u8], path: &[u8]) -> Reply {
    proto_cmd::getfield(e, &a(&[b"PROTO.GETFIELD", key, path])).await
}

async fn getjson(e: &Arc<Engine>, key: &[u8]) -> serde_json::Value {
    match proto_cmd::getjson(e, &a(&[b"PROTO.GETJSON", key])).await {
        Reply::Bulk(b) => serde_json::from_slice(&b).unwrap(),
        other => panic!("expected Bulk json, got {other:?}"),
    }
}

fn ok(r: Reply) {
    assert_eq!(r, Reply::Simple("OK"));
}

async fn setjson(e: &Arc<Engine>, key: &[u8], json: &[u8]) {
    ok(proto_cmd::setjson(e, &a(&[b"PROTO.SETJSON", key, json])).await);
}

async fn setfield(e: &Arc<Engine>, key: &[u8], path: &[u8], val: &[u8]) {
    ok(proto_cmd::setfield(e, &a(&[b"PROTO.SETFIELD", key, path, val])).await);
}

fn tags_of(v: &serde_json::Value) -> Vec<String> {
    v.get("tags")
        .and_then(|t| t.as_array())
        .map(|a| a.iter().map(|s| s.as_str().unwrap().to_string()).collect())
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// headline: concurrent SETFIELD on different fields both survive
// ---------------------------------------------------------------------------

#[tokio::test]
async fn concurrent_setfield_different_fields_converge_both_orders() {
    for reverse_second in [false, true] {
        let (_d1, e1) = setup(1).await;
        let (_d2, e2) = setup(2).await;
        setjson(&e1, b"rec:1", br#"{"a":"init"}"#).await;
        replicate_proto(&e1, &e2, b"rec:1", false).await;

        // concurrent edits to DISJOINT fields — the whole-message-LWW loss case
        setfield(&e1, b"rec:1", b"a", b"from-1").await;
        setfield(&e2, b"rec:1", b"b", b"42").await;

        // deliver in the chosen order, then reconcile
        replicate_proto(&e1, &e2, b"rec:1", false).await;
        replicate_proto(&e2, &e1, b"rec:1", reverse_second).await;
        replicate_proto(&e1, &e2, b"rec:1", false).await;

        for e in [&e1, &e2] {
            assert_eq!(gf(e, b"rec:1", b"a").await, Reply::Bulk(b"from-1".to_vec()));
            assert_eq!(gf(e, b"rec:1", b"b").await, Reply::Int(42));
        }
        assert_eq!(getjson(&e1, b"rec:1").await, getjson(&e2, b"rec:1").await);
    }
}

#[tokio::test]
async fn concurrent_same_field_is_lww() {
    let (_d1, e1) = setup(1).await;
    let (_d2, e2) = setup(2).await;
    setjson(&e1, b"rec:1", br#"{"a":"init"}"#).await;
    replicate_proto(&e1, &e2, b"rec:1", false).await;

    setfield(&e1, b"rec:1", b"a", b"one").await;
    setfield(&e2, b"rec:1", b"a", b"two").await; // e2 issued later → higher HLC

    exchange(&e1, &e2, b"rec:1").await;
    // same value on both, and it is exactly one of the two writes
    let v1 = gf(&e1, b"rec:1", b"a").await;
    assert_eq!(v1, gf(&e2, b"rec:1", b"a").await);
    assert!(matches!(&v1, Reply::Bulk(b) if b == b"one" || b == b"two"));
}

#[tokio::test]
async fn concurrent_repeated_appends_stay_contiguous() {
    let (_d1, e1) = setup(1).await;
    let (_d2, e2) = setup(2).await;
    setjson(&e1, b"rec:1", br#"{"tags":["x"]}"#).await;
    replicate_proto(&e1, &e2, b"rec:1", false).await;

    // each side appends a two-element run (index == len appends)
    setfield(&e1, b"rec:1", b"tags.1", b"n1a").await;
    setfield(&e1, b"rec:1", b"tags.2", b"n1b").await;
    setfield(&e2, b"rec:1", b"tags.1", b"n2a").await;
    setfield(&e2, b"rec:1", b"tags.2", b"n2b").await;

    exchange(&e1, &e2, b"rec:1").await;
    let t1 = tags_of(&getjson(&e1, b"rec:1").await);
    let t2 = tags_of(&getjson(&e2, b"rec:1").await);
    assert_eq!(t1, t2, "replicas diverged: {t1:?} vs {t2:?}");
    assert_eq!(t1.len(), 5, "all appends present: {t1:?}");
    assert_eq!(t1[0], "x");
    // each concurrent run is contiguous (RGA left-anchoring)
    let p1 = t1.iter().position(|s| s == "n1a").unwrap();
    assert_eq!(t1[p1 + 1], "n1b", "run n1a/n1b split: {t1:?}");
    let p2 = t1.iter().position(|s| s == "n2a").unwrap();
    assert_eq!(t1[p2 + 1], "n2b", "run n2a/n2b split: {t1:?}");
}

#[tokio::test]
async fn concurrent_map_disjoint_keys_and_same_key_add_wins() {
    // disjoint keys: both survive
    let (_d1, e1) = setup(1).await;
    let (_d2, e2) = setup(2).await;
    setjson(&e1, b"rec:1", br#"{"a":"init"}"#).await;
    replicate_proto(&e1, &e2, b"rec:1", false).await;
    setfield(&e1, b"rec:1", b"m.k1", b"1").await;
    setfield(&e2, b"rec:1", b"m.k2", b"2").await;
    exchange(&e1, &e2, b"rec:1").await;
    for e in [&e1, &e2] {
        assert_eq!(gf(e, b"rec:1", b"m.k1").await, Reply::Int(1));
        assert_eq!(gf(e, b"rec:1", b"m.k2").await, Reply::Int(2));
    }

    // same key: clear vs concurrent set → add-wins (the set survives)
    setfield(&e1, b"rec:1", b"m.k", b"1").await;
    exchange(&e1, &e2, b"rec:1").await;
    proto_cmd::clearfield(&e1, &a(&[b"PROTO.CLEARFIELD", b"rec:1", b"m.k"])).await;
    setfield(&e2, b"rec:1", b"m.k", b"9").await; // observed the old add, installs fresh
    exchange(&e1, &e2, b"rec:1").await;
    assert_eq!(
        gf(&e1, b"rec:1", b"m.k").await,
        gf(&e2, b"rec:1", b"m.k").await
    );
    assert_eq!(gf(&e1, b"rec:1", b"m.k").await, Reply::Int(9), "add-wins");
}

#[tokio::test]
async fn oneof_race_converges_identically_both_orders() {
    let mut winners = Vec::new();
    for reverse_second in [false, true] {
        let (_d1, e1) = setup(1).await;
        let (_d2, e2) = setup(2).await;
        setjson(&e1, b"rec:1", br#"{"a":"init"}"#).await;
        replicate_proto(&e1, &e2, b"rec:1", false).await;

        setfield(&e1, b"rec:1", b"label", b"L").await; // member 5
        setfield(&e2, b"rec:1", b"code", b"7").await; // member 6

        replicate_proto(&e1, &e2, b"rec:1", false).await;
        replicate_proto(&e2, &e1, b"rec:1", reverse_second).await;
        replicate_proto(&e1, &e2, b"rec:1", false).await;

        let v1 = getjson(&e1, b"rec:1").await;
        let v2 = getjson(&e2, b"rec:1").await;
        assert_eq!(v1, v2, "oneof replicas diverged");
        // exactly one member is live
        let has_label = v1.get("label").is_some();
        let has_code = v1.get("code").is_some();
        assert!(has_label ^ has_code, "oneof must have one winner: {v1}");
        winners.push(v1);
    }
    // deterministic across delivery orders
    assert_eq!(winners[0], winners[1], "oneof winner depends on order");
}

// ---------------------------------------------------------------------------
// fmt=1 → fmt=2 upgrade under concurrency
// ---------------------------------------------------------------------------

#[tokio::test]
async fn concurrent_upgrade_from_shared_fmt1_no_duplication() {
    let (_d1, e1) = setup(1).await;
    let (_d2, e2) = setup(2).await;
    // a shared legacy value with two repeated elements, a TTL and a del clock
    let msg = rec_bytes(|m| {
        m.set_field_by_name("a", prost_reflect::Value::String("legacy".into()));
        m.set_field_by_name(
            "tags",
            prost_reflect::Value::List(vec![
                prost_reflect::Value::String("x".into()),
                prost_reflect::Value::String("y".into()),
            ]),
        );
    });
    let ttl = marekvs_engine::store::now_ms() + 100_000;
    seed_fmt1(&e1, b"rec:1", &msg, 4242, ttl).await;
    // ship the identical fmt=1 head to e2 (byte-identical legacy state)
    replicate_proto(&e1, &e2, b"rec:1", false).await;

    // both upgrade-on-write concurrently, editing DIFFERENT fields
    setfield(&e1, b"rec:1", b"a", b"edit-1").await;
    setfield(&e2, b"rec:1", b"b", b"7").await;

    exchange(&e1, &e2, b"rec:1").await;

    for e in [&e1, &e2] {
        // both edits survived the upgrade
        assert_eq!(gf(e, b"rec:1", b"a").await, Reply::Bulk(b"edit-1".to_vec()));
        assert_eq!(gf(e, b"rec:1", b"b").await, Reply::Int(7));
        // repeated field transcribed once — NOT duplicated across the two
        // independent upgrades (identical derived element ids)
        let t = tags_of(&getjson(e, b"rec:1").await);
        assert_eq!(
            t,
            vec!["x", "y"],
            "repeated run duplicated on upgrade: {t:?}"
        );
    }
    assert_eq!(getjson(&e1, b"rec:1").await, getjson(&e2, b"rec:1").await);

    // both are decomposed now, TTL + del clock preserved through the upgrade
    let info = proto_cmd::info(&e1, &a(&[b"PROTO.INFO", b"rec:1"])).await;
    match info {
        Reply::Map(pairs) => {
            let fmt = pairs
                .iter()
                .find_map(|(k, v)| matches!(k, Reply::Bulk(b) if b == b"format").then_some(v));
            assert_eq!(fmt, Some(&Reply::bulk_str("fields")));
        }
        other => panic!("expected Map, got {other:?}"),
    }
    let ttl_left = match generic::ttl(&e1, &a(&[b"TTL", b"rec:1"]), false).await {
        Reply::Int(n) => n,
        other => panic!("expected Int ttl, got {other:?}"),
    };
    assert!(
        ttl_left > 90 && ttl_left <= 100,
        "TTL not preserved: {ttl_left}"
    );
    // del clock survived: read the head and decode its del_hlc
    let del = e1
        .store
        .run_key(b"rec:1", |ctx| {
            let raw = store::get_raw(ctx, &ikey::head_key(b"rec:1")).unwrap();
            let (_, pay) = Envelope::decode(&raw).unwrap();
            head::decode(pay).unwrap().1
        })
        .await;
    assert!(
        del >= 4242,
        "delete clock not preserved across upgrade: {del}"
    );
}

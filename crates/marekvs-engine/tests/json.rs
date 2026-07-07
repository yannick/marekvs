//! JSON engine integration tests (design/16). Phase A2 covers keyspace
//! plumbing (TYPE/OBJECT/EXISTS/DEL/EXPIRE/RENAME/COPY) over hand-minted
//! records; later phases add the JSON.* command matrix.

use std::sync::Arc;

use marekvs_core::envelope::{head, Envelope, RecordType};
use marekvs_core::ikey;
use marekvs_core::json::{
    build_doc, decode_path, decompose, ArrElem, Eid, JVal, JsonRecord, NodeIn, Seg,
};
use marekvs_core::merge::{element_set, ElementState};
use marekvs_engine::cmd::generic;
use marekvs_engine::reply::Reply;
use marekvs_engine::store::{self, Store, StoreConfig};
use marekvs_engine::Engine;

fn engine() -> (tempfile::TempDir, Arc<Engine>) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&StoreConfig {
        data_dir: dir.path().to_string_lossy().into_owned(),
        node_id: 7,
        shard_threads: 2,
        ..StoreConfig::default()
    })
    .unwrap();
    (dir, Engine::new(store))
}

fn a(parts: &[&[u8]]) -> Vec<Vec<u8>> {
    parts.iter().map(|p| p.to_vec()).collect()
}

/// Mint a JSON document at `key` by decomposing `v` and writing the records
/// the way the (not yet existing) JSON.SET root handler will: a head with
/// CTYPE_JSON plus per-path records.
async fn mint_doc(e: &Arc<Engine>, key: &[u8], v: serde_json::Value) {
    let key = key.to_vec();
    e.store
        .run_key(&key.clone(), move |ctx| {
            let env = Envelope::head(ctx.hlc.now(), ctx.node_id);
            store::write_merged(
                ctx,
                &ikey::head_key(&key),
                &env.encode_with(&head::encode(head::CTYPE_JSON, 0)),
            );
            let mut fresh = || Eid {
                hlc: ctx.hlc.now(),
                origin: ctx.node_id,
            };
            for rec in decompose(&[], &v, &mut fresh) {
                match rec {
                    JsonRecord::Map { path, val } => {
                        let rec = element_set(
                            RecordType::HashField,
                            ctx.hlc.now(),
                            ctx.node_id,
                            &val.encode(),
                            &[],
                        );
                        store::write_merged(ctx, &ikey::json_node_key(&key, &path), &rec);
                    }
                    JsonRecord::Arr { path, elem } => {
                        let rec = Envelope::new(RecordType::List, ctx.hlc.now(), ctx.node_id)
                            .encode_with(&elem.encode());
                        store::write_merged(ctx, &ikey::json_node_key(&key, &path), &rec);
                    }
                }
            }
        })
        .await;
}

/// Materialize the stored document (mirrors the engine's load path).
async fn read_doc(e: &Arc<Engine>, key: &[u8]) -> Option<serde_json::Value> {
    let key = key.to_vec();
    e.store
        .run_key(&key.clone(), move |ctx| {
            let (env, ctype, del) = store::get_head(ctx, &key)?;
            if env.is_tombstone() || ctype != head::CTYPE_JSON {
                return None;
            }
            let mut nodes: Vec<(Vec<u8>, NodeIn)> = Vec::new();
            let now = store::now_ms();
            store::scan_prefix(
                ctx,
                &ikey::collection_prefix(ikey::Tag::Json, &key),
                |k, v| {
                    let (p, (env, pay)) = match (ikey::parse(k), Envelope::decode(v)) {
                        (Some(p), Some(d)) => (p, d),
                        _ => return true,
                    };
                    if env.hlc <= del || env.is_expired(now) {
                        return true;
                    }
                    let is_elem = matches!(
                        decode_path(p.suffix).and_then(|mut s| s.pop()),
                        Some(Seg::Elem(_))
                    );
                    if is_elem {
                        if let Some(elem) = ArrElem::decode(pay) {
                            nodes.push((
                                p.suffix.to_vec(),
                                NodeIn::ArrElem {
                                    elem,
                                    live: !env.is_tombstone(),
                                },
                            ));
                        }
                    } else if !env.is_tombstone() {
                        if let Some(st) = ElementState::decode(pay) {
                            if let Some(vb) = st.value() {
                                if let Some(val) = JVal::decode(vb) {
                                    nodes.push((
                                        p.suffix.to_vec(),
                                        NodeIn::Map {
                                            val,
                                            dots: st.dots(),
                                        },
                                    ));
                                }
                            }
                        }
                    }
                    true
                },
            );
            build_doc(&nodes).map(|d| d.value)
        })
        .await
}

#[tokio::test]
async fn type_object_exists_del_lifecycle() {
    let (_d, e) = engine();
    mint_doc(&e, b"doc", serde_json::json!({"a": 1, "tags": ["x"]})).await;

    assert_eq!(
        generic::type_cmd(&e, &a(&[b"TYPE", b"doc"])).await,
        Reply::Simple("ReJSON-RL")
    );
    assert_eq!(
        generic::object(&e, &a(&[b"OBJECT", b"ENCODING", b"doc"])).await,
        Reply::bulk_str("json")
    );
    assert_eq!(
        generic::exists(&e, &a(&[b"EXISTS", b"doc"])).await,
        Reply::Int(1)
    );

    assert_eq!(generic::del(&e, &a(&[b"DEL", b"doc"])).await, Reply::Int(1));
    assert_eq!(
        generic::exists(&e, &a(&[b"EXISTS", b"doc"])).await,
        Reply::Int(0)
    );
    assert_eq!(
        generic::type_cmd(&e, &a(&[b"TYPE", b"doc"])).await,
        Reply::Simple("none")
    );
    assert_eq!(read_doc(&e, b"doc").await, None);
}

#[tokio::test]
async fn expire_ttl_persist() {
    let (_d, e) = engine();
    mint_doc(&e, b"doc", serde_json::json!({"a": 1})).await;

    assert_eq!(
        generic::expire(&e, &a(&[b"EXPIRE", b"doc", b"100"]), 1000, false).await,
        Reply::Int(1)
    );
    match generic::ttl(&e, &a(&[b"TTL", b"doc"]), false).await {
        Reply::Int(n) => assert!(n > 90 && n <= 100, "ttl was {n}"),
        other => panic!("expected Int, got {other:?}"),
    }
    assert_eq!(
        generic::persist(&e, &a(&[b"PERSIST", b"doc"])).await,
        Reply::Int(1)
    );
    assert_eq!(
        generic::ttl(&e, &a(&[b"TTL", b"doc"]), false).await,
        Reply::Int(-1)
    );
}

#[tokio::test]
async fn rename_rebuilds_doc_cleanly() {
    let (_d, e) = engine();
    let v = serde_json::json!({"title": "plan", "nested": {"n": 2}, "tags": ["a", "b"]});
    mint_doc(&e, b"src", v.clone()).await;

    // tombstone one array element first: the copy must NOT carry the
    // tombstone across (fresh decomposition at the destination)
    let key = b"src".to_vec();
    e.store
        .run_key(&key.clone(), move |ctx| {
            let mut target: Option<(Vec<u8>, ArrElem)> = None;
            store::scan_prefix(
                ctx,
                &ikey::collection_prefix(ikey::Tag::Json, &key),
                |k, val| {
                    let p = ikey::parse(k).unwrap();
                    if let Some(Seg::Elem(_)) = decode_path(p.suffix).and_then(|mut s| s.pop()) {
                        let (_, pay) = Envelope::decode(val).unwrap();
                        let elem = ArrElem::decode(pay).unwrap();
                        if elem.val == JVal::Str(b"a".to_vec()) {
                            target = Some((k.to_vec(), elem));
                            return false;
                        }
                    }
                    true
                },
            );
            let (ik, elem) = target.expect("array element found");
            let tomb = Envelope::tombstone(RecordType::List, ctx.hlc.now(), ctx.node_id)
                .encode_with(&elem.encode());
            store::write_merged(ctx, &ik, &tomb);
        })
        .await;
    let expected = serde_json::json!({"title": "plan", "nested": {"n": 2}, "tags": ["b"]});
    assert_eq!(read_doc(&e, b"src").await.unwrap(), expected);

    assert_eq!(
        generic::rename(&e, &a(&[b"RENAME", b"src", b"dst"]), false).await,
        Reply::Simple("OK")
    );
    assert_eq!(read_doc(&e, b"dst").await.unwrap(), expected);
    assert_eq!(read_doc(&e, b"src").await, None);
    assert_eq!(
        generic::type_cmd(&e, &a(&[b"TYPE", b"dst"])).await,
        Reply::Simple("ReJSON-RL")
    );

    // fresh decomposition: no tombstoned records at the destination
    let dst = b"dst".to_vec();
    let tomb_count = e
        .store
        .run_key(&dst.clone(), move |ctx| {
            let mut n = 0;
            store::scan_prefix(
                ctx,
                &ikey::collection_prefix(ikey::Tag::Json, &dst),
                |_k, v| {
                    if Envelope::decode(v).is_some_and(|(env, _)| env.is_tombstone()) {
                        n += 1;
                    }
                    true
                },
            );
            n
        })
        .await;
    assert_eq!(tomb_count, 0, "destination must have a clean record set");
}

#[tokio::test]
async fn copy_preserves_source() {
    let (_d, e) = engine();
    let v = serde_json::json!({"x": [1, 2, {"y": null}]});
    mint_doc(&e, b"c1", v.clone()).await;
    assert_eq!(
        generic::copy(&e, &a(&[b"COPY", b"c1", b"c2"])).await,
        Reply::Int(1)
    );
    assert_eq!(read_doc(&e, b"c1").await.unwrap(), v);
    assert_eq!(read_doc(&e, b"c2").await.unwrap(), v);
}

// ===========================================================================
// Phase A4: JSON.* command matrix
// ===========================================================================

use marekvs_engine::cmd::json;

fn bulk(r: &Reply) -> String {
    match r {
        Reply::Bulk(b) => String::from_utf8(b.clone()).unwrap(),
        other => panic!("expected Bulk, got {other:?}"),
    }
}

fn jval(r: &Reply) -> serde_json::Value {
    serde_json::from_str(&bulk(r)).unwrap()
}

async fn jset(e: &Arc<Engine>, key: &[u8], path: &[u8], val: &[u8]) -> Reply {
    json::set(e, &a(&[b"JSON.SET", key, path, val])).await
}

async fn jget(e: &Arc<Engine>, key: &[u8], path: &[u8]) -> Reply {
    json::get(e, &a(&[b"JSON.GET", key, path])).await
}

#[tokio::test]
async fn set_get_roundtrip_root() {
    let (_d, e) = engine();
    assert_eq!(
        jset(&e, b"d", b"$", br#"{"a":1,"b":[true,null,"s"]}"#).await,
        Reply::Simple("OK")
    );
    // legacy root: bare doc
    assert_eq!(
        jval(&jget(&e, b"d", b".").await),
        serde_json::json!({"a":1,"b":[true,null,"s"]})
    );
    // $: array of matches
    assert_eq!(
        jval(&jget(&e, b"d", b"$").await),
        serde_json::json!([{"a":1,"b":[true,null,"s"]}])
    );
    // no-path form: bare doc
    assert_eq!(
        jval(&json::get(&e, &a(&[b"JSON.GET", b"d"])).await),
        serde_json::json!({"a":1,"b":[true,null,"s"]})
    );
    // missing key → Null
    assert_eq!(jget(&e, b"nope", b"$").await, Reply::Null);
    // invalid JSON value → error
    match jset(&e, b"d2", b"$", b"{oops").await {
        Reply::Err(m) => assert!(m.contains("ERR")),
        other => panic!("expected Err, got {other:?}"),
    }
    // non-root set on missing doc → error
    match jset(&e, b"d3", b"$.a", b"1").await {
        Reply::Err(m) => assert!(m.to_lowercase().contains("root"), "{m}"),
        other => panic!("expected Err, got {other:?}"),
    }
}

#[tokio::test]
async fn set_path_create_update_nx_xx() {
    let (_d, e) = engine();
    jset(&e, b"d", b"$", br#"{"a":{"b":1},"arr":[1,2]}"#).await;

    // update existing
    assert_eq!(jset(&e, b"d", b"$.a.b", b"5").await, Reply::Simple("OK"));
    assert_eq!(jval(&jget(&e, b"d", b".a.b").await), serde_json::json!(5));

    // create a new key on an existing object
    assert_eq!(
        jset(&e, b"d", b"$.a.c", b"\"new\"").await,
        Reply::Simple("OK")
    );
    assert_eq!(
        jval(&jget(&e, b"d", b".a.c").await),
        serde_json::json!("new")
    );

    // missing intermediate → error
    match jset(&e, b"d", b"$.x.y", b"1").await {
        Reply::Err(_) => {}
        other => panic!("expected Err, got {other:?}"),
    }

    // NX on existing path → Null, no change
    assert_eq!(
        json::set(&e, &a(&[b"JSON.SET", b"d", b"$.a.b", b"9", b"NX"])).await,
        Reply::Null
    );
    assert_eq!(jval(&jget(&e, b"d", b".a.b").await), serde_json::json!(5));
    // XX on new path → Null, not created
    assert_eq!(
        json::set(&e, &a(&[b"JSON.SET", b"d", b"$.a.zz", b"9", b"XX"])).await,
        Reply::Null
    );
    // NX on new path → OK
    assert_eq!(
        json::set(&e, &a(&[b"JSON.SET", b"d", b"$.a.nx", b"9", b"NX"])).await,
        Reply::Simple("OK")
    );

    // subtree replace: object → scalar, then read the old child is gone
    assert_eq!(jset(&e, b"d", b"$.a", b"7").await, Reply::Simple("OK"));
    assert_eq!(jval(&jget(&e, b"d", b".a").await), serde_json::json!(7));
    assert_eq!(jget(&e, b"d", b".a.b").await, Reply::Null);

    // set an array element by index
    assert_eq!(
        jset(&e, b"d", b"$.arr[1]", b"22").await,
        Reply::Simple("OK")
    );
    assert_eq!(
        jval(&jget(&e, b"d", b".arr").await),
        serde_json::json!([1, 22])
    );

    // WRONGTYPE fence: a string key
    marekvs_engine::cmd::string::set(&e, &a(&[b"SET", b"str", b"v"])).await;
    match jset(&e, b"str", b"$", b"1").await {
        Reply::Err(m) => assert!(m.starts_with("WRONGTYPE"), "{m}"),
        other => panic!("expected WRONGTYPE, got {other:?}"),
    }
}

#[tokio::test]
async fn get_multi_path_and_query() {
    let (_d, e) = engine();
    jset(&e, b"d", b"$", br#"{"a":{"b":2},"b":3}"#).await;
    // query multi-match
    let v = jval(&jget(&e, b"d", b"$..b").await);
    let arr = v.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert!(arr.contains(&serde_json::json!(2)) && arr.contains(&serde_json::json!(3)));
    // multiple paths → object keyed by path arg
    let v = jval(&json::get(&e, &a(&[b"JSON.GET", b"d", b"$.a.b", b".b"])).await);
    assert_eq!(v, serde_json::json!({"$.a.b": [2], ".b": 3}));
    // no match: $ → empty array; legacy → Null
    assert_eq!(jval(&jget(&e, b"d", b"$.zz").await), serde_json::json!([]));
    assert_eq!(jget(&e, b"d", b".zz").await, Reply::Null);
    // INDENT/NEWLINE/SPACE formatting
    let r = json::get(
        &e,
        &a(&[
            b"JSON.GET",
            b"d",
            b"INDENT",
            b"\t",
            b"NEWLINE",
            b"\n",
            b"SPACE",
            b" ",
            b".a",
        ]),
    )
    .await;
    assert_eq!(bulk(&r), "{\n\t\"b\": 2\n}");
}

#[tokio::test]
async fn type_del_clear() {
    let (_d, e) = engine();
    jset(
        &e,
        b"d",
        b"$",
        br#"{"o":{"x":1},"a":[1],"s":"t","i":3,"f":1.5,"t":true,"n":null}"#,
    )
    .await;

    assert_eq!(
        json::type_cmd(&e, &a(&[b"JSON.TYPE", b"d", b".o"])).await,
        Reply::bulk_str("object")
    );
    assert_eq!(
        json::type_cmd(&e, &a(&[b"JSON.TYPE", b"d", b".a"])).await,
        Reply::bulk_str("array")
    );
    assert_eq!(
        json::type_cmd(&e, &a(&[b"JSON.TYPE", b"d", b".s"])).await,
        Reply::bulk_str("string")
    );
    assert_eq!(
        json::type_cmd(&e, &a(&[b"JSON.TYPE", b"d", b".i"])).await,
        Reply::bulk_str("integer")
    );
    assert_eq!(
        json::type_cmd(&e, &a(&[b"JSON.TYPE", b"d", b".f"])).await,
        Reply::bulk_str("number")
    );
    assert_eq!(
        json::type_cmd(&e, &a(&[b"JSON.TYPE", b"d", b".t"])).await,
        Reply::bulk_str("boolean")
    );
    assert_eq!(
        json::type_cmd(&e, &a(&[b"JSON.TYPE", b"d", b".n"])).await,
        Reply::bulk_str("null")
    );
    assert_eq!(
        json::type_cmd(&e, &a(&[b"JSON.TYPE", b"d", b"$.i"])).await,
        Reply::Array(vec![Reply::bulk_str("integer")])
    );
    // default path = root
    assert_eq!(
        json::type_cmd(&e, &a(&[b"JSON.TYPE", b"d"])).await,
        Reply::bulk_str("object")
    );

    // DEL a subtree
    assert_eq!(
        json::del(&e, &a(&[b"JSON.DEL", b"d", b"$.o"])).await,
        Reply::Int(1)
    );
    assert_eq!(jget(&e, b"d", b".o").await, Reply::Null);
    // the rest of the doc is intact
    assert_eq!(jval(&jget(&e, b"d", b".i").await), serde_json::json!(3));
    // DEL missing path → 0
    assert_eq!(
        json::del(&e, &a(&[b"JSON.DEL", b"d", b"$.o"])).await,
        Reply::Int(0)
    );
    // root DEL deletes the doc
    assert_eq!(json::del(&e, &a(&[b"JSON.DEL", b"d"])).await, Reply::Int(1));
    assert_eq!(jget(&e, b"d", b"$").await, Reply::Null);
    // DEL on a missing key → 0
    assert_eq!(json::del(&e, &a(&[b"JSON.DEL", b"d"])).await, Reply::Int(0));

    // CLEAR: containers emptied, numbers zeroed, strings/bools untouched
    jset(
        &e,
        b"c",
        b"$",
        br#"{"o":{"x":1},"a":[1,2],"i":7,"s":"keep"}"#,
    )
    .await;
    assert_eq!(
        json::clear(&e, &a(&[b"JSON.CLEAR", b"c", b"$.*"])).await,
        Reply::Int(3)
    );
    assert_eq!(
        jval(&jget(&e, b"c", b".").await),
        serde_json::json!({"o":{},"a":[],"i":0,"s":"keep"})
    );
}

#[tokio::test]
async fn num_ops() {
    let (_d, e) = engine();
    jset(
        &e,
        b"d",
        b"$",
        br#"{"i":4,"f":2.5,"s":"x","nest":{"i":10}}"#,
    )
    .await;

    // legacy: bare number string
    assert_eq!(
        bulk(&json::numop(&e, &a(&[b"JSON.NUMINCRBY", b"d", b".i", b"3"]), false).await),
        "7"
    );
    // $: JSON array of results
    assert_eq!(
        bulk(&json::numop(&e, &a(&[b"JSON.NUMINCRBY", b"d", b"$.i", b"1"]), false).await),
        "[8]"
    );
    // float result
    assert_eq!(
        bulk(&json::numop(&e, &a(&[b"JSON.NUMINCRBY", b"d", b".f", b"0.5"]), false).await),
        "3"
    );
    // multiply
    assert_eq!(
        bulk(&json::numop(&e, &a(&[b"JSON.NUMMULTBY", b"d", b".i", b"2"]), true).await),
        "16"
    );
    // multi-match with a non-number → null slot
    let r = json::numop(&e, &a(&[b"JSON.NUMINCRBY", b"d", b"$..i", b"1"]), false).await;
    let v: serde_json::Value = serde_json::from_str(&bulk(&r)).unwrap();
    let arr = v.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    // legacy on a non-number → error
    match json::numop(&e, &a(&[b"JSON.NUMINCRBY", b"d", b".s", b"1"]), false).await {
        Reply::Err(_) => {}
        other => panic!("expected Err, got {other:?}"),
    }
    // persisted
    assert_eq!(jval(&jget(&e, b"d", b".i").await), serde_json::json!(17));
}

#[tokio::test]
async fn str_ops_and_toggle() {
    let (_d, e) = engine();
    jset(&e, b"d", b"$", br#"{"s":"abc","b":true,"n":5}"#).await;

    // STRLEN
    assert_eq!(
        json::strlen(&e, &a(&[b"JSON.STRLEN", b"d", b".s"])).await,
        Reply::Int(3)
    );
    assert_eq!(
        json::strlen(&e, &a(&[b"JSON.STRLEN", b"d", b"$.s"])).await,
        Reply::Array(vec![Reply::Int(3)])
    );
    // STRAPPEND takes a JSON-encoded string
    assert_eq!(
        json::strappend(&e, &a(&[b"JSON.STRAPPEND", b"d", b".s", b"\"de\""])).await,
        Reply::Int(5)
    );
    assert_eq!(
        jval(&jget(&e, b"d", b".s").await),
        serde_json::json!("abcde")
    );
    // $ non-string match → null slot
    assert_eq!(
        json::strappend(&e, &a(&[b"JSON.STRAPPEND", b"d", b"$.n", b"\"x\""])).await,
        Reply::Array(vec![Reply::Null])
    );

    // TOGGLE: legacy → "false"/"true" strings; $ → 0/1 ints
    assert_eq!(
        json::toggle(&e, &a(&[b"JSON.TOGGLE", b"d", b".b"])).await,
        Reply::bulk_str("false")
    );
    assert_eq!(
        json::toggle(&e, &a(&[b"JSON.TOGGLE", b"d", b"$.b"])).await,
        Reply::Array(vec![Reply::Int(1)])
    );
    assert_eq!(jval(&jget(&e, b"d", b".b").await), serde_json::json!(true));
}

#[tokio::test]
async fn arr_ops() {
    let (_d, e) = engine();
    jset(&e, b"d", b"$", br#"{"a":[1,2,3]}"#).await;

    // ARRLEN
    assert_eq!(
        json::arrlen(&e, &a(&[b"JSON.ARRLEN", b"d", b".a"])).await,
        Reply::Int(3)
    );
    // ARRAPPEND
    assert_eq!(
        json::arrappend(&e, &a(&[b"JSON.ARRAPPEND", b"d", b".a", b"4", b"5"])).await,
        Reply::Int(5)
    );
    assert_eq!(
        jval(&jget(&e, b"d", b".a").await),
        serde_json::json!([1, 2, 3, 4, 5])
    );
    // ARRINDEX
    assert_eq!(
        json::arrindex(&e, &a(&[b"JSON.ARRINDEX", b"d", b".a", b"4"])).await,
        Reply::Int(3)
    );
    assert_eq!(
        json::arrindex(&e, &a(&[b"JSON.ARRINDEX", b"d", b".a", b"99"])).await,
        Reply::Int(-1)
    );
    // ARRINSERT before index 1
    assert_eq!(
        json::arrinsert(&e, &a(&[b"JSON.ARRINSERT", b"d", b".a", b"1", b"\"x\""])).await,
        Reply::Int(6)
    );
    assert_eq!(
        jval(&jget(&e, b"d", b".a").await),
        serde_json::json!([1, "x", 2, 3, 4, 5])
    );
    // ARRPOP default last
    assert_eq!(
        bulk(&json::arrpop(&e, &a(&[b"JSON.ARRPOP", b"d", b".a"])).await),
        "5"
    );
    // ARRPOP index 0
    assert_eq!(
        bulk(&json::arrpop(&e, &a(&[b"JSON.ARRPOP", b"d", b".a", b"0"])).await),
        "1"
    );
    assert_eq!(
        jval(&jget(&e, b"d", b".a").await),
        serde_json::json!(["x", 2, 3, 4])
    );
    // ARRTRIM to [1,2]
    assert_eq!(
        json::arrtrim(&e, &a(&[b"JSON.ARRTRIM", b"d", b".a", b"1", b"2"])).await,
        Reply::Int(2)
    );
    assert_eq!(
        jval(&jget(&e, b"d", b".a").await),
        serde_json::json!([2, 3])
    );
    // pop from empty
    json::arrtrim(&e, &a(&[b"JSON.ARRTRIM", b"d", b".a", b"1", b"0"])).await;
    assert_eq!(
        json::arrpop(&e, &a(&[b"JSON.ARRPOP", b"d", b".a"])).await,
        Reply::Null
    );
    // nested container append round-trips
    jset(&e, b"d", b"$.a", br#"[]"#).await;
    assert_eq!(
        json::arrappend(&e, &a(&[b"JSON.ARRAPPEND", b"d", b".a", br#"{"k":[1]}"#])).await,
        Reply::Int(1)
    );
    assert_eq!(
        jval(&jget(&e, b"d", b".a").await),
        serde_json::json!([{"k":[1]}])
    );
}

#[tokio::test]
async fn obj_ops_mget_mset_merge_resp_debug() {
    let (_d, e) = engine();
    jset(&e, b"d", b"$", br#"{"o":{"b":1,"a":2},"i":5}"#).await;

    // OBJKEYS lexicographic
    assert_eq!(
        json::objkeys(&e, &a(&[b"JSON.OBJKEYS", b"d", b".o"])).await,
        Reply::Array(vec![Reply::bulk_str("a"), Reply::bulk_str("b")])
    );
    assert_eq!(
        json::objlen(&e, &a(&[b"JSON.OBJLEN", b"d", b".o"])).await,
        Reply::Int(2)
    );
    // non-object → Null
    assert_eq!(
        json::objkeys(&e, &a(&[b"JSON.OBJKEYS", b"d", b".i"])).await,
        Reply::Null
    );

    // MSET + MGET
    assert_eq!(
        json::mset(
            &e,
            &a(&[
                b"JSON.MSET",
                b"m1",
                b"$",
                b"{\"v\":1}",
                b"m2",
                b"$",
                b"{\"v\":2}"
            ])
        )
        .await,
        Reply::Simple("OK")
    );
    let r = json::mget(&e, &a(&[b"JSON.MGET", b"m1", b"m2", b"nope", b"$.v"])).await;
    match r {
        Reply::Array(items) => {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], Reply::bulk_str("[1]"));
            assert_eq!(items[1], Reply::bulk_str("[2]"));
            assert_eq!(items[2], Reply::Null);
        }
        other => panic!("expected Array, got {other:?}"),
    }

    // MERGE (RFC 7386): update + delete-by-null + add
    assert_eq!(
        json::merge(
            &e,
            &a(&[
                b"JSON.MERGE",
                b"d",
                b"$",
                br#"{"o":{"a":null,"c":3},"i":6}"#
            ])
        )
        .await,
        Reply::Simple("OK")
    );
    assert_eq!(
        jval(&jget(&e, b"d", b".").await),
        serde_json::json!({"o":{"b":1,"c":3},"i":6})
    );

    // RESP encoding
    let r = json::resp(&e, &a(&[b"JSON.RESP", b"d", b".o"])).await;
    match r {
        Reply::Array(items) => {
            assert_eq!(items[0], Reply::bulk_str("{"));
        }
        other => panic!("expected Array, got {other:?}"),
    }

    // DEBUG
    match json::debug(&e, &a(&[b"JSON.DEBUG", b"MEMORY", b"d"])).await {
        Reply::Int(n) => assert!(n > 0),
        other => panic!("expected Int, got {other:?}"),
    }
    match json::debug(&e, &a(&[b"JSON.DEBUG", b"HELP"])).await {
        Reply::Array(_) => {}
        other => panic!("expected Array, got {other:?}"),
    }
}

// ===========================================================================
// Phase A5: restart persistence + foreign-origin merge simulations
// ===========================================================================

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

#[tokio::test]
async fn restart_persistence() {
    let dir = tempfile::tempdir().unwrap();
    let v = serde_json::json!({"a": {"b": [1, "x", null]}, "n": 3.5});
    {
        let e = open_engine(&dir, 7);
        jset(&e, b"p", b"$", serde_json::to_string(&v).unwrap().as_bytes()).await;
        json::arrappend(&e, &a(&[b"JSON.ARRAPPEND", b"p", b".a.b", b"9"])).await;
        json::del(&e, &a(&[b"JSON.DEL", b"p", b"$.a.b[0]"])).await;
        drop(e); // Store::drop closes ondadb cleanly
    }
    let e = open_engine(&dir, 7);
    assert_eq!(
        jval(&jget(&e, b"p", b".").await),
        serde_json::json!({"a": {"b": ["x", null, 9]}, "n": 3.5})
    );
}

/// Copy every JSON record of `key` from `src` to `dst` in the given order
/// (simulating replication delivery order).
async fn replicate_json(src: &Arc<Engine>, dst: &Arc<Engine>, key: &[u8], reverse: bool) {
    let k = key.to_vec();
    let mut records: Vec<(Vec<u8>, Vec<u8>)> = src
        .store
        .run_key(&k.clone(), move |ctx| {
            let mut out = Vec::new();
            if let Some(h) = store::get_raw(ctx, &ikey::head_key(&k)) {
                out.push((ikey::head_key(&k), h));
            }
            store::scan_prefix(ctx, &ikey::collection_prefix(ikey::Tag::Json, &k), |ik, v| {
                out.push((ik.to_vec(), v.to_vec()));
                true
            });
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

/// Two nodes edit the same doc concurrently; both replicate to the other in
/// opposite delivery orders; both must materialize the identical document.
#[tokio::test]
async fn concurrent_editors_converge() {
    let (_d1, e1) = {
        let dir = tempfile::tempdir().unwrap();
        let e = open_engine(&dir, 1);
        (dir, e)
    };
    let (_d2, e2) = {
        let dir = tempfile::tempdir().unwrap();
        let e = open_engine(&dir, 2);
        (dir, e)
    };
    // node 1 creates the doc; ship it to node 2
    jset(&e1, b"c", b"$", br#"{"title":"x","tags":["a"],"meta":{"k":1}}"#).await;
    replicate_json(&e1, &e2, b"c", false).await;
    assert_eq!(jval(&jget(&e2, b"c", b".").await), jval(&jget(&e1, b"c", b".").await));

    // concurrent edits: disjoint fields, same-array appends, subtree delete
    jset(&e1, b"c", b"$.title", br#""from-1""#).await;
    json::arrappend(&e1, &a(&[b"JSON.ARRAPPEND", b"c", b".tags", br#""n1""#])).await;
    json::del(&e1, &a(&[b"JSON.DEL", b"c", b"$.meta"])).await;

    jset(&e2, b"c", b"$.other", b"42").await;
    json::arrappend(&e2, &a(&[b"JSON.ARRAPPEND", b"c", b".tags", br#""n2a""#, br#""n2b""#])).await;
    // concurrent write INTO the subtree node 1 deletes: the delete wins —
    // a leaf write does not re-assert its ancestors' presence (documented)
    jset(&e2, b"c", b"$.meta.fresh", b"true").await;

    // exchange in opposite orders
    replicate_json(&e1, &e2, b"c", false).await;
    replicate_json(&e2, &e1, b"c", true).await;
    // (e1 now has everything; ship e1's merged state back so both saw all)
    replicate_json(&e1, &e2, b"c", false).await;

    let v1 = jval(&jget(&e1, b"c", b".").await);
    let v2 = jval(&jget(&e2, b"c", b".").await);
    assert_eq!(v1, v2, "replicas diverged");

    // invariants: disjoint fields both present
    assert_eq!(v1["title"], serde_json::json!("from-1"));
    assert_eq!(v1["other"], serde_json::json!(42));
    // both append runs present and contiguous
    let tags: Vec<String> = v1["tags"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s.as_str().unwrap().to_string())
        .collect();
    assert_eq!(tags.len(), 4, "tags: {tags:?}");
    assert!(tags.contains(&"n1".to_string()));
    let p2a = tags.iter().position(|t| t == "n2a").unwrap();
    assert_eq!(tags[p2a + 1], "n2b", "run n2a/n2b interleaved: {tags:?}");
    // delete vs concurrent-leaf-create: the subtree delete wins; only
    // re-creating the branch node itself would resurrect it (documented)
    assert!(v1.get("meta").is_none(), "meta should be gone: {v1}");
}

/// The same element popped on both sides concurrently: converges, element
/// gone once, no error.
#[tokio::test]
async fn double_arrpop_converges() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let e1 = open_engine(&d1, 1);
    let e2 = open_engine(&d2, 2);
    jset(&e1, b"q", b"$", br#"[10,20,30]"#).await;
    replicate_json(&e1, &e2, b"q", false).await;

    let p1 = bulk(&json::arrpop(&e1, &a(&[b"JSON.ARRPOP", b"q"])).await);
    let p2 = bulk(&json::arrpop(&e2, &a(&[b"JSON.ARRPOP", b"q"])).await);
    assert_eq!(p1, "30");
    assert_eq!(p2, "30", "both nodes popped the same tail concurrently");

    replicate_json(&e1, &e2, b"q", false).await;
    replicate_json(&e2, &e1, b"q", true).await;
    assert_eq!(jval(&jget(&e1, b"q", b".").await), serde_json::json!([10, 20]));
    assert_eq!(jval(&jget(&e2, b"q", b".").await), serde_json::json!([10, 20]));
}

/// Field set racing a field delete: the set's unobserved dot survives
/// (add-wins), in both delivery orders.
#[tokio::test]
async fn set_vs_del_add_wins() {
    let d1 = tempfile::tempdir().unwrap();
    let d2 = tempfile::tempdir().unwrap();
    let e1 = open_engine(&d1, 1);
    let e2 = open_engine(&d2, 2);
    jset(&e1, b"r", b"$", br#"{"f":1}"#).await;
    replicate_json(&e1, &e2, b"r", false).await;

    json::del(&e1, &a(&[b"JSON.DEL", b"r", b"$.f"])).await;
    jset(&e2, b"r", b"$.f", b"2").await; // observed the old dot, sets fresh

    replicate_json(&e1, &e2, b"r", false).await;
    replicate_json(&e2, &e1, b"r", true).await;
    let v1 = jval(&jget(&e1, b"r", b".").await);
    let v2 = jval(&jget(&e2, b"r", b".").await);
    assert_eq!(v1, v2);
    // e2's SET covered the same dot e1's DEL covered, but installed a fresh
    // one the delete never observed → the new value survives
    assert_eq!(v1, serde_json::json!({"f": 2}));
}

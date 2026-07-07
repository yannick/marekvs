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

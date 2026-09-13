use marekvs_core::{
    envelope::Envelope,
    ikey,
    json::{self, ArrElem, JVal, NodeIn, Seg},
    merge::ElementState,
};
use marekvs_engine::{
    cmd::{diff::import::import, json as commands},
    reply::Reply,
    store::{self, Store, StoreConfig},
    Engine,
};
use serde_json::{json, Value};
use std::sync::Arc;
fn engine() -> (tempfile::TempDir, Arc<Engine>) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&StoreConfig {
        data_dir: dir.path().to_string_lossy().into_owned(),
        node_id: 7,
        shard_threads: 1,
        ..Default::default()
    })
    .unwrap();
    (dir, Engine::new(store))
}
async fn set(e: &Arc<Engine>, key: &str, v: &Value) {
    assert_eq!(
        commands::set(
            e,
            &[
                b"JSON.SET".to_vec(),
                key.as_bytes().to_vec(),
                b"$".to_vec(),
                serde_json::to_vec(v).unwrap()
            ]
        )
        .await,
        Reply::ok()
    );
}
async fn upload(e: &Arc<Engine>, key: &str, v: &Value, from: Option<&str>) -> Reply {
    let mut a = vec![
        b"DIFF.IMPORT".to_vec(),
        key.as_bytes().to_vec(),
        serde_json::to_vec(v).unwrap(),
    ];
    if let Some(from) = from {
        a.extend([b"FROM".to_vec(), from.as_bytes().to_vec()]);
    }
    import(e, &a).await
}
fn field<'a>(r: &'a Reply, key: &str) -> &'a Reply {
    let Reply::Map(items) = r else {
        panic!("{r:?}")
    };
    items
        .iter()
        .find(|(k, _)| *k == Reply::bulk_str(key))
        .map(|(_, v)| v)
        .unwrap()
}
async fn records(e: &Arc<Engine>, key: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    let key = key.as_bytes().to_vec();
    e.store
        .run_key(&key.clone(), move |ctx| {
            let mut records = vec![];
            if let Some(h) = store::get_raw(ctx, &ikey::head_key(&key)) {
                records.push((ikey::head_key(&key), h));
            }
            store::scan_prefix_cmd(
                ctx,
                &ikey::collection_prefix(ikey::Tag::Json, &key),
                |k, v| {
                    records.push((k.to_vec(), v.to_vec()));
                    true
                },
            );
            records
        })
        .await
}
fn nodes(rs: &[(Vec<u8>, Vec<u8>)]) -> Vec<(Vec<u8>, NodeIn)> {
    rs.iter()
        .filter_map(|(k, v)| {
            let parsed = ikey::parse(k)?;
            if parsed.tag != ikey::Tag::Json as u8 {
                return None;
            }
            let (env, pay) = Envelope::decode(v)?;
            let node = if matches!(json::split_last(parsed.suffix), Some((_, Seg::Elem(_)))) {
                NodeIn::ArrElem {
                    elem: ArrElem::decode(pay)?,
                    live: !env.is_tombstone(),
                }
            } else {
                let st = ElementState::decode(pay)?;
                NodeIn::Map {
                    val: JVal::decode(st.value()?)?,
                    dots: st.dots(),
                }
            };
            Some((parsed.suffix.to_vec(), node))
        })
        .collect()
}
async fn value(e: &Arc<Engine>, key: &str) -> Value {
    json::build_doc(&nodes(&records(e, key).await))
        .unwrap()
        .value
}
fn original() -> Value {
    json!({"t":"doc","c":[{"t":"sec","a":{"title":"Alpha"},"c":[{"t":"par","c":[{"t":"sen","x":"First clause shall apply."}]}]},{"t":"sec","a":{"title":"Beta"},"c":[{"t":"par","c":[{"t":"sen","x":"Second clause shall apply."}]}]}]})
}
#[tokio::test]
async fn import_moves_complete_subtree_and_preserves_stable_identity() {
    let (_d, e) = engine();
    let key = "doc:{imp}:b:main";
    let a = original();
    set(&e, key, &a).await;
    let before = nodes(&records(&e, key).await);
    let mut b = a.clone();
    b["c"].as_array_mut().unwrap().swap(0, 1);
    let reply = upload(&e, key, &b, None).await;
    assert!(matches!(reply, Reply::Map(_)), "{reply:?}");
    assert_eq!(value(&e, key).await, b);
    let after = nodes(&records(&e, key).await);
    let aa = marekvs_diff::records::snapshot_with_binding(&before, &Default::default())
        .unwrap()
        .0;
    let bb = marekvs_diff::records::snapshot_with_binding(&after, &Default::default())
        .unwrap()
        .0;
    let (_, m) = marekvs_diff::diff_with_matching(&aa, &bb, &Default::default()).unwrap();
    assert!(m
        .pairs()
        .any(|(ai, _)| m.layer(ai) == Some(marekvs_diff::matching::Layer::I0)));
    assert!(*field(&reply, "writes") != Reply::Int(0));
}
#[tokio::test]
async fn repeat_import_leaves_branch_records_byte_identical() {
    let (_d, e) = engine();
    let key = "doc:{repeat}:b:main";
    let a = original();
    let reply = upload(&e, key, &a, None).await;
    assert!(matches!(reply, Reply::Map(_)), "{reply:?}");
    let first = records(&e, key).await;
    let reply = upload(&e, key, &a, None).await;
    assert_eq!(field(&reply, "writes"), &Reply::Int(0));
    assert_eq!(first, records(&e, key).await);
    let all_before = e
        .store
        .run_key(key.as_bytes(), move |ctx| {
            let mut out = Vec::new();
            store::scan_prefix_cmd(ctx, b"", |k, v| {
                out.push((k.to_vec(), v.to_vec()));
                true
            });
            out
        })
        .await;
    let again = upload(&e, key, &a, None).await;
    let all_after = e
        .store
        .run_key(key.as_bytes(), move |ctx| {
            let mut out = Vec::new();
            store::scan_prefix_cmd(ctx, b"", |k, v| {
                out.push((k.to_vec(), v.to_vec()));
                true
            });
            out
        })
        .await;
    assert_eq!(
        all_before, all_after,
        "repeat changed an existing snapshot or graph artifact"
    );
    assert_eq!(field(&reply, "gid"), field(&again, "gid"));
    assert_eq!(first, records(&e, key).await);
}
#[tokio::test]
async fn from_initializes_absent_and_unrelated_destinations() {
    let (_d, e) = engine();
    let src = "doc:{from}:b:source";
    let a = original();
    set(&e, src, &a).await;
    for dest in ["doc:{from}:b:fresh", "doc:{from}:b:other"] {
        if dest.ends_with("other") {
            set(
                &e,
                dest,
                &json!({"t":"doc","c":[{"t":"sen","x":"Unrelated."}]}),
            )
            .await;
        }
        let reply = upload(&e, dest, &a, Some(src)).await;
        assert!(matches!(reply, Reply::Map(_)), "{reply:?}");
        assert_eq!(value(&e, dest).await, a);
        let source = json::build_doc(&nodes(&records(&e, src).await)).unwrap();
        let target = json::build_doc(&nodes(&records(&e, dest).await)).unwrap();
        assert_eq!(source.index.arrays, target.index.arrays);
    }
}
#[tokio::test]
async fn attrs_and_format_updates_cover_observed_map_dots() {
    let (_d, e) = engine();
    let key = "doc:{marks}:b:main";
    let a = original();
    set(&e, key, &a).await;
    let before = records(&e, key).await;
    let mut b = a;
    b["c"][0]["a"]["title"] = json!("Duration");
    b["c"][0]["c"][0]["c"][0]["f"] = json!([[0,5,{"b":true}]]);
    let reply = upload(&e, key, &b, None).await;
    assert!(matches!(reply, Reply::Map(_)), "{reply:?}");
    assert_eq!(value(&e, key).await, b);
    let after = records(&e, key).await;
    let mut covered = false;
    for (k, v) in before {
        let Some(p) = ikey::parse(&k) else { continue };
        if !matches!(json::split_last(p.suffix),Some((_,Seg::Field(ref f)))if f==b"title") {
            continue;
        }
        let (_, pay) = Envelope::decode(&v).unwrap();
        let old = ElementState::decode(pay).unwrap();
        let new = after.iter().find(|(kk, _)| kk == &k).unwrap();
        let (_, pay) = Envelope::decode(&new.1).unwrap();
        let new = ElementState::decode(pay).unwrap();
        if new.value() != old.value() {
            assert!(old.dots().iter().all(|d| new.covered.contains(d)));
            covered = true;
        }
    }
    assert!(covered);
}

#[tokio::test]
async fn deleting_a_predecessor_preserves_anchor_and_repeat_is_noop() {
    let (_d, e) = engine();
    let key = "doc:{anchor}:b:main";
    let a = json!({"t":"doc","c":[{"t":"sen","x":"Alpha."},{"t":"sen","x":"Beta."},{"t":"sen","x":"Gamma."}]});
    set(&e, key, &a).await;
    let b = json!({"t":"doc","c":[{"t":"sen","x":"Beta."},{"t":"sen","x":"Gamma."}]});
    let r = upload(&e, key, &b, None).await;
    assert!(matches!(r, Reply::Map(_)), "{r:?}");
    assert_eq!(value(&e, key).await, b);
    let before = records(&e, key).await;
    let r = upload(&e, key, &b, None).await;
    assert_eq!(field(&r, "writes"), &Reply::Int(0));
    assert_eq!(before, records(&e, key).await);
    let c = json!({"t":"doc","c":[{"t":"sen","x":"Beta."},{"t":"sen","x":"New."},{"t":"sen","x":"Gamma."}]});
    let r = upload(&e, key, &c, None).await;
    assert!(matches!(r, Reply::Map(_)), "{r:?}");
    assert_eq!(value(&e, key).await, c);
}

#[tokio::test]
async fn from_clobbers_other_destination_types() {
    let (_d, e) = engine();
    let source = "doc:{type}:b:source";
    let dest = "doc:{type}:b:dest";
    let a = original();
    set(&e, source, &a).await;
    assert_eq!(
        marekvs_engine::cmd::string::set(
            &e,
            &[
                b"SET".to_vec(),
                dest.as_bytes().to_vec(),
                b"old string".to_vec()
            ]
        )
        .await,
        Reply::ok()
    );
    let reply = upload(&e, dest, &a, Some(source)).await;
    assert!(matches!(reply, Reply::Map(_)), "{reply:?}");
    assert_eq!(value(&e, dest).await, a);
    let key = dest.as_bytes().to_vec();
    assert_eq!(
        e.store
            .run_key(&key.clone(), move |ctx| store::key_type(ctx, &key))
            .await,
        Some(marekvs_core::envelope::head::CTYPE_JSON)
    );
}

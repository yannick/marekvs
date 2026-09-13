//! Replication-order laws for immutable DIFF artifacts and scalar decisions.
use marekvs_core::{envelope::Envelope, ikey};
use marekvs_engine::{
    cmd::{
        diff::{apply, compare, decide},
        json,
    },
    reply::Reply,
    store::{self, Store, StoreConfig},
    Engine,
};
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Arc};
fn engine(id: u16) -> (tempfile::TempDir, Arc<Engine>) {
    let d = tempfile::tempdir().unwrap();
    let store = Store::open(&StoreConfig {
        data_dir: d.path().to_string_lossy().into_owned(),
        node_id: id,
        shard_threads: 2,
        ..Default::default()
    })
    .unwrap();
    (d, Engine::new(store))
}
fn args(parts: &[&str]) -> Vec<Vec<u8>> {
    parts.iter().map(|s| s.as_bytes().to_vec()).collect()
}
fn bulk(r: &Reply) -> String {
    match r {
        Reply::Bulk(b) => String::from_utf8(b.clone()).unwrap(),
        _ => panic!("expected bulk: {r:?}"),
    }
}
fn field<'a>(r: &'a Reply, name: &str) -> &'a Reply {
    let Reply::Map(p) = r else {
        panic!("expected map: {r:?}")
    };
    &p.iter()
        .find(|(k, _)| k == &Reply::bulk_str(name))
        .unwrap()
        .1
}
async fn put(e: &Arc<Engine>, key: &str, value: Value) {
    assert_eq!(
        json::set(e, &args(&["JSON.SET", key, "$", &value.to_string()])).await,
        Reply::ok()
    );
}
type Records = Vec<(Vec<u8>, Vec<u8>)>;
async fn records(e: &Arc<Engine>, key: &str) -> Records {
    let k = key.as_bytes().to_vec();
    e.store
        .run_key(key.as_bytes(), move |ctx| {
            let mut out = vec![];
            for ik in [ikey::head_key(&k), ikey::string_key(&k)] {
                if let Some(v) = store::get_raw(ctx, &ik) {
                    out.push((ik, v));
                }
            }
            store::scan_prefix(
                ctx,
                &ikey::collection_prefix(ikey::Tag::Json, &k),
                |ik, v| {
                    out.push((ik.to_vec(), v.to_vec()));
                    true
                },
            )
            .unwrap();
            out
        })
        .await
}
async fn deliver(e: &Arc<Engine>, key: &str, mut records: Records, reverse: bool) {
    if reverse {
        records.reverse();
    }
    e.store
        .run_key(key.as_bytes(), move |ctx| {
            for (k, v) in records {
                ctx.hlc.observe(Envelope::decode(&v).unwrap().0.hlc);
                store::write_merged(ctx, &k, &v);
            }
        })
        .await;
}
async fn replicate(src: &Arc<Engine>, dst: &Arc<Engine>, key: &str, reverse: bool) {
    deliver(dst, key, records(src, key).await, reverse).await;
}
async fn exchange(a: &Arc<Engine>, b: &Arc<Engine>, key: &str) {
    let ar = records(a, key).await;
    let br = records(b, key).await;
    deliver(a, key, br, true).await;
    deliver(b, key, ar, false).await;
}
async fn pair(a: &Arc<Engine>, b: &Arc<Engine>) -> (String, compare::StoredGraph) {
    put(a,"doc:{r}:b:a",json!({"t":"doc","c":[{"t":"sen","x":"First clause has an old requirement."},{"t":"sen","x":"Second clause has an old condition."}]})).await;
    put(a,"doc:{r}:b:b",json!({"t":"doc","c":[{"t":"sen","x":"First clause has a new requirement."},{"t":"sen","x":"Second clause has a new condition."}]})).await;
    for key in ["doc:{r}:b:a", "doc:{r}:b:b"] {
        replicate(a, b, key, true).await;
    }
    let ar = compare::compare(a, &args(&["DIFF.COMPARE", "doc:{r}:b:a", "doc:{r}:b:b"])).await;
    let br = compare::compare(b, &args(&["DIFF.COMPARE", "doc:{r}:b:a", "doc:{r}:b:b"])).await;
    assert_eq!(ar, br);
    let Reply::Array(p) = ar else {
        panic!("{ar:?}")
    };
    let g: compare::StoredGraph = serde_json::from_str(&bulk(&p[1])).unwrap();
    assert!(g.graph().changes.len() >= 2);
    (bulk(&p[0]), g)
}
async fn view(e: &Arc<Engine>, gkey: &str) -> Value {
    let r = decide::decisions(e, &args(&["DIFF.DECISIONS", gkey])).await;
    serde_json::from_str(&bulk(field(&r, "decisions"))).unwrap()
}
fn dkey(g: &compare::StoredGraph) -> String {
    String::from_utf8(decide::decision_key("r", g)).unwrap()
}
async fn raw_payload(e: &Arc<Engine>, key: &str) -> Vec<u8> {
    let r = records(e, key).await;
    let (_, v) = r
        .into_iter()
        .find(|(k, _)| ikey::parse(k).is_some_and(|p| p.tag == ikey::Tag::String as u8))
        .unwrap();
    Envelope::decode(&v).unwrap().1.to_vec()
}
fn snapshot_key(sid: marekvs_diff::Sid) -> String {
    format!("doc:{{r}}:s:{:032x}", sid.0)
}

#[tokio::test]
async fn concurrent_distinct_decisions_converge_and_results_have_identical_payloads() {
    let (_da, a) = engine(1);
    let (_db, b) = engine(2);
    let (gkey, g) = pair(&a, &b).await;
    let ids: Vec<_> = g.graph().changes.iter().map(|c| c.id.0.as_str()).collect();
    assert_eq!(
        decide::decide(
            &a,
            &args(&["DIFF.DECIDE", &gkey, "BY", "alice|editor", ids[0], "accept"])
        )
        .await,
        Reply::ok()
    );
    assert_eq!(
        decide::decide(
            &b,
            &args(&[
                "DIFF.DECIDE",
                &gkey,
                "BY",
                "bob\"reviewer",
                ids[1],
                "accept"
            ])
        )
        .await,
        Reply::ok()
    );
    exchange(&a, &b, &dkey(&g)).await;
    assert_eq!(view(&a, &gkey).await, view(&b, &gkey).await);
    let av = view(&a, &gkey).await;
    assert_eq!(av[ids[0]]["state"], "accepted");
    assert_eq!(av[ids[1]]["state"], "accepted");
    let ar = apply::apply(&a, &args(&["DIFF.APPLY", &gkey, "REQUEST", "alice"])).await;
    let br = apply::apply(&b, &args(&["DIFF.APPLY", &gkey, "REQUEST", "bob"])).await;
    assert_eq!(field(&ar, "sid"), field(&br, "sid"));
    assert_eq!(
        raw_payload(&a, "diff:{r}:r:alice").await,
        raw_payload(&b, "diff:{r}:r:bob").await
    );
    let result = snapshot_key(marekvs_diff::Sid(
        u128::from_str_radix(&bulk(field(&ar, "sid")), 16).unwrap(),
    ));
    let ap: BTreeMap<_, _> = records(&a, &result)
        .await
        .into_iter()
        .map(|(k, v)| (k, Envelope::decode(&v).unwrap().1.to_vec()))
        .collect();
    let bp: BTreeMap<_, _> = records(&b, &result)
        .await
        .into_iter()
        .map(|(k, v)| (k, Envelope::decode(&v).unwrap().1.to_vec()))
        .collect();
    assert_eq!(ap, bp);
    // Retried delivery and opposite order retain exactly one decision per id.
    exchange(&a, &b, &dkey(&g)).await;
    assert_eq!(av, view(&a, &gkey).await);
}

#[tokio::test]
async fn concurrent_same_change_keeps_the_winning_scalar_tuple_atomic() {
    let (_da, a) = engine(1);
    let (_db, b) = engine(2);
    let (gkey, g) = pair(&a, &b).await;
    let id = &g.graph().changes[0].id.0;
    assert_eq!(
        decide::decide(
            &a,
            &args(&["DIFF.DECIDE", &gkey, "BY", "alice|accept", id, "accept"])
        )
        .await,
        Reply::ok()
    );
    assert_eq!(
        decide::decide(
            &b,
            &args(&["DIFF.DECIDE", &gkey, "BY", "bob|reject", id, "reject"])
        )
        .await,
        Reply::ok()
    );
    let av = view(&a, &gkey).await[id].clone();
    let bv = view(&b, &gkey).await[id].clone();
    exchange(&a, &b, &dkey(&g)).await;
    let winner = view(&a, &gkey).await[id].clone();
    assert!(winner == av || winner == bv);
    assert_eq!(winner, view(&b, &gkey).await[id]);
    exchange(&a, &b, &dkey(&g)).await;
    assert_eq!(winner, view(&a, &gkey).await[id]);
}

#[tokio::test]
async fn lagging_merge_publisher_never_resets_human_decisions() {
    let (_da, a) = engine(1);
    let (_db, b) = engine(2);
    for (key, text) in [
        ("doc:{r}:b:base", "One old clause."),
        ("doc:{r}:b:left", "One new clause."),
        ("doc:{r}:b:right", "One old clause."),
    ] {
        put(&a, key, json!({"t":"doc","c":[{"t":"sen","x":text}]})).await;
        replicate(&a, &b, key, true).await;
    }
    let command = args(&[
        "DIFF.MERGE3",
        "doc:{r}:b:base",
        "doc:{r}:b:left",
        "doc:{r}:b:right",
    ]);
    let first = apply::merge3(&a, &command).await;
    let Reply::Array(p) = &first else {
        panic!("{first:?}")
    };
    let key = bulk(&p[0]);
    let g: compare::StoredGraph = serde_json::from_str(&bulk(&p[1])).unwrap();
    let id = &g.graph().changes[0].id.0;
    assert_eq!(view(&a, &key).await[id]["state"], "accepted");
    assert_eq!(
        decide::decide(
            &a,
            &args(&["DIFF.DECIDE", &key, "BY", "human", id, "reject"])
        )
        .await,
        Reply::ok()
    );
    let lagging = apply::merge3(&b, &command).await;
    assert_eq!(lagging, first);
    assert!(records(&b, &dkey(&g)).await.is_empty());
    replicate(&b, &a, &key, true).await;
    for sid in g.inputs() {
        replicate(&b, &a, &snapshot_key(sid), true).await;
    }
    assert_eq!(view(&a, &key).await[id]["state"], "rejected");
    replicate(&a, &b, &dkey(&g), false).await;
    assert_eq!(view(&a, &key).await, view(&b, &key).await);
}

#[tokio::test]
async fn stored_request_survives_changed_decisions_and_missing_snapshots() {
    let (_da, a) = engine(1);
    let (_db, b) = engine(2);
    let (gkey, g) = pair(&a, &b).await;
    let id = &g.graph().changes[0].id.0;
    assert_eq!(
        decide::decide(&a, &args(&["DIFF.DECIDE", &gkey, id, "accept"])).await,
        Reply::ok()
    );
    let request = args(&["DIFF.APPLY", &gkey, "REQUEST", "durable"]);
    let first = apply::apply(&a, &request).await;
    assert!(matches!(first, Reply::Map(_)), "{first:?}");
    assert_eq!(
        decide::decide(&a, &args(&["DIFF.DECIDE", &gkey, id, "reject"])).await,
        Reply::ok()
    );
    for sid in g.inputs() {
        let key = snapshot_key(sid);
        let k = key.as_bytes().to_vec();
        a.store
            .run_key(key.as_bytes(), move |ctx| {
                let raw = store::get_raw(ctx, &ikey::head_key(&k)).unwrap();
                let (env, pay) = Envelope::decode(&raw).unwrap();
                let env = Envelope {
                    hlc: ctx.hlc.now(),
                    flags: env.flags | marekvs_core::envelope::TOMBSTONE,
                    ..env
                };
                store::write_merged(ctx, &ikey::head_key(&k), &env.encode_with(pay));
            })
            .await;
    }
    assert_eq!(apply::apply(&a, &request).await, first);
    assert!(
        matches!(apply::apply(&a,&args(&["DIFF.APPLY",&gkey,"REQUEST","new"])).await,Reply::Err(x)if x.starts_with("DIFFNOSNAPSHOT"))
    );
}

#[tokio::test]
async fn apply_rejects_partial_target_even_when_selection_needs_only_base() {
    let (_da, a) = engine(1);
    let (_db, b) = engine(2);
    let (_dc, c) = engine(3);
    let (gkey, g) = pair(&a, &b).await;
    replicate(&a, &c, &gkey, false).await;
    replicate(&a, &c, &snapshot_key(g.graph().from), true).await;
    let target = snapshot_key(g.graph().to);
    let head_only = records(&a, &target)
        .await
        .into_iter()
        .filter(|(k, _)| ikey::parse(k).is_some_and(|p| p.tag == ikey::Tag::Head as u8))
        .collect();
    deliver(&c, &target, head_only, false).await;
    let r = apply::apply(&c, &args(&["DIFF.APPLY", &gkey, "REQUEST", "partial"])).await;
    assert!(matches!(r,Reply::Err(x)if x.starts_with("DIFFNOSNAPSHOT")));
    assert!(records(&c, "diff:{r}:r:partial").await.is_empty());
    replicate(&a, &c, &target, true).await;
    assert!(matches!(
        apply::apply(&c, &args(&["DIFF.APPLY", &gkey, "REQUEST", "complete"])).await,
        Reply::Map(_)
    ));
}

#[tokio::test]
async fn merge_outer_metadata_is_part_of_verified_graph_identity() {
    let (_dir, e) = engine(1);
    for (key, text) in [
        ("doc:{r}:b:base", "One old clause."),
        ("doc:{r}:b:left", "One new clause."),
        ("doc:{r}:b:right", "One old clause."),
    ] {
        put(&e, key, json!({"t":"doc","c":[{"t":"sen","x":text}]})).await;
    }
    let r = apply::merge3(
        &e,
        &args(&[
            "DIFF.MERGE3",
            "doc:{r}:b:base",
            "doc:{r}:b:left",
            "doc:{r}:b:right",
        ]),
    )
    .await;
    let Reply::Array(p) = r else { panic!("{r:?}") };
    let key = bulk(&p[0]);
    let mut stored: compare::StoredGraph = serde_json::from_str(&bulk(&p[1])).unwrap();
    let id = stored.graph().changes[0].id.clone();
    let decisions = dkey(&stored);
    let compare::StoredGraph::Three(merge) = &mut stored else {
        panic!("expected merge")
    };
    // This used to change defaults without changing the checked inner gid.
    merge.conflicts.push(vec![id.clone()]);
    let bytes = serde_json::to_vec(&stored).unwrap();
    let k = key.clone();
    e.store
        .run_key(key.as_bytes(), move |ctx| {
            let raw = Envelope::new(
                marekvs_core::envelope::RecordType::String,
                ctx.hlc.now(),
                ctx.node_id,
            )
            .encode_with(&bytes);
            store::write_merged(ctx, &ikey::string_key(k.as_bytes()), &raw);
        })
        .await;
    assert!(
        matches!(decide::decide(&e,&args(&["DIFF.DECIDE",&key,&id.0,"accept"])).await,Reply::Err(x)if x.starts_with("DIFFINVALID"))
    );
    assert!(records(&e, &decisions).await.is_empty());
}

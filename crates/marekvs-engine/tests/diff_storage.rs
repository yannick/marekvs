use marekvs_core::{envelope::Envelope, ikey, merge::ElementState};
use marekvs_engine::{
    cmd::{
        diff::{fork, snapshot},
        generic, json,
    },
    reply::Reply,
    store::{self, Store, StoreConfig},
    Engine,
};
use std::sync::Arc;
fn engine(node_id: u16) -> (tempfile::TempDir, Arc<Engine>) {
    let d = tempfile::tempdir().unwrap();
    let s = Store::open(&StoreConfig {
        data_dir: d.path().to_string_lossy().into_owned(),
        node_id,
        shard_threads: 2,
        ..StoreConfig::default()
    })
    .unwrap();
    (d, Engine::new(s))
}
fn a(parts: &[&[u8]]) -> Vec<Vec<u8>> {
    parts.iter().map(|p| p.to_vec()).collect()
}
const DOC: &[u8] = br#"{"t":"doc","c":[{"t":"sen","x":"one"},{"t":"sen","x":"two"}]}"#;
async fn put(e: &Arc<Engine>, key: &[u8]) {
    assert_eq!(
        json::set(e, &a(&[b"JSON.SET", key, b"$", DOC])).await,
        Reply::ok()
    );
}
async fn snap(e: &Arc<Engine>, key: &[u8]) -> Vec<u8> {
    match snapshot::snapshot(e, &a(&[b"DIFF.SNAPSHOT", key])).await {
        Reply::Bulk(k) => k,
        r => panic!("{r:?}"),
    }
}
async fn records(e: &Arc<Engine>, key: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let k = key.to_vec();
    e.store
        .run_key(key, move |ctx| {
            let mut r = vec![];
            store::scan_prefix_cmd(
                ctx,
                &ikey::collection_prefix(ikey::Tag::Json, &k),
                |k, v| {
                    r.push((ikey::parse(k).unwrap().suffix.to_vec(), v.to_vec()));
                    true
                },
            );
            r
        })
        .await
}
#[tokio::test]
async fn snapshot_payloads_are_replica_identical_and_retry_writes_nothing() {
    let (_d, e) = engine(1);
    let (_d2, e2) = engine(2);
    let key = b"doc:{s}:b:a";
    put(&e, key).await;
    put(&e2, key).await;
    let s = snap(&e, key).await;
    assert_eq!(s, snap(&e2, key).await);
    let sk = s.clone();
    let head_before = e
        .store
        .run_key(&s, move |ctx| {
            store::get_raw(ctx, &ikey::head_key(&sk)).unwrap()
        })
        .await;
    let before = records(&e, &s).await;
    let other = records(&e2, &s).await;
    assert_eq!(before.len(), other.len());
    for ((p, a), (q, b)) in before.iter().zip(other) {
        assert_eq!(p, &q);
        let (ae, ap) = Envelope::decode(a).unwrap();
        let (_, bp) = Envelope::decode(&b).unwrap();
        assert_eq!(ap, bp);
        if ae.rtype().is_or_element() {
            assert_eq!(ElementState::decode(ap).unwrap().live.len(), 1);
        }
    }
    assert_eq!(s, snap(&e, key).await);
    assert_eq!(before, records(&e, &s).await);
    let sk = s.clone();
    assert_eq!(
        head_before,
        e.store
            .run_key(&s, move |ctx| store::get_raw(ctx, &ikey::head_key(&sk))
                .unwrap())
            .await
    );
}
#[tokio::test]
async fn partial_replication_rejected_and_repaired_with_head_last() {
    let (_d, e) = engine(1);
    let (_d2, e2) = engine(2);
    let key = b"doc:{s}:b:a";
    put(&e, key).await;
    put(&e2, key).await;
    let s = snap(&e, key).await;
    let all = records(&e, &s).await;
    let partial = all[..all.len() / 2].to_vec();
    let dest = s.clone();
    e2.store
        .run_key(&s, move |ctx| {
            for (p, v) in partial {
                store::write_merged(ctx, &ikey::json_node_key(&dest, &p), &v);
            }
        })
        .await;
    assert!(
        matches!(snapshot::snapshot(&e2,&a(&[b"DIFF.SNAPSHOT",&s])).await,Reply::Err(x) if x.starts_with("DIFFNOSNAPSHOT"))
    );
    // A head arriving before the remaining records must still be rejected.
    let source = s.clone();
    let head = e
        .store
        .run_key(&s, move |ctx| {
            store::get_raw(ctx, &ikey::head_key(&source)).unwrap()
        })
        .await;
    let dest = s.clone();
    e2.store
        .run_key(&s, move |ctx| {
            store::write_merged(ctx, &ikey::head_key(&dest), &head);
        })
        .await;
    assert!(
        matches!(snapshot::snapshot(&e2,&a(&[b"DIFF.SNAPSHOT",&s])).await,Reply::Err(x) if x.starts_with("DIFFNOSNAPSHOT"))
    );
    assert_eq!(s, snap(&e2, key).await);
    assert_eq!(s, snap(&e2, &s).await);
    assert_eq!(all.len(), records(&e2, &s).await.len());
}
#[tokio::test]
async fn fork_reuse_drops_ttl_and_snapshot_insert_order_is_natural() {
    let (_d, e) = engine(1);
    let key = b"doc:{s}:b:a";
    let dest = b"doc:{s}:b:b";
    put(&e, key).await;
    let s = snap(&e, key).await;
    put(&e, dest).await;
    assert_eq!(generic::del(&e, &a(&[b"DEL", dest])).await, Reply::Int(1));
    assert_eq!(
        fork::fork(&e, &a(&[b"DIFF.FORK", &s, dest])).await,
        Reply::ok()
    );
    let k = dest.to_vec();
    assert_eq!(
        e.store
            .run_key(dest, move |ctx| store::get_head(ctx, &k)
                .unwrap()
                .0
                .ttl_deadline_ms)
            .await,
        0
    );
    let inserted = br#"{"t":"sen","x":"head"}"#;
    let result = json::arrinsert(&e, &a(&[b"JSON.ARRINSERT", dest, b"$.c", b"0", inserted])).await;
    assert!(!matches!(result, Reply::Err(_)), "{result:?}");
    let value = json::get(&e, &a(&[b"JSON.GET", dest, b"$.c[0].x"])).await;
    assert_eq!(value, Reply::Bulk(br#"["head"]"#.to_vec()));
    let result = json::arrinsert(
        &e,
        &a(&[
            b"JSON.ARRINSERT",
            dest,
            b"$.c",
            b"2",
            br#"{"t":"sen","x":"middle"}"#,
        ]),
    )
    .await;
    assert!(!matches!(result, Reply::Err(_)), "{result:?}");
    assert_eq!(
        json::get(&e, &a(&[b"JSON.GET", dest, b"$.c[*].x"])).await,
        Reply::Bulk(br#"["head","one","middle","two"]"#.to_vec())
    );
}
#[tokio::test]
async fn fork_preserves_dead_anchors_and_does_not_inherit_expiry() {
    let (_d, e) = engine(1);
    let key = b"doc:{s}:b:a";
    let dest = b"doc:{s}:b:b";
    put(&e, key).await;
    assert_eq!(
        json::del(&e, &a(&[b"JSON.DEL", key, b"$.c[0]"])).await,
        Reply::Int(1)
    );
    let k = key.to_vec();
    e.store
        .run_key(key, move |ctx| {
            let raw = store::get_raw(ctx, &ikey::head_key(&k)).unwrap();
            let (env, pay) = Envelope::decode(&raw).unwrap();
            let newer = Envelope {
                hlc: ctx.hlc.now(),
                ttl_deadline_ms: store::now_ms() + 60_000,
                ..env
            };
            store::write_merged(ctx, &ikey::head_key(&k), &newer.encode_with(pay));
        })
        .await;
    assert_eq!(
        fork::fork(&e, &a(&[b"DIFF.FORK", key, dest])).await,
        Reply::ok()
    );
    let before = records(&e, key).await;
    let after = records(&e, dest).await;
    let after: std::collections::BTreeMap<_, _> = after.into_iter().collect();
    let mut dead = 0;
    for (p, v) in &before {
        let (env, pay) = Envelope::decode(v).unwrap();
        if env.rtype() == marekvs_core::envelope::RecordType::List {
            let (other, other_pay) = Envelope::decode(&after[p]).unwrap();
            assert_eq!(pay, other_pay);
            assert_eq!(env.is_tombstone(), other.is_tombstone());
            dead += usize::from(env.is_tombstone());
        } else if !env.is_tombstone() {
            let source = ElementState::decode(pay).unwrap();
            let copied = ElementState::decode(Envelope::decode(&after[p]).unwrap().1).unwrap();
            assert_eq!(source.value(), copied.value());
            assert_ne!(source.dots(), copied.dots());
        }
    }
    assert!(dead > 0);
    let k = dest.to_vec();
    assert_eq!(
        e.store
            .run_key(dest, move |ctx| store::get_head(ctx, &k)
                .unwrap()
                .0
                .ttl_deadline_ms)
            .await,
        0
    );
    assert_eq!(
        json::get(&e, &a(&[b"JSON.GET", dest, b"$.c[*].x"])).await,
        Reply::Bulk(br#"["two"]"#.to_vec())
    );
}

#[tokio::test]
async fn snapshot_collision_never_overwrites_foreign_records() {
    let (_dir, e) = engine(1);
    let key = b"doc:{collision}:b:a";
    put(&e, key).await;
    let s = snap(&e, key).await;
    let target = s.clone();
    e.store
        .run_key(&s, move |ctx| {
            let path =
                marekvs_core::json::encode_path(&[marekvs_core::json::Seg::Field(b"t".to_vec())]);
            let rk = ikey::json_node_key(&target, &path);
            let old = store::get_raw(ctx, &rk).unwrap();
            let dots = ElementState::decode(Envelope::decode(&old).unwrap().1)
                .unwrap()
                .dots();
            let raw = marekvs_core::merge::element_set(
                marekvs_core::envelope::RecordType::HashField,
                ctx.hlc.now(),
                ctx.node_id,
                &marekvs_core::json::JVal::Str(b"foreign".to_vec()).encode(),
                &dots,
            );
            store::write_merged(ctx, &rk, &raw);
        })
        .await;
    let before = records(&e, &s).await;
    assert!(
        matches!(snapshot::snapshot(&e,&a(&[b"DIFF.SNAPSHOT",key])).await,Reply::Err(x) if x.starts_with("DIFFCOLLISION"))
    );
    assert_eq!(before, records(&e, &s).await);
    assert!(
        matches!(snapshot::snapshot(&e,&a(&[b"DIFF.SNAPSHOT",&s])).await,Reply::Err(x) if x.starts_with("DIFFNOSNAPSHOT"))
    );
}

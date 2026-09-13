//! Regression cases found during the independent DIFF endpoint review.
use marekvs_engine::{
    cmd::{
        diff::{compare, decide},
        json,
    },
    reply::Reply,
    store::{Store, StoreConfig},
    Engine,
};
use std::sync::Arc;
fn engine() -> (tempfile::TempDir, Arc<Engine>) {
    let d = tempfile::tempdir().unwrap();
    let store = Store::open(&StoreConfig {
        data_dir: d.path().to_string_lossy().into_owned(),
        node_id: 9,
        shard_threads: 1,
        ..Default::default()
    })
    .unwrap();
    let mut e = Engine::new(store);
    Arc::get_mut(&mut e).unwrap().diff.config.max_bytes = 4096;
    (d, e)
}
fn arg(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}
#[tokio::test]
async fn cumulative_decision_budget_rejects_batch_before_any_write() {
    let (_d, e) = engine();
    for (key, version) in [
        ("doc:{decision-budget}:b:a", 0),
        ("doc:{decision-budget}:b:b", 1),
    ] {
        let v = serde_json::json!({"t":"doc","c":(0..4).map(|i|serde_json::json!({"t":"sen","x":format!("Clause {i}."),"a":{"version":version}})).collect::<Vec<_>>()});
        assert_eq!(
            json::set(
                &e,
                &[
                    arg("JSON.SET"),
                    arg(key),
                    arg("$"),
                    serde_json::to_vec(&v).unwrap()
                ]
            )
            .await,
            Reply::ok()
        );
    }
    let reply = compare::compare(
        &e,
        &[
            arg("DIFF.COMPARE"),
            arg("doc:{decision-budget}:b:a"),
            arg("doc:{decision-budget}:b:b"),
        ],
    )
    .await;
    let Reply::Array(parts) = reply else {
        panic!("{reply:?}")
    };
    let Reply::Bulk(key) = &parts[0] else {
        panic!("{parts:?}")
    };
    let Reply::Bulk(raw) = &parts[1] else {
        panic!("{parts:?}")
    };
    let graph: serde_json::Value = serde_json::from_slice(raw).unwrap();
    let ids: Vec<_> = graph["changes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| arg(c["id"].as_str().unwrap()))
        .collect();
    assert_eq!(ids.len(), 4);
    let principal = vec![b'x'; 1024];
    assert_eq!(
        decide::decide(
            &e,
            &[
                arg("DIFF.DECIDE"),
                key.clone(),
                arg("BY"),
                principal.clone(),
                ids[0].clone(),
                arg("accept"),
                ids[1].clone(),
                arg("accept")
            ]
        )
        .await,
        Reply::ok()
    );
    let before = decide::decisions(&e, &[arg("DIFF.DECISIONS"), key.clone()]).await;
    assert!(matches!(before, Reply::Map(_)), "{before:?}");
    let rejected = decide::decide(
        &e,
        &[
            arg("DIFF.DECIDE"),
            key.clone(),
            arg("BY"),
            principal,
            ids[2].clone(),
            arg("accept"),
            ids[3].clone(),
            arg("accept"),
        ],
    )
    .await;
    assert!(
        matches!(rejected,Reply::Err(ref s)if s.starts_with("DIFFTOOBIG")),
        "accepted a batch that makes existing decisions unreadable: {rejected:?}"
    );
    assert_eq!(
        decide::decisions(&e, &[arg("DIFF.DECISIONS"), key.clone()]).await,
        before,
        "rejected oversized batch partially changed decisions"
    );
}

#[tokio::test]
async fn concurrent_string_record_blocks_decision_json_access() {
    let (_d, e) = engine();
    for (key, version) in [
        ("doc:{decision-type}:b:a", 0),
        ("doc:{decision-type}:b:b", 1),
    ] {
        let v = serde_json::json!({"t":"doc","c":[{"t":"sen","x":"Same clause.","a":{"version":version}}]});
        assert_eq!(
            json::set(
                &e,
                &[
                    arg("JSON.SET"),
                    arg(key),
                    arg("$"),
                    serde_json::to_vec(&v).unwrap()
                ]
            )
            .await,
            Reply::ok()
        );
    }
    let reply = compare::compare(
        &e,
        &[
            arg("DIFF.COMPARE"),
            arg("doc:{decision-type}:b:a"),
            arg("doc:{decision-type}:b:b"),
        ],
    )
    .await;
    let Reply::Array(parts) = reply else {
        panic!("{reply:?}")
    };
    let Reply::Bulk(gkey) = &parts[0] else {
        panic!("{parts:?}")
    };
    let Reply::Bulk(raw) = &parts[1] else {
        panic!("{parts:?}")
    };
    let graph: serde_json::Value = serde_json::from_slice(raw).unwrap();
    let id = arg(graph["changes"][0]["id"].as_str().unwrap());
    assert_eq!(
        decide::decide(
            &e,
            &[arg("DIFF.DECIDE"), gkey.clone(), id.clone(), arg("accept")]
        )
        .await,
        Reply::ok()
    );
    let dkey = String::from_utf8(gkey.clone())
        .unwrap()
        .replace(":g:", ":d:")
        .into_bytes();
    // A SET made on a replica that had not seen the decision head delivers
    // only its string record; both physical types can then be live locally.
    e.store
        .run_key(&dkey.clone(), move |ctx| {
            let record = marekvs_engine::store::new_lww(
                ctx,
                marekvs_core::envelope::RecordType::String,
                b"concurrent SET",
                0,
            );
            marekvs_engine::store::write_merged(
                ctx,
                &marekvs_core::ikey::string_key(&dkey),
                &record,
            );
            assert!(marekvs_engine::store::check_type(
                ctx,
                &dkey,
                marekvs_core::envelope::head::CTYPE_JSON
            )
            .is_err());
        })
        .await;
    assert!(
        matches!(decide::decisions(&e,&[arg("DIFF.DECISIONS"),gkey.clone()]).await,Reply::Err(ref s)if s.starts_with("WRONGTYPE")),
        "DIFF read hidden JSON despite the ordinary type fence"
    );
    assert!(
        matches!(decide::decide(&e,&[arg("DIFF.DECIDE"),gkey.clone(),id,arg("reject")]).await,Reply::Err(ref s)if s.starts_with("WRONGTYPE")),
        "DIFF wrote hidden JSON despite the ordinary type fence"
    );
}

async fn one_change(e: &Arc<Engine>, tag: &str) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
    let a = format!("doc:{{{tag}}}:b:a");
    let b = format!("doc:{{{tag}}}:b:b");
    for (key, version) in [(&a, 0), (&b, 1)] {
        let v =
            serde_json::json!({"t":"doc","c":[{"t":"sen","x":"Clause.","a":{"version":version}}]});
        assert_eq!(
            json::set(
                e,
                &[
                    arg("JSON.SET"),
                    arg(key),
                    arg("$"),
                    serde_json::to_vec(&v).unwrap()
                ]
            )
            .await,
            Reply::ok()
        );
    }
    let reply = compare::compare(e, &[arg("DIFF.COMPARE"), arg(&a), arg(&b)]).await;
    let Reply::Array(parts) = reply else {
        panic!("{reply:?}")
    };
    let Reply::Bulk(gkey) = &parts[0] else {
        panic!("{parts:?}")
    };
    let Reply::Bulk(raw) = &parts[1] else {
        panic!("{parts:?}")
    };
    let graph: serde_json::Value = serde_json::from_slice(raw).unwrap();
    let id = arg(graph["changes"][0]["id"].as_str().unwrap());
    let dkey = String::from_utf8(gkey.clone())
        .unwrap()
        .replace(":g:", ":d:")
        .into_bytes();
    assert_eq!(
        decide::decide(
            e,
            &[arg("DIFF.DECIDE"), gkey.clone(), id.clone(), arg("accept")]
        )
        .await,
        Reply::ok()
    );
    (gkey.clone(), id, dkey)
}
#[tokio::test]
async fn malformed_live_decision_is_not_replaced_by_a_default() {
    let (_d, e) = engine();
    let (gkey, id, dkey) = one_change(&e, "malformed-decision").await;
    e.store
        .run_key(&dkey.clone(), move |ctx| {
            let path = marekvs_core::json::encode_path(&[marekvs_core::json::Seg::Field(id)]);
            let raw = marekvs_engine::store::new_lww(
                ctx,
                marekvs_core::envelope::RecordType::HashField,
                b"malformed OR state",
                0,
            );
            marekvs_engine::store::put_raw(
                ctx,
                &marekvs_core::ikey::json_node_key(&dkey, &path),
                &raw,
            );
        })
        .await;
    assert!(
        matches!(decide::decisions(&e,&[arg("DIFF.DECISIONS"),gkey]).await,Reply::Err(ref s)if s.starts_with("DIFFDECISIONS"))
    );
}
#[tokio::test]
async fn oversized_decision_root_is_charged_before_or_state_decoding() {
    let (_d, e) = engine();
    let (gkey, _id, dkey) = one_change(&e, "root-budget").await;
    e.store
        .run_key(&dkey.clone(), move |ctx| {
            let raw = marekvs_engine::store::new_lww(
                ctx,
                marekvs_core::envelope::RecordType::HashField,
                &vec![0; 5000],
                0,
            );
            marekvs_engine::store::put_raw(
                ctx,
                &marekvs_core::ikey::json_node_key(&dkey, &[]),
                &raw,
            );
        })
        .await;
    assert!(
        matches!(decide::decisions(&e,&[arg("DIFF.DECISIONS"),gkey]).await,Reply::Err(ref s)if s.starts_with("DIFFTOOBIG"))
    );
}

#[tokio::test]
async fn apply_rechecks_revision_after_captured_decisions_before_publication() {
    use marekvs_engine::{
        cmd::diff::{apply, pool::DiffPool},
        ReadThrough,
    };
    use std::{future::Future, pin::Pin};
    struct Observe {
        key: Vec<u8>,
        notify: Arc<tokio::sync::Notify>,
    }
    impl ReadThrough for Observe {
        fn fetch<'a>(&'a self, key: &'a [u8]) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
            Box::pin(async move {
                if key == self.key {
                    self.notify.notify_one();
                }
                false
            })
        }
    }
    let (_d, mut e) = engine();
    let mut config = e.diff.config.clone();
    config.threads = 1;
    Arc::get_mut(&mut e).unwrap().diff.pool = DiffPool::new(&config);
    let (gkey, id, dkey) = one_change(&e, "apply-race").await;
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let admission = e.diff.pool.admit(0).unwrap();
    let block_engine = e.clone();
    let blocker = tokio::spawn(async move {
        block_engine
            .diff
            .pool
            .run(admission, move |_| {
                let _ = started_tx.send(());
                release_rx
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .expect("test failed to release worker");
                Ok(())
            })
            .await
    });
    started_rx.await.unwrap();
    let notify = Arc::new(tokio::sync::Notify::new());
    e.set_read_through(Arc::new(Observe {
        key: dkey.clone(),
        notify: notify.clone(),
    }));
    let app_engine = e.clone();
    let app_key = gkey.clone();
    let applying = tokio::spawn(async move {
        apply::apply(
            &app_engine,
            &[arg("DIFF.APPLY"), app_key, arg("REQUEST"), arg("race")],
        )
        .await
    });
    // This current-thread runtime cannot resume this test while APPLY is
    // still polling. After the ready fetch notifies us, APPLY synchronously
    // queues its capture and yields. The next shard job is therefore a FIFO
    // barrier strictly after the capture, while its CPU worker remains blocked.
    notify.notified().await;
    e.store
        .run_key(&dkey.clone(), move |ctx| {
            let path = marekvs_core::json::encode_path(&[marekvs_core::json::Seg::Field(id)]);
            let key = marekvs_core::ikey::json_node_key(&dkey, &path);
            let raw = marekvs_engine::store::get_raw(ctx, &key).unwrap();
            let (_, pay) = marekvs_core::envelope::Envelope::decode(&raw).unwrap();
            let observed = marekvs_core::merge::element_dots(pay);
            let value = marekvs_core::json::JVal::Str(
                serde_json::to_vec(&("rejected", "reviewer", marekvs_engine::store::now_ms()))
                    .unwrap(),
            );
            let incoming = marekvs_core::merge::element_set(
                marekvs_core::envelope::RecordType::HashField,
                ctx.hlc.now(),
                ctx.node_id,
                &value.encode(),
                &observed,
            );
            marekvs_engine::store::write_merged(ctx, &key, &incoming);
        })
        .await;
    release_tx.send(()).unwrap();
    blocker.await.unwrap().unwrap();
    let result = applying.await.unwrap();
    assert!(
        matches!(result,Reply::Err(ref s)if s.starts_with("DIFFSTALE")),
        "stale captured plan was published: {result:?}"
    );
    let rkey = arg("diff:{apply-race}:r:race");
    assert!(e
        .store
        .run_key(&rkey.clone(), move |ctx| marekvs_engine::store::get_raw(
            ctx,
            &marekvs_core::ikey::string_key(&rkey)
        ))
        .await
        .is_none());
}

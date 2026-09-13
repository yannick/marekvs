use marekvs_engine::{
    cmd::{
        diff::{apply, compare, decide},
        json,
    },
    reply::Reply,
    store::{Store, StoreConfig},
    Engine,
};
use std::sync::Arc;
fn engine() -> (tempfile::TempDir, Arc<Engine>) {
    let d = tempfile::tempdir().unwrap();
    let s = Store::open(&StoreConfig {
        data_dir: d.path().to_string_lossy().into_owned(),
        node_id: 11,
        shard_threads: 2,
        ..Default::default()
    })
    .unwrap();
    (d, Engine::new(s))
}
fn args(parts: &[&str]) -> Vec<Vec<u8>> {
    parts.iter().map(|s| s.as_bytes().to_vec()).collect()
}
fn field<'a>(reply: &'a Reply, name: &str) -> &'a Reply {
    let pairs = match reply {
        Reply::Map(p) => p,
        _ => panic!("not a map: {reply:?}"),
    };
    &pairs
        .iter()
        .find(|(k, _)| k == &Reply::bulk_str(name))
        .unwrap()
        .1
}
fn bulk(reply: &Reply) -> String {
    match reply {
        Reply::Bulk(b) => String::from_utf8(b.clone()).unwrap(),
        _ => panic!("not bulk: {reply:?}"),
    }
}
async fn pair(e: &Arc<Engine>) -> (String, serde_json::Value) {
    for (key, text) in [
        ("doc:{t/a}:b:a", "The parties shall comply."),
        ("doc:{t/a}:b:b", "The parties may comply."),
    ] {
        let tree = serde_json::json!({"t":"doc","c":[{"t":"sen","x":text}]}).to_string();
        assert_eq!(
            json::set(e, &args(&["JSON.SET", key, "$", &tree])).await,
            Reply::ok()
        );
    }
    let r = compare::compare(
        e,
        &args(&["DIFF.COMPARE", "doc:{t/a}:b:a", "doc:{t/a}:b:b"]),
    )
    .await;
    let Reply::Array(parts) = r else {
        panic!("{r:?}")
    };
    (
        bulk(&parts[0]),
        serde_json::from_str(&bulk(&parts[1])).unwrap(),
    )
}
#[tokio::test]
async fn compare_decide_apply_retry_and_revision() {
    let (_d, e) = engine();
    let (key, g) = pair(&e).await;
    assert_eq!(g["changes"].as_array().unwrap().len(), 1);
    let id = g["changes"][0]["id"].as_str().unwrap();
    let pending = decide::decisions(&e, &args(&["DIFF.DECISIONS", &key])).await;
    let old = bulk(field(&pending, "drev"));
    assert_eq!(
        decide::decide(
            &e,
            &args(&["DIFF.DECIDE", &key, "BY", "a|b\"c", id, "accept"])
        )
        .await,
        Reply::ok()
    );
    let r = apply::apply(
        &e,
        &args(&["DIFF.APPLY", &key, "REQUEST", "old", "DECISIONS", &old]),
    )
    .await;
    assert!(
        matches!(r,Reply::Err(ref s) if s.starts_with("DIFFSTALE")),
        "{r:?}"
    );
    let done = apply::apply(&e, &args(&["DIFF.APPLY", &key, "REQUEST", "one"])).await;
    let sid = bulk(field(&done, "sid"));
    assert_eq!(
        decide::decide(&e, &args(&["DIFF.DECIDE", &key, id, "reject"])).await,
        Reply::ok()
    );
    assert_eq!(
        apply::apply(&e, &args(&["DIFF.APPLY", &key, "REQUEST", "one"])).await,
        done
    );
    let r = json::get(&e, &args(&["JSON.GET", &format!("doc:{{t/a}}:s:{sid}")])).await;
    assert!(bulk(&r).contains("may"));
}
#[tokio::test]
async fn invalid_decision_batch_writes_nothing() {
    let (_d, e) = engine();
    let (key, g) = pair(&e).await;
    let id = g["changes"][0]["id"].as_str().unwrap();
    let before = decide::decisions(&e, &args(&["DIFF.DECISIONS", &key])).await;
    let r = decide::decide(
        &e,
        &args(&["DIFF.DECIDE", &key, id, "accept", "c:unknown", "reject"]),
    )
    .await;
    assert!(matches!(r, Reply::Err(_)));
    assert_eq!(
        decide::decisions(&e, &args(&["DIFF.DECISIONS", &key])).await,
        before
    );
}
#[tokio::test]
async fn compare_same_inputs_same_bytes_cross_tag_rejected() {
    let (_d, e) = engine();
    let (key, g) = pair(&e).await;
    let r = compare::compare(
        &e,
        &args(&["DIFF.COMPARE", "doc:{t/a}:b:a", "doc:{t/a}:b:b"]),
    )
    .await;
    let Reply::Array(v) = r else { panic!("{r:?}") };
    assert_eq!(bulk(&v[0]), key);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&bulk(&v[1])).unwrap(),
        g
    );
    assert!(
        matches!(compare::compare(&e,&args(&["DIFF.COMPARE","doc:{t/a}:b:a","doc:{other}:b:b"])).await,Reply::Err(ref s) if s.starts_with("CROSSSLOT"))
    );
}

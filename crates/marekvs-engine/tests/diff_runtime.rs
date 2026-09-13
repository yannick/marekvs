use marekvs_diff::{Sid, Tree};
use marekvs_engine::cmd::diff::{
    cache::TreeCache,
    keys::{is_immutable_key, DiffKey},
    pool::{DiffConfig, DiffPool},
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
#[test]
fn strict_keys_and_reserved_prefixes() {
    for key in [
        "doc:{x}:b:main",
        "doc:{x}:s:0123456789abcdef0123456789abcdef",
        "diff:{x}:r:opaque:token",
    ] {
        assert_eq!(DiffKey::parse(key.as_bytes()).unwrap().tag(), "x");
    }
    for key in [
        "doc:{}:b:x",
        "doc:{x}:b:a{b}",
        "doc:{x}:s:ABCDEF0123456789abcdef0123456789",
        "diff:{x}:g:no",
        "doc:{x}:b:",
    ] {
        assert!(DiffKey::parse(key.as_bytes()).is_err(), "{key}");
    }
    assert!(is_immutable_key(b"doc:{x}:s:malformed"));
    assert!(is_immutable_key(b"diff:{x}:g:malformed"));
    assert!(!is_immutable_key(b"diff:{x}:d:malformed"));
}
#[tokio::test]
async fn default_admits_three_inputs_and_restores_capacity() {
    let cfg = DiffConfig::default();
    let pool = DiffPool::new(&cfg);
    let permit = pool.admit(3 * cfg.max_bytes).unwrap();
    assert_eq!(pool.run(permit, |_| Ok(42)).await.unwrap(), 42);
    assert_eq!(pool.inflight_bytes(), 0);
}
#[tokio::test]
async fn dropped_caller_does_not_release_running_worker() {
    let cfg = DiffConfig {
        threads: 1,
        inflight_bytes: 100,
        ..DiffConfig::default()
    };
    let pool = Arc::new(DiffPool::new(&cfg));
    let started = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let (p, s, r) = (pool.clone(), started.clone(), release.clone());
    let job = tokio::spawn(async move {
        let permit = p.admit(100).unwrap();
        p.run(permit, move |cancel| {
            s.store(true, Ordering::SeqCst);
            while !r.load(Ordering::SeqCst) {
                std::thread::yield_now();
            }
            cancel.check()?;
            Ok(())
        })
        .await
    });
    while !started.load(Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }
    job.abort();
    let _ = job.await;
    assert!(pool.admit(1).is_err());
    assert_eq!(pool.inflight_bytes(), 100);
    release.store(true, Ordering::SeqCst);
    for _ in 0..1000 {
        if pool.inflight_bytes() == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    assert_eq!(pool.inflight_bytes(), 0);
}
#[test]
fn cache_strips_identity_and_evicts() {
    let mut tree = Tree::from_json(&serde_json::json!({"t":"doc","c":[]})).unwrap();
    tree.nodes[0].eid = Some([1; 10]);
    let cache = TreeCache::new(100_000);
    cache.insert(Arc::new(tree.clone()));
    assert_eq!(cache.get(tree.sid).unwrap().nodes[0].eid, None);
    assert_eq!(tree.nodes[0].eid, Some([1; 10]));
    let tiny = TreeCache::new(1);
    tiny.insert(Arc::new(tree));
    assert!(tiny.get(Sid(0)).is_none());
    assert_eq!(tiny.bytes(), 0);
}

fn engine() -> (tempfile::TempDir, Arc<marekvs_engine::Engine>) {
    let dir = tempfile::tempdir().unwrap();
    let store = marekvs_engine::store::Store::open(&marekvs_engine::store::StoreConfig {
        data_dir: dir.path().to_string_lossy().into_owned(),
        node_id: 7,
        shard_threads: 1,
        ..Default::default()
    })
    .unwrap();
    (dir, marekvs_engine::Engine::new(store))
}
fn args(parts: &[&str]) -> Vec<Vec<u8>> {
    parts.iter().map(|s| s.as_bytes().to_vec()).collect()
}
async fn dispatch(
    e: &Arc<marekvs_engine::Engine>,
    parts: &[&str],
    internal: bool,
) -> marekvs_engine::reply::Reply {
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let mut sess = marekvs_engine::Session::new(1, tx);
    sess.internal = internal;
    marekvs_engine::cmd::dispatch(
        e,
        &mut sess,
        parts[0],
        args(parts),
        &mut marekvs_resp::ReplyBuf::new(false),
    )
    .await
}
#[tokio::test]
async fn immutable_guard_prevalidates_multi_key_writes_and_direct_handlers() {
    use marekvs_engine::{
        cmd::{generic, json, string},
        reply::Reply,
    };
    let (_dir, e) = engine();
    let protected = "doc:{x}:s:0123456789abcdef0123456789abcdef";
    for parts in [
        vec!["MSET", "ordinary", "first", protected, "second"],
        vec!["JSON.MSET", "ordinary", "$", "{}", protected, "$", "{}"],
        vec!["DEL", "ordinary", protected],
        vec!["LMPOP", "2", "ordinary", protected, "LEFT"],
        vec!["ZUNIONSTORE", protected, "1", "ordinary"],
        vec!["COPY", "ordinary", protected],
        vec!["PROTO.SET", protected, "bad"],
    ] {
        assert!(
            matches!(dispatch(&e,&parts,false).await,Reply::Err(msg) if msg.starts_with("DIFFIMMUTABLE")),
            "{parts:?}"
        );
    }
    assert_eq!(
        string::get(&e, &args(&["GET", "ordinary"])).await,
        Reply::Null
    );
    assert!(matches!(
        string::mset(&e, &args(&["MSET", "ordinary", "x", protected, "y"])).await,
        Reply::Err(_)
    ));
    assert!(matches!(
        json::mset(
            &e,
            &args(&["JSON.MSET", "ordinary", "$", "{}", protected, "$", "{}"])
        )
        .await,
        Reply::Err(_)
    ));
    assert!(matches!(
        generic::del(&e, &args(&["DEL", "ordinary", protected])).await,
        Reply::Err(_)
    ));
    assert_eq!(
        dispatch(&e, &["SET", protected, "replicated"], true).await,
        Reply::ok()
    );
    assert_eq!(
        dispatch(&e, &["GET", protected], false).await,
        Reply::bulk_str("replicated")
    );
    assert_eq!(
        dispatch(&e, &["COPY", protected, "copy"], false).await,
        Reply::Int(1)
    );
    assert!(matches!(
        dispatch(&e, &["APPEND", protected, "x"], false).await,
        Reply::Err(_)
    ));
    assert!(matches!(
        dispatch(&e, &["GETDEL", protected], false).await,
        Reply::Err(_)
    ));
    assert!(matches!(
        dispatch(&e, &["GETEX", protected, "PERSIST"], false).await,
        Reply::Err(_)
    ));
    assert_eq!(dispatch(&e, &["FLUSHDB"], false).await, Reply::ok());
}
#[tokio::test]
async fn lua_cannot_mutate_immutable_keys_or_start_diff_work() {
    use marekvs_engine::reply::Reply;
    let (_dir, e) = engine();
    let protected = "doc:{x}:s:0123456789abcdef0123456789abcdef";
    let reply = dispatch(
        &e,
        &[
            "EVAL",
            "return redis.call('SET', KEYS[1], 'x')",
            "1",
            protected,
        ],
        false,
    )
    .await;
    assert!(
        matches!(reply,Reply::Err(ref msg) if msg.contains("DIFFIMMUTABLE")),
        "{reply:?}"
    );
    let reply = dispatch(
        &e,
        &[
            "EVAL",
            "return redis.call('DIFF.SNAPSHOT', KEYS[1])",
            "1",
            "doc:{x}:b:main",
        ],
        false,
    )
    .await;
    assert!(
        matches!(reply,Reply::Err(ref msg) if msg.contains("not allowed from scripts")),
        "{reply:?}"
    );
    assert_eq!(e.diff.pool.inflight_requests(), 0);
}
#[tokio::test]
async fn deadline_cancels_worker_without_releasing_early() {
    let cfg = DiffConfig {
        threads: 1,
        inflight_bytes: 100,
        time_limit_ms: 10,
        ..DiffConfig::default()
    };
    let pool = DiffPool::new(&cfg);
    let release = Arc::new(AtomicBool::new(false));
    let r = release.clone();
    let result = pool
        .run(pool.admit(100).unwrap(), move |cancel| {
            while !r.load(Ordering::SeqCst) {
                std::thread::yield_now();
            }
            cancel.check()?;
            Ok(())
        })
        .await;
    assert!(result.is_err());
    assert_eq!(pool.inflight_bytes(), 100);
    release.store(true, Ordering::SeqCst);
    for _ in 0..1000 {
        if pool.inflight_bytes() == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    assert_eq!(pool.inflight_bytes(), 0);
}
#[tokio::test]
async fn queue_wait_has_its_own_deadline() {
    let cfg = DiffConfig {
        threads: 1,
        inflight_bytes: 100,
        queue_ms: 10,
        time_limit_ms: 1000,
        ..DiffConfig::default()
    };
    let pool = Arc::new(DiffPool::new(&cfg));
    let started = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let (p, s, r) = (pool.clone(), started.clone(), release.clone());
    let first = tokio::spawn(async move {
        p.run(p.admit(50).unwrap(), move |_| {
            s.store(true, Ordering::SeqCst);
            while !r.load(Ordering::SeqCst) {
                std::thread::yield_now();
            }
            Ok(())
        })
        .await
    });
    while !started.load(Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(150),
        pool.run(pool.admit(50).unwrap(), |_| Ok(())),
    )
    .await;
    release.store(true, Ordering::SeqCst);
    first.await.unwrap().unwrap();
    assert!(
        matches!(result, Ok(Err(_))),
        "queue deadline must respond independently of blocked worker"
    );
}
#[test]
fn cache_evicts_least_recently_used_semantic_entry() {
    let trees: Vec<_> = ["a", "b", "c"]
        .into_iter()
        .map(|text| {
            Arc::new(
                Tree::from_json(&serde_json::json!({"t":"doc","c":[{"t":"sen","x":text}]}))
                    .unwrap(),
            )
        })
        .collect();
    let measured = TreeCache::new(100_000);
    measured.insert(trees[0].clone());
    let cache = TreeCache::new(2 * measured.bytes());
    cache.insert(trees[0].clone());
    cache.insert(trees[1].clone());
    assert!(cache.get(trees[0].sid).is_some());
    cache.insert(trees[2].clone());
    assert!(cache.get(trees[0].sid).is_some());
    assert!(cache.get(trees[1].sid).is_none());
    assert!(cache.get(trees[2].sid).is_some());
}
#[tokio::test]
async fn worker_panic_restores_capacity_and_worker_survives() {
    let cfg = DiffConfig {
        threads: 1,
        ..DiffConfig::default()
    };
    let pool = DiffPool::new(&cfg);
    let result = pool
        .run(
            pool.admit(10).unwrap(),
            |_| -> Result<(), marekvs_diff::DiffError> { panic!("injected worker panic") },
        )
        .await;
    assert!(result.is_err());
    assert_eq!(pool.inflight_bytes(), 0);
    assert_eq!(
        pool.run(pool.admit(10).unwrap(), |_| Ok(7)).await.unwrap(),
        7
    );
}
#[tokio::test]
async fn successful_stage_keeps_admission_valid_for_publication() {
    let pool = DiffPool::new(&DiffConfig::default());
    let admission = pool.admit(10).unwrap();
    pool.run(admission.clone(), |_| Ok(())).await.unwrap();
    assert!(admission.cancel.check().is_ok());
    assert_eq!(pool.inflight_bytes(), 10);
    drop(admission);
    assert_eq!(pool.inflight_bytes(), 0);
}
#[test]
fn command_cancellation_keeps_accounting_until_shard_guard_drops() {
    let pool = DiffPool::new(&DiffConfig::default());
    let admission = pool.admit(10).unwrap();
    let command_guard = admission.cancel_on_drop();
    let shard_guard = admission.clone();
    drop(admission);
    drop(command_guard);
    assert!(shard_guard.cancel.check().is_err());
    assert_eq!(pool.inflight_bytes(), 10);
    drop(shard_guard);
    assert_eq!(pool.inflight_bytes(), 0);
}
#[tokio::test]
async fn last_pool_owner_can_drop_on_its_worker_thread() {
    let pool = Arc::new(DiffPool::new(&DiffConfig {
        threads: 1,
        ..DiffConfig::default()
    }));
    let release = Arc::new(AtomicBool::new(false));
    let started = Arc::new(AtomicBool::new(false));
    let (p, p2, r, s) = (pool.clone(), pool.clone(), release.clone(), started.clone());
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let request = tokio::spawn(async move {
        p.run(p.admit(1).unwrap(), move |_| {
            s.store(true, Ordering::SeqCst);
            while !r.load(Ordering::SeqCst) {
                std::thread::yield_now();
            }
            drop(p2);
            let _ = done_tx.send(());
            Ok(())
        })
        .await
    });
    drop(pool);
    while !started.load(Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }
    request.abort();
    let _ = request.await;
    release.store(true, Ordering::SeqCst);
    tokio::time::timeout(std::time::Duration::from_secs(1), done_rx)
        .await
        .unwrap()
        .unwrap();
}
#[tokio::test]
async fn all_diff_mutators_obey_write_stop_before_storage() {
    use marekvs_engine::{reply::Reply, Engine};
    let (_dir, e) = engine();
    e.write_stopped.store(true, Ordering::Relaxed);
    for verb in [
        "DIFF.SNAPSHOT",
        "DIFF.FORK",
        "DIFF.COMPARE",
        "DIFF.IMPORT",
        "DIFF.DECIDE",
        "DIFF.APPLY",
        "DIFF.MERGE3",
    ] {
        assert!(Engine::is_write_command(verb), "{verb}");
        assert!(
            matches!(dispatch(&e,&[verb],false).await,Reply::Err(msg) if msg.starts_with("MISCONF")),
            "{verb}"
        );
        assert!(!Engine::parallel_safe(verb));
    }
    for verb in ["DIFF.HASH", "DIFF.STATS", "DIFF.DECISIONS"] {
        assert!(!Engine::is_write_command(verb));
        assert!(!Engine::parallel_safe(verb));
    }
    assert_eq!(e.diff.pool.inflight_requests(), 0);
}

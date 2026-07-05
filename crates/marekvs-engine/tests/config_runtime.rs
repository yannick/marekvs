//! Runtime-settable config knobs (design/05 §Defaults table): CONFIG SET
//! requirepass / lua-time-limit / loglevel apply live; unknown keys stay
//! accepted-but-ignored for client compat.

use std::sync::Arc;

use marekvs_engine::cmd::server;
use marekvs_engine::reply::Reply;
use marekvs_engine::store::{Store, StoreConfig};
use marekvs_engine::{Engine, Session};

fn test_engine(dir: &tempfile::TempDir) -> Arc<Engine> {
    let store = Store::open(&StoreConfig {
        data_dir: dir.path().to_string_lossy().into_owned(),
        node_id: 7,
        shard_threads: 2,
        ..StoreConfig::default()
    })
    .unwrap();
    Engine::new(store)
}

fn args(parts: &[&str]) -> Vec<Vec<u8>> {
    parts.iter().map(|p| p.as_bytes().to_vec()).collect()
}

fn get_one(engine: &Arc<Engine>, key: &str) -> Option<String> {
    match server::config(engine, &args(&["CONFIG", "GET", key])) {
        Reply::Map(pairs) => pairs.into_iter().find_map(|(k, v)| match (k, v) {
            (Reply::Bulk(k), Reply::Bulk(v)) if k == key.as_bytes() => {
                Some(String::from_utf8(v).unwrap())
            }
            _ => None,
        }),
        r => panic!("CONFIG GET returned {r:?}"),
    }
}

#[tokio::test]
async fn requirepass_applies_live() {
    let dir = tempfile::tempdir().unwrap();
    let engine = test_engine(&dir);
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let mut sess = Session::new(1, tx);

    // No password: AUTH errors, sessions start authenticated.
    assert!(engine.requirepass.read().is_empty());
    assert!(matches!(
        server::auth(&engine, &mut sess, &args(&["AUTH", "x"])),
        Reply::Err(_)
    ));

    // CONFIG SET requirepass takes effect for subsequent AUTH.
    assert!(matches!(
        server::config(&engine, &args(&["CONFIG", "SET", "requirepass", "s3cret"])),
        Reply::Simple("OK")
    ));
    assert_eq!(get_one(&engine, "requirepass").as_deref(), Some("s3cret"));
    assert!(matches!(
        server::auth(&engine, &mut sess, &args(&["AUTH", "wrong"])),
        Reply::Err(_)
    ));
    assert!(matches!(
        server::auth(&engine, &mut sess, &args(&["AUTH", "s3cret"])),
        Reply::Simple("OK")
    ));
    assert!(sess.authenticated);

    // Clearing disables auth again.
    server::config(&engine, &args(&["CONFIG", "SET", "requirepass", ""]));
    assert!(engine.requirepass.read().is_empty());
}

#[tokio::test]
async fn lua_time_limit_applies_live() {
    let dir = tempfile::tempdir().unwrap();
    let engine = test_engine(&dir);
    assert_eq!(get_one(&engine, "lua-time-limit").as_deref(), Some("20"));

    assert!(matches!(
        server::config(&engine, &args(&["CONFIG", "SET", "lua-time-limit", "250"])),
        Reply::Simple("OK")
    ));
    assert_eq!(
        engine
            .script_time_limit_ms
            .load(std::sync::atomic::Ordering::Relaxed),
        250
    );
    // Redis 7 alias reads the same value.
    assert_eq!(
        get_one(&engine, "busy-reply-threshold").as_deref(),
        Some("250")
    );
    // Non-integer → error, value untouched.
    assert!(matches!(
        server::config(&engine, &args(&["CONFIG", "SET", "lua-time-limit", "abc"])),
        Reply::Err(_)
    ));
    assert_eq!(get_one(&engine, "lua-time-limit").as_deref(), Some("250"));
}

#[tokio::test]
async fn loglevel_uses_installed_hook() {
    let dir = tempfile::tempdir().unwrap();
    let engine = test_engine(&dir);

    // No hook installed (embedded use): explicit error, not silent OK.
    assert!(matches!(
        server::config(&engine, &args(&["CONFIG", "SET", "loglevel", "debug"])),
        Reply::Err(_)
    ));

    // Hook installed: Redis levels map to tracing directives.
    let seen = Arc::new(parking_lot::Mutex::new(Vec::<String>::new()));
    let sink = seen.clone();
    engine.set_log_reload(
        "info,chitchat=warn".into(),
        Arc::new(move |spec| {
            sink.lock().push(spec.to_string());
            if spec == "bad(" {
                return Err("invalid filter".into());
            }
            Ok(())
        }),
    );
    assert_eq!(
        get_one(&engine, "loglevel").as_deref(),
        Some("info,chitchat=warn")
    );
    for (level, expect) in [
        ("debug", "trace"),
        ("verbose", "debug"),
        ("notice", "info"),
        ("warning", "warn"),
        ("nothing", "off"),
        ("info,chitchat=debug", "info,chitchat=debug"), // raw EnvFilter spec
    ] {
        assert!(matches!(
            server::config(&engine, &args(&["CONFIG", "SET", "loglevel", level])),
            Reply::Simple("OK")
        ));
        assert_eq!(seen.lock().last().map(|s| s.as_str()), Some(expect));
        assert_eq!(get_one(&engine, "loglevel").as_deref(), Some(expect));
    }
    // Hook failure propagates and does NOT update the reported filter.
    assert!(matches!(
        server::config(&engine, &args(&["CONFIG", "SET", "loglevel", "bad("])),
        Reply::Err(_)
    ));
    assert_eq!(
        get_one(&engine, "loglevel").as_deref(),
        Some("info,chitchat=debug")
    );
}

#[tokio::test]
async fn unknown_keys_stay_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let engine = test_engine(&dir);
    // redis-benchmark & friends set these; must stay OK-and-ignored.
    assert!(matches!(
        server::config(&engine, &args(&["CONFIG", "SET", "maxmemory", "100mb"])),
        Reply::Simple("OK")
    ));
    // Multi-pair form (Redis 7).
    assert!(matches!(
        server::config(
            &engine,
            &args(&["CONFIG", "SET", "appendonly", "no", "lua-time-limit", "77"])
        ),
        Reply::Simple("OK")
    ));
    assert_eq!(get_one(&engine, "lua-time-limit").as_deref(), Some("77"));
    // Odd arg count → error.
    assert!(matches!(
        server::config(&engine, &args(&["CONFIG", "SET", "requirepass"])),
        Reply::Err(_)
    ));
}

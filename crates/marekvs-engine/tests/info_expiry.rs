use marekvs_core::{
    envelope::{head, Envelope, RecordType},
    ikey,
};
use marekvs_engine::{
    cmd::{generic, server},
    reply::Reply,
    store::{self, Store, StoreConfig},
    Engine,
};

#[tokio::test]
async fn info_counts_only_live_key_deadlines_and_obeys_sections() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&StoreConfig {
        data_dir: dir.path().to_string_lossy().into_owned(),
        shard_threads: 2,
        ..StoreConfig::default()
    })
    .unwrap();
    let engine = Engine::new(store.clone());
    let deadline = store::now_ms() + 60_000;
    for (key, ttl) in [
        (b"persistent".as_slice(), 0),
        (b"expiring", deadline),
        (b"expired", 1),
    ] {
        let key = key.to_vec();
        store
            .run_key(&key.clone(), move |ctx| {
                let value = store::new_lww(ctx, RecordType::String, b"value", ttl);
                store::put_raw(ctx, &ikey::string_key(&key), &value);
            })
            .await;
    }
    for (key, head_ttl, member_ttl) in [
        (b"head-ttl".as_slice(), deadline, 0),
        (b"member-ttl", 0, deadline),
        (b"deleted", 0, 0),
    ] {
        let key = key.to_vec();
        store
            .run_key(&key.clone(), move |ctx| {
                store::ensure_head(ctx, &key, head::CTYPE_HASH);
                let raw = store::get_raw(ctx, &ikey::head_key(&key)).unwrap();
                let (mut env, pay) = Envelope::decode(&raw).unwrap();
                env.ttl_deadline_ms = head_ttl;
                store::put_raw(ctx, &ikey::head_key(&key), &env.encode_with(pay));
                let value = marekvs_core::merge::element_add(
                    RecordType::HashField,
                    ctx.hlc.now(),
                    ctx.node_id,
                    b"value",
                );
                let (mut env, pay) = Envelope::decode(&value).unwrap();
                env.ttl_deadline_ms = member_ttl;
                store::put_raw(
                    ctx,
                    &ikey::hash_field_key(&key, b"field"),
                    &env.encode_with(pay),
                );
                if key == b"deleted" {
                    generic::del_key(ctx, &key);
                }
            })
            .await;
    }
    let Reply::Bulk(info) = server::info(&engine, &[b"INFO".to_vec(), b"keyspace".to_vec()]).await
    else {
        panic!("expected INFO bulk");
    };
    let info = String::from_utf8(info).unwrap();
    assert!(info.contains("db0:keys=4,expires=2,avg_ttl="), "{info}");
    let avg: u64 = info
        .split("avg_ttl=")
        .nth(1)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(
        (deadline.saturating_sub(store::now_ms())..=60_000).contains(&avg),
        "{avg}"
    );
    assert!(!info.contains("# Server"));
    let Reply::Bulk(info) = server::info(&engine, &[b"INFO".to_vec(), b"server".to_vec()]).await
    else {
        panic!("expected INFO bulk");
    };
    assert!(!String::from_utf8(info).unwrap().contains("# Keyspace"));
    assert!(matches!(server::dbsize(&engine).await, Reply::Int(4)));
}

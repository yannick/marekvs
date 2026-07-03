//! Regression: a collection recreated after DEL must carry the previous
//! delete clock forward, so stale pre-delete elements (arriving later via
//! replication/anti-entropy) stay dead (design/02 §Whole-collection delete).

use marekvs_core::envelope::{head, RecordType};
use marekvs_core::ikey;
use marekvs_core::merge::element_add;
use marekvs_engine::cmd::generic::del_key;
use marekvs_engine::store::{
    check_type, ensure_head, read_element, write_merged, Store, StoreConfig,
};

fn test_store(dir: &tempfile::TempDir) -> std::sync::Arc<Store> {
    Store::open(&StoreConfig {
        data_dir: dir.path().to_string_lossy().into_owned(),
        node_id: 7,
        shard_threads: 2,
        ..StoreConfig::default()
    })
    .unwrap()
}

#[tokio::test]
async fn recreated_collection_keeps_delete_clock() {
    let dir = tempfile::tempdir().unwrap();
    let store = test_store(&dir);
    let key = b"myset".to_vec();

    let (pre_del_member, del_clock) = store
        .run_key(&key, {
            let key = key.clone();
            move |ctx| {
                // Create the set with one member.
                ensure_head(ctx, &key, head::CTYPE_SET);
                let add = element_add(RecordType::SetMember, ctx.hlc.now(), ctx.node_id, &[]);
                let mkey = ikey::set_member_key(&key, b"old");
                write_merged(ctx, &mkey, &add);
                assert!(read_element(ctx, &mkey, 0).is_some());

                // DEL the collection.
                assert!(del_key(ctx, &key));

                // Recreate (as SADD would): the delete clock must survive.
                let del = ensure_head(ctx, &key, head::CTYPE_SET);
                assert!(del > 0, "recreated head lost the delete clock");
                let visible_del = check_type(ctx, &key, head::CTYPE_SET).unwrap();
                (mkey, visible_del.max(del))
            }
        })
        .await;

    // A stale pre-delete element record replayed later (e.g. from a lagging
    // replica) must remain invisible under the carried-forward clock.
    store
        .run_key(&key, move |ctx| {
            assert!(
                read_element(ctx, &pre_del_member, del_clock).is_none(),
                "stale pre-delete member resurrected"
            );
            // But a genuinely new add (fresh HLC > delete clock) is visible.
            let add = element_add(RecordType::SetMember, ctx.hlc.now(), ctx.node_id, &[]);
            let mkey = ikey::set_member_key(b"myset", b"new");
            write_merged(ctx, &mkey, &add);
            assert!(read_element(ctx, &mkey, del_clock).is_some());
        })
        .await;
}

#[tokio::test]
async fn check_type_gates_elements_after_del() {
    let dir = tempfile::tempdir().unwrap();
    let store = test_store(&dir);
    store
        .run_key(b"h", |ctx| {
            ensure_head(ctx, b"h", head::CTYPE_HASH);
            let add = element_add(RecordType::HashField, ctx.hlc.now(), ctx.node_id, b"v");
            write_merged(ctx, &ikey::hash_field_key(b"h", b"f"), &add);
            del_key(ctx, b"h");
            // After DEL: reads through check_type's clock see nothing.
            let del = check_type(ctx, b"h", head::CTYPE_HASH).unwrap();
            assert!(read_element(ctx, &ikey::hash_field_key(b"h", b"f"), del).is_none());
        })
        .await;
}

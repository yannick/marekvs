//! PROTO.* engine integration tests (design/17): core plumbing, schema
//! registry, bindings, typed values, field access, collection elements.

use std::sync::Arc;

use marekvs_core::envelope::{head, Envelope};
use marekvs_core::{ikey, protohead};
use marekvs_engine::cmd::generic;
use marekvs_engine::reply::Reply;
use marekvs_engine::store::{write_merged, Store, StoreConfig};
use marekvs_engine::Engine;

fn engine() -> (tempfile::TempDir, Arc<Engine>) {
    let dir = tempfile::tempdir().unwrap();
    let e = open(&dir, 7);
    (dir, e)
}

fn open(dir: &tempfile::TempDir, node_id: u16) -> Arc<Engine> {
    let store = Store::open(&StoreConfig {
        data_dir: dir.path().to_string_lossy().into_owned(),
        node_id,
        shard_threads: 2,
        ..StoreConfig::default()
    })
    .unwrap();
    Engine::new(store)
}

fn a(parts: &[&[u8]]) -> Vec<Vec<u8>> {
    parts.iter().map(|p| p.to_vec()).collect()
}

#[allow(dead_code)]
fn int(r: Reply) -> i64 {
    match r {
        Reply::Int(n) => n,
        other => panic!("expected Int, got {other:?}"),
    }
}

fn bulk(r: Reply) -> Vec<u8> {
    match r {
        Reply::Bulk(b) => b,
        other => panic!("expected Bulk, got {other:?}"),
    }
}

fn simple(r: Reply) -> String {
    match r {
        Reply::Simple(s) => s.to_string(),
        Reply::SimpleOwned(s) => s,
        other => panic!("expected Simple, got {other:?}"),
    }
}

/// Write a raw proto head record directly (plumbing tests run below the
/// command layer, before the PROTO.* handlers exist).
async fn write_raw_proto(e: &Arc<Engine>, key: &[u8], schema: &str, ver: u32, tname: &str, msg: &[u8]) {
    let key = key.to_vec();
    let (schema, tname, msg) = (schema.to_string(), tname.to_string(), msg.to_vec());
    e.store
        .run_key(&key.clone(), move |ctx| {
            let mut payload = head::encode(head::CTYPE_PROTO, 0);
            payload.extend_from_slice(&protohead::encode(&schema, ver, &tname, &msg));
            let env = Envelope::head(ctx.hlc.now(), ctx.node_id);
            write_merged(ctx, &ikey::head_key(&key), &env.encode_with(&payload));
        })
        .await;
}

// ---------------------------------------------------------------------------
// B1 — core plumbing: TYPE / OBJECT ENCODING / EXISTS / DEL / RENAME / EXPIRE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn type_reports_proto() {
    let (_d, e) = engine();
    write_raw_proto(&e, b"k", "orders", 1, "shop.v1.Order", b"\x08\x2a").await;
    assert_eq!(simple(generic::type_cmd(&e, &a(&[b"TYPE", b"k"])).await), "proto");
    assert_eq!(int(generic::exists(&e, &a(&[b"EXISTS", b"k"])).await), 1);
}

#[tokio::test]
async fn object_encoding_reports_fq_type() {
    let (_d, e) = engine();
    write_raw_proto(&e, b"k", "orders", 2, "shop.v1.Order", b"").await;
    let enc = bulk(generic::object(&e, &a(&[b"OBJECT", b"ENCODING", b"k"])).await);
    assert_eq!(enc, b"shop.v1.Order");
}

#[tokio::test]
async fn del_removes_proto_value() {
    let (_d, e) = engine();
    write_raw_proto(&e, b"k", "s", 1, "pkg.T", b"x").await;
    assert_eq!(int(generic::del(&e, &a(&[b"DEL", b"k"])).await), 1);
    assert_eq!(simple(generic::type_cmd(&e, &a(&[b"TYPE", b"k"])).await), "none");
}

#[tokio::test]
async fn rename_and_copy_preserve_proto_tail() {
    let (_d, e) = engine();
    write_raw_proto(&e, b"src", "s", 4, "pkg.T", b"body").await;
    generic::rename(&e, &a(&[b"RENAME", b"src", b"dst"]), false).await;
    assert_eq!(simple(generic::type_cmd(&e, &a(&[b"TYPE", b"dst"])).await), "proto");
    assert_eq!(
        bulk(generic::object(&e, &a(&[b"OBJECT", b"ENCODING", b"dst"])).await),
        b"pkg.T"
    );
    assert_eq!(simple(generic::type_cmd(&e, &a(&[b"TYPE", b"src"])).await), "none");

    assert_eq!(int(generic::copy(&e, &a(&[b"COPY", b"dst", b"c2"])).await), 1);
    assert_eq!(
        bulk(generic::object(&e, &a(&[b"OBJECT", b"ENCODING", b"c2"])).await),
        b"pkg.T"
    );
}

#[tokio::test]
async fn expire_preserves_proto_tail() {
    let (_d, e) = engine();
    write_raw_proto(&e, b"k", "s", 1, "pkg.T", b"x").await;
    assert_eq!(
        int(generic::expire(&e, &a(&[b"EXPIRE", b"k", b"100"]), 1000, false).await),
        1
    );
    let ttl = int(generic::ttl(&e, &a(&[b"TTL", b"k"]), false).await);
    assert!(ttl > 90 && ttl <= 100, "ttl {ttl}");
    // The head tail (and thus OBJECT ENCODING) must survive the re-stamp.
    assert_eq!(
        bulk(generic::object(&e, &a(&[b"OBJECT", b"ENCODING", b"k"])).await),
        b"pkg.T"
    );
    // PERSIST keeps it too.
    assert_eq!(int(generic::persist(&e, &a(&[b"PERSIST", b"k"])).await), 1);
    assert_eq!(
        bulk(generic::object(&e, &a(&[b"OBJECT", b"ENCODING", b"k"])).await),
        b"pkg.T"
    );
}

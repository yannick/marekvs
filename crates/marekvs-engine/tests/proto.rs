//! PROTO.* engine integration tests (design/17): core plumbing, schema
//! registry, bindings, typed values, field access, collection elements.

use std::sync::Arc;

use marekvs_core::envelope::{head, Envelope};
use marekvs_core::{ikey, protohead};
use marekvs_engine::cmd::{generic, proto as proto_cmd};
use marekvs_engine::proto::registry;
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

// ---------------------------------------------------------------------------
// shared fixtures + reply helpers (B3+)
// ---------------------------------------------------------------------------

const ORDER_SRC: &str = r#"
    syntax = "proto3";
    package shop.v1;
    message Order {
        string id = 1;
        uint64 total_cents = 2;
        repeated string tags = 3;
        map<string, int64> scores = 4;
        Customer customer = 5;
        double ratio = 6;
        bool paid = 7;
    }
    message Customer { string name = 1; int32 tier = 2; }
"#;

fn assert_err_contains(r: Reply, needle: &str) {
    match r {
        Reply::Err(e) => assert!(e.contains(needle), "error {e:?} did not contain {needle:?}"),
        other => panic!("expected Err containing {needle:?}, got {other:?}"),
    }
}

fn map_get(r: &Reply, key: &str) -> Reply {
    match r {
        Reply::Map(pairs) => pairs
            .iter()
            .find_map(|(k, v)| match k {
                Reply::Bulk(k) if k == key.as_bytes() => Some(v.clone()),
                _ => None,
            })
            .unwrap_or_else(|| panic!("missing map field {key} in {r:?}")),
        other => panic!("expected Map, got {other:?}"),
    }
}

fn array(r: Reply) -> Vec<Reply> {
    match r {
        Reply::Array(items) => items,
        other => panic!("expected Array, got {other:?}"),
    }
}

async fn schema_set_source(e: &Arc<Engine>, name: &[u8], src: &str) -> Reply {
    proto_cmd::schema(
        e,
        &a(&[b"PROTO.SCHEMA", b"SET", name, b"SOURCE", src.as_bytes()]),
    )
    .await
}

// ---------------------------------------------------------------------------
// B3 — PROTO.SCHEMA registry + PROTO.BIND/UNBIND/BINDINGS
// ---------------------------------------------------------------------------

#[tokio::test]
async fn schema_set_versioning_increments() {
    let (_d, e) = engine();
    assert_eq!(int(schema_set_source(&e, b"orders", ORDER_SRC).await), 1);
    assert_eq!(int(schema_set_source(&e, b"orders", ORDER_SRC).await), 2);

    let list = proto_cmd::schema(&e, &a(&[b"PROTO.SCHEMA", b"LIST"])).await;
    assert_eq!(map_get(&list, "orders"), Reply::Int(2));

    let g = proto_cmd::schema(&e, &a(&[b"PROTO.SCHEMA", b"GET", b"orders"])).await;
    assert_eq!(map_get(&g, "version"), Reply::Int(2));
    assert_eq!(map_get(&g, "kind"), Reply::bulk_str("source"));
    let types = array(map_get(&g, "types"));
    assert!(types.contains(&Reply::bulk_str("shop.v1.Order")), "{types:?}");

    // Old version stays addressable.
    let g1 = proto_cmd::schema(
        &e,
        &a(&[b"PROTO.SCHEMA", b"GET", b"orders", b"VERSION", b"1"]),
    )
    .await;
    assert_eq!(map_get(&g1, "version"), Reply::Int(1));

    // GET ... SOURCE returns the original text.
    let src = proto_cmd::schema(
        &e,
        &a(&[b"PROTO.SCHEMA", b"GET", b"orders", b"SOURCE"]),
    )
    .await;
    assert_eq!(bulk(src), ORDER_SRC.as_bytes());
}

#[tokio::test]
async fn schema_types_and_del_retains_versions() {
    let (_d, e) = engine();
    assert_eq!(int(schema_set_source(&e, b"orders", ORDER_SRC).await), 1);
    let types = array(
        proto_cmd::schema(&e, &a(&[b"PROTO.SCHEMA", b"TYPES", b"orders"])).await,
    );
    assert_eq!(
        types,
        vec![
            Reply::bulk_str("shop.v1.Customer"),
            Reply::bulk_str("shop.v1.Order")
        ]
    );

    assert_eq!(
        int(proto_cmd::schema(&e, &a(&[b"PROTO.SCHEMA", b"DEL", b"orders"])).await),
        1
    );
    // Latest is gone…
    assert_err_contains(
        proto_cmd::schema(&e, &a(&[b"PROTO.SCHEMA", b"GET", b"orders"])).await,
        "NOSCHEMA",
    );
    let list = proto_cmd::schema(&e, &a(&[b"PROTO.SCHEMA", b"LIST"])).await;
    assert_eq!(list, Reply::Map(vec![]));
    // …but the immutable version record survives (stored values decode).
    let g1 = proto_cmd::schema(
        &e,
        &a(&[b"PROTO.SCHEMA", b"GET", b"orders", b"VERSION", b"1"]),
    )
    .await;
    assert_eq!(map_get(&g1, "version"), Reply::Int(1));
}

#[tokio::test]
async fn schema_compile_is_dry_run() {
    let (_d, e) = engine();
    let r = proto_cmd::schema(
        &e,
        &a(&[b"PROTO.SCHEMA", b"COMPILE", b"x", b"SOURCE", ORDER_SRC.as_bytes()]),
    )
    .await;
    let types = array(r);
    assert!(types.contains(&Reply::bulk_str("shop.v1.Order")));
    // Nothing stored.
    let list = proto_cmd::schema(&e, &a(&[b"PROTO.SCHEMA", b"LIST"])).await;
    assert_eq!(list, Reply::Map(vec![]));
}

#[tokio::test]
async fn schema_compile_error_surfaces_schemaerr() {
    let (_d, e) = engine();
    assert_err_contains(
        schema_set_source(&e, b"bad", "message Broken {").await,
        "SCHEMAERR",
    );
    assert_err_contains(
        schema_set_source(
            &e,
            b"needy",
            "syntax = \"proto3\"; import \"missing.proto\"; message X { int32 a = 1; }",
        )
        .await,
        "missing.proto",
    );
}

#[tokio::test]
async fn schema_import_chain_resolves_from_registry() {
    let (_d, e) = engine();
    let common = r#"
        syntax = "proto3";
        package shop.common;
        message Money { int64 cents = 1; }
    "#;
    assert_eq!(int(schema_set_source(&e, b"common", common).await), 1);
    let main = r#"
        syntax = "proto3";
        package shop.v1;
        import "common.proto";
        message Invoice { shop.common.Money total = 1; }
    "#;
    assert_eq!(int(schema_set_source(&e, b"invoices", main).await), 1);
    let types = array(
        proto_cmd::schema(&e, &a(&[b"PROTO.SCHEMA", b"TYPES", b"invoices"])).await,
    );
    assert!(types.contains(&Reply::bulk_str("shop.v1.Invoice")));
    // Self-contained: imported type is in the stored set too.
    assert!(types.contains(&Reply::bulk_str("shop.common.Money")));
}

#[tokio::test]
async fn schema_descriptor_upload() {
    let (_d, e) = engine();
    // Build the FDS with protox inside the test.
    let out = marekvs_engine::proto::compile::compile_source(
        "orders",
        ORDER_SRC,
        Default::default(),
        &marekvs_engine::proto::ProtoLimits::from_env(),
    )
    .unwrap();
    let r = proto_cmd::schema(
        &e,
        &a(&[b"PROTO.SCHEMA", b"SET", b"orders", b"DESCRIPTOR", &out.fds]),
    )
    .await;
    assert_eq!(int(r), 1);
    let g = proto_cmd::schema(&e, &a(&[b"PROTO.SCHEMA", b"GET", b"orders"])).await;
    assert_eq!(map_get(&g, "kind"), Reply::bulk_str("descriptor"));
    // GET ... DESCRIPTOR round-trips the bytes.
    let fds = proto_cmd::schema(
        &e,
        &a(&[b"PROTO.SCHEMA", b"GET", b"orders", b"DESCRIPTOR"]),
    )
    .await;
    assert_eq!(bulk(fds), out.fds);
    // Garbage rejected.
    assert_err_contains(
        proto_cmd::schema(
            &e,
            &a(&[b"PROTO.SCHEMA", b"SET", b"junk", b"DESCRIPTOR", b"\xff\xffgarbage"]),
        )
        .await,
        "SCHEMAERR",
    );
}

#[tokio::test]
async fn schema_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let e = open(&dir, 7);
        assert_eq!(int(schema_set_source(&e, b"orders", ORDER_SRC).await), 1);
        proto_cmd::bind(&e, &a(&[b"PROTO.BIND", b"order:", b"shop.v1.Order"])).await;
    } // drop → store closed
    let e = open(&dir, 7);
    let g = proto_cmd::schema(&e, &a(&[b"PROTO.SCHEMA", b"GET", b"orders"])).await;
    assert_eq!(map_get(&g, "version"), Reply::Int(1));
    let b = proto_cmd::bindings_cmd(&e, &a(&[b"PROTO.BINDINGS"])).await;
    let entry = map_get(&b, "order:");
    assert_eq!(map_get(&entry, "type"), Reply::bulk_str("shop.v1.Order"));
}

#[tokio::test]
async fn bind_longest_prefix_and_unbind() {
    let (_d, e) = engine();
    assert_eq!(int(schema_set_source(&e, b"orders", ORDER_SRC).await), 1);
    ok(proto_cmd::bind(&e, &a(&[b"PROTO.BIND", b"user:", b"shop.v1.Customer"])).await);
    ok(proto_cmd::bind(
        &e,
        &a(&[b"PROTO.BIND", b"user:order:", b"shop.v1.Order", b"SCHEMA", b"orders"]),
    )
    .await);

    let b = registry::binding_for_key(&e, b"user:order:1").await.unwrap();
    assert_eq!(b.type_name, "shop.v1.Order");
    let b = registry::binding_for_key(&e, b"user:1").await.unwrap();
    assert_eq!(b.type_name, "shop.v1.Customer");
    assert!(registry::binding_for_key(&e, b"other:1").await.is_none());

    // MATCH filter on BINDINGS
    let filtered = proto_cmd::bindings_cmd(&e, &a(&[b"PROTO.BINDINGS", b"MATCH", b"user:order*"])).await;
    match &filtered {
        Reply::Map(pairs) => assert_eq!(pairs.len(), 1, "{filtered:?}"),
        other => panic!("expected Map, got {other:?}"),
    }

    assert_eq!(
        int(proto_cmd::unbind(&e, &a(&[b"PROTO.UNBIND", b"user:order:"])).await),
        1
    );
    assert_eq!(
        int(proto_cmd::unbind(&e, &a(&[b"PROTO.UNBIND", b"user:order:"])).await),
        0
    );
    let b = registry::binding_for_key(&e, b"user:order:1").await.unwrap();
    assert_eq!(b.type_name, "shop.v1.Customer"); // falls back to shorter prefix
}

#[tokio::test]
async fn bind_unknown_and_ambiguous_types_error() {
    let (_d, e) = engine();
    assert_err_contains(
        proto_cmd::bind(&e, &a(&[b"PROTO.BIND", b"x:", b"no.such.Type"])).await,
        "NOSCHEMA",
    );
    assert_eq!(int(schema_set_source(&e, b"a", ORDER_SRC).await), 1);
    assert_eq!(int(schema_set_source(&e, b"b", ORDER_SRC).await), 1);
    // Same type in two schemas: ambiguous without SCHEMA…
    assert_err_contains(
        proto_cmd::bind(&e, &a(&[b"PROTO.BIND", b"x:", b"shop.v1.Order"])).await,
        "ambiguous",
    );
    // …fine with SCHEMA.
    ok(proto_cmd::bind(
        &e,
        &a(&[b"PROTO.BIND", b"x:", b"shop.v1.Order", b"SCHEMA", b"b"]),
    )
    .await);
    // Binding to a type the schema does not define fails.
    assert_err_contains(
        proto_cmd::bind(
            &e,
            &a(&[b"PROTO.BIND", b"y:", b"shop.v1.Nope", b"SCHEMA", b"a"]),
        )
        .await,
        "NOSCHEMA",
    );
}

#[tokio::test]
async fn hidden_registry_keys_stay_hidden() {
    let (_d, e) = engine();
    assert_eq!(int(schema_set_source(&e, b"orders", ORDER_SRC).await), 1);
    ok(proto_cmd::bind(&e, &a(&[b"PROTO.BIND", b"o:", b"shop.v1.Order"])).await);
    let keys = array(generic::keys(&e, &a(&[b"KEYS", b"*"])).await);
    assert!(keys.is_empty(), "registry keys leaked into KEYS: {keys:?}");
}

fn ok(r: Reply) {
    assert_eq!(r, Reply::Simple("OK"));
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

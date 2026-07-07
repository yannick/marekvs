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
async fn write_raw_proto(
    e: &Arc<Engine>,
    key: &[u8],
    schema: &str,
    ver: u32,
    tname: &str,
    msg: &[u8],
) {
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
    assert_eq!(
        simple(generic::type_cmd(&e, &a(&[b"TYPE", b"k"])).await),
        "proto"
    );
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
    assert_eq!(
        simple(generic::type_cmd(&e, &a(&[b"TYPE", b"k"])).await),
        "none"
    );
}

#[tokio::test]
async fn rename_and_copy_preserve_proto_tail() {
    let (_d, e) = engine();
    write_raw_proto(&e, b"src", "s", 4, "pkg.T", b"body").await;
    generic::rename(&e, &a(&[b"RENAME", b"src", b"dst"]), false).await;
    assert_eq!(
        simple(generic::type_cmd(&e, &a(&[b"TYPE", b"dst"])).await),
        "proto"
    );
    assert_eq!(
        bulk(generic::object(&e, &a(&[b"OBJECT", b"ENCODING", b"dst"])).await),
        b"pkg.T"
    );
    assert_eq!(
        simple(generic::type_cmd(&e, &a(&[b"TYPE", b"src"])).await),
        "none"
    );

    assert_eq!(
        int(generic::copy(&e, &a(&[b"COPY", b"dst", b"c2"])).await),
        1
    );
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
    assert!(
        types.contains(&Reply::bulk_str("shop.v1.Order")),
        "{types:?}"
    );

    // Old version stays addressable.
    let g1 = proto_cmd::schema(
        &e,
        &a(&[b"PROTO.SCHEMA", b"GET", b"orders", b"VERSION", b"1"]),
    )
    .await;
    assert_eq!(map_get(&g1, "version"), Reply::Int(1));

    // GET ... SOURCE returns the original text.
    let src = proto_cmd::schema(&e, &a(&[b"PROTO.SCHEMA", b"GET", b"orders", b"SOURCE"])).await;
    assert_eq!(bulk(src), ORDER_SRC.as_bytes());
}

#[tokio::test]
async fn schema_types_and_del_retains_versions() {
    let (_d, e) = engine();
    assert_eq!(int(schema_set_source(&e, b"orders", ORDER_SRC).await), 1);
    let types = array(proto_cmd::schema(&e, &a(&[b"PROTO.SCHEMA", b"TYPES", b"orders"])).await);
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
        &a(&[
            b"PROTO.SCHEMA",
            b"COMPILE",
            b"x",
            b"SOURCE",
            ORDER_SRC.as_bytes(),
        ]),
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
    let types = array(proto_cmd::schema(&e, &a(&[b"PROTO.SCHEMA", b"TYPES", b"invoices"])).await);
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
    let fds = proto_cmd::schema(&e, &a(&[b"PROTO.SCHEMA", b"GET", b"orders", b"DESCRIPTOR"])).await;
    assert_eq!(bulk(fds), out.fds);
    // Garbage rejected.
    assert_err_contains(
        proto_cmd::schema(
            &e,
            &a(&[
                b"PROTO.SCHEMA",
                b"SET",
                b"junk",
                b"DESCRIPTOR",
                b"\xff\xffgarbage",
            ]),
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
        &a(&[
            b"PROTO.BIND",
            b"user:order:",
            b"shop.v1.Order",
            b"SCHEMA",
            b"orders",
        ]),
    )
    .await);

    let b = registry::binding_for_key(&e, b"user:order:1")
        .await
        .unwrap();
    assert_eq!(b.type_name, "shop.v1.Order");
    let b = registry::binding_for_key(&e, b"user:1").await.unwrap();
    assert_eq!(b.type_name, "shop.v1.Customer");
    assert!(registry::binding_for_key(&e, b"other:1").await.is_none());

    // MATCH filter on BINDINGS
    let filtered =
        proto_cmd::bindings_cmd(&e, &a(&[b"PROTO.BINDINGS", b"MATCH", b"user:order*"])).await;
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
    let b = registry::binding_for_key(&e, b"user:order:1")
        .await
        .unwrap();
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

// ---------------------------------------------------------------------------
// B4 — typed values: PROTO.SET/GET/INFO/GETJSON/SETJSON
// ---------------------------------------------------------------------------

/// Build encoded `shop.v1.Order` bytes without going through the server.
fn order_bytes(id: &str, cents: u64) -> Vec<u8> {
    use prost::Message;
    let out = marekvs_engine::proto::compile::compile_source(
        "orders",
        ORDER_SRC,
        Default::default(),
        &marekvs_engine::proto::ProtoLimits::from_env(),
    )
    .unwrap();
    let pool = marekvs_engine::proto::compile::pool_from_fds(&out.fds).unwrap();
    let desc = pool.get_message_by_name("shop.v1.Order").unwrap();
    let mut m = prost_reflect::DynamicMessage::new(desc);
    m.set_field_by_name("id", prost_reflect::Value::String(id.into()));
    m.set_field_by_name("total_cents", prost_reflect::Value::U64(cents));
    m.encode_to_vec()
}

/// Registry + binding fixture: schema "orders" bound to prefix "order:".
async fn bound_engine() -> (tempfile::TempDir, Arc<Engine>) {
    let (d, e) = engine();
    assert_eq!(int(schema_set_source(&e, b"orders", ORDER_SRC).await), 1);
    ok(proto_cmd::bind(&e, &a(&[b"PROTO.BIND", b"order:", b"shop.v1.Order"])).await);
    (d, e)
}

#[tokio::test]
async fn proto_set_get_info_via_binding() {
    let (_d, e) = bound_engine().await;
    let msg = order_bytes("o-1", 995);
    ok(proto_cmd::set(&e, &a(&[b"PROTO.SET", b"order:1", &msg])).await);
    assert_eq!(
        bulk(proto_cmd::get(&e, &a(&[b"PROTO.GET", b"order:1"])).await),
        msg
    );
    assert_eq!(
        simple(generic::type_cmd(&e, &a(&[b"TYPE", b"order:1"])).await),
        "proto"
    );
    assert_eq!(
        bulk(generic::object(&e, &a(&[b"OBJECT", b"ENCODING", b"order:1"])).await),
        b"shop.v1.Order"
    );
    let info = proto_cmd::info(&e, &a(&[b"PROTO.INFO", b"order:1"])).await;
    assert_eq!(map_get(&info, "schema"), Reply::bulk_str("orders"));
    assert_eq!(map_get(&info, "version"), Reply::Int(1));
    assert_eq!(map_get(&info, "type"), Reply::bulk_str("shop.v1.Order"));
    assert_eq!(map_get(&info, "bytes"), Reply::Int(msg.len() as i64));
    // Absent keys → Null / err
    assert_eq!(
        proto_cmd::get(&e, &a(&[b"PROTO.GET", b"order:none"])).await,
        Reply::Null
    );
    assert_err_contains(
        proto_cmd::info(&e, &a(&[b"PROTO.INFO", b"order:none"])).await,
        "no such key",
    );
}

#[tokio::test]
async fn proto_set_type_arg_overrides_and_nobinding_errors() {
    let (_d, e) = bound_engine().await;
    let msg = order_bytes("o-2", 1);
    // Key outside any binding: TYPE arg required.
    assert_err_contains(
        proto_cmd::set(&e, &a(&[b"PROTO.SET", b"free:1", &msg])).await,
        "NOBINDING",
    );
    ok(proto_cmd::set(
        &e,
        &a(&[b"PROTO.SET", b"free:1", &msg, b"TYPE", b"shop.v1.Order"]),
    )
    .await);
    assert_eq!(
        bulk(generic::object(&e, &a(&[b"OBJECT", b"ENCODING", b"free:1"])).await),
        b"shop.v1.Order"
    );
    // Unknown TYPE errors.
    assert_err_contains(
        proto_cmd::set(
            &e,
            &a(&[b"PROTO.SET", b"free:2", &msg, b"TYPE", b"no.Type"]),
        )
        .await,
        "NOSCHEMA",
    );
}

#[tokio::test]
async fn proto_set_validates_bytes() {
    let (_d, e) = bound_engine().await;
    assert_err_contains(
        proto_cmd::set(&e, &a(&[b"PROTO.SET", b"order:1", b"\xff\xff\xff\xff\xff"])).await,
        "PROTOVALIDATE",
    );
    assert_eq!(
        proto_cmd::get(&e, &a(&[b"PROTO.GET", b"order:1"])).await,
        Reply::Null
    );
}

#[tokio::test]
async fn proto_wrongtype_and_plain_set_shadowing() {
    let (_d, e) = bound_engine().await;
    let msg = order_bytes("o", 1);

    // A string under the key: PROTO.SET/GET are WRONGTYPE.
    use marekvs_engine::cmd::string as string_cmd;
    ok(string_cmd::set(&e, &a(&[b"SET", b"order:s", b"plain"])).await);
    assert_err_contains(
        proto_cmd::set(&e, &a(&[b"PROTO.SET", b"order:s", &msg])).await,
        "WRONGTYPE",
    );
    assert_err_contains(
        proto_cmd::get(&e, &a(&[b"PROTO.GET", b"order:s"])).await,
        "WRONGTYPE",
    );

    // A hash under the key blocks PROTO.SET too.
    use marekvs_engine::cmd::hash as hash_cmd;
    hash_cmd::hset(&e, &a(&[b"HSET", b"order:h", b"f", b"v"]), false).await;
    assert_err_contains(
        proto_cmd::set(&e, &a(&[b"PROTO.SET", b"order:h", &msg])).await,
        "WRONGTYPE",
    );

    // Plain SET shadows an existing proto value (standard Redis semantics).
    ok(proto_cmd::set(&e, &a(&[b"PROTO.SET", b"order:p", &msg])).await);
    ok(string_cmd::set(&e, &a(&[b"SET", b"order:p", b"shadow"])).await);
    assert_eq!(
        simple(generic::type_cmd(&e, &a(&[b"TYPE", b"order:p"])).await),
        "string"
    );
    assert_err_contains(
        proto_cmd::get(&e, &a(&[b"PROTO.GET", b"order:p"])).await,
        "WRONGTYPE",
    );
}

#[tokio::test]
async fn proto_set_nx_xx() {
    let (_d, e) = bound_engine().await;
    let msg = order_bytes("o", 1);
    assert_eq!(
        proto_cmd::set(&e, &a(&[b"PROTO.SET", b"order:1", &msg, b"XX"])).await,
        Reply::Null
    );
    ok(proto_cmd::set(&e, &a(&[b"PROTO.SET", b"order:1", &msg, b"NX"])).await);
    assert_eq!(
        proto_cmd::set(&e, &a(&[b"PROTO.SET", b"order:1", &msg, b"NX"])).await,
        Reply::Null
    );
    ok(proto_cmd::set(&e, &a(&[b"PROTO.SET", b"order:1", &msg, b"XX"])).await);
    // NX+XX is a syntax error.
    assert_err_contains(
        proto_cmd::set(&e, &a(&[b"PROTO.SET", b"order:1", &msg, b"NX", b"XX"])).await,
        "syntax",
    );
}

#[tokio::test]
async fn proto_set_ttl_options() {
    let (_d, e) = bound_engine().await;
    let msg = order_bytes("o", 1);
    ok(proto_cmd::set(&e, &a(&[b"PROTO.SET", b"order:t", &msg, b"EX", b"100"])).await);
    let ttl = int(generic::ttl(&e, &a(&[b"TTL", b"order:t"]), false).await);
    assert!(ttl > 90 && ttl <= 100, "ttl {ttl}");
    // Overwrite without KEEPTTL clears the TTL…
    ok(proto_cmd::set(&e, &a(&[b"PROTO.SET", b"order:t", &msg])).await);
    assert_eq!(
        int(generic::ttl(&e, &a(&[b"TTL", b"order:t"]), false).await),
        -1
    );
    // …and KEEPTTL retains it.
    ok(proto_cmd::set(&e, &a(&[b"PROTO.SET", b"order:t", &msg, b"PX", b"90000"])).await);
    ok(proto_cmd::set(&e, &a(&[b"PROTO.SET", b"order:t", &msg, b"KEEPTTL"])).await);
    let ttl = int(generic::ttl(&e, &a(&[b"TTL", b"order:t"]), false).await);
    assert!(ttl > 0, "ttl {ttl}");
}

#[tokio::test]
async fn proto_json_roundtrip() {
    let (_d, e) = bound_engine().await;
    let json = br#"{"id":"o-9","totalCents":"1234","tags":["a"],"paid":true}"#;
    ok(proto_cmd::setjson(&e, &a(&[b"PROTO.SETJSON", b"order:j", json])).await);
    let out = bulk(proto_cmd::getjson(&e, &a(&[b"PROTO.GETJSON", b"order:j"])).await);
    let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(v["id"], "o-9");
    assert_eq!(v["totalCents"], "1234"); // canonical: u64 as string
    assert_eq!(v["paid"], true);
    assert_eq!(v["tags"][0], "a");
    // The stored value is real protobuf bytes.
    let raw = bulk(proto_cmd::get(&e, &a(&[b"PROTO.GET", b"order:j"])).await);
    assert!(!raw.is_empty());
    // Bad JSON → PROTOVALIDATE.
    assert_err_contains(
        proto_cmd::setjson(&e, &a(&[b"PROTO.SETJSON", b"order:j", b"{nope"])).await,
        "PROTOVALIDATE",
    );
    assert_err_contains(
        proto_cmd::setjson(&e, &a(&[b"PROTO.SETJSON", b"order:j", br#"{"nosuch":1}"#])).await,
        "PROTOVALIDATE",
    );
    // GETJSON on a plain string key → WRONGTYPE.
    use marekvs_engine::cmd::string as string_cmd;
    ok(string_cmd::set(&e, &a(&[b"SET", b"order:js", b"x"])).await);
    assert_err_contains(
        proto_cmd::getjson(&e, &a(&[b"PROTO.GETJSON", b"order:js"])).await,
        "WRONGTYPE",
    );
}

#[tokio::test]
async fn proto_lww_converges_in_both_orders() {
    let (_d, e) = bound_engine().await;
    // Two competing head records with distinct versions, applied in both
    // orders to two fresh keys — the stored bytes must converge.
    let m1 = order_bytes("first", 1);
    let m2 = order_bytes("second", 2);
    let mk = |hlc: u64, origin: u16, msg: &[u8]| {
        let mut payload = head::encode(head::CTYPE_PROTO, 0);
        payload.extend_from_slice(&protohead::encode("orders", 1, "shop.v1.Order", msg));
        Envelope::head(hlc, origin).encode_with(&payload)
    };
    let (a_rec, b_rec) = (mk(1000 << 16, 3, &m1), mk(2000 << 16, 2, &m2));
    let (ra, rb) = (a_rec.clone(), b_rec.clone());
    e.store
        .run_key(b"cvg:1", move |ctx| {
            write_merged(ctx, &ikey::head_key(b"cvg:1"), &ra);
            write_merged(ctx, &ikey::head_key(b"cvg:1"), &rb);
        })
        .await;
    let (ra, rb) = (a_rec.clone(), b_rec.clone());
    e.store
        .run_key(b"cvg:2", move |ctx| {
            write_merged(ctx, &ikey::head_key(b"cvg:2"), &rb);
            write_merged(ctx, &ikey::head_key(b"cvg:2"), &ra);
        })
        .await;
    let v1 = e
        .store
        .run_key(b"cvg:1", |ctx| {
            marekvs_engine::store::get_raw(ctx, &ikey::head_key(b"cvg:1")).unwrap()
        })
        .await;
    let v2 = e
        .store
        .run_key(b"cvg:2", |ctx| {
            marekvs_engine::store::get_raw(ctx, &ikey::head_key(b"cvg:2")).unwrap()
        })
        .await;
    assert_eq!(v1, v2, "merge must be order-independent");
    // The winner is the higher-HLC record (m2).
    let (_, pay) = Envelope::decode(&v1).unwrap();
    let ph = protohead::decode(&pay[9..]).unwrap();
    assert_eq!(ph.msg, &m2[..]);
}

#[tokio::test]
async fn proto_del_recreate_keeps_delete_clock() {
    let (_d, e) = bound_engine().await;
    let msg = order_bytes("o", 1);
    ok(proto_cmd::set(&e, &a(&[b"PROTO.SET", b"order:d", &msg])).await);
    assert_eq!(int(generic::del(&e, &a(&[b"DEL", b"order:d"])).await), 1);
    let msg2 = order_bytes("o2", 2);
    ok(proto_cmd::set(&e, &a(&[b"PROTO.SET", b"order:d", &msg2])).await);
    assert_eq!(
        bulk(proto_cmd::get(&e, &a(&[b"PROTO.GET", b"order:d"])).await),
        msg2
    );
    // The re-created head must carry a non-zero delete clock (design/02
    // carry-forward: stale pre-delete records may not resurrect).
    let raw = e
        .store
        .run_key(b"order:d", |ctx| {
            marekvs_engine::store::get_raw(ctx, &ikey::head_key(b"order:d")).unwrap()
        })
        .await;
    let (_, pay) = Envelope::decode(&raw).unwrap();
    let (ctype, del_hlc) = head::decode(pay).unwrap();
    assert_eq!(ctype, head::CTYPE_PROTO);
    assert!(del_hlc > 0, "delete clock must carry forward");
}

// ---------------------------------------------------------------------------
// B5 — field access: PROTO.GETFIELD / SETFIELD / CLEARFIELD
// ---------------------------------------------------------------------------

async fn seed_order(e: &Arc<Engine>, key: &[u8]) {
    let json = br#"{
        "id": "o-1",
        "totalCents": "18446744073709551615",
        "tags": ["a", "b"],
        "scores": {"alice": "10"},
        "customer": {"name": "Ada", "tier": 3},
        "ratio": 1.5,
        "paid": true
    }"#;
    ok(proto_cmd::setjson(e, &a(&[b"PROTO.SETJSON", key, json])).await);
}

#[tokio::test]
async fn getfield_native_types() {
    let (_d, e) = bound_engine().await;
    seed_order(&e, b"order:f").await;
    let gf = |path: &'static [u8]| {
        let e = e.clone();
        async move { proto_cmd::getfield(&e, &a(&[b"PROTO.GETFIELD", b"order:f", path])).await }
    };
    assert_eq!(gf(b"id").await, Reply::Bulk(b"o-1".to_vec()));
    // u64 above i64::MAX renders as its decimal string
    assert_eq!(
        gf(b"total_cents").await,
        Reply::Bulk(b"18446744073709551615".to_vec())
    );
    assert_eq!(gf(b"ratio").await, Reply::Double(1.5));
    assert_eq!(gf(b"paid").await, Reply::Bool(true));
    assert_eq!(gf(b"customer.tier").await, Reply::Int(3));
    assert_eq!(gf(b"tags.0").await, Reply::Bulk(b"a".to_vec()));
    assert_eq!(gf(b"scores.alice").await, Reply::Int(10));
    // containers → canonical JSON
    assert_eq!(gf(b"tags").await, Reply::Bulk(br#"["a","b"]"#.to_vec()));
    assert_eq!(
        gf(b"customer").await,
        Reply::Bulk(br#"{"name":"Ada","tier":3}"#.to_vec())
    );
    // unset/missing → Null
    assert_eq!(gf(b"tags.9").await, Reply::Null);
    assert_eq!(gf(b"scores.nobody").await, Reply::Null);
    // multiple paths → Array
    let r = proto_cmd::getfield(&e, &a(&[b"PROTO.GETFIELD", b"order:f", b"id", b"paid"])).await;
    assert_eq!(
        r,
        Reply::Array(vec![Reply::Bulk(b"o-1".to_vec()), Reply::Bool(true)])
    );
}

#[tokio::test]
async fn getfield_path_errors() {
    let (_d, e) = bound_engine().await;
    seed_order(&e, b"order:pe").await;
    assert_err_contains(
        proto_cmd::getfield(&e, &a(&[b"PROTO.GETFIELD", b"order:pe", b"nosuch"])).await,
        "PROTOPATH",
    );
    let deep = vec!["a"; 33].join(".");
    assert_err_contains(
        proto_cmd::getfield(&e, &a(&[b"PROTO.GETFIELD", b"order:pe", deep.as_bytes()])).await,
        "PROTOPATH",
    );
    assert_err_contains(
        proto_cmd::getfield(&e, &a(&[b"PROTO.GETFIELD", b"order:pe", b"id.deeper"])).await,
        "PROTOPATH",
    );
    // absent key → Null
    assert_eq!(
        proto_cmd::getfield(&e, &a(&[b"PROTO.GETFIELD", b"order:absent", b"id"])).await,
        Reply::Null
    );
}

#[tokio::test]
async fn setfield_rmw_preserves_untouched_fields() {
    let (_d, e) = bound_engine().await;
    seed_order(&e, b"order:rm").await;
    ok(proto_cmd::setfield(
        &e,
        &a(&[b"PROTO.SETFIELD", b"order:rm", b"customer.name", b"Grace"]),
    )
    .await);
    // Touched path changed…
    assert_eq!(
        proto_cmd::getfield(&e, &a(&[b"PROTO.GETFIELD", b"order:rm", b"customer.name"])).await,
        Reply::Bulk(b"Grace".to_vec())
    );
    // …everything else intact.
    assert_eq!(
        proto_cmd::getfield(&e, &a(&[b"PROTO.GETFIELD", b"order:rm", b"id"])).await,
        Reply::Bulk(b"o-1".to_vec())
    );
    assert_eq!(
        proto_cmd::getfield(&e, &a(&[b"PROTO.GETFIELD", b"order:rm", b"customer.tier"])).await,
        Reply::Int(3)
    );
    assert_eq!(
        proto_cmd::getfield(&e, &a(&[b"PROTO.GETFIELD", b"order:rm", b"tags"])).await,
        Reply::Bulk(br#"["a","b"]"#.to_vec())
    );

    // Multiple path/value pairs in one atomic RMW; JSON for containers.
    ok(proto_cmd::setfield(
        &e,
        &a(&[
            b"PROTO.SETFIELD",
            b"order:rm",
            b"tags",
            br#"["x","y","z"]"#,
            b"scores.bob",
            b"7",
        ]),
    )
    .await);
    assert_eq!(
        proto_cmd::getfield(&e, &a(&[b"PROTO.GETFIELD", b"order:rm", b"tags.2"])).await,
        Reply::Bulk(b"z".to_vec())
    );
    assert_eq!(
        proto_cmd::getfield(&e, &a(&[b"PROTO.GETFIELD", b"order:rm", b"scores.bob"])).await,
        Reply::Int(7)
    );

    // Errors: unknown field, bad value, absent key.
    assert_err_contains(
        proto_cmd::setfield(&e, &a(&[b"PROTO.SETFIELD", b"order:rm", b"nosuch", b"1"])).await,
        "PROTOPATH",
    );
    assert_err_contains(
        proto_cmd::setfield(
            &e,
            &a(&[
                b"PROTO.SETFIELD",
                b"order:rm",
                b"customer.tier",
                b"notanint",
            ]),
        )
        .await,
        "PROTOPATH",
    );
    assert_err_contains(
        proto_cmd::setfield(&e, &a(&[b"PROTO.SETFIELD", b"order:no", b"id", b"x"])).await,
        "no such key",
    );
}

#[tokio::test]
async fn setfield_keeps_ttl_and_lww_stamps() {
    let (_d, e) = bound_engine().await;
    let msg = order_bytes("keepttl", 5);
    ok(proto_cmd::set(&e, &a(&[b"PROTO.SET", b"order:st", &msg, b"EX", b"100"])).await);
    ok(proto_cmd::setfield(&e, &a(&[b"PROTO.SETFIELD", b"order:st", b"id", b"new-id"])).await);
    let ttl = int(generic::ttl(&e, &a(&[b"TTL", b"order:st"]), false).await);
    assert!(
        ttl > 90 && ttl <= 100,
        "SETFIELD must keep the TTL, got {ttl}"
    );
    assert_eq!(
        proto_cmd::getfield(&e, &a(&[b"PROTO.GETFIELD", b"order:st", b"id"])).await,
        Reply::Bulk(b"new-id".to_vec())
    );
}

#[tokio::test]
async fn clearfield_scalars_lists_maps() {
    let (_d, e) = bound_engine().await;
    seed_order(&e, b"order:cl").await;
    // clear a scalar, a list element and a map key: 3 cleared
    let n = int(proto_cmd::clearfield(
        &e,
        &a(&[
            b"PROTO.CLEARFIELD",
            b"order:cl",
            b"paid",
            b"tags.0",
            b"scores.alice",
        ]),
    )
    .await);
    assert_eq!(n, 3);
    assert_eq!(
        proto_cmd::getfield(&e, &a(&[b"PROTO.GETFIELD", b"order:cl", b"paid"])).await,
        Reply::Bool(false) // proto3 scalar resets to default
    );
    assert_eq!(
        proto_cmd::getfield(&e, &a(&[b"PROTO.GETFIELD", b"order:cl", b"tags"])).await,
        Reply::Bulk(br#"["b"]"#.to_vec())
    );
    assert_eq!(
        proto_cmd::getfield(&e, &a(&[b"PROTO.GETFIELD", b"order:cl", b"scores.alice"])).await,
        Reply::Null
    );
    // clearing something already absent counts 0
    assert_eq!(
        int(
            proto_cmd::clearfield(&e, &a(&[b"PROTO.CLEARFIELD", b"order:cl", b"scores.alice"]))
                .await
        ),
        0
    );
    // message field clears to unset (Null)
    assert_eq!(
        int(proto_cmd::clearfield(&e, &a(&[b"PROTO.CLEARFIELD", b"order:cl", b"customer"])).await),
        1
    );
    assert_eq!(
        proto_cmd::getfield(&e, &a(&[b"PROTO.GETFIELD", b"order:cl", b"customer"])).await,
        Reply::Null
    );
}

#[tokio::test]
async fn setjson_getjson_roundtrip_after_field_edits() {
    let (_d, e) = bound_engine().await;
    seed_order(&e, b"order:rt").await;
    ok(proto_cmd::setfield(&e, &a(&[b"PROTO.SETFIELD", b"order:rt", b"id", b"o-2"])).await);
    let out = bulk(proto_cmd::getjson(&e, &a(&[b"PROTO.GETJSON", b"order:rt"])).await);
    let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(v["id"], "o-2");
    assert_eq!(v["totalCents"], "18446744073709551615"); // u64 as string
    assert_eq!(v["customer"]["name"], "Ada");
    // Round-trip the produced JSON back through SETJSON: identical JSON out.
    ok(proto_cmd::setjson(&e, &a(&[b"PROTO.SETJSON", b"order:rt2", &out])).await);
    let out2 = bulk(proto_cmd::getjson(&e, &a(&[b"PROTO.GETJSON", b"order:rt2"])).await);
    assert_eq!(out, out2);
}

#[tokio::test]
async fn proto_set_decomposes_to_fmt2() {
    let (_d, e) = bound_engine().await;
    let msg = order_bytes("o-1", 995);
    ok(proto_cmd::set(&e, &a(&[b"PROTO.SET", b"order:1", &msg])).await);
    let info = proto_cmd::info(&e, &a(&[b"PROTO.INFO", b"order:1"])).await;
    assert_eq!(map_get(&info, "format"), Reply::bulk_str("fields"));
    // root marker + id + total_cents = 3 records
    assert_eq!(map_get(&info, "records"), Reply::Int(3));
    // GET materializes the field records back to the identical wire bytes.
    assert_eq!(
        bulk(proto_cmd::get(&e, &a(&[b"PROTO.GET", b"order:1"])).await),
        msg
    );
}

#[tokio::test]
async fn legacy_fmt1_upgrades_on_setfield() {
    let (_d, e) = bound_engine().await;
    let msg = order_bytes("legacy", 42);
    // A pre-design/18 whole-message (fmt=1) value written directly.
    write_raw_proto(&e, b"order:leg", "orders", 1, "shop.v1.Order", &msg).await;
    let info = proto_cmd::info(&e, &a(&[b"PROTO.INFO", b"order:leg"])).await;
    assert_eq!(map_get(&info, "format"), Reply::bulk_str("whole"));
    // Readable before any upgrade.
    assert_eq!(
        proto_cmd::getfield(&e, &a(&[b"PROTO.GETFIELD", b"order:leg", b"id"])).await,
        Reply::Bulk(b"legacy".to_vec())
    );
    // One SETFIELD upgrades it to fmt=2 in place.
    ok(proto_cmd::setfield(&e, &a(&[b"PROTO.SETFIELD", b"order:leg", b"id", b"new"])).await);
    let info = proto_cmd::info(&e, &a(&[b"PROTO.INFO", b"order:leg"])).await;
    assert_eq!(map_get(&info, "format"), Reply::bulk_str("fields"));
    // Edited field changed, untouched field preserved through the upgrade.
    assert_eq!(
        proto_cmd::getfield(&e, &a(&[b"PROTO.GETFIELD", b"order:leg", b"id"])).await,
        Reply::Bulk(b"new".to_vec())
    );
    assert_eq!(
        proto_cmd::getfield(&e, &a(&[b"PROTO.GETFIELD", b"order:leg", b"total_cents"])).await,
        Reply::Int(42)
    );
}

#[tokio::test]
async fn setfield_appends_and_replaces_repeated() {
    let (_d, e) = bound_engine().await;
    seed_order(&e, b"order:ap").await; // tags = ["a","b"]
                                       // index == len appends
    ok(proto_cmd::setfield(&e, &a(&[b"PROTO.SETFIELD", b"order:ap", b"tags.2", b"c"])).await);
    assert_eq!(
        proto_cmd::getfield(&e, &a(&[b"PROTO.GETFIELD", b"order:ap", b"tags"])).await,
        Reply::Bulk(br#"["a","b","c"]"#.to_vec())
    );
    // index < len replaces in place (RGA order preserved)
    ok(proto_cmd::setfield(&e, &a(&[b"PROTO.SETFIELD", b"order:ap", b"tags.1", b"B"])).await);
    assert_eq!(
        proto_cmd::getfield(&e, &a(&[b"PROTO.GETFIELD", b"order:ap", b"tags"])).await,
        Reply::Bulk(br#"["a","B","c"]"#.to_vec())
    );
    // index past the end errors
    assert_err_contains(
        proto_cmd::setfield(&e, &a(&[b"PROTO.SETFIELD", b"order:ap", b"tags.9", b"x"])).await,
        "range",
    );
}

// ---------------------------------------------------------------------------
// B6 — validated collection elements: PROTO.HSET/SADD/HGETJSON/HGETFIELD
// ---------------------------------------------------------------------------

#[tokio::test]
async fn proto_hset_validates_then_delegates() {
    let (_d, e) = bound_engine().await;
    use marekvs_engine::cmd::hash as hash_cmd;
    let m1 = order_bytes("h-1", 11);
    let m2 = order_bytes("h-2", 22);
    assert_eq!(
        int(proto_cmd::hset(&e, &a(&[b"PROTO.HSET", b"order:h", b"f1", &m1, b"f2", &m2])).await),
        2
    );
    // Element payloads stay raw proto bytes: plain HGET returns them.
    assert_eq!(
        bulk(hash_cmd::hget(&e, &a(&[b"HGET", b"order:h", b"f1"])).await),
        m1
    );
    assert_eq!(
        simple(generic::type_cmd(&e, &a(&[b"TYPE", b"order:h"])).await),
        "hash"
    );

    // One invalid value → PROTOVALIDATE, and NOTHING is written.
    assert_err_contains(
        proto_cmd::hset(
            &e,
            &a(&[
                b"PROTO.HSET",
                b"order:h2",
                b"good",
                &m1,
                b"bad",
                b"\xff\xff\xff\xff\xff",
            ]),
        )
        .await,
        "PROTOVALIDATE",
    );
    assert_eq!(
        hash_cmd::hget(&e, &a(&[b"HGET", b"order:h2", b"good"])).await,
        Reply::Null
    );

    // No binding and no TYPE → NOBINDING; TYPE clause right after the key.
    assert_err_contains(
        proto_cmd::hset(&e, &a(&[b"PROTO.HSET", b"free:h", b"f", &m1])).await,
        "NOBINDING",
    );
    assert_eq!(
        int(proto_cmd::hset(
            &e,
            &a(&[
                b"PROTO.HSET",
                b"free:h",
                b"TYPE",
                b"shop.v1.Order",
                b"f",
                &m1
            ]),
        )
        .await),
        1
    );
}

#[tokio::test]
async fn proto_sadd_validates_then_delegates() {
    let (_d, e) = bound_engine().await;
    use marekvs_engine::cmd::set as set_cmd;
    let m1 = order_bytes("s-1", 1);
    let m2 = order_bytes("s-2", 2);
    assert_eq!(
        int(proto_cmd::sadd(&e, &a(&[b"PROTO.SADD", b"order:set", &m1, &m2])).await),
        2
    );
    // duplicate re-add counts 0
    assert_eq!(
        int(proto_cmd::sadd(&e, &a(&[b"PROTO.SADD", b"order:set", &m1])).await),
        0
    );
    assert_eq!(
        int(set_cmd::scard(&e, &a(&[b"SCARD", b"order:set"])).await),
        2
    );
    assert_eq!(
        simple(generic::type_cmd(&e, &a(&[b"TYPE", b"order:set"])).await),
        "set"
    );
    assert_err_contains(
        proto_cmd::sadd(
            &e,
            &a(&[b"PROTO.SADD", b"order:set", b"\xff\xff\xff\xff\xff"]),
        )
        .await,
        "PROTOVALIDATE",
    );
}

#[tokio::test]
async fn proto_hgetjson_and_hgetfield() {
    let (_d, e) = bound_engine().await;
    let m = order_bytes("hj-1", 777);
    assert_eq!(
        int(proto_cmd::hset(&e, &a(&[b"PROTO.HSET", b"order:hj", b"f", &m])).await),
        1
    );
    let out = bulk(proto_cmd::hgetjson(&e, &a(&[b"PROTO.HGETJSON", b"order:hj", b"f"])).await);
    let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(v["id"], "hj-1");
    assert_eq!(v["totalCents"], "777");
    // Missing field → Null.
    assert_eq!(
        proto_cmd::hgetjson(&e, &a(&[b"PROTO.HGETJSON", b"order:hj", b"none"])).await,
        Reply::Null
    );
    // Field path into the element.
    assert_eq!(
        proto_cmd::hgetfield(&e, &a(&[b"PROTO.HGETFIELD", b"order:hj", b"f", b"id"])).await,
        Reply::Bulk(b"hj-1".to_vec())
    );
    assert_eq!(
        proto_cmd::hgetfield(
            &e,
            &a(&[b"PROTO.HGETFIELD", b"order:hj", b"f", b"total_cents"]),
        )
        .await,
        Reply::Int(777)
    );
    assert_err_contains(
        proto_cmd::hgetfield(&e, &a(&[b"PROTO.HGETFIELD", b"order:hj", b"f", b"nosuch"])).await,
        "PROTOPATH",
    );

    // TYPE arg resolution for keys without a binding.
    let m2 = order_bytes("hj-2", 2);
    assert_eq!(
        int(proto_cmd::hset(
            &e,
            &a(&[
                b"PROTO.HSET",
                b"free:hj",
                b"TYPE",
                b"shop.v1.Order",
                b"f",
                &m2
            ]),
        )
        .await),
        1
    );
    assert_err_contains(
        proto_cmd::hgetjson(&e, &a(&[b"PROTO.HGETJSON", b"free:hj", b"f"])).await,
        "NOBINDING",
    );
    let out = bulk(
        proto_cmd::hgetjson(
            &e,
            &a(&[
                b"PROTO.HGETJSON",
                b"free:hj",
                b"f",
                b"TYPE",
                b"shop.v1.Order",
            ]),
        )
        .await,
    );
    let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
    assert_eq!(v["id"], "hj-2");
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

//! PROTO.* — protobuf schema registry, prefix bindings and typed values
//! (design/17).
//!
//! Registry state lives in hidden replicated system records (`\x00proto:*`,
//! see `crate::proto::registry`); protox compilation always runs in
//! `tokio::task::spawn_blocking`, never on shard threads.

mod doc;

use std::sync::Arc;

use prost::Message;
use prost_reflect::{DynamicMessage, FieldDescriptor, Kind, MessageDescriptor, Value};

use crate::cmd::{eq_ignore_case, parse_u64};
use crate::proto::fields::{self, build_msg, decompose_msg, resolve_path, PDoc, Resolved};
use crate::proto::{compile, path, registry, ProtoErr};
use crate::pubsub::glob_match;
use crate::reply::Reply;
use crate::store::{get_head, get_raw, now_ms, read_lww, write_merged, ShardCtx};
use crate::Engine;
use marekvs_core::envelope::{head, Envelope};
use marekvs_core::merge::Dot;
use marekvs_core::pdoc::{push_seg, PNodeIn, PRecord, PSeg, PVal};
use marekvs_core::{ikey, protohead};

use doc::Slot;

/// Reasonable cap on registry schema names.
const MAX_NAME: usize = 255;

fn parse_name(raw: &[u8]) -> Result<String, Reply> {
    let s =
        std::str::from_utf8(raw).map_err(|_| Reply::err("SCHEMAERR schema name must be utf-8"))?;
    if s.is_empty() || s.len() > MAX_NAME || s.contains('\0') {
        return Err(Reply::err("SCHEMAERR invalid schema name"));
    }
    Ok(s.to_string())
}

fn parse_version_u32(raw: &[u8]) -> Result<u32, Reply> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .filter(|v| *v > 0)
        .ok_or_else(|| Reply::err("ERR VERSION must be a positive integer"))
}

/// Run a compile closure off the async runtime (and off shard threads).
async fn blocking_compile<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, ProtoErr> + Send + 'static,
) -> Result<T, ProtoErr> {
    match tokio::task::spawn_blocking(f).await {
        Ok(r) => r,
        Err(e) => Err(ProtoErr::Other(format!("ERR compile task failed: {e}"))),
    }
}

// ---------------------------------------------------------------------------
// PROTO.SCHEMA <SET|COMPILE|GET|LIST|TYPES|DEL>
// ---------------------------------------------------------------------------

pub async fn schema(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 2 {
        return Reply::wrong_args("proto.schema");
    }
    let sub = String::from_utf8_lossy(&args[1]).to_uppercase();
    match sub.as_str() {
        "SET" => schema_set(engine, args, false).await,
        "COMPILE" => schema_set(engine, args, true).await,
        "GET" => schema_get(engine, args).await,
        "LIST" => schema_list(engine).await,
        "TYPES" => schema_types(engine, args).await,
        "DEL" => schema_del(engine, args).await,
        _ => Reply::err(format!("ERR Unknown PROTO.SCHEMA subcommand '{sub}'")),
    }
}

/// `PROTO.SCHEMA SET|COMPILE <name> SOURCE <text> | DESCRIPTOR <fds>`.
/// COMPILE is the dry run: same pipeline, nothing stored, returns the type
/// names.
async fn schema_set(engine: &Arc<Engine>, args: &[Vec<u8>], dry_run: bool) -> Reply {
    if args.len() != 5 {
        return Reply::wrong_args("proto.schema");
    }
    let name = match parse_name(&args[2]) {
        Ok(n) => n,
        Err(r) => return r,
    };
    let limits = engine.proto.limits;
    let (out, kind, source, imports) = if eq_ignore_case(&args[3], "SOURCE") {
        let Ok(source) = String::from_utf8(args[4].clone()) else {
            return Reply::err("SCHEMAERR source must be utf-8");
        };
        if let Err(e) = compile::check_source_size(&source, &limits) {
            return e.reply();
        }
        // BFS-resolve the import closure from the registry, then compile
        // off the runtime.
        let deps = match registry::resolve_imports(engine, &source).await {
            Ok(d) => d,
            Err(e) => return e.reply(),
        };
        let imports = compile::extract_imports(&source);
        let (n, s) = (name.clone(), source.clone());
        let out = blocking_compile(move || compile::compile_source(&n, &s, deps, &limits)).await;
        match out {
            Ok(out) => (out, registry::KIND_SOURCE, source.into_bytes(), imports),
            Err(e) => return e.reply(),
        }
    } else if eq_ignore_case(&args[3], "DESCRIPTOR") {
        let fds = args[4].clone();
        let out = blocking_compile(move || compile::compile_descriptor(&fds, &limits)).await;
        match out {
            Ok(out) => (out, registry::KIND_DESCRIPTOR, Vec::new(), Vec::new()),
            Err(e) => return e.reply(),
        }
    } else {
        return Reply::syntax();
    };

    if dry_run {
        return Reply::Array(
            out.types
                .iter()
                .map(|t| Reply::bulk_str(t.clone()))
                .collect(),
        );
    }

    // Monotonic per-name version. Concurrent same-name SET is LWW on the
    // latest pointer (documented admin caveat).
    let version = match registry::load_schema(engine, &name, 0).await {
        Ok(prev) => prev.version + 1,
        Err(ProtoErr::NoSchema(_)) => 1,
        Err(e) => return e.reply(),
    };
    let rec = registry::SchemaRecord {
        version,
        kind,
        source,
        fds: out.fds,
        imports,
        types: out.types,
        created_ms: now_ms(),
    };
    registry::store_schema(engine, &name, &rec).await;
    Reply::Int(version as i64)
}

/// `PROTO.SCHEMA GET <name> [VERSION v] [SOURCE|DESCRIPTOR]`.
async fn schema_get(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 3 {
        return Reply::wrong_args("proto.schema|get");
    }
    let name = match parse_name(&args[2]) {
        Ok(n) => n,
        Err(r) => return r,
    };
    let mut version = 0u32;
    let mut want: Option<u8> = None; // b's' source, b'd' descriptor
    let mut i = 3;
    while i < args.len() {
        if eq_ignore_case(&args[i], "VERSION") {
            let Some(v) = args.get(i + 1) else {
                return Reply::syntax();
            };
            version = match parse_version_u32(v) {
                Ok(v) => v,
                Err(r) => return r,
            };
            i += 2;
        } else if eq_ignore_case(&args[i], "SOURCE") {
            want = Some(b's');
            i += 1;
        } else if eq_ignore_case(&args[i], "DESCRIPTOR") {
            want = Some(b'd');
            i += 1;
        } else {
            return Reply::syntax();
        }
    }
    let rec = match registry::load_schema(engine, &name, version).await {
        Ok(r) => r,
        Err(e) => return e.reply(),
    };
    match want {
        Some(b's') => {
            if rec.source.is_empty() {
                Reply::Null // descriptor upload: no source text
            } else {
                Reply::Bulk(rec.source)
            }
        }
        Some(_) => Reply::Bulk(rec.fds),
        None => Reply::Map(vec![
            (Reply::bulk_str("name"), Reply::bulk_str(name)),
            (Reply::bulk_str("version"), Reply::Int(rec.version as i64)),
            (
                Reply::bulk_str("kind"),
                Reply::bulk_str(if rec.kind == registry::KIND_DESCRIPTOR {
                    "descriptor"
                } else {
                    "source"
                }),
            ),
            (
                Reply::bulk_str("types"),
                Reply::Array(rec.types.into_iter().map(Reply::bulk_str).collect()),
            ),
            (
                Reply::bulk_str("imports"),
                Reply::Array(rec.imports.into_iter().map(Reply::bulk_str).collect()),
            ),
        ]),
    }
}

async fn schema_list(engine: &Arc<Engine>) -> Reply {
    Reply::Map(
        registry::schema_index(engine)
            .await
            .into_iter()
            .map(|(name, ver)| (Reply::bulk_str(name), Reply::Int(ver as i64)))
            .collect(),
    )
}

/// `PROTO.SCHEMA TYPES <name> [VERSION v]`.
async fn schema_types(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 3 && args.len() != 5 {
        return Reply::wrong_args("proto.schema|types");
    }
    let name = match parse_name(&args[2]) {
        Ok(n) => n,
        Err(r) => return r,
    };
    let mut version = 0u32;
    if args.len() == 5 {
        if !eq_ignore_case(&args[3], "VERSION") {
            return Reply::syntax();
        }
        version = match parse_version_u32(&args[4]) {
            Ok(v) => v,
            Err(r) => return r,
        };
    }
    match registry::load_schema(engine, &name, version).await {
        Ok(rec) => Reply::Array(rec.types.into_iter().map(Reply::bulk_str).collect()),
        Err(e) => e.reply(),
    }
}

/// `PROTO.SCHEMA DEL <name>` — tombstones latest + idx entry; immutable
/// version records are RETAINED so stored values keep decoding.
async fn schema_del(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 3 {
        return Reply::wrong_args("proto.schema|del");
    }
    let name = match parse_name(&args[2]) {
        Ok(n) => n,
        Err(r) => return r,
    };
    Reply::Int(registry::delete_schema(engine, &name).await as i64)
}

// ---------------------------------------------------------------------------
// PROTO.BIND / PROTO.UNBIND / PROTO.BINDINGS
// ---------------------------------------------------------------------------

/// `PROTO.BIND <prefix> <fq-type> [SCHEMA name] [VERSION v]`.
pub async fn bind(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 3 {
        return Reply::wrong_args("proto.bind");
    }
    let prefix = args[1].clone();
    if prefix.is_empty() || prefix[0] == 0 {
        return Reply::err("ERR invalid prefix");
    }
    let Ok(type_name) = String::from_utf8(args[2].clone()) else {
        return Reply::err("ERR type name must be utf-8");
    };
    let mut schema_name: Option<String> = None;
    let mut version = 0u32; // 0 = track latest
    let mut i = 3;
    while i < args.len() {
        if eq_ignore_case(&args[i], "SCHEMA") {
            let Some(v) = args.get(i + 1) else {
                return Reply::syntax();
            };
            schema_name = Some(match parse_name(v) {
                Ok(n) => n,
                Err(r) => return r,
            });
            i += 2;
        } else if eq_ignore_case(&args[i], "VERSION") {
            let Some(v) = args.get(i + 1) else {
                return Reply::syntax();
            };
            version = match parse_version_u32(v) {
                Ok(v) => v,
                Err(r) => return r,
            };
            i += 2;
        } else {
            return Reply::syntax();
        }
    }
    // Resolve + verify the type exists before storing the binding.
    let resolved = registry::resolve_for_key(
        engine,
        &prefix,
        Some(&type_name),
        schema_name.as_deref(),
        version,
    )
    .await;
    let resolved = match resolved {
        Ok(r) => r,
        Err(e) => return e.reply(),
    };
    let rec = registry::BindingRecord {
        schema: resolved.schema,
        version, // keep 0 = latest unless pinned explicitly
        type_name: resolved.type_name,
    };
    registry::set_binding(engine, &prefix, &rec).await;
    Reply::ok()
}

pub async fn unbind(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 2 {
        return Reply::wrong_args("proto.unbind");
    }
    Reply::Int(registry::remove_binding(engine, &args[1]).await as i64)
}

// ---------------------------------------------------------------------------
// typed values: PROTO.SET / GET / INFO / GETJSON / SETJSON
// ---------------------------------------------------------------------------

/// Options shared by PROTO.SET and PROTO.SETJSON.
#[derive(Default)]
struct SetOpts {
    type_name: Option<String>,
    nx: bool,
    xx: bool,
    keepttl: bool,
    /// Absolute TTL deadline in ms.
    ttl: Option<u64>,
}

fn parse_set_opts(args: &[Vec<u8>], mut i: usize) -> Result<SetOpts, Reply> {
    let mut o = SetOpts::default();
    let now = now_ms();
    while i < args.len() {
        let arg = &args[i];
        if eq_ignore_case(arg, "TYPE") {
            let Some(t) = args.get(i + 1) else {
                return Err(Reply::syntax());
            };
            let Ok(t) = String::from_utf8(t.clone()) else {
                return Err(Reply::err("ERR type name must be utf-8"));
            };
            o.type_name = Some(t);
            i += 1;
        } else if eq_ignore_case(arg, "NX") {
            o.nx = true;
        } else if eq_ignore_case(arg, "XX") {
            o.xx = true;
        } else if eq_ignore_case(arg, "KEEPTTL") {
            o.keepttl = true;
        } else if eq_ignore_case(arg, "EX") || eq_ignore_case(arg, "PX") {
            let Some(n) = args.get(i + 1).and_then(|b| parse_u64(b)) else {
                return Err(Reply::not_int());
            };
            if n == 0 {
                return Err(Reply::err("ERR invalid expire time in 'proto.set' command"));
            }
            let mult = if eq_ignore_case(arg, "EX") { 1000 } else { 1 };
            o.ttl = Some(now + n * mult);
            i += 1;
        } else if eq_ignore_case(arg, "EXAT") || eq_ignore_case(arg, "PXAT") {
            let Some(n) = args.get(i + 1).and_then(|b| parse_u64(b)) else {
                return Err(Reply::not_int());
            };
            let mult = if eq_ignore_case(arg, "EXAT") { 1000 } else { 1 };
            o.ttl = Some(n * mult);
            i += 1;
        } else {
            return Err(Reply::syntax());
        }
        i += 1;
    }
    if o.nx && o.xx {
        return Err(Reply::syntax());
    }
    Ok(o)
}

/// Shard-side read of a key's proto head. `Err(())` = WRONGTYPE (a live
/// string, legacy list blob or other collection holds the key);
/// `Ok(None)` = absent. Returns (envelope, del_hlc, protohead tail).
fn read_proto_head(ctx: &ShardCtx, key: &[u8]) -> Result<Option<(Envelope, u64, Vec<u8>)>, ()> {
    if read_lww(ctx, &ikey::string_key(key), 0).is_some()
        || read_lww(ctx, &ikey::list_key(key), 0).is_some()
    {
        return Err(()); // live string/list shadows the proto head
    }
    let Some((env, ctype, del)) = get_head(ctx, key) else {
        return Ok(None);
    };
    if env.is_tombstone() || env.is_expired(now_ms()) {
        return Ok(None);
    }
    if ctype != head::CTYPE_PROTO {
        return Err(());
    }
    let raw = get_raw(ctx, &ikey::head_key(key)).ok_or(())?;
    let (_, pay) = Envelope::decode(&raw).ok_or(())?;
    Ok(Some((env, del, pay.get(9..).unwrap_or(&[]).to_vec())))
}

/// Root-set a decomposed proto value (design/18): cover every stored `'p'`
/// record, write the fresh field records, and stamp a fmt=2 head. Carries the
/// delete clock forward, honours NX/XX + TTL exactly like the legacy path.
async fn write_value(
    engine: &Arc<Engine>,
    key: Vec<u8>,
    msg: DynamicMessage,
    o: SetOpts,
    resolved: registry::ResolvedType,
) -> Reply {
    let encoded_len = msg.encoded_len();
    if encoded_len > engine.proto.limits.max_value {
        return ProtoErr::Validate(format!(
            "value too large ({} bytes, limit {})",
            encoded_len, engine.proto.limits.max_value
        ))
        .reply();
    }
    // NX/XX/KEEPTTL and the delete clock read the current head; make sure a
    // cluster-remote key is observable first.
    engine.ensure_local(&key).await;
    let (schema, version, type_name) = (resolved.schema, resolved.version, resolved.type_name);
    engine
        .store
        .run_key(&key.clone(), move |ctx| {
            let existing = match read_proto_head(ctx, &key) {
                Err(()) => return Reply::wrongtype(),
                Ok(v) => v,
            };
            if (o.nx && existing.is_some()) || (o.xx && existing.is_none()) {
                return Reply::Null;
            }
            // Carry forward the previous delete clock (ensure_head
            // precedent): a re-created value must keep shadowing stale
            // pre-delete records arriving via replication/AE.
            let now = now_ms();
            let prev_del = get_head(ctx, &key).map_or(0, |(env, _, del)| {
                let mut d = del;
                if env.is_tombstone() {
                    d = d.max(env.hlc);
                }
                if env.is_expired(now) {
                    d = d.max(env.expiry_hlc());
                }
                d
            });
            let ttl = match (o.ttl, o.keepttl) {
                (Some(t), _) => t,
                (None, true) => existing
                    .as_ref()
                    .map_or(0, |(env, _, _)| env.ttl_deadline_ms),
                (None, false) => 0,
            };
            // Root SET: cover all prior descendants, then write the fresh
            // decomposition. The root marker covers the previous root's dots.
            let observed = doc::node_observed(ctx, &key, &[]);
            doc::cover_descendants(ctx, &key, &[], prev_del);
            let mut fresh = |_: &[u8], _: u32| doc::fresh_eid(ctx);
            for rec in decompose_msg(&[], &msg, &mut fresh) {
                match rec {
                    marekvs_core::pdoc::PRecord::Node { path, val } if path.is_empty() => {
                        doc::write_node(ctx, &key, &[], &val, &observed)
                    }
                    marekvs_core::pdoc::PRecord::Node { path, val } => {
                        doc::write_node(ctx, &key, &path, &val, &[])
                    }
                    marekvs_core::pdoc::PRecord::Elem { path, elem } => {
                        doc::write_elem(ctx, &key, &path, &elem)
                    }
                }
            }
            let mut payload = head::encode(head::CTYPE_PROTO, prev_del);
            payload.extend_from_slice(&protohead::encode_v2(&schema, version, &type_name));
            let env = Envelope::head(ctx.hlc.now(), ctx.node_id).with_ttl(ttl);
            write_merged(ctx, &ikey::head_key(&key), &env.encode_with(&payload));
            Reply::ok()
        })
        .await
}

/// `PROTO.SET key value [TYPE t] [NX|XX] [EX s|PX ms|EXAT ts|PXAT ts|KEEPTTL]`.
pub async fn set(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 3 {
        return Reply::wrong_args("proto.set");
    }
    let (key, msg) = (args[1].clone(), args[2].clone());
    let o = match parse_set_opts(args, 3) {
        Ok(o) => o,
        Err(r) => return r,
    };
    let resolved =
        match registry::resolve_for_key(engine, &key, o.type_name.as_deref(), None, 0).await {
            Ok(r) => r,
            Err(e) => return e.reply(),
        };
    // Validate: the bytes must decode against the resolved message type.
    let m = match DynamicMessage::decode(resolved.desc.clone(), &msg[..]) {
        Ok(m) => m,
        Err(e) => return ProtoErr::Validate(format!("{}: {e}", resolved.type_name)).reply(),
    };
    write_value(engine, key, m, o, resolved).await
}

/// `PROTO.SETJSON key json [TYPE t] [NX|XX] [ttl opts]` — canonical
/// protobuf-JSON in, validated protobuf bytes stored.
pub async fn setjson(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 3 {
        return Reply::wrong_args("proto.setjson");
    }
    let (key, json) = (args[1].clone(), args[2].clone());
    let o = match parse_set_opts(args, 3) {
        Ok(o) => o,
        Err(r) => return r,
    };
    let resolved =
        match registry::resolve_for_key(engine, &key, o.type_name.as_deref(), None, 0).await {
            Ok(r) => r,
            Err(e) => return e.reply(),
        };
    let mut de = serde_json::Deserializer::from_slice(&json);
    let msg = match DynamicMessage::deserialize(resolved.desc.clone(), &mut de) {
        Ok(m) => m,
        Err(e) => return ProtoErr::Validate(format!("{}: {e}", resolved.type_name)).reply(),
    };
    write_value(engine, key, msg, o, resolved).await
}

/// `PROTO.GET key` → the message bytes (fmt=1: verbatim; fmt=2: materialized
/// from field records). Byte-unstable across calls for map-bearing messages
/// (spec-legal; design/18).
pub async fn get(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 2 {
        return Reply::wrong_args("proto.get");
    }
    match load_message(engine, &args[1]).await {
        Err(r) => r,
        Ok(None) => Reply::Null,
        Ok(Some((m, _))) => Reply::Bulk(m.encode_to_vec()),
    }
}

/// `PROTO.INFO key` → Map {schema, version, type, format, [records,] bytes}.
/// `format` is `whole` (fmt=1) or `fields` (fmt=2, with a `records` count).
pub async fn info(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 2 {
        return Reply::wrong_args("proto.info");
    }
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    let k = key.clone();
    let tail = engine
        .store
        .run_key(&key, move |ctx| read_proto_head(ctx, &k))
        .await;
    let (del, tail) = match tail {
        Err(()) => return Reply::wrongtype(),
        Ok(None) => return Reply::err("ERR no such key"),
        Ok(Some((_, del, t))) => (del, t),
    };
    let Some(ph) = protohead::decode(&tail) else {
        return Reply::err("ERR corrupt proto value");
    };
    let (schema, version, tname) = (
        ph.schema.to_string(),
        ph.schema_version,
        ph.type_name.to_string(),
    );
    if ph.fmt == protohead::FMT_V1 {
        return Reply::Map(vec![
            (Reply::bulk_str("schema"), Reply::bulk_str(schema)),
            (Reply::bulk_str("version"), Reply::Int(version as i64)),
            (Reply::bulk_str("type"), Reply::bulk_str(tname)),
            (Reply::bulk_str("format"), Reply::bulk_str("whole")),
            (Reply::bulk_str("bytes"), Reply::Int(ph.msg.len() as i64)),
        ]);
    }
    let (version, pool) = match registry::pool_for(engine, &schema, version).await {
        Ok(v) => v,
        Err(e) => return e.reply(),
    };
    let Some(desc) = pool.get_message_by_name(&tname) else {
        return ProtoErr::NoSchema(format!(
            "schema '{schema}' version {version} does not define type '{tname}'"
        ))
        .reply();
    };
    let k = key.clone();
    let (records, bytes) = engine
        .store
        .run_key(&key, move |ctx| {
            let nodes = doc::load_pnodes(ctx, &k, del);
            let n = nodes.len();
            let bytes = build_msg(&desc, &nodes).map_or(0, |d| d.msg.encoded_len());
            (n, bytes)
        })
        .await;
    Reply::Map(vec![
        (Reply::bulk_str("schema"), Reply::bulk_str(schema)),
        (Reply::bulk_str("version"), Reply::Int(version as i64)),
        (Reply::bulk_str("type"), Reply::bulk_str(tname)),
        (Reply::bulk_str("format"), Reply::bulk_str("fields")),
        (Reply::bulk_str("records"), Reply::Int(records as i64)),
        (Reply::bulk_str("bytes"), Reply::Int(bytes as i64)),
    ])
}

/// Read + decode a stored proto value into a DynamicMessage (resolves the
/// EXACT schema version the value was written with — version records are
/// retained even after PROTO.SCHEMA DEL, so old values always decode).
async fn load_message(
    engine: &Arc<Engine>,
    key: &[u8],
) -> Result<Option<(DynamicMessage, registry::ResolvedType)>, Reply> {
    engine.ensure_local(key).await;
    let k = key.to_vec();
    let tail = engine
        .store
        .run_key(key, move |ctx| read_proto_head(ctx, &k))
        .await;
    let (del, tail) = match tail {
        Err(()) => return Err(Reply::wrongtype()),
        Ok(None) => return Ok(None),
        Ok(Some((_, del, t))) => (del, t),
    };
    let Some(ph) = protohead::decode(&tail) else {
        return Err(Reply::err("ERR corrupt proto value"));
    };
    let (schema, version, type_name, fmt, msg) = (
        ph.schema.to_string(),
        ph.schema_version,
        ph.type_name.to_string(),
        ph.fmt,
        ph.msg.to_vec(),
    );
    let (version, pool) = match registry::pool_for(engine, &schema, version).await {
        Ok(v) => v,
        Err(e) => return Err(e.reply()),
    };
    let Some(desc) = pool.get_message_by_name(&type_name) else {
        return Err(ProtoErr::NoSchema(format!(
            "schema '{schema}' version {version} does not define type '{type_name}'"
        ))
        .reply());
    };
    let m = if fmt == protohead::FMT_V1 {
        match DynamicMessage::decode(desc.clone(), &msg[..]) {
            Ok(m) => m,
            Err(e) => return Err(Reply::err(format!("ERR stored value does not decode: {e}"))),
        }
    } else {
        // fmt=2: materialize the field records against the winning descriptor.
        let (k, d) = (key.to_vec(), desc.clone());
        engine
            .store
            .run_key(key, move |ctx| {
                let nodes = doc::load_pnodes(ctx, &k, del);
                build_msg(&d, &nodes)
                    .map(|doc| doc.msg)
                    .unwrap_or_else(|| DynamicMessage::new(d.clone()))
            })
            .await
    };
    Ok(Some((
        m,
        registry::ResolvedType {
            schema,
            version,
            type_name,
            desc,
        },
    )))
}

// ---------------------------------------------------------------------------
// field access: PROTO.GETFIELD / SETFIELD / CLEARFIELD
// ---------------------------------------------------------------------------

/// `PROTO.GETFIELD key path [path…]` — scalars as native RESP types, enums
/// as names, message/repeated/map as canonical JSON; unset → Null. One
/// path → the value; several → an Array.
pub async fn getfield(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 3 {
        return Reply::wrong_args("proto.getfield");
    }
    let mut paths = Vec::with_capacity(args.len() - 2);
    for raw in &args[2..] {
        match path::parse_path(raw) {
            Ok(p) => paths.push(p),
            Err(e) => return e.reply(),
        }
    }
    let (m, _) = match load_message(engine, &args[1]).await {
        Err(r) => return r,
        Ok(None) => return Reply::Null,
        Ok(Some(v)) => v,
    };
    let mut out = Vec::with_capacity(paths.len());
    for p in &paths {
        match path::get_path(&m, p) {
            Ok(Some(r)) => out.push(path::render(&r)),
            Ok(None) => out.push(Reply::Null),
            Err(e) => return e.reply(),
        }
    }
    if out.len() == 1 {
        out.pop().unwrap()
    } else {
        Reply::Array(out)
    }
}

enum RmwOut {
    Done(Reply),
    /// The stored (schema, version, type) changed between the identity read
    /// and the shard RMW — re-resolve the descriptor and try again. A
    /// concurrent fmt flip does NOT retry (both fmts materialize the same
    /// message).
    Retry,
}

/// A field-level operation applied by [`field_rmw`].
enum FieldOp {
    Set(Vec<(Vec<String>, Vec<u8>)>),
    Clear(Vec<Vec<String>>),
}

/// A resolved SETFIELD write, execution-ready (parsed value + record path).
enum SetWrite {
    Node {
        intermediates: Vec<(Vec<u8>, PVal)>,
        node_path: Vec<u8>,
        observed: Vec<Dot>,
        oneof_siblings: Vec<Vec<u8>>,
        value: Value,
        fd: FieldDescriptor,
        remove_default: bool,
    },
    ElemReplace {
        intermediates: Vec<(Vec<u8>, PVal)>,
        elem_path: Vec<u8>,
        value: Value,
        fd: FieldDescriptor,
    },
    Append {
        intermediates: Vec<(Vec<u8>, PVal)>,
        list_path: Vec<u8>,
        left: marekvs_core::json::Eid,
        value: Value,
        fd: FieldDescriptor,
    },
}

/// A resolved CLEARFIELD target.
enum ClearWrite {
    Node {
        node_path: Vec<u8>,
        observed: Vec<Dot>,
    },
    Elem {
        elem_path: Vec<u8>,
    },
    Nothing,
}

/// A proto3 non-presence scalar written at its default value decomposes to
/// *no record* (wire-format parity), so we remove instead of writing it.
fn is_scalar_kind(fd: &FieldDescriptor) -> bool {
    !fd.is_list() && !fd.is_map() && !matches!(fd.kind(), Kind::Message(_))
}

fn is_default_scalar(v: &Value) -> bool {
    match v {
        Value::Bool(b) => !*b,
        Value::I32(x) => *x == 0,
        Value::I64(x) => *x == 0,
        Value::U32(x) => *x == 0,
        Value::U64(x) => *x == 0,
        Value::F32(x) => *x == 0.0,
        Value::F64(x) => *x == 0.0,
        Value::EnumNumber(n) => *n == 0,
        Value::String(s) => s.is_empty(),
        Value::Bytes(b) => b.is_empty(),
        _ => false,
    }
}

/// Resolve + parse every SETFIELD op against the materialized value (pure —
/// no writes). Errors abort the whole command before anything is written.
fn plan_set(
    desc: &MessageDescriptor,
    pdoc: &PDoc,
    ops: &[(Vec<String>, Vec<u8>)],
    max_value: usize,
) -> Result<Vec<SetWrite>, ProtoErr> {
    let mut out = Vec::with_capacity(ops.len());
    for (segs, raw) in ops {
        if raw.len() > max_value {
            return Err(ProtoErr::Validate(format!(
                "value too large ({} bytes, limit {max_value})",
                raw.len()
            )));
        }
        let wp = resolve_path(desc, segs, &pdoc.index)?;
        match wp.target {
            Resolved::OutOfRange => return Err(ProtoErr::Path("list index out of range".into())),
            Resolved::Node {
                node_path,
                fd,
                oneof_siblings,
            } => {
                let value = path::parse_value(raw, &fd, false)?;
                let observed = pdoc
                    .index
                    .node_dots
                    .get(&node_path)
                    .cloned()
                    .unwrap_or_default();
                let remove_default =
                    is_scalar_kind(&fd) && !fd.supports_presence() && is_default_scalar(&value);
                out.push(SetWrite::Node {
                    intermediates: wp.intermediates,
                    node_path,
                    observed,
                    oneof_siblings,
                    value,
                    fd,
                    remove_default,
                });
            }
            Resolved::Elem { elem_path, fd } => {
                let value = path::parse_value(raw, &fd, true)?;
                out.push(SetWrite::ElemReplace {
                    intermediates: wp.intermediates,
                    elem_path,
                    value,
                    fd,
                });
            }
            Resolved::Append {
                list_path,
                left,
                fd,
            } => {
                let value = path::parse_value(raw, &fd, true)?;
                out.push(SetWrite::Append {
                    intermediates: wp.intermediates,
                    list_path,
                    left,
                    value,
                    fd,
                });
            }
        }
    }
    Ok(out)
}

fn execute_set(ctx: &ShardCtx, key: &[u8], del: u64, writes: Vec<SetWrite>) {
    for w in writes {
        match w {
            SetWrite::Node {
                intermediates,
                node_path,
                observed,
                oneof_siblings,
                value,
                fd,
                remove_default,
            } => {
                for (p, marker) in &intermediates {
                    doc::write_node(ctx, key, p, marker, &[]);
                }
                for sib in &oneof_siblings {
                    doc::remove_stored_node(ctx, key, sib, del);
                }
                if remove_default {
                    doc::delete_node(ctx, key, &node_path, &observed, del);
                } else {
                    doc::write_value_at(
                        ctx,
                        key,
                        &node_path,
                        Slot::Node(observed),
                        &value,
                        &fd,
                        false,
                        del,
                    );
                }
            }
            SetWrite::ElemReplace {
                intermediates,
                elem_path,
                value,
                fd,
            } => {
                for (p, marker) in &intermediates {
                    doc::write_node(ctx, key, p, marker, &[]);
                }
                doc::write_value_at(
                    ctx,
                    key,
                    &elem_path,
                    Slot::ElemReplace,
                    &value,
                    &fd,
                    true,
                    del,
                );
            }
            SetWrite::Append {
                intermediates,
                list_path,
                left,
                value,
                fd,
            } => {
                for (p, marker) in &intermediates {
                    doc::write_node(ctx, key, p, marker, &[]);
                }
                let e = doc::fresh_eid(ctx);
                let mut ep = list_path;
                push_seg(&mut ep, &PSeg::Elem(e));
                doc::write_value_at(
                    ctx,
                    key,
                    &ep,
                    Slot::ElemAppend(left),
                    &value,
                    &fd,
                    true,
                    del,
                );
            }
        }
    }
}

/// Resolve every CLEARFIELD path (pure). Unknown fields error (PROTOPATH);
/// absent targets clear nothing.
fn plan_clear(
    desc: &MessageDescriptor,
    pdoc: &PDoc,
    paths: &[Vec<String>],
) -> Result<Vec<ClearWrite>, ProtoErr> {
    let mut out = Vec::with_capacity(paths.len());
    for segs in paths {
        let wp = resolve_path(desc, segs, &pdoc.index)?;
        out.push(match wp.target {
            Resolved::Node { node_path, .. } => match pdoc.index.node_dots.get(&node_path) {
                Some(dots) => ClearWrite::Node {
                    node_path,
                    observed: dots.clone(),
                },
                None => ClearWrite::Nothing,
            },
            Resolved::Elem { elem_path, .. } => ClearWrite::Elem { elem_path },
            Resolved::Append { .. } | Resolved::OutOfRange => ClearWrite::Nothing,
        });
    }
    Ok(out)
}

fn execute_clear(ctx: &ShardCtx, key: &[u8], del: u64, clears: Vec<ClearWrite>) -> i64 {
    let mut n = 0i64;
    for c in clears {
        match c {
            ClearWrite::Node {
                node_path,
                observed,
            } => {
                doc::delete_node(ctx, key, &node_path, &observed, del);
                n += 1;
            }
            ClearWrite::Elem { elem_path } => {
                doc::delete_elem(ctx, key, &elem_path, del);
                n += 1;
            }
            ClearWrite::Nothing => {}
        }
    }
    n
}

/// One field-level RMW (design/18): identity read → async descriptor resolve
/// → shard RMW writing per-record deltas. fmt=1 values upgrade-on-write
/// (transcription stamped with the original head version) before the edit,
/// then the head is restamped fmt=2 (fresh HLC, delete clock + TTL preserved).
async fn field_rmw(engine: &Arc<Engine>, key: &[u8], op: FieldOp) -> Reply {
    engine.ensure_local(key).await;
    let max_value = engine.proto.limits.max_value;
    let op = Arc::new(op);
    for _ in 0..3 {
        // 1. identity read: which (schema, version, type) is stored now?
        let k = key.to_vec();
        let tail = engine
            .store
            .run_key(key, move |ctx| read_proto_head(ctx, &k))
            .await;
        let (schema, version, tname) = match tail {
            Err(()) => return Reply::wrongtype(),
            Ok(None) => return Reply::err("ERR no such key (write it with PROTO.SET first)"),
            Ok(Some((_, _, t))) => match protohead::decode(&t) {
                Some(ph) => (
                    ph.schema.to_string(),
                    ph.schema_version,
                    ph.type_name.to_string(),
                ),
                None => return Reply::err("ERR corrupt proto value"),
            },
        };
        // 2. resolve the descriptor (async — never from inside the shard job).
        let (_, pool) = match registry::pool_for(engine, &schema, version).await {
            Ok(v) => v,
            Err(e) => return e.reply(),
        };
        let Some(desc) = pool.get_message_by_name(&tname) else {
            return ProtoErr::NoSchema(format!(
                "schema '{schema}' version {version} does not define type '{tname}'"
            ))
            .reply();
        };
        // 3. shard-thread RMW.
        let (k, op2) = (key.to_vec(), op.clone());
        let (schema_c, tname_c) = (schema.clone(), tname.clone());
        let out = engine
            .store
            .run_key(key, move |ctx| {
                let (env, del, tail) = match read_proto_head(ctx, &k) {
                    Err(()) => return RmwOut::Done(Reply::wrongtype()),
                    Ok(None) => {
                        return RmwOut::Done(Reply::err(
                            "ERR no such key (write it with PROTO.SET first)",
                        ))
                    }
                    Ok(Some(v)) => v,
                };
                let Some(ph) = protohead::decode(&tail) else {
                    return RmwOut::Done(Reply::err("ERR corrupt proto value"));
                };
                if ph.schema != schema_c || ph.schema_version != version || ph.type_name != tname_c
                {
                    return RmwOut::Retry;
                }
                // Materialize. fmt=1: from the decoded legacy message via
                // transcription (no writes yet — planning is pure). fmt=2:
                // from the stored field records.
                let legacy: Option<DynamicMessage>;
                let pdoc = if ph.fmt == protohead::FMT_V1 {
                    let msg = match DynamicMessage::decode(desc.clone(), ph.msg) {
                        Ok(m) => m,
                        Err(e) => {
                            return RmwOut::Done(Reply::err(format!(
                                "ERR stored value does not decode: {e}"
                            )))
                        }
                    };
                    let dot = Dot {
                        hlc: env.hlc,
                        origin: env.origin,
                    };
                    let nodes: Vec<(Vec<u8>, PNodeIn)> =
                        fields::transcribe_records(&msg, env.hlc, env.origin)
                            .into_iter()
                            .map(|r| match r {
                                PRecord::Node { path, val } => (
                                    path,
                                    PNodeIn::Node {
                                        val,
                                        dots: vec![dot],
                                    },
                                ),
                                PRecord::Elem { path, elem } => {
                                    (path, PNodeIn::Elem { elem, live: true })
                                }
                            })
                            .collect();
                    let Some(pdoc) = build_msg(&desc, &nodes) else {
                        return RmwOut::Done(Reply::err("ERR corrupt proto value"));
                    };
                    legacy = Some(msg);
                    pdoc
                } else {
                    let nodes = doc::load_pnodes(ctx, &k, del);
                    let Some(pdoc) = build_msg(&desc, &nodes) else {
                        return RmwOut::Done(Reply::err("ERR no such key"));
                    };
                    legacy = None;
                    pdoc
                };
                // Plan (pure): errors abort before any write.
                let reply = match &*op2 {
                    FieldOp::Set(ops) => match plan_set(&desc, &pdoc, ops, max_value) {
                        Ok(writes) => {
                            if let Some(m) = &legacy {
                                doc::transcribe_v1(ctx, &k, env.hlc, env.origin, m);
                            }
                            execute_set(ctx, &k, del, writes);
                            Reply::ok()
                        }
                        Err(e) => return RmwOut::Done(e.reply()),
                    },
                    FieldOp::Clear(paths) => match plan_clear(&desc, &pdoc, paths) {
                        Ok(clears) => {
                            if let Some(m) = &legacy {
                                doc::transcribe_v1(ctx, &k, env.hlc, env.origin, m);
                            }
                            Reply::Int(execute_clear(ctx, &k, del, clears))
                        }
                        Err(e) => return RmwOut::Done(e.reply()),
                    },
                };
                // Restamp the head fmt=2 (fresh HLC, delete clock + TTL kept).
                let mut payload = head::encode(head::CTYPE_PROTO, del);
                payload.extend_from_slice(&protohead::encode_v2(&schema_c, version, &tname_c));
                let env2 = Envelope::head(ctx.hlc.now(), ctx.node_id).with_ttl(env.ttl_deadline_ms);
                write_merged(ctx, &ikey::head_key(&k), &env2.encode_with(&payload));
                RmwOut::Done(reply)
            })
            .await;
        match out {
            RmwOut::Done(r) => return r,
            RmwOut::Retry => continue,
        }
    }
    Reply::err("TRYAGAIN concurrent type change on the key; retry")
}

/// `PROTO.SETFIELD key path value [path value…]` — per-field CRDT writes;
/// scalar values from strings, message/repeated/map values from JSON.
pub async fn setfield(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 4 || (args.len() - 2) % 2 != 0 {
        return Reply::wrong_args("proto.setfield");
    }
    let mut ops: Vec<(Vec<String>, Vec<u8>)> = Vec::with_capacity((args.len() - 2) / 2);
    for pair in args[2..].chunks(2) {
        match path::parse_path(&pair[0]) {
            Ok(p) => ops.push((p, pair[1].clone())),
            Err(e) => return e.reply(),
        }
    }
    field_rmw(engine, &args[1], FieldOp::Set(ops)).await
}

/// `PROTO.CLEARFIELD key path [path…]` → Int: how many paths had something
/// to clear.
pub async fn clearfield(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 3 {
        return Reply::wrong_args("proto.clearfield");
    }
    let mut paths = Vec::with_capacity(args.len() - 2);
    for raw in &args[2..] {
        match path::parse_path(raw) {
            Ok(p) => paths.push(p),
            Err(e) => return e.reply(),
        }
    }
    field_rmw(engine, &args[1], FieldOp::Clear(paths)).await
}

/// `PROTO.GETJSON key` → canonical protobuf-JSON of the stored message.
pub async fn getjson(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 2 {
        return Reply::wrong_args("proto.getjson");
    }
    match load_message(engine, &args[1]).await {
        Err(r) => r,
        Ok(None) => Reply::Null,
        Ok(Some((m, _))) => match serde_json::to_string(&m) {
            Ok(s) => Reply::Bulk(s.into_bytes()),
            Err(e) => Reply::err(format!("ERR json render failed: {e}")),
        },
    }
}

// ---------------------------------------------------------------------------
// validated collection elements: PROTO.HSET / SADD / HGETJSON / HGETFIELD
// ---------------------------------------------------------------------------

/// Parse the optional `TYPE t` clause at `args[at]`, returning the type and
/// the index of the first argument after the clause.
fn parse_type_clause(args: &[Vec<u8>], at: usize) -> Result<(Option<String>, usize), Reply> {
    if args.len() > at && eq_ignore_case(&args[at], "TYPE") {
        let Some(t) = args.get(at + 1) else {
            return Err(Reply::syntax());
        };
        let Ok(t) = String::from_utf8(t.clone()) else {
            return Err(Reply::err("ERR type name must be utf-8"));
        };
        return Ok((Some(t), at + 2));
    }
    Ok((None, at))
}

/// Validate `raw` decodes as the resolved type (with the value size bound).
fn validate_element(
    engine: &Arc<Engine>,
    resolved: &registry::ResolvedType,
    raw: &[u8],
) -> Result<(), Reply> {
    if raw.len() > engine.proto.limits.max_value {
        return Err(ProtoErr::Validate(format!(
            "value too large ({} bytes, limit {})",
            raw.len(),
            engine.proto.limits.max_value
        ))
        .reply());
    }
    DynamicMessage::decode(resolved.desc.clone(), raw)
        .map(|_| ())
        .map_err(|e| ProtoErr::Validate(format!("{}: {e}", resolved.type_name)).reply())
}

/// `PROTO.HSET key [TYPE t] field value [field value…]` — validate every
/// value against the resolved type, then DELEGATE to `hash::hset` verbatim
/// (element payloads stay raw proto bytes; OR-merge machinery untouched).
pub async fn hset(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 4 {
        return Reply::wrong_args("proto.hset");
    }
    let key = args[1].clone();
    let (type_name, rest_at) = match parse_type_clause(args, 2) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let rest = &args[rest_at..];
    if rest.is_empty() || rest.len() % 2 != 0 {
        return Reply::wrong_args("proto.hset");
    }
    let resolved =
        match registry::resolve_for_key(engine, &key, type_name.as_deref(), None, 0).await {
            Ok(r) => r,
            Err(e) => return e.reply(),
        };
    for pair in rest.chunks(2) {
        if let Err(r) = validate_element(engine, &resolved, &pair[1]) {
            return r; // nothing written: all values validate up front
        }
    }
    let mut delegate: Vec<Vec<u8>> = Vec::with_capacity(2 + rest.len());
    delegate.push(b"HSET".to_vec());
    delegate.push(key);
    delegate.extend(rest.iter().cloned());
    super::hash::hset(engine, &delegate, false).await
}

/// `PROTO.SADD key [TYPE t] member [member…]` — validate, then delegate to
/// `set::sadd`.
pub async fn sadd(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 3 {
        return Reply::wrong_args("proto.sadd");
    }
    let key = args[1].clone();
    let (type_name, rest_at) = match parse_type_clause(args, 2) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let rest = &args[rest_at..];
    if rest.is_empty() {
        return Reply::wrong_args("proto.sadd");
    }
    let resolved =
        match registry::resolve_for_key(engine, &key, type_name.as_deref(), None, 0).await {
            Ok(r) => r,
            Err(e) => return e.reply(),
        };
    for member in rest {
        if let Err(r) = validate_element(engine, &resolved, member) {
            return r;
        }
    }
    let mut delegate: Vec<Vec<u8>> = Vec::with_capacity(2 + rest.len());
    delegate.push(b"SADD".to_vec());
    delegate.push(key);
    delegate.extend(rest.iter().cloned());
    super::set::sadd(engine, &delegate).await
}

/// Read one hash element and decode it against the resolved type. The type
/// is resolved at READ time (explicit TYPE > binding) — rebinding a prefix
/// changes interpretation, not the stored bytes (documented caveat).
async fn load_hash_element(
    engine: &Arc<Engine>,
    key: &[u8],
    field: &[u8],
    type_name: Option<&str>,
) -> Result<Option<DynamicMessage>, Reply> {
    let resolved = match registry::resolve_for_key(engine, key, type_name, None, 0).await {
        Ok(r) => r,
        Err(e) => return Err(e.reply()),
    };
    let raw =
        match super::hash::hget(engine, &[b"HGET".to_vec(), key.to_vec(), field.to_vec()]).await {
            Reply::Bulk(b) => b,
            Reply::Null => return Ok(None),
            other => return Err(other), // WRONGTYPE etc.
        };
    match DynamicMessage::decode(resolved.desc.clone(), &raw[..]) {
        Ok(m) => Ok(Some(m)),
        Err(e) => Err(ProtoErr::Validate(format!(
            "stored element does not decode as {}: {e}",
            resolved.type_name
        ))
        .reply()),
    }
}

/// `PROTO.HGETJSON key field [TYPE t]` → canonical protobuf-JSON of one
/// hash element.
pub async fn hgetjson(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 3 && args.len() != 5 {
        return Reply::wrong_args("proto.hgetjson");
    }
    let (type_name, rest_at) = match parse_type_clause(args, 3) {
        Ok(v) => v,
        Err(r) => return r,
    };
    if rest_at != args.len() {
        return Reply::syntax();
    }
    match load_hash_element(engine, &args[1], &args[2], type_name.as_deref()).await {
        Err(r) => r,
        Ok(None) => Reply::Null,
        Ok(Some(m)) => match serde_json::to_string(&m) {
            Ok(s) => Reply::Bulk(s.into_bytes()),
            Err(e) => Reply::err(format!("ERR json render failed: {e}")),
        },
    }
}

/// `PROTO.HGETFIELD key field path [TYPE t]` → one field of one hash
/// element (GETFIELD rendering rules).
pub async fn hgetfield(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 4 && args.len() != 6 {
        return Reply::wrong_args("proto.hgetfield");
    }
    let (type_name, rest_at) = match parse_type_clause(args, 4) {
        Ok(v) => v,
        Err(r) => return r,
    };
    if rest_at != args.len() {
        return Reply::syntax();
    }
    let segs = match path::parse_path(&args[3]) {
        Ok(p) => p,
        Err(e) => return e.reply(),
    };
    match load_hash_element(engine, &args[1], &args[2], type_name.as_deref()).await {
        Err(r) => r,
        Ok(None) => Reply::Null,
        Ok(Some(m)) => match path::get_path(&m, &segs) {
            Ok(Some(r)) => path::render(&r),
            Ok(None) => Reply::Null,
            Err(e) => e.reply(),
        },
    }
}

/// `PROTO.BINDINGS [MATCH glob]` → Map prefix → {schema, version, type}.
pub async fn bindings_cmd(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    let pattern: Option<Vec<u8>> = match args.len() {
        1 => None,
        3 if eq_ignore_case(&args[1], "MATCH") => Some(args[2].clone()),
        _ => return Reply::syntax(),
    };
    let mut pairs = Vec::new();
    for (prefix, rec) in registry::bindings(engine).await {
        if pattern
            .as_deref()
            .is_some_and(|pat| !glob_match(pat, &prefix))
        {
            continue;
        }
        pairs.push((
            Reply::Bulk(prefix),
            Reply::Map(vec![
                (Reply::bulk_str("schema"), Reply::bulk_str(rec.schema)),
                (Reply::bulk_str("version"), Reply::Int(rec.version as i64)),
                (Reply::bulk_str("type"), Reply::bulk_str(rec.type_name)),
            ]),
        ));
    }
    Reply::Map(pairs)
}

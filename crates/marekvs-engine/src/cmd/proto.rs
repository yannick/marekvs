//! PROTO.* — protobuf schema registry, prefix bindings and typed values
//! (design/17).
//!
//! Registry state lives in hidden replicated system records (`\x00proto:*`,
//! see `crate::proto::registry`); protox compilation always runs in
//! `tokio::task::spawn_blocking`, never on shard threads.

use std::sync::Arc;

use prost::Message;
use prost_reflect::DynamicMessage;

use crate::cmd::{eq_ignore_case, parse_u64};
use crate::proto::{compile, registry, ProtoErr};
use crate::pubsub::glob_match;
use crate::reply::Reply;
use crate::store::{get_head, get_raw, now_ms, read_lww, write_merged, ShardCtx};
use crate::Engine;
use marekvs_core::envelope::{head, Envelope};
use marekvs_core::{ikey, protohead};

/// Reasonable cap on registry schema names.
const MAX_NAME: usize = 255;

fn parse_name(raw: &[u8]) -> Result<String, Reply> {
    let s = std::str::from_utf8(raw)
        .map_err(|_| Reply::err("SCHEMAERR schema name must be utf-8"))?;
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
        return Reply::Array(out.types.iter().map(|t| Reply::bulk_str(t.clone())).collect());
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

/// Resolve the message type for `key`, validate `msg` decodes against it,
/// then LWW-write the head record (del-clock carry-forward, NX/XX, TTL).
async fn write_value(
    engine: &Arc<Engine>,
    key: Vec<u8>,
    msg: Vec<u8>,
    o: SetOpts,
    resolved: registry::ResolvedType,
) -> Reply {
    if msg.len() > engine.proto.limits.max_value {
        return ProtoErr::Validate(format!(
            "value too large ({} bytes, limit {})",
            msg.len(),
            engine.proto.limits.max_value
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
                (None, true) => existing.as_ref().map_or(0, |(env, _, _)| env.ttl_deadline_ms),
                (None, false) => 0,
            };
            let mut payload = head::encode(head::CTYPE_PROTO, prev_del);
            payload.extend_from_slice(&protohead::encode(&schema, version, &type_name, &msg));
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
    if let Err(e) = DynamicMessage::decode(resolved.desc.clone(), &msg[..]) {
        return ProtoErr::Validate(format!("{}: {e}", resolved.type_name)).reply();
    }
    write_value(engine, key, msg, o, resolved).await
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
    write_value(engine, key, msg.encode_to_vec(), o, resolved).await
}

/// `PROTO.GET key` → the raw message bytes (no schema needed).
pub async fn get(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 2 {
        return Reply::wrong_args("proto.get");
    }
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| match read_proto_head(ctx, &key) {
            Err(()) => Reply::wrongtype(),
            Ok(None) => Reply::Null,
            Ok(Some((_, _, tail))) => match protohead::decode(&tail) {
                Some(ph) => Reply::Bulk(ph.msg.to_vec()),
                None => Reply::err("ERR corrupt proto value"),
            },
        })
        .await
}

/// `PROTO.INFO key` → Map {schema, version, type, bytes}.
pub async fn info(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 2 {
        return Reply::wrong_args("proto.info");
    }
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| match read_proto_head(ctx, &key) {
            Err(()) => Reply::wrongtype(),
            Ok(None) => Reply::err("ERR no such key"),
            Ok(Some((_, _, tail))) => match protohead::decode(&tail) {
                Some(ph) => Reply::Map(vec![
                    (Reply::bulk_str("schema"), Reply::bulk_str(ph.schema)),
                    (
                        Reply::bulk_str("version"),
                        Reply::Int(ph.schema_version as i64),
                    ),
                    (Reply::bulk_str("type"), Reply::bulk_str(ph.type_name)),
                    (Reply::bulk_str("bytes"), Reply::Int(ph.msg.len() as i64)),
                ]),
                None => Reply::err("ERR corrupt proto value"),
            },
        })
        .await
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
    let tail = match tail {
        Err(()) => return Err(Reply::wrongtype()),
        Ok(None) => return Ok(None),
        Ok(Some((_, _, t))) => t,
    };
    let Some(ph) = protohead::decode(&tail) else {
        return Err(Reply::err("ERR corrupt proto value"));
    };
    let (schema, version, type_name, msg) = (
        ph.schema.to_string(),
        ph.schema_version,
        ph.type_name.to_string(),
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
    let m = match DynamicMessage::decode(desc.clone(), &msg[..]) {
        Ok(m) => m,
        Err(e) => return Err(Reply::err(format!("ERR stored value does not decode: {e}"))),
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

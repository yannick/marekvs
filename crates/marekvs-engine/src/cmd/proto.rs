//! PROTO.* — protobuf schema registry, prefix bindings and typed values
//! (design/17).
//!
//! Registry state lives in hidden replicated system records (`\x00proto:*`,
//! see `crate::proto::registry`); protox compilation always runs in
//! `tokio::task::spawn_blocking`, never on shard threads.

use std::sync::Arc;

use crate::cmd::eq_ignore_case;
use crate::proto::{compile, registry, ProtoErr};
use crate::pubsub::glob_match;
use crate::reply::Reply;
use crate::store::now_ms;
use crate::Engine;

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

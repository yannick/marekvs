//! Schema registry storage (design/17): hidden replicated system records
//! under `\x00proto:*` in the ordinary data CF — the SCRIPT LOAD precedent.
//! They replicate like data, survive restarts, are filtered from SCAN/KEYS,
//! and read-through (`ensure_local`) heals misses on other nodes.
//!
//! ```text
//! \x00proto:s:<name>             String: postcard(SchemaRecord) — latest
//! \x00proto:v:<name>:<%08x ver>  String: postcard(SchemaRecord) — immutable
//! \x00proto:idx                  Hash: name → latest version (ascii)
//! \x00proto:bind                 Hash: prefix → postcard(BindingRecord)
//! ```

use std::sync::Arc;

use prost_reflect::{DescriptorPool, MessageDescriptor};
use serde::{Deserialize, Serialize};

use crate::store::{self, now_ms, read_lww, scan_prefix, write_merged};
use crate::Engine;
use marekvs_core::envelope::{head, Envelope, RecordType};
use marekvs_core::ikey;
use marekvs_core::merge::{element_add, element_dots, element_remove, element_value};

use super::compile;
use super::err::ProtoErr;
use super::BindCache;

pub const KIND_SOURCE: u8 = 0;
pub const KIND_DESCRIPTOR: u8 = 1;

/// One schema version as stored in the registry. The `fds` bytes are a
/// SELF-CONTAINED compiled FileDescriptorSet (imports inlined at compile
/// time), so old values always decode even if their imports were deleted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaRecord {
    pub version: u32,
    /// KIND_SOURCE (uploaded as `.proto` text) or KIND_DESCRIPTOR.
    pub kind: u8,
    /// Original source text (empty for descriptor uploads).
    pub source: Vec<u8>,
    /// Self-contained compiled FileDescriptorSet.
    pub fds: Vec<u8>,
    /// Import file names the source declared (informational).
    pub imports: Vec<String>,
    /// Fully-qualified message type names the schema defines.
    pub types: Vec<String>,
    pub created_ms: u64,
}

/// A prefix binding: keys under `prefix` default to `type_name` of
/// `schema` (version 0 = track latest).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BindingRecord {
    pub schema: String,
    pub version: u32,
    pub type_name: String,
}

pub const SYS_IDX_KEY: &[u8] = b"\x00proto:idx";
pub const SYS_BIND_KEY: &[u8] = b"\x00proto:bind";

pub fn latest_key(name: &str) -> Vec<u8> {
    [b"\x00proto:s:", name.as_bytes()].concat()
}

pub fn version_key(name: &str, version: u32) -> Vec<u8> {
    format!("\x00proto:v:{name}:{version:08x}").into_bytes()
}

// ---------------------------------------------------------------------------
// hidden-record primitives (string + hash, on shard threads)
// ---------------------------------------------------------------------------

async fn read_sys_string(engine: &Arc<Engine>, syskey: Vec<u8>) -> Option<Vec<u8>> {
    // EVALSHA fallback pattern: give the repl layer a chance to fetch the
    // exact hidden key from a home replica before concluding absence.
    engine.ensure_local(&syskey).await;
    let k = syskey.clone();
    engine
        .store
        .run_key(&syskey, move |ctx| {
            read_lww(ctx, &ikey::string_key(&k), 0).map(|(_, payload)| payload)
        })
        .await
}

async fn write_sys_string(engine: &Arc<Engine>, syskey: Vec<u8>, payload: Vec<u8>) {
    let k = syskey.clone();
    engine
        .store
        .run_key(&syskey, move |ctx| {
            let rec = store::new_lww(ctx, RecordType::String, &payload, 0);
            write_merged(ctx, &ikey::string_key(&k), &rec);
        })
        .await;
}

async fn tombstone_sys_string(engine: &Arc<Engine>, syskey: Vec<u8>) -> bool {
    engine.ensure_local(&syskey).await;
    let k = syskey.clone();
    engine
        .store
        .run_key(&syskey, move |ctx| {
            if read_lww(ctx, &ikey::string_key(&k), 0).is_none() {
                return false;
            }
            let tomb = store::new_tombstone(ctx, RecordType::String);
            write_merged(ctx, &ikey::string_key(&k), &tomb);
            true
        })
        .await
}

async fn hset_sys(engine: &Arc<Engine>, syskey: Vec<u8>, field: Vec<u8>, value: Vec<u8>) {
    engine.ensure_local(&syskey).await; // adds must observe existing dots
    let k = syskey.clone();
    engine
        .store
        .run_key(&syskey, move |ctx| {
            store::ensure_head(ctx, &k, head::CTYPE_HASH);
            let rec = element_add(RecordType::HashField, ctx.hlc.now(), ctx.node_id, &value);
            write_merged(ctx, &ikey::hash_field_key(&k, &field), &rec);
        })
        .await;
}

async fn hdel_sys(engine: &Arc<Engine>, syskey: Vec<u8>, field: Vec<u8>) -> bool {
    engine.ensure_local(&syskey).await; // observed-remove needs local dots
    let k = syskey.clone();
    engine
        .store
        .run_key(&syskey, move |ctx| {
            let ik = ikey::hash_field_key(&k, &field);
            let Some(v) = store::get_raw(ctx, &ik) else {
                return false;
            };
            let Some((env, pay)) = Envelope::decode(&v) else {
                return false;
            };
            if store::visible(&env, pay, 0, now_ms()).is_none() || element_value(pay).is_none() {
                return false;
            }
            let dots = element_dots(pay);
            let rm = element_remove(RecordType::HashField, ctx.hlc.now(), ctx.node_id, &dots);
            write_merged(ctx, &ik, &rm);
            true
        })
        .await
}

async fn hgetall_sys(engine: &Arc<Engine>, syskey: Vec<u8>) -> Vec<(Vec<u8>, Vec<u8>)> {
    hgetall_sys_traced(engine, syskey).await.0
}

/// Like `hgetall_sys`, additionally reporting whether ANY record bytes for
/// the hash exist locally (live or tombstoned). "Empty with a trace" is an
/// authoritative empty (everything was removed); "empty with no trace" means
/// this node simply does not hold the data — a partitioned non-owner cannot
/// tell absence from ignorance.
async fn hgetall_sys_traced(
    engine: &Arc<Engine>,
    syskey: Vec<u8>,
) -> (Vec<(Vec<u8>, Vec<u8>)>, bool) {
    engine.ensure_local(&syskey).await;
    let k = syskey.clone();
    engine
        .store
        .run_key(&syskey, move |ctx| {
            let now = now_ms();
            let mut out = Vec::new();
            let mut saw_record = false;
            scan_prefix(
                ctx,
                &ikey::collection_prefix(ikey::Tag::HashField, &k),
                |ik, v| {
                    saw_record = true;
                    if let (Some(p), Some((env, pay))) = (ikey::parse(ik), Envelope::decode(v)) {
                        if store::visible(&env, pay, 0, now).is_some() {
                            if let Some(val) = element_value(pay) {
                                out.push((p.suffix.to_vec(), val));
                            }
                        }
                    }
                    true
                },
            );
            (out, saw_record)
        })
        .await
}

// ---------------------------------------------------------------------------
// schema records
// ---------------------------------------------------------------------------

/// Load one schema record; `version == 0` = latest.
pub async fn load_schema(
    engine: &Arc<Engine>,
    name: &str,
    version: u32,
) -> Result<SchemaRecord, ProtoErr> {
    let syskey = if version == 0 {
        latest_key(name)
    } else {
        version_key(name, version)
    };
    let Some(bytes) = read_sys_string(engine, syskey).await else {
        return Err(ProtoErr::NoSchema(if version == 0 {
            format!("no schema '{name}'")
        } else {
            format!("no schema '{name}' version {version}")
        }));
    };
    postcard::from_bytes(&bytes)
        .map_err(|e| ProtoErr::Other(format!("ERR corrupt schema record for '{name}': {e}")))
}

/// Store one schema version: immutable per-version record, latest pointer,
/// idx entry. Concurrent same-name SET is LWW on the latest pointer
/// (documented admin caveat).
pub async fn store_schema(engine: &Arc<Engine>, name: &str, rec: &SchemaRecord) {
    let bytes = postcard::to_allocvec(rec).expect("schema record encodes");
    write_sys_string(engine, version_key(name, rec.version), bytes.clone()).await;
    write_sys_string(engine, latest_key(name), bytes).await;
    hset_sys(
        engine,
        SYS_IDX_KEY.to_vec(),
        name.as_bytes().to_vec(),
        rec.version.to_string().into_bytes(),
    )
    .await;
}

/// Tombstone a schema's latest pointer + idx entry. Version records are
/// RETAINED so stored values keep decoding.
pub async fn delete_schema(engine: &Arc<Engine>, name: &str) -> bool {
    let had = tombstone_sys_string(engine, latest_key(name)).await;
    let deleted = hdel_sys(engine, SYS_IDX_KEY.to_vec(), name.as_bytes().to_vec()).await;
    had || deleted
}

/// All registered schemas as (name, latest version), from the idx hash.
pub async fn schema_index(engine: &Arc<Engine>) -> Vec<(String, u32)> {
    let mut out: Vec<(String, u32)> = hgetall_sys(engine, SYS_IDX_KEY.to_vec())
        .await
        .into_iter()
        .filter_map(|(name, ver)| {
            Some((
                String::from_utf8(name).ok()?,
                std::str::from_utf8(&ver).ok()?.parse().ok()?,
            ))
        })
        .collect();
    out.sort();
    out
}

// ---------------------------------------------------------------------------
// descriptor pools
// ---------------------------------------------------------------------------

/// The compiled pool for `(name, version)` (0 = latest). Cached per exact
/// version — entries are immutable, no invalidation. Returns the ACTUAL
/// version resolved.
pub async fn pool_for(
    engine: &Arc<Engine>,
    name: &str,
    version: u32,
) -> Result<(u32, DescriptorPool), ProtoErr> {
    if version != 0 {
        if let Some(pool) = engine.proto.pools.lock().get(name, version) {
            return Ok((version, pool));
        }
    }
    let rec = load_schema(engine, name, version).await?;
    if let Some(pool) = engine.proto.pools.lock().get(name, rec.version) {
        return Ok((rec.version, pool));
    }
    let pool = compile::pool_from_fds(&rec.fds)?;
    engine
        .proto
        .pools
        .lock()
        .put(name.to_string(), rec.version, pool.clone());
    Ok((rec.version, pool))
}

/// Search the whole registry for the schema defining `type_name`.
/// Ambiguity across schemas is an error (pass SCHEMA to disambiguate).
pub async fn find_type(engine: &Arc<Engine>, type_name: &str) -> Result<(String, u32), ProtoErr> {
    let mut hits: Vec<(String, u32)> = Vec::new();
    for (name, ver) in schema_index(engine).await {
        if let Ok(rec) = load_schema(engine, &name, 0).await {
            if rec.types.iter().any(|t| t == type_name) {
                hits.push((name, ver));
            }
        }
    }
    match hits.len() {
        0 => Err(ProtoErr::NoSchema(format!(
            "no registered schema defines type '{type_name}'"
        ))),
        1 => Ok(hits.pop().unwrap()),
        _ => Err(ProtoErr::Schema(format!(
            "type '{type_name}' is ambiguous across schemas {}; pass SCHEMA",
            hits.iter()
                .map(|(n, _)| n.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ))),
    }
}

// ---------------------------------------------------------------------------
// bindings
// ---------------------------------------------------------------------------

/// Should a refresh that came back EMPTY replace a non-empty cached table?
/// Only when the emptiness is authoritative (this node holds the hash's
/// records — live or tombstoned — so "no live entries" is a fact). With no
/// local trace, the node cannot tell "all unbound" from "I never got the
/// data / the mesh is cut" — keep serving the stale table (AP: bounded
/// staleness beats spurious NOBINDING during partitions).
fn accept_empty_refresh(saw_local_trace: bool) -> bool {
    saw_local_trace
}

/// Current binding table, longest-prefix-first. Node-locally cached for
/// `bind_ttl_ms` (refreshed immediately after local BIND/UNBIND).
pub async fn bindings(engine: &Arc<Engine>) -> Vec<(Vec<u8>, BindingRecord)> {
    let now = now_ms();
    {
        let cache = engine.proto.bindings.lock();
        if let Some(c) = cache.as_ref() {
            if now.saturating_sub(c.loaded_ms) < engine.proto.bind_ttl_ms {
                return c.entries.clone();
            }
        }
    }
    refresh_bindings(engine).await
}

/// Bypass the TTL and refresh the binding cache now (warmer + post-BIND).
pub async fn refresh_bindings(engine: &Arc<Engine>) -> Vec<(Vec<u8>, BindingRecord)> {
    let now = now_ms();
    let (raw_entries, saw_trace) = hgetall_sys_traced(engine, SYS_BIND_KEY.to_vec()).await;
    let mut entries: Vec<(Vec<u8>, BindingRecord)> = raw_entries
        .into_iter()
        .filter_map(|(prefix, raw)| Some((prefix, postcard::from_bytes(&raw).ok()?)))
        .collect();
    if entries.is_empty() && !accept_empty_refresh(saw_trace) {
        let cache = engine.proto.bindings.lock();
        if let Some(c) = cache.as_ref() {
            if !c.entries.is_empty() {
                // stale-serve; loaded_ms untouched so the next call retries
                return c.entries.clone();
            }
        }
    }
    // Longest prefix first; ties break lexicographically for determinism.
    entries.sort_by(|a, b| b.0.len().cmp(&a.0.len()).then(a.0.cmp(&b.0)));
    *engine.proto.bindings.lock() = Some(BindCache {
        loaded_ms: now,
        entries: entries.clone(),
    });
    entries
}

/// Background binding/descriptor warmer: refreshes the binding table and
/// resolves every bound `(schema, version)` into the descriptor-pool LRU.
/// The read-throughs this triggers pull the hidden registry records onto
/// THIS node (with an interest subscription) while the mesh is healthy — so
/// a later partition finds bindings and pools already local, and typed
/// writes keep working on every node (design/17 availability note).
/// `MAREKVS_PROTO_WARM_SECS` tunes the cadence; 0 disables.
pub fn spawn_warmer(engine: Arc<Engine>) -> Option<tokio::task::JoinHandle<()>> {
    let secs = super::env_u64("MAREKVS_PROTO_WARM_SECS", 30);
    if secs == 0 {
        return None;
    }
    Some(tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(secs));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let table = refresh_bindings(&engine).await;
            for (_, rec) in &table {
                // version 0 tracks latest: resolving it re-reads the latest
                // pointer and warms that version's pool
                let _ = pool_for(&engine, &rec.schema, rec.version).await;
            }
        }
    }))
}

pub async fn set_binding(engine: &Arc<Engine>, prefix: &[u8], rec: &BindingRecord) {
    let raw = postcard::to_allocvec(rec).expect("binding record encodes");
    hset_sys(engine, SYS_BIND_KEY.to_vec(), prefix.to_vec(), raw).await;
    engine.proto.invalidate_bindings();
}

pub async fn remove_binding(engine: &Arc<Engine>, prefix: &[u8]) -> bool {
    let removed = hdel_sys(engine, SYS_BIND_KEY.to_vec(), prefix.to_vec()).await;
    engine.proto.invalidate_bindings();
    removed
}

/// Longest-prefix binding covering `key`, if any.
pub async fn binding_for_key(engine: &Arc<Engine>, key: &[u8]) -> Option<BindingRecord> {
    bindings(engine)
        .await
        .into_iter()
        .find(|(prefix, _)| key.starts_with(prefix))
        .map(|(_, rec)| rec)
}

// ---------------------------------------------------------------------------
// type resolution for typed commands
// ---------------------------------------------------------------------------

/// A fully resolved message type for a typed-value command.
pub struct ResolvedType {
    pub schema: String,
    pub version: u32,
    pub type_name: String,
    pub desc: MessageDescriptor,
}

/// Resolution order (design/17): explicit TYPE arg → longest-prefix
/// binding → NOBINDING.
pub async fn resolve_for_key(
    engine: &Arc<Engine>,
    key: &[u8],
    explicit_type: Option<&str>,
    explicit_schema: Option<&str>,
    explicit_version: u32,
) -> Result<ResolvedType, ProtoErr> {
    let (schema, version, type_name) = if let Some(t) = explicit_type {
        match explicit_schema {
            Some(s) => (s.to_string(), explicit_version, t.to_string()),
            None => {
                let (schema, _) = find_type(engine, t).await?;
                (schema, explicit_version, t.to_string())
            }
        }
    } else if let Some(b) = binding_for_key(engine, key).await {
        (b.schema, b.version, b.type_name)
    } else {
        return Err(ProtoErr::NoBinding);
    };
    let (version, pool) = pool_for(engine, &schema, version).await?;
    let desc = pool.get_message_by_name(&type_name).ok_or_else(|| {
        ProtoErr::NoSchema(format!(
            "schema '{schema}' version {version} does not define type '{type_name}'"
        ))
    })?;
    Ok(ResolvedType {
        schema,
        version,
        type_name,
        desc,
    })
}

/// BFS-resolve the import closure of `source` from the registry into a
/// protox dep map (design/17): `google/protobuf/*` resolves from protox's
/// bundled well-known types and is skipped here. Depth ≤ max_depth,
/// files ≤ max_files.
pub async fn resolve_imports(
    engine: &Arc<Engine>,
    source: &str,
) -> Result<std::collections::HashMap<String, compile::DepFile>, ProtoErr> {
    use std::collections::HashMap;
    let limits = engine.proto.limits;
    let mut deps: HashMap<String, compile::DepFile> = HashMap::new();
    let mut frontier: Vec<String> = compile::extract_imports(source);
    let mut depth = 0usize;
    while !frontier.is_empty() {
        depth += 1;
        if depth > limits.max_depth {
            return Err(ProtoErr::Schema(format!(
                "import chain deeper than {} levels",
                limits.max_depth
            )));
        }
        let mut next: Vec<String> = Vec::new();
        for import in frontier {
            if import.starts_with("google/protobuf/") || deps.contains_key(&import) {
                continue;
            }
            if deps.len() + 1 > limits.max_files {
                return Err(ProtoErr::Schema(format!(
                    "more than {} files in the import closure",
                    limits.max_files
                )));
            }
            // Registry lookup: exact import name first, then the schema
            // name without the `.proto` suffix.
            let rec = match load_schema(engine, &import, 0).await {
                Ok(r) => r,
                Err(_) => {
                    let trimmed = import.strip_suffix(".proto").unwrap_or(&import);
                    load_schema(engine, trimmed, 0).await.map_err(|_| {
                        ProtoErr::Schema(format!(
                            "import '{import}' not found (upload it first or use DESCRIPTOR)"
                        ))
                    })?
                }
            };
            if rec.kind == KIND_SOURCE {
                let src = String::from_utf8(rec.source.clone()).map_err(|_| {
                    ProtoErr::Other(format!("ERR corrupt schema source for '{import}'"))
                })?;
                next.extend(compile::extract_imports(&src));
                deps.insert(import, compile::DepFile::Source(src));
            } else {
                // Descriptor upload: contribute every file of its
                // self-contained set (covers its own imports too).
                use prost::Message;
                let set = prost_types::FileDescriptorSet::decode(&rec.fds[..]).map_err(|e| {
                    ProtoErr::Other(format!("ERR corrupt schema fds for '{import}': {e}"))
                })?;
                for f in &set.file {
                    let fname = f.name().to_string();
                    deps.entry(fname)
                        .or_insert_with(|| compile::DepFile::Files(vec![f.clone()]));
                }
                deps.entry(import)
                    .or_insert_with(|| compile::DepFile::Files(set.file.clone()));
            }
        }
        frontier = next;
    }
    Ok(deps)
}

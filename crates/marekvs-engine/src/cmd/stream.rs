//! Stream family (design/02 §Streams, design/03 §Streams).
//!
//! Each entry is an LWW record at `stream_entry_key(key, ms, seq)` whose
//! payload is the entry's field/value pairs. Entry ids are `ms-seq`; auto ids
//! embed the origin node in the sequence half so they are cluster-unique
//! without coordination. XDEL/XTRIM write per-entry LWW tombstones. Consumer
//! groups are v1.1 — only the raw entry ops live here.

use std::sync::Arc;

use crate::cmd::{eq_ignore_case, parse_u64};
use crate::reply::Reply;
use crate::store::{
    check_type, ensure_head, get_raw, key_type, new_lww, new_tombstone, now_ms, scan_prefix,
    visible, write_merged, ShardCtx,
};
use crate::Engine;
use marekvs_core::envelope::{head, Envelope, RecordType};
use marekvs_core::ikey::{self, Tag};

const STREAM_META_MAGIC: &[u8; 4] = b"MVS1";

#[derive(Clone, Copy, Debug, Default)]
struct StreamMeta {
    last_id: (u64, u64),
    entries_added: u64,
    max_deleted_id: (u64, u64),
}

fn encode_stream_head(del_hlc: u64, meta: StreamMeta) -> Vec<u8> {
    let mut out = head::encode(head::CTYPE_STREAM, del_hlc);
    out.extend_from_slice(STREAM_META_MAGIC);
    out.extend_from_slice(&meta.last_id.0.to_be_bytes());
    out.extend_from_slice(&meta.last_id.1.to_be_bytes());
    out.extend_from_slice(&meta.entries_added.to_be_bytes());
    out.extend_from_slice(&meta.max_deleted_id.0.to_be_bytes());
    out.extend_from_slice(&meta.max_deleted_id.1.to_be_bytes());
    out
}

fn decode_stream_meta(payload: &[u8]) -> StreamMeta {
    let base = 9;
    if payload.len() < base + 44 || &payload[base..base + 4] != STREAM_META_MAGIC {
        return StreamMeta::default();
    }
    let mut p = base + 4;
    let read = |buf: &[u8], p: &mut usize| -> u64 {
        let v = u64::from_be_bytes(buf[*p..*p + 8].try_into().unwrap());
        *p += 8;
        v
    };
    let last_ms = read(payload, &mut p);
    let last_seq = read(payload, &mut p);
    let entries_added = read(payload, &mut p);
    let del_ms = read(payload, &mut p);
    let del_seq = read(payload, &mut p);
    StreamMeta {
        last_id: (last_ms, last_seq),
        entries_added,
        max_deleted_id: (del_ms, del_seq),
    }
}

fn stream_head(ctx: &ShardCtx, key: &[u8]) -> Option<(Envelope, u64, StreamMeta)> {
    let raw = get_raw(ctx, &ikey::head_key(key))?;
    let (env, pay) = Envelope::decode(&raw)?;
    let (ctype, del) = head::decode(pay)?;
    if ctype != head::CTYPE_STREAM {
        return None;
    }
    Some((env, del, decode_stream_meta(pay)))
}

/// Like `stream_head` but treats a tombstoned/expired stream head as absent.
/// Read/metadata ops (XINFO, XSETID) must not operate on a DEL'd or
/// TTL-expired stream; the mutating ops (XADD/XDEL/XTRIM) keep using
/// `stream_head` because they need the tombstone's delete clock to recreate
/// or merge.
fn live_stream_head(ctx: &ShardCtx, key: &[u8]) -> Option<(Envelope, u64, StreamMeta)> {
    let (env, del, meta) = stream_head(ctx, key)?;
    if env.is_tombstone() || env.is_expired(now_ms()) {
        return None;
    }
    Some((env, del, meta))
}

/// Error reply when a stream read/metadata op finds no live stream: `no such
/// key` when the key is absent or a dead/empty stream, WRONGTYPE when a live
/// key of another type occupies it. (`key_type` already returns None for a
/// tombstoned/expired/empty collection, so a dead stream maps to no-such-key.)
fn no_live_stream_err(ctx: &ShardCtx, key: &[u8]) -> Reply {
    match key_type(ctx, key) {
        None => Reply::err("ERR no such key"),
        Some(_) => Reply::wrongtype(),
    }
}

fn write_stream_head(ctx: &ShardCtx, key: &[u8], ttl: u64, del: u64, meta: StreamMeta) {
    let env = Envelope::head(ctx.hlc.now(), ctx.node_id).with_ttl(ttl);
    write_merged(
        ctx,
        &ikey::head_key(key),
        &env.encode_with(&encode_stream_head(del, meta)),
    );
}

fn max_id(a: (u64, u64), b: (u64, u64)) -> (u64, u64) {
    if a >= b {
        a
    } else {
        b
    }
}

// ---------------------------------------------------------------------------
// field/value payload codec (private varint copies)
// ---------------------------------------------------------------------------

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let b = (v & 0x7F) as u8;
        v >>= 7;
        if v == 0 {
            out.push(b);
            return;
        }
        out.push(b | 0x80);
    }
}

fn get_varint(buf: &[u8]) -> Option<(u64, usize)> {
    let mut v = 0u64;
    let mut shift = 0;
    for (i, &b) in buf.iter().enumerate() {
        v |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 {
            return Some((v, i + 1));
        }
        shift += 7;
        if shift > 63 {
            return None;
        }
    }
    None
}

fn encode_fields(pairs: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
    let mut out = Vec::new();
    put_varint(&mut out, pairs.len() as u64);
    for (f, v) in pairs {
        put_varint(&mut out, f.len() as u64);
        out.extend_from_slice(f);
        put_varint(&mut out, v.len() as u64);
        out.extend_from_slice(v);
    }
    out
}

fn decode_fields(buf: &[u8]) -> Option<Vec<(Vec<u8>, Vec<u8>)>> {
    let (count, mut pos) = get_varint(buf)?;
    let mut pairs = Vec::with_capacity(count.min(1024) as usize);
    for _ in 0..count {
        let (flen, adv) = get_varint(buf.get(pos..)?)?;
        pos += adv;
        let fend = pos.checked_add(flen as usize)?;
        if buf.len() < fend {
            return None;
        }
        let field = buf[pos..fend].to_vec();
        pos = fend;
        let (vlen, adv) = get_varint(buf.get(pos..)?)?;
        pos += adv;
        let vend = pos.checked_add(vlen as usize)?;
        if buf.len() < vend {
            return None;
        }
        pairs.push((field, buf[pos..vend].to_vec()));
        pos = vend;
    }
    Some(pairs)
}

// ---------------------------------------------------------------------------
// id helpers
// ---------------------------------------------------------------------------

fn fmt_id(ms: u64, seq: u64) -> Vec<u8> {
    format!("{ms}-{seq}").into_bytes()
}

/// Origin-embedded sequence half → cluster-unique auto ids (design/02).
fn gen_seq(ctx: &ShardCtx) -> u64 {
    ((ctx.node_id as u64) << 20) | (ctx.hlc.now() & 0xF_FFFF)
}

enum IdSpec {
    Auto,        // "*"
    MsAuto(u64), // "ms" or "ms-*"
    Explicit(u64, u64),
}

fn parse_id_spec(b: &[u8]) -> Option<IdSpec> {
    if b == b"*" {
        return Some(IdSpec::Auto);
    }
    let s = std::str::from_utf8(b).ok()?;
    match s.split_once('-') {
        None => Some(IdSpec::MsAuto(s.parse().ok()?)),
        Some((ms, "*")) => Some(IdSpec::MsAuto(ms.parse().ok()?)),
        Some((ms, seq)) => Some(IdSpec::Explicit(ms.parse().ok()?, seq.parse().ok()?)),
    }
}

/// Parse an XRANGE bound. `-`/`+` are the open ends; a bare `ms` gets seq 0
/// as a start bound or `u64::MAX` as an end bound.
fn parse_range_id(b: &[u8], is_start: bool) -> Option<(u64, u64)> {
    if b == b"-" {
        return Some((0, 0));
    }
    if b == b"+" {
        return Some((u64::MAX, u64::MAX));
    }
    let s = std::str::from_utf8(b).ok()?;
    match s.split_once('-') {
        None => Some((s.parse().ok()?, if is_start { 0 } else { u64::MAX })),
        Some((ms, seq)) => Some((ms.parse().ok()?, seq.parse().ok()?)),
    }
}

// ---------------------------------------------------------------------------
// entry scans
// ---------------------------------------------------------------------------

/// Visible entries in ascending id order: `(ms, seq, payload)`.
fn collect_entries(ctx: &ShardCtx, key: &[u8], del: u64) -> Vec<(u64, u64, Vec<u8>)> {
    let now = now_ms();
    let mut out = Vec::new();
    scan_prefix(
        ctx,
        &ikey::collection_prefix(Tag::StreamEntry, key),
        |k, v| {
            if let (Some(p), Some((env, pay))) = (ikey::parse(k), Envelope::decode(v)) {
                if p.suffix.len() == 16 && visible(&env, pay, del, now).is_some() {
                    let ms = u64::from_be_bytes(p.suffix[..8].try_into().unwrap());
                    let seq = u64::from_be_bytes(p.suffix[8..16].try_into().unwrap());
                    out.push((ms, seq, pay.to_vec()));
                }
            }
            true
        },
    );
    out
}

/// The greatest id ever written (including tombstoned entries), for
/// monotonic-id enforcement and `$`.
fn stream_last_id(ctx: &ShardCtx, key: &[u8]) -> Option<(u64, u64)> {
    let mut last = None;
    scan_prefix(
        ctx,
        &ikey::collection_prefix(Tag::StreamEntry, key),
        |k, _v| {
            if let Some(p) = ikey::parse(k) {
                if p.suffix.len() == 16 {
                    let ms = u64::from_be_bytes(p.suffix[..8].try_into().unwrap());
                    let seq = u64::from_be_bytes(p.suffix[8..16].try_into().unwrap());
                    last = Some((ms, seq));
                }
            }
            true
        },
    );
    last
}

fn effective_last_id(ctx: &ShardCtx, key: &[u8], meta: StreamMeta) -> Option<(u64, u64)> {
    match stream_last_id(ctx, key) {
        Some(scan) => Some(max_id(meta.last_id, scan)),
        None if meta.last_id != (0, 0) => Some(meta.last_id),
        None => None,
    }
}

fn entry_visible(ctx: &ShardCtx, key: &[u8], ms: u64, seq: u64, del: u64) -> bool {
    let Some(v) = get_raw(ctx, &ikey::stream_entry_key(key, ms, seq)) else {
        return false;
    };
    let Some((env, pay)) = Envelope::decode(&v) else {
        return false;
    };
    visible(&env, pay, del, now_ms()).is_some()
}

fn entry_reply(ms: u64, seq: u64, payload: &[u8]) -> Reply {
    let fields = decode_fields(payload).unwrap_or_default();
    let mut fv = Vec::with_capacity(fields.len() * 2);
    for (f, v) in fields {
        fv.push(Reply::Bulk(f));
        fv.push(Reply::Bulk(v));
    }
    Reply::Array(vec![Reply::Bulk(fmt_id(ms, seq)), Reply::Array(fv)])
}

/// Tombstone the oldest visible entries beyond `maxlen`; return trimmed count.
fn trim_maxlen(ctx: &ShardCtx, key: &[u8], del: u64, maxlen: u64) -> i64 {
    let entries = collect_entries(ctx, key, del);
    let maxlen = maxlen as usize;
    if entries.len() <= maxlen {
        return 0;
    }
    let excess = entries.len() - maxlen;
    for (ms, seq, _) in entries.iter().take(excess) {
        let tomb = new_tombstone(ctx, RecordType::StreamEntry);
        write_merged(ctx, &ikey::stream_entry_key(key, *ms, *seq), &tomb);
    }
    excess as i64
}

// ---------------------------------------------------------------------------
// commands
// ---------------------------------------------------------------------------

pub async fn xadd(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 5 {
        return Reply::wrong_args("xadd");
    }
    let key = args[1].clone();
    let mut i = 2;
    let mut nomkstream = false;
    let mut maxlen: Option<u64> = None;
    loop {
        if i >= args.len() {
            return Reply::wrong_args("xadd");
        }
        if eq_ignore_case(&args[i], "NOMKSTREAM") {
            nomkstream = true;
            i += 1;
        } else if eq_ignore_case(&args[i], "MAXLEN") {
            i += 1;
            if let Some(a) = args.get(i) {
                if a == b"~" || a == b"=" {
                    i += 1;
                }
            }
            match args.get(i).and_then(|b| parse_u64(b)) {
                Some(n) => maxlen = Some(n),
                None => return Reply::not_int(),
            }
            i += 1;
        } else {
            break; // this token is the id
        }
    }
    let Some(spec) = parse_id_spec(&args[i]) else {
        return Reply::err("ERR Invalid stream ID specified as stream command argument");
    };
    i += 1;
    let rest = &args[i..];
    if rest.is_empty() || rest.len() % 2 != 0 {
        return Reply::wrong_args("xadd");
    }
    let fields: Vec<(Vec<u8>, Vec<u8>)> = rest
        .chunks(2)
        .map(|c| (c[0].clone(), c[1].clone()))
        .collect();

    engine
        .store
        .run_key(&args[1], move |ctx| {
            if check_type(ctx, &key, head::CTYPE_STREAM).is_err() {
                return Reply::wrongtype();
            }
            let head_state = stream_head(ctx, &key);
            let stream_exists = matches!(head_state, Some((env, _, _))
                if !env.is_tombstone() && !env.is_expired(now_ms()));
            if nomkstream && !stream_exists {
                return Reply::Null;
            }
            let meta = head_state.map(|(_, _, m)| m).unwrap_or_default();
            let last = effective_last_id(ctx, &key, meta);
            let (ms, seq) = match spec {
                IdSpec::Auto => {
                    let mut ms = now_ms();
                    let mut seq = gen_seq(ctx);
                    if let Some((lms, ls)) = last {
                        if ms < lms {
                            ms = lms;
                        }
                        if ms == lms && seq <= ls {
                            seq = ls + 1;
                        }
                    }
                    (ms, seq)
                }
                IdSpec::MsAuto(ms) => {
                    let seq = match last {
                        Some((lms, _)) if lms > ms => {
                            return Reply::err(
                                "ERR The ID specified in XADD is equal or smaller than the target stream top item",
                            );
                        }
                        Some((lms, ls)) if lms == ms => ls + 1,
                        _ => 0,
                    };
                    (ms, seq)
                }
                IdSpec::Explicit(ms, seq) => {
                    if ms == 0 && seq == 0 {
                        return Reply::err(
                            "ERR The ID specified in XADD must be greater than 0-0",
                        );
                    }
                    if let Some(l) = last {
                        if (ms, seq) <= l {
                            return Reply::err(
                                "ERR The ID specified in XADD is equal or smaller than the target stream top item",
                            );
                        }
                    }
                    (ms, seq)
                }
            };

            ensure_head(ctx, &key, head::CTYPE_STREAM);
            let (head_env, del, mut meta) = stream_head(ctx, &key).unwrap_or_else(|| {
                (
                    Envelope::head(ctx.hlc.now(), ctx.node_id),
                    head_del(ctx, &key),
                    StreamMeta::default(),
                )
            });
            let rec = new_lww(ctx, RecordType::StreamEntry, &encode_fields(&fields), 0);
            write_merged(ctx, &ikey::stream_entry_key(&key, ms, seq), &rec);
            meta.last_id = max_id(meta.last_id, (ms, seq));
            meta.entries_added = meta.entries_added.saturating_add(1);
            if let Some(n) = maxlen {
                trim_maxlen(ctx, &key, del, n);
            }
            write_stream_head(ctx, &key, head_env.ttl_deadline_ms, del, meta);
            Reply::Bulk(fmt_id(ms, seq))
        })
        .await
}

/// The collection delete clock for a stream key (0 when never deleted).
fn head_del(ctx: &ShardCtx, key: &[u8]) -> u64 {
    check_type(ctx, key, head::CTYPE_STREAM).unwrap_or(0)
}

pub async fn xlen(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 2 {
        return Reply::wrong_args("xlen");
    }
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = check_type(ctx, &key, head::CTYPE_STREAM) else {
                return Reply::wrongtype();
            };
            Reply::Int(collect_entries(ctx, &key, del).len() as i64)
        })
        .await
}

pub async fn xrange(engine: &Arc<Engine>, args: &[Vec<u8>], rev: bool) -> Reply {
    if args.len() < 4 {
        return Reply::wrong_args("xrange");
    }
    let key = args[1].clone();
    // XREVRANGE takes the high end first, then the low end.
    let (start_arg, end_arg) = if rev {
        (args[3].clone(), args[2].clone())
    } else {
        (args[2].clone(), args[3].clone())
    };
    let (Some(lo), Some(hi)) = (
        parse_range_id(&start_arg, true),
        parse_range_id(&end_arg, false),
    ) else {
        return Reply::err("ERR Invalid stream ID specified as stream command argument");
    };
    let mut count: Option<usize> = None;
    if args.len() > 4 {
        if args.len() != 6 || !eq_ignore_case(&args[4], "COUNT") {
            return Reply::syntax();
        }
        match parse_u64(&args[5]) {
            Some(n) => count = Some(n as usize),
            None => return Reply::not_int(),
        }
    }
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = check_type(ctx, &key, head::CTYPE_STREAM) else {
                return Reply::wrongtype();
            };
            let mut entries: Vec<(u64, u64, Vec<u8>)> = collect_entries(ctx, &key, del)
                .into_iter()
                .filter(|(ms, seq, _)| (*ms, *seq) >= lo && (*ms, *seq) <= hi)
                .collect();
            if rev {
                entries.reverse();
            }
            if let Some(n) = count {
                entries.truncate(n);
            }
            Reply::Array(
                entries
                    .into_iter()
                    .map(|(ms, seq, pay)| entry_reply(ms, seq, &pay))
                    .collect(),
            )
        })
        .await
}

pub async fn xread(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    // XREAD [COUNT n] [BLOCK ms] STREAMS key [key ...] id [id ...]
    let mut i = 1;
    let mut count: Option<usize> = None;
    let mut streams_at: Option<usize> = None;
    while i < args.len() {
        if eq_ignore_case(&args[i], "COUNT") {
            match args.get(i + 1).and_then(|b| parse_u64(b)) {
                Some(n) => count = Some(n as usize),
                None => return Reply::not_int(),
            }
            i += 2;
        } else if eq_ignore_case(&args[i], "BLOCK") {
            // Non-blocking engine: accept and ignore the timeout.
            if args.get(i + 1).and_then(|b| parse_u64(b)).is_none() {
                return Reply::not_int();
            }
            i += 2;
        } else if eq_ignore_case(&args[i], "STREAMS") {
            streams_at = Some(i + 1);
            break;
        } else {
            return Reply::syntax();
        }
    }
    let Some(streams_at) = streams_at else {
        return Reply::syntax();
    };
    let tail = &args[streams_at..];
    if tail.is_empty() || tail.len() % 2 != 0 {
        return Reply::err(
            "ERR Unbalanced XREAD list of streams: for each stream key an ID or '$' must be specified.",
        );
    }
    let n = tail.len() / 2;
    let keys = tail[..n].to_vec();
    let ids = tail[n..].to_vec();

    let mut out = Vec::new();
    for (key, id_arg) in keys.into_iter().zip(ids) {
        engine.ensure_local(&key).await;
        let k = key.clone();
        let entries = engine
            .store
            .run_key(&key, move |ctx| {
                let Ok(del) = check_type(ctx, &k, head::CTYPE_STREAM) else {
                    return Err(());
                };
                // `$` = only entries newer than the current last id.
                let from = if id_arg == b"$" {
                    stream_last_id(ctx, &k).unwrap_or((0, 0))
                } else {
                    match parse_range_id(&id_arg, true) {
                        Some(f) => f,
                        None => return Ok(Vec::new()),
                    }
                };
                let mut es: Vec<Reply> = collect_entries(ctx, &k, del)
                    .into_iter()
                    .filter(|(ms, seq, _)| (*ms, *seq) > from)
                    .map(|(ms, seq, pay)| entry_reply(ms, seq, &pay))
                    .collect();
                if let Some(c) = count {
                    es.truncate(c);
                }
                Ok(es)
            })
            .await;
        match entries {
            Err(()) => return Reply::wrongtype(),
            Ok(es) if !es.is_empty() => {
                out.push(Reply::Array(vec![Reply::Bulk(key), Reply::Array(es)]));
            }
            Ok(_) => {}
        }
    }
    if out.is_empty() {
        Reply::NullArray
    } else {
        Reply::Array(out)
    }
}

pub async fn xdel(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 3 {
        return Reply::wrong_args("xdel");
    }
    let key = args[1].clone();
    let id_args: Vec<Vec<u8>> = args[2..].to_vec();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = check_type(ctx, &key, head::CTYPE_STREAM) else {
                return Reply::wrongtype();
            };
            let mut n = 0;
            let mut max_deleted = (0, 0);
            for id in &id_args {
                let Some((ms, seq)) = parse_range_id(id, true) else {
                    return Reply::err(
                        "ERR Invalid stream ID specified as stream command argument",
                    );
                };
                if entry_visible(ctx, &key, ms, seq, del) {
                    let tomb = new_tombstone(ctx, RecordType::StreamEntry);
                    write_merged(ctx, &ikey::stream_entry_key(&key, ms, seq), &tomb);
                    max_deleted = max_id(max_deleted, (ms, seq));
                    n += 1;
                }
            }
            if n > 0 {
                if let Some((head_env, del, mut meta)) = stream_head(ctx, &key) {
                    meta.max_deleted_id = max_id(meta.max_deleted_id, max_deleted);
                    write_stream_head(ctx, &key, head_env.ttl_deadline_ms, del, meta);
                }
            }
            Reply::Int(n)
        })
        .await
}

pub async fn xtrim(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 4 {
        return Reply::wrong_args("xtrim");
    }
    let key = args[1].clone();
    let mut i = 2;
    if !eq_ignore_case(&args[i], "MAXLEN") {
        return Reply::syntax();
    }
    i += 1;
    if let Some(a) = args.get(i) {
        if a == b"~" || a == b"=" {
            i += 1;
        }
    }
    let Some(maxlen) = args.get(i).and_then(|b| parse_u64(b)) else {
        return Reply::not_int();
    };
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = check_type(ctx, &key, head::CTYPE_STREAM) else {
                return Reply::wrongtype();
            };
            let before = collect_entries(ctx, &key, del);
            let n = trim_maxlen(ctx, &key, del, maxlen);
            if n > 0 {
                if let Some((head_env, del, mut meta)) = stream_head(ctx, &key) {
                    let max_deleted = before
                        .iter()
                        .take(n as usize)
                        .fold((0, 0), |acc, (ms, seq, _)| max_id(acc, (*ms, *seq)));
                    meta.max_deleted_id = max_id(meta.max_deleted_id, max_deleted);
                    write_stream_head(ctx, &key, head_env.ttl_deadline_ms, del, meta);
                }
            }
            Reply::Int(n)
        })
        .await
}

pub async fn xsetid(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 3 {
        return Reply::wrong_args("xsetid");
    }
    let key = args[1].clone();
    let Some(id) = parse_range_id(&args[2], true) else {
        return Reply::err("ERR Invalid stream ID specified as stream command argument");
    };
    let mut entries_added: Option<u64> = None;
    let mut max_deleted_id: Option<(u64, u64)> = None;
    let mut i = 3;
    while i < args.len() {
        if eq_ignore_case(&args[i], "ENTRIESADDED") {
            let Some(n) = args.get(i + 1).and_then(|b| parse_u64(b)) else {
                return Reply::not_int();
            };
            entries_added = Some(n);
            i += 2;
        } else if eq_ignore_case(&args[i], "MAXDELETEDID") {
            let Some(mid) = args.get(i + 1).and_then(|b| parse_range_id(b, true)) else {
                return Reply::err("ERR Invalid stream ID specified as stream command argument");
            };
            max_deleted_id = Some(mid);
            i += 2;
        } else {
            return Reply::syntax();
        }
    }
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Some((head_env, del, mut meta)) = live_stream_head(ctx, &key) else {
                return no_live_stream_err(ctx, &key);
            };
            if let Some(last) = stream_last_id(ctx, &key) {
                if id < last {
                    return Reply::err(
                        "ERR The ID specified in XSETID is smaller than the target stream top item",
                    );
                }
            }
            meta.last_id = id;
            if let Some(n) = entries_added {
                meta.entries_added = n;
            }
            if let Some(mid) = max_deleted_id {
                meta.max_deleted_id = mid;
            }
            write_stream_head(ctx, &key, head_env.ttl_deadline_ms, del, meta);
            Reply::ok()
        })
        .await
}

pub async fn xinfo(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 2 {
        return Reply::wrong_args("xinfo");
    }
    if !eq_ignore_case(&args[1], "STREAM") {
        if eq_ignore_case(&args[1], "HELP") {
            return Reply::Array(vec![
                Reply::bulk_str("XINFO STREAM <key>"),
                Reply::bulk_str("XINFO HELP"),
            ]);
        }
        return Reply::err("ERR unknown subcommand");
    }
    if args.len() != 3 {
        return Reply::wrong_args("xinfo stream");
    }
    let key = args[2].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[2], move |ctx| {
            let Some((_head_env, del, meta)) = live_stream_head(ctx, &key) else {
                return no_live_stream_err(ctx, &key);
            };
            let entries = collect_entries(ctx, &key, del);
            let last_id = effective_last_id(ctx, &key, meta).unwrap_or((0, 0));
            let first = entries
                .first()
                .map_or(Reply::Null, |(ms, seq, pay)| entry_reply(*ms, *seq, pay));
            let last = entries
                .last()
                .map_or(Reply::Null, |(ms, seq, pay)| entry_reply(*ms, *seq, pay));
            Reply::Array(vec![
                Reply::bulk_str("length"),
                Reply::Int(entries.len() as i64),
                Reply::bulk_str("radix-tree-keys"),
                Reply::Int(1),
                Reply::bulk_str("radix-tree-nodes"),
                Reply::Int(2),
                Reply::bulk_str("last-generated-id"),
                Reply::Bulk(fmt_id(last_id.0, last_id.1)),
                Reply::bulk_str("max-deleted-entry-id"),
                Reply::Bulk(fmt_id(meta.max_deleted_id.0, meta.max_deleted_id.1)),
                Reply::bulk_str("entries-added"),
                Reply::Int(meta.entries_added as i64),
                Reply::bulk_str("groups"),
                Reply::Int(0),
                Reply::bulk_str("first-entry"),
                first,
                Reply::bulk_str("last-entry"),
                last,
            ])
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fields_roundtrip() {
        let pairs = vec![
            (b"field1".to_vec(), b"value1".to_vec()),
            (b"".to_vec(), b"empty-field".to_vec()),
        ];
        assert_eq!(decode_fields(&encode_fields(&pairs)), Some(pairs));
    }

    #[test]
    fn fields_reject_truncated() {
        let mut enc = encode_fields(&[(b"f".to_vec(), b"vvvv".to_vec())]);
        enc.pop();
        assert_eq!(decode_fields(&enc), None);
    }

    #[test]
    fn id_spec_parsing() {
        assert!(matches!(parse_id_spec(b"*"), Some(IdSpec::Auto)));
        assert!(matches!(parse_id_spec(b"5"), Some(IdSpec::MsAuto(5))));
        assert!(matches!(parse_id_spec(b"5-*"), Some(IdSpec::MsAuto(5))));
        assert!(matches!(
            parse_id_spec(b"5-3"),
            Some(IdSpec::Explicit(5, 3))
        ));
        assert!(parse_id_spec(b"5-x").is_none());
    }

    #[test]
    fn range_id_defaults() {
        assert_eq!(parse_range_id(b"-", true), Some((0, 0)));
        assert_eq!(parse_range_id(b"+", false), Some((u64::MAX, u64::MAX)));
        assert_eq!(parse_range_id(b"5", true), Some((5, 0)));
        assert_eq!(parse_range_id(b"5", false), Some((5, u64::MAX)));
        assert_eq!(parse_range_id(b"5-2", true), Some((5, 2)));
        assert!(parse_range_id(b"nope", true).is_none());
    }
}

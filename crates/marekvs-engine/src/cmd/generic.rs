//! Generic/keyspace family: DEL, EXISTS, TYPE, expiry, SCAN, KEYS, RENAME, COPY, OBJECT.

use std::collections::HashSet;
use std::sync::Arc;

use crate::cmd::{eq_ignore_case, parse_i64, parse_u64};
use crate::pubsub::glob_match;
use crate::reply::Reply;
use crate::store::{
    self, get_head, get_raw, key_type, now_ms, read_lww, scan_prefix, write_merged, ShardCtx,
};
use crate::Engine;
use marekvs_core::envelope::{head, Envelope, RecordType};
use marekvs_core::ikey;

/// Delete one key whatever its type. Returns true if something was deleted.
pub fn del_key(ctx: &ShardCtx, key: &[u8]) -> bool {
    del_key_hlc(ctx, key).is_some()
}

/// Delete one key whatever its type, returning the delete clock it stamped
/// (`None` if the key was absent). Callers that immediately re-create the
/// key on the same shard (COPY/RENAME clobber) need this clock: the fresh
/// head they write must carry `del_hlc >= clobber clock` so the destination's
/// stale element records (physically still on disk) stay shadowed. Ignoring
/// it — writing the source's `del_hlc` instead — resurrects the destination's
/// old members.
pub fn del_key_hlc(ctx: &ShardCtx, key: &[u8]) -> Option<u64> {
    match key_type(ctx, key) {
        None => None,
        Some(b's') => {
            let hlc = ctx.hlc.now();
            let tomb = Envelope::tombstone(RecordType::String, hlc, ctx.node_id).encode_with(&[]);
            write_merged(ctx, &ikey::string_key(key), &tomb);
            Some(hlc)
        }
        // Lists are head-gated collections (ctype 5); they take the head-
        // tombstone path below, same as hash/set/zset.
        Some(ctype) => {
            // Head tombstone whose payload carries del_hlc = delete clock;
            // every element older than it is dead (design/02).
            let hlc = ctx.hlc.now();
            let env = Envelope {
                flags: marekvs_core::envelope::COLLECTION_HEAD | marekvs_core::envelope::TOMBSTONE,
                hlc,
                origin: ctx.node_id,
                ttl_deadline_ms: 0,
            };
            let val = env.encode_with(&head::encode(ctype, hlc));
            write_merged(ctx, &ikey::head_key(key), &val);
            Some(hlc)
        }
    }
}

pub async fn del(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 2 {
        return Reply::wrong_args("del");
    }
    let mut n = 0;
    for keyarg in &args[1..] {
        // Deleting needs the key observable locally: a cluster-remote key
        // would otherwise be a silent no-op (no tombstone written).
        engine.ensure_local(keyarg).await;
        let key = keyarg.clone();
        if engine
            .store
            .run_key(keyarg, move |ctx| del_key(ctx, &key))
            .await
        {
            n += 1;
        }
    }
    Reply::Int(n)
}

pub async fn exists(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 2 {
        return Reply::wrong_args("exists");
    }
    let mut n = 0;
    for keyarg in &args[1..] {
        engine.ensure_local(keyarg).await;
        let key = keyarg.clone();
        if engine
            .store
            .run_key(keyarg, move |ctx| key_type(ctx, &key).is_some())
            .await
        {
            n += 1;
        }
    }
    Reply::Int(n)
}

pub async fn type_cmd(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 2 {
        return Reply::wrong_args("type");
    }
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    let t = engine
        .store
        .run_key(&args[1], move |ctx| key_type(ctx, &key))
        .await;
    Reply::Simple(match t {
        None => "none",
        Some(b's') => "string",
        Some(head::CTYPE_LIST) => "list",
        Some(head::CTYPE_HASH) => "hash",
        Some(head::CTYPE_SET) => "set",
        Some(head::CTYPE_ZSET) => "zset",
        Some(head::CTYPE_STREAM) => "stream",
        Some(head::CTYPE_HLL) => "string", // Redis compat: PF keys report as strings
        Some(head::CTYPE_BUDGET) => "budget",
        Some(_) => "none",
    })
}

/// The envelope currently carrying this key's TTL (string, list, or head).
fn ttl_envelope(ctx: &ShardCtx, key: &[u8]) -> Option<Envelope> {
    // Lists carry their TTL on the collection head now (ctype 5 → `_` arm).
    match key_type(ctx, key)? {
        b's' => read_lww(ctx, &ikey::string_key(key), 0).map(|(e, _)| e),
        _ => get_head(ctx, key).map(|(e, _, _)| e),
    }
}

/// The element key holding `member` of `key`, per the collection's head
/// ctype. None = key absent or not a member-bearing collection.
fn member_element_key(ctx: &ShardCtx, key: &[u8], member: &[u8]) -> Option<Vec<u8>> {
    let (env, ctype, _) = get_head(ctx, key)?;
    if env.is_tombstone() || env.is_expired(now_ms()) {
        return None;
    }
    match ctype {
        head::CTYPE_HASH => Some(ikey::hash_field_key(key, member)),
        head::CTYPE_SET => Some(ikey::set_member_key(key, member)),
        head::CTYPE_ZSET => Some(ikey::zset_member_key(key, member)),
        _ => None, // lists/streams address elements by position/id, not name
    }
}

/// KeyDB-style per-member expiry (EXPIREMEMBER / EXPIREMEMBERAT /
/// PEXPIREMEMBERAT): re-stamp the element record's envelope with a fresh
/// version and the new deadline. The payload (dot sets, values) is untouched;
/// element merges propagate the TTL from the higher envelope version, the
/// read path already filters expired elements, and the expiry sweeper turns
/// them into observed-remove tombstones — so the whole lifecycle was already
/// built; this is just the setter.
fn set_member_deadline(ctx: &ShardCtx, key: &[u8], member: &[u8], deadline_ms: u64) -> bool {
    let Some(elem_key) = member_element_key(ctx, key, member) else {
        return false;
    };
    let del = get_head_del(ctx, key);
    let Some(raw) = get_raw(ctx, &elem_key) else {
        return false;
    };
    let Some((env, payload)) = Envelope::decode(&raw) else {
        return false;
    };
    if store::visible(&env, payload, del, now_ms()).is_none()
        || marekvs_core::merge::element_value(payload).is_none()
    {
        return false; // dead/absent member: KeyDB returns 0
    }
    if deadline_ms != 0 && deadline_ms <= now_ms() {
        // Past deadline = expire right now: observed-remove, like the sweeper.
        let dots = marekvs_core::merge::element_dots(payload);
        let rm =
            marekvs_core::merge::element_remove(env.rtype(), ctx.hlc.now(), ctx.node_id, &dots);
        write_merged(ctx, &elem_key, &rm);
        return true;
    }
    let restamped = Envelope {
        flags: env.flags,
        hlc: ctx.hlc.now(),
        origin: ctx.node_id,
        ttl_deadline_ms: deadline_ms,
    }
    .encode_with(payload);
    write_merged(ctx, &elem_key, &restamped);
    true
}

/// EXPIREMEMBER key member n [s|ms] / EXPIREMEMBERAT key member unix-s /
/// PEXPIREMEMBERAT key member unix-ms. `mult` converts n to ms; `absolute`
/// skips the now() offset. Reply: 1 applied, 0 key-or-member missing.
pub async fn expiremember(
    engine: &Arc<Engine>,
    args: &[Vec<u8>],
    mut mult: u64,
    absolute: bool,
) -> Reply {
    if args.len() < 4 || (absolute && args.len() != 4) || args.len() > 5 {
        return Reply::wrong_args("expiremember");
    }
    let Some(n) = parse_i64(&args[3]) else {
        return Reply::not_int();
    };
    if let Some(unit) = args.get(4) {
        if eq_ignore_case(unit, "s") {
            mult = 1000;
        } else if eq_ignore_case(unit, "ms") {
            mult = 1;
        } else {
            return Reply::syntax();
        }
    }
    let deadline = if absolute {
        (n.max(0) as u64) * mult
    } else if n <= 0 {
        1 // instant expiry
    } else {
        now_ms() + n as u64 * mult
    };
    let (key, member) = (args[1].clone(), args[2].clone());
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            Reply::Int(set_member_deadline(ctx, &key, &member, deadline) as i64)
        })
        .await
}

/// TTL/PTTL with the KeyDB member extension: `TTL key member`.
pub async fn member_ttl(engine: &Arc<Engine>, args: &[Vec<u8>], millis: bool) -> Reply {
    let (key, member) = (args[1].clone(), args[2].clone());
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Some(elem_key) = member_element_key(ctx, &key, &member) else {
                return Reply::Int(-2);
            };
            let del = get_head_del(ctx, &key);
            let now = now_ms();
            match get_raw(ctx, &elem_key).and_then(|raw| {
                Envelope::decode(&raw).and_then(|(env, pay)| {
                    store::visible(&env, pay, del, now)?;
                    marekvs_core::merge::element_value(pay)?;
                    Some(env)
                })
            }) {
                None => Reply::Int(-2),
                Some(env) if env.ttl_deadline_ms == 0 => Reply::Int(-1),
                Some(env) => {
                    let remain = env.ttl_deadline_ms.saturating_sub(now);
                    Reply::Int(if millis {
                        remain as i64
                    } else {
                        (remain / 1000) as i64
                    })
                }
            }
        })
        .await
}

pub async fn ttl(engine: &Arc<Engine>, args: &[Vec<u8>], millis: bool) -> Reply {
    if args.len() != 2 {
        return Reply::wrong_args("ttl");
    }
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| match ttl_envelope(ctx, &key) {
            None => Reply::Int(-2),
            Some(env) if env.ttl_deadline_ms == 0 => Reply::Int(-1),
            Some(env) => {
                let remain = env.ttl_deadline_ms.saturating_sub(now_ms());
                Reply::Int(if millis {
                    remain as i64
                } else {
                    (remain / 1000) as i64
                })
            }
        })
        .await
}

pub async fn expiretime(engine: &Arc<Engine>, args: &[Vec<u8>], millis: bool) -> Reply {
    if args.len() != 2 {
        return Reply::wrong_args("expiretime");
    }
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| match ttl_envelope(ctx, &key) {
            None => Reply::Int(-2),
            Some(env) if env.ttl_deadline_ms == 0 => Reply::Int(-1),
            Some(env) => Reply::Int(if millis {
                env.ttl_deadline_ms as i64
            } else {
                (env.ttl_deadline_ms / 1000) as i64
            }),
        })
        .await
}

/// Rewrite the TTL-carrying record with a new deadline (fresh HLC → LWW).
fn set_deadline(ctx: &ShardCtx, key: &[u8], deadline: u64) -> bool {
    match key_type(ctx, key) {
        None => false,
        Some(b's') => {
            if let Some((env, payload)) = read_lww(ctx, &ikey::string_key(key), 0) {
                // Counters keep their record type: re-encode the same state
                // with the new deadline and a fresh envelope version, so the
                // TTL change wins LWW without freezing the counter.
                let rec = if env.rtype() == RecordType::Counter {
                    Envelope::new(RecordType::Counter, ctx.hlc.now(), ctx.node_id)
                        .with_ttl(deadline)
                        .encode_with(&payload)
                } else {
                    store::new_lww(ctx, RecordType::String, &payload, deadline)
                };
                write_merged(ctx, &ikey::string_key(key), &rec);
                return true;
            }
            false
        }
        // Lists (ctype 5) rewrite the head TTL via the head branch below.
        Some(ctype) => {
            if let Some((_, t, del)) = get_head(ctx, key) {
                if t == ctype {
                    let env = Envelope::head(ctx.hlc.now(), ctx.node_id).with_ttl(deadline);
                    let val = env.encode_with(&head::encode(ctype, del));
                    write_merged(ctx, &ikey::head_key(key), &val);
                    return true;
                }
            }
            false
        }
    }
}

pub async fn expire(engine: &Arc<Engine>, args: &[Vec<u8>], mult: u64, absolute: bool) -> Reply {
    if args.len() < 3 {
        return Reply::wrong_args("expire");
    }
    let Some(n) = parse_i64(&args[2]) else {
        return Reply::not_int();
    };
    let key = args[1].clone();
    let deadline = if absolute {
        (n.max(0) as u64) * mult
    } else if n <= 0 {
        1 // already in the past: delete via instant expiry
    } else {
        now_ms() + n as u64 * mult
    };
    // Re-stamping the TTL needs the current record; fetch cluster-remote
    // keys or EXPIRE would report 0 and change nothing.
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            // Budget keys must not expire: escrow ledgers and open tokens
            // are foldable only by their issuers (design/13). DEL works.
            if key_type(ctx, &key) == Some(head::CTYPE_BUDGET) {
                return Reply::err("ERR EXPIRE is not supported for budget keys (use DEL)");
            }
            if deadline <= now_ms() {
                return Reply::Int(if del_key(ctx, &key) { 1 } else { 0 });
            }
            Reply::Int(if set_deadline(ctx, &key, deadline) {
                1
            } else {
                0
            })
        })
        .await
}

pub async fn persist(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 2 {
        return Reply::wrong_args("persist");
    }
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| match ttl_envelope(ctx, &key) {
            Some(env) if env.ttl_deadline_ms != 0 => {
                Reply::Int(if set_deadline(ctx, &key, 0) { 1 } else { 0 })
            }
            _ => Reply::Int(0),
        })
        .await
}

/// Collect distinct user keys with a visible record, walking the data CF.
/// `start_after`: resume point (internal key); returns (keys, next_cursor).
fn scan_userkeys(
    ctx: &ShardCtx,
    start_after: &[u8],
    limit: usize,
    pattern: Option<&[u8]>,
) -> (Vec<Vec<u8>>, Vec<u8>) {
    let mut seen: HashSet<Vec<u8>> = HashSet::new();
    let mut out = Vec::new();
    let mut next = Vec::new();
    let now = now_ms();
    let mut count = 0usize;
    scan_prefix(ctx, &[], |k, v| {
        count += 1;
        if count > limit * 64 {
            // hard budget per SCAN call
            next = k.to_vec();
            return false;
        }
        if !start_after.is_empty() && k <= start_after {
            return true;
        }
        if let Some(p) = ikey::parse(k) {
            if p.tag == b'Z' {
                return true; // derived index keys are not user data
            }
            if p.userkey.first() == Some(&0) {
                return true; // hidden system keys (e.g. replicated scripts)
            }
            if seen.contains(p.userkey) {
                return true;
            }
            if let Some((env, pay)) = Envelope::decode(v) {
                let vis = if p.tag == b'M' {
                    !env.is_tombstone() && !env.is_expired(now)
                } else {
                    store::visible(&env, pay, 0, now).is_some()
                        && (!env.rtype().is_or_element()
                            || marekvs_core::merge::element_value(pay).is_some())
                };
                if vis && pattern.is_none_or(|pat| glob_match(pat, p.userkey)) {
                    seen.insert(p.userkey.to_vec());
                    out.push(p.userkey.to_vec());
                    if out.len() >= limit {
                        next = k.to_vec();
                        return false;
                    }
                }
            }
        }
        true
    });
    (out, next)
}

pub async fn keys(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 2 {
        return Reply::wrong_args("keys");
    }
    let pattern = args[1].clone();
    engine
        .store
        .run(0, move |ctx| {
            let (keys, _) = scan_userkeys(ctx, &[], usize::MAX / 128, Some(&pattern));
            Reply::Array(keys.into_iter().map(Reply::Bulk).collect())
        })
        .await
}

pub async fn scan(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 2 {
        return Reply::wrong_args("scan");
    }
    let cursor_arg = args[1].clone();
    let mut pattern: Option<Vec<u8>> = None;
    let mut count = 10usize;
    let mut i = 2;
    while i < args.len() {
        if eq_ignore_case(&args[i], "MATCH") {
            pattern = args.get(i + 1).cloned();
            i += 2;
        } else if eq_ignore_case(&args[i], "COUNT") {
            count = args.get(i + 1).and_then(|b| parse_u64(b)).unwrap_or(10) as usize;
            i += 2;
        } else if eq_ignore_case(&args[i], "TYPE") {
            i += 2; // accepted, ignored in v1
        } else {
            return Reply::syntax();
        }
    }
    // Cursor: "0" = start; otherwise hex of the last internal key returned.
    let start_after = if cursor_arg == b"0" {
        Vec::new()
    } else {
        match hex_decode(&cursor_arg) {
            Some(v) => v,
            None => return Reply::err("ERR invalid cursor"),
        }
    };
    engine
        .store
        .run(0, move |ctx| {
            let (keys, next) = scan_userkeys(ctx, &start_after, count.max(1), pattern.as_deref());
            let cursor = if next.is_empty() {
                "0".to_string()
            } else {
                hex_encode(&next)
            };
            Reply::Array(vec![
                Reply::Bulk(cursor.into_bytes()),
                Reply::Array(keys.into_iter().map(Reply::Bulk).collect()),
            ])
        })
        .await
}

pub async fn randomkey(engine: &Arc<Engine>) -> Reply {
    engine
        .store
        .run(0, move |ctx| {
            let (keys, _) = scan_userkeys(ctx, &[], 1, None);
            keys.into_iter().next().map_or(Reply::Null, Reply::Bulk)
        })
        .await
}

/// Budget keys cannot be moved or cloned: escrow slots and token ids embed
/// the budget generation and issuer identity — a copied ledger would be an
/// unfoldable orphan that double-counts escrow (design/13). Returns the
/// error reply when `src` or `dst` is a live budget.
async fn budget_move_fence(
    engine: &Arc<Engine>,
    what: &'static str,
    src: &[u8],
    dst: &[u8],
) -> Option<Reply> {
    for key in [src, dst] {
        let k = key.to_vec();
        let is_budget = engine
            .store
            .run_key(key, move |ctx| {
                key_type(ctx, &k) == Some(head::CTYPE_BUDGET)
            })
            .await;
        if is_budget {
            return Some(Reply::err(format!(
                "ERR {what} is not supported for budget keys"
            )));
        }
    }
    None
}

pub async fn rename(engine: &Arc<Engine>, args: &[Vec<u8>], nx: bool) -> Reply {
    if args.len() != 3 {
        return Reply::wrong_args("rename");
    }
    let (src, dst) = (args[1].clone(), args[2].clone());
    // Source records are read locally; the NX guard checks dst existence.
    // Both need cluster-remote keys fetched first.
    engine.ensure_local(&src).await;
    if let Some(fence) = budget_move_fence(engine, "RENAME", &src, &dst).await {
        return fence;
    }
    if nx {
        engine.ensure_local(&dst).await;
        let d = dst.clone();
        let exists = engine
            .store
            .run_key(&dst, move |ctx| key_type(ctx, &d).is_some())
            .await;
        if exists {
            return Reply::Int(0);
        }
    }
    // Read source records on its shard, then write to dst shard, then delete.
    let s = src.clone();
    let records: Option<Vec<KeyRecord>> = engine
        .store
        .run_key(&src, move |ctx| collect_key_records(ctx, &s))
        .await;
    let Some(records) = records else {
        return Reply::err("ERR no such key");
    };
    let d = dst.clone();
    engine
        .store
        .run_key(&dst, move |ctx| {
            let clobber = del_key_hlc(ctx, &d).unwrap_or(0); // clobber whatever was there
            for (tag, suffix, value) in records {
                let ik = rebuild_ikey(tag, &d, &suffix);
                if let Some(value) = restamp_record(ctx, &value, clobber) {
                    write_merged(ctx, &ik, &value);
                }
            }
        })
        .await;
    let s2 = src.clone();
    engine
        .store
        .run_key(&src, move |ctx| del_key(ctx, &s2))
        .await;
    if nx {
        Reply::Int(1)
    } else {
        Reply::ok()
    }
}

pub async fn copy(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 3 {
        return Reply::wrong_args("copy");
    }
    let (src, dst) = (args[1].clone(), args[2].clone());
    let mut replace = false;
    let mut i = 3;
    while i < args.len() {
        if eq_ignore_case(&args[i], "REPLACE") {
            replace = true;
            i += 1;
        } else if eq_ignore_case(&args[i], "DB") {
            let Some(db) = args.get(i + 1).and_then(|b| parse_u64(b)) else {
                return Reply::not_int();
            };
            if db != 0 {
                return Reply::err("ERR DB index is out of range");
            }
            i += 2;
        } else {
            return Reply::syntax();
        }
    }
    engine.ensure_local(&src).await;
    if let Some(fence) = budget_move_fence(engine, "COPY", &src, &dst).await {
        return fence;
    }

    if !replace {
        engine.ensure_local(&dst).await;
        let d = dst.clone();
        let exists = engine
            .store
            .run_key(&dst, move |ctx| key_type(ctx, &d).is_some())
            .await;
        if exists {
            return Reply::Int(0);
        }
    }

    let s = src.clone();
    let records: Option<Vec<KeyRecord>> = engine
        .store
        .run_key(&src, move |ctx| collect_key_records(ctx, &s))
        .await;
    let Some(records) = records else {
        return Reply::Int(0);
    };

    let d = dst.clone();
    engine
        .store
        .run_key(&dst, move |ctx| {
            let clobber = if replace {
                del_key_hlc(ctx, &d).unwrap_or(0)
            } else {
                0
            };
            for (tag, suffix, value) in records {
                let ik = rebuild_ikey(tag, &d, &suffix);
                if let Some(value) = restamp_record(ctx, &value, clobber) {
                    write_merged(ctx, &ik, &value);
                }
            }
        })
        .await;
    Reply::Int(1)
}

pub async fn object(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 2 {
        return Reply::wrong_args("object");
    }
    let sub = String::from_utf8_lossy(&args[1]).to_ascii_uppercase();
    if sub == "HELP" {
        return Reply::Array(vec![
            Reply::bulk_str("OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"),
            Reply::bulk_str("ENCODING <key>"),
            Reply::bulk_str("REFCOUNT <key>"),
            Reply::bulk_str("IDLETIME <key>"),
            Reply::bulk_str("FREQ <key>"),
            Reply::bulk_str("HELP"),
        ]);
    }
    if args.len() != 3 {
        return Reply::wrong_args("object");
    }
    if sub == "FREQ" {
        return Reply::err(
            "ERR An LFU maxmemory policy is not selected, access frequency not tracked.",
        );
    }
    let key = args[2].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[2], move |ctx| {
            let Some(t) = key_type(ctx, &key) else {
                return Reply::err("ERR no such key");
            };
            match sub.as_str() {
                "REFCOUNT" => Reply::Int(1),
                "IDLETIME" => Reply::Int(0),
                "ENCODING" => Reply::bulk_str(match t {
                    b's' => "raw",
                    head::CTYPE_HASH => "hashtable",
                    head::CTYPE_SET => "hashtable",
                    head::CTYPE_ZSET => "skiplist",
                    head::CTYPE_LIST => "quicklist",
                    head::CTYPE_STREAM => "stream",
                    head::CTYPE_HLL => "raw",
                    _ => "raw",
                }),
                _ => Reply::err("ERR unknown subcommand"),
            }
        })
        .await
}

/// One portable record: (tag, element suffix, full stored value).
type KeyRecord = (u8, Vec<u8>, Vec<u8>);

/// All visible records of a user key as (tag, suffix, fresh-HLC value).
/// Values are re-stamped so the copy wins at the destination.
fn collect_key_records(ctx: &ShardCtx, key: &[u8]) -> Option<Vec<KeyRecord>> {
    let t = key_type(ctx, key)?;
    let now = now_ms();
    let mut out = Vec::new();
    match t {
        b's' => {
            let (env, pay) = read_lww(ctx, &ikey::string_key(key), 0)?;
            // RENAME freezes counters into a plain string at the destination
            // (a rename is a copy under a new identity; the delta history
            // belongs to the old key).
            let pay = if env.rtype() == RecordType::Counter {
                marekvs_core::counter::CounterState::decode(&pay)?
                    .value()?
                    .to_string()
                    .into_bytes()
            } else {
                pay
            };
            out.push((
                b's',
                Vec::new(),
                store::new_lww(ctx, RecordType::String, &pay, env.ttl_deadline_ms),
            ));
        }
        ctype => {
            // Collections (hash/set/zset/stream/list) — head + elements.
            // List elements are position-keyed LWW records; they fall into the
            // non-OR-element branch below and copy with their position suffix
            // intact, so list order survives a RENAME.
            let (head_env, _, del) = get_head(ctx, key)?;
            let head_raw = get_raw(ctx, &ikey::head_key(key))?;
            let (_, head_payload) = Envelope::decode(&head_raw)?;
            let env = Envelope::head(ctx.hlc.now(), ctx.node_id).with_ttl(head_env.ttl_deadline_ms);
            out.push((b'M', Vec::new(), env.encode_with(head_payload)));
            let tag = match ctype {
                head::CTYPE_HASH => ikey::Tag::HashField,
                head::CTYPE_SET => ikey::Tag::SetMember,
                head::CTYPE_ZSET => ikey::Tag::ZsetMember,
                head::CTYPE_STREAM => ikey::Tag::StreamEntry,
                head::CTYPE_HLL => ikey::Tag::HllRegister,
                head::CTYPE_LIST => ikey::Tag::ListElem,
                _ => return None,
            };
            scan_prefix(ctx, &ikey::collection_prefix(tag, key), |k, v| {
                if let (Some(p), Some((env, pay))) = (ikey::parse(k), Envelope::decode(v)) {
                    if store::visible(&env, pay, del, now).is_some() {
                        if env.rtype().is_or_element() {
                            if let Some(val) = marekvs_core::merge::element_value(pay) {
                                let rec = marekvs_core::merge::element_add_ttl(
                                    env.rtype(),
                                    ctx.hlc.now(),
                                    ctx.node_id,
                                    &val,
                                    env.ttl_deadline_ms,
                                );
                                out.push((tag as u8, p.suffix.to_vec(), rec));
                            }
                        } else {
                            out.push((
                                tag as u8,
                                p.suffix.to_vec(),
                                store::new_lww(ctx, env.rtype(), pay, env.ttl_deadline_ms),
                            ));
                        }
                    }
                }
                true
            });
        }
    }
    Some(out)
}

/// Raise a head payload's `del_hlc` field (`[ctype][del_hlc u64][tail]`) to
/// at least `min`, preserving ctype and any type-specific tail. Used to make
/// a COPY/RENAME clobber shadow the destination's stale elements.
fn shadow_head_del_hlc(payload: &[u8], min: u64) -> Vec<u8> {
    let mut out = payload.to_vec();
    if out.len() >= 9 {
        let cur = u64::from_be_bytes(out[1..9].try_into().unwrap());
        if min > cur {
            out[1..9].copy_from_slice(&min.to_be_bytes());
        }
    }
    out
}

fn rebuild_ikey(tag: u8, userkey: &[u8], suffix: &[u8]) -> Vec<u8> {
    match tag {
        b's' => ikey::string_key(userkey),
        b'l' => ikey::list_key(userkey),
        b'M' => ikey::head_key(userkey),
        b'h' => ikey::hash_field_key(userkey, suffix),
        b'S' => ikey::set_member_key(userkey, suffix),
        b'z' => ikey::zset_member_key(userkey, suffix),
        b'q' => ikey::prefixed(ikey::Tag::ListElem, userkey, suffix),
        b'x' => ikey::prefixed(ikey::Tag::StreamEntry, userkey, suffix),
        b'H' => ikey::prefixed(ikey::Tag::HllRegister, userkey, suffix),
        _ => unreachable!("unknown tag"),
    }
}

/// Re-stamp a collected source record for writing at the destination. For
/// the head record, `min_head_del_hlc` forces the head's `del_hlc` up to at
/// least the destination clobber clock (0 = no clobber / fresh dest), so a
/// REPLACE onto an existing collection shadows the destination's stale
/// elements instead of resurrecting them. The type-specific head tail
/// (stream counters) is preserved.
fn restamp_record(ctx: &ShardCtx, value: &[u8], min_head_del_hlc: u64) -> Option<Vec<u8>> {
    let (env, payload) = Envelope::decode(value)?;
    if env.is_head() {
        let payload = shadow_head_del_hlc(payload, min_head_del_hlc);
        return Some(
            Envelope::head(ctx.hlc.now(), ctx.node_id)
                .with_ttl(env.ttl_deadline_ms)
                .encode_with(&payload),
        );
    }
    if env.rtype().is_or_element() {
        let val = marekvs_core::merge::element_value(payload)?;
        return Some(marekvs_core::merge::element_add_ttl(
            env.rtype(),
            ctx.hlc.now(),
            ctx.node_id,
            &val,
            env.ttl_deadline_ms,
        ));
    }
    Some(store::new_lww(
        ctx,
        env.rtype(),
        payload,
        env.ttl_deadline_ms,
    ))
}

fn hex_encode(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn hex_decode(s: &[u8]) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    let s = std::str::from_utf8(s).ok()?;
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

/// Notify the zset module: after applying a remote zset member record the
/// score index must be rebuilt. Exposed here for the replication apply path.
pub fn get_head_del(ctx: &ShardCtx, userkey: &[u8]) -> u64 {
    get_head(ctx, userkey).map_or(0, |(env, _, del)| {
        let now = now_ms();
        let mut d = del;
        if env.is_tombstone() {
            d = d.max(env.hlc);
        }
        if env.is_expired(now) {
            d = d.max(env.expiry_hlc());
        }
        d
    })
}

/// Re-read helper used by SCAN-family commands of collections.
pub fn raw_head_exists(ctx: &ShardCtx, userkey: &[u8]) -> bool {
    get_raw(ctx, &ikey::head_key(userkey)).is_some()
}

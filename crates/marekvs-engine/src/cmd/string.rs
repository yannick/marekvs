//! String family (design/03). Every mutation is a fresh-HLC LWW record fed
//! through `write_merged`; read-modify-write commands are atomic because a
//! key's shard thread serializes them.
//!
//! v1.1: INCR/DECR/INCRBY/DECRBY are PN counters (marekvs_core::counter) —
//! concurrent increments on different nodes all survive; an explicit SET (or
//! DEL) resets. Reads materialize counter records into decimal strings, so
//! every other string command sees an ordinary value.

use std::sync::Arc;

use crate::cmd::{eq_ignore_case, fmt_f64, parse_f64, parse_i64, parse_u64};
use crate::reply::Reply;
use crate::store::{check_type, new_lww, new_tombstone, now_ms, read_lww, write_merged, ShardCtx};
use crate::Engine;
use marekvs_core::counter::CounterState;
use marekvs_core::envelope::{Envelope, RecordType};
use marekvs_core::ikey;

/// Materialized read of a string key: counter records render as decimal.
///
/// Fast path first: a live string record shadows collections (`key_type`
/// checks strings before heads), so the type gate is only consulted on a
/// string miss — hot GET/INCR cost one point read instead of three
/// (profiling: the eager gate was the top marekvs cost under load).
fn read_string(ctx: &ShardCtx, key: &[u8]) -> Result<Option<Vec<u8>>, ()> {
    if let Some((env, payload)) = read_lww(ctx, &ikey::string_key(key), 0) {
        return Ok(if env.rtype() == RecordType::Counter {
            CounterState::decode(&payload).and_then(|st| Some(st.value()?.to_string().into_bytes()))
        } else {
            Some(payload)
        });
    }
    check_type(ctx, key, b's')?; // miss: WRONGTYPE if a collection holds the key
    Ok(None)
}

fn write_string(ctx: &ShardCtx, key: &[u8], value: &[u8], ttl_deadline_ms: u64) {
    let rec = new_lww(ctx, RecordType::String, value, ttl_deadline_ms);
    // Blind put: a fresh local LWW record always wins the merge (HLC
    // monotonicity + the receive rule), so write_merged's read is waste.
    crate::store::put_raw(ctx, &ikey::string_key(key), &rec);
}

fn current_ttl_deadline(ctx: &ShardCtx, key: &[u8]) -> u64 {
    read_lww(ctx, &ikey::string_key(key), 0).map_or(0, |(env, _)| env.ttl_deadline_ms)
}

pub async fn get(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 2 {
        return Reply::wrong_args("get");
    }
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| match read_string(ctx, &key) {
            Err(()) => Reply::wrongtype(),
            Ok(Some(v)) => Reply::Bulk(v),
            Ok(None) => Reply::Null,
        })
        .await
}

pub async fn set(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 3 {
        return Reply::wrong_args("set");
    }
    let key = args[1].clone();
    let value = args[2].clone();

    let mut nx = false;
    let mut xx = false;
    let mut keepttl = false;
    let mut get_old = false;
    let mut ttl: Option<u64> = None; // absolute deadline ms
    let now = now_ms();

    let mut i = 3;
    while i < args.len() {
        let a = &args[i];
        if eq_ignore_case(a, "NX") {
            nx = true;
        } else if eq_ignore_case(a, "XX") {
            xx = true;
        } else if eq_ignore_case(a, "KEEPTTL") {
            keepttl = true;
        } else if eq_ignore_case(a, "GET") {
            get_old = true;
        } else if eq_ignore_case(a, "EX") || eq_ignore_case(a, "PX") {
            let Some(n) = args.get(i + 1).and_then(|b| parse_u64(b)) else {
                return Reply::not_int();
            };
            if n == 0 {
                return Reply::err("ERR invalid expire time in 'set' command");
            }
            let mult = if eq_ignore_case(a, "EX") { 1000 } else { 1 };
            ttl = Some(now + n * mult);
            i += 1;
        } else if eq_ignore_case(a, "EXAT") || eq_ignore_case(a, "PXAT") {
            let Some(n) = args.get(i + 1).and_then(|b| parse_u64(b)) else {
                return Reply::not_int();
            };
            let mult = if eq_ignore_case(a, "EXAT") { 1000 } else { 1 };
            ttl = Some(n * mult);
            i += 1;
        } else {
            return Reply::syntax();
        }
        i += 1;
    }
    if nx && xx {
        return Reply::syntax();
    }

    engine
        .store
        .run_key(&args[1], move |ctx| {
            // SET overwrites any type in Redis (our string record shadows
            // collections via key_type priority), so the plain form needs
            // NO reads at all — the type gate only matters for SET ... GET,
            // and the old value only for NX/XX/KEEPTTL/GET.
            if get_old && check_type(ctx, &key, b's').is_err() {
                return Reply::wrongtype();
            }
            let need_old = nx || xx || keepttl || get_old;
            let old = if need_old {
                read_lww(ctx, &ikey::string_key(&key), 0)
            } else {
                None
            };
            if need_old {
                let exists = old.is_some();
                if (nx && exists) || (xx && !exists) {
                    return if get_old {
                        old.map_or(Reply::Null, |(_, v)| Reply::Bulk(v))
                    } else {
                        Reply::Null
                    };
                }
            }
            let deadline = if keepttl {
                old.as_ref().map_or(0, |(env, _)| env.ttl_deadline_ms)
            } else {
                ttl.unwrap_or(0)
            };
            write_string(ctx, &key, &value, deadline);
            if get_old {
                old.map_or(Reply::Null, |(_, v)| Reply::Bulk(v))
            } else {
                Reply::ok()
            }
        })
        .await
}

pub async fn setnx(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 3 {
        return Reply::wrong_args("setnx");
    }
    let (key, value) = (args[1].clone(), args[2].clone());
    engine
        .store
        .run_key(&args[1], move |ctx| {
            if check_type(ctx, &key, b's').is_err() {
                return Reply::Int(0);
            }
            if read_lww(ctx, &ikey::string_key(&key), 0).is_some() {
                return Reply::Int(0);
            }
            write_string(ctx, &key, &value, 0);
            Reply::Int(1)
        })
        .await
}

pub async fn setex(engine: &Arc<Engine>, args: &[Vec<u8>], mult: u64) -> Reply {
    if args.len() != 4 {
        return Reply::wrong_args("setex");
    }
    let Some(secs) = parse_u64(&args[2]) else {
        return Reply::not_int();
    };
    if secs == 0 {
        return Reply::err("ERR invalid expire time in 'setex' command");
    }
    let (key, value) = (args[1].clone(), args[3].clone());
    let deadline = now_ms() + secs * mult;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            write_string(ctx, &key, &value, deadline);
            Reply::ok()
        })
        .await
}

pub async fn getset(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 3 {
        return Reply::wrong_args("getset");
    }
    let (key, value) = (args[1].clone(), args[2].clone());
    engine
        .store
        .run_key(&args[1], move |ctx| match read_string(ctx, &key) {
            Err(()) => Reply::wrongtype(),
            Ok(old) => {
                write_string(ctx, &key, &value, 0);
                old.map_or(Reply::Null, Reply::Bulk)
            }
        })
        .await
}

pub async fn getdel(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 2 {
        return Reply::wrong_args("getdel");
    }
    let key = args[1].clone();
    engine
        .store
        .run_key(&args[1], move |ctx| match read_string(ctx, &key) {
            Err(()) => Reply::wrongtype(),
            Ok(Some(v)) => {
                let tomb = new_tombstone(ctx, RecordType::String);
                write_merged(ctx, &ikey::string_key(&key), &tomb);
                Reply::Bulk(v)
            }
            Ok(None) => Reply::Null,
        })
        .await
}

pub async fn getex(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 2 {
        return Reply::wrong_args("getex");
    }
    let key = args[1].clone();
    let now = now_ms();
    let mut new_deadline: Option<u64> = None; // None = leave, Some(0) = persist
    let mut i = 2;
    while i < args.len() {
        let a = &args[i];
        if eq_ignore_case(a, "PERSIST") {
            new_deadline = Some(0);
        } else if eq_ignore_case(a, "EX") || eq_ignore_case(a, "PX") {
            let Some(n) = args.get(i + 1).and_then(|b| parse_u64(b)) else {
                return Reply::not_int();
            };
            let mult = if eq_ignore_case(a, "EX") { 1000 } else { 1 };
            new_deadline = Some(now + n * mult);
            i += 1;
        } else if eq_ignore_case(a, "EXAT") || eq_ignore_case(a, "PXAT") {
            let Some(n) = args.get(i + 1).and_then(|b| parse_u64(b)) else {
                return Reply::not_int();
            };
            let mult = if eq_ignore_case(a, "EXAT") { 1000 } else { 1 };
            new_deadline = Some(n * mult);
            i += 1;
        } else {
            return Reply::syntax();
        }
        i += 1;
    }
    engine
        .store
        .run_key(&args[1], move |ctx| match read_string(ctx, &key) {
            Err(()) => Reply::wrongtype(),
            Ok(Some(v)) => {
                if let Some(d) = new_deadline {
                    write_string(ctx, &key, &v, d);
                }
                Reply::Bulk(v)
            }
            Ok(None) => Reply::Null,
        })
        .await
}

pub async fn append(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 3 {
        return Reply::wrong_args("append");
    }
    let (key, suffix) = (args[1].clone(), args[2].clone());
    engine
        .store
        .run_key(&args[1], move |ctx| match read_string(ctx, &key) {
            Err(()) => Reply::wrongtype(),
            Ok(old) => {
                let keep_ttl = current_ttl_deadline(ctx, &key);
                let mut v = old.unwrap_or_default();
                v.extend_from_slice(&suffix);
                let len = v.len();
                write_string(ctx, &key, &v, keep_ttl);
                Reply::Int(len as i64)
            }
        })
        .await
}

pub async fn strlen(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 2 {
        return Reply::wrong_args("strlen");
    }
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| match read_string(ctx, &key) {
            Err(()) => Reply::wrongtype(),
            Ok(v) => Reply::Int(v.map_or(0, |v| v.len() as i64)),
        })
        .await
}

pub async fn incrby_cmd(
    engine: &Arc<Engine>,
    args: &[Vec<u8>],
    sign: i64,
    takes_arg: bool,
) -> Reply {
    let want = if takes_arg { 3 } else { 2 };
    if args.len() != want {
        return Reply::wrong_args("incr");
    }
    let delta = if takes_arg {
        match parse_i64(&args[2]) {
            Some(d) => d,
            None => return Reply::not_int(),
        }
    } else {
        1
    } * sign;
    let key = args[1].clone();
    engine
        .store
        .run_key(&args[1], move |ctx| {
            // PN counter (design/02 v1.1): fold the current record into a
            // counter state, bump our own slot, write the joined record.
            // Fast path: an existing live string/counter shadows collections,
            // so the type gate only runs on a miss (1 read hot, not 4).
            let existing = read_lww(ctx, &ikey::string_key(&key), 0);
            if existing.is_none() && check_type(ctx, &key, b's').is_err() {
                return Reply::wrongtype();
            }
            let mut state = match &existing {
                None => CounterState::on_base(0, 0, 0),
                Some((env, payload)) if env.rtype() == RecordType::Counter => {
                    match CounterState::decode(payload) {
                        Some(st) => st,
                        None => return Reply::not_int(),
                    }
                }
                Some((env, payload)) => {
                    // Plain string: it becomes this counter's base register,
                    // keyed by ITS version — concurrent INCRs on other nodes
                    // that read the same string produce the same base and
                    // join instead of racing.
                    let parsed: i64 = match std::str::from_utf8(payload)
                        .ok()
                        .and_then(|s| s.trim().parse().ok())
                    {
                        Some(n) => n,
                        None => return Reply::not_int(),
                    };
                    CounterState::on_base(env.hlc, env.origin, parsed)
                }
            };
            state.bump(ctx.node_id, delta);
            let Some(new) = state.value() else {
                return Reply::err("ERR increment or decrement would overflow");
            };
            let keep_ttl = existing.map_or(0, |(env, _)| env.ttl_deadline_ms);
            let env =
                Envelope::new(RecordType::Counter, ctx.hlc.now(), ctx.node_id).with_ttl(keep_ttl);
            write_merged(
                ctx,
                &ikey::string_key(&key),
                &env.encode_with(&state.encode()),
            );
            Reply::Int(new)
        })
        .await
}

pub async fn incrbyfloat(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 3 {
        return Reply::wrong_args("incrbyfloat");
    }
    let Some(delta) = parse_f64(&args[2]) else {
        return Reply::not_float();
    };
    let key = args[1].clone();
    engine
        .store
        .run_key(&args[1], move |ctx| match read_string(ctx, &key) {
            Err(()) => Reply::wrongtype(),
            Ok(old) => {
                let cur: f64 = match &old {
                    None => 0.0,
                    Some(v) => match std::str::from_utf8(v).ok().and_then(|s| s.parse().ok()) {
                        Some(n) => n,
                        None => return Reply::not_float(),
                    },
                };
                let new = cur + delta;
                if new.is_nan() || new.is_infinite() {
                    return Reply::err("ERR increment would produce NaN or Infinity");
                }
                let s = fmt_f64(new);
                let keep_ttl = current_ttl_deadline(ctx, &key);
                write_string(ctx, &key, s.as_bytes(), keep_ttl);
                Reply::Bulk(s.into_bytes())
            }
        })
        .await
}

pub async fn mget(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 2 {
        return Reply::wrong_args("mget");
    }
    // Group keys by shard: one job per shard reads all of that shard's
    // keys (read-through fetch still runs per key beforehand for
    // cluster-remote keys).
    for keyarg in &args[1..] {
        engine.ensure_local(keyarg).await;
    }
    /// Per-shard read plan: representative pid + (reply index, user key).
    type ReadPlan = (marekvs_core::ikey::Pid, Vec<(usize, Vec<u8>)>);
    let mut per_shard: std::collections::HashMap<usize, ReadPlan> =
        std::collections::HashMap::new();
    for (i, keyarg) in args[1..].iter().enumerate() {
        let pid = marekvs_core::pid_of(keyarg);
        let shard = engine.store.shard_of(pid);
        per_shard
            .entry(shard)
            .or_insert_with(|| (pid, Vec::new()))
            .1
            .push((i, keyarg.clone()));
    }
    let mut tasks = tokio::task::JoinSet::new();
    for (_, (pid, keys)) in per_shard {
        let engine = engine.clone();
        tasks.spawn(async move {
            engine
                .store
                .run(pid, move |ctx| {
                    keys.into_iter()
                        .map(|(i, k)| {
                            let r = match read_string(ctx, &k) {
                                Ok(Some(v)) => Reply::Bulk(v),
                                _ => Reply::Null,
                            };
                            (i, r)
                        })
                        .collect::<Vec<_>>()
                })
                .await
        });
    }
    let mut replies = vec![Reply::Null; args.len() - 1];
    while let Some(Ok(chunk)) = tasks.join_next().await {
        for (i, r) in chunk {
            replies[i] = r;
        }
    }
    Reply::Array(replies)
}

pub async fn mset(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 3 || args.len() % 2 == 0 {
        return Reply::wrong_args("mset");
    }
    // Group pairs by shard: ONE job and ONE ondadb transaction per shard
    // (one WAL group-commit frame) instead of a full write round-trip per
    // key — MSET(10) previously cost exactly 10× a single SET.
    /// Per-shard write plan: representative pid + (key, value) pairs.
    type WritePlan = (marekvs_core::ikey::Pid, Vec<(Vec<u8>, Vec<u8>)>);
    let mut per_shard: std::collections::HashMap<usize, WritePlan> =
        std::collections::HashMap::new();
    for pair in args[1..].chunks(2) {
        let pid = marekvs_core::pid_of(&pair[0]);
        let shard = engine.store.shard_of(pid);
        per_shard
            .entry(shard)
            .or_insert_with(|| (pid, Vec::new()))
            .1
            .push((pair[0].clone(), pair[1].clone()));
    }
    let mut tasks = tokio::task::JoinSet::new();
    for (_, (pid, pairs)) in per_shard {
        let engine = engine.clone();
        tasks.spawn(async move {
            engine
                .store
                .run(pid, move |ctx| {
                    let items: Vec<(Vec<u8>, Vec<u8>)> = pairs
                        .iter()
                        .map(|(k, v)| {
                            (
                                ikey::string_key(k),
                                crate::store::new_lww(ctx, RecordType::String, v, 0),
                            )
                        })
                        .collect();
                    crate::store::put_many_lww(ctx, &items);
                })
                .await;
        });
    }
    while tasks.join_next().await.is_some() {}
    Reply::ok()
}

pub async fn msetnx(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 3 || args.len() % 2 == 0 {
        return Reply::wrong_args("msetnx");
    }
    // Check-then-set; atomic per shard only (documented cross-shard caveat).
    for pair in args[1..].chunks(2) {
        let key = pair[0].clone();
        let exists = engine
            .store
            .run_key(&pair[0], move |ctx| {
                read_lww(ctx, &ikey::string_key(&key), 0).is_some()
            })
            .await;
        if exists {
            return Reply::Int(0);
        }
    }
    for pair in args[1..].chunks(2) {
        let (key, value) = (pair[0].clone(), pair[1].clone());
        engine
            .store
            .run_key(&pair[0], move |ctx| write_string(ctx, &key, &value, 0))
            .await;
    }
    Reply::Int(1)
}

pub async fn setrange(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 4 {
        return Reply::wrong_args("setrange");
    }
    let Some(offset) = parse_i64(&args[2]) else {
        return Reply::not_int();
    };
    if offset < 0 {
        return Reply::err("ERR offset is out of range");
    }
    let offset = offset as usize;
    let (key, patch) = (args[1].clone(), args[3].clone());
    engine
        .store
        .run_key(&args[1], move |ctx| match read_string(ctx, &key) {
            Err(()) => Reply::wrongtype(),
            Ok(old) => {
                let mut v = old.unwrap_or_default();
                if patch.is_empty() {
                    return Reply::Int(v.len() as i64);
                }
                if v.len() < offset + patch.len() {
                    v.resize(offset + patch.len(), 0);
                }
                v[offset..offset + patch.len()].copy_from_slice(&patch);
                let len = v.len();
                let keep_ttl = current_ttl_deadline(ctx, &key);
                write_string(ctx, &key, &v, keep_ttl);
                Reply::Int(len as i64)
            }
        })
        .await
}

pub async fn getrange(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 4 {
        return Reply::wrong_args("getrange");
    }
    let (Some(start), Some(end)) = (parse_i64(&args[2]), parse_i64(&args[3])) else {
        return Reply::not_int();
    };
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| match read_string(ctx, &key) {
            Err(()) => Reply::wrongtype(),
            Ok(None) => Reply::Bulk(Vec::new()),
            Ok(Some(v)) => {
                let len = v.len() as i64;
                let mut s = if start < 0 {
                    (len + start).max(0)
                } else {
                    start.min(len)
                };
                let mut e = if end < 0 { len + end } else { end };
                e = e.min(len - 1);
                if s > e || len == 0 {
                    return Reply::Bulk(Vec::new());
                }
                s = s.max(0);
                Reply::Bulk(v[s as usize..=(e as usize)].to_vec())
            }
        })
        .await
}

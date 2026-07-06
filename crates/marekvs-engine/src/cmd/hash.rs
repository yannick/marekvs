//! Hash family. Fields are OR elements: one storage record per field
//! (design/02), adds/removes merge via dot sets.

use std::sync::Arc;

use crate::cmd::{eq_ignore_case, fmt_f64, parse_f64, parse_i64, parse_u64};
use crate::reply::Reply;
use crate::store::{
    check_type, ensure_head, get_raw, now_ms, read_element, scan_prefix, visible, write_merged,
    ShardCtx,
};
use crate::Engine;
use marekvs_core::envelope::{head, Envelope, RecordType};
use marekvs_core::ikey;
use marekvs_core::merge::{
    element_add, element_add_ttl, element_dots, element_remove, element_value,
};

pub(crate) fn hash_del_hlc(ctx: &ShardCtx, key: &[u8]) -> Result<u64, ()> {
    check_type(ctx, key, head::CTYPE_HASH)
}

/// All visible (field, value) pairs.
pub(crate) fn hash_entries(ctx: &ShardCtx, key: &[u8], del: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
    let now = now_ms();
    let mut out = Vec::new();
    scan_prefix(
        ctx,
        &ikey::collection_prefix(ikey::Tag::HashField, key),
        |k, v| {
            if let (Some(p), Some((env, pay))) = (ikey::parse(k), Envelope::decode(v)) {
                if visible(&env, pay, del, now).is_some() {
                    if let Some(val) = element_value(pay) {
                        out.push((p.suffix.to_vec(), val));
                    }
                }
            }
            true
        },
    );
    out
}

fn field_exists(ctx: &ShardCtx, key: &[u8], field: &[u8], del: u64) -> bool {
    read_element_checked(ctx, key, field, del).is_some()
}

fn read_element_checked(ctx: &ShardCtx, key: &[u8], field: &[u8], del: u64) -> Option<Vec<u8>> {
    read_element(ctx, &ikey::hash_field_key(key, field), del)
}

fn write_field(ctx: &ShardCtx, key: &[u8], field: &[u8], value: &[u8]) {
    let rec = element_add(RecordType::HashField, ctx.hlc.now(), ctx.node_id, value);
    write_merged(ctx, &ikey::hash_field_key(key, field), &rec);
}

fn write_field_ttl(ctx: &ShardCtx, key: &[u8], field: &[u8], value: &[u8], ttl: u64) {
    let rec = element_add_ttl(
        RecordType::HashField,
        ctx.hlc.now(),
        ctx.node_id,
        value,
        ttl,
    );
    write_merged(ctx, &ikey::hash_field_key(key, field), &rec);
}

/// Re-stamp an EXISTING field's TTL while PRESERVING its OR-element dots
/// (the same restamp `set_member_deadline`/EXPIREMEMBER uses). A metadata-
/// only TTL change (HEXPIRE/HPERSIST/HGETEX EX) must NOT mint a fresh add-dot
/// like `write_field_ttl`/HSET do: minting a new dot makes the change a
/// logical re-add, which loses a concurrent cross-node HDEL after merge
/// (the delete's covered-dot no longer covers the new dot) — resurrecting a
/// deleted field and diverging from EXPIREMEMBER on the same data. The caller
/// must have already confirmed the field is visible and `deadline` is in the
/// future (past deadlines go through `remove_field`).
fn restamp_field_ttl(ctx: &ShardCtx, key: &[u8], field: &[u8], deadline: u64) -> bool {
    let ik = ikey::hash_field_key(key, field);
    let Some(raw) = get_raw(ctx, &ik) else {
        return false;
    };
    let Some((env, payload)) = Envelope::decode(&raw) else {
        return false;
    };
    let restamped = Envelope {
        flags: env.flags,
        hlc: ctx.hlc.now(),
        origin: ctx.node_id,
        ttl_deadline_ms: deadline,
    }
    .encode_with(payload);
    write_merged(ctx, &ik, &restamped);
    true
}

fn remove_field(ctx: &ShardCtx, key: &[u8], field: &[u8], del: u64) -> bool {
    let ik = ikey::hash_field_key(key, field);
    let Some(v) = get_raw(ctx, &ik) else {
        return false;
    };
    let Some((env, pay)) = Envelope::decode(&v) else {
        return false;
    };
    if visible(&env, pay, del, now_ms()).is_none() || element_value(pay).is_none() {
        return false;
    }
    let dots = element_dots(pay);
    let rm = element_remove(RecordType::HashField, ctx.hlc.now(), ctx.node_id, &dots);
    write_merged(ctx, &ik, &rm);
    true
}

pub async fn hset(engine: &Arc<Engine>, args: &[Vec<u8>], hmset_reply: bool) -> Reply {
    if args.len() < 4 || args.len() % 2 != 0 {
        return Reply::wrong_args("hset");
    }
    let key = args[1].clone();
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = args[2..]
        .chunks(2)
        .map(|c| (c[0].clone(), c[1].clone()))
        .collect();
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = hash_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            ensure_head(ctx, &key, head::CTYPE_HASH);
            let mut added = 0;
            for (f, v) in &pairs {
                if !field_exists(ctx, &key, f, del) {
                    added += 1;
                }
                write_field(ctx, &key, f, v);
            }
            if hmset_reply {
                Reply::ok()
            } else {
                Reply::Int(added)
            }
        })
        .await
}

pub async fn hsetnx(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 4 {
        return Reply::wrong_args("hsetnx");
    }
    let (key, field, value) = (args[1].clone(), args[2].clone(), args[3].clone());
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = hash_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            if field_exists(ctx, &key, &field, del) {
                return Reply::Int(0);
            }
            ensure_head(ctx, &key, head::CTYPE_HASH);
            write_field(ctx, &key, &field, &value);
            Reply::Int(1)
        })
        .await
}

pub async fn hget(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 3 {
        return Reply::wrong_args("hget");
    }
    let (key, field) = (args[1].clone(), args[2].clone());
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = hash_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            read_element_checked(ctx, &key, &field, del).map_or(Reply::Null, Reply::Bulk)
        })
        .await
}

pub async fn hmget(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 3 {
        return Reply::wrong_args("hmget");
    }
    let key = args[1].clone();
    let fields: Vec<Vec<u8>> = args[2..].to_vec();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = hash_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            Reply::Array(
                fields
                    .iter()
                    .map(|f| {
                        read_element_checked(ctx, &key, f, del).map_or(Reply::Null, Reply::Bulk)
                    })
                    .collect(),
            )
        })
        .await
}

pub async fn hgetall(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 2 {
        return Reply::wrong_args("hgetall");
    }
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = hash_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            Reply::Map(
                hash_entries(ctx, &key, del)
                    .into_iter()
                    .map(|(f, v)| (Reply::Bulk(f), Reply::Bulk(v)))
                    .collect(),
            )
        })
        .await
}

pub async fn hdel(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 3 {
        return Reply::wrong_args("hdel");
    }
    let key = args[1].clone();
    let fields: Vec<Vec<u8>> = args[2..].to_vec();
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = hash_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            let mut n = 0;
            for f in &fields {
                if remove_field(ctx, &key, f, del) {
                    n += 1;
                }
            }
            Reply::Int(n)
        })
        .await
}

#[derive(Clone, Copy)]
enum ExpireCond {
    None,
    Nx,
    Xx,
    Gt,
    Lt,
}

#[derive(Clone, Copy)]
enum HashExpireMode {
    Deadline(u64),
    Persist,
    KeepTtl,
    None,
}

fn read_field_env(
    ctx: &ShardCtx,
    key: &[u8],
    field: &[u8],
    del: u64,
) -> Option<(Envelope, Vec<u8>)> {
    let raw = get_raw(ctx, &ikey::hash_field_key(key, field))?;
    let (env, pay) = Envelope::decode(&raw)?;
    visible(&env, pay, del, now_ms())?;
    let value = element_value(pay)?;
    Some((env, value))
}

fn ttl_condition_passes(current: u64, new_deadline: u64, cond: ExpireCond) -> bool {
    match cond {
        ExpireCond::None => true,
        ExpireCond::Nx => current == 0,
        ExpireCond::Xx => current != 0,
        // Redis treats a non-volatile item as infinite TTL for GT/LT.
        ExpireCond::Gt => current != 0 && new_deadline > current,
        ExpireCond::Lt => current == 0 || new_deadline < current,
    }
}

fn set_field_deadline(
    ctx: &ShardCtx,
    key: &[u8],
    field: &[u8],
    del: u64,
    deadline: u64,
    cond: ExpireCond,
) -> i64 {
    let Some((env, _value)) = read_field_env(ctx, key, field, del) else {
        return -2;
    };
    if !ttl_condition_passes(env.ttl_deadline_ms, deadline, cond) {
        return 0;
    }
    if deadline != 0 && deadline <= now_ms() {
        if remove_field(ctx, key, field, del) {
            return 2;
        }
        return -2;
    }
    // Dot-preserving restamp: a TTL-only change must not resurrect a
    // concurrently-deleted field (see restamp_field_ttl).
    restamp_field_ttl(ctx, key, field, deadline);
    1
}

fn field_ttl_status(ctx: &ShardCtx, key: &[u8], field: &[u8], del: u64, millis: bool) -> i64 {
    let Some((env, _)) = read_field_env(ctx, key, field, del) else {
        return -2;
    };
    if env.ttl_deadline_ms == 0 {
        return -1;
    }
    let remain = env.ttl_deadline_ms.saturating_sub(now_ms());
    if millis {
        remain as i64
    } else {
        (remain / 1000) as i64
    }
}

fn field_expiretime_status(
    ctx: &ShardCtx,
    key: &[u8],
    field: &[u8],
    del: u64,
    millis: bool,
) -> i64 {
    let Some((env, _)) = read_field_env(ctx, key, field, del) else {
        return -2;
    };
    if env.ttl_deadline_ms == 0 {
        -1
    } else if millis {
        env.ttl_deadline_ms as i64
    } else {
        (env.ttl_deadline_ms / 1000) as i64
    }
}

fn parse_fields_block(args: &[Vec<u8>], i: usize) -> Result<Vec<Vec<u8>>, Reply> {
    if args.get(i).is_none_or(|a| !eq_ignore_case(a, "FIELDS")) {
        return Err(Reply::syntax());
    }
    let Some(n) = args.get(i + 1).and_then(|b| parse_u64(b)) else {
        return Err(Reply::not_int());
    };
    let n = n as usize;
    if n == 0 || args.len() != i + 2 + n {
        return Err(Reply::syntax());
    }
    Ok(args[i + 2..].to_vec())
}

fn deadline_from(n: i64, mult: u64, absolute: bool) -> u64 {
    if absolute {
        (n.max(0) as u64) * mult
    } else if n <= 0 {
        1
    } else {
        now_ms() + n as u64 * mult
    }
}

pub async fn hexpire(engine: &Arc<Engine>, args: &[Vec<u8>], mult: u64, absolute: bool) -> Reply {
    if args.len() < 5 {
        return Reply::wrong_args("hexpire");
    }
    let Some(n) = parse_i64(&args[2]) else {
        return Reply::not_int();
    };
    let mut cond = ExpireCond::None;
    let mut i = 3;
    if args.get(i).is_some_and(|a| {
        eq_ignore_case(a, "NX")
            || eq_ignore_case(a, "XX")
            || eq_ignore_case(a, "GT")
            || eq_ignore_case(a, "LT")
    }) {
        cond = if eq_ignore_case(&args[i], "NX") {
            ExpireCond::Nx
        } else if eq_ignore_case(&args[i], "XX") {
            ExpireCond::Xx
        } else if eq_ignore_case(&args[i], "GT") {
            ExpireCond::Gt
        } else {
            ExpireCond::Lt
        };
        i += 1;
    }
    let fields = match parse_fields_block(args, i) {
        Ok(f) => f,
        Err(r) => return r,
    };
    let key = args[1].clone();
    let deadline = deadline_from(n, mult, absolute);
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = hash_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            Reply::Array(
                fields
                    .iter()
                    .map(|f| Reply::Int(set_field_deadline(ctx, &key, f, del, deadline, cond)))
                    .collect(),
            )
        })
        .await
}

pub async fn httl(engine: &Arc<Engine>, args: &[Vec<u8>], millis: bool, expiretime: bool) -> Reply {
    if args.len() < 4 {
        return Reply::wrong_args("httl");
    }
    let fields = match parse_fields_block(args, 2) {
        Ok(f) => f,
        Err(r) => return r,
    };
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = hash_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            Reply::Array(
                fields
                    .iter()
                    .map(|f| {
                        Reply::Int(if expiretime {
                            field_expiretime_status(ctx, &key, f, del, millis)
                        } else {
                            field_ttl_status(ctx, &key, f, del, millis)
                        })
                    })
                    .collect(),
            )
        })
        .await
}

pub async fn hpersist(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 4 {
        return Reply::wrong_args("hpersist");
    }
    let fields = match parse_fields_block(args, 2) {
        Ok(f) => f,
        Err(r) => return r,
    };
    let key = args[1].clone();
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = hash_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            Reply::Array(
                fields
                    .iter()
                    .map(|f| {
                        let Some((env, _)) = read_field_env(ctx, &key, f, del) else {
                            return Reply::Int(-2);
                        };
                        if env.ttl_deadline_ms == 0 {
                            Reply::Int(-1)
                        } else {
                            Reply::Int(set_field_deadline(ctx, &key, f, del, 0, ExpireCond::None))
                        }
                    })
                    .collect(),
            )
        })
        .await
}

pub async fn hgetdel(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 4 {
        return Reply::wrong_args("hgetdel");
    }
    let fields = match parse_fields_block(args, 2) {
        Ok(f) => f,
        Err(r) => return r,
    };
    let key = args[1].clone();
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = hash_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            Reply::Array(
                fields
                    .iter()
                    .map(|f| {
                        let val = read_element_checked(ctx, &key, f, del)
                            .map(Reply::Bulk)
                            .unwrap_or(Reply::Null);
                        remove_field(ctx, &key, f, del);
                        val
                    })
                    .collect(),
            )
        })
        .await
}

fn parse_hash_expiration(
    args: &[Vec<u8>],
    i: &mut usize,
    allow_persist: bool,
    allow_keepttl: bool,
) -> Result<HashExpireMode, Reply> {
    if *i >= args.len() {
        return Ok(HashExpireMode::None);
    }
    if allow_persist && eq_ignore_case(&args[*i], "PERSIST") {
        *i += 1;
        return Ok(HashExpireMode::Persist);
    }
    if allow_keepttl && eq_ignore_case(&args[*i], "KEEPTTL") {
        *i += 1;
        return Ok(HashExpireMode::KeepTtl);
    }
    let (mult, absolute) = if eq_ignore_case(&args[*i], "EX") {
        (1000, false)
    } else if eq_ignore_case(&args[*i], "PX") {
        (1, false)
    } else if eq_ignore_case(&args[*i], "EXAT") {
        (1000, true)
    } else if eq_ignore_case(&args[*i], "PXAT") {
        (1, true)
    } else {
        return Ok(HashExpireMode::None);
    };
    let Some(n) = args.get(*i + 1).and_then(|b| parse_i64(b)) else {
        return Err(Reply::not_int());
    };
    *i += 2;
    Ok(HashExpireMode::Deadline(deadline_from(n, mult, absolute)))
}

pub async fn hgetex(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 4 {
        return Reply::wrong_args("hgetex");
    }
    let mut i = 2;
    let mode = match parse_hash_expiration(args, &mut i, true, false) {
        Ok(m) => m,
        Err(r) => return r,
    };
    let fields = match parse_fields_block(args, i) {
        Ok(f) => f,
        Err(r) => return r,
    };
    let key = args[1].clone();
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = hash_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            Reply::Array(
                fields
                    .iter()
                    .map(|f| {
                        let Some((env, value)) = read_field_env(ctx, &key, f, del) else {
                            return Reply::Null;
                        };
                        match mode {
                            HashExpireMode::Deadline(d) => {
                                set_field_deadline(ctx, &key, f, del, d, ExpireCond::None);
                            }
                            HashExpireMode::Persist => {
                                if env.ttl_deadline_ms != 0 {
                                    set_field_deadline(ctx, &key, f, del, 0, ExpireCond::None);
                                }
                            }
                            HashExpireMode::KeepTtl | HashExpireMode::None => {}
                        }
                        Reply::Bulk(value)
                    })
                    .collect(),
            )
        })
        .await
}

pub async fn hsetex(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 5 {
        return Reply::wrong_args("hsetex");
    }
    let key = args[1].clone();
    let mut i = 2;
    let mut fnx = false;
    let mut fxx = false;
    if args
        .get(i)
        .is_some_and(|a| eq_ignore_case(a, "FNX") || eq_ignore_case(a, "FXX"))
    {
        fnx = eq_ignore_case(&args[i], "FNX");
        fxx = eq_ignore_case(&args[i], "FXX");
        i += 1;
    }
    let mode = match parse_hash_expiration(args, &mut i, false, true) {
        Ok(m) => m,
        Err(r) => return r,
    };
    if args.get(i).is_none_or(|a| !eq_ignore_case(a, "FVS")) {
        return Reply::syntax();
    }
    let Some(n) = args.get(i + 1).and_then(|b| parse_u64(b)) else {
        return Reply::not_int();
    };
    let n = n as usize;
    if n == 0 || args.len() != i + 2 + n * 2 {
        return Reply::syntax();
    }
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = args[i + 2..]
        .chunks(2)
        .map(|c| (c[0].clone(), c[1].clone()))
        .collect();
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = hash_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            if fnx && pairs.iter().any(|(f, _)| field_exists(ctx, &key, f, del)) {
                return Reply::Int(0);
            }
            if fxx && pairs.iter().any(|(f, _)| !field_exists(ctx, &key, f, del)) {
                return Reply::Int(0);
            }
            ensure_head(ctx, &key, head::CTYPE_HASH);
            for (f, v) in &pairs {
                let ttl = match mode {
                    HashExpireMode::Deadline(d) => d,
                    HashExpireMode::KeepTtl => read_field_env(ctx, &key, f, del)
                        .map(|(env, _)| env.ttl_deadline_ms)
                        .unwrap_or(0),
                    HashExpireMode::Persist | HashExpireMode::None => 0,
                };
                if ttl != 0 && ttl <= now_ms() {
                    write_field_ttl(ctx, &key, f, v, ttl);
                    remove_field(ctx, &key, f, del);
                } else {
                    write_field_ttl(ctx, &key, f, v, ttl);
                }
            }
            Reply::Int(1)
        })
        .await
}

pub async fn hexists(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 3 {
        return Reply::wrong_args("hexists");
    }
    let (key, field) = (args[1].clone(), args[2].clone());
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = hash_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            Reply::Int(field_exists(ctx, &key, &field, del) as i64)
        })
        .await
}

pub async fn hlen(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 2 {
        return Reply::wrong_args("hlen");
    }
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = hash_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            Reply::Int(hash_entries(ctx, &key, del).len() as i64)
        })
        .await
}

pub async fn hkeys(engine: &Arc<Engine>, args: &[Vec<u8>], keys: bool) -> Reply {
    if args.len() != 2 {
        return Reply::wrong_args("hkeys");
    }
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = hash_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            Reply::Array(
                hash_entries(ctx, &key, del)
                    .into_iter()
                    .map(|(f, v)| Reply::Bulk(if keys { f } else { v }))
                    .collect(),
            )
        })
        .await
}

pub async fn hstrlen(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 3 {
        return Reply::wrong_args("hstrlen");
    }
    let (key, field) = (args[1].clone(), args[2].clone());
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = hash_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            Reply::Int(read_element_checked(ctx, &key, &field, del).map_or(0, |v| v.len() as i64))
        })
        .await
}

pub async fn hincrby(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 4 {
        return Reply::wrong_args("hincrby");
    }
    let Some(delta) = parse_i64(&args[3]) else {
        return Reply::not_int();
    };
    let (key, field) = (args[1].clone(), args[2].clone());
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = hash_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            let cur: i64 = match read_element_checked(ctx, &key, &field, del) {
                None => 0,
                Some(v) => match std::str::from_utf8(&v).ok().and_then(|s| s.parse().ok()) {
                    Some(n) => n,
                    None => return Reply::err("ERR hash value is not an integer"),
                },
            };
            let Some(new) = cur.checked_add(delta) else {
                return Reply::err("ERR increment or decrement would overflow");
            };
            ensure_head(ctx, &key, head::CTYPE_HASH);
            write_field(ctx, &key, &field, new.to_string().as_bytes());
            Reply::Int(new)
        })
        .await
}

pub async fn hincrbyfloat(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 4 {
        return Reply::wrong_args("hincrbyfloat");
    }
    let Some(delta) = parse_f64(&args[3]) else {
        return Reply::not_float();
    };
    let (key, field) = (args[1].clone(), args[2].clone());
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = hash_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            let cur: f64 = match read_element_checked(ctx, &key, &field, del) {
                None => 0.0,
                Some(v) => match std::str::from_utf8(&v).ok().and_then(|s| s.parse().ok()) {
                    Some(n) => n,
                    None => return Reply::err("ERR hash value is not a float"),
                },
            };
            let new = cur + delta;
            if new.is_nan() || new.is_infinite() {
                return Reply::err("ERR increment would produce NaN or Infinity");
            }
            ensure_head(ctx, &key, head::CTYPE_HASH);
            let s = fmt_f64(new);
            write_field(ctx, &key, &field, s.as_bytes());
            Reply::Bulk(s.into_bytes())
        })
        .await
}

pub async fn hrandfield(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 2 || args.len() > 4 {
        return Reply::wrong_args("hrandfield");
    }
    let key = args[1].clone();
    let count = args.get(2).and_then(|b| parse_i64(b));
    let withvalues = args
        .get(3)
        .map(|a| eq_ignore_case(a, "WITHVALUES"))
        .unwrap_or(false);
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = hash_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            let entries = hash_entries(ctx, &key, del);
            if entries.is_empty() {
                return match count {
                    None => Reply::Null,
                    Some(_) => Reply::Array(vec![]),
                };
            }
            // Pseudo-random pick via HLC low bits (no rand dep on hot path).
            let seed = ctx.hlc.now() as usize;
            match count {
                None => Reply::Bulk(entries[seed % entries.len()].0.clone()),
                Some(n) => {
                    let n = n.unsigned_abs() as usize;
                    let mut out = Vec::new();
                    for i in 0..n.min(entries.len()) {
                        let (f, v) = &entries[(seed + i) % entries.len()];
                        out.push(Reply::Bulk(f.clone()));
                        if withvalues {
                            out.push(Reply::Bulk(v.clone()));
                        }
                    }
                    Reply::Array(out)
                }
            }
        })
        .await
}

pub async fn hscan(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 3 {
        return Reply::wrong_args("hscan");
    }
    let key = args[1].clone();
    let mut pattern: Option<Vec<u8>> = None;
    let mut novalues = false;
    let mut i = 3;
    while i < args.len() {
        if eq_ignore_case(&args[i], "MATCH") {
            pattern = args.get(i + 1).cloned();
            i += 2;
        } else if eq_ignore_case(&args[i], "COUNT") {
            let _ = args.get(i + 1).and_then(|b| parse_u64(b));
            i += 2;
        } else if eq_ignore_case(&args[i], "NOVALUES") {
            novalues = true;
            i += 1;
        } else {
            return Reply::syntax();
        }
    }
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = hash_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            // v1: single-pass full scan (cursor always returns 0).
            let mut items = Vec::new();
            for (f, v) in hash_entries(ctx, &key, del) {
                if pattern
                    .as_deref()
                    .is_none_or(|p| crate::pubsub::glob_match(p, &f))
                {
                    items.push(Reply::Bulk(f));
                    if !novalues {
                        items.push(Reply::Bulk(v));
                    }
                }
            }
            Reply::Array(vec![Reply::Bulk(b"0".to_vec()), Reply::Array(items)])
        })
        .await
}

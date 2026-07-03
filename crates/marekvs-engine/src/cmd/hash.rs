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
use marekvs_core::merge::{element_add, element_dots, element_remove, element_value};

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

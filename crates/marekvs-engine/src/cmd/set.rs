//! Set family. Members are OR elements with empty values — pure dot sets,
//! the canonical ORSWOT-lite case (design/02).

use std::collections::HashSet;
use std::sync::Arc;

use crate::cmd::{eq_ignore_case, parse_i64, parse_u64};
use crate::pubsub::glob_match;
use crate::reply::Reply;
use crate::store::{
    check_type, ensure_head, get_raw, now_ms, scan_prefix, visible, write_merged, ShardCtx,
};
use crate::Engine;
use marekvs_core::envelope::{head, Envelope, RecordType};
use marekvs_core::ikey;
use marekvs_core::merge::{element_add, element_dots, element_remove, element_value};

pub(crate) fn set_del_hlc(ctx: &ShardCtx, key: &[u8]) -> Result<u64, ()> {
    check_type(ctx, key, head::CTYPE_SET)
}

/// Visible members in key order, stopping after `limit` hits. The early
/// stop keeps SPOP/SRANDMEMBER O(count) instead of O(set) — pop-heavy
/// workloads on one big set otherwise stall the shard queue behind full
/// scans (found by the KeyDB benchmark harness).
pub(crate) fn set_members_limited(
    ctx: &ShardCtx,
    key: &[u8],
    del: u64,
    limit: usize,
) -> Vec<Vec<u8>> {
    let now = now_ms();
    let mut out = Vec::new();
    if limit == 0 {
        return out;
    }
    scan_prefix(
        ctx,
        &ikey::collection_prefix(ikey::Tag::SetMember, key),
        |k, v| {
            if let (Some(p), Some((env, pay))) = (ikey::parse(k), Envelope::decode(v)) {
                if visible(&env, pay, del, now).is_some() && element_value(pay).is_some() {
                    out.push(p.suffix.to_vec());
                    if out.len() >= limit {
                        return false;
                    }
                }
            }
            true
        },
    );
    out
}

pub(crate) fn set_members(ctx: &ShardCtx, key: &[u8], del: u64) -> Vec<Vec<u8>> {
    set_members_limited(ctx, key, del, usize::MAX)
}

/// Pop-shaped read: first `limit` visible members using the shard's
/// pop-front cursor to seek past the tombstone prefix that pops accumulate
/// (LSM queue anti-pattern; design/09 "Measured findings"). On a miss from
/// the hinted position, wraps around once from the prefix start so members
/// that sort before the cursor (later re-adds) are never lost.
fn pop_candidates(ctx: &ShardCtx, key: &[u8], del: u64, limit: usize) -> Vec<Vec<u8>> {
    let prefix = ikey::collection_prefix(ikey::Tag::SetMember, key);
    let now = now_ms();
    let mut out = Vec::new();
    let mut last_visited: Option<Vec<u8>> = None;
    let mut collect = |k: &[u8], v: &[u8], out: &mut Vec<Vec<u8>>| -> bool {
        last_visited = Some(k.to_vec());
        if let (Some(p), Some((env, pay))) = (ikey::parse(k), Envelope::decode(v)) {
            if visible(&env, pay, del, now).is_some() && element_value(pay).is_some() {
                out.push(p.suffix.to_vec());
            }
        }
        out.len() < limit
    };
    match crate::store::get_pop_hint(ctx, &prefix) {
        Some(crate::store::PopHint::Empty) => return Vec::new(), // known drained
        Some(crate::store::PopHint::At(hint)) => {
            crate::store::scan_from(ctx, &hint, &prefix, |k, v| collect(k, v, &mut out));
            if out.is_empty() {
                // Dead segment from hint to end: rescan from the start.
                crate::store::clear_pop_hint(ctx, &prefix);
                crate::store::scan_prefix(ctx, &prefix, |k, v| collect(k, v, &mut out));
            }
        }
        None => {
            crate::store::scan_prefix(ctx, &prefix, |k, v| collect(k, v, &mut out));
        }
    }
    if out.is_empty() {
        // Full rescan came up dry: mark drained so pops on an empty
        // collection stop paying full dead-record scans.
        crate::store::set_pop_hint_empty(ctx, &prefix);
    } else if let Some(lk) = last_visited {
        crate::store::set_pop_hint(ctx, &prefix, &lk);
    }
    out
}

fn member_exists(ctx: &ShardCtx, key: &[u8], member: &[u8], del: u64) -> bool {
    let Some(v) = get_raw(ctx, &ikey::set_member_key(key, member)) else {
        return false;
    };
    let Some((env, pay)) = Envelope::decode(&v) else {
        return false;
    };
    visible(&env, pay, del, now_ms()).is_some() && element_value(pay).is_some()
}

pub(crate) fn add_member(ctx: &ShardCtx, key: &[u8], member: &[u8]) {
    let rec = element_add(RecordType::SetMember, ctx.hlc.now(), ctx.node_id, &[]);
    // Pop-cursor invalidation happens centrally in write_merged (it must
    // also fire for replicated/AE/bootstrap member writes).
    write_merged(ctx, &ikey::set_member_key(key, member), &rec);
}

pub(crate) fn remove_member(ctx: &ShardCtx, key: &[u8], member: &[u8], del: u64) -> bool {
    let ik = ikey::set_member_key(key, member);
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
    let rm = element_remove(RecordType::SetMember, ctx.hlc.now(), ctx.node_id, &dots);
    write_merged(ctx, &ik, &rm);
    true
}

pub async fn sadd(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 3 {
        return Reply::wrong_args("sadd");
    }
    let key = args[1].clone();
    let members: Vec<Vec<u8>> = args[2..].to_vec();
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = set_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            ensure_head(ctx, &key, head::CTYPE_SET);
            let mut added = 0;
            for m in &members {
                if !member_exists(ctx, &key, m, del) {
                    added += 1;
                }
                add_member(ctx, &key, m);
            }
            Reply::Int(added)
        })
        .await
}

pub async fn srem(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 3 {
        return Reply::wrong_args("srem");
    }
    let key = args[1].clone();
    let members: Vec<Vec<u8>> = args[2..].to_vec();
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = set_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            let mut n = 0;
            for m in &members {
                if remove_member(ctx, &key, m, del) {
                    n += 1;
                }
            }
            Reply::Int(n)
        })
        .await
}

pub async fn scard(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 2 {
        return Reply::wrong_args("scard");
    }
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = set_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            Reply::Int(set_members(ctx, &key, del).len() as i64)
        })
        .await
}

pub async fn sismember(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 3 {
        return Reply::wrong_args("sismember");
    }
    let (key, member) = (args[1].clone(), args[2].clone());
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = set_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            Reply::Int(member_exists(ctx, &key, &member, del) as i64)
        })
        .await
}

pub async fn smismember(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 3 {
        return Reply::wrong_args("smismember");
    }
    let key = args[1].clone();
    let members: Vec<Vec<u8>> = args[2..].to_vec();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = set_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            Reply::Array(
                members
                    .iter()
                    .map(|m| Reply::Int(member_exists(ctx, &key, m, del) as i64))
                    .collect(),
            )
        })
        .await
}

pub async fn smembers(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 2 {
        return Reply::wrong_args("smembers");
    }
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = set_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            Reply::Set(
                set_members(ctx, &key, del)
                    .into_iter()
                    .map(Reply::Bulk)
                    .collect(),
            )
        })
        .await
}

pub async fn spop(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 2 || args.len() > 3 {
        return Reply::wrong_args("spop");
    }
    let key = args[1].clone();
    let count = match args.get(2) {
        None => None,
        Some(b) => match parse_u64(b) {
            Some(n) => Some(n as usize),
            None => return Reply::not_int(),
        },
    };
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = set_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            // Hinted early-stop scan: pops the first `n` members from the
            // pop-front cursor rather than a random pick — key order over
            // member bytes is effectively arbitrary, and O(count) beats
            // O(set) (Redis makes no distribution promise for SPOP either).
            let victims = pop_candidates(ctx, &key, del, count.unwrap_or(1));
            if victims.is_empty() {
                return match count {
                    None => Reply::Null,
                    Some(_) => Reply::Set(vec![]),
                };
            }
            let mut popped = Vec::with_capacity(victims.len());
            for m in victims {
                if remove_member(ctx, &key, &m, del) {
                    popped.push(m);
                }
            }
            match count {
                None => popped.pop().map_or(Reply::Null, Reply::Bulk),
                Some(_) => Reply::Set(popped.into_iter().map(Reply::Bulk).collect()),
            }
        })
        .await
}

pub async fn srandmember(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 2 || args.len() > 3 {
        return Reply::wrong_args("srandmember");
    }
    let key = args[1].clone();
    let count = args.get(2).and_then(|b| parse_i64(b));
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = set_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            // Early-stop scan (see spop): first-N in key order, O(count).
            // Negative count may repeat members (Redis semantics) — cycle
            // through the fetched window.
            let want = match count {
                None => 1,
                Some(n) => n.unsigned_abs() as usize,
            };
            let members = set_members_limited(ctx, &key, del, want.max(1));
            if members.is_empty() {
                return match count {
                    None => Reply::Null,
                    Some(_) => Reply::Array(vec![]),
                };
            }
            match count {
                None => Reply::Bulk(members[0].clone()),
                Some(n) => {
                    let take = if n < 0 { want } else { want.min(members.len()) };
                    let mut out = Vec::with_capacity(take);
                    for i in 0..take {
                        out.push(Reply::Bulk(members[i % members.len()].clone()));
                    }
                    Reply::Array(out)
                }
            }
        })
        .await
}

pub async fn sscan(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 3 {
        return Reply::wrong_args("sscan");
    }
    let key = args[1].clone();
    let mut pattern: Option<Vec<u8>> = None;
    let mut i = 3;
    while i < args.len() {
        if eq_ignore_case(&args[i], "MATCH") {
            pattern = args.get(i + 1).cloned();
            i += 2;
        } else if eq_ignore_case(&args[i], "COUNT") {
            i += 2;
        } else {
            return Reply::syntax();
        }
    }
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = set_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            let items: Vec<Reply> = set_members(ctx, &key, del)
                .into_iter()
                .filter(|m| pattern.as_deref().is_none_or(|p| glob_match(p, m)))
                .map(Reply::Bulk)
                .collect();
            Reply::Array(vec![Reply::Bulk(b"0".to_vec()), Reply::Array(items)])
        })
        .await
}

pub async fn smove(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 4 {
        return Reply::wrong_args("smove");
    }
    let (src, dst, member) = (args[1].clone(), args[2].clone(), args[3].clone());
    // Remove from src on its shard, then add to dst on its shard.
    let (s, m) = (src.clone(), member.clone());
    let removed = engine
        .store
        .run_key(&src, move |ctx| {
            let Ok(del) = set_del_hlc(ctx, &s) else {
                return None;
            };
            Some(remove_member(ctx, &s, &m, del))
        })
        .await;
    match removed {
        None => Reply::wrongtype(),
        Some(false) => Reply::Int(0),
        Some(true) => {
            let (d, m) = (dst.clone(), member.clone());
            engine
                .store
                .run_key(&dst, move |ctx| {
                    if set_del_hlc(ctx, &d).is_err() {
                        return Reply::wrongtype();
                    }
                    ensure_head(ctx, &d, head::CTYPE_SET);
                    add_member(ctx, &d, &m);
                    Reply::Int(1)
                })
                .await
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
pub enum SetOp {
    Union,
    Inter,
    Diff,
}

async fn members_of(engine: &Arc<Engine>, key: &[u8]) -> Result<Vec<Vec<u8>>, ()> {
    let k = key.to_vec();
    engine.ensure_local(key).await;
    engine
        .store
        .run_key(key, move |ctx| {
            let del = set_del_hlc(ctx, &k)?;
            Ok(set_members(ctx, &k, del))
        })
        .await
}

pub async fn setop(engine: &Arc<Engine>, args: &[Vec<u8>], op: SetOp, store_dst: bool) -> Reply {
    let min = if store_dst { 3 } else { 2 };
    if args.len() < min {
        return Reply::wrong_args("sunion");
    }
    let keys = if store_dst { &args[2..] } else { &args[1..] };

    let mut acc: Option<Vec<Vec<u8>>> = None;
    for k in keys {
        let members = match members_of(engine, k).await {
            Ok(m) => m,
            Err(()) => return Reply::wrongtype(),
        };
        acc = Some(match acc {
            None => members,
            Some(cur) => {
                let set: HashSet<Vec<u8>> = members.into_iter().collect();
                match op {
                    SetOp::Union => {
                        let mut cur = cur;
                        for m in set {
                            if !cur.contains(&m) {
                                cur.push(m);
                            }
                        }
                        cur
                    }
                    SetOp::Inter => cur.into_iter().filter(|m| set.contains(m)).collect(),
                    SetOp::Diff => cur.into_iter().filter(|m| !set.contains(m)).collect(),
                }
            }
        });
    }
    let result = acc.unwrap_or_default();

    if store_dst {
        let dst = args[1].clone();
        let res = result.clone();
        engine
            .store
            .run_key(&args[1], move |ctx| {
                let Ok(del) = set_del_hlc(ctx, &dst) else {
                    return Reply::wrongtype();
                };
                // Clear existing members, then add the result set.
                for m in set_members(ctx, &dst, del) {
                    remove_member(ctx, &dst, &m, del);
                }
                if !res.is_empty() {
                    ensure_head(ctx, &dst, head::CTYPE_SET);
                }
                for m in &res {
                    add_member(ctx, &dst, m);
                }
                Reply::Int(res.len() as i64)
            })
            .await
    } else {
        Reply::Set(result.into_iter().map(Reply::Bulk).collect())
    }
}

pub async fn sintercard(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 3 {
        return Reply::wrong_args("sintercard");
    }
    let Some(numkeys) = parse_u64(&args[1]) else {
        return Reply::not_int();
    };
    let numkeys = numkeys as usize;
    if numkeys == 0 || args.len() < 2 + numkeys {
        return Reply::syntax();
    }
    let mut limit = usize::MAX;
    if args.len() > 2 + numkeys {
        if !eq_ignore_case(&args[2 + numkeys], "LIMIT") {
            return Reply::syntax();
        }
        match args.get(3 + numkeys).and_then(|b| parse_u64(b)) {
            Some(0) => limit = usize::MAX,
            Some(n) => limit = n as usize,
            None => return Reply::not_int(),
        }
    }
    let mut acc: Option<HashSet<Vec<u8>>> = None;
    for k in &args[2..2 + numkeys] {
        let members = match members_of(engine, k).await {
            Ok(m) => m,
            Err(()) => return Reply::wrongtype(),
        };
        let set: HashSet<Vec<u8>> = members.into_iter().collect();
        acc = Some(match acc {
            None => set,
            Some(cur) => cur.intersection(&set).cloned().collect(),
        });
    }
    Reply::Int(acc.map_or(0, |s| s.len().min(limit)) as i64)
}

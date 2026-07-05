//! Sorted-set family (design/02 §zset score index, design/03 §Sorted sets).
//!
//! A member is an OR element at `zset_member_key`; its element value is the
//! 8-byte big-endian raw bits of the score (`f64`). A *second*, node-local and
//! never-replicated key — the score index at `zset_score_key` — carries an
//! empty value and exists only so range/rank queries become an ordered prefix
//! walk. The index is maintained transactionally next to every member change;
//! range reads still re-verify each member record, so a dangling index entry
//! (possible after a crash) is tolerated, not trusted.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use crate::cmd::{eq_ignore_case, fmt_f64, parse_f64, parse_i64};
use crate::pubsub::glob_match;
use crate::reply::Reply;
use crate::store::{
    check_type, del_raw, ensure_head, get_raw, now_ms, scan_prefix, visible, write_merged, ShardCtx,
};
use crate::Engine;
use marekvs_core::envelope::{head, Envelope, RecordType};
use marekvs_core::ikey::{self, Tag};
use marekvs_core::merge::{element_add, element_dots, element_remove, element_value};
use marekvs_core::score::encode_score;

fn zset_del_hlc(ctx: &ShardCtx, key: &[u8]) -> Result<u64, ()> {
    check_type(ctx, key, head::CTYPE_ZSET)
}

/// Decode the score carried by a full member record's bytes (envelope +
/// element payload). `None` when the element is dead or malformed.
fn score_from_record(bytes: &[u8]) -> Option<f64> {
    let (_, pay) = Envelope::decode(bytes)?;
    let val = element_value(pay)?;
    if val.len() == 8 {
        Some(f64::from_be_bytes(val[..8].try_into().unwrap()))
    } else {
        None
    }
}

/// Current visible score of a member, respecting the collection delete clock.
fn member_score(ctx: &ShardCtx, key: &[u8], member: &[u8], del: u64) -> Option<f64> {
    let v = get_raw(ctx, &ikey::zset_member_key(key, member))?;
    let (env, pay) = Envelope::decode(&v)?;
    visible(&env, pay, del, now_ms())?;
    let val = element_value(pay)?;
    if val.len() == 8 {
        Some(f64::from_be_bytes(val[..8].try_into().unwrap()))
    } else {
        None
    }
}

/// Write an empty node-local index key (no envelope, no ondaDB TTL).
fn put_index(ctx: &ShardCtx, idx_key: &[u8]) {
    if let Err(e) = ctx.db.put(&ctx.data, idx_key, &[], Duration::ZERO) {
        tracing::error!(?e, "zset score-index put failed");
    }
    // ZPOPMIN pop-front cursor maintenance: an entry inserted BELOW the
    // cursor would be missed until wraparound, and ZPOPMIN must return the
    // true minimum — rewind. Also clears a known-drained marker.
    if let Some(p) = ikey::parse(idx_key) {
        let prefix = ikey::collection_prefix(Tag::ZsetScore, p.userkey);
        crate::store::pop_hint_on_insert(ctx, &prefix, idx_key);
    }
}

/// Merge a member record (local write or replication apply) and keep the
/// score index consistent. Returns true if the stored record changed.
pub fn apply_member_record(ctx: &ShardCtx, userkey: &[u8], member: &[u8], incoming: &[u8]) -> bool {
    let mk = ikey::zset_member_key(userkey, member);
    let old_score = get_raw(ctx, &mk).as_deref().and_then(score_from_record);
    let changed = write_merged(ctx, &mk, incoming);
    let new_score = get_raw(ctx, &mk).as_deref().and_then(score_from_record);
    if old_score != new_score {
        if let Some(s) = old_score {
            del_raw(ctx, &ikey::zset_score_key(userkey, encode_score(s), member));
        }
        if let Some(s) = new_score {
            put_index(ctx, &ikey::zset_score_key(userkey, encode_score(s), member));
        }
    }
    changed
}

/// Local ZADD-style write of one member at `score`.
fn write_member(ctx: &ShardCtx, key: &[u8], member: &[u8], score: f64) {
    let rec = element_add(
        RecordType::ZsetMember,
        ctx.hlc.now(),
        ctx.node_id,
        &score.to_be_bytes(),
    );
    apply_member_record(ctx, key, member, &rec);
}

/// Remove a member (observed-remove) and drop its score-index entry.
fn remove_member(ctx: &ShardCtx, key: &[u8], member: &[u8], del: u64) -> bool {
    let mk = ikey::zset_member_key(key, member);
    let Some(v) = get_raw(ctx, &mk) else {
        return false;
    };
    let Some((env, pay)) = Envelope::decode(&v) else {
        return false;
    };
    if visible(&env, pay, del, now_ms()).is_none() || element_value(pay).is_none() {
        return false;
    }
    let old_score = score_from_record(&v);
    let dots = element_dots(pay);
    let rm = element_remove(RecordType::ZsetMember, ctx.hlc.now(), ctx.node_id, &dots);
    write_merged(ctx, &mk, &rm);
    if let Some(s) = old_score {
        del_raw(ctx, &ikey::zset_score_key(key, encode_score(s), member));
    }
    true
}

/// Visible `(score, member)` pairs in ascending score order (member
/// lexicographic within equal scores), walking the score index. Stops after
/// `limit` hits — the early stop keeps ZPOPMIN O(count) instead of O(zset);
/// pop-heavy workloads on one big zset otherwise stall the whole shard queue
/// behind full-set scans (found by the KeyDB benchmark harness).
fn scored_members_limited(
    ctx: &ShardCtx,
    key: &[u8],
    del: u64,
    limit: usize,
) -> Vec<(f64, Vec<u8>)> {
    let mut out = Vec::new();
    if limit == 0 {
        return out;
    }
    scan_prefix(
        ctx,
        &ikey::collection_prefix(Tag::ZsetScore, key),
        |k, _v| {
            if let Some(p) = ikey::parse(k) {
                if p.suffix.len() >= 8 {
                    let enc = u64::from_be_bytes(p.suffix[..8].try_into().unwrap());
                    let member = &p.suffix[8..];
                    // Trust but verify: the index can dangle after a crash.
                    if let Some(sc) = member_score(ctx, key, member, del) {
                        if encode_score(sc) == enc {
                            out.push((sc, member.to_vec()));
                            if out.len() >= limit {
                                return false;
                            }
                        }
                    }
                }
            }
            true
        },
    );
    out
}

/// All visible `(score, member)` pairs (full walk).
fn scored_members(ctx: &ShardCtx, key: &[u8], del: u64) -> Vec<(f64, Vec<u8>)> {
    scored_members_limited(ctx, key, del, usize::MAX)
}

/// Pop-shaped read over the score index with the shard's pop-front cursor:
/// ZPOPMIN removals leave ondadb delete-tombstones at the low end of the
/// index that every fresh scan re-skips (LSM queue anti-pattern). Seek from
/// the cursor; on a miss, wrap once from the prefix start (entries can sort
/// before the cursor after later ZADDs with lower scores).
fn pop_scored_candidates(
    ctx: &ShardCtx,
    key: &[u8],
    del: u64,
    limit: usize,
) -> Vec<(f64, Vec<u8>)> {
    let prefix = ikey::collection_prefix(Tag::ZsetScore, key);
    let mut out = Vec::new();
    let mut last_visited: Option<Vec<u8>> = None;
    let mut collect = |k: &[u8], _v: &[u8], out: &mut Vec<(f64, Vec<u8>)>| -> bool {
        last_visited = Some(k.to_vec());
        if let Some(p) = ikey::parse(k) {
            if p.suffix.len() >= 8 {
                let enc = u64::from_be_bytes(p.suffix[..8].try_into().unwrap());
                let member = &p.suffix[8..];
                if let Some(sc) = member_score(ctx, key, member, del) {
                    if encode_score(sc) == enc {
                        out.push((sc, member.to_vec()));
                    }
                }
            }
        }
        out.len() < limit
    };
    match crate::store::get_pop_hint(ctx, &prefix) {
        Some(crate::store::PopHint::Empty) => return Vec::new(), // known drained
        Some(crate::store::PopHint::At(hint)) => {
            crate::store::scan_from(ctx, &hint, &prefix, |k, v| collect(k, v, &mut out));
            if out.is_empty() {
                crate::store::clear_pop_hint(ctx, &prefix);
                crate::store::scan_prefix(ctx, &prefix, |k, v| collect(k, v, &mut out));
            }
        }
        None => {
            crate::store::scan_prefix(ctx, &prefix, |k, v| collect(k, v, &mut out));
        }
    }
    if out.is_empty() {
        crate::store::set_pop_hint_empty(ctx, &prefix);
    } else if let Some(lk) = last_visited {
        crate::store::set_pop_hint(ctx, &prefix, &lk);
    }
    out
}

/// Count visible members via the member-key prefix (avoids the per-entry
/// re-read the score index would need).
fn zset_card(ctx: &ShardCtx, key: &[u8], del: u64) -> i64 {
    let now = now_ms();
    let mut n = 0i64;
    scan_prefix(
        ctx,
        &ikey::collection_prefix(Tag::ZsetMember, key),
        |_k, v| {
            if let Some((env, pay)) = Envelope::decode(v) {
                if visible(&env, pay, del, now).is_some() && element_value(pay).is_some() {
                    n += 1;
                }
            }
            true
        },
    );
    n
}

// ---------------------------------------------------------------------------
// score-bound / reply helpers
// ---------------------------------------------------------------------------

/// Parse a ZRANGEBYSCORE bound: `5`, `(5` (exclusive), `-inf`, `+inf`.
/// Returns `(value, exclusive)`.
fn parse_score_bound(b: &[u8]) -> Option<(f64, bool)> {
    if b.first() == Some(&b'(') {
        Some((parse_f64(&b[1..])?, true))
    } else {
        Some((parse_f64(b)?, false))
    }
}

fn in_range(score: f64, min: (f64, bool), max: (f64, bool)) -> bool {
    let lo = if min.1 { score > min.0 } else { score >= min.0 };
    let hi = if max.1 { score < max.0 } else { score <= max.0 };
    lo && hi
}

fn parse_lex_bound(b: &[u8], is_min: bool) -> Option<Option<(Vec<u8>, bool)>> {
    if b == b"-" && is_min {
        return Some(None);
    }
    if b == b"+" && !is_min {
        return Some(None);
    }
    match b.first()? {
        b'(' => Some(Some((b[1..].to_vec(), true))),
        b'[' => Some(Some((b[1..].to_vec(), false))),
        _ => None,
    }
}

fn in_lex(member: &[u8], min: Option<(Vec<u8>, bool)>, max: Option<(Vec<u8>, bool)>) -> bool {
    let lo = match min {
        None => true,
        Some((ref b, excl)) if excl => member > b.as_slice(),
        Some((ref b, _)) => member >= b.as_slice(),
    };
    let hi = match max {
        None => true,
        Some((ref b, excl)) if excl => member < b.as_slice(),
        Some((ref b, _)) => member <= b.as_slice(),
    };
    lo && hi
}

fn apply_limit(v: Vec<(f64, Vec<u8>)>, limit: Option<(i64, i64)>) -> Vec<(f64, Vec<u8>)> {
    match limit {
        None => v,
        Some((off, cnt)) => {
            let off = off.max(0) as usize;
            if off >= v.len() {
                return Vec::new();
            }
            let it = v.into_iter().skip(off);
            if cnt < 0 {
                it.collect()
            } else {
                it.take(cnt as usize).collect()
            }
        }
    }
}

fn emit(items: Vec<(f64, Vec<u8>)>, withscores: bool) -> Reply {
    let mut out = Vec::with_capacity(items.len() * if withscores { 2 } else { 1 });
    for (s, m) in items {
        out.push(Reply::Bulk(m));
        if withscores {
            out.push(Reply::Double(s));
        }
    }
    Reply::Array(out)
}

// ---------------------------------------------------------------------------
// commands
// ---------------------------------------------------------------------------

pub async fn zadd(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 4 {
        return Reply::wrong_args("zadd");
    }
    let key = args[1].clone();
    let (mut nx, mut xx, mut gt, mut lt, mut ch, mut incr) =
        (false, false, false, false, false, false);
    let mut i = 2;
    while i < args.len() {
        let a = &args[i];
        if eq_ignore_case(a, "NX") {
            nx = true;
        } else if eq_ignore_case(a, "XX") {
            xx = true;
        } else if eq_ignore_case(a, "GT") {
            gt = true;
        } else if eq_ignore_case(a, "LT") {
            lt = true;
        } else if eq_ignore_case(a, "CH") {
            ch = true;
        } else if eq_ignore_case(a, "INCR") {
            incr = true;
        } else {
            break;
        }
        i += 1;
    }
    if (nx && xx) || (gt && lt) || (nx && (gt || lt)) {
        return Reply::err("ERR GT, LT, and/or NX options at the same time are not compatible");
    }
    let rest = &args[i..];
    if rest.is_empty() || rest.len() % 2 != 0 {
        return Reply::syntax();
    }
    if incr && rest.len() != 2 {
        return Reply::err("ERR INCR option supports a single increment-element pair");
    }
    // Parse+validate every score up front.
    let mut pairs: Vec<(f64, Vec<u8>)> = Vec::with_capacity(rest.len() / 2);
    for c in rest.chunks(2) {
        let Some(score) = parse_f64(&c[0]) else {
            return Reply::not_float();
        };
        pairs.push((score, c[1].clone()));
    }

    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = zset_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            let mut added = 0i64;
            let mut updated = 0i64;
            let mut incr_result: Option<f64> = None;
            for (score, member) in &pairs {
                match member_score(ctx, &key, member, del) {
                    Some(cur) => {
                        if nx {
                            if incr {
                                return Reply::Null;
                            }
                            continue;
                        }
                        let newscore = if incr { cur + *score } else { *score };
                        if incr && newscore.is_nan() {
                            return Reply::err("ERR resulting score is not a number (NaN)");
                        }
                        if (gt && newscore <= cur) || (lt && newscore >= cur) {
                            if incr {
                                return Reply::Null;
                            }
                            continue;
                        }
                        if newscore != cur {
                            ensure_head(ctx, &key, head::CTYPE_ZSET);
                            write_member(ctx, &key, member, newscore);
                            updated += 1;
                        }
                        incr_result = Some(newscore);
                    }
                    None => {
                        if xx {
                            if incr {
                                return Reply::Null;
                            }
                            continue;
                        }
                        let newscore = *score;
                        ensure_head(ctx, &key, head::CTYPE_ZSET);
                        write_member(ctx, &key, member, newscore);
                        added += 1;
                        incr_result = Some(newscore);
                    }
                }
            }
            if incr {
                incr_result.map_or(Reply::Null, Reply::Double)
            } else if ch {
                Reply::Int(added + updated)
            } else {
                Reply::Int(added)
            }
        })
        .await
}

pub async fn zscore(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 3 {
        return Reply::wrong_args("zscore");
    }
    let (key, member) = (args[1].clone(), args[2].clone());
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = zset_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            member_score(ctx, &key, &member, del).map_or(Reply::Null, Reply::Double)
        })
        .await
}

pub async fn zmscore(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 3 {
        return Reply::wrong_args("zmscore");
    }
    let key = args[1].clone();
    let members: Vec<Vec<u8>> = args[2..].to_vec();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = zset_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            Reply::Array(
                members
                    .iter()
                    .map(|m| member_score(ctx, &key, m, del).map_or(Reply::Null, Reply::Double))
                    .collect(),
            )
        })
        .await
}

pub async fn zcard(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 2 {
        return Reply::wrong_args("zcard");
    }
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = zset_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            Reply::Int(zset_card(ctx, &key, del))
        })
        .await
}

pub async fn zincrby(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 4 {
        return Reply::wrong_args("zincrby");
    }
    let Some(delta) = parse_f64(&args[2]) else {
        return Reply::not_float();
    };
    let (key, member) = (args[1].clone(), args[3].clone());
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = zset_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            let cur = member_score(ctx, &key, &member, del).unwrap_or(0.0);
            let newscore = cur + delta;
            if newscore.is_nan() {
                return Reply::err("ERR resulting score is not a number (NaN)");
            }
            ensure_head(ctx, &key, head::CTYPE_ZSET);
            write_member(ctx, &key, &member, newscore);
            Reply::Double(newscore)
        })
        .await
}

pub async fn zrem(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 3 {
        return Reply::wrong_args("zrem");
    }
    let key = args[1].clone();
    let members: Vec<Vec<u8>> = args[2..].to_vec();
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = zset_del_hlc(ctx, &key) else {
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

pub async fn zrange(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 4 {
        return Reply::wrong_args("zrange");
    }
    let key = args[1].clone();
    let a2 = args[2].clone();
    let a3 = args[3].clone();
    let mut byscore = false;
    let mut bylex = false;
    let mut rev = false;
    let mut withscores = false;
    let mut limit: Option<(i64, i64)> = None;
    let mut i = 4;
    while i < args.len() {
        if eq_ignore_case(&args[i], "BYSCORE") {
            byscore = true;
            i += 1;
        } else if eq_ignore_case(&args[i], "BYLEX") {
            bylex = true;
            i += 1;
        } else if eq_ignore_case(&args[i], "REV") {
            rev = true;
            i += 1;
        } else if eq_ignore_case(&args[i], "WITHSCORES") {
            withscores = true;
            i += 1;
        } else if eq_ignore_case(&args[i], "LIMIT") {
            let (Some(off), Some(cnt)) = (
                args.get(i + 1).and_then(|b| parse_i64(b)),
                args.get(i + 2).and_then(|b| parse_i64(b)),
            ) else {
                return Reply::not_int();
            };
            limit = Some((off, cnt));
            i += 3;
        } else {
            return Reply::syntax();
        }
    }
    if limit.is_some() && !byscore {
        if !bylex {
            return Reply::err(
                "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX",
            );
        }
    }

    if bylex {
        let (min_arg, max_arg) = if rev { (a3, a2) } else { (a2, a3) };
        let (Some(min), Some(max)) = (
            parse_lex_bound(&min_arg, true),
            parse_lex_bound(&max_arg, false),
        ) else {
            return Reply::syntax();
        };
        return engine
            .store
            .run_key(&args[1], move |ctx| {
                let Ok(del) = zset_del_hlc(ctx, &key) else {
                    return Reply::wrongtype();
                };
                let mut items: Vec<(f64, Vec<u8>)> = scored_members(ctx, &key, del)
                    .into_iter()
                    .filter(|(_, m)| in_lex(m, min.clone(), max.clone()))
                    .collect();
                items.sort_by(|a, b| a.1.cmp(&b.1));
                if rev {
                    items.reverse();
                }
                emit(apply_limit(items, limit), withscores)
            })
            .await;
    }

    if byscore {
        // REV swaps the order of the bound arguments (max first, then min).
        let (min_arg, max_arg) = if rev { (a3, a2) } else { (a2, a3) };
        let (Some(min), Some(max)) = (parse_score_bound(&min_arg), parse_score_bound(&max_arg))
        else {
            return Reply::err("ERR min or max is not a float");
        };
        return engine
            .store
            .run_key(&args[1], move |ctx| {
                let Ok(del) = zset_del_hlc(ctx, &key) else {
                    return Reply::wrongtype();
                };
                let mut items: Vec<(f64, Vec<u8>)> = scored_members(ctx, &key, del)
                    .into_iter()
                    .filter(|(s, _)| in_range(*s, min, max))
                    .collect();
                if rev {
                    items.reverse();
                }
                emit(apply_limit(items, limit), withscores)
            })
            .await;
    }

    // Index form.
    let (Some(start), Some(stop)) = (parse_i64(&a2), parse_i64(&a3)) else {
        return Reply::not_int();
    };
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = zset_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            let mut all = scored_members(ctx, &key, del);
            if rev {
                all.reverse();
            }
            emit(slice_by_index(all, start, stop), withscores)
        })
        .await
}

/// Slice an ordered list by inclusive Redis indices (negatives count from end).
fn slice_by_index(all: Vec<(f64, Vec<u8>)>, start: i64, stop: i64) -> Vec<(f64, Vec<u8>)> {
    let len = all.len() as i64;
    if len == 0 {
        return Vec::new();
    }
    let s = if start < 0 {
        (len + start).max(0)
    } else {
        start.min(len)
    };
    let e = if stop < 0 {
        len + stop
    } else {
        stop.min(len - 1)
    };
    if s > e || s >= len {
        return Vec::new();
    }
    let e = e.max(0);
    all.into_iter()
        .skip(s as usize)
        .take((e - s + 1) as usize)
        .collect()
}

pub async fn zrangebyscore(engine: &Arc<Engine>, args: &[Vec<u8>], rev: bool) -> Reply {
    if args.len() < 4 {
        return Reply::wrong_args("zrangebyscore");
    }
    let key = args[1].clone();
    // ZREVRANGEBYSCORE takes max then min.
    let (min_arg, max_arg) = if rev {
        (args[3].clone(), args[2].clone())
    } else {
        (args[2].clone(), args[3].clone())
    };
    let (Some(min), Some(max)) = (parse_score_bound(&min_arg), parse_score_bound(&max_arg)) else {
        return Reply::err("ERR min or max is not a float");
    };
    let mut withscores = false;
    let mut limit: Option<(i64, i64)> = None;
    let mut i = 4;
    while i < args.len() {
        if eq_ignore_case(&args[i], "WITHSCORES") {
            withscores = true;
            i += 1;
        } else if eq_ignore_case(&args[i], "LIMIT") {
            let (Some(off), Some(cnt)) = (
                args.get(i + 1).and_then(|b| parse_i64(b)),
                args.get(i + 2).and_then(|b| parse_i64(b)),
            ) else {
                return Reply::not_int();
            };
            limit = Some((off, cnt));
            i += 3;
        } else {
            return Reply::syntax();
        }
    }
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = zset_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            let mut items: Vec<(f64, Vec<u8>)> = scored_members(ctx, &key, del)
                .into_iter()
                .filter(|(s, _)| in_range(*s, min, max))
                .collect();
            if rev {
                items.reverse();
            }
            emit(apply_limit(items, limit), withscores)
        })
        .await
}

pub async fn zrevrange(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 4 {
        return Reply::wrong_args("zrevrange");
    }
    let key = args[1].clone();
    let (Some(start), Some(stop)) = (parse_i64(&args[2]), parse_i64(&args[3])) else {
        return Reply::not_int();
    };
    let mut withscores = false;
    if let Some(a) = args.get(4) {
        if eq_ignore_case(a, "WITHSCORES") {
            withscores = true;
        } else {
            return Reply::syntax();
        }
    }
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = zset_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            let mut all = scored_members(ctx, &key, del);
            all.reverse();
            emit(slice_by_index(all, start, stop), withscores)
        })
        .await
}

pub async fn zrank(engine: &Arc<Engine>, args: &[Vec<u8>], rev: bool) -> Reply {
    if args.len() < 3 || args.len() > 4 {
        return Reply::wrong_args("zrank");
    }
    let (key, member) = (args[1].clone(), args[2].clone());
    let withscore = match args.get(3) {
        None => false,
        Some(a) if eq_ignore_case(a, "WITHSCORE") => true,
        Some(_) => return Reply::syntax(),
    };
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = zset_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            let mut all = scored_members(ctx, &key, del);
            if rev {
                all.reverse();
            }
            match all.iter().position(|(_, m)| m == &member) {
                None => {
                    if withscore {
                        Reply::NullArray
                    } else {
                        Reply::Null
                    }
                }
                Some(idx) => {
                    if withscore {
                        Reply::Array(vec![Reply::Int(idx as i64), Reply::Double(all[idx].0)])
                    } else {
                        Reply::Int(idx as i64)
                    }
                }
            }
        })
        .await
}

pub async fn zcount(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 4 {
        return Reply::wrong_args("zcount");
    }
    let key = args[1].clone();
    let (Some(min), Some(max)) = (parse_score_bound(&args[2]), parse_score_bound(&args[3])) else {
        return Reply::err("ERR min or max is not a float");
    };
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = zset_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            let n = scored_members(ctx, &key, del)
                .into_iter()
                .filter(|(s, _)| in_range(*s, min, max))
                .count();
            Reply::Int(n as i64)
        })
        .await
}

pub async fn zpop(engine: &Arc<Engine>, args: &[Vec<u8>], max: bool) -> Reply {
    if args.len() < 2 || args.len() > 3 {
        return Reply::wrong_args("zpopmin");
    }
    let key = args[1].clone();
    let count = match args.get(2) {
        None => None,
        Some(b) => match parse_i64(b) {
            Some(n) if n >= 0 => Some(n as usize),
            _ => return Reply::not_int(),
        },
    };
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = zset_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            let n = count.unwrap_or(1);
            // ZPOPMIN: the score index is ascending — walk-and-stop, O(count).
            // ZPOPMAX needs the tail: one full pass keeping a bounded window
            // (no reverse iterator in the scan API; memory stays O(count)).
            let victims: Vec<(f64, Vec<u8>)> = if max {
                let mut window = std::collections::VecDeque::with_capacity(n + 1);
                for pair in scored_members(ctx, &key, del) {
                    window.push_back(pair);
                    if window.len() > n {
                        window.pop_front();
                    }
                }
                window.into_iter().rev().collect()
            } else {
                pop_scored_candidates(ctx, &key, del, n)
            };
            let mut popped = Vec::with_capacity(victims.len());
            for (score, member) in victims {
                if remove_member(ctx, &key, &member, del) {
                    popped.push((score, member));
                }
            }
            emit(popped, true)
        })
        .await
}

pub async fn zremrangebyscore(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 4 {
        return Reply::wrong_args("zremrangebyscore");
    }
    let key = args[1].clone();
    let (Some(min), Some(max)) = (parse_score_bound(&args[2]), parse_score_bound(&args[3])) else {
        return Reply::err("ERR min or max is not a float");
    };
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = zset_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            let victims: Vec<Vec<u8>> = scored_members(ctx, &key, del)
                .into_iter()
                .filter(|(s, _)| in_range(*s, min, max))
                .map(|(_, m)| m)
                .collect();
            let mut n = 0;
            for m in &victims {
                if remove_member(ctx, &key, m, del) {
                    n += 1;
                }
            }
            Reply::Int(n)
        })
        .await
}

pub async fn zrandmember(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 2 || args.len() > 4 {
        return Reply::wrong_args("zrandmember");
    }
    let key = args[1].clone();
    let count = args.get(2).and_then(|b| parse_i64(b));
    let withscores = match args.get(3) {
        None => false,
        Some(a) if eq_ignore_case(a, "WITHSCORES") => true,
        Some(_) => return Reply::syntax(),
    };
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = zset_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            let want = match count {
                None => 1,
                Some(n) => n.unsigned_abs() as usize,
            };
            let items = scored_members_limited(ctx, &key, del, want.max(1));
            if items.is_empty() {
                return if count.is_none() {
                    Reply::Null
                } else {
                    Reply::Array(vec![])
                };
            }
            match count {
                None => {
                    if withscores {
                        emit(vec![items[0].clone()], true)
                    } else {
                        Reply::Bulk(items[0].1.clone())
                    }
                }
                Some(n) => {
                    let take = if n < 0 { want } else { want.min(items.len()) };
                    let mut out = Vec::with_capacity(take * if withscores { 2 } else { 1 });
                    for i in 0..take {
                        let (s, m) = &items[i % items.len()];
                        out.push(Reply::Bulk(m.clone()));
                        if withscores {
                            out.push(Reply::Double(*s));
                        }
                    }
                    Reply::Array(out)
                }
            }
        })
        .await
}

pub async fn zremrangebyrank(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 4 {
        return Reply::wrong_args("zremrangebyrank");
    }
    let key = args[1].clone();
    let (Some(start), Some(stop)) = (parse_i64(&args[2]), parse_i64(&args[3])) else {
        return Reply::not_int();
    };
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = zset_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            let victims: Vec<Vec<u8>> = slice_by_index(scored_members(ctx, &key, del), start, stop)
                .into_iter()
                .map(|(_, m)| m)
                .collect();
            let mut n = 0;
            for m in victims {
                if remove_member(ctx, &key, &m, del) {
                    n += 1;
                }
            }
            Reply::Int(n)
        })
        .await
}

pub async fn zlexcount(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 4 {
        return Reply::wrong_args("zlexcount");
    }
    let key = args[1].clone();
    let (Some(min), Some(max)) = (
        parse_lex_bound(&args[2], true),
        parse_lex_bound(&args[3], false),
    ) else {
        return Reply::syntax();
    };
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = zset_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            let n = scored_members(ctx, &key, del)
                .into_iter()
                .filter(|(_, m)| in_lex(m, min.clone(), max.clone()))
                .count();
            Reply::Int(n as i64)
        })
        .await
}

pub async fn zrangebylex(engine: &Arc<Engine>, args: &[Vec<u8>], rev: bool) -> Reply {
    if args.len() < 4 {
        return Reply::wrong_args("zrangebylex");
    }
    let mut zargs = vec![
        b"ZRANGE".to_vec(),
        args[1].clone(),
        args[2].clone(),
        args[3].clone(),
        b"BYLEX".to_vec(),
    ];
    if rev {
        zargs.push(b"REV".to_vec());
    }
    zargs.extend_from_slice(&args[4..]);
    zrange(engine, &zargs).await
}

pub async fn zremrangebylex(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 4 {
        return Reply::wrong_args("zremrangebylex");
    }
    let key = args[1].clone();
    let (Some(min), Some(max)) = (
        parse_lex_bound(&args[2], true),
        parse_lex_bound(&args[3], false),
    ) else {
        return Reply::syntax();
    };
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = zset_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            let victims: Vec<Vec<u8>> = scored_members(ctx, &key, del)
                .into_iter()
                .filter(|(_, m)| in_lex(m, min.clone(), max.clone()))
                .map(|(_, m)| m)
                .collect();
            let mut n = 0;
            for m in victims {
                if remove_member(ctx, &key, &m, del) {
                    n += 1;
                }
            }
            Reply::Int(n)
        })
        .await
}

pub async fn zrangestore(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 5 {
        return Reply::wrong_args("zrangestore");
    }
    let dst = args[1].clone();
    let src = args[2].clone();
    let zargs: Vec<Vec<u8>> = std::iter::once(b"ZRANGE".to_vec())
        .chain(args[2..].iter().cloned())
        .collect();
    let selected = match zrange_items(engine, &zargs).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    let out = selected.clone();
    let dst_arg = dst.clone();
    engine
        .store
        .run_key(&dst_arg, move |ctx| {
            crate::cmd::generic::del_key(ctx, &dst);
            if !out.is_empty() {
                ensure_head(ctx, &dst, head::CTYPE_ZSET);
            }
            for (s, m) in out {
                write_member(ctx, &dst, &m, s);
            }
        })
        .await;
    let _ = src;
    Reply::Int(selected.len() as i64)
}

fn try_zpop(
    ctx: &ShardCtx,
    key: &[u8],
    max: bool,
    count: usize,
) -> Result<Vec<(f64, Vec<u8>)>, ()> {
    let del = zset_del_hlc(ctx, key)?;
    let victims: Vec<(f64, Vec<u8>)> = if max {
        let mut window = std::collections::VecDeque::with_capacity(count + 1);
        for pair in scored_members(ctx, key, del) {
            window.push_back(pair);
            if window.len() > count {
                window.pop_front();
            }
        }
        window.into_iter().rev().collect()
    } else {
        pop_scored_candidates(ctx, key, del, count)
    };
    let mut popped = Vec::with_capacity(victims.len());
    for (score, member) in victims {
        if remove_member(ctx, key, &member, del) {
            popped.push((score, member));
        }
    }
    Ok(popped)
}

async fn zmpop_inner(
    engine: &Arc<Engine>,
    keys: &[Vec<u8>],
    max: bool,
    count: usize,
) -> Result<Option<(Vec<u8>, Vec<(f64, Vec<u8>)>)>, Reply> {
    for key in keys {
        engine.ensure_local(key).await;
        let k = key.clone();
        let popped = engine
            .store
            .run_key(key, move |ctx| try_zpop(ctx, &k, max, count))
            .await;
        match popped {
            Err(()) => return Err(Reply::wrongtype()),
            Ok(items) if !items.is_empty() => return Ok(Some((key.clone(), items))),
            Ok(_) => {}
        }
    }
    Ok(None)
}

fn zmpop_reply(hit: Option<(Vec<u8>, Vec<(f64, Vec<u8>)>)>) -> Reply {
    match hit {
        None => Reply::NullArray,
        Some((key, items)) => Reply::Array(vec![Reply::Bulk(key), emit(items, true)]),
    }
}

fn parse_zmpop_tail(args: &[Vec<u8>], start: usize) -> Result<(Vec<Vec<u8>>, bool, usize), Reply> {
    let Some(numkeys) = args.get(start).and_then(|b| crate::cmd::parse_u64(b)) else {
        return Err(Reply::not_int());
    };
    if numkeys == 0 {
        return Err(Reply::syntax());
    }
    let numkeys = numkeys as usize;
    let keys_start = start + 1;
    let where_idx = keys_start + numkeys;
    if args.len() <= where_idx {
        return Err(Reply::syntax());
    }
    let max = if eq_ignore_case(&args[where_idx], "MAX") {
        true
    } else if eq_ignore_case(&args[where_idx], "MIN") {
        false
    } else {
        return Err(Reply::syntax());
    };
    let mut count = 1usize;
    let mut i = where_idx + 1;
    while i < args.len() {
        if eq_ignore_case(&args[i], "COUNT") {
            let Some(n) = args.get(i + 1).and_then(|b| parse_i64(b)) else {
                return Err(Reply::not_int());
            };
            if n <= 0 {
                return Err(Reply::err("ERR count should be greater than 0"));
            }
            count = n as usize;
            i += 2;
        } else {
            return Err(Reply::syntax());
        }
    }
    Ok((args[keys_start..where_idx].to_vec(), max, count))
}

pub async fn zmpop(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 4 {
        return Reply::wrong_args("zmpop");
    }
    let (keys, max, count) = match parse_zmpop_tail(args, 1) {
        Ok(v) => v,
        Err(r) => return r,
    };
    match zmpop_inner(engine, &keys, max, count).await {
        Ok(hit) => zmpop_reply(hit),
        Err(r) => r,
    }
}

fn parse_timeout(b: &[u8]) -> Option<Option<std::time::Duration>> {
    let secs = parse_f64(b)?;
    if secs < 0.0 {
        return None;
    }
    Some(if secs == 0.0 {
        None
    } else {
        Some(std::time::Duration::from_secs_f64(secs))
    })
}

pub async fn bzpop(engine: &Arc<Engine>, args: &[Vec<u8>], max: bool) -> Reply {
    if args.len() < 3 {
        return Reply::wrong_args("bzpop");
    }
    let Some(timeout) = parse_timeout(args.last().unwrap()) else {
        return Reply::err("ERR timeout is negative or not a float");
    };
    let keys: Vec<Vec<u8>> = args[1..args.len() - 1].to_vec();
    let deadline = timeout.map(|d| std::time::Instant::now() + d);
    loop {
        match zmpop_inner(engine, &keys, max, 1).await {
            Err(r) => return r,
            Ok(Some((key, mut items))) => {
                let (score, member) = items.remove(0);
                return Reply::Array(vec![
                    Reply::Bulk(key),
                    Reply::Bulk(member),
                    Reply::Double(score),
                ]);
            }
            Ok(None) => {}
        }
        if let Some(d) = deadline {
            if std::time::Instant::now() >= d {
                return Reply::NullArray;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

pub async fn bzmpop(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 5 {
        return Reply::wrong_args("bzmpop");
    }
    let Some(timeout) = parse_timeout(&args[1]) else {
        return Reply::err("ERR timeout is negative or not a float");
    };
    let (keys, max, count) = match parse_zmpop_tail(args, 2) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let deadline = timeout.map(|d| std::time::Instant::now() + d);
    loop {
        match zmpop_inner(engine, &keys, max, count).await {
            Err(r) => return r,
            Ok(Some(hit)) => return zmpop_reply(Some(hit)),
            Ok(None) => {}
        }
        if let Some(d) = deadline {
            if std::time::Instant::now() >= d {
                return Reply::NullArray;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

#[derive(Clone, Copy)]
pub enum ZSetOp {
    Union,
    Inter,
    Diff,
}

#[derive(Clone, Copy)]
enum Aggregate {
    Sum,
    Min,
    Max,
}

async fn zitems_of(engine: &Arc<Engine>, key: &[u8]) -> Result<Vec<(Vec<u8>, f64)>, ()> {
    let k = key.to_vec();
    engine.ensure_local(key).await;
    engine
        .store
        .run_key(key, move |ctx| {
            let del = zset_del_hlc(ctx, &k)?;
            Ok(scored_members(ctx, &k, del)
                .into_iter()
                .map(|(s, m)| (m, s))
                .collect())
        })
        .await
}

fn aggregate(cur: f64, next: f64, agg: Aggregate) -> f64 {
    match agg {
        Aggregate::Sum => cur + next,
        Aggregate::Min => cur.min(next),
        Aggregate::Max => cur.max(next),
    }
}

pub async fn zsetop(engine: &Arc<Engine>, args: &[Vec<u8>], op: ZSetOp, store_dst: bool) -> Reply {
    let min = if store_dst { 4 } else { 3 };
    if args.len() < min {
        return Reply::wrong_args("zsetop");
    }
    let mut idx = if store_dst { 2 } else { 1 };
    let Some(numkeys) = args.get(idx).and_then(|b| crate::cmd::parse_u64(b)) else {
        return Reply::not_int();
    };
    if numkeys == 0 {
        return Reply::syntax();
    }
    let numkeys = numkeys as usize;
    idx += 1;
    if args.len() < idx + numkeys {
        return Reply::syntax();
    }
    let keys = args[idx..idx + numkeys].to_vec();
    idx += numkeys;
    let mut weights = vec![1.0; numkeys];
    let mut agg = Aggregate::Sum;
    let mut withscores = false;
    while idx < args.len() {
        if eq_ignore_case(&args[idx], "WEIGHTS") {
            if args.len() < idx + 1 + numkeys {
                return Reply::syntax();
            }
            for j in 0..numkeys {
                let Some(w) = parse_f64(&args[idx + 1 + j]) else {
                    return Reply::not_float();
                };
                weights[j] = w;
            }
            idx += 1 + numkeys;
        } else if eq_ignore_case(&args[idx], "AGGREGATE") {
            let Some(a) = args.get(idx + 1) else {
                return Reply::syntax();
            };
            agg = if eq_ignore_case(a, "SUM") {
                Aggregate::Sum
            } else if eq_ignore_case(a, "MIN") {
                Aggregate::Min
            } else if eq_ignore_case(a, "MAX") {
                Aggregate::Max
            } else {
                return Reply::syntax();
            };
            idx += 2;
        } else if eq_ignore_case(&args[idx], "WITHSCORES") && !store_dst {
            withscores = true;
            idx += 1;
        } else {
            return Reply::syntax();
        }
    }

    let mut maps = Vec::with_capacity(numkeys);
    for (k, w) in keys.iter().zip(weights.iter()) {
        let items = match zitems_of(engine, k).await {
            Ok(v) => v,
            Err(()) => return Reply::wrongtype(),
        };
        maps.push(
            items
                .into_iter()
                .map(|(m, s)| (m, s * *w))
                .collect::<HashMap<Vec<u8>, f64>>(),
        );
    }

    let mut result: HashMap<Vec<u8>, f64> = HashMap::new();
    match op {
        ZSetOp::Union => {
            for m in maps {
                for (member, score) in m {
                    result
                        .entry(member)
                        .and_modify(|s| *s = aggregate(*s, score, agg))
                        .or_insert(score);
                }
            }
        }
        ZSetOp::Inter => {
            if let Some(first) = maps.first() {
                let mut candidates: HashSet<Vec<u8>> = first.keys().cloned().collect();
                for m in maps.iter().skip(1) {
                    candidates.retain(|member| m.contains_key(member));
                }
                for member in candidates {
                    let mut score = maps[0][&member];
                    for m in maps.iter().skip(1) {
                        score = aggregate(score, m[&member], agg);
                    }
                    result.insert(member, score);
                }
            }
        }
        ZSetOp::Diff => {
            if let Some(first) = maps.first() {
                for (member, score) in first {
                    if maps.iter().skip(1).all(|m| !m.contains_key(member)) {
                        result.insert(member.clone(), *score);
                    }
                }
            }
        }
    }

    let mut out: Vec<(f64, Vec<u8>)> = result.into_iter().map(|(m, s)| (s, m)).collect();
    out.sort_by(|a, b| {
        a.0.partial_cmp(&b.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.1.cmp(&b.1))
    });

    if store_dst {
        let dst = args[1].clone();
        let rows = out.clone();
        let dst_arg = dst.clone();
        engine
            .store
            .run_key(&dst_arg, move |ctx| {
                crate::cmd::generic::del_key(ctx, &dst);
                if !rows.is_empty() {
                    ensure_head(ctx, &dst, head::CTYPE_ZSET);
                }
                for (s, m) in rows {
                    write_member(ctx, &dst, &m, s);
                }
            })
            .await;
        Reply::Int(out.len() as i64)
    } else {
        emit(out, withscores)
    }
}

pub async fn zintercard(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 3 {
        return Reply::wrong_args("zintercard");
    }
    let Some(numkeys) = args.get(1).and_then(|b| crate::cmd::parse_u64(b)) else {
        return Reply::not_int();
    };
    if numkeys == 0 {
        return Reply::syntax();
    }
    let numkeys = numkeys as usize;
    if args.len() < 2 + numkeys {
        return Reply::syntax();
    }
    let mut limit = usize::MAX;
    let mut idx = 2 + numkeys;
    while idx < args.len() {
        if eq_ignore_case(&args[idx], "LIMIT") {
            let Some(n) = args.get(idx + 1).and_then(|b| crate::cmd::parse_u64(b)) else {
                return Reply::not_int();
            };
            if n != 0 {
                limit = n as usize;
            }
            idx += 2;
        } else {
            return Reply::syntax();
        }
    }
    let mut acc: Option<HashSet<Vec<u8>>> = None;
    for k in &args[2..2 + numkeys] {
        let items = match zitems_of(engine, k).await {
            Ok(v) => v,
            Err(()) => return Reply::wrongtype(),
        };
        let set: HashSet<Vec<u8>> = items.into_iter().map(|(m, _)| m).collect();
        acc = Some(match acc {
            None => set,
            Some(cur) => cur.intersection(&set).cloned().collect(),
        });
        if acc.as_ref().is_some_and(|s| s.len() >= limit) {
            break;
        }
    }
    Reply::Int(acc.map_or(0, |s| s.len().min(limit)) as i64)
}

async fn zrange_items(
    engine: &Arc<Engine>,
    args: &[Vec<u8>],
) -> Result<Vec<(f64, Vec<u8>)>, Reply> {
    if args.len() < 4 {
        return Err(Reply::wrong_args("zrange"));
    }
    let key = args[1].clone();
    let a2 = args[2].clone();
    let a3 = args[3].clone();
    let mut byscore = false;
    let mut bylex = false;
    let mut rev = false;
    let mut limit: Option<(i64, i64)> = None;
    let mut i = 4;
    while i < args.len() {
        if eq_ignore_case(&args[i], "BYSCORE") {
            byscore = true;
            i += 1;
        } else if eq_ignore_case(&args[i], "BYLEX") {
            bylex = true;
            i += 1;
        } else if eq_ignore_case(&args[i], "REV") {
            rev = true;
            i += 1;
        } else if eq_ignore_case(&args[i], "WITHSCORES") {
            i += 1;
        } else if eq_ignore_case(&args[i], "LIMIT") {
            let (Some(off), Some(cnt)) = (
                args.get(i + 1).and_then(|b| parse_i64(b)),
                args.get(i + 2).and_then(|b| parse_i64(b)),
            ) else {
                return Err(Reply::not_int());
            };
            limit = Some((off, cnt));
            i += 3;
        } else {
            return Err(Reply::syntax());
        }
    }
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Ok(del) = zset_del_hlc(ctx, &key) else {
                return Err(Reply::wrongtype());
            };
            let mut items = scored_members(ctx, &key, del);
            if byscore {
                let (min_arg, max_arg) = if rev { (a3, a2) } else { (a2, a3) };
                let (Some(min), Some(max)) =
                    (parse_score_bound(&min_arg), parse_score_bound(&max_arg))
                else {
                    return Err(Reply::err("ERR min or max is not a float"));
                };
                items.retain(|(s, _)| in_range(*s, min, max));
            } else if bylex {
                let (min_arg, max_arg) = if rev { (a3, a2) } else { (a2, a3) };
                let (Some(min), Some(max)) = (
                    parse_lex_bound(&min_arg, true),
                    parse_lex_bound(&max_arg, false),
                ) else {
                    return Err(Reply::syntax());
                };
                items.retain(|(_, m)| in_lex(m, min.clone(), max.clone()));
                items.sort_by(|a, b| a.1.cmp(&b.1));
            } else {
                let (Some(start), Some(stop)) = (parse_i64(&a2), parse_i64(&a3)) else {
                    return Err(Reply::not_int());
                };
                if rev {
                    items.reverse();
                }
                return Ok(slice_by_index(items, start, stop));
            }
            if rev {
                items.reverse();
            }
            Ok(apply_limit(items, limit))
        })
        .await
}

pub async fn zscan(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 3 {
        return Reply::wrong_args("zscan");
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
            let Ok(del) = zset_del_hlc(ctx, &key) else {
                return Reply::wrongtype();
            };
            let mut items = Vec::new();
            for (s, m) in scored_members(ctx, &key, del) {
                if pattern.as_deref().is_none_or(|p| glob_match(p, &m)) {
                    items.push(Reply::Bulk(m));
                    items.push(Reply::Bulk(fmt_f64(s).into_bytes()));
                }
            }
            Reply::Array(vec![Reply::Bulk(b"0".to_vec()), Reply::Array(items)])
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn score_bound_parsing() {
        assert_eq!(parse_score_bound(b"5"), Some((5.0, false)));
        assert_eq!(parse_score_bound(b"(5"), Some((5.0, true)));
        assert_eq!(parse_score_bound(b"-inf"), Some((f64::NEG_INFINITY, false)));
        assert_eq!(parse_score_bound(b"+inf"), Some((f64::INFINITY, false)));
        assert_eq!(parse_score_bound(b"(inf"), Some((f64::INFINITY, true)));
        assert_eq!(parse_score_bound(b"bogus"), None);
    }

    #[test]
    fn range_inclusive_exclusive() {
        // inclusive both ends
        assert!(in_range(5.0, (1.0, false), (10.0, false)));
        assert!(in_range(1.0, (1.0, false), (10.0, false)));
        assert!(in_range(10.0, (1.0, false), (10.0, false)));
        // exclusive lower
        assert!(!in_range(1.0, (1.0, true), (10.0, false)));
        // exclusive upper
        assert!(!in_range(10.0, (1.0, false), (10.0, true)));
        // out of range
        assert!(!in_range(0.5, (1.0, false), (10.0, false)));
    }

    #[test]
    fn score_record_roundtrip() {
        let rec = element_add(RecordType::ZsetMember, 42, 1, &3.5f64.to_be_bytes());
        assert_eq!(score_from_record(&rec), Some(3.5));
    }

    #[test]
    fn index_key_orders_by_score_then_member() {
        // Lower score sorts before higher, regardless of member bytes.
        let a = ikey::zset_score_key(b"z", encode_score(1.0), b"zzz");
        let b = ikey::zset_score_key(b"z", encode_score(2.0), b"aaa");
        assert!(a < b);
        // Equal score: member lexicographic.
        let c = ikey::zset_score_key(b"z", encode_score(1.0), b"aaa");
        let d = ikey::zset_score_key(b"z", encode_score(1.0), b"bbb");
        assert!(c < d);
    }

    #[test]
    fn slice_by_index_negatives() {
        let v: Vec<(f64, Vec<u8>)> = (0..5).map(|i| (i as f64, vec![b'a' + i])).collect();
        // full range 0..-1
        assert_eq!(slice_by_index(v.clone(), 0, -1).len(), 5);
        // last two
        let last2 = slice_by_index(v.clone(), -2, -1);
        assert_eq!(last2.len(), 2);
        assert_eq!(last2[0].1, vec![b'd']);
        // start past end
        assert_eq!(slice_by_index(v.clone(), 10, 20).len(), 0);
        // start > stop
        assert_eq!(slice_by_index(v, 3, 1).len(), 0);
    }
}

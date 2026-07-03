//! List family (design/02 §Lists, design/03 §Lists).
//!
//! A list is a head-gated collection (ctype 5) of position-keyed LWW element
//! records at `list_elem_key(key, pos)`. `pos` is an unsigned u64 whose
//! big-endian key suffix makes memcmp order equal list order: the first push
//! lands at `LIST_CENTER`, LPUSH allocates `head-1`, RPUSH `tail+1`. Each
//! element payload is the raw value bytes — a plain LWW register (higher
//! `(hlc, origin)` wins) — so push/pop touch O(1) records and replicate as
//! ordinary deltas through `write_merged` (no list-specific merge code).
//!
//! Head/tail discovery uses a per-shard in-memory hint stored in the same
//! `pop_hints` map the set/zset pop cursors use, namespaced by the ListElem
//! collection prefix. The hint caches `(head_pos, tail_pos)`; on a miss (fresh
//! process, post-rebuild, or when a cheap verify finds the cached head no
//! longer live) a full prefix scan recovers the live min/max position. The
//! hint is a pure optimization — reads (LLEN/LRANGE/LINDEX/LPOS) always walk
//! the records — so a wrong hint can at worst cost a rescan, never a wrong
//! answer.
//!
//! Concurrency caveat (design/02): two nodes pushing concurrently can allocate
//! the same position; the element records then merge LWW and one push is lost
//! — a bounded, per-collision loss, strictly better than the retired whole-
//! list blob where an entire concurrent push SET was dropped. Interior
//! mutations (LINSERT/LREM/LTRIM) rebuild the list compacted from CENTER
//! (O(n), documented). The whole-list TTL rides the collection head like a
//! hash/set; elements carry no TTL of their own.

use std::sync::Arc;

use crate::cmd::{eq_ignore_case, norm_index, parse_i64};
use crate::reply::Reply;
use crate::store::{
    check_type, ensure_head, get_raw, new_lww, new_tombstone, now_ms, scan_from, scan_prefix,
    visible, write_merged, ShardCtx,
};
use crate::Engine;
use marekvs_core::envelope::{head, Envelope, RecordType};
use marekvs_core::ikey::{self, Tag, LIST_CENTER};

// ---------------------------------------------------------------------------
// per-shard head/tail position hint (namespaced in the pop_hints map)
// ---------------------------------------------------------------------------

fn hint_key(key: &[u8]) -> Vec<u8> {
    ikey::collection_prefix(Tag::ListElem, key)
}

enum Hint {
    /// List is known empty (no live element).
    Empty,
    /// Cached live head/tail positions.
    Known(u64, u64),
    /// Nothing cached — recover by scan.
    Unknown,
}

fn get_hint(ctx: &ShardCtx, key: &[u8]) -> Hint {
    match ctx.pop_hints.borrow().get(&hint_key(key)) {
        None => Hint::Unknown,
        Some(v) if v.is_empty() => Hint::Empty,
        Some(v) if v.len() >= 16 => Hint::Known(
            u64::from_be_bytes(v[..8].try_into().unwrap()),
            u64::from_be_bytes(v[8..16].try_into().unwrap()),
        ),
        Some(_) => Hint::Unknown,
    }
}

fn set_known(ctx: &ShardCtx, key: &[u8], head_pos: u64, tail_pos: u64) {
    let mut v = Vec::with_capacity(16);
    v.extend_from_slice(&head_pos.to_be_bytes());
    v.extend_from_slice(&tail_pos.to_be_bytes());
    ctx.pop_hints.borrow_mut().insert(hint_key(key), v);
}

fn set_empty(ctx: &ShardCtx, key: &[u8]) {
    ctx.pop_hints.borrow_mut().insert(hint_key(key), Vec::new());
}

/// Drop the cached head/tail range so the next list op re-derives it by scan.
///
/// The position hint is node-local derived state, like the zset score index.
/// A list element that lands via replication/anti-entropy/bootstrap goes
/// straight through `write_merged` and never touches this hint, so a replica
/// that had cached an `Empty` (or now-stale) range would otherwise keep
/// serving it and miss the new element (e.g. a cross-node BLPOP that blocks
/// before the push arrives). The replication apply path calls this for every
/// incoming list-element record — analogous to `zset::apply_member_record`
/// rebuilding the score index — so the derived state re-syncs. Local pushes
/// and pops maintain the hint directly and never need this.
pub fn invalidate_hint(ctx: &ShardCtx, key: &[u8]) {
    ctx.pop_hints.borrow_mut().remove(&hint_key(key));
}

// ---------------------------------------------------------------------------
// element read / write
// ---------------------------------------------------------------------------

/// Visible value at `pos` (payload is the raw value bytes); None if the record
/// is missing, a tombstone, expired, or gated by the collection delete clock.
fn read_elem(ctx: &ShardCtx, key: &[u8], pos: u64, del: u64) -> Option<Vec<u8>> {
    let v = get_raw(ctx, &ikey::list_elem_key(key, pos))?;
    let (env, pay) = Envelope::decode(&v)?;
    visible(&env, pay, del, now_ms())?;
    Some(pay.to_vec())
}

fn write_elem(ctx: &ShardCtx, key: &[u8], pos: u64, val: &[u8]) {
    let rec = new_lww(ctx, RecordType::List, val, 0);
    write_merged(ctx, &ikey::list_elem_key(key, pos), &rec);
}

fn tombstone_elem(ctx: &ShardCtx, key: &[u8], pos: u64) {
    let t = new_tombstone(ctx, RecordType::List);
    write_merged(ctx, &ikey::list_elem_key(key, pos), &t);
}

// ---------------------------------------------------------------------------
// live-position discovery
// ---------------------------------------------------------------------------

/// Full scan for the live (min, max) position. Ascending scan → first live is
/// the head, last live is the tail. None when no element is live.
fn scan_minmax(ctx: &ShardCtx, key: &[u8], del: u64) -> Option<(u64, u64)> {
    let now = now_ms();
    let mut lo: Option<u64> = None;
    let mut hi = 0u64;
    scan_prefix(ctx, &hint_key(key), |k, v| {
        if let (Some(p), Some((env, pay))) = (ikey::parse(k), Envelope::decode(v)) {
            if visible(&env, pay, del, now).is_some() {
                if let Some(pos) = ikey::list_pos(p.suffix) {
                    if lo.is_none() {
                        lo = Some(pos);
                    }
                    hi = pos;
                }
            }
        }
        true
    });
    lo.map(|l| (l, hi))
}

fn recover(ctx: &ShardCtx, key: &[u8], del: u64) -> Option<(u64, u64)> {
    match scan_minmax(ctx, key, del) {
        Some((h, t)) => {
            set_known(ctx, key, h, t);
            Some((h, t))
        }
        None => {
            set_empty(ctx, key);
            None
        }
    }
}

/// Current (head, tail) live positions, hint-cached. None = empty list.
/// A cached range is verified cheaply against the head record so a DEL, TTL
/// expiry, or remote pop that killed the cached head triggers a rescan.
fn list_range(ctx: &ShardCtx, key: &[u8], del: u64) -> Option<(u64, u64)> {
    match get_hint(ctx, key) {
        Hint::Empty => None,
        Hint::Known(h, t) => {
            if read_elem(ctx, key, h, del).is_some() {
                Some((h, t))
            } else {
                recover(ctx, key, del)
            }
        }
        Hint::Unknown => recover(ctx, key, del),
    }
}

/// First live element at/after `from`. Fast when `from` is live (one read);
/// otherwise seeks past dead records the way the set/zset pop cursor does.
fn find_front(ctx: &ShardCtx, key: &[u8], from: u64, del: u64) -> Option<(u64, Vec<u8>)> {
    if let Some(val) = read_elem(ctx, key, from, del) {
        return Some((from, val));
    }
    let now = now_ms();
    let prefix = hint_key(key);
    let start = ikey::list_elem_key(key, from);
    let mut found = None;
    scan_from(ctx, &start, &prefix, |k, v| {
        if let (Some(p), Some((env, pay))) = (ikey::parse(k), Envelope::decode(v)) {
            if visible(&env, pay, del, now).is_some() {
                if let Some(pos) = ikey::list_pos(p.suffix) {
                    found = Some((pos, pay.to_vec()));
                    return false;
                }
            }
        }
        true
    });
    found
}

/// Live element at the tail. Fast when the cached `tail` is live; otherwise
/// (no reverse iterator) scans the range once for the true max live position.
fn find_back(ctx: &ShardCtx, key: &[u8], tail: u64, del: u64) -> Option<(u64, Vec<u8>)> {
    if let Some(val) = read_elem(ctx, key, tail, del) {
        return Some((tail, val));
    }
    let now = now_ms();
    let mut last = None;
    scan_prefix(ctx, &hint_key(key), |k, v| {
        if let (Some(p), Some((env, pay))) = (ikey::parse(k), Envelope::decode(v)) {
            if visible(&env, pay, del, now).is_some() {
                if let Some(pos) = ikey::list_pos(p.suffix) {
                    last = Some((pos, pay.to_vec()));
                }
            }
        }
        true
    });
    last
}

/// All live values in list order (O(n)); used by LLEN/full LRANGE/LPOS/rebuild.
fn live_items(ctx: &ShardCtx, key: &[u8], del: u64) -> Vec<Vec<u8>> {
    let now = now_ms();
    let mut out = Vec::new();
    scan_prefix(ctx, &hint_key(key), |_k, v| {
        if let Some((env, pay)) = Envelope::decode(v) {
            if visible(&env, pay, del, now).is_some() {
                out.push(pay.to_vec());
            }
        }
        true
    });
    out
}

/// Live `(pos, value)` pairs in list order; used by LSET (target position) and
/// rebuild (old positions to tombstone).
fn live_pairs(ctx: &ShardCtx, key: &[u8], del: u64) -> Vec<(u64, Vec<u8>)> {
    let now = now_ms();
    let mut out = Vec::new();
    scan_prefix(ctx, &hint_key(key), |k, v| {
        if let (Some(p), Some((env, pay))) = (ikey::parse(k), Envelope::decode(v)) {
            if visible(&env, pay, del, now).is_some() {
                if let Some(pos) = ikey::list_pos(p.suffix) {
                    out.push((pos, pay.to_vec()));
                }
            }
        }
        true
    });
    out
}

/// First `stop_incl + 1` live values (bounded LRANGE/LINDEX with non-negative
/// indices — never materializes the whole list).
fn live_items_bounded(ctx: &ShardCtx, key: &[u8], del: u64, stop_incl: usize) -> Vec<Vec<u8>> {
    let now = now_ms();
    let mut out = Vec::new();
    scan_prefix(ctx, &hint_key(key), |_k, v| {
        if let Some((env, pay)) = Envelope::decode(v) {
            if visible(&env, pay, del, now).is_some() {
                out.push(pay.to_vec());
                if out.len() > stop_incl {
                    return false;
                }
            }
        }
        true
    });
    out
}

fn live_count(ctx: &ShardCtx, key: &[u8], del: u64) -> usize {
    let now = now_ms();
    let mut n = 0;
    scan_prefix(ctx, &hint_key(key), |_k, v| {
        if let Some((env, pay)) = Envelope::decode(v) {
            if visible(&env, pay, del, now).is_some() {
                n += 1;
            }
        }
        true
    });
    n
}

// ---------------------------------------------------------------------------
// push / pop / rebuild primitives (operate on the owning shard thread)
// ---------------------------------------------------------------------------

/// Push `values` onto `key` (front if `left`); the head must already exist.
/// Returns the new length as the contiguous span `tail - head + 1` — exact for
/// single-node lists, best-effort under concurrent cross-node pushes.
fn do_push(ctx: &ShardCtx, key: &[u8], values: &[Vec<u8>], left: bool, del: u64) -> i64 {
    // Recenter if a push would run off the u64 edge (astronomically rare;
    // keeps the position arithmetic non-wrapping).
    if let Some((h, t)) = list_range(ctx, key, del) {
        let n = values.len() as u64;
        if (left && h < n) || (!left && t > u64::MAX - n) {
            rebuild(ctx, key, del, None);
        }
    }
    let mut range = list_range(ctx, key, del);
    for v in values {
        let pos = match range {
            None => LIST_CENTER,
            Some((h, t)) => {
                if left {
                    h - 1
                } else {
                    t + 1
                }
            }
        };
        write_elem(ctx, key, pos, v);
        range = Some(match range {
            None => (pos, pos),
            Some((h, t)) => {
                if left {
                    (pos, t)
                } else {
                    (h, pos)
                }
            }
        });
    }
    match range {
        None => 0,
        Some((h, t)) => {
            set_known(ctx, key, h, t);
            (t - h + 1) as i64
        }
    }
}

/// Pop one element from `key` (head if `left`), tombstoning it and advancing
/// the hint. None when the list is empty.
fn do_pop(ctx: &ShardCtx, key: &[u8], left: bool, del: u64) -> Option<Vec<u8>> {
    let (h, t) = list_range(ctx, key, del)?;
    if left {
        let (pos, val) = find_front(ctx, key, h, del)?;
        tombstone_elem(ctx, key, pos);
        if pos >= t {
            set_empty(ctx, key);
        } else {
            set_known(ctx, key, pos + 1, t);
        }
        Some(val)
    } else {
        let (pos, val) = find_back(ctx, key, t, del)?;
        tombstone_elem(ctx, key, pos);
        if pos <= h {
            set_empty(ctx, key);
        } else {
            set_known(ctx, key, h, pos - 1);
        }
        Some(val)
    }
}

/// Rebuild the list compacted from CENTER. `replacement = None` keeps the
/// current live values (used only to recenter on position exhaustion);
/// `Some(v)` installs `v` (LINSERT/LREM/LTRIM). O(n): every live element is
/// tombstoned and the new sequence is rewritten from CENTER. Resets the hint.
fn rebuild(ctx: &ShardCtx, key: &[u8], del: u64, replacement: Option<Vec<Vec<u8>>>) {
    let pairs = live_pairs(ctx, key, del);
    let new_values =
        replacement.unwrap_or_else(|| pairs.iter().map(|(_, v)| v.clone()).collect::<Vec<_>>());
    for (pos, _) in &pairs {
        tombstone_elem(ctx, key, *pos);
    }
    if new_values.is_empty() {
        set_empty(ctx, key);
        return;
    }
    // Fresh HLC on each rewrite sorts after the tombstones above, so positions
    // reused from the old range (CENTER…) resolve LWW to the new value.
    for (i, v) in new_values.iter().enumerate() {
        write_elem(ctx, key, LIST_CENTER + i as u64, v);
    }
    set_known(
        ctx,
        key,
        LIST_CENTER,
        LIST_CENTER + new_values.len() as u64 - 1,
    );
}

/// Resolve a Redis [start,stop] index pair against `len` to a half-open
/// `[s, e)` range, clamped. Returns `None` for an empty selection.
fn clamp_range(start: i64, stop: i64, len: usize) -> Option<(usize, usize)> {
    if len == 0 {
        return None;
    }
    let len = len as i64;
    let s = norm_index(start, len as usize).max(0);
    let mut e = norm_index(stop, len as usize);
    e = e.min(len - 1);
    if s > e || s >= len {
        return None;
    }
    Some((s as usize, (e + 1) as usize))
}

// ---------------------------------------------------------------------------
// commands
// ---------------------------------------------------------------------------

pub async fn push(engine: &Arc<Engine>, args: &[Vec<u8>], left: bool, xx: bool) -> Reply {
    if args.len() < 3 {
        return Reply::wrong_args("lpush");
    }
    let key = args[1].clone();
    let values: Vec<Vec<u8>> = args[2..].to_vec();
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let del = match check_type(ctx, &key, head::CTYPE_LIST) {
                Err(()) => return Reply::wrongtype(),
                Ok(d) => d,
            };
            if xx && list_range(ctx, &key, del).is_none() {
                return Reply::Int(0);
            }
            ensure_head(ctx, &key, head::CTYPE_LIST);
            Reply::Int(do_push(ctx, &key, &values, left, del))
        })
        .await
}

pub async fn pop(engine: &Arc<Engine>, args: &[Vec<u8>], left: bool) -> Reply {
    if args.len() < 2 || args.len() > 3 {
        return Reply::wrong_args("lpop");
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
            let del = match check_type(ctx, &key, head::CTYPE_LIST) {
                Err(()) => return Reply::wrongtype(),
                Ok(d) => d,
            };
            match count {
                None => match do_pop(ctx, &key, left, del) {
                    Some(v) => Reply::Bulk(v),
                    None => Reply::Null,
                },
                Some(n) => {
                    if list_range(ctx, &key, del).is_none() {
                        return Reply::NullArray;
                    }
                    let mut popped = Vec::with_capacity(n);
                    for _ in 0..n {
                        match do_pop(ctx, &key, left, del) {
                            Some(v) => popped.push(Reply::Bulk(v)),
                            None => break,
                        }
                    }
                    Reply::Array(popped)
                }
            }
        })
        .await
}

pub async fn llen(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 2 {
        return Reply::wrong_args("llen");
    }
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            match check_type(ctx, &key, head::CTYPE_LIST) {
                Err(()) => Reply::wrongtype(),
                Ok(del) => Reply::Int(live_count(ctx, &key, del) as i64),
            }
        })
        .await
}

pub async fn lrange(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 4 {
        return Reply::wrong_args("lrange");
    }
    let key = args[1].clone();
    let (Some(start), Some(stop)) = (parse_i64(&args[2]), parse_i64(&args[3])) else {
        return Reply::not_int();
    };
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let del = match check_type(ctx, &key, head::CTYPE_LIST) {
                Err(()) => return Reply::wrongtype(),
                Ok(d) => d,
            };
            // Non-negative bounds: bounded forward walk, O(stop). Otherwise a
            // negative index needs the length, so materialize and slice.
            let items: Vec<Vec<u8>> = if start >= 0 && stop >= 0 {
                if stop < start {
                    Vec::new()
                } else {
                    let bounded = live_items_bounded(ctx, &key, del, stop as usize);
                    let s = (start as usize).min(bounded.len());
                    let e = (stop as usize + 1).min(bounded.len());
                    if s >= e {
                        Vec::new()
                    } else {
                        bounded[s..e].to_vec()
                    }
                }
            } else {
                let all = live_items(ctx, &key, del);
                match clamp_range(start, stop, all.len()) {
                    None => Vec::new(),
                    Some((s, e)) => all[s..e].to_vec(),
                }
            };
            Reply::Array(items.into_iter().map(Reply::Bulk).collect())
        })
        .await
}

pub async fn lindex(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 3 {
        return Reply::wrong_args("lindex");
    }
    let key = args[1].clone();
    let Some(index) = parse_i64(&args[2]) else {
        return Reply::not_int();
    };
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let del = match check_type(ctx, &key, head::CTYPE_LIST) {
                Err(()) => return Reply::wrongtype(),
                Ok(d) => d,
            };
            let val = if index >= 0 {
                live_items_bounded(ctx, &key, del, index as usize)
                    .into_iter()
                    .nth(index as usize)
            } else {
                let all = live_items(ctx, &key, del);
                let idx = norm_index(index, all.len());
                if idx < 0 {
                    None
                } else {
                    all.into_iter().nth(idx as usize)
                }
            };
            val.map_or(Reply::Null, Reply::Bulk)
        })
        .await
}

pub async fn lset(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 4 {
        return Reply::wrong_args("lset");
    }
    let key = args[1].clone();
    let value = args[3].clone();
    let Some(index) = parse_i64(&args[2]) else {
        return Reply::not_int();
    };
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let del = match check_type(ctx, &key, head::CTYPE_LIST) {
                Err(()) => return Reply::wrongtype(),
                Ok(d) => d,
            };
            let pairs = live_pairs(ctx, &key, del);
            if pairs.is_empty() {
                return Reply::err("ERR no such key");
            }
            let idx = norm_index(index, pairs.len());
            if idx < 0 || idx as usize >= pairs.len() {
                return Reply::err("ERR index out of range");
            }
            // Overwrite in place — the position (and thus the hint) is stable.
            write_elem(ctx, &key, pairs[idx as usize].0, &value);
            Reply::ok()
        })
        .await
}

pub async fn lrem(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 4 {
        return Reply::wrong_args("lrem");
    }
    let key = args[1].clone();
    let Some(count) = parse_i64(&args[2]) else {
        return Reply::not_int();
    };
    let value = args[3].clone();
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let del = match check_type(ctx, &key, head::CTYPE_LIST) {
                Err(()) => return Reply::wrongtype(),
                Ok(d) => d,
            };
            let items = live_items(ctx, &key, del);
            if items.is_empty() {
                return Reply::Int(0);
            }
            let mut removed = 0i64;
            let kept: Vec<Vec<u8>> = if count >= 0 {
                let limit = if count == 0 {
                    usize::MAX
                } else {
                    count as usize
                };
                let mut out = Vec::with_capacity(items.len());
                for it in items {
                    if (removed as usize) < limit && it == value {
                        removed += 1;
                    } else {
                        out.push(it);
                    }
                }
                out
            } else {
                let limit = count.unsigned_abs() as usize;
                let mut out: Vec<Vec<u8>> = Vec::with_capacity(items.len());
                for it in items.into_iter().rev() {
                    if (removed as usize) < limit && it == value {
                        removed += 1;
                    } else {
                        out.push(it);
                    }
                }
                out.reverse();
                out
            };
            if removed > 0 {
                rebuild(ctx, &key, del, Some(kept));
            }
            Reply::Int(removed)
        })
        .await
}

pub async fn ltrim(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 4 {
        return Reply::wrong_args("ltrim");
    }
    let key = args[1].clone();
    let (Some(start), Some(stop)) = (parse_i64(&args[2]), parse_i64(&args[3])) else {
        return Reply::not_int();
    };
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let del = match check_type(ctx, &key, head::CTYPE_LIST) {
                Err(()) => return Reply::wrongtype(),
                Ok(d) => d,
            };
            let items = live_items(ctx, &key, del);
            if items.is_empty() {
                return Reply::ok();
            }
            let kept = match clamp_range(start, stop, items.len()) {
                None => Vec::new(),
                Some((s, e)) => items[s..e].to_vec(),
            };
            rebuild(ctx, &key, del, Some(kept));
            Reply::ok()
        })
        .await
}

pub async fn linsert(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 5 {
        return Reply::wrong_args("linsert");
    }
    let key = args[1].clone();
    let before = if eq_ignore_case(&args[2], "BEFORE") {
        true
    } else if eq_ignore_case(&args[2], "AFTER") {
        false
    } else {
        return Reply::syntax();
    };
    let pivot = args[3].clone();
    let value = args[4].clone();
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let del = match check_type(ctx, &key, head::CTYPE_LIST) {
                Err(()) => return Reply::wrongtype(),
                Ok(d) => d,
            };
            let mut items = live_items(ctx, &key, del);
            if items.is_empty() {
                return Reply::Int(0);
            }
            let Some(pos) = items.iter().position(|it| it == &pivot) else {
                return Reply::Int(-1);
            };
            let at = if before { pos } else { pos + 1 };
            items.insert(at, value);
            let len = items.len();
            rebuild(ctx, &key, del, Some(items));
            Reply::Int(len as i64)
        })
        .await
}

pub async fn lpos(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 3 {
        return Reply::wrong_args("lpos");
    }
    let key = args[1].clone();
    let element = args[2].clone();
    let mut rank: i64 = 1;
    let mut count: Option<i64> = None;
    let mut maxlen: usize = 0;
    let mut i = 3;
    while i < args.len() {
        if eq_ignore_case(&args[i], "RANK") {
            match args.get(i + 1).and_then(|b| parse_i64(b)) {
                Some(0) => return Reply::err("ERR RANK can't be zero"),
                Some(r) => rank = r,
                None => return Reply::not_int(),
            }
            i += 2;
        } else if eq_ignore_case(&args[i], "COUNT") {
            match args.get(i + 1).and_then(|b| parse_i64(b)) {
                Some(c) if c >= 0 => count = Some(c),
                Some(_) => return Reply::err("ERR COUNT can't be negative"),
                None => return Reply::not_int(),
            }
            i += 2;
        } else if eq_ignore_case(&args[i], "MAXLEN") {
            match args.get(i + 1).and_then(|b| parse_i64(b)) {
                Some(m) if m >= 0 => maxlen = m as usize,
                Some(_) => return Reply::err("ERR MAXLEN can't be negative"),
                None => return Reply::not_int(),
            }
            i += 2;
        } else {
            return Reply::syntax();
        }
    }
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let del = match check_type(ctx, &key, head::CTYPE_LIST) {
                Err(()) => return Reply::wrongtype(),
                Ok(d) => d,
            };
            let items = live_items(ctx, &key, del);
            let want = count.map(|c| if c == 0 { usize::MAX } else { c as usize });
            let skip = (rank.unsigned_abs() - 1) as usize;
            let mut matches: Vec<i64> = Vec::new();
            let mut seen = 0usize;
            let mut compared = 0usize;
            // rank>0 walks head→tail, rank<0 walks tail→head.
            let indices: Vec<usize> = if rank > 0 {
                (0..items.len()).collect()
            } else {
                (0..items.len()).rev().collect()
            };
            for idx in indices {
                compared += 1;
                if maxlen != 0 && compared > maxlen {
                    break;
                }
                if items[idx] == element {
                    if seen >= skip {
                        matches.push(idx as i64);
                        if let Some(w) = want {
                            if matches.len() >= w {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                    seen += 1;
                }
            }
            match count {
                None => matches.first().map_or(Reply::Null, |&i| Reply::Int(i)),
                Some(_) => Reply::Array(matches.into_iter().map(Reply::Int).collect()),
            }
        })
        .await
}

pub async fn lmove(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 5 {
        return Reply::wrong_args("lmove");
    }
    let from_left = if eq_ignore_case(&args[3], "LEFT") {
        true
    } else if eq_ignore_case(&args[3], "RIGHT") {
        false
    } else {
        return Reply::syntax();
    };
    let to_left = if eq_ignore_case(&args[4], "LEFT") {
        true
    } else if eq_ignore_case(&args[4], "RIGHT") {
        false
    } else {
        return Reply::syntax();
    };
    do_move(engine, args[1].clone(), args[2].clone(), from_left, to_left).await
}

pub async fn rpoplpush(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 3 {
        return Reply::wrong_args("rpoplpush");
    }
    do_move(engine, args[1].clone(), args[2].clone(), false, true).await
}

/// Pop one element from `src` (head if `from_left`) and push it onto `dst`
/// (head if `to_left`). Same-key rotation runs in one shard op; otherwise pop
/// then push on their respective shards.
async fn do_move(
    engine: &Arc<Engine>,
    src: Vec<u8>,
    dst: Vec<u8>,
    from_left: bool,
    to_left: bool,
) -> Reply {
    if src == dst {
        let sk = src.clone();
        return engine
            .store
            .run_key(&sk, move |ctx| {
                let del = match check_type(ctx, &src, head::CTYPE_LIST) {
                    Err(()) => return Reply::wrongtype(),
                    Ok(d) => d,
                };
                let Some(v) = do_pop(ctx, &src, from_left, del) else {
                    return Reply::Null;
                };
                ensure_head(ctx, &src, head::CTYPE_LIST);
                do_push(ctx, &src, std::slice::from_ref(&v), to_left, del);
                Reply::Bulk(v)
            })
            .await;
    }

    let s = src.clone();
    let popped: Option<Option<Vec<u8>>> = engine
        .store
        .run_key(&src, move |ctx| {
            match check_type(ctx, &s, head::CTYPE_LIST) {
                Err(()) => None,
                Ok(del) => Some(do_pop(ctx, &s, from_left, del)),
            }
        })
        .await;

    let value = match popped {
        None => return Reply::wrongtype(),
        Some(None) => return Reply::Null,
        Some(Some(v)) => v,
    };

    let d = dst.clone();
    let v = value.clone();
    engine
        .store
        .run_key(&dst, move |ctx| {
            let del = match check_type(ctx, &d, head::CTYPE_LIST) {
                Err(()) => return Reply::wrongtype(),
                Ok(d) => d,
            };
            ensure_head(ctx, &d, head::CTYPE_LIST);
            do_push(ctx, &d, std::slice::from_ref(&v), to_left, del);
            Reply::Bulk(v)
        })
        .await
}

// ---------------------------------------------------------------------------
// Blocking variants (v1.1) — polling implementation, design/03.
//
// The connection task polls the non-blocking operation every POLL_MS until it
// yields or the timeout expires. Shard threads never block; only this client's
// task waits. Wakeup granularity is POLL_MS (documented).
// ---------------------------------------------------------------------------

const POLL_MS: u64 = 50;

fn parse_timeout(b: &[u8]) -> Option<Option<std::time::Duration>> {
    let secs = crate::cmd::parse_f64(b)?;
    if secs < 0.0 {
        return None;
    }
    Some(if secs == 0.0 {
        None // block forever
    } else {
        Some(std::time::Duration::from_secs_f64(secs))
    })
}

/// Try to pop one element from one key (no count, no blocking).
async fn try_pop_one(engine: &Arc<Engine>, key: &[u8], left: bool) -> Result<Option<Vec<u8>>, ()> {
    let k = key.to_vec();
    engine
        .store
        .run_key(key, move |ctx| {
            match check_type(ctx, &k, head::CTYPE_LIST) {
                Err(()) => Err(()),
                Ok(del) => Ok(do_pop(ctx, &k, left, del)),
            }
        })
        .await
}

/// BLPOP / BRPOP: `B*POP key [key ...] timeout` → [key, value] | null array.
pub async fn bpop(engine: &Arc<Engine>, args: &[Vec<u8>], left: bool) -> Reply {
    if args.len() < 3 {
        return Reply::wrong_args("blpop");
    }
    let Some(timeout) = parse_timeout(args.last().unwrap()) else {
        return Reply::err("ERR timeout is negative or not a float");
    };
    let keys: Vec<Vec<u8>> = args[1..args.len() - 1].to_vec();
    let deadline = timeout.map(|d| std::time::Instant::now() + d);
    loop {
        for key in &keys {
            engine.ensure_local(key).await;
            match try_pop_one(engine, key, left).await {
                Err(()) => return Reply::wrongtype(),
                Ok(Some(v)) => return Reply::Array(vec![Reply::Bulk(key.clone()), Reply::Bulk(v)]),
                Ok(None) => {}
            }
        }
        if let Some(d) = deadline {
            if std::time::Instant::now() >= d {
                return Reply::NullArray;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(POLL_MS)).await;
    }
}

/// BLMOVE src dst LEFT|RIGHT LEFT|RIGHT timeout → moved value | null.
pub async fn blmove(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 6 {
        return Reply::wrong_args("blmove");
    }
    let Some(timeout) = parse_timeout(&args[5]) else {
        return Reply::err("ERR timeout is negative or not a float");
    };
    let inner: Vec<Vec<u8>> = vec![
        b"LMOVE".to_vec(),
        args[1].clone(),
        args[2].clone(),
        args[3].clone(),
        args[4].clone(),
    ];
    poll_move(engine, inner, timeout).await
}

/// BRPOPLPUSH src dst timeout → moved value | null.
pub async fn brpoplpush(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 4 {
        return Reply::wrong_args("brpoplpush");
    }
    let Some(timeout) = parse_timeout(&args[3]) else {
        return Reply::err("ERR timeout is negative or not a float");
    };
    let inner: Vec<Vec<u8>> = vec![
        b"LMOVE".to_vec(),
        args[1].clone(),
        args[2].clone(),
        b"RIGHT".to_vec(),
        b"LEFT".to_vec(),
    ];
    poll_move(engine, inner, timeout).await
}

async fn poll_move(
    engine: &Arc<Engine>,
    lmove_args: Vec<Vec<u8>>,
    timeout: Option<std::time::Duration>,
) -> Reply {
    let deadline = timeout.map(|d| std::time::Instant::now() + d);
    loop {
        engine.ensure_local(&lmove_args[1]).await;
        match lmove(engine, &lmove_args).await {
            Reply::Null | Reply::NullArray => {}
            other => return other,
        }
        if let Some(d) = deadline {
            if std::time::Instant::now() >= d {
                return Reply::Null;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(POLL_MS)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamp_range_basics() {
        assert_eq!(clamp_range(0, -1, 5), Some((0, 5)));
        assert_eq!(clamp_range(-2, -1, 5), Some((3, 5)));
        assert_eq!(clamp_range(1, 3, 5), Some((1, 4)));
        assert_eq!(clamp_range(3, 1, 5), None);
        assert_eq!(clamp_range(0, 0, 0), None);
        assert_eq!(clamp_range(-100, 100, 3), Some((0, 3)));
    }
}

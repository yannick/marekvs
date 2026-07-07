//! JSON.* command family (design/16): RedisJSON-compatible surface over the
//! per-path CRDT document model in `marekvs_core::json`.
//!
//! Every handler: `ensure_local` first (writes must observe the dots they
//! cover), then ALL per-key work inside ONE `run_key` closure (node-locally
//! atomic via shard-thread serialization). Index-addressed paths resolve
//! against the LOCAL materialized doc and emit deltas by stable record
//! paths / element ids, so replicas apply to the same nodes.
//!
//! Reply-shape rule (RedisJSON v2): `$…` paths reply per-match arrays with
//! Null slots for non-applicable matches; legacy paths reply bare values and
//! error where RedisJSON's legacy dialect errors.

pub(crate) mod doc;
pub(crate) mod path;

use std::sync::Arc;

use crate::cmd::{eq_ignore_case, generic, parse_i64};
use crate::reply::Reply;
use crate::store::{self, scan_prefix, ShardCtx};
use crate::Engine;
use marekvs_core::envelope::head;
use marekvs_core::ikey;
use marekvs_core::json::{jval_of, push_seg, Doc, JVal, Seg, EID_HEAD};
use serde_json::Value;

use path::{Dialect, Loc, LocElem, ParsedPath, StaticTarget};

// ---------------------------------------------------------------------------
// shared helpers
// ---------------------------------------------------------------------------

/// The materialized value at a resolved location.
fn value_at<'v>(root: &'v Value, loc: &Loc) -> Option<&'v Value> {
    let mut cur = root;
    for e in loc {
        cur = match e {
            LocElem::Key(k) => cur.get(k.as_str())?,
            LocElem::Index(i) => cur.get(*i)?,
        };
    }
    Some(cur)
}

/// Resolved matches with their stable record paths.
fn targets(d: &Doc, pp: &ParsedPath) -> Vec<(Loc, Vec<u8>)> {
    path::resolve_read(&d.value, pp)
        .into_iter()
        .filter_map(|loc| path::loc_to_record_path(&loc, &d.index).map(|rp| (loc, rp)))
        .collect()
}

/// Shape a per-match result list by dialect. `legacy_missing` is the reply
/// when a legacy path had no applicable match.
fn shape(dialect: Dialect, results: Vec<Option<Reply>>, legacy_missing: Reply) -> Reply {
    match dialect {
        Dialect::Query => Reply::Array(
            results
                .into_iter()
                .map(|o| o.unwrap_or(Reply::Null))
                .collect(),
        ),
        Dialect::Legacy => results
            .into_iter()
            .flatten()
            .next()
            .unwrap_or(legacy_missing),
    }
}

fn parse_json(bytes: &[u8]) -> Result<Value, Reply> {
    serde_json::from_slice(bytes).map_err(|_| Reply::err("ERR invalid JSON value"))
}

fn parse_path(arg: &[u8]) -> Result<ParsedPath, Reply> {
    path::parse(arg).map_err(Reply::Err)
}

const ROOT_ONLY: &str = "ERR new objects must be created at the root";

// ---------------------------------------------------------------------------
// serialization with INDENT/NEWLINE/SPACE
// ---------------------------------------------------------------------------

#[derive(Default, Clone)]
struct FmtOpts {
    indent: Vec<u8>,
    newline: Vec<u8>,
    space: Vec<u8>,
}

impl FmtOpts {
    fn is_plain(&self) -> bool {
        self.indent.is_empty() && self.newline.is_empty() && self.space.is_empty()
    }
}

/// RedisJSON-style formatter: NEWLINE between items, INDENT per level,
/// SPACE after the object-key colon (serde_json PrettyFormatter shape with
/// caller-supplied strings).
struct RjFmt<'a> {
    o: &'a FmtOpts,
    depth: usize,
    has_value: bool,
}

impl RjFmt<'_> {
    fn newline_indent<W: ?Sized + std::io::Write>(&self, w: &mut W) -> std::io::Result<()> {
        w.write_all(&self.o.newline)?;
        for _ in 0..self.depth {
            w.write_all(&self.o.indent)?;
        }
        Ok(())
    }
}

impl serde_json::ser::Formatter for RjFmt<'_> {
    fn begin_array<W: ?Sized + std::io::Write>(&mut self, w: &mut W) -> std::io::Result<()> {
        self.depth += 1;
        self.has_value = false;
        w.write_all(b"[")
    }

    fn end_array<W: ?Sized + std::io::Write>(&mut self, w: &mut W) -> std::io::Result<()> {
        self.depth -= 1;
        if self.has_value {
            self.newline_indent(w)?;
        }
        w.write_all(b"]")
    }

    fn begin_array_value<W: ?Sized + std::io::Write>(
        &mut self,
        w: &mut W,
        first: bool,
    ) -> std::io::Result<()> {
        if !first {
            w.write_all(b",")?;
        }
        self.newline_indent(w)
    }

    fn end_array_value<W: ?Sized + std::io::Write>(&mut self, _w: &mut W) -> std::io::Result<()> {
        self.has_value = true;
        Ok(())
    }

    fn begin_object<W: ?Sized + std::io::Write>(&mut self, w: &mut W) -> std::io::Result<()> {
        self.depth += 1;
        self.has_value = false;
        w.write_all(b"{")
    }

    fn end_object<W: ?Sized + std::io::Write>(&mut self, w: &mut W) -> std::io::Result<()> {
        self.depth -= 1;
        if self.has_value {
            self.newline_indent(w)?;
        }
        w.write_all(b"}")
    }

    fn begin_object_key<W: ?Sized + std::io::Write>(
        &mut self,
        w: &mut W,
        first: bool,
    ) -> std::io::Result<()> {
        if !first {
            w.write_all(b",")?;
        }
        self.newline_indent(w)
    }

    fn begin_object_value<W: ?Sized + std::io::Write>(&mut self, w: &mut W) -> std::io::Result<()> {
        w.write_all(b":")?;
        w.write_all(&self.o.space)
    }

    fn end_object_value<W: ?Sized + std::io::Write>(&mut self, _w: &mut W) -> std::io::Result<()> {
        self.has_value = true;
        Ok(())
    }
}

fn ser_value(v: &Value, o: &FmtOpts) -> Vec<u8> {
    if o.is_plain() {
        return serde_json::to_vec(v).unwrap_or_default();
    }
    let mut out = Vec::new();
    let fmt = RjFmt {
        o,
        depth: 0,
        has_value: false,
    };
    let mut ser = serde_json::Serializer::with_formatter(&mut out, fmt);
    if serde::Serialize::serialize(v, &mut ser).is_err() {
        return serde_json::to_vec(v).unwrap_or_default();
    }
    out
}

// ---------------------------------------------------------------------------
// JSON.SET / JSON.MSET
// ---------------------------------------------------------------------------

fn set_in(ctx: &ShardCtx, key: &[u8], pp: &ParsedPath, value: &Value, nx: bool, xx: bool) -> Reply {
    if doc::other_type_holds(ctx, key) {
        return Reply::wrongtype();
    }
    let loaded = doc::load_doc(ctx, key);
    if pp.is_root() {
        if (loaded.is_some() && nx) || (loaded.is_none() && xx) {
            return Reply::Null;
        }
        let _ = store::ensure_head(ctx, key, head::CTYPE_JSON);
        // re-read: ensure_head may have carried forward a delete clock
        match &loaded {
            Some((d, del)) => doc::replace_subtree(ctx, key, &[], value, &d.index, *del),
            None => {
                doc::write_map_node(ctx, key, &[], &jval_of(value), &[]);
                doc::write_children(ctx, key, &[], value);
            }
        }
        return Reply::Simple("OK");
    }
    let Some((d, del)) = loaded else {
        return Reply::err(ROOT_ONLY);
    };
    let matches = targets(&d, pp);
    if !matches.is_empty() {
        if nx {
            return Reply::Null;
        }
        for (_, rp) in &matches {
            doc::replace_subtree(ctx, key, rp, value, &d.index, del);
        }
        return Reply::Simple("OK");
    }
    if let Some(segs) = &pp.static_segs {
        if let StaticTarget::NewKey { parent, key: fname } = path::resolve_static(&d.value, segs) {
            if xx {
                return Reply::Null;
            }
            if let Some(mut rp) = path::loc_to_record_path(&parent, &d.index) {
                push_seg(&mut rp, &Seg::Field(fname.into_bytes()));
                doc::write_map_node(ctx, key, &rp, &jval_of(value), &[]);
                doc::write_children(ctx, key, &rp, value);
                return Reply::Simple("OK");
            }
        }
    }
    Reply::err(ROOT_ONLY)
}

pub async fn set(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 4 || args.len() > 5 {
        return Reply::wrong_args("json.set");
    }
    let pp = match parse_path(&args[2]) {
        Ok(p) => p,
        Err(e) => return e,
    };
    let value = match parse_json(&args[3]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let (mut nx, mut xx) = (false, false);
    if let Some(flag) = args.get(4) {
        if eq_ignore_case(flag, "NX") {
            nx = true;
        } else if eq_ignore_case(flag, "XX") {
            xx = true;
        } else {
            return Reply::syntax();
        }
    }
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| set_in(ctx, &key, &pp, &value, nx, xx))
        .await
}

pub async fn mset(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 4 || (args.len() - 1) % 3 != 0 {
        return Reply::wrong_args("json.mset");
    }
    // validate everything up front; the writes are still per-key only
    let mut triplets = Vec::new();
    for chunk in args[1..].chunks(3) {
        let pp = match parse_path(&chunk[1]) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let value = match parse_json(&chunk[2]) {
            Ok(v) => v,
            Err(e) => return e,
        };
        triplets.push((chunk[0].clone(), pp, value));
    }
    for (key, pp, value) in triplets {
        engine.ensure_local(&key).await;
        let k = key.clone();
        let r = engine
            .store
            .run_key(&key, move |ctx| set_in(ctx, &k, &pp, &value, false, false))
            .await;
        if matches!(r, Reply::Err(_)) {
            return r; // NOT atomic across keys (documented AP gap)
        }
    }
    Reply::Simple("OK")
}

// ---------------------------------------------------------------------------
// JSON.GET / JSON.MGET
// ---------------------------------------------------------------------------

/// Evaluate one path: query → array of matches; legacy → the single match
/// (None = legacy path missing).
fn eval_path(docv: &Value, pp: &ParsedPath) -> Option<Value> {
    let locs = path::resolve_read(docv, pp);
    match pp.dialect {
        Dialect::Query => Some(Value::Array(
            locs.iter()
                .filter_map(|l| value_at(docv, l).cloned())
                .collect(),
        )),
        Dialect::Legacy => locs.first().and_then(|l| value_at(docv, l)).cloned(),
    }
}

pub async fn get(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 2 {
        return Reply::wrong_args("json.get");
    }
    let mut fmt = FmtOpts::default();
    let mut paths: Vec<(Vec<u8>, ParsedPath)> = Vec::new();
    let mut i = 2;
    while i < args.len() {
        let arg = &args[i];
        if eq_ignore_case(arg, "INDENT")
            || eq_ignore_case(arg, "NEWLINE")
            || eq_ignore_case(arg, "SPACE")
        {
            let Some(v) = args.get(i + 1) else {
                return Reply::syntax();
            };
            if eq_ignore_case(arg, "INDENT") {
                fmt.indent = v.clone();
            } else if eq_ignore_case(arg, "NEWLINE") {
                fmt.newline = v.clone();
            } else {
                fmt.space = v.clone();
            }
            i += 2;
        } else if eq_ignore_case(arg, "NOESCAPE") {
            i += 1; // legacy no-op, accepted for compatibility
        } else {
            match parse_path(arg) {
                Ok(p) => paths.push((arg.clone(), p)),
                Err(e) => return e,
            }
            i += 1;
        }
    }
    if paths.is_empty() {
        paths.push((b".".to_vec(), parse_path(b".").expect("root parses")));
    }
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            if doc::other_type_holds(ctx, &key) {
                return Reply::wrongtype();
            }
            let Some((d, _)) = doc::load_doc(ctx, &key) else {
                return Reply::Null;
            };
            if paths.len() == 1 {
                let (_, pp) = &paths[0];
                return match eval_path(&d.value, pp) {
                    Some(v) => Reply::Bulk(ser_value(&v, &fmt)),
                    None => Reply::Null,
                };
            }
            let mut obj = serde_json::Map::with_capacity(paths.len());
            for (orig, pp) in &paths {
                let v = eval_path(&d.value, pp).unwrap_or(Value::Null);
                obj.insert(String::from_utf8_lossy(orig).into_owned(), v);
            }
            Reply::Bulk(ser_value(&Value::Object(obj), &fmt))
        })
        .await
}

pub async fn mget(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 3 {
        return Reply::wrong_args("json.mget");
    }
    let pp = match parse_path(args.last().expect("checked len")) {
        Ok(p) => p,
        Err(e) => return e,
    };
    let mut out = Vec::with_capacity(args.len() - 2);
    for keyarg in &args[1..args.len() - 1] {
        engine.ensure_local(keyarg).await;
        let key = keyarg.clone();
        let pp = ParsedPath {
            dialect: pp.dialect,
            query: pp.query.clone(),
            static_segs: pp.static_segs.clone(),
        };
        let r = engine
            .store
            .run_key(keyarg, move |ctx| {
                let Some((d, _)) = doc::load_doc(ctx, &key) else {
                    return Reply::Null;
                };
                match eval_path(&d.value, &pp) {
                    Some(v) => Reply::Bulk(ser_value(&v, &FmtOpts::default())),
                    None => Reply::Null,
                }
            })
            .await;
        out.push(r);
    }
    Reply::Array(out)
}

// ---------------------------------------------------------------------------
// JSON.DEL / JSON.FORGET
// ---------------------------------------------------------------------------

pub async fn del(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 2 || args.len() > 3 {
        return Reply::wrong_args("json.del");
    }
    let pp = match args.get(2) {
        Some(p) => match parse_path(p) {
            Ok(p) => p,
            Err(e) => return e,
        },
        None => parse_path(b".").expect("root parses"),
    };
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            if doc::other_type_holds(ctx, &key) {
                return Reply::wrongtype();
            }
            let Some((d, del_hlc)) = doc::load_doc(ctx, &key) else {
                return Reply::Int(0);
            };
            if pp.is_root() {
                return Reply::Int(generic::del_key(ctx, &key) as i64);
            }
            let matches = targets(&d, &pp);
            let n = matches.len() as i64;
            for (loc, rp) in matches {
                if loc.is_empty() {
                    generic::del_key(ctx, &key);
                } else {
                    doc::delete_subtree(ctx, &key, &rp, &d.index, del_hlc);
                }
            }
            Reply::Int(n)
        })
        .await
}

// ---------------------------------------------------------------------------
// JSON.TYPE
// ---------------------------------------------------------------------------

fn type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(n) if n.is_i64() || n.is_u64() => "integer",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

pub async fn type_cmd(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 2 || args.len() > 3 {
        return Reply::wrong_args("json.type");
    }
    let pp = match args.get(2) {
        Some(p) => match parse_path(p) {
            Ok(p) => p,
            Err(e) => return e,
        },
        None => parse_path(b".").expect("root parses"),
    };
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Some((d, _)) = doc::load_doc(ctx, &key) else {
                return Reply::Null;
            };
            let results = path::resolve_read(&d.value, &pp)
                .iter()
                .map(|l| value_at(&d.value, l).map(|v| Reply::bulk_str(type_name(v))))
                .collect();
            shape(pp.dialect, results, Reply::Null)
        })
        .await
}

// ---------------------------------------------------------------------------
// JSON.NUMINCRBY / JSON.NUMMULTBY
// ---------------------------------------------------------------------------

/// Apply the numeric op. Integral float results normalize to integers
/// (documented formatting deviation; matches RedisJSON's visible output).
fn num_apply(cur: &Value, delta: &Value, mult: bool) -> Option<Value> {
    if !cur.is_number() {
        return None;
    }
    if let (Some(a), Some(b)) = (cur.as_i64(), delta.as_i64()) {
        let r = if mult {
            a.checked_mul(b)
        } else {
            a.checked_add(b)
        };
        if let Some(r) = r {
            return Some(Value::Number(r.into()));
        }
    }
    let a = cur.as_f64()?;
    let b = delta.as_f64()?;
    let r = if mult { a * b } else { a + b };
    if !r.is_finite() {
        return None;
    }
    if r.fract() == 0.0 && r.abs() < 9.0e15 {
        return Some(Value::Number((r as i64).into()));
    }
    serde_json::Number::from_f64(r).map(Value::Number)
}

pub async fn numop(engine: &Arc<Engine>, args: &[Vec<u8>], mult: bool) -> Reply {
    if args.len() != 4 {
        return Reply::wrong_args(if mult {
            "json.nummultby"
        } else {
            "json.numincrby"
        });
    }
    let pp = match parse_path(&args[2]) {
        Ok(p) => p,
        Err(e) => return e,
    };
    let Ok(delta) = serde_json::from_slice::<Value>(&args[3]) else {
        return Reply::err("ERR value is not a number");
    };
    if !delta.is_number() {
        return Reply::err("ERR value is not a number");
    }
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            if doc::other_type_holds(ctx, &key) {
                return Reply::wrongtype();
            }
            let Some((d, _)) = doc::load_doc(ctx, &key) else {
                return Reply::err("ERR no such key");
            };
            let mut results: Vec<Option<Value>> = Vec::new();
            for (loc, rp) in targets(&d, &pp) {
                let updated = value_at(&d.value, &loc).and_then(|cur| num_apply(cur, &delta, mult));
                if let Some(v) = &updated {
                    doc::update_scalar_node(ctx, &key, &rp, &jval_of(v), &d.index);
                }
                results.push(updated);
            }
            match pp.dialect {
                Dialect::Query => Reply::Bulk(serde_json::to_vec(&results).unwrap_or_default()),
                Dialect::Legacy => match results.into_iter().flatten().next() {
                    Some(v) => Reply::Bulk(serde_json::to_vec(&v).unwrap_or_default()),
                    None => Reply::err("ERR path is not a number"),
                },
            }
        })
        .await
}

// ---------------------------------------------------------------------------
// JSON.STRAPPEND / JSON.STRLEN / JSON.TOGGLE
// ---------------------------------------------------------------------------

pub async fn strappend(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 3 || args.len() > 4 {
        return Reply::wrong_args("json.strappend");
    }
    let (patharg, valarg): (&[u8], &[u8]) = if args.len() == 4 {
        (&args[2], &args[3])
    } else {
        (b".", &args[2])
    };
    let pp = match parse_path(patharg) {
        Ok(p) => p,
        Err(e) => return e,
    };
    let Ok(Value::String(suffix)) = serde_json::from_slice::<Value>(valarg) else {
        return Reply::err("ERR expected a JSON string argument");
    };
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            if doc::other_type_holds(ctx, &key) {
                return Reply::wrongtype();
            }
            let Some((d, _)) = doc::load_doc(ctx, &key) else {
                return Reply::err("ERR no such key");
            };
            let mut results: Vec<Option<Reply>> = Vec::new();
            for (loc, rp) in targets(&d, &pp) {
                let updated = value_at(&d.value, &loc).and_then(|v| v.as_str()).map(|s| {
                    let ns = format!("{s}{suffix}");
                    let len = ns.len() as i64;
                    doc::update_scalar_node(ctx, &key, &rp, &JVal::Str(ns.into_bytes()), &d.index);
                    Reply::Int(len)
                });
                results.push(updated);
            }
            shape(
                pp.dialect,
                results,
                Reply::err("ERR path does not hold a string"),
            )
        })
        .await
}

pub async fn strlen(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 2 || args.len() > 3 {
        return Reply::wrong_args("json.strlen");
    }
    let pp = match args.get(2) {
        Some(p) => match parse_path(p) {
            Ok(p) => p,
            Err(e) => return e,
        },
        None => parse_path(b".").expect("root parses"),
    };
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Some((d, _)) = doc::load_doc(ctx, &key) else {
                return Reply::Null;
            };
            let results = path::resolve_read(&d.value, &pp)
                .iter()
                .map(|l| {
                    value_at(&d.value, l)
                        .and_then(|v| v.as_str())
                        .map(|s| Reply::Int(s.len() as i64))
                })
                .collect();
            shape(
                pp.dialect,
                results,
                Reply::err("ERR path does not hold a string"),
            )
        })
        .await
}

pub async fn toggle(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 3 {
        return Reply::wrong_args("json.toggle");
    }
    let pp = match parse_path(&args[2]) {
        Ok(p) => p,
        Err(e) => return e,
    };
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            if doc::other_type_holds(ctx, &key) {
                return Reply::wrongtype();
            }
            let Some((d, _)) = doc::load_doc(ctx, &key) else {
                return Reply::err("ERR no such key");
            };
            let mut results: Vec<Option<bool>> = Vec::new();
            for (loc, rp) in targets(&d, &pp) {
                let updated = value_at(&d.value, &loc).and_then(Value::as_bool).map(|b| {
                    doc::update_scalar_node(ctx, &key, &rp, &JVal::Bool(!b), &d.index);
                    !b
                });
                results.push(updated);
            }
            match pp.dialect {
                Dialect::Query => Reply::Array(
                    results
                        .into_iter()
                        .map(|o| o.map_or(Reply::Null, |b| Reply::Int(b as i64)))
                        .collect(),
                ),
                Dialect::Legacy => match results.into_iter().flatten().next() {
                    Some(b) => Reply::bulk_str(if b { "true" } else { "false" }),
                    None => Reply::err("ERR path does not hold a boolean"),
                },
            }
        })
        .await
}

// ---------------------------------------------------------------------------
// JSON.ARR*
// ---------------------------------------------------------------------------

fn path_arg_or_root(args: &[Vec<u8>], i: usize) -> Result<ParsedPath, Reply> {
    match args.get(i) {
        Some(p) => parse_path(p),
        None => Ok(parse_path(b".").expect("root parses")),
    }
}

const NOT_ARRAY: &str = "ERR path does not hold an array";

pub async fn arrappend(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 4 {
        return Reply::wrong_args("json.arrappend");
    }
    let pp = match parse_path(&args[2]) {
        Ok(p) => p,
        Err(e) => return e,
    };
    let mut values = Vec::new();
    for v in &args[3..] {
        match parse_json(v) {
            Ok(v) => values.push(v),
            Err(e) => return e,
        }
    }
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            if doc::other_type_holds(ctx, &key) {
                return Reply::wrongtype();
            }
            let Some((d, _)) = doc::load_doc(ctx, &key) else {
                return Reply::err("ERR no such key");
            };
            let mut results: Vec<Option<Reply>> = Vec::new();
            for (loc, _) in targets(&d, &pp) {
                let appended = path::array_info(&loc, &d.index).map(|(info, apath)| {
                    let mut left = info.last.unwrap_or(EID_HEAD);
                    for v in &values {
                        let e = doc::fresh_eid(ctx);
                        let mut ep = apath.clone();
                        push_seg(&mut ep, &Seg::Elem(e));
                        doc::write_arr_node(
                            ctx,
                            &key,
                            &ep,
                            &marekvs_core::json::ArrElem {
                                left,
                                val: jval_of(v),
                            },
                        );
                        doc::write_children(ctx, &key, &ep, v);
                        left = e;
                    }
                    Reply::Int((info.order.len() + values.len()) as i64)
                });
                results.push(appended);
            }
            shape(pp.dialect, results, Reply::err(NOT_ARRAY))
        })
        .await
}

pub async fn arrindex(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 4 || args.len() > 6 {
        return Reply::wrong_args("json.arrindex");
    }
    let pp = match parse_path(&args[2]) {
        Ok(p) => p,
        Err(e) => return e,
    };
    let needle = match parse_json(&args[3]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let start = match args.get(4) {
        Some(b) => match parse_i64(b) {
            Some(n) => n,
            None => return Reply::not_int(),
        },
        None => 0,
    };
    let stop = match args.get(5) {
        Some(b) => match parse_i64(b) {
            Some(n) => n,
            None => return Reply::not_int(),
        },
        None => 0, // 0 = to the end (RedisJSON convention)
    };
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Some((d, _)) = doc::load_doc(ctx, &key) else {
                return Reply::err("ERR no such key");
            };
            let results = path::resolve_read(&d.value, &pp)
                .iter()
                .map(|loc| {
                    let arr = value_at(&d.value, loc)?.as_array()?;
                    let len = arr.len() as i64;
                    let s = if start < 0 {
                        (len + start).max(0)
                    } else {
                        start.min(len)
                    };
                    let e = if stop <= 0 {
                        len + stop
                    } else {
                        stop.min(len - 1)
                    };
                    for i in s..=e.min(len - 1) {
                        if i >= 0 && arr[i as usize] == needle {
                            return Some(Reply::Int(i));
                        }
                    }
                    Some(Reply::Int(-1))
                })
                .collect();
            shape(pp.dialect, results, Reply::err(NOT_ARRAY))
        })
        .await
}

pub async fn arrinsert(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 5 {
        return Reply::wrong_args("json.arrinsert");
    }
    let pp = match parse_path(&args[2]) {
        Ok(p) => p,
        Err(e) => return e,
    };
    let Some(idx) = parse_i64(&args[3]) else {
        return Reply::not_int();
    };
    let mut values = Vec::new();
    for v in &args[4..] {
        match parse_json(v) {
            Ok(v) => values.push(v),
            Err(e) => return e,
        }
    }
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            if doc::other_type_holds(ctx, &key) {
                return Reply::wrongtype();
            }
            let Some((d, _)) = doc::load_doc(ctx, &key) else {
                return Reply::err("ERR no such key");
            };
            let mut out_of_range = false;
            let mut results: Vec<Option<Reply>> = Vec::new();
            for (loc, _) in targets(&d, &pp) {
                let inserted = path::array_info(&loc, &d.index).and_then(|(info, apath)| {
                    let n = info.order.len() as i64;
                    let pos = if idx < 0 { n + idx } else { idx };
                    if !(0..=n).contains(&pos) {
                        out_of_range = true;
                        return None;
                    }
                    let mut left = if pos == 0 {
                        EID_HEAD
                    } else {
                        info.order[pos as usize - 1]
                    };
                    for v in &values {
                        let e = doc::fresh_eid(ctx);
                        let mut ep = apath.clone();
                        push_seg(&mut ep, &Seg::Elem(e));
                        doc::write_arr_node(
                            ctx,
                            &key,
                            &ep,
                            &marekvs_core::json::ArrElem {
                                left,
                                val: jval_of(v),
                            },
                        );
                        doc::write_children(ctx, &key, &ep, v);
                        left = e;
                    }
                    Some(Reply::Int(n + values.len() as i64))
                });
                results.push(inserted);
            }
            if out_of_range && results.iter().all(Option::is_none) {
                return Reply::err("ERR index out of range");
            }
            shape(pp.dialect, results, Reply::err(NOT_ARRAY))
        })
        .await
}

pub async fn arrlen(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 2 || args.len() > 3 {
        return Reply::wrong_args("json.arrlen");
    }
    let pp = match path_arg_or_root(args, 2) {
        Ok(p) => p,
        Err(e) => return e,
    };
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Some((d, _)) = doc::load_doc(ctx, &key) else {
                return Reply::Null;
            };
            let results = path::resolve_read(&d.value, &pp)
                .iter()
                .map(|loc| {
                    value_at(&d.value, loc)
                        .and_then(Value::as_array)
                        .map(|a| Reply::Int(a.len() as i64))
                })
                .collect();
            shape(pp.dialect, results, Reply::err(NOT_ARRAY))
        })
        .await
}

pub async fn arrpop(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 2 || args.len() > 4 {
        return Reply::wrong_args("json.arrpop");
    }
    let pp = match path_arg_or_root(args, 2) {
        Ok(p) => p,
        Err(e) => return e,
    };
    let idx = match args.get(3) {
        Some(b) => match parse_i64(b) {
            Some(n) => n,
            None => return Reply::not_int(),
        },
        None => -1,
    };
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            if doc::other_type_holds(ctx, &key) {
                return Reply::wrongtype();
            }
            let Some((d, del_hlc)) = doc::load_doc(ctx, &key) else {
                return Reply::err("ERR no such key");
            };
            let mut results: Vec<Option<Reply>> = Vec::new();
            for (loc, _) in targets(&d, &pp) {
                let popped = (|| {
                    let arr = value_at(&d.value, &loc)?.as_array()?;
                    if arr.is_empty() {
                        return None;
                    }
                    // out-of-range pop indexes clamp to the boundaries
                    let len = arr.len() as i64;
                    let pos = if idx < 0 {
                        (len + idx).max(0)
                    } else {
                        idx.min(len - 1)
                    } as usize;
                    let (info, apath) = path::array_info(&loc, &d.index)?;
                    let eid = *info.order.get(pos)?;
                    let mut ep = apath.clone();
                    push_seg(&mut ep, &Seg::Elem(eid));
                    let val = serde_json::to_vec(&arr[pos]).unwrap_or_default();
                    doc::delete_subtree(ctx, &key, &ep, &d.index, del_hlc);
                    Some(Reply::Bulk(val))
                })();
                results.push(popped);
            }
            shape(pp.dialect, results, Reply::Null)
        })
        .await
}

pub async fn arrtrim(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 5 {
        return Reply::wrong_args("json.arrtrim");
    }
    let pp = match parse_path(&args[2]) {
        Ok(p) => p,
        Err(e) => return e,
    };
    let (Some(start), Some(stop)) = (parse_i64(&args[3]), parse_i64(&args[4])) else {
        return Reply::not_int();
    };
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            if doc::other_type_holds(ctx, &key) {
                return Reply::wrongtype();
            }
            let Some((d, del_hlc)) = doc::load_doc(ctx, &key) else {
                return Reply::err("ERR no such key");
            };
            let mut results: Vec<Option<Reply>> = Vec::new();
            for (loc, _) in targets(&d, &pp) {
                let trimmed = path::array_info(&loc, &d.index).map(|(info, apath)| {
                    let len = info.order.len() as i64;
                    let s = if start < 0 {
                        (len + start).max(0)
                    } else {
                        start
                    };
                    let e = if stop < 0 {
                        len + stop
                    } else {
                        stop.min(len - 1)
                    };
                    let mut kept = 0i64;
                    for (i, eid) in info.order.iter().enumerate() {
                        let i = i as i64;
                        if i >= s && i <= e {
                            kept += 1;
                            continue;
                        }
                        let mut ep = apath.clone();
                        push_seg(&mut ep, &Seg::Elem(*eid));
                        doc::delete_subtree(ctx, &key, &ep, &d.index, del_hlc);
                    }
                    Reply::Int(kept)
                });
                results.push(trimmed);
            }
            shape(pp.dialect, results, Reply::err(NOT_ARRAY))
        })
        .await
}

// ---------------------------------------------------------------------------
// JSON.OBJKEYS / JSON.OBJLEN
// ---------------------------------------------------------------------------

pub async fn objkeys(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 2 || args.len() > 3 {
        return Reply::wrong_args("json.objkeys");
    }
    let pp = match path_arg_or_root(args, 2) {
        Ok(p) => p,
        Err(e) => return e,
    };
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Some((d, _)) = doc::load_doc(ctx, &key) else {
                return Reply::Null;
            };
            let results = path::resolve_read(&d.value, &pp)
                .iter()
                .map(|loc| {
                    value_at(&d.value, loc).and_then(Value::as_object).map(|m| {
                        // lexicographic (serde_json maps are ordered) —
                        // documented deviation from insertion order
                        Reply::Array(m.keys().map(String::as_str).map(Reply::bulk_str).collect())
                    })
                })
                .collect();
            shape(pp.dialect, results, Reply::Null)
        })
        .await
}

pub async fn objlen(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 2 || args.len() > 3 {
        return Reply::wrong_args("json.objlen");
    }
    let pp = match path_arg_or_root(args, 2) {
        Ok(p) => p,
        Err(e) => return e,
    };
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Some((d, _)) = doc::load_doc(ctx, &key) else {
                return Reply::Null;
            };
            let results = path::resolve_read(&d.value, &pp)
                .iter()
                .map(|loc| {
                    value_at(&d.value, loc)
                        .and_then(Value::as_object)
                        .map(|m| Reply::Int(m.len() as i64))
                })
                .collect();
            shape(pp.dialect, results, Reply::Null)
        })
        .await
}

// ---------------------------------------------------------------------------
// JSON.CLEAR
// ---------------------------------------------------------------------------

pub async fn clear(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 2 || args.len() > 3 {
        return Reply::wrong_args("json.clear");
    }
    let pp = match path_arg_or_root(args, 2) {
        Ok(p) => p,
        Err(e) => return e,
    };
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            if doc::other_type_holds(ctx, &key) {
                return Reply::wrongtype();
            }
            let Some((d, del_hlc)) = doc::load_doc(ctx, &key) else {
                return Reply::Int(0);
            };
            let mut cleared = 0i64;
            for (loc, rp) in targets(&d, &pp) {
                match value_at(&d.value, &loc) {
                    Some(Value::Object(_)) | Some(Value::Array(_)) => {
                        doc::cover_descendants(ctx, &key, &rp, del_hlc);
                        cleared += 1;
                    }
                    Some(Value::Number(n)) if n.as_f64() != Some(0.0) => {
                        doc::update_scalar_node(ctx, &key, &rp, &JVal::Int(0), &d.index);
                        cleared += 1;
                    }
                    _ => {}
                }
            }
            Reply::Int(cleared)
        })
        .await
}

// ---------------------------------------------------------------------------
// JSON.MERGE (RFC 7386)
// ---------------------------------------------------------------------------

/// RFC 7386: object members with null values are removed when the patch is
/// used as a replacement value.
fn strip_nulls(v: &Value) -> Value {
    match v {
        Value::Object(m) => Value::Object(
            m.iter()
                .filter(|(_, v)| !v.is_null())
                .map(|(k, v)| (k.clone(), strip_nulls(v)))
                .collect(),
        ),
        Value::Array(a) => Value::Array(a.iter().map(strip_nulls).collect()),
        other => other.clone(),
    }
}

fn apply_merge(
    ctx: &ShardCtx,
    key: &[u8],
    d: &Doc,
    del_hlc: u64,
    rp: &[u8],
    cur: &Value,
    patch: &Value,
) {
    match (cur, patch) {
        (Value::Object(curm), Value::Object(patchm)) => {
            for (k, pv) in patchm {
                let mut crp = rp.to_vec();
                push_seg(&mut crp, &Seg::Field(k.as_bytes().to_vec()));
                match (curm.get(k), pv) {
                    (Some(_), Value::Null) => {
                        doc::delete_subtree(ctx, key, &crp, &d.index, del_hlc)
                    }
                    (Some(cv @ Value::Object(_)), Value::Object(_)) => {
                        apply_merge(ctx, key, d, del_hlc, &crp, cv, pv)
                    }
                    (Some(_), _) => {
                        doc::replace_subtree(ctx, key, &crp, &strip_nulls(pv), &d.index, del_hlc)
                    }
                    (None, Value::Null) => {}
                    (None, _) => {
                        let v = strip_nulls(pv);
                        doc::write_map_node(ctx, key, &crp, &jval_of(&v), &[]);
                        doc::write_children(ctx, key, &crp, &v);
                    }
                }
            }
        }
        _ => doc::replace_subtree(ctx, key, rp, &strip_nulls(patch), &d.index, del_hlc),
    }
}

pub async fn merge(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 4 {
        return Reply::wrong_args("json.merge");
    }
    let pp = match parse_path(&args[2]) {
        Ok(p) => p,
        Err(e) => return e,
    };
    let patch = match parse_json(&args[3]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            if doc::other_type_holds(ctx, &key) {
                return Reply::wrongtype();
            }
            let loaded = doc::load_doc(ctx, &key);
            let Some((d, del_hlc)) = loaded else {
                // missing doc: root MERGE behaves like JSON.SET root
                if pp.is_root() {
                    if patch.is_null() {
                        return Reply::Simple("OK");
                    }
                    let v = strip_nulls(&patch);
                    let _ = store::ensure_head(ctx, &key, head::CTYPE_JSON);
                    doc::write_map_node(ctx, &key, &[], &jval_of(&v), &[]);
                    doc::write_children(ctx, &key, &[], &v);
                    return Reply::Simple("OK");
                }
                return Reply::err(ROOT_ONLY);
            };
            if pp.is_root() && patch.is_null() {
                generic::del_key(ctx, &key);
                return Reply::Simple("OK");
            }
            let matches = targets(&d, &pp);
            if !matches.is_empty() {
                for (loc, rp) in &matches {
                    if patch.is_null() {
                        if loc.is_empty() {
                            generic::del_key(ctx, &key);
                        } else {
                            doc::delete_subtree(ctx, &key, rp, &d.index, del_hlc);
                        }
                        continue;
                    }
                    let cur = value_at(&d.value, loc).cloned().unwrap_or(Value::Null);
                    apply_merge(ctx, &key, &d, del_hlc, rp, &cur, &patch);
                }
                return Reply::Simple("OK");
            }
            // no match: create like JSON.SET (nulls stripped per RFC 7386)
            if let Some(segs) = &pp.static_segs {
                if let StaticTarget::NewKey { parent, key: fname } =
                    path::resolve_static(&d.value, segs)
                {
                    if !patch.is_null() {
                        if let Some(mut rp) = path::loc_to_record_path(&parent, &d.index) {
                            push_seg(&mut rp, &Seg::Field(fname.into_bytes()));
                            let v = strip_nulls(&patch);
                            doc::write_map_node(ctx, &key, &rp, &jval_of(&v), &[]);
                            doc::write_children(ctx, &key, &rp, &v);
                        }
                    }
                    return Reply::Simple("OK");
                }
            }
            Reply::err(ROOT_ONLY)
        })
        .await
}

// ---------------------------------------------------------------------------
// JSON.RESP / JSON.DEBUG
// ---------------------------------------------------------------------------

fn resp_of(v: &Value) -> Reply {
    match v {
        Value::Null => Reply::Null,
        Value::Bool(b) => Reply::bulk_str(if *b { "true" } else { "false" }),
        Value::Number(n) => match n.as_i64() {
            Some(i) => Reply::Int(i),
            None => Reply::Bulk(n.to_string().into_bytes()),
        },
        Value::String(s) => Reply::bulk_str(s),
        Value::Array(a) => {
            let mut items = vec![Reply::bulk_str("[")];
            items.extend(a.iter().map(resp_of));
            Reply::Array(items)
        }
        Value::Object(m) => {
            let mut items = vec![Reply::bulk_str("{")];
            for (k, v) in m {
                items.push(Reply::bulk_str(k));
                items.push(resp_of(v));
            }
            Reply::Array(items)
        }
    }
}

pub async fn resp(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 2 || args.len() > 3 {
        return Reply::wrong_args("json.resp");
    }
    let pp = match path_arg_or_root(args, 2) {
        Ok(p) => p,
        Err(e) => return e,
    };
    let key = args[1].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let Some((d, _)) = doc::load_doc(ctx, &key) else {
                return Reply::Null;
            };
            let results = path::resolve_read(&d.value, &pp)
                .iter()
                .map(|loc| value_at(&d.value, loc).map(resp_of))
                .collect();
            shape(pp.dialect, results, Reply::Null)
        })
        .await
}

pub async fn debug(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 2 {
        return Reply::wrong_args("json.debug");
    }
    if eq_ignore_case(&args[1], "HELP") {
        return Reply::Array(vec![
            Reply::bulk_str("JSON.DEBUG MEMORY <key> [path] - reports the size in bytes of the stored records under the path"),
            Reply::bulk_str("JSON.DEBUG HELP - this message"),
        ]);
    }
    if !eq_ignore_case(&args[1], "MEMORY") || args.len() < 3 || args.len() > 4 {
        return Reply::wrong_args("json.debug");
    }
    let pp = match path_arg_or_root(args, 3) {
        Ok(p) => p,
        Err(e) => return e,
    };
    let key = args[2].clone();
    engine.ensure_local(&key).await;
    engine
        .store
        .run_key(&args[2], move |ctx| {
            let Some((d, _)) = doc::load_doc(ctx, &key) else {
                return Reply::Int(0);
            };
            let results: Vec<Option<Reply>> = path::resolve_read(&d.value, &pp)
                .iter()
                .map(|loc| {
                    let rp = path::loc_to_record_path(loc, &d.index)?;
                    let mut bytes = 0i64;
                    scan_prefix(ctx, &ikey::json_node_key(&key, &rp), |_k, v| {
                        bytes += v.len() as i64;
                        true
                    });
                    if rp.is_empty() {
                        // whole doc: include the head record
                        if let Some(h) = store::get_raw(ctx, &ikey::head_key(&key)) {
                            bytes += h.len() as i64;
                        }
                    }
                    Some(Reply::Int(bytes))
                })
                .collect();
            shape(pp.dialect, results, Reply::Int(0))
        })
        .await
}

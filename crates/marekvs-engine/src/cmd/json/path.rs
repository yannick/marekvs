//! JSON path dialects (design/16).
//!
//! RedisJSON v2 rules: a path starting with `$` is an RFC 9535 JSONPath
//! (multi-match, array-shaped replies, evaluated by `serde_json_path`);
//! anything else is a legacy static path (`.a.b[3]`, `a["x"][-1]`,
//! single-match, bare replies). Legacy paths and *static* `$`-paths (no
//! wildcards/filters/slices/descent) also resolve for writes: mutation
//! commands resolve them against the LOCAL materialized doc and then emit
//! deltas addressed by stable record paths / element ids.

use marekvs_core::json::{push_seg, ArrInfo, DocIndex, Eid, Seg};
use serde_json::Value;
use serde_json_path::JsonPath;

/// One element of a concrete location in the materialized document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LocElem {
    Key(String),
    Index(usize),
}

pub(crate) type Loc = Vec<LocElem>;

/// One segment of a static (write-capable) path. Indexes may be negative
/// (from-the-end addressing, resolved against the current array length).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StaticSeg {
    Key(String),
    Index(i64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Dialect {
    /// `$…` — JSONPath; replies are arrays of per-match results.
    Query,
    /// legacy — single match; bare replies.
    Legacy,
}

pub(crate) struct ParsedPath {
    pub dialect: Dialect,
    /// Compiled query (always present for `Query`, absent for `Legacy`).
    pub query: Option<JsonPath>,
    /// Present when the path is static: only names and integer indexes.
    pub static_segs: Option<Vec<StaticSeg>>,
}

impl ParsedPath {
    pub fn is_root(&self) -> bool {
        matches!(self.static_segs.as_deref(), Some([]))
    }
}

/// Parse a path argument. `Err` carries a client-facing message.
pub(crate) fn parse(arg: &[u8]) -> Result<ParsedPath, String> {
    let s = std::str::from_utf8(arg).map_err(|_| "ERR path is not valid UTF-8".to_string())?;
    if let Some(rest) = s.strip_prefix('$') {
        let query = JsonPath::parse(s).map_err(|e| format!("ERR invalid JSONPath: {e}"))?;
        // a $-path with only name/index segments is also write-capable
        let static_segs = parse_static(rest).ok();
        Ok(ParsedPath {
            dialect: Dialect::Query,
            query: Some(query),
            static_segs,
        })
    } else {
        let segs = parse_static(s)?;
        Ok(ParsedPath {
            dialect: Dialect::Legacy,
            query: None,
            static_segs: Some(segs),
        })
    }
}

/// Static grammar: `.` or empty = root; else `[.name | [int] | ["name"] |
/// ['name']]*` with an optional bare leading name (`a.b`).
fn parse_static(s: &str) -> Result<Vec<StaticSeg>, String> {
    let syntax = || format!("ERR invalid path '{s}'");
    let mut segs = Vec::new();
    if s.is_empty() || s == "." {
        return Ok(segs);
    }
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'.' => {
                i += 1;
                let start = i;
                while i < b.len() && b[i] != b'.' && b[i] != b'[' {
                    i += 1;
                }
                if i == start {
                    return Err(syntax()); // ".." or trailing "."
                }
                segs.push(StaticSeg::Key(s[start..i].to_string()));
            }
            b'[' => {
                i += 1;
                match b.get(i) {
                    Some(&q @ (b'"' | b'\'')) => {
                        i += 1;
                        let start = i;
                        while i < b.len() && b[i] != q {
                            i += 1;
                        }
                        if i >= b.len() {
                            return Err(syntax());
                        }
                        let name = &s[start..i];
                        i += 1;
                        if b.get(i) != Some(&b']') {
                            return Err(syntax());
                        }
                        i += 1;
                        segs.push(StaticSeg::Key(name.to_string()));
                    }
                    Some(_) => {
                        let start = i;
                        while i < b.len() && b[i] != b']' {
                            i += 1;
                        }
                        if i >= b.len() {
                            return Err(syntax());
                        }
                        let n: i64 = s[start..i].parse().map_err(|_| syntax())?;
                        i += 1;
                        segs.push(StaticSeg::Index(n));
                    }
                    None => return Err(syntax()),
                }
            }
            _ => {
                // bare name allowed only at the very start (legacy "a.b")
                if i != 0 {
                    return Err(syntax());
                }
                let start = i;
                while i < b.len() && b[i] != b'.' && b[i] != b'[' {
                    i += 1;
                }
                segs.push(StaticSeg::Key(s[start..i].to_string()));
            }
        }
    }
    Ok(segs)
}

/// Resolve a parsed path against a materialized doc for reading: every
/// matching location, in document order for queries, at most one for legacy.
pub(crate) fn resolve_read(doc: &Value, path: &ParsedPath) -> Vec<Loc> {
    match &path.query {
        Some(q) => q
            .query_located(doc)
            .locations()
            .map(|np| {
                np.iter()
                    .map(|el| match el {
                        serde_json_path::PathElement::Name(n) => LocElem::Key((*n).to_string()),
                        serde_json_path::PathElement::Index(i) => LocElem::Index(*i),
                    })
                    .collect()
            })
            .collect(),
        None => {
            let segs = path.static_segs.as_deref().unwrap_or(&[]);
            match resolve_static(doc, segs) {
                StaticTarget::Exists(loc) => vec![loc],
                _ => Vec::new(),
            }
        }
    }
}

/// Where a static path points for a write.
#[derive(Debug, PartialEq)]
pub(crate) enum StaticTarget {
    /// The addressed node exists.
    Exists(Loc),
    /// The parent exists and is an object; the final segment names a new key.
    NewKey { parent: Loc, key: String },
    /// Unresolvable (missing intermediate, index out of range, parent not a
    /// container of the right shape).
    Missing,
}

/// Normalize a possibly-negative index against `len`.
pub(crate) fn norm_index(n: i64, len: usize) -> Option<usize> {
    let r = if n < 0 { len as i64 + n } else { n };
    (0..len as i64).contains(&r).then_some(r as usize)
}

/// Resolve a static path against a materialized doc for writing.
pub(crate) fn resolve_static(doc: &Value, segs: &[StaticSeg]) -> StaticTarget {
    let mut loc = Loc::new();
    let mut cur = doc;
    for (i, seg) in segs.iter().enumerate() {
        let last = i + 1 == segs.len();
        match seg {
            StaticSeg::Key(key) => match cur {
                Value::Object(m) => match m.get(key) {
                    Some(v) => {
                        cur = v;
                        loc.push(LocElem::Key(key.clone()));
                    }
                    None if last => {
                        return StaticTarget::NewKey {
                            parent: loc,
                            key: key.clone(),
                        }
                    }
                    None => return StaticTarget::Missing,
                },
                _ => return StaticTarget::Missing,
            },
            StaticSeg::Index(n) => match cur {
                Value::Array(a) => match norm_index(*n, a.len()) {
                    Some(idx) => {
                        cur = &a[idx];
                        loc.push(LocElem::Index(idx));
                    }
                    // new array elements are created by ARRAPPEND/ARRINSERT,
                    // never through an out-of-range index (RedisJSON rule)
                    None => return StaticTarget::Missing,
                },
                _ => return StaticTarget::Missing,
            },
        }
    }
    StaticTarget::Exists(loc)
}

/// Translate a concrete location into the stable record path, using the
/// index built during materialization (array indexes become element ids).
pub(crate) fn loc_to_record_path(loc: &Loc, index: &DocIndex) -> Option<Vec<u8>> {
    let mut path = Vec::new();
    for elem in loc {
        match elem {
            LocElem::Key(k) => push_seg(&mut path, &Seg::Field(k.as_bytes().to_vec())),
            LocElem::Index(i) => {
                let info = index.arrays.get(&path)?;
                let eid = *info.order.get(*i)?;
                push_seg(&mut path, &Seg::Elem(eid));
            }
        }
    }
    Some(path)
}

/// The ordered element info of the array at `loc` (for ARR* commands).
pub(crate) fn array_info<'a>(
    loc: &Loc,
    index: &'a DocIndex,
) -> Option<(
    &'a ArrInfo,
    Vec<u8>, /* record path of the array node */
)> {
    let path = loc_to_record_path(loc, index)?;
    index.arrays.get(&path).map(|info| (info, path))
}

/// The element id at (possibly negative) `idx` of an array, plus the id of
/// its left neighbor in materialized order (for ARRINSERT anchoring).
#[allow(dead_code)]
pub(crate) fn eid_at(info: &ArrInfo, idx: i64) -> Option<(Eid, Eid)> {
    let i = norm_index(idx, info.order.len())?;
    let left = if i == 0 {
        marekvs_core::json::EID_HEAD
    } else {
        info.order[i - 1]
    };
    Some((info.order[i], left))
}

#[cfg(test)]
mod tests {
    use super::*;
    use marekvs_core::json::{build_doc, decompose, encode_path, JsonRecord, NodeIn, EID_HEAD};
    use serde_json::json;

    fn parse_ok(s: &str) -> ParsedPath {
        parse(s.as_bytes()).unwrap_or_else(|e| panic!("parse {s:?} failed: {e}"))
    }

    fn statics(s: &str) -> Vec<StaticSeg> {
        parse_ok(s).static_segs.expect("static path")
    }

    fn k(s: &str) -> StaticSeg {
        StaticSeg::Key(s.to_string())
    }

    // -- parsing -------------------------------------------------------------

    #[test]
    fn dialect_detection() {
        assert_eq!(parse_ok("$").dialect, Dialect::Query);
        assert_eq!(parse_ok("$.a").dialect, Dialect::Query);
        assert_eq!(parse_ok(".").dialect, Dialect::Legacy);
        assert_eq!(parse_ok(".a.b").dialect, Dialect::Legacy);
        assert_eq!(parse_ok("a.b").dialect, Dialect::Legacy);
        assert_eq!(parse_ok("").dialect, Dialect::Legacy);
    }

    #[test]
    fn root_forms() {
        for s in ["$", ".", ""] {
            assert!(parse_ok(s).is_root(), "{s:?} should be root");
        }
    }

    #[test]
    fn static_segments() {
        assert_eq!(statics(".a.b"), vec![k("a"), k("b")]);
        assert_eq!(statics("a.b"), vec![k("a"), k("b")]);
        assert_eq!(
            statics(".a[3].b"),
            vec![k("a"), StaticSeg::Index(3), k("b")]
        );
        assert_eq!(statics("[-1]"), vec![StaticSeg::Index(-1)]);
        assert_eq!(statics(r#"["x y"]"#), vec![k("x y")]);
        assert_eq!(statics("['x']"), vec![k("x")]);
        // $-paths that are static keep their static resolution
        assert_eq!(statics("$.a[0]"), vec![k("a"), StaticSeg::Index(0)]);
        assert_eq!(statics(r#"$["b"]"#), vec![k("b")]);
    }

    #[test]
    fn non_static_query_paths() {
        for s in ["$..b", "$.a[*]", "$.a[?(@.b > 1)]", "$.a[0:2]"] {
            let p = parse_ok(s);
            assert_eq!(p.dialect, Dialect::Query, "{s}");
            assert!(p.static_segs.is_none(), "{s} must not be static");
        }
    }

    #[test]
    fn parse_errors() {
        assert!(parse(b".a[").is_err());
        assert!(parse(b"a..b").is_err()); // legacy has no descent
        assert!(parse(b"[x]").is_err());
        assert!(parse(b"$.a[").is_err()); // invalid JSONPath too
        assert!(parse(b".a[\"unterminated]").is_err());
    }

    // -- read resolution -----------------------------------------------------

    fn doc() -> Value {
        json!({
            "a": {"b": 1, "c": [10, 20, 30]},
            "b": 2
        })
    }

    fn key_loc(parts: &[&str]) -> Loc {
        parts.iter().map(|p| LocElem::Key(p.to_string())).collect()
    }

    #[test]
    fn read_legacy_single_match() {
        let d = doc();
        assert_eq!(
            resolve_read(&d, &parse_ok(".a.b")),
            vec![key_loc(&["a", "b"])]
        );
        assert_eq!(
            resolve_read(&d, &parse_ok(".a.c[1]")),
            vec![vec![
                LocElem::Key("a".into()),
                LocElem::Key("c".into()),
                LocElem::Index(1)
            ]]
        );
        // negative index
        assert_eq!(
            resolve_read(&d, &parse_ok(".a.c[-1]")),
            vec![vec![
                LocElem::Key("a".into()),
                LocElem::Key("c".into()),
                LocElem::Index(2)
            ]]
        );
        assert_eq!(resolve_read(&d, &parse_ok(".missing")), Vec::<Loc>::new());
        assert_eq!(resolve_read(&d, &parse_ok(".a.c[9]")), Vec::<Loc>::new());
        assert_eq!(resolve_read(&d, &parse_ok(".")), vec![Loc::new()]);
    }

    #[test]
    fn read_query_multi_match() {
        let d = doc();
        let locs = resolve_read(&d, &parse_ok("$..b"));
        assert_eq!(locs.len(), 2);
        assert!(locs.contains(&key_loc(&["a", "b"])));
        assert!(locs.contains(&key_loc(&["b"])));
        let all = resolve_read(&d, &parse_ok("$.a.c[*]"));
        assert_eq!(all.len(), 3);
        assert_eq!(resolve_read(&d, &parse_ok("$")), vec![Loc::new()]);
    }

    // -- write resolution ----------------------------------------------------

    #[test]
    fn static_write_targets() {
        let d = doc();
        assert_eq!(
            resolve_static(&d, &statics(".a.b")),
            StaticTarget::Exists(key_loc(&["a", "b"]))
        );
        assert_eq!(
            resolve_static(&d, &statics(".a.new")),
            StaticTarget::NewKey {
                parent: key_loc(&["a"]),
                key: "new".into()
            }
        );
        assert_eq!(
            resolve_static(&d, &statics(".a.c[-2]")),
            StaticTarget::Exists(vec![
                LocElem::Key("a".into()),
                LocElem::Key("c".into()),
                LocElem::Index(1)
            ])
        );
        // new elements cannot be created through an index
        assert_eq!(
            resolve_static(&d, &statics(".a.c[3]")),
            StaticTarget::Missing
        );
        // missing intermediate
        assert_eq!(resolve_static(&d, &statics(".x.y")), StaticTarget::Missing);
        // parent is a scalar
        assert_eq!(resolve_static(&d, &statics(".b.c")), StaticTarget::Missing);
        // root always exists
        assert_eq!(resolve_static(&d, &[]), StaticTarget::Exists(Loc::new()));
    }

    // -- location → record path ----------------------------------------------

    #[test]
    fn loc_translation_uses_stable_eids() {
        let v = json!({"tags": ["x", "y"]});
        let mut n = 0u64;
        let mut fresh = || {
            n += 1;
            Eid { hlc: n, origin: 1 }
        };
        let recs = decompose(&[], &v, &mut fresh);
        let nodes: Vec<(Vec<u8>, NodeIn)> = recs
            .iter()
            .map(|r| match r {
                JsonRecord::Map { path, val } => (
                    path.clone(),
                    NodeIn::Map {
                        val: val.clone(),
                        dots: vec![],
                    },
                ),
                JsonRecord::Arr { path, elem } => (
                    path.clone(),
                    NodeIn::ArrElem {
                        elem: elem.clone(),
                        live: true,
                    },
                ),
            })
            .collect();
        let d = build_doc(&nodes).unwrap();

        let tags = encode_path(&[Seg::Field(b"tags".to_vec())]);
        let loc = vec![LocElem::Key("tags".into()), LocElem::Index(1)];
        let mut want = tags.clone();
        push_seg(&mut want, &Seg::Elem(Eid { hlc: 2, origin: 1 }));
        assert_eq!(loc_to_record_path(&loc, &d.index).unwrap(), want);
        assert_eq!(
            loc_to_record_path(&vec![LocElem::Key("tags".into())], &d.index).unwrap(),
            tags
        );
        assert_eq!(
            loc_to_record_path(&Loc::new(), &d.index).unwrap(),
            Vec::<u8>::new()
        );

        // array info + eid_at
        let (info, apath) = array_info(&vec![LocElem::Key("tags".into())], &d.index).unwrap();
        assert_eq!(apath, tags);
        assert_eq!(eid_at(info, 0), Some((Eid { hlc: 1, origin: 1 }, EID_HEAD)));
        assert_eq!(
            eid_at(info, -1),
            Some((Eid { hlc: 2, origin: 1 }, Eid { hlc: 1, origin: 1 }))
        );
        assert_eq!(eid_at(info, 5), None);
    }
}

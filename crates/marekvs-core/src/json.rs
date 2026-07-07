//! JSON document CRDT: pure codecs, RGA ordering, decomposition and
//! materialization (design/16).
//!
//! A document is stored as one record per JSON path under `ikey::Tag::Json`.
//! The path suffix is a chain of self-delimiting segments:
//!
//! ```text
//! field segment   [0x01][varint len][field bytes]        (map entry)
//! array segment   [0x02][hlc u64 BE][origin u16 BE]      (element Eid)
//! ```
//!
//! Record kinds are discriminated by the LAST segment (root = empty path):
//! * **map entry / root** — ORSWOT element (`RecordType::HashField` reuse);
//!   the dot-lattice payload's value bytes are one [`JVal`] encoding.
//! * **array element** — LWW register (`RecordType::List` reuse); payload is
//!   an [`ArrElem`]: the RGA left-anchor Eid + the [`JVal`].
//!
//! RGA order: walk from the head sentinel; siblings sharing a left anchor
//! sort by `(hlc, origin)` **descending** (research2.md §3.2). Tombstoned
//! elements keep their payload so they still anchor, but render nothing.

use crate::merge::Dot;
use crate::NodeId;
use std::collections::HashMap;

pub const SEG_FIELD: u8 = 0x01;
pub const SEG_ELEM: u8 = 0x02;

/// Stable array-element id. `Hlc::now()` is strictly monotone per process,
/// so `(hlc, origin)` is cluster-unique.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Eid {
    pub hlc: u64,
    pub origin: NodeId,
}

/// Insert-at-array-head sentinel (never a real element id).
pub const EID_HEAD: Eid = Eid { hlc: 0, origin: 0 };

impl Eid {
    pub fn encode_to(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.hlc.to_be_bytes());
        out.extend_from_slice(&self.origin.to_be_bytes());
    }

    pub fn decode(b: &[u8]) -> Option<Eid> {
        if b.len() < 10 {
            return None;
        }
        Some(Eid {
            hlc: u64::from_be_bytes(b[..8].try_into().unwrap()),
            origin: u16::from_be_bytes(b[8..10].try_into().unwrap()),
        })
    }
}

/// One path segment.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Seg {
    Field(Vec<u8>),
    Elem(Eid),
}

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

/// Append one segment's bytes to a path.
pub fn push_seg(path: &mut Vec<u8>, seg: &Seg) {
    match seg {
        Seg::Field(f) => {
            path.push(SEG_FIELD);
            put_varint(path, f.len() as u64);
            path.extend_from_slice(f);
        }
        Seg::Elem(e) => {
            path.push(SEG_ELEM);
            e.encode_to(path);
        }
    }
}

pub fn encode_path(segs: &[Seg]) -> Vec<u8> {
    let mut out = Vec::new();
    for s in segs {
        push_seg(&mut out, s);
    }
    out
}

/// Decode one segment at `buf`; returns the segment and its encoded length.
fn decode_seg(buf: &[u8]) -> Option<(Seg, usize)> {
    match *buf.first()? {
        SEG_FIELD => {
            let (len, n) = get_varint(&buf[1..])?;
            let len = len as usize;
            if buf.len() < 1 + n + len {
                return None;
            }
            Some((Seg::Field(buf[1 + n..1 + n + len].to_vec()), 1 + n + len))
        }
        SEG_ELEM => {
            let e = Eid::decode(buf.get(1..)?)?;
            Some((Seg::Elem(e), 11))
        }
        _ => None,
    }
}

pub fn decode_path(suffix: &[u8]) -> Option<Vec<Seg>> {
    let mut segs = Vec::new();
    let mut pos = 0;
    while pos < suffix.len() {
        let (seg, n) = decode_seg(&suffix[pos..])?;
        segs.push(seg);
        pos += n;
    }
    Some(segs)
}

/// Split a non-empty path into `(parent bytes, last segment)`.
pub fn split_last(path: &[u8]) -> Option<(&[u8], Seg)> {
    let mut pos = 0;
    let mut last = None;
    while pos < path.len() {
        let (seg, n) = decode_seg(&path[pos..])?;
        last = Some((pos, seg));
        pos += n;
    }
    last.map(|(start, seg)| (&path[..start], seg))
}

/// A JSON leaf/container marker as stored in record payloads.
#[derive(Clone, PartialEq, Debug)]
pub enum JVal {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(Vec<u8>),
    Obj,
    Arr,
}

const JT_NULL: u8 = 0;
const JT_FALSE: u8 = 1;
const JT_TRUE: u8 = 2;
const JT_INT: u8 = 3;
const JT_FLT: u8 = 4;
const JT_STR: u8 = 5;
const JT_OBJ: u8 = 6;
const JT_ARR: u8 = 7;

impl JVal {
    pub fn encode(&self) -> Vec<u8> {
        match self {
            JVal::Null => vec![JT_NULL],
            JVal::Bool(false) => vec![JT_FALSE],
            JVal::Bool(true) => vec![JT_TRUE],
            JVal::Int(i) => {
                let mut v = Vec::with_capacity(9);
                v.push(JT_INT);
                v.extend_from_slice(&i.to_be_bytes());
                v
            }
            JVal::Float(f) => {
                let mut v = Vec::with_capacity(9);
                v.push(JT_FLT);
                v.extend_from_slice(&f.to_bits().to_be_bytes());
                v
            }
            JVal::Str(s) => {
                let mut v = Vec::with_capacity(1 + s.len());
                v.push(JT_STR);
                v.extend_from_slice(s);
                v
            }
            JVal::Obj => vec![JT_OBJ],
            JVal::Arr => vec![JT_ARR],
        }
    }

    pub fn decode(b: &[u8]) -> Option<JVal> {
        match (*b.first()?, &b[1..]) {
            (JT_NULL, []) => Some(JVal::Null),
            (JT_FALSE, []) => Some(JVal::Bool(false)),
            (JT_TRUE, []) => Some(JVal::Bool(true)),
            (JT_INT, rest) if rest.len() == 8 => {
                Some(JVal::Int(i64::from_be_bytes(rest.try_into().unwrap())))
            }
            (JT_FLT, rest) if rest.len() == 8 => Some(JVal::Float(f64::from_bits(
                u64::from_be_bytes(rest.try_into().unwrap()),
            ))),
            (JT_STR, rest) => Some(JVal::Str(rest.to_vec())),
            (JT_OBJ, []) => Some(JVal::Obj),
            (JT_ARR, []) => Some(JVal::Arr),
            _ => None,
        }
    }
}

/// Array-element record payload: RGA left anchor + value.
#[derive(Clone, PartialEq, Debug)]
pub struct ArrElem {
    pub left: Eid,
    pub val: JVal,
}

impl ArrElem {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(10 + 9);
        self.left.encode_to(&mut out);
        out.extend_from_slice(&self.val.encode());
        out
    }

    pub fn decode(b: &[u8]) -> Option<ArrElem> {
        let left = Eid::decode(b)?;
        let val = JVal::decode(&b[10..])?;
        Some(ArrElem { left, val })
    }
}

/// Materialize RGA order over `(eid, left, live)` element descriptors.
/// Returns ALL elements (live and dead) in document order; callers filter
/// dead ones for rendering but need them for append anchors. Elements whose
/// left anchor is absent attach to the head sentinel (defensive, still
/// deterministic).
pub fn rga_order(elems: &[(Eid, Eid, bool)]) -> Vec<(Eid, bool)> {
    use std::collections::HashSet;
    let present: HashSet<Eid> = elems.iter().map(|(e, _, _)| *e).collect();
    // anchor → children, later re-sorted so higher (hlc, origin) comes first
    let mut children: HashMap<Eid, Vec<(Eid, bool)>> = HashMap::new();
    for &(eid, left, live) in elems {
        let anchor = if left != EID_HEAD && !present.contains(&left) {
            EID_HEAD // orphaned left ref: attach to the sentinel
        } else {
            left
        };
        children.entry(anchor).or_default().push((eid, live));
    }
    for v in children.values_mut() {
        // pushed onto the stack in ascending order so the highest pops first
        v.sort_unstable_by_key(|(e, _)| *e);
    }
    // pre-order DFS: emit each child (desc), then its subtree. Malformed
    // cycles (impossible from our writers) are unreachable from the sentinel
    // and drop out deterministically on every replica.
    let mut out = Vec::with_capacity(elems.len());
    let mut stack: Vec<(Eid, bool)> = children.remove(&EID_HEAD).unwrap_or_default();
    while let Some((eid, live)) = stack.pop() {
        out.push((eid, live));
        if let Some(kids) = children.remove(&eid) {
            stack.extend(kids);
        }
    }
    out
}

/// One decomposed record to write.
#[derive(Clone, PartialEq, Debug)]
pub enum JsonRecord {
    /// Map entry or root (kind A): ORSWOT element whose value is `val`.
    Map { path: Vec<u8>, val: JVal },
    /// Array element (kind B): LWW register holding `elem`.
    Arr { path: Vec<u8>, elem: ArrElem },
}

impl JsonRecord {
    pub fn path(&self) -> &[u8] {
        match self {
            JsonRecord::Map { path, .. } | JsonRecord::Arr { path, .. } => path,
        }
    }
}

/// Flatten `v` into per-path records rooted at `base` (base itself becomes a
/// kind-A record). Array elements get fresh eids from `fresh` and chain their
/// left anchors in document order.
pub fn decompose(
    base: &[Seg],
    v: &serde_json::Value,
    fresh: &mut dyn FnMut() -> Eid,
) -> Vec<JsonRecord> {
    let mut out = Vec::new();
    let mut path = encode_path(base);
    decompose_into(&mut path, v, fresh, &mut out);
    out
}

/// The scalar/container marker for a value (containers carry no data).
fn jval_of(v: &serde_json::Value) -> JVal {
    match v {
        serde_json::Value::Null => JVal::Null,
        serde_json::Value::Bool(b) => JVal::Bool(*b),
        serde_json::Value::Number(n) => match n.as_i64() {
            Some(i) => JVal::Int(i),
            // u64 > i64::MAX or fractional: stored as f64 (documented)
            None => JVal::Float(n.as_f64().unwrap_or(0.0)),
        },
        serde_json::Value::String(s) => JVal::Str(s.as_bytes().to_vec()),
        serde_json::Value::Object(_) => JVal::Obj,
        serde_json::Value::Array(_) => JVal::Arr,
    }
}

fn decompose_into(
    path: &mut Vec<u8>,
    v: &serde_json::Value,
    fresh: &mut dyn FnMut() -> Eid,
    out: &mut Vec<JsonRecord>,
) {
    out.push(JsonRecord::Map {
        path: path.clone(),
        val: jval_of(v),
    });
    decompose_children(path, v, fresh, out);
}

/// Emit the child records of a container (nothing for scalars). The
/// container's own record — kind A for fields/root, the ArrElem payload for
/// array elements — is emitted by the caller.
fn decompose_children(
    path: &mut Vec<u8>,
    v: &serde_json::Value,
    fresh: &mut dyn FnMut() -> Eid,
    out: &mut Vec<JsonRecord>,
) {
    match v {
        serde_json::Value::Object(m) => {
            for (k, child) in m {
                let len = path.len();
                push_seg(path, &Seg::Field(k.as_bytes().to_vec()));
                decompose_into(path, child, fresh, out);
                path.truncate(len);
            }
        }
        serde_json::Value::Array(a) => {
            let mut left = EID_HEAD;
            for child in a {
                let e = fresh();
                let len = path.len();
                push_seg(path, &Seg::Elem(e));
                out.push(JsonRecord::Arr {
                    path: path.clone(),
                    elem: ArrElem {
                        left,
                        val: jval_of(child),
                    },
                });
                decompose_children(path, child, fresh, out);
                path.truncate(len);
                left = e;
            }
        }
        _ => {}
    }
}

/// Decoded node passed into [`build_doc`]. Engine filters dead map entries
/// (their OR state is empty) but passes array tombstones (`live = false`)
/// because they still anchor ordering.
#[derive(Clone, PartialEq, Debug)]
pub enum NodeIn {
    Map { val: JVal, dots: Vec<Dot> },
    ArrElem { elem: ArrElem, live: bool },
}

/// Per-array materialized info.
#[derive(Clone, PartialEq, Debug, Default)]
pub struct ArrInfo {
    /// Live elements in document order.
    pub order: Vec<Eid>,
    /// Last element in RGA order INCLUDING tombstones (append anchor).
    pub last: Option<Eid>,
}

/// Index from record paths to CRDT state, used to translate user-facing
/// locations into stable record addresses and to collect dots for covering.
#[derive(Clone, PartialEq, Debug, Default)]
pub struct DocIndex {
    /// Live kind-A record path → its live dots.
    pub map_dots: HashMap<Vec<u8>, Vec<Dot>>,
    /// Array node record path → materialized element info.
    pub arrays: HashMap<Vec<u8>, ArrInfo>,
}

#[derive(Clone, PartialEq, Debug)]
pub struct Doc {
    pub value: serde_json::Value,
    pub index: DocIndex,
}

/// Materialize a document from its records. `nodes` is the full record set
/// of one doc: `(path suffix, decoded node)`. Returns None when there is no
/// visible root. Records invisible under the winning tree (orphans under
/// dead or retyped parents) are skipped deterministically.
pub fn build_doc(nodes: &[(Vec<u8>, NodeIn)]) -> Option<Doc> {
    let mut kids: HashMap<&[u8], Vec<(Seg, &NodeIn)>> = HashMap::new();
    let mut root: Option<&NodeIn> = None;
    for (path, node) in nodes {
        if path.is_empty() {
            root = Some(node);
            continue;
        }
        // malformed paths are skipped deterministically on every replica
        let Some((parent, last)) = split_last(path) else {
            continue;
        };
        kids.entry(parent).or_default().push((last, node));
    }
    let Some(NodeIn::Map { val, dots }) = root else {
        return None;
    };
    let mut index = DocIndex::default();
    index.map_dots.insert(Vec::new(), dots.clone());
    let value = materialize(&mut Vec::new(), val, &kids, &mut index);
    Some(Doc { value, index })
}

fn scalar_value(val: &JVal) -> serde_json::Value {
    match val {
        JVal::Null => serde_json::Value::Null,
        JVal::Bool(b) => serde_json::Value::Bool(*b),
        JVal::Int(i) => serde_json::Value::Number((*i).into()),
        JVal::Float(f) => serde_json::Number::from_f64(*f)
            .map_or(serde_json::Value::Null, serde_json::Value::Number),
        JVal::Str(s) => serde_json::Value::String(String::from_utf8_lossy(s).into_owned()),
        JVal::Obj | JVal::Arr => unreachable!("containers handled by materialize"),
    }
}

/// Materialize the node at `path` whose winning value is `val`. Children that
/// do not match the parent's container type (stale records under a retyped
/// or dead branch) are skipped — the same records skip on every replica.
fn materialize(
    path: &mut Vec<u8>,
    val: &JVal,
    kids: &HashMap<&[u8], Vec<(Seg, &NodeIn)>>,
    index: &mut DocIndex,
) -> serde_json::Value {
    match val {
        JVal::Obj => {
            let mut fields: Vec<(&Vec<u8>, &JVal, &Vec<Dot>)> = Vec::new();
            if let Some(children) = kids.get(path.as_slice()) {
                for (seg, node) in children {
                    if let (Seg::Field(name), NodeIn::Map { val, dots }) = (seg, node) {
                        fields.push((name, val, dots));
                    }
                }
            }
            fields.sort_unstable_by_key(|(name, _, _)| *name);
            let mut map = serde_json::Map::with_capacity(fields.len());
            for (name, fval, dots) in fields {
                let len = path.len();
                push_seg(path, &Seg::Field(name.clone()));
                index.map_dots.insert(path.clone(), dots.clone());
                let v = materialize(path, fval, kids, index);
                path.truncate(len);
                map.insert(String::from_utf8_lossy(name).into_owned(), v);
            }
            serde_json::Value::Object(map)
        }
        JVal::Arr => {
            let mut elems: Vec<(Eid, Eid, bool)> = Vec::new();
            let mut by_eid: HashMap<Eid, &ArrElem> = HashMap::new();
            if let Some(children) = kids.get(path.as_slice()) {
                for (seg, node) in children {
                    if let (Seg::Elem(e), NodeIn::ArrElem { elem, live }) = (seg, node) {
                        elems.push((*e, elem.left, *live));
                        by_eid.insert(*e, elem);
                    }
                }
            }
            let ordered = rga_order(&elems);
            let mut info = ArrInfo {
                order: Vec::new(),
                last: ordered.last().map(|(e, _)| *e),
            };
            let mut arr = Vec::new();
            for (e, live) in &ordered {
                if !live {
                    continue;
                }
                info.order.push(*e);
                let elem = by_eid[e];
                let len = path.len();
                push_seg(path, &Seg::Elem(*e));
                arr.push(materialize(path, &elem.val, kids, index));
                path.truncate(len);
            }
            index.arrays.insert(path.clone(), info);
            serde_json::Value::Array(arr)
        }
        scalar => scalar_value(scalar),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn eid(hlc: u64, origin: NodeId) -> Eid {
        Eid { hlc, origin }
    }

    fn f(name: &str) -> Seg {
        Seg::Field(name.as_bytes().to_vec())
    }

    // -- path codec --------------------------------------------------------

    #[test]
    fn path_roundtrip() {
        let segs = vec![f("a"), Seg::Elem(eid(7, 3)), f("b c"), f("")];
        let bytes = encode_path(&segs);
        assert_eq!(decode_path(&bytes).unwrap(), segs);
        assert_eq!(decode_path(&[]).unwrap(), Vec::<Seg>::new());
    }

    #[test]
    fn path_parent_is_byte_prefix() {
        let parent = encode_path(&[f("a")]);
        let child = encode_path(&[f("a"), f("b")]);
        let elem = encode_path(&[f("a"), Seg::Elem(eid(9, 1))]);
        assert!(child.starts_with(&parent));
        assert!(elem.starts_with(&parent));
        // sibling with a longer name diverges (varint length differs)
        let sibling = encode_path(&[f("ab")]);
        assert!(!sibling.starts_with(&parent));
    }

    #[test]
    fn path_rejects_garbage() {
        assert!(decode_path(&[0xFF]).is_none());
        assert!(decode_path(&[SEG_ELEM, 1, 2, 3]).is_none()); // short eid
        assert!(decode_path(&[SEG_FIELD, 5, b'a']).is_none()); // short field
    }

    #[test]
    fn eid_key_bytes_sort_numerically() {
        let mut a = Vec::new();
        eid(0x0100, 2).encode_to(&mut a);
        let mut b = Vec::new();
        eid(0x0200, 1).encode_to(&mut b);
        assert!(a < b);
    }

    // -- JVal / ArrElem codecs ----------------------------------------------

    #[test]
    fn jval_roundtrip() {
        for v in [
            JVal::Null,
            JVal::Bool(false),
            JVal::Bool(true),
            JVal::Int(-42),
            JVal::Int(i64::MAX),
            JVal::Float(3.25),
            JVal::Str(b"hello".to_vec()),
            JVal::Str(vec![]),
            JVal::Obj,
            JVal::Arr,
        ] {
            assert_eq!(JVal::decode(&v.encode()).unwrap(), v);
        }
        assert!(JVal::decode(&[]).is_none());
        assert!(JVal::decode(&[99]).is_none());
        assert!(JVal::decode(&[3, 0, 0]).is_none()); // short int
    }

    #[test]
    fn arr_elem_roundtrip() {
        let e = ArrElem {
            left: eid(55, 4),
            val: JVal::Str(b"x".to_vec()),
        };
        assert_eq!(ArrElem::decode(&e.encode()).unwrap(), e);
        let h = ArrElem {
            left: EID_HEAD,
            val: JVal::Obj,
        };
        assert_eq!(ArrElem::decode(&h.encode()).unwrap(), h);
        assert!(ArrElem::decode(&[1, 2]).is_none());
    }

    // -- RGA ordering --------------------------------------------------------

    #[test]
    fn rga_chained_appends_keep_insert_order() {
        // a <- b <- c, all one origin: document order a b c
        let a = eid(10, 1);
        let b = eid(20, 1);
        let c = eid(30, 1);
        let out = rga_order(&[(a, EID_HEAD, true), (b, a, true), (c, b, true)]);
        assert_eq!(out, vec![(a, true), (b, true), (c, true)]);
    }

    #[test]
    fn rga_concurrent_runs_do_not_interleave() {
        // Node 1 appends x1 x2 after HEAD; node 2 concurrently appends y1 y2
        // after HEAD. Later run (higher hlc) sorts first; runs stay contiguous.
        let x1 = eid(10, 1);
        let x2 = eid(11, 1);
        let y1 = eid(20, 2);
        let y2 = eid(21, 2);
        let out = rga_order(&[
            (x1, EID_HEAD, true),
            (x2, x1, true),
            (y1, EID_HEAD, true),
            (y2, y1, true),
        ]);
        assert_eq!(out, vec![(y1, true), (y2, true), (x1, true), (x2, true)]);
    }

    #[test]
    fn rga_tombstone_still_anchors() {
        // a(dead) <- b: b keeps its position after a even though a is gone
        let a = eid(10, 1);
        let b = eid(20, 1);
        let c = eid(30, 1); // inserted at head later
        let out = rga_order(&[(a, EID_HEAD, false), (b, a, true), (c, EID_HEAD, true)]);
        assert_eq!(out, vec![(c, true), (a, false), (b, true)]);
    }

    #[test]
    fn rga_orphan_attaches_to_head_deterministically() {
        let ghost = eid(5, 9); // never present
        let o = eid(50, 3);
        let a = eid(10, 1);
        let out = rga_order(&[(a, EID_HEAD, true), (o, ghost, true)]);
        // orphan treated as head-anchored: (50,3) > (10,1) so it sorts first
        assert_eq!(out, vec![(o, true), (a, true)]);
    }

    #[test]
    fn rga_same_anchor_ties_break_by_origin_desc() {
        let a = eid(10, 1);
        let b = eid(10, 2);
        let out = rga_order(&[(a, EID_HEAD, true), (b, EID_HEAD, true)]);
        assert_eq!(out, vec![(b, true), (a, true)]);
    }

    // -- decompose -----------------------------------------------------------

    fn counter_eids() -> impl FnMut() -> Eid {
        let mut n = 0u64;
        move || {
            n += 1;
            Eid { hlc: n, origin: 1 }
        }
    }

    #[test]
    fn decompose_scalar_root() {
        let mut fresh = counter_eids();
        let recs = decompose(&[], &json!(42), &mut fresh);
        assert_eq!(
            recs,
            vec![JsonRecord::Map {
                path: vec![],
                val: JVal::Int(42)
            }]
        );
    }

    #[test]
    fn decompose_nested_object() {
        let mut fresh = counter_eids();
        let recs = decompose(&[], &json!({"a": {"b": 1}}), &mut fresh);
        assert_eq!(
            recs,
            vec![
                JsonRecord::Map {
                    path: vec![],
                    val: JVal::Obj
                },
                JsonRecord::Map {
                    path: encode_path(&[f("a")]),
                    val: JVal::Obj
                },
                JsonRecord::Map {
                    path: encode_path(&[f("a"), f("b")]),
                    val: JVal::Int(1)
                },
            ]
        );
    }

    #[test]
    fn decompose_array_chains_left_anchors() {
        let mut fresh = counter_eids();
        let recs = decompose(&[], &json!(["x", "y"]), &mut fresh);
        let e1 = eid(1, 1);
        let e2 = eid(2, 1);
        assert_eq!(
            recs,
            vec![
                JsonRecord::Map {
                    path: vec![],
                    val: JVal::Arr
                },
                JsonRecord::Arr {
                    path: encode_path(&[Seg::Elem(e1)]),
                    elem: ArrElem {
                        left: EID_HEAD,
                        val: JVal::Str(b"x".to_vec())
                    }
                },
                JsonRecord::Arr {
                    path: encode_path(&[Seg::Elem(e2)]),
                    elem: ArrElem {
                        left: e1,
                        val: JVal::Str(b"y".to_vec())
                    }
                },
            ]
        );
    }

    #[test]
    fn decompose_container_inside_array() {
        let mut fresh = counter_eids();
        let recs = decompose(&[f("k")], &json!([{"t": true}]), &mut fresh);
        let e1 = eid(1, 1);
        assert_eq!(recs.len(), 3);
        assert_eq!(
            recs[1],
            JsonRecord::Arr {
                path: encode_path(&[f("k"), Seg::Elem(e1)]),
                elem: ArrElem {
                    left: EID_HEAD,
                    val: JVal::Obj
                }
            }
        );
        assert_eq!(
            recs[2],
            JsonRecord::Map {
                path: encode_path(&[f("k"), Seg::Elem(e1), f("t")]),
                val: JVal::Bool(true)
            }
        );
    }

    // -- build_doc -----------------------------------------------------------

    fn dot(hlc: u64, origin: NodeId) -> Dot {
        Dot { hlc, origin }
    }

    /// decompose → NodeIn set (all live, dots synthesized per record).
    fn as_nodes(recs: &[JsonRecord]) -> Vec<(Vec<u8>, NodeIn)> {
        recs.iter()
            .enumerate()
            .map(|(i, r)| match r {
                JsonRecord::Map { path, val } => (
                    path.clone(),
                    NodeIn::Map {
                        val: val.clone(),
                        dots: vec![dot(1000 + i as u64, 1)],
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
            .collect()
    }

    #[test]
    fn build_doc_roundtrips_decompose() {
        let v = json!({
            "title": "Plan",
            "meta": {"n": 3, "ok": true, "score": 1.5},
            "tags": ["a", "b", "c"],
            "rows": [{"id": 1}, {"id": 2}],
            "none": null
        });
        let mut fresh = counter_eids();
        let recs = decompose(&[], &v, &mut fresh);
        let doc = build_doc(&as_nodes(&recs)).unwrap();
        assert_eq!(doc.value, v);
    }

    #[test]
    fn build_doc_missing_root_is_none() {
        assert!(build_doc(&[]).is_none());
        // records exist but no root record → no visible doc
        let nodes = vec![(
            encode_path(&[f("a")]),
            NodeIn::Map {
                val: JVal::Int(1),
                dots: vec![dot(5, 1)],
            },
        )];
        assert!(build_doc(&nodes).is_none());
    }

    #[test]
    fn build_doc_skips_orphans_under_retyped_parent() {
        // root says "a" is an Int, but a stale child record a.b lingers
        let nodes = vec![
            (
                vec![],
                NodeIn::Map {
                    val: JVal::Obj,
                    dots: vec![dot(9, 1)],
                },
            ),
            (
                encode_path(&[f("a")]),
                NodeIn::Map {
                    val: JVal::Int(7),
                    dots: vec![dot(10, 1)],
                },
            ),
            (
                encode_path(&[f("a"), f("b")]),
                NodeIn::Map {
                    val: JVal::Int(1),
                    dots: vec![dot(5, 1)],
                },
            ),
        ];
        let doc = build_doc(&nodes).unwrap();
        assert_eq!(doc.value, json!({"a": 7}));
        // the orphan is not indexed
        assert!(!doc
            .index
            .map_dots
            .contains_key(&encode_path(&[f("a"), f("b")])));
    }

    #[test]
    fn build_doc_array_tombstones_hidden_but_anchor() {
        let a = eid(10, 1);
        let b = eid(20, 1);
        let nodes = vec![
            (
                vec![],
                NodeIn::Map {
                    val: JVal::Arr,
                    dots: vec![dot(9, 1)],
                },
            ),
            (
                encode_path(&[Seg::Elem(a)]),
                NodeIn::ArrElem {
                    elem: ArrElem {
                        left: EID_HEAD,
                        val: JVal::Str(b"gone".to_vec()),
                    },
                    live: false,
                },
            ),
            (
                encode_path(&[Seg::Elem(b)]),
                NodeIn::ArrElem {
                    elem: ArrElem {
                        left: a,
                        val: JVal::Str(b"kept".to_vec()),
                    },
                    live: true,
                },
            ),
        ];
        let doc = build_doc(&nodes).unwrap();
        assert_eq!(doc.value, json!(["kept"]));
        let info = &doc.index.arrays[&Vec::<u8>::new()];
        assert_eq!(info.order, vec![b]);
        assert_eq!(info.last, Some(b)); // dead `a` anchors, live `b` is last
    }

    #[test]
    fn build_doc_index_exposes_dots_and_array_order() {
        let v = json!({"tags": ["x", "y"]});
        let mut fresh = counter_eids();
        let recs = decompose(&[], &v, &mut fresh);
        let doc = build_doc(&as_nodes(&recs)).unwrap();
        let tags_path = encode_path(&[f("tags")]);
        assert!(doc.index.map_dots.contains_key(&Vec::<u8>::new()));
        assert!(doc.index.map_dots.contains_key(&tags_path));
        let info = &doc.index.arrays[&tags_path];
        assert_eq!(info.order, vec![eid(1, 1), eid(2, 1)]);
        assert_eq!(info.last, Some(eid(2, 1)));
    }
}

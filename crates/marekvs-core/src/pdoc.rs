//! Protobuf field-record codecs (design/18): path segments, leaf values,
//! and the descriptor-free structural recompose used by RENAME/COPY.
//!
//! A decomposed proto value is one record per field path under
//! `ikey::Tag::ProtoField`, mirroring the JSON per-path CRDT (design/16).
//! Segments are keyed by FIELD NUMBER (stable across schema renames):
//!
//! ```text
//! field segment    [0x01][varint len][varint field_number]
//! elem segment     [0x02][hlc u64 BE][origin u16 BE]              (= json Eid)
//! map-key segment  [0x03][varint len][kind u8][canonical key bytes]
//! ```
//!
//! Record kinds are discriminated by the LAST segment (root = empty path):
//! singular fields, map entries and container markers are ORSWOT elements
//! (`RecordType::HashField` reuse); repeated elements are LWW registers
//! (`RecordType::List` reuse) whose tombstones keep their payload as RGA
//! anchors. No prost dependency here — descriptor logic lives in the
//! engine's `proto` module (protohead.rs precedent).

use crate::json::{rga_order, Eid, EID_HEAD};
use crate::merge::Dot;
use std::collections::HashMap;

pub const SEG_FIELD: u8 = 0x01;
pub const SEG_ELEM: u8 = 0x02;
pub const SEG_MAPKEY: u8 = 0x03;

/// Map-entry key. Fixed-width big-endian integer encodings keep the byte
/// form canonical (one record per logical key) and memcmp-ordered.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum MKey {
    Bool(bool),
    I32(i32),
    I64(i64),
    U32(u32),
    U64(u64),
    Str(Vec<u8>),
}

/// One path segment.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum PSeg {
    /// Message field, addressed by field number.
    Field(u32),
    /// Map entry under a map field.
    MapKey(MKey),
    /// Repeated element (stable RGA id).
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

const MKIND_BOOL: u8 = 0x01;
const MKIND_I32: u8 = 0x02;
const MKIND_I64: u8 = 0x03;
const MKIND_U32: u8 = 0x04;
const MKIND_U64: u8 = 0x05;
const MKIND_STR: u8 = 0x06;

impl MKey {
    /// `[kind u8][canonical key bytes]` — the length-framed window body of a
    /// SEG_MAPKEY segment.
    fn encode_body(&self, out: &mut Vec<u8>) {
        match self {
            MKey::Bool(b) => {
                out.push(MKIND_BOOL);
                out.push(*b as u8);
            }
            MKey::I32(v) => {
                out.push(MKIND_I32);
                out.extend_from_slice(&v.to_be_bytes());
            }
            MKey::I64(v) => {
                out.push(MKIND_I64);
                out.extend_from_slice(&v.to_be_bytes());
            }
            MKey::U32(v) => {
                out.push(MKIND_U32);
                out.extend_from_slice(&v.to_be_bytes());
            }
            MKey::U64(v) => {
                out.push(MKIND_U64);
                out.extend_from_slice(&v.to_be_bytes());
            }
            MKey::Str(s) => {
                out.push(MKIND_STR);
                out.extend_from_slice(s);
            }
        }
    }

    fn decode_body(b: &[u8]) -> Option<MKey> {
        match (*b.first()?, &b[1..]) {
            (MKIND_BOOL, [0]) => Some(MKey::Bool(false)),
            (MKIND_BOOL, [1]) => Some(MKey::Bool(true)),
            (MKIND_I32, rest) if rest.len() == 4 => {
                Some(MKey::I32(i32::from_be_bytes(rest.try_into().unwrap())))
            }
            (MKIND_I64, rest) if rest.len() == 8 => {
                Some(MKey::I64(i64::from_be_bytes(rest.try_into().unwrap())))
            }
            (MKIND_U32, rest) if rest.len() == 4 => {
                Some(MKey::U32(u32::from_be_bytes(rest.try_into().unwrap())))
            }
            (MKIND_U64, rest) if rest.len() == 8 => {
                Some(MKey::U64(u64::from_be_bytes(rest.try_into().unwrap())))
            }
            (MKIND_STR, rest) => Some(MKey::Str(rest.to_vec())),
            _ => None,
        }
    }
}

pub fn push_seg(path: &mut Vec<u8>, seg: &PSeg) {
    match seg {
        PSeg::Field(n) => {
            path.push(SEG_FIELD);
            let mut num = Vec::with_capacity(5);
            put_varint(&mut num, *n as u64);
            put_varint(path, num.len() as u64);
            path.extend_from_slice(&num);
        }
        PSeg::Elem(e) => {
            path.push(SEG_ELEM);
            e.encode_to(path);
        }
        PSeg::MapKey(k) => {
            path.push(SEG_MAPKEY);
            let mut body = Vec::new();
            k.encode_body(&mut body);
            put_varint(path, body.len() as u64);
            path.extend_from_slice(&body);
        }
    }
}

pub fn encode_path(segs: &[PSeg]) -> Vec<u8> {
    let mut out = Vec::new();
    for s in segs {
        push_seg(&mut out, s);
    }
    out
}

/// Decode one segment at `buf`; returns the segment and its encoded length.
fn decode_seg(buf: &[u8]) -> Option<(PSeg, usize)> {
    match *buf.first()? {
        SEG_FIELD => {
            let (len, n) = get_varint(&buf[1..])?;
            let len = len as usize;
            let window = buf.get(1 + n..1 + n + len)?;
            let (num, used) = get_varint(window)?;
            if used != len || num > u32::MAX as u64 {
                return None; // trailing bytes inside the window = corruption
            }
            Some((PSeg::Field(num as u32), 1 + n + len))
        }
        SEG_ELEM => {
            let e = Eid::decode(buf.get(1..)?)?;
            Some((PSeg::Elem(e), 11))
        }
        SEG_MAPKEY => {
            let (len, n) = get_varint(&buf[1..])?;
            let len = len as usize;
            let window = buf.get(1 + n..1 + n + len)?;
            let key = MKey::decode_body(window)?;
            Some((PSeg::MapKey(key), 1 + n + len))
        }
        _ => None,
    }
}

pub fn decode_path(suffix: &[u8]) -> Option<Vec<PSeg>> {
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
pub fn split_last(path: &[u8]) -> Option<(&[u8], PSeg)> {
    let mut pos = 0;
    let mut last = None;
    while pos < path.len() {
        let (seg, n) = decode_seg(&path[pos..])?;
        last = Some((pos, seg));
        pos += n;
    }
    last.map(|(start, seg)| (&path[..start], seg))
}

/// A proto leaf/container marker as stored in record payloads. Floats are
/// stored and compared by bit pattern (NaN-safe, bit-exact roundtrips).
#[derive(Clone, Debug)]
pub enum PVal {
    Bool(bool),
    I32(i32),
    I64(i64),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
    Str(Vec<u8>),
    Bytes(Vec<u8>),
    /// Enum value by number; names render via the descriptor at read time.
    Enum(i32),
    /// Container markers carry no data — children are child records.
    Msg,
    List,
    Map,
}

impl PartialEq for PVal {
    fn eq(&self, other: &Self) -> bool {
        use PVal::*;
        match (self, other) {
            (Bool(a), Bool(b)) => a == b,
            (I32(a), I32(b)) => a == b,
            (I64(a), I64(b)) => a == b,
            (U32(a), U32(b)) => a == b,
            (U64(a), U64(b)) => a == b,
            (F32(a), F32(b)) => a.to_bits() == b.to_bits(),
            (F64(a), F64(b)) => a.to_bits() == b.to_bits(),
            (Str(a), Str(b)) => a == b,
            (Bytes(a), Bytes(b)) => a == b,
            (Enum(a), Enum(b)) => a == b,
            (Msg, Msg) | (List, List) | (Map, Map) => true,
            _ => false,
        }
    }
}

const PT_FALSE: u8 = 0x00;
const PT_TRUE: u8 = 0x01;
const PT_I32: u8 = 0x02;
const PT_I64: u8 = 0x03;
const PT_U32: u8 = 0x04;
const PT_U64: u8 = 0x05;
const PT_F32: u8 = 0x06;
const PT_F64: u8 = 0x07;
const PT_STR: u8 = 0x08;
const PT_BYTES: u8 = 0x09;
const PT_ENUM: u8 = 0x0A;
const PT_MSG: u8 = 0x0B;
const PT_LIST: u8 = 0x0C;
const PT_MAP: u8 = 0x0D;

impl PVal {
    pub fn encode(&self) -> Vec<u8> {
        fn fixed(tag: u8, bytes: &[u8]) -> Vec<u8> {
            let mut v = Vec::with_capacity(1 + bytes.len());
            v.push(tag);
            v.extend_from_slice(bytes);
            v
        }
        match self {
            PVal::Bool(false) => vec![PT_FALSE],
            PVal::Bool(true) => vec![PT_TRUE],
            PVal::I32(v) => fixed(PT_I32, &v.to_be_bytes()),
            PVal::I64(v) => fixed(PT_I64, &v.to_be_bytes()),
            PVal::U32(v) => fixed(PT_U32, &v.to_be_bytes()),
            PVal::U64(v) => fixed(PT_U64, &v.to_be_bytes()),
            PVal::F32(v) => fixed(PT_F32, &v.to_bits().to_be_bytes()),
            PVal::F64(v) => fixed(PT_F64, &v.to_bits().to_be_bytes()),
            PVal::Str(s) => fixed(PT_STR, s),
            PVal::Bytes(b) => fixed(PT_BYTES, b),
            PVal::Enum(v) => fixed(PT_ENUM, &v.to_be_bytes()),
            PVal::Msg => vec![PT_MSG],
            PVal::List => vec![PT_LIST],
            PVal::Map => vec![PT_MAP],
        }
    }

    pub fn decode(b: &[u8]) -> Option<PVal> {
        match (*b.first()?, &b[1..]) {
            (PT_FALSE, []) => Some(PVal::Bool(false)),
            (PT_TRUE, []) => Some(PVal::Bool(true)),
            (PT_I32, r) if r.len() == 4 => {
                Some(PVal::I32(i32::from_be_bytes(r.try_into().unwrap())))
            }
            (PT_I64, r) if r.len() == 8 => {
                Some(PVal::I64(i64::from_be_bytes(r.try_into().unwrap())))
            }
            (PT_U32, r) if r.len() == 4 => {
                Some(PVal::U32(u32::from_be_bytes(r.try_into().unwrap())))
            }
            (PT_U64, r) if r.len() == 8 => {
                Some(PVal::U64(u64::from_be_bytes(r.try_into().unwrap())))
            }
            (PT_F32, r) if r.len() == 4 => Some(PVal::F32(f32::from_bits(u32::from_be_bytes(
                r.try_into().unwrap(),
            )))),
            (PT_F64, r) if r.len() == 8 => Some(PVal::F64(f64::from_bits(u64::from_be_bytes(
                r.try_into().unwrap(),
            )))),
            (PT_STR, r) => Some(PVal::Str(r.to_vec())),
            (PT_BYTES, r) => Some(PVal::Bytes(r.to_vec())),
            (PT_ENUM, r) if r.len() == 4 => {
                Some(PVal::Enum(i32::from_be_bytes(r.try_into().unwrap())))
            }
            (PT_MSG, []) => Some(PVal::Msg),
            (PT_LIST, []) => Some(PVal::List),
            (PT_MAP, []) => Some(PVal::Map),
            _ => None,
        }
    }
}

/// Repeated-element record payload: RGA left anchor + value.
#[derive(Clone, PartialEq, Debug)]
pub struct PArrElem {
    pub left: Eid,
    pub val: PVal,
}

impl PArrElem {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(10 + 9);
        self.left.encode_to(&mut out);
        out.extend_from_slice(&self.val.encode());
        out
    }

    pub fn decode(b: &[u8]) -> Option<PArrElem> {
        let left = Eid::decode(b)?;
        let val = PVal::decode(b.get(10..)?)?;
        Some(PArrElem { left, val })
    }
}

/// Decoded node passed into [`recompose_tree`] (and the engine's builder).
/// Engines filter dead kind-A nodes but pass repeated-element tombstones
/// (`live = false`) — they still anchor RGA ordering.
#[derive(Clone, PartialEq, Debug)]
pub enum PNodeIn {
    Node { val: PVal, dots: Vec<Dot> },
    Elem { elem: PArrElem, live: bool },
}

/// One record to write.
#[derive(Clone, PartialEq, Debug)]
pub enum PRecord {
    Node { path: Vec<u8>, val: PVal },
    Elem { path: Vec<u8>, elem: PArrElem },
}

impl PRecord {
    pub fn path(&self) -> &[u8] {
        match self {
            PRecord::Node { path, .. } | PRecord::Elem { path, .. } => path,
        }
    }
}

/// Descriptor-free structural recompose for copy-under-new-identity
/// (RENAME/COPY): keeps only nodes whose ancestor chain is live and whose
/// container kinds match, re-chains repeated elements with fresh eids in
/// materialized order (descendant paths rewritten), drops tombstones, and
/// emits kind-A nodes in ascending `(max live dot, path)` order so a
/// monotone restamp preserves relative dot order (oneof winners survive the
/// copy).
pub fn recompose_tree(
    nodes: &[(Vec<u8>, PNodeIn)],
    fresh: &mut dyn FnMut() -> Eid,
) -> Vec<PRecord> {
    // group children by parent path (byte prefix)
    let mut kids: HashMap<&[u8], Vec<(PSeg, &PNodeIn)>> = HashMap::new();
    let mut root: Option<&PNodeIn> = None;
    for (path, node) in nodes {
        if path.is_empty() {
            root = Some(node);
            continue;
        }
        let Some((parent, last)) = split_last(path) else {
            continue; // malformed: skipped deterministically everywhere
        };
        kids.entry(parent).or_default().push((last, node));
    }
    let Some(PNodeIn::Node {
        val: root_val,
        dots: root_dots,
    }) = root
    else {
        return Vec::new();
    };

    // collected kind-A nodes carry their original max live dot for ordering
    let mut out_nodes: Vec<(Dot, Vec<u8>, PVal)> = Vec::new();
    let mut out_elems: Vec<PRecord> = Vec::new();

    fn max_dot(dots: &[Dot]) -> Dot {
        dots.iter()
            .copied()
            .max()
            .unwrap_or(Dot { hlc: 0, origin: 0 })
    }

    /// `old_path` addresses the source records (children lookup); `new_path`
    /// is the destination path with repeated-element ids rewritten fresh.
    #[allow(clippy::too_many_arguments)]
    fn walk(
        old_path: &mut Vec<u8>,
        new_path: &mut Vec<u8>,
        val: &PVal,
        kids: &HashMap<&[u8], Vec<(PSeg, &PNodeIn)>>,
        fresh: &mut dyn FnMut() -> Eid,
        out_nodes: &mut Vec<(Dot, Vec<u8>, PVal)>,
        out_elems: &mut Vec<PRecord>,
    ) {
        let children = kids.get(old_path.as_slice());
        match val {
            PVal::Msg | PVal::Map => {
                let Some(children) = children else { return };
                for (seg, node) in children {
                    // container/segment kind gating: Msg gets Field children,
                    // Map gets MapKey children; anything else is a stale
                    // record under a retyped parent — skipped
                    let matches_kind = matches!(
                        (val, seg),
                        (PVal::Msg, PSeg::Field(_)) | (PVal::Map, PSeg::MapKey(_))
                    );
                    let PNodeIn::Node { val: cval, dots } = node else {
                        continue;
                    };
                    if !matches_kind {
                        continue;
                    }
                    let (olen, nlen) = (old_path.len(), new_path.len());
                    push_seg(old_path, seg);
                    push_seg(new_path, seg);
                    out_nodes.push((max_dot(dots), new_path.clone(), cval.clone()));
                    walk(old_path, new_path, cval, kids, fresh, out_nodes, out_elems);
                    old_path.truncate(olen);
                    new_path.truncate(nlen);
                }
            }
            PVal::List => {
                let Some(children) = children else { return };
                let mut elems: Vec<(Eid, Eid, bool)> = Vec::new();
                let mut by_eid: HashMap<Eid, &PArrElem> = HashMap::new();
                for (seg, node) in children {
                    if let (PSeg::Elem(e), PNodeIn::Elem { elem, live }) = (seg, node) {
                        elems.push((*e, elem.left, *live));
                        by_eid.insert(*e, elem);
                    }
                }
                let mut new_left = EID_HEAD;
                for (old_eid, live) in rga_order(&elems) {
                    if !live {
                        continue; // tombstones are not copied — clean chains
                    }
                    let elem = by_eid[&old_eid];
                    let new_eid = fresh();
                    let (olen, nlen) = (old_path.len(), new_path.len());
                    push_seg(old_path, &PSeg::Elem(old_eid));
                    push_seg(new_path, &PSeg::Elem(new_eid));
                    out_elems.push(PRecord::Elem {
                        path: new_path.clone(),
                        elem: PArrElem {
                            left: new_left,
                            val: elem.val.clone(),
                        },
                    });
                    walk(
                        old_path, new_path, &elem.val, kids, fresh, out_nodes, out_elems,
                    );
                    old_path.truncate(olen);
                    new_path.truncate(nlen);
                    new_left = new_eid;
                }
            }
            _ => {} // scalar leaves have no children
        }
    }

    out_nodes.push((max_dot(root_dots), Vec::new(), root_val.clone()));
    walk(
        &mut Vec::new(),
        &mut Vec::new(),
        root_val,
        &kids,
        fresh,
        &mut out_nodes,
        &mut out_elems,
    );

    // ascending original-dot order (ties by path bytes): restamping with a
    // monotone clock then preserves relative dot order, so materialize-time
    // winners (oneofs, same-path LWW) cannot flip at the destination
    out_nodes.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    let mut out: Vec<PRecord> = out_nodes
        .into_iter()
        .map(|(_, path, val)| PRecord::Node { path, val })
        .collect();
    out.extend(out_elems);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NodeId;

    fn eid(hlc: u64, origin: NodeId) -> Eid {
        Eid { hlc, origin }
    }

    fn f(n: u32) -> PSeg {
        PSeg::Field(n)
    }

    // -- path codec ----------------------------------------------------------

    #[test]
    fn path_roundtrip_all_segment_kinds() {
        let segs = vec![
            f(7),
            f(300),       // multi-byte varint field number
            f(536870911), // max field number (2^29 - 1)
            PSeg::Elem(eid(9, 3)),
            PSeg::MapKey(MKey::Bool(true)),
            PSeg::MapKey(MKey::I32(-5)),
            PSeg::MapKey(MKey::I64(i64::MIN)),
            PSeg::MapKey(MKey::U32(u32::MAX)),
            PSeg::MapKey(MKey::U64(u64::MAX)),
            PSeg::MapKey(MKey::Str(b"key with spaces".to_vec())),
            PSeg::MapKey(MKey::Str(vec![])),
        ];
        let bytes = encode_path(&segs);
        assert_eq!(decode_path(&bytes).unwrap(), segs);
        assert_eq!(decode_path(&[]).unwrap(), Vec::<PSeg>::new());
    }

    #[test]
    fn parent_is_byte_prefix_of_descendants_only() {
        let parent = encode_path(&[f(1)]);
        let child = encode_path(&[f(1), f(2)]);
        let elem = encode_path(&[f(1), PSeg::Elem(eid(5, 1))]);
        let mapchild = encode_path(&[f(1), PSeg::MapKey(MKey::Str(b"k".to_vec()))]);
        assert!(child.starts_with(&parent));
        assert!(elem.starts_with(&parent));
        assert!(mapchild.starts_with(&parent));
        // field numbers whose varints differ in length diverge at the len byte
        let sibling = encode_path(&[f(300)]);
        assert!(!sibling.starts_with(&parent));
        // a one-byte-number sibling diverges at the number byte
        let sibling2 = encode_path(&[f(2)]);
        assert!(!sibling2.starts_with(&parent));
    }

    #[test]
    fn split_last_works() {
        let full = encode_path(&[f(1), PSeg::MapKey(MKey::U32(9)), f(2)]);
        let (parent, last) = split_last(&full).unwrap();
        assert_eq!(parent, encode_path(&[f(1), PSeg::MapKey(MKey::U32(9))]));
        assert_eq!(last, f(2));
        assert!(split_last(&[]).is_none());
    }

    #[test]
    fn path_rejects_garbage() {
        assert!(decode_path(&[0xFF]).is_none()); // unknown segment tag
        assert!(decode_path(&[SEG_ELEM, 1, 2]).is_none()); // short eid
        assert!(decode_path(&[SEG_MAPKEY, 1, 0x99]).is_none()); // unknown key kind
        assert!(decode_path(&[SEG_MAPKEY, 2, 0x01]).is_none()); // short bool key
                                                                // SEG_FIELD whose len window has trailing bytes after the varint
        let mut bad = vec![SEG_FIELD, 2, 0x07, 0x00];
        assert!(decode_path(&bad).is_none());
        bad = vec![SEG_FIELD, 5, 0x07]; // len overruns buffer
        assert!(decode_path(&bad).is_none());
    }

    // -- PVal / PArrElem codecs ------------------------------------------------

    #[test]
    fn pval_roundtrip_all_variants() {
        for v in [
            PVal::Bool(false),
            PVal::Bool(true),
            PVal::I32(-42),
            PVal::I64(i64::MIN),
            PVal::U32(u32::MAX),
            PVal::U64(u64::MAX),
            PVal::F32(3.5),
            PVal::F32(f32::NAN),
            PVal::F64(-0.0),
            PVal::F64(f64::NAN),
            PVal::Str(b"hello".to_vec()),
            PVal::Str(vec![]),
            PVal::Bytes(vec![0, 255, 7]),
            PVal::Enum(-1),
            PVal::Msg,
            PVal::List,
            PVal::Map,
        ] {
            let enc = v.encode();
            assert_eq!(PVal::decode(&enc).unwrap(), v, "{v:?}");
        }
        assert!(PVal::decode(&[]).is_none());
        assert!(PVal::decode(&[0x7F]).is_none()); // unknown tag
        assert!(PVal::decode(&[0x02, 0, 0]).is_none()); // short i32
        assert!(PVal::decode(&[0x0B, 0]).is_none()); // Msg with trailing byte
    }

    #[test]
    fn parr_elem_roundtrip() {
        let e = PArrElem {
            left: eid(55, 4),
            val: PVal::Str(b"x".to_vec()),
        };
        assert_eq!(PArrElem::decode(&e.encode()).unwrap(), e);
        let h = PArrElem {
            left: EID_HEAD,
            val: PVal::Msg,
        };
        assert_eq!(PArrElem::decode(&h.encode()).unwrap(), h);
        assert!(PArrElem::decode(&[1, 2]).is_none());
    }

    // -- recompose_tree ---------------------------------------------------------

    fn dot(hlc: u64, origin: NodeId) -> Dot {
        Dot { hlc, origin }
    }

    fn node(val: PVal, hlc: u64) -> PNodeIn {
        PNodeIn::Node {
            val,
            dots: vec![dot(hlc, 1)],
        }
    }

    fn counter_eids() -> impl FnMut() -> Eid {
        let mut n = 1000u64;
        move || {
            n += 1;
            Eid { hlc: n, origin: 9 }
        }
    }

    #[test]
    fn recompose_simple_message() {
        // root Msg { 1: "a", 2: Msg { 3: 7 } }
        let nodes = vec![
            (vec![], node(PVal::Msg, 10)),
            (encode_path(&[f(1)]), node(PVal::Str(b"a".to_vec()), 20)),
            (encode_path(&[f(2)]), node(PVal::Msg, 15)),
            (encode_path(&[f(2), f(3)]), node(PVal::I32(7), 30)),
        ];
        let mut fresh = counter_eids();
        let out = recompose_tree(&nodes, &mut fresh);
        // all four survive as Node records with the same paths/values
        assert_eq!(out.len(), 4);
        // ascending original-dot order: root(10), f2(15), f1(20), f2.f3(30)
        let paths: Vec<&[u8]> = out.iter().map(|r| r.path()).collect();
        assert_eq!(
            paths,
            vec![
                &[] as &[u8],
                &encode_path(&[f(2)])[..],
                &encode_path(&[f(1)])[..],
                &encode_path(&[f(2), f(3)])[..],
            ]
        );
    }

    #[test]
    fn recompose_rechains_arrays_and_rewrites_descendants() {
        // root Msg { 4: List [ e1: Msg { 5: true }, e2(dead), e3: I32(9) ] }
        let (e1, e2, e3) = (eid(10, 1), eid(20, 1), eid(30, 1));
        let list_path = encode_path(&[f(4)]);
        let p = |e: Eid| encode_path(&[f(4), PSeg::Elem(e)]);
        let nodes = vec![
            (vec![], node(PVal::Msg, 1)),
            (list_path.clone(), node(PVal::List, 2)),
            (
                p(e1),
                PNodeIn::Elem {
                    elem: PArrElem {
                        left: EID_HEAD,
                        val: PVal::Msg,
                    },
                    live: true,
                },
            ),
            (
                encode_path(&[f(4), PSeg::Elem(e1), f(5)]),
                node(PVal::Bool(true), 3),
            ),
            (
                p(e2),
                PNodeIn::Elem {
                    elem: PArrElem {
                        left: e1,
                        val: PVal::Str(b"gone".to_vec()),
                    },
                    live: false, // tombstone: anchors but must not be copied
                },
            ),
            (
                p(e3),
                PNodeIn::Elem {
                    elem: PArrElem {
                        left: e2,
                        val: PVal::I32(9),
                    },
                    live: true,
                },
            ),
        ];
        let mut fresh = counter_eids();
        let out = recompose_tree(&nodes, &mut fresh);
        // live elements only, freshly chained from HEAD in materialized order
        let elems: Vec<&PArrElem> = out
            .iter()
            .filter_map(|r| match r {
                PRecord::Elem { elem, .. } => Some(elem),
                _ => None,
            })
            .collect();
        assert_eq!(elems.len(), 2);
        assert_eq!(elems[0].left, EID_HEAD);
        assert_eq!(elems[0].val, PVal::Msg);
        assert_eq!(elems[1].val, PVal::I32(9));
        // second element anchors on the first's FRESH eid
        let first_fresh = match &out
            .iter()
            .find(|r| matches!(r, PRecord::Elem { elem, .. } if elem.val == PVal::Msg))
            .unwrap()
        {
            PRecord::Elem { path, .. } => match split_last(path).unwrap().1 {
                PSeg::Elem(e) => e,
                _ => panic!(),
            },
            _ => panic!(),
        };
        assert_eq!(elems[1].left, first_fresh);
        assert!(first_fresh.hlc > 1000, "must be a fresh eid");
        // the nested field under e1 was rewritten to the fresh eid path
        let nested = out
            .iter()
            .find(|r| matches!(r, PRecord::Node { val, .. } if *val == PVal::Bool(true)))
            .unwrap();
        assert_eq!(
            nested.path(),
            &encode_path(&[f(4), PSeg::Elem(first_fresh), f(5)])[..]
        );
        // nothing from the tombstone survives
        assert!(!out.iter().any(
            |r| matches!(r, PRecord::Elem { elem, .. } if elem.val == PVal::Str(b"gone".to_vec()))
        ));
    }

    #[test]
    fn recompose_skips_orphans_and_kind_mismatches() {
        let nodes = vec![
            (vec![], node(PVal::Msg, 1)),
            // child under a missing parent (f(9) has no record)
            (encode_path(&[f(9), f(1)]), node(PVal::I32(1), 2)),
            // MapKey child under a Msg-marked parent: kind mismatch
            (encode_path(&[f(2)]), node(PVal::Msg, 3)),
            (
                encode_path(&[f(2), PSeg::MapKey(MKey::U32(1))]),
                node(PVal::I32(5), 4),
            ),
            // Elem child under a Map-marked parent: kind mismatch
            (encode_path(&[f(3)]), node(PVal::Map, 5)),
            (
                encode_path(&[f(3), PSeg::Elem(eid(7, 1))]),
                PNodeIn::Elem {
                    elem: PArrElem {
                        left: EID_HEAD,
                        val: PVal::I32(6),
                    },
                    live: true,
                },
            ),
        ];
        let mut fresh = counter_eids();
        let out = recompose_tree(&nodes, &mut fresh);
        let paths: Vec<&[u8]> = out.iter().map(|r| r.path()).collect();
        assert!(paths.contains(&&encode_path(&[f(2)])[..]));
        assert!(paths.contains(&&encode_path(&[f(3)])[..]));
        assert_eq!(out.len(), 3, "only root, f2, f3 survive: {out:?}");
    }
}

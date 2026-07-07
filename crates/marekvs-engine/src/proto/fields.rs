//! DynamicMessage ↔ field-record walkers (design/18): decompose a message
//! into per-field records, materialize records back into a message with the
//! deterministic skip/oneof rules, and transcribe legacy whole-message
//! values with version-derived element ids.

use std::collections::HashMap;

use super::err::ProtoErr;
use marekvs_core::json::{rga_order, Eid, EID_HEAD};
use marekvs_core::merge::Dot;
use marekvs_core::pdoc::{push_seg, split_last, MKey, PArrElem, PNodeIn, PRecord, PSeg, PVal};
use marekvs_core::NodeId;
use prost_reflect::{DynamicMessage, FieldDescriptor, Kind, MapKey, MessageDescriptor, Value};

/// Per-repeated-field materialized info (json::ArrInfo mirror).
#[derive(Clone, PartialEq, Debug, Default)]
pub struct ArrInfo {
    /// Live elements in document order.
    pub order: Vec<Eid>,
    /// Last element in RGA order INCLUDING tombstones (append anchor).
    pub last: Option<Eid>,
}

/// Index from record paths to CRDT state (dots for covering, array order
/// for index addressing / append anchors).
#[derive(Clone, PartialEq, Debug, Default)]
pub struct PIndex {
    pub node_dots: HashMap<Vec<u8>, Vec<Dot>>,
    pub arrays: HashMap<Vec<u8>, ArrInfo>,
}

/// A materialized decomposed value.
#[derive(Debug)]
pub struct PDoc {
    pub msg: DynamicMessage,
    pub index: PIndex,
}

/// The stored leaf form of a prost-reflect value (containers → markers).
pub fn pval_of(v: &Value) -> PVal {
    match v {
        Value::Bool(b) => PVal::Bool(*b),
        Value::I32(x) => PVal::I32(*x),
        Value::I64(x) => PVal::I64(*x),
        Value::U32(x) => PVal::U32(*x),
        Value::U64(x) => PVal::U64(*x),
        Value::F32(x) => PVal::F32(*x),
        Value::F64(x) => PVal::F64(*x),
        Value::String(s) => PVal::Str(s.as_bytes().to_vec()),
        Value::Bytes(b) => PVal::Bytes(b.to_vec()),
        Value::EnumNumber(n) => PVal::Enum(*n),
        Value::Message(_) => PVal::Msg,
        Value::List(_) => PVal::List,
        Value::Map(_) => PVal::Map,
    }
}

/// The prost-reflect scalar for a stored leaf, validated against the field
/// kind. `None` = kind mismatch (schema skew) — the record is skipped
/// deterministically. Containers are handled by the tree walk, not here.
pub fn value_of(pv: &PVal, kind: &Kind) -> Option<Value> {
    Some(match (pv, kind) {
        (PVal::Bool(b), Kind::Bool) => Value::Bool(*b),
        (PVal::I32(x), Kind::Int32 | Kind::Sint32 | Kind::Sfixed32) => Value::I32(*x),
        (PVal::I64(x), Kind::Int64 | Kind::Sint64 | Kind::Sfixed64) => Value::I64(*x),
        (PVal::U32(x), Kind::Uint32 | Kind::Fixed32) => Value::U32(*x),
        (PVal::U64(x), Kind::Uint64 | Kind::Fixed64) => Value::U64(*x),
        (PVal::F32(x), Kind::Float) => Value::F32(*x),
        (PVal::F64(x), Kind::Double) => Value::F64(*x),
        (PVal::Str(s), Kind::String) => Value::String(String::from_utf8(s.clone()).ok()?),
        (PVal::Bytes(b), Kind::Bytes) => Value::Bytes(b.clone().into()),
        (PVal::Enum(n), Kind::Enum(_)) => Value::EnumNumber(*n),
        _ => return None,
    })
}

fn mkey_of(k: &MapKey) -> MKey {
    match k {
        MapKey::Bool(b) => MKey::Bool(*b),
        MapKey::I32(x) => MKey::I32(*x),
        MapKey::I64(x) => MKey::I64(*x),
        MapKey::U32(x) => MKey::U32(*x),
        MapKey::U64(x) => MKey::U64(*x),
        MapKey::String(s) => MKey::Str(s.as_bytes().to_vec()),
    }
}

/// Stored map key → prost-reflect map key, validated against the map's key
/// kind. `None` = kind mismatch (skipped).
fn map_key_value(k: &MKey, kind: &Kind) -> Option<MapKey> {
    Some(match (k, kind) {
        (MKey::Bool(b), Kind::Bool) => MapKey::Bool(*b),
        (MKey::I32(x), Kind::Int32 | Kind::Sint32 | Kind::Sfixed32) => MapKey::I32(*x),
        (MKey::I64(x), Kind::Int64 | Kind::Sint64 | Kind::Sfixed64) => MapKey::I64(*x),
        (MKey::U32(x), Kind::Uint32 | Kind::Fixed32) => MapKey::U32(*x),
        (MKey::U64(x), Kind::Uint64 | Kind::Fixed64) => MapKey::U64(*x),
        (MKey::Str(s), Kind::String) => MapKey::String(String::from_utf8(s.clone()).ok()?),
        _ => return None,
    })
}

/// Fresh-element-id source. Normal writes mint shard-clock eids and ignore
/// the arguments; transcription derives deterministic ids from the legacy
/// head version (`derived_eid`).
pub type FreshEid<'a> = dyn FnMut(&[u8], u32) -> Eid + 'a;

/// Flatten a message into per-field records rooted at `base` (the node at
/// `base` itself — root or a message-valued field — is emitted too, as a
/// `PVal::Msg` marker). Only PRESENT fields produce records: absence of a
/// record is absence of the field, matching wire encoding.
pub fn decompose_msg(
    base: &[PSeg],
    msg: &DynamicMessage,
    fresh: &mut FreshEid<'_>,
) -> Vec<PRecord> {
    let mut out = Vec::new();
    let mut path = marekvs_core::pdoc::encode_path(base);
    out.push(PRecord::Node {
        path: path.clone(),
        val: PVal::Msg,
    });
    decompose_children(&mut path, msg, fresh, &mut out);
    out
}

/// A message or scalar value at `path` (map values and repeated elements'
/// children route through here).
fn decompose_value(
    path: &mut Vec<u8>,
    v: &Value,
    fresh: &mut FreshEid<'_>,
    out: &mut Vec<PRecord>,
) {
    if let Value::Message(m) = v {
        out.push(PRecord::Node {
            path: path.clone(),
            val: PVal::Msg,
        });
        decompose_children(path, m, fresh, out);
    } else {
        out.push(PRecord::Node {
            path: path.clone(),
            val: pval_of(v),
        });
    }
}

fn decompose_children(
    path: &mut Vec<u8>,
    msg: &DynamicMessage,
    fresh: &mut FreshEid<'_>,
    out: &mut Vec<PRecord>,
) {
    // fields() iterates present fields in declaration order (deterministic —
    // transcription relies on it)
    for (fd, value) in msg.fields() {
        let len = path.len();
        push_seg(path, &PSeg::Field(fd.number()));
        if fd.is_map() {
            out.push(PRecord::Node {
                path: path.clone(),
                val: PVal::Map,
            });
            if let Value::Map(map) = value {
                // sorted by encoded key segment: map iteration order is not
                // deterministic, transcription must be
                let mut entries: Vec<(Vec<u8>, MKey, &Value)> = map
                    .iter()
                    .map(|(k, v)| {
                        let mk = mkey_of(k);
                        let mut seg = Vec::new();
                        push_seg(&mut seg, &PSeg::MapKey(mk.clone()));
                        (seg, mk, v)
                    })
                    .collect();
                entries.sort_by(|a, b| a.0.cmp(&b.0));
                for (_, mk, v) in entries {
                    let l2 = path.len();
                    push_seg(path, &PSeg::MapKey(mk));
                    decompose_value(path, v, fresh, out);
                    path.truncate(l2);
                }
            }
        } else if fd.is_list() {
            out.push(PRecord::Node {
                path: path.clone(),
                val: PVal::List,
            });
            if let Value::List(list) = value {
                let mut left = EID_HEAD;
                for (ordinal, v) in list.iter().enumerate() {
                    let e = fresh(path, ordinal as u32);
                    let l2 = path.len();
                    push_seg(path, &PSeg::Elem(e));
                    out.push(PRecord::Elem {
                        path: path.clone(),
                        elem: PArrElem {
                            left,
                            val: pval_of(v),
                        },
                    });
                    if let Value::Message(m) = v {
                        decompose_children(path, m, fresh, out);
                    }
                    path.truncate(l2);
                    left = e;
                }
            }
        } else {
            decompose_value(path, value, fresh, out);
        }
        path.truncate(len);
    }
}

/// Materialize a decomposed record set against `desc` (the head's winning
/// schema version). Deterministic skip rules: unknown field numbers, kind
/// mismatches, orphans under missing/retyped parents. Real oneof groups
/// resolve to the member with the highest `(max live dot, field number)`.
/// `None` when there is no live root record.
pub fn build_msg(desc: &MessageDescriptor, nodes: &[(Vec<u8>, PNodeIn)]) -> Option<PDoc> {
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
        val: PVal::Msg,
        dots,
    }) = root
    else {
        return None;
    };
    let mut index = PIndex::default();
    index.node_dots.insert(Vec::new(), dots.clone());
    let msg = walk_msg(desc, &mut Vec::new(), &kids, &mut index);
    Some(PDoc { msg, index })
}

fn max_dot(dots: &[Dot]) -> Dot {
    dots.iter()
        .copied()
        .max()
        .unwrap_or(Dot { hlc: 0, origin: 0 })
}

/// A real (mutually exclusive) oneof. proto3 `optional` compiles to a
/// synthetic single-member oneof, which must not participate in exclusion —
/// a one-member group needs no exclusion anyway, so member count is a safe,
/// version-independent detector.
pub(crate) fn real_oneof(fd: &FieldDescriptor) -> Option<prost_reflect::OneofDescriptor> {
    fd.containing_oneof().filter(|o| o.fields().count() > 1)
}

fn walk_msg(
    desc: &MessageDescriptor,
    path: &mut Vec<u8>,
    kids: &HashMap<&[u8], Vec<(PSeg, &PNodeIn)>>,
    index: &mut PIndex,
) -> DynamicMessage {
    let mut m = DynamicMessage::new(desc.clone());
    let Some(children) = kids.get(path.as_slice()) else {
        return m;
    };
    // accept known Field segments carried by kind-A nodes
    let mut fields: Vec<(FieldDescriptor, &PVal, &Vec<Dot>)> = Vec::new();
    for (seg, node) in children {
        let PSeg::Field(n) = seg else { continue };
        let PNodeIn::Node { val, dots } = node else {
            continue;
        };
        let Some(fd) = desc.get_field(*n) else {
            continue; // unknown field number (schema skew): skipped
        };
        fields.push((fd, val, dots));
    }
    // oneof exclusion: highest (max live dot, field number) member wins
    let mut oneof_winner: HashMap<String, (Dot, u32)> = HashMap::new();
    for (fd, _, dots) in &fields {
        if let Some(o) = real_oneof(fd) {
            let cand = (max_dot(dots), fd.number());
            let entry = oneof_winner
                .entry(o.full_name().to_string())
                .or_insert(cand);
            if cand > *entry {
                *entry = cand;
            }
        }
    }

    for (fd, val, dots) in fields {
        if let Some(o) = real_oneof(&fd) {
            if oneof_winner[o.full_name()] != (max_dot(dots), fd.number()) {
                continue; // losing oneof member: record stays stored, unrendered
            }
        }
        let len = path.len();
        push_seg(path, &PSeg::Field(fd.number()));
        let built: Option<Value> = if fd.is_map() {
            (*val == PVal::Map).then(|| build_map(&fd, path, kids, index))
        } else if fd.is_list() {
            (*val == PVal::List).then(|| build_list(&fd, path, kids, index))
        } else if let Kind::Message(sub) = fd.kind() {
            (*val == PVal::Msg).then(|| Value::Message(walk_msg(&sub, path, kids, index)))
        } else {
            value_of(val, &fd.kind())
        };
        if let Some(v) = built {
            index.node_dots.insert(path.clone(), dots.clone());
            m.set_field(&fd, v);
        }
        path.truncate(len);
    }
    m
}

fn build_map(
    fd: &FieldDescriptor,
    path: &mut Vec<u8>,
    kids: &HashMap<&[u8], Vec<(PSeg, &PNodeIn)>>,
    index: &mut PIndex,
) -> Value {
    let Kind::Message(entry) = fd.kind() else {
        return Value::Map(Default::default());
    };
    let key_kind = entry.map_entry_key_field().kind();
    let value_fd = entry.map_entry_value_field();
    let mut out: HashMap<MapKey, Value> = HashMap::new();
    let Some(children) = kids.get(path.as_slice()).map(|v| v.as_slice()) else {
        return Value::Map(out);
    };
    // NB: children slice is borrowed from `kids`; collect owned segs first
    let children: Vec<(PSeg, &PNodeIn)> = children.to_vec();
    for (seg, node) in children {
        let PSeg::MapKey(mk) = &seg else { continue };
        let PNodeIn::Node { val, dots } = node else {
            continue;
        };
        let Some(key) = map_key_value(mk, &key_kind) else {
            continue; // key-kind mismatch (schema skew): skipped
        };
        let len = path.len();
        push_seg(path, &seg);
        let built: Option<Value> = if let Kind::Message(sub) = value_fd.kind() {
            (*val == PVal::Msg).then(|| Value::Message(walk_msg(&sub, path, kids, index)))
        } else {
            value_of(val, &value_fd.kind())
        };
        if let Some(v) = built {
            index.node_dots.insert(path.clone(), dots.clone());
            out.insert(key, v);
        }
        path.truncate(len);
    }
    Value::Map(out)
}

fn build_list(
    fd: &FieldDescriptor,
    path: &mut Vec<u8>,
    kids: &HashMap<&[u8], Vec<(PSeg, &PNodeIn)>>,
    index: &mut PIndex,
) -> Value {
    let mut elems: Vec<(Eid, Eid, bool)> = Vec::new();
    let mut by_eid: HashMap<Eid, &PArrElem> = HashMap::new();
    if let Some(children) = kids.get(path.as_slice()) {
        for (seg, node) in children {
            if let (PSeg::Elem(e), PNodeIn::Elem { elem, live }) = (seg, node) {
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
    let mut out: Vec<Value> = Vec::new();
    for (e, live) in &ordered {
        if !live {
            continue;
        }
        let elem = by_eid[e];
        let len = path.len();
        push_seg(path, &PSeg::Elem(*e));
        let built: Option<Value> = if let Kind::Message(sub) = fd.kind() {
            (elem.val == PVal::Msg).then(|| Value::Message(walk_msg(&sub, path, kids, index)))
        } else {
            value_of(&elem.val, &fd.kind())
        };
        path.truncate(len);
        if let Some(v) = built {
            info.order.push(*e);
            out.push(v);
        }
    }
    index.arrays.insert(path.clone(), info);
    Value::List(out)
}

/// Transcribe a legacy whole-message value into records stamped with the
/// legacy head's version (design/18 upgrade): every node carries the dot
/// `(head_hlc, head_origin)`, repeated-element ids derive deterministically
/// from `(head_hlc, array path, ordinal)` — two nodes upgrading from the
/// same legacy state produce byte-identical records.
pub fn transcribe_records(
    msg: &DynamicMessage,
    head_hlc: u64,
    head_origin: NodeId,
) -> Vec<PRecord> {
    let mut fresh = |array_path: &[u8], ordinal: u32| Eid {
        hlc: fnv1a64(&[&head_hlc.to_be_bytes(), array_path, &ordinal.to_be_bytes()]),
        origin: head_origin,
    };
    decompose_msg(&[], msg, &mut fresh)
}

/// Emit the DESCENDANT records of `value` rooted at `node_path` (the node at
/// `node_path` itself is written kind-aware by the caller). `fd` is the leaf
/// field context; `element` = `value` is a single repeated element (its own
/// record is kind-B, so a message element's fields still descend here). Fresh
/// element ids come from `fresh` (normal writes ignore its arguments).
pub fn children_records(
    node_path: &[u8],
    value: &Value,
    fd: &FieldDescriptor,
    element: bool,
    fresh: &mut FreshEid<'_>,
) -> Vec<PRecord> {
    let mut out = Vec::new();
    let mut path = node_path.to_vec();
    if !element && fd.is_map() {
        if let Value::Map(map) = value {
            // sorted by encoded key segment (map iteration order is not
            // deterministic; the record set must be)
            let mut entries: Vec<(Vec<u8>, MKey, &Value)> = map
                .iter()
                .map(|(k, v)| {
                    let mk = mkey_of(k);
                    let mut seg = Vec::new();
                    push_seg(&mut seg, &PSeg::MapKey(mk.clone()));
                    (seg, mk, v)
                })
                .collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            for (_, mk, v) in entries {
                let l = path.len();
                push_seg(&mut path, &PSeg::MapKey(mk));
                decompose_value(&mut path, v, fresh, &mut out);
                path.truncate(l);
            }
        }
    } else if !element && fd.is_list() {
        if let Value::List(list) = value {
            let mut left = EID_HEAD;
            for (ordinal, v) in list.iter().enumerate() {
                let e = fresh(&path, ordinal as u32);
                let l = path.len();
                push_seg(&mut path, &PSeg::Elem(e));
                out.push(PRecord::Elem {
                    path: path.clone(),
                    elem: PArrElem {
                        left,
                        val: pval_of(v),
                    },
                });
                if let Value::Message(m) = v {
                    decompose_children(&mut path, m, fresh, &mut out);
                }
                path.truncate(l);
                left = e;
            }
        }
    } else if let Value::Message(m) = value {
        decompose_children(&mut path, m, fresh, &mut out);
    }
    out
}

/// The resolved target of a user dot-path against a decomposed value's
/// descriptor + [`PIndex`] (design/18). Record paths are keyed by field
/// number so they survive schema renames.
pub enum Resolved {
    /// A kind-A slot (singular field, whole repeated/map field, or map-entry
    /// value). `fd` is the value's field context (`parse_value` with
    /// `element = false`); `oneof_siblings` are the record paths of the other
    /// members of a real oneof this field belongs to (empty otherwise).
    Node {
        node_path: Vec<u8>,
        fd: FieldDescriptor,
        oneof_siblings: Vec<Vec<u8>>,
    },
    /// An existing repeated element (index < len): rewrite in place keeping
    /// the stored left anchor. `fd` is the repeated field (`element = true`).
    Elem {
        elem_path: Vec<u8>,
        fd: FieldDescriptor,
    },
    /// Append past the last element (index == len): a fresh element chained
    /// after `left`. `fd` is the repeated field (`element = true`).
    Append {
        list_path: Vec<u8>,
        left: Eid,
        fd: FieldDescriptor,
    },
    /// The index addresses past the end of the list, or descends through a
    /// missing element. SETFIELD errors; CLEARFIELD counts nothing.
    OutOfRange,
}

/// A resolved write path plus the container marker nodes that must be created
/// first (missing intermediate messages/maps/lists), each written kind-A with
/// observed = [].
pub struct WritePath {
    pub intermediates: Vec<(Vec<u8>, PVal)>,
    pub target: Resolved,
}

/// Resolve a parsed dot-path (`super::path::parse_path`) into a record-level
/// write target. Walks the descriptor exactly like `path::set_in_value`, but
/// against the stored [`PIndex`] instead of a live message, so index/key
/// addressing resolves to stable element ids and observed dots; container
/// nodes that don't yet exist are collected as `intermediates`.
pub fn resolve_path(
    desc: &MessageDescriptor,
    segs: &[String],
    index: &PIndex,
) -> Result<WritePath, ProtoErr> {
    use super::path::{parse_index, parse_map_key, resolve_field};
    let mut path: Vec<u8> = Vec::new(); // record path of the container we're in
    let mut cur = desc.clone();
    let mut intermediates: Vec<(Vec<u8>, PVal)> = Vec::new();
    let mut i = 0;
    let ensure = |intermediates: &mut Vec<(Vec<u8>, PVal)>, p: &[u8], marker: PVal| {
        if !index.node_dots.contains_key(p) {
            intermediates.push((p.to_vec(), marker));
        }
    };
    loop {
        let fd = resolve_field(&cur, &segs[i])?;
        let mut field_path = path.clone();
        push_seg(&mut field_path, &PSeg::Field(fd.number()));
        let last = i + 1 == segs.len();
        if fd.is_map() {
            if last {
                return Ok(WritePath {
                    intermediates,
                    target: Resolved::Node {
                        node_path: field_path,
                        fd,
                        oneof_siblings: Vec::new(),
                    },
                });
            }
            ensure(&mut intermediates, &field_path, PVal::Map);
            let Kind::Message(entry) = fd.kind() else {
                return Err(ProtoErr::Path("internal: map entry kind".into()));
            };
            let key = parse_map_key(&segs[i + 1], &entry.map_entry_key_field().kind())?;
            let vfd = entry.map_entry_value_field();
            let mut entry_path = field_path;
            push_seg(&mut entry_path, &PSeg::MapKey(mkey_of(&key)));
            if i + 1 == segs.len() - 1 {
                return Ok(WritePath {
                    intermediates,
                    target: Resolved::Node {
                        node_path: entry_path,
                        fd: vfd,
                        oneof_siblings: Vec::new(),
                    },
                });
            }
            let Kind::Message(sub) = vfd.kind() else {
                return Err(ProtoErr::Path(format!(
                    "cannot descend into scalar at '{}'",
                    segs[i + 2]
                )));
            };
            ensure(&mut intermediates, &entry_path, PVal::Msg);
            cur = sub;
            path = entry_path;
            i += 2;
            continue;
        }
        if fd.is_list() {
            if last {
                return Ok(WritePath {
                    intermediates,
                    target: Resolved::Node {
                        node_path: field_path,
                        fd,
                        oneof_siblings: Vec::new(),
                    },
                });
            }
            ensure(&mut intermediates, &field_path, PVal::List);
            let idx = parse_index(&segs[i + 1])?;
            let arr = index.arrays.get(&field_path);
            let len = arr.map_or(0, |a| a.order.len());
            let elem_is_last = i + 1 == segs.len() - 1;
            if idx > len || (idx == len && !elem_is_last) {
                return Ok(WritePath {
                    intermediates,
                    target: Resolved::OutOfRange,
                });
            }
            if idx == len {
                let left = arr.and_then(|a| a.last).unwrap_or(EID_HEAD);
                return Ok(WritePath {
                    intermediates,
                    target: Resolved::Append {
                        list_path: field_path,
                        left,
                        fd,
                    },
                });
            }
            let eid = arr.expect("idx < len implies arr").order[idx];
            let mut elem_path = field_path;
            push_seg(&mut elem_path, &PSeg::Elem(eid));
            if elem_is_last {
                return Ok(WritePath {
                    intermediates,
                    target: Resolved::Elem { elem_path, fd },
                });
            }
            let Kind::Message(sub) = fd.kind() else {
                return Err(ProtoErr::Path(format!(
                    "cannot descend into scalar at '{}'",
                    segs[i + 2]
                )));
            };
            cur = sub;
            path = elem_path;
            i += 2;
            continue;
        }
        // singular field
        if last {
            let oneof_siblings = real_oneof(&fd)
                .map(|o| {
                    o.fields()
                        .filter(|s| s.number() != fd.number())
                        .map(|s| {
                            let mut p = path.clone();
                            push_seg(&mut p, &PSeg::Field(s.number()));
                            p
                        })
                        .collect()
                })
                .unwrap_or_default();
            return Ok(WritePath {
                intermediates,
                target: Resolved::Node {
                    node_path: field_path,
                    fd,
                    oneof_siblings,
                },
            });
        }
        let Kind::Message(sub) = fd.kind() else {
            return Err(ProtoErr::Path(format!(
                "cannot descend into scalar at '{}'",
                segs[i + 1]
            )));
        };
        ensure(&mut intermediates, &field_path, PVal::Msg);
        cur = sub;
        path = field_path;
        i += 1;
    }
}

/// FNV-1a 64-bit (derived-eid hash; local impl, no dependency).
fn fnv1a64(parts: &[&[u8]]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for part in parts {
        for &b in *part {
            h ^= b as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::compile::{compile_source, pool_from_fds};
    use crate::proto::ProtoLimits;
    use marekvs_core::envelope::{Envelope, RecordType};
    use marekvs_core::merge::{element_set, merge_values, resolve};
    use marekvs_core::pdoc::encode_path;
    use prost_reflect::DescriptorPool;

    fn pool() -> DescriptorPool {
        let src = r#"
            syntax = "proto3";
            package t;
            enum Color { COLOR_UNSPECIFIED = 0; RED = 1; BLUE = 2; }
            message Inner { string note = 1; uint64 big = 2; }
            message Torture {
                string name = 1;
                int32 count = 2;
                double ratio = 3;
                bool ok = 4;
                bytes blob = 5;
                Color color = 6;
                Inner inner = 7;
                repeated Inner items = 8;
                map<string, int64> scores = 9;
                repeated string tags = 10;
                map<int32, Inner> by_id = 11;
                map<bool, string> flags = 12;
                oneof choice { string label = 13; int32 code = 14; Inner boxed = 15; }
                optional int32 maybe = 16;
                float ratio32 = 17;
                sint64 signed = 18;
                fixed32 fixed = 19;
            }
        "#;
        let limits = ProtoLimits::from_env();
        let out = compile_source("t", src, Default::default(), &limits).unwrap();
        pool_from_fds(&out.fds).unwrap()
    }

    fn torture_desc(pool: &DescriptorPool) -> MessageDescriptor {
        pool.get_message_by_name("t.Torture").unwrap()
    }

    /// A message exercising every field class.
    fn torture_msg(pool: &DescriptorPool) -> DynamicMessage {
        let desc = torture_desc(pool);
        let mut m = DynamicMessage::new(desc.clone());
        let fd = |n: u32| desc.get_field(n).unwrap();
        m.set_field(&fd(1), Value::String("plan".into()));
        m.set_field(&fd(2), Value::I32(-7));
        m.set_field(&fd(3), Value::F64(2.5));
        m.set_field(&fd(4), Value::Bool(true));
        m.set_field(&fd(5), Value::Bytes(vec![0u8, 255, 7].into()));
        m.set_field(&fd(6), Value::EnumNumber(2));
        let inner_desc = pool.get_message_by_name("t.Inner").unwrap();
        let mut inner = DynamicMessage::new(inner_desc.clone());
        inner.set_field_by_name("note", Value::String("deep".into()));
        inner.set_field_by_name("big", Value::U64(u64::MAX));
        m.set_field(&fd(7), Value::Message(inner.clone()));
        let mut i2 = DynamicMessage::new(inner_desc.clone());
        i2.set_field_by_name("note", Value::String("second".into()));
        m.set_field(
            &fd(8),
            Value::List(vec![Value::Message(inner.clone()), Value::Message(i2)]),
        );
        m.set_field(
            &fd(9),
            Value::Map(
                [
                    (MapKey::String("a".into()), Value::I64(1)),
                    (MapKey::String("b".into()), Value::I64(-2)),
                ]
                .into_iter()
                .collect(),
            ),
        );
        m.set_field(
            &fd(10),
            Value::List(vec![Value::String("x".into()), Value::String("y".into())]),
        );
        m.set_field(
            &fd(11),
            Value::Map(
                [(MapKey::I32(-3), Value::Message(inner.clone()))]
                    .into_iter()
                    .collect(),
            ),
        );
        m.set_field(
            &fd(12),
            Value::Map(
                [(MapKey::Bool(true), Value::String("on".into()))]
                    .into_iter()
                    .collect(),
            ),
        );
        m.set_field(&fd(13), Value::String("chosen".into())); // oneof member
        m.set_field(&fd(16), Value::I32(0)); // optional explicitly at default
        m.set_field(&fd(17), Value::F32(1.25));
        m.set_field(&fd(18), Value::I64(-9));
        m.set_field(&fd(19), Value::U32(9));
        m
    }

    fn seq_eids() -> impl FnMut(&[u8], u32) -> Eid {
        let mut n = 0u64;
        move |_, _| {
            n += 1;
            Eid { hlc: n, origin: 1 }
        }
    }

    /// Decomposed records → build_msg input (all live, per-record dots).
    fn as_nodes(recs: &[PRecord]) -> Vec<(Vec<u8>, PNodeIn)> {
        recs.iter()
            .enumerate()
            .map(|(i, r)| match r {
                PRecord::Node { path, val } => (
                    path.clone(),
                    PNodeIn::Node {
                        val: val.clone(),
                        dots: vec![Dot {
                            hlc: 1000 + i as u64,
                            origin: 1,
                        }],
                    },
                ),
                PRecord::Elem { path, elem } => (
                    path.clone(),
                    PNodeIn::Elem {
                        elem: elem.clone(),
                        live: true,
                    },
                ),
            })
            .collect()
    }

    #[test]
    fn decompose_build_roundtrip() {
        let pool = pool();
        let msg = torture_msg(&pool);
        let mut fresh = seq_eids();
        let recs = decompose_msg(&[], &msg, &mut fresh);
        let doc = build_msg(&torture_desc(&pool), &as_nodes(&recs)).unwrap();
        assert_eq!(doc.msg, msg);
        // presence roundtrips: optional-at-default survives, oneof set
        let desc = torture_desc(&pool);
        assert!(doc.msg.has_field(&desc.get_field(16).unwrap()));
        assert!(doc.msg.has_field(&desc.get_field(13).unwrap()));
    }

    #[test]
    fn absent_fields_produce_no_records() {
        let pool = pool();
        let desc = torture_desc(&pool);
        let mut m = DynamicMessage::new(desc.clone());
        m.set_field(&desc.get_field(1).unwrap(), Value::String("only".into()));
        let mut fresh = seq_eids();
        let recs = decompose_msg(&[], &m, &mut fresh);
        // root marker + one field record
        assert_eq!(recs.len(), 2);
        assert_eq!(
            recs[0],
            PRecord::Node {
                path: vec![],
                val: PVal::Msg
            }
        );
        assert_eq!(
            recs[1],
            PRecord::Node {
                path: encode_path(&[PSeg::Field(1)]),
                val: PVal::Str(b"only".to_vec())
            }
        );
    }

    #[test]
    fn oneof_winner_is_deterministic() {
        let pool = pool();
        let desc = torture_desc(&pool);
        // two live members of `choice`: label(13)@dot 50, code(14)@dot 40
        let nodes = vec![
            (
                vec![],
                PNodeIn::Node {
                    val: PVal::Msg,
                    dots: vec![Dot { hlc: 1, origin: 1 }],
                },
            ),
            (
                encode_path(&[PSeg::Field(13)]),
                PNodeIn::Node {
                    val: PVal::Str(b"label".to_vec()),
                    dots: vec![Dot { hlc: 50, origin: 1 }],
                },
            ),
            (
                encode_path(&[PSeg::Field(14)]),
                PNodeIn::Node {
                    val: PVal::I32(9),
                    dots: vec![Dot { hlc: 40, origin: 2 }],
                },
            ),
        ];
        let doc = build_msg(&desc, &nodes).unwrap();
        assert!(doc.msg.has_field(&desc.get_field(13).unwrap()));
        assert!(!doc.msg.has_field(&desc.get_field(14).unwrap()));
        // equal dots → higher field number wins (defensive determinism)
        let nodes_tie = vec![
            nodes[0].clone(),
            (
                encode_path(&[PSeg::Field(13)]),
                PNodeIn::Node {
                    val: PVal::Str(b"label".to_vec()),
                    dots: vec![Dot { hlc: 50, origin: 1 }],
                },
            ),
            (
                encode_path(&[PSeg::Field(14)]),
                PNodeIn::Node {
                    val: PVal::I32(9),
                    dots: vec![Dot { hlc: 50, origin: 1 }],
                },
            ),
        ];
        let doc = build_msg(&desc, &nodes_tie).unwrap();
        assert!(doc.msg.has_field(&desc.get_field(14).unwrap()));
        assert!(!doc.msg.has_field(&desc.get_field(13).unwrap()));
        // proto3 optional (synthetic oneof) must NOT participate in exclusion
        let nodes_opt = vec![
            nodes[0].clone(),
            (
                encode_path(&[PSeg::Field(13)]),
                PNodeIn::Node {
                    val: PVal::Str(b"l".to_vec()),
                    dots: vec![Dot { hlc: 5, origin: 1 }],
                },
            ),
            (
                encode_path(&[PSeg::Field(16)]),
                PNodeIn::Node {
                    val: PVal::I32(3),
                    dots: vec![Dot { hlc: 9, origin: 1 }],
                },
            ),
        ];
        let doc = build_msg(&desc, &nodes_opt).unwrap();
        assert!(doc.msg.has_field(&desc.get_field(13).unwrap()));
        assert!(doc.msg.has_field(&desc.get_field(16).unwrap()));
    }

    #[test]
    fn skip_rules_are_deterministic() {
        let pool = pool();
        let desc = torture_desc(&pool);
        let mk = |path: Vec<u8>, val: PVal, hlc: u64| {
            (
                path,
                PNodeIn::Node {
                    val,
                    dots: vec![Dot { hlc, origin: 1 }],
                },
            )
        };
        let nodes = vec![
            mk(vec![], PVal::Msg, 1),
            mk(encode_path(&[PSeg::Field(1)]), PVal::Str(b"ok".to_vec()), 2),
            // unknown field number
            mk(encode_path(&[PSeg::Field(999)]), PVal::I32(1), 3),
            // kind mismatch: string field holding an int record
            mk(encode_path(&[PSeg::Field(2)]), PVal::Str(b"no".to_vec()), 4),
            // map-key kind mismatch: scores is map<string,_>, key is u32
            mk(encode_path(&[PSeg::Field(9)]), PVal::Map, 5),
            mk(
                encode_path(&[PSeg::Field(9), PSeg::MapKey(MKey::U32(1))]),
                PVal::I64(1),
                6,
            ),
            // orphan under a field with no container record
            mk(
                encode_path(&[PSeg::Field(7), PSeg::Field(1)]),
                PVal::Str(b"x".to_vec()),
                7,
            ),
        ];
        let doc = build_msg(&desc, &nodes).unwrap();
        assert!(doc.msg.has_field(&desc.get_field(1).unwrap()));
        assert!(!doc.msg.has_field(&desc.get_field(2).unwrap()));
        assert!(!doc.msg.has_field(&desc.get_field(7).unwrap()));
        // scores materializes as an empty (present) map with no entries
        let scores = doc.msg.get_field(&desc.get_field(9).unwrap());
        assert!(scores.as_map().is_none_or(|m| m.is_empty()));
    }

    #[test]
    fn transcription_is_deterministic_and_unique() {
        let pool = pool();
        let msg = torture_msg(&pool);
        let a = transcribe_records(&msg, 12345, 3);
        let b = transcribe_records(&msg, 12345, 3);
        assert_eq!(a, b, "same legacy state must transcribe byte-identically");
        // all derived eids unique
        let mut eids = Vec::new();
        for r in &a {
            if let PRecord::Elem { path, .. } = r {
                if let Some((_, PSeg::Elem(e))) = split_last(path) {
                    eids.push(e);
                }
            }
        }
        let n = eids.len();
        eids.sort_unstable();
        eids.dedup();
        assert_eq!(eids.len(), n, "derived eids must not collide");
        assert!(n >= 4, "torture message has two lists");
        // a different legacy version produces different eids
        let c = transcribe_records(&msg, 12346, 3);
        assert_ne!(a, c);
        // and the records materialize back to the same message
        let doc = build_msg(&torture_desc(&pool), &as_nodes(&a)).unwrap();
        assert_eq!(doc.msg, msg);
    }

    /// Fold decomposed records through the production merge in rotated
    /// orders; the materialized message must be identical every time.
    #[test]
    fn merge_order_independent_materialization() {
        let pool = pool();
        let msg = torture_msg(&pool);
        let mut fresh = seq_eids();
        let recs = decompose_msg(&[], &msg, &mut fresh);
        // encode as real stored records
        let stored: Vec<(Vec<u8>, Vec<u8>)> = recs
            .iter()
            .enumerate()
            .map(|(i, r)| match r {
                PRecord::Node { path, val } => (
                    path.clone(),
                    element_set(
                        RecordType::HashField,
                        1000 + i as u64,
                        1,
                        &val.encode(),
                        &[],
                    ),
                ),
                PRecord::Elem { path, elem } => (
                    path.clone(),
                    Envelope::new(RecordType::List, 1000 + i as u64, 1).encode_with(&elem.encode()),
                ),
            })
            .collect();
        let fold = |order: &[usize]| -> DynamicMessage {
            let mut store: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
            for &i in order {
                let (p, r) = &stored[i];
                match store.get(p) {
                    None => {
                        store.insert(p.clone(), r.clone());
                    }
                    Some(local) => {
                        let m = resolve(local, r, &merge_values(local, r)).to_vec();
                        store.insert(p.clone(), m);
                    }
                }
            }
            let nodes: Vec<(Vec<u8>, PNodeIn)> = store
                .iter()
                .filter_map(|(p, rec)| {
                    let (env, pay) = Envelope::decode(rec)?;
                    if matches!(marekvs_core::pdoc::split_last(p), Some((_, PSeg::Elem(_)))) {
                        Some((
                            p.clone(),
                            PNodeIn::Elem {
                                elem: PArrElem::decode(pay)?,
                                live: !env.is_tombstone(),
                            },
                        ))
                    } else {
                        let st = marekvs_core::merge::ElementState::decode(pay)?;
                        let vb = st.value()?;
                        Some((
                            p.clone(),
                            PNodeIn::Node {
                                val: PVal::decode(vb)?,
                                dots: st.dots(),
                            },
                        ))
                    }
                })
                .collect();
            build_msg(&torture_desc(&pool), &nodes).unwrap().msg
        };
        let n = stored.len();
        let base: Vec<usize> = (0..n).collect();
        let baseline = fold(&base);
        assert_eq!(baseline, msg);
        for rot in 1..n.min(17) {
            let mut order = base.clone();
            order.rotate_left(rot);
            assert_eq!(fold(&order), baseline, "rotation {rot} diverged");
            order.reverse();
            assert_eq!(fold(&order), baseline, "reversed rotation {rot} diverged");
        }
    }

    // -- resolve_path -----------------------------------------------------------

    /// Materialize the torture message and index for resolve_path tests.
    fn torture_pdoc(pool: &DescriptorPool) -> PDoc {
        let msg = torture_msg(pool);
        let recs = decompose_msg(&[], &msg, &mut seq_eids());
        build_msg(&torture_desc(pool), &as_nodes(&recs)).unwrap()
    }

    fn segs(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn resolve_singular_scalar_and_nested_message() {
        let pool = pool();
        let desc = torture_desc(&pool);
        let pdoc = torture_pdoc(&pool);
        // singular scalar `name` (field 1): kind-A node, no intermediates
        let wp = resolve_path(&desc, &segs(&["name"]), &pdoc.index).unwrap();
        assert!(wp.intermediates.is_empty());
        match wp.target {
            Resolved::Node { node_path, .. } => {
                assert_eq!(node_path, encode_path(&[PSeg::Field(1)]))
            }
            _ => panic!("expected Node"),
        }
        // existing nested message field `inner.note` (7.1): no intermediates
        let wp = resolve_path(&desc, &segs(&["inner", "note"]), &pdoc.index).unwrap();
        assert!(wp.intermediates.is_empty());
        match wp.target {
            Resolved::Node { node_path, .. } => {
                assert_eq!(node_path, encode_path(&[PSeg::Field(7), PSeg::Field(1)]))
            }
            _ => panic!("expected Node"),
        }
    }

    #[test]
    fn resolve_creates_missing_intermediate_markers() {
        let pool = pool();
        let desc = torture_desc(&pool);
        // a message with ONLY `name` set — `inner` has no marker record
        let mut m = DynamicMessage::new(desc.clone());
        m.set_field(&desc.get_field(1).unwrap(), Value::String("x".into()));
        let recs = decompose_msg(&[], &m, &mut seq_eids());
        let pdoc = build_msg(&desc, &as_nodes(&recs)).unwrap();
        let wp = resolve_path(&desc, &segs(&["inner", "note"]), &pdoc.index).unwrap();
        assert_eq!(
            wp.intermediates,
            vec![(encode_path(&[PSeg::Field(7)]), PVal::Msg)]
        );
    }

    #[test]
    fn resolve_repeated_replace_append_out_of_range() {
        let pool = pool();
        let desc = torture_desc(&pool);
        let pdoc = torture_pdoc(&pool); // items (field 8) has 2 elements
        assert!(matches!(
            resolve_path(&desc, &segs(&["items", "0"]), &pdoc.index)
                .unwrap()
                .target,
            Resolved::Elem { .. }
        ));
        assert!(matches!(
            resolve_path(&desc, &segs(&["items", "2"]), &pdoc.index)
                .unwrap()
                .target,
            Resolved::Append { .. }
        ));
        assert!(matches!(
            resolve_path(&desc, &segs(&["items", "5"]), &pdoc.index)
                .unwrap()
                .target,
            Resolved::OutOfRange
        ));
    }

    #[test]
    fn resolve_map_entry_and_oneof_siblings() {
        let pool = pool();
        let desc = torture_desc(&pool);
        let pdoc = torture_pdoc(&pool); // scores {a,b}, choice=label(13)
                                        // map entry `scores.a` → Field(9)+MapKey("a")
        let wp = resolve_path(&desc, &segs(&["scores", "a"]), &pdoc.index).unwrap();
        match wp.target {
            Resolved::Node { node_path, .. } => assert_eq!(
                node_path,
                encode_path(&[PSeg::Field(9), PSeg::MapKey(MKey::Str(b"a".to_vec()))])
            ),
            _ => panic!("expected Node"),
        }
        // real oneof member `code` (14): siblings are label(13) + boxed(15)
        let wp = resolve_path(&desc, &segs(&["code"]), &pdoc.index).unwrap();
        match wp.target {
            Resolved::Node { oneof_siblings, .. } => {
                assert_eq!(oneof_siblings.len(), 2);
                assert!(oneof_siblings.contains(&encode_path(&[PSeg::Field(13)])));
                assert!(oneof_siblings.contains(&encode_path(&[PSeg::Field(15)])));
            }
            _ => panic!("expected Node"),
        }
        // proto3 optional `maybe` (16, synthetic oneof) has NO siblings
        let wp = resolve_path(&desc, &segs(&["maybe"]), &pdoc.index).unwrap();
        match wp.target {
            Resolved::Node { oneof_siblings, .. } => assert!(oneof_siblings.is_empty()),
            _ => panic!("expected Node"),
        }
    }
}

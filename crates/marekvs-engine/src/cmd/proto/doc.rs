//! Shard-side load + write helpers for field-decomposed proto values
//! (design/18). Mirrors `cmd/json/doc.rs`: one record per field path under
//! `ikey::Tag::ProtoField`, PVal-typed. Records are ORSWOT elements
//! (`RecordType::HashField`) for singular fields / map entries / container
//! markers and LWW registers (`RecordType::List`) for repeated elements.

use prost_reflect::{DynamicMessage, FieldDescriptor, Value};

use crate::proto::fields::{self, pval_of};
use crate::store::{get_raw, now_ms, scan_prefix, write_merged, ShardCtx};
use marekvs_core::envelope::{Envelope, RecordType};
use marekvs_core::merge::{element_dots, element_remove, element_set, Dot, ElementState};
use marekvs_core::pdoc::{self, PArrElem, PNodeIn, PRecord, PSeg, PVal};
use marekvs_core::{ikey, NodeId};

use marekvs_core::json::{Eid, EID_HEAD};

/// The stored record set of one decomposed value, decoded for `build_msg`:
/// records shadowed by the head delete clock or expired are dropped; dead
/// kind-A elements (empty OR state / tombstone) are dropped; repeated-element
/// tombstones are kept with `live = false` (they still anchor RGA ordering).
pub(crate) fn load_pnodes(ctx: &ShardCtx, key: &[u8], del_hlc: u64) -> Vec<(Vec<u8>, PNodeIn)> {
    let mut nodes: Vec<(Vec<u8>, PNodeIn)> = Vec::new();
    let now = now_ms();
    scan_prefix(
        ctx,
        &ikey::collection_prefix(ikey::Tag::ProtoField, key),
        |k, v| {
            let (p, (env, pay)) = match (ikey::parse(k), Envelope::decode(v)) {
                (Some(p), Some(d)) => (p, d),
                _ => return true,
            };
            if env.hlc <= del_hlc || env.is_expired(now) {
                return true;
            }
            // root has an empty suffix → split_last None → kind-A node.
            if matches!(pdoc::split_last(p.suffix), Some((_, PSeg::Elem(_)))) {
                if let Some(elem) = PArrElem::decode(pay) {
                    nodes.push((
                        p.suffix.to_vec(),
                        PNodeIn::Elem {
                            elem,
                            live: !env.is_tombstone(),
                        },
                    ));
                }
            } else if !env.is_tombstone() {
                if let Some(st) = ElementState::decode(pay) {
                    if let (Some(vb), dots) = (st.value(), st.dots()) {
                        if let Some(val) = PVal::decode(vb) {
                            nodes.push((p.suffix.to_vec(), PNodeIn::Node { val, dots }));
                        }
                    }
                }
            }
            true
        },
    );
    nodes
}

// ---------------------------------------------------------------------------
// record write primitives
// ---------------------------------------------------------------------------

pub(crate) fn fresh_eid(ctx: &ShardCtx) -> Eid {
    Eid {
        hlc: ctx.hlc.now(),
        origin: ctx.node_id,
    }
}

/// Write/overwrite a kind-A node (singular field / map entry / container
/// marker): covers `observed` and installs one fresh add in a single record.
pub(crate) fn write_node(ctx: &ShardCtx, key: &[u8], path: &[u8], val: &PVal, observed: &[Dot]) {
    let rec = element_set(
        RecordType::HashField,
        ctx.hlc.now(),
        ctx.node_id,
        &val.encode(),
        observed,
    );
    write_merged(ctx, &ikey::proto_field_key(key, path), &rec);
}

/// Observed-remove a kind-A node.
pub(crate) fn remove_node(ctx: &ShardCtx, key: &[u8], path: &[u8], observed: &[Dot]) {
    let rec = element_remove(RecordType::HashField, ctx.hlc.now(), ctx.node_id, observed);
    write_merged(ctx, &ikey::proto_field_key(key, path), &rec);
}

/// Write/overwrite a kind-B repeated-element node.
pub(crate) fn write_elem(ctx: &ShardCtx, key: &[u8], path: &[u8], elem: &PArrElem) {
    let rec =
        Envelope::new(RecordType::List, ctx.hlc.now(), ctx.node_id).encode_with(&elem.encode());
    write_merged(ctx, &ikey::proto_field_key(key, path), &rec);
}

/// Tombstone a repeated-element node. The payload (left anchor + value) is
/// preserved — it keeps ordering the other elements after physical death.
pub(crate) fn tomb_elem(ctx: &ShardCtx, key: &[u8], path: &[u8], elem: &PArrElem) {
    let rec = Envelope::tombstone(RecordType::List, ctx.hlc.now(), ctx.node_id)
        .encode_with(&elem.encode());
    write_merged(ctx, &ikey::proto_field_key(key, path), &rec);
}

/// The live add-dots of the kind-A node stored at `path` (empty when absent).
pub(crate) fn node_observed(ctx: &ShardCtx, key: &[u8], path: &[u8]) -> Vec<Dot> {
    get_raw(ctx, &ikey::proto_field_key(key, path)).map_or_else(Vec::new, |v| {
        Envelope::decode(&v).map_or_else(Vec::new, |(_, pay)| element_dots(pay))
    })
}

/// The left anchor stored in the repeated-element record at `path` (EID_HEAD
/// when unreadable — defensive).
fn stored_left(ctx: &ShardCtx, key: &[u8], path: &[u8]) -> Eid {
    get_raw(ctx, &ikey::proto_field_key(key, path))
        .and_then(|v| Envelope::decode(&v).and_then(|(_, pay)| PArrElem::decode(pay)))
        .map_or(EID_HEAD, |e| e.left)
}

/// The full stored repeated element at `path`, if any.
fn stored_elem(ctx: &ShardCtx, key: &[u8], path: &[u8]) -> Option<PArrElem> {
    get_raw(ctx, &ikey::proto_field_key(key, path))
        .and_then(|v| Envelope::decode(&v).and_then(|(_, pay)| PArrElem::decode(pay)))
}

/// Cover every stored record STRICTLY below `node_path` (descendants only;
/// the node's own record is rewritten by the caller). Records already
/// shadowed by the head delete clock are left alone.
pub(crate) fn cover_descendants(ctx: &ShardCtx, key: &[u8], node_path: &[u8], del_hlc: u64) {
    let mut kind_a: Vec<(Vec<u8>, Vec<Dot>)> = Vec::new();
    let mut kind_b: Vec<(Vec<u8>, PArrElem)> = Vec::new();
    scan_prefix(ctx, &ikey::proto_field_key(key, node_path), |k, v| {
        let (p, (env, pay)) = match (ikey::parse(k), Envelope::decode(v)) {
            (Some(p), Some(d)) => (p, d),
            _ => return true,
        };
        if p.suffix == node_path || env.is_tombstone() || env.hlc <= del_hlc {
            return true;
        }
        match pdoc::split_last(p.suffix) {
            Some((_, PSeg::Elem(_))) => {
                if let Some(elem) = PArrElem::decode(pay) {
                    kind_b.push((p.suffix.to_vec(), elem));
                }
            }
            _ => kind_a.push((p.suffix.to_vec(), element_dots(pay))),
        }
        true
    });
    for (path, dots) in kind_a {
        remove_node(ctx, key, &path, &dots);
    }
    for (path, elem) in kind_b {
        tomb_elem(ctx, key, &path, &elem);
    }
}

// ---------------------------------------------------------------------------
// high-level writes (SETFIELD/CLEARFIELD building blocks)
// ---------------------------------------------------------------------------

/// How to stamp the ROOT record of a written value.
pub(crate) enum Slot {
    /// kind-A node: `element_set` covering these observed dots.
    Node(Vec<Dot>),
    /// kind-B repeated element, rewritten in place (stored left preserved).
    ElemReplace,
    /// kind-B repeated element append (fresh element already appended to the
    /// path); chained after `left`.
    ElemAppend(Eid),
}

/// Write `value` at `node_path`: cover any prior descendants, write the
/// kind-aware root, then the fresh child records. `fd`/`element` are the leaf
/// field context (drive child decomposition). Handles scalar leaves (no
/// children) and whole message/repeated/map containers uniformly.
#[allow(clippy::too_many_arguments)]
pub(crate) fn write_value_at(
    ctx: &ShardCtx,
    key: &[u8],
    node_path: &[u8],
    slot: Slot,
    value: &Value,
    fd: &FieldDescriptor,
    element: bool,
    del_hlc: u64,
) {
    cover_descendants(ctx, key, node_path, del_hlc);
    let root = pval_of(value);
    match slot {
        Slot::Node(observed) => write_node(ctx, key, node_path, &root, &observed),
        Slot::ElemReplace => {
            let left = stored_left(ctx, key, node_path);
            write_elem(ctx, key, node_path, &PArrElem { left, val: root });
        }
        Slot::ElemAppend(left) => {
            write_elem(ctx, key, node_path, &PArrElem { left, val: root });
        }
    }
    let mut fresh = |_: &[u8], _: u32| fresh_eid(ctx);
    for rec in fields::children_records(node_path, value, fd, element, &mut fresh) {
        match rec {
            PRecord::Node { path, val } => write_node(ctx, key, &path, &val, &[]),
            PRecord::Elem { path, elem } => write_elem(ctx, key, &path, &elem),
        }
    }
}

/// Delete a kind-A node subtree (CLEARFIELD on a field / map entry): cover
/// descendants + observed-remove the node.
pub(crate) fn delete_node(
    ctx: &ShardCtx,
    key: &[u8],
    node_path: &[u8],
    observed: &[Dot],
    del_hlc: u64,
) {
    cover_descendants(ctx, key, node_path, del_hlc);
    remove_node(ctx, key, node_path, observed);
}

/// Delete a repeated element (CLEARFIELD on a list index): cover its
/// descendants + tombstone the element (preserving its anchor).
pub(crate) fn delete_elem(ctx: &ShardCtx, key: &[u8], elem_path: &[u8], del_hlc: u64) {
    cover_descendants(ctx, key, elem_path, del_hlc);
    if let Some(elem) = stored_elem(ctx, key, elem_path) {
        tomb_elem(ctx, key, elem_path, &elem);
    }
}

/// Remove a stored sibling oneof member (and its subtree) if present, covering
/// its live dots. Returns whether a live record was found.
pub(crate) fn remove_stored_node(ctx: &ShardCtx, key: &[u8], path: &[u8], del_hlc: u64) -> bool {
    let Some((env, dots)) = get_raw(ctx, &ikey::proto_field_key(key, path))
        .and_then(|v| Envelope::decode(&v).map(|(env, pay)| (env, element_dots(pay))))
    else {
        return false;
    };
    if env.is_tombstone() {
        return false;
    }
    cover_descendants(ctx, key, path, del_hlc);
    remove_node(ctx, key, path, &dots);
    true
}

// ---------------------------------------------------------------------------
// legacy upgrade (fmt=1 → fmt=2)
// ---------------------------------------------------------------------------

/// Transcribe a legacy whole-message value into field records stamped with the
/// ORIGINAL head version (design/18 upgrade-on-write). Kind-A records carry the
/// dot `(head_hlc, head_origin)`; repeated elements use deterministic derived
/// ids — two nodes upgrading from the same fmt=1 state write byte-identical
/// records (idempotent under merge).
pub(crate) fn transcribe_v1(
    ctx: &ShardCtx,
    key: &[u8],
    head_hlc: u64,
    head_origin: NodeId,
    msg: &DynamicMessage,
) {
    for rec in fields::transcribe_records(msg, head_hlc, head_origin) {
        match rec {
            PRecord::Node { path, val } => {
                let r = element_set(
                    RecordType::HashField,
                    head_hlc,
                    head_origin,
                    &val.encode(),
                    &[],
                );
                write_merged(ctx, &ikey::proto_field_key(key, &path), &r);
            }
            PRecord::Elem { path, elem } => {
                let r = Envelope::new(RecordType::List, head_hlc, head_origin)
                    .encode_with(&elem.encode());
                write_merged(ctx, &ikey::proto_field_key(key, &path), &r);
            }
        }
    }
}

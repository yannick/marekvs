//! Load + materialize JSON documents from the store, plus the shared write
//! helpers every JSON.* mutation goes through (design/16).

use crate::store::{self, get_head, get_raw, now_ms, scan_prefix, write_merged, ShardCtx};
use marekvs_core::envelope::{head, Envelope, RecordType};
use marekvs_core::ikey;
use marekvs_core::json::{
    build_doc, decode_path, decompose_children_at, jval_of, split_last, ArrElem, Doc, DocIndex,
    Eid, JVal, JsonRecord, NodeIn, Seg, EID_HEAD,
};
use marekvs_core::merge::{element_dots, element_remove, element_set, Dot, ElementState};

/// The stored record set of one document, decoded for `build_doc`:
/// * records shadowed by the head delete clock or expired are dropped;
/// * dead map entries (empty OR state) are dropped;
/// * array-element tombstones are kept with `live = false` — they still
///   anchor RGA ordering.
pub(crate) fn load_nodes(ctx: &ShardCtx, key: &[u8], del_hlc: u64) -> Vec<(Vec<u8>, NodeIn)> {
    let mut nodes: Vec<(Vec<u8>, NodeIn)> = Vec::new();
    let now = now_ms();
    scan_prefix(
        ctx,
        &ikey::collection_prefix(ikey::Tag::Json, key),
        |k, v| {
            let (p, (env, pay)) = match (ikey::parse(k), Envelope::decode(v)) {
                (Some(p), Some(d)) => (p, d),
                _ => return true,
            };
            if env.hlc <= del_hlc || env.is_expired(now) {
                return true;
            }
            let is_elem = matches!(
                decode_path(p.suffix).and_then(|mut s| s.pop()),
                Some(Seg::Elem(_))
            );
            if is_elem {
                if let Some(elem) = ArrElem::decode(pay) {
                    nodes.push((
                        p.suffix.to_vec(),
                        NodeIn::ArrElem {
                            elem,
                            live: !env.is_tombstone(),
                        },
                    ));
                }
            } else if !env.is_tombstone() {
                if let Some(st) = ElementState::decode(pay) {
                    if let (Some(vb), dots) = (st.value(), st.dots()) {
                        if let Some(val) = JVal::decode(vb) {
                            nodes.push((p.suffix.to_vec(), NodeIn::Map { val, dots }));
                        }
                    }
                }
            }
            true
        },
    );
    nodes
}

/// Materialize the visible document at `key`. `None` when the key is absent,
/// deleted, expired, or not a JSON doc. Returns the doc plus the head's
/// delete clock (writers need it for shadow-correct record stamping).
#[allow(dead_code)] // wired up by the JSON.* handlers (phase A4)
pub(crate) fn load_doc(ctx: &ShardCtx, key: &[u8]) -> Option<(Doc, u64)> {
    let (env, ctype, del) = get_head(ctx, key)?;
    if env.is_tombstone() || env.is_expired(now_ms()) || ctype != head::CTYPE_JSON {
        return None;
    }
    let nodes = load_nodes(ctx, key, del);
    build_doc(&nodes).map(|d| (d, del))
}

/// WRONGTYPE fence: does another (non-JSON) type currently hold this key?
pub(crate) fn other_type_holds(ctx: &ShardCtx, key: &[u8]) -> bool {
    !matches!(store::key_type(ctx, key), None | Some(head::CTYPE_JSON))
}

// ---------------------------------------------------------------------------
// write helpers — every JSON mutation is one of these record shapes
// ---------------------------------------------------------------------------

pub(crate) fn fresh_eid(ctx: &ShardCtx) -> Eid {
    Eid {
        hlc: ctx.hlc.now(),
        origin: ctx.node_id,
    }
}

/// Write/overwrite a map-entry (kind A) node: covers `observed` dots and
/// installs one fresh add in a single record.
pub(crate) fn write_map_node(
    ctx: &ShardCtx,
    key: &[u8],
    path: &[u8],
    val: &JVal,
    observed: &[Dot],
) {
    let rec = element_set(
        RecordType::HashField,
        ctx.hlc.now(),
        ctx.node_id,
        &val.encode(),
        observed,
    );
    write_merged(ctx, &ikey::json_node_key(key, path), &rec);
}

/// Observed-remove a map-entry node.
pub(crate) fn remove_map_node(ctx: &ShardCtx, key: &[u8], path: &[u8], observed: &[Dot]) {
    let rec = element_remove(RecordType::HashField, ctx.hlc.now(), ctx.node_id, observed);
    write_merged(ctx, &ikey::json_node_key(key, path), &rec);
}

/// Write/overwrite an array-element (kind B) node.
pub(crate) fn write_arr_node(ctx: &ShardCtx, key: &[u8], path: &[u8], elem: &ArrElem) {
    let rec =
        Envelope::new(RecordType::List, ctx.hlc.now(), ctx.node_id).encode_with(&elem.encode());
    write_merged(ctx, &ikey::json_node_key(key, path), &rec);
}

/// Tombstone an array-element node. The payload (left anchor) is preserved —
/// it keeps ordering other elements after physical death.
pub(crate) fn tomb_arr_node(ctx: &ShardCtx, key: &[u8], path: &[u8], elem: &ArrElem) {
    let rec = Envelope::tombstone(RecordType::List, ctx.hlc.now(), ctx.node_id)
        .encode_with(&elem.encode());
    write_merged(ctx, &ikey::json_node_key(key, path), &rec);
}

/// Cover every stored record STRICTLY below `node_path` (descendants only;
/// the node's own record is rewritten by the caller). Records already
/// shadowed by the head delete clock are left alone.
pub(crate) fn cover_descendants(ctx: &ShardCtx, key: &[u8], node_path: &[u8], del_hlc: u64) {
    let mut kind_a: Vec<(Vec<u8>, Vec<Dot>)> = Vec::new();
    let mut kind_b: Vec<(Vec<u8>, ArrElem)> = Vec::new();
    scan_prefix(ctx, &ikey::json_node_key(key, node_path), |k, v| {
        let (p, (env, pay)) = match (ikey::parse(k), Envelope::decode(v)) {
            (Some(p), Some(d)) => (p, d),
            _ => return true,
        };
        if p.suffix == node_path || env.is_tombstone() || env.hlc <= del_hlc {
            return true;
        }
        match split_last(p.suffix) {
            Some((_, Seg::Elem(_))) => {
                if let Some(elem) = ArrElem::decode(pay) {
                    kind_b.push((p.suffix.to_vec(), elem));
                }
            }
            _ => kind_a.push((p.suffix.to_vec(), element_dots(pay))),
        }
        true
    });
    for (path, dots) in kind_a {
        remove_map_node(ctx, key, &path, &dots);
    }
    for (path, elem) in kind_b {
        tomb_arr_node(ctx, key, &path, &elem);
    }
}

/// The left anchor stored in the array-element record at `node_path`
/// (EID_HEAD when the record is unreadable — defensive).
fn stored_left(ctx: &ShardCtx, key: &[u8], node_path: &[u8]) -> Eid {
    get_raw(ctx, &ikey::json_node_key(key, node_path))
        .and_then(|v| Envelope::decode(&v).and_then(|(_, pay)| ArrElem::decode(pay)))
        .map_or(EID_HEAD, |e| e.left)
}

/// Write the fresh child records of `value` under `node_path`.
pub(crate) fn write_children(
    ctx: &ShardCtx,
    key: &[u8],
    node_path: &[u8],
    value: &serde_json::Value,
) {
    let mut fresh = || fresh_eid(ctx);
    for rec in decompose_children_at(node_path, value, &mut fresh) {
        match rec {
            JsonRecord::Map { path, val } => write_map_node(ctx, key, &path, &val, &[]),
            JsonRecord::Arr { path, elem } => write_arr_node(ctx, key, &path, &elem),
        }
    }
}

/// Overwrite a scalar node in place (NUMINCRBY/STRAPPEND/TOGGLE/CLEAR-zero):
/// map entries cover their observed dots; array elements keep their stored
/// left anchor.
pub(crate) fn update_scalar_node(
    ctx: &ShardCtx,
    key: &[u8],
    node_path: &[u8],
    val: &JVal,
    index: &DocIndex,
) {
    match split_last(node_path) {
        Some((_, Seg::Elem(_))) => {
            let left = stored_left(ctx, key, node_path);
            write_arr_node(
                ctx,
                key,
                node_path,
                &ArrElem {
                    left,
                    val: val.clone(),
                },
            );
        }
        _ => {
            let observed = index.map_dots.get(node_path).cloned().unwrap_or_default();
            write_map_node(ctx, key, node_path, val, &observed);
        }
    }
}

/// Replace the subtree rooted at `node_path` with `value` (JSON.SET on an
/// existing or freshly-created node). Handles both node kinds; array-element
/// nodes keep their stored left anchor so their position is stable.
pub(crate) fn replace_subtree(
    ctx: &ShardCtx,
    key: &[u8],
    node_path: &[u8],
    value: &serde_json::Value,
    index: &DocIndex,
    del_hlc: u64,
) {
    cover_descendants(ctx, key, node_path, del_hlc);
    match split_last(node_path) {
        Some((_, Seg::Elem(_))) => {
            let left = stored_left(ctx, key, node_path);
            write_arr_node(
                ctx,
                key,
                node_path,
                &ArrElem {
                    left,
                    val: jval_of(value),
                },
            );
        }
        _ => {
            let observed = index.map_dots.get(node_path).cloned().unwrap_or_default();
            write_map_node(ctx, key, node_path, &jval_of(value), &observed);
        }
    }
    write_children(ctx, key, node_path, value);
}

/// Delete the subtree rooted at `node_path` (node + descendants).
pub(crate) fn delete_subtree(
    ctx: &ShardCtx,
    key: &[u8],
    node_path: &[u8],
    index: &DocIndex,
    del_hlc: u64,
) {
    cover_descendants(ctx, key, node_path, del_hlc);
    match split_last(node_path) {
        Some((_, Seg::Elem(_))) => {
            let left = stored_left(ctx, key, node_path);
            // value replaced by Null in the tombstone: only the anchor matters
            tomb_arr_node(
                ctx,
                key,
                node_path,
                &ArrElem {
                    left,
                    val: JVal::Null,
                },
            );
        }
        _ => {
            let observed = index.map_dots.get(node_path).cloned().unwrap_or_default();
            remove_map_node(ctx, key, node_path, &observed);
        }
    }
}

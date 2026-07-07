//! Load + materialize JSON documents from the store (design/16).

use crate::store::{self, get_head, now_ms, scan_prefix, ShardCtx};
use marekvs_core::envelope::{head, Envelope};
use marekvs_core::ikey;
use marekvs_core::json::{build_doc, decode_path, ArrElem, Doc, JVal, NodeIn, Seg};
use marekvs_core::merge::ElementState;

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
#[allow(dead_code)] // wired up by the JSON.* handlers (phase A4)
pub(crate) fn other_type_holds(ctx: &ShardCtx, key: &[u8]) -> bool {
    !matches!(store::key_type(ctx, key), None | Some(head::CTYPE_JSON))
}

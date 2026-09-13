//! Adapter for already visibility-gated marekvs JSON records. Dead array
//! anchors must be retained by the caller; head clocks and expiry are gated
//! by the engine before this boundary.
use crate::{model::ModelError, Budget, Sid, Tree};
use marekvs_core::json::{self, NodeIn, Seg};
use std::collections::{BTreeMap, BTreeSet};
/// Evidence from one source scan. Never put this in a cache keyed only by Sid.
#[derive(Clone, Debug)]
pub struct EidBinding {
    pub sid: Sid,
    pub ordinals: BTreeMap<Vec<u32>, [u8; 10]>,
}
pub fn from_snapshot(nodes: &[(Vec<u8>, NodeIn)]) -> Result<Tree, ModelError> {
    snapshot_with_binding(nodes, &Budget::default()).map(|(t, _)| t)
}
pub fn snapshot_with_binding(
    nodes: &[(Vec<u8>, NodeIn)],
    budget: &Budget,
) -> Result<(Tree, EidBinding), ModelError> {
    // Guard raw record work and recursion before invoking the core materializer.
    let mut seen = BTreeSet::new();
    let mut bytes = 0usize;
    for (path, node) in nodes {
        if !seen.insert(path.as_slice()) {
            return Err(ModelError::Shape {
                path: "$".into(),
                why: "duplicate record path",
            });
        }
        let segments = json::decode_path(path).ok_or(ModelError::Shape {
            path: "$".into(),
            why: "invalid record path",
        })?;
        if segments.len() > budget.max_depth.saturating_mul(2).saturating_add(8) {
            return Err(ModelError::Limit {
                bound: "depth",
                limit: budget.max_depth,
            });
        }
        bytes = bytes.saturating_add(path.len()).saturating_add(match node {
            NodeIn::Map { val, .. } => val.encode().len(),
            NodeIn::ArrElem { elem, .. } => elem.encode().len(),
        });
        if bytes > budget.max_bytes {
            return Err(ModelError::Limit {
                bound: "record_bytes",
                limit: budget.max_bytes,
            });
        }
    }
    let doc = json::build_doc(nodes).ok_or(ModelError::Shape {
        path: "$".into(),
        why: "missing visible root",
    })?;
    let mut tree = Tree::from_json_with_budget(&doc.value, budget)?;
    let mut binding = EidBinding {
        sid: tree.sid,
        ordinals: BTreeMap::new(),
    };
    let mut stack = vec![(tree.root, Vec::<u8>::new())];
    while let Some((id, path)) = stack.pop() {
        let mut children_path = path;
        json::push_seg(&mut children_path, &Seg::Field("c".into()));
        if let Some(info) = doc.index.arrays.get(&children_path) {
            if info.order.len() != tree.children(id).len() {
                return Err(ModelError::Shape {
                    path: "$".into(),
                    why: "child identity count mismatch",
                });
            }
            for (&child, &eid) in tree.children(id).iter().zip(&info.order).rev() {
                let mut encoded = Vec::with_capacity(10);
                eid.encode_to(&mut encoded);
                binding
                    .ordinals
                    .insert(tree.ordinal_path(child), encoded.try_into().unwrap());
                let mut p = children_path.clone();
                json::push_seg(&mut p, &Seg::Elem(eid));
                stack.push((child, p));
            }
        }
    }
    bind_eids(&mut tree, &binding)?;
    Ok((tree, binding))
}
pub fn bind_eids(tree: &mut Tree, binding: &EidBinding) -> Result<(), ModelError> {
    if tree.sid != binding.sid {
        return Err(ModelError::Shape {
            path: "$".into(),
            why: "identity binding snapshot mismatch",
        });
    }
    let mut assigned = Vec::with_capacity(tree.nodes.len());
    let valid: BTreeSet<_> = (0..tree.nodes.len())
        .map(|id| tree.ordinal_path(id as u32))
        .collect();
    if binding
        .ordinals
        .keys()
        .any(|p| !valid.contains(p) || p.is_empty())
    {
        return Err(ModelError::Shape {
            path: "$".into(),
            why: "invalid identity ordinal",
        });
    }
    for id in 0..tree.nodes.len() {
        assigned.push(binding.ordinals.get(&tree.ordinal_path(id as u32)).copied());
    }
    for (n, eid) in tree.nodes.iter_mut().zip(assigned) {
        n.eid = eid;
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    use marekvs_core::json::{decompose, Eid, JsonRecord};
    use serde_json::json;
    fn records(v: &serde_json::Value, offset: u64) -> Vec<(Vec<u8>, NodeIn)> {
        let mut seq = offset;
        decompose(&[], v, &mut || {
            seq += 1;
            Eid {
                hlc: seq,
                origin: 1,
            }
        })
        .into_iter()
        .map(|r| match r {
            JsonRecord::Map { path, val } => (path, NodeIn::Map { val, dots: vec![] }),
            JsonRecord::Arr { path, elem } => (path, NodeIn::ArrElem { elem, live: true }),
        })
        .collect()
    }
    #[test]
    fn records_roundtrip_and_source_binding() {
        let v =
            json!({"t":"doc","c":[{"t":"sen","x":"a","f":[[0,1,{"b":true}]]},{"t":"sen","x":"b"}]});
        let (a, binding) = snapshot_with_binding(&records(&v, 10), &Budget::default()).unwrap();
        let b = from_snapshot(&records(&v, 100)).unwrap();
        assert_eq!(a.to_json(), v);
        assert_eq!(a.sid, b.sid);
        assert_ne!(a.nodes[1].eid, b.nodes[1].eid);
        assert_ne!(a.nodes[1].eid, a.nodes[2].eid);
        let mut clean = Tree::from_json(&v).unwrap();
        bind_eids(&mut clean, &binding).unwrap();
        assert_eq!(clean.nodes[2].eid, a.nodes[2].eid);
    }
    #[test]
    fn records_dead_anchors_and_orphans_use_core_visibility() {
        let v = json!({"t":"doc","c":[{"t":"sen","x":"gone"},{"t":"sen","x":"kept"}]});
        let mut recs = records(&v, 10);
        let first = recs
            .iter_mut()
            .find(|(_, n)| matches!(n, NodeIn::ArrElem { .. }))
            .unwrap();
        if let NodeIn::ArrElem { live, .. } = &mut first.1 {
            *live = false;
        }
        let a = from_snapshot(&recs).unwrap();
        assert_eq!(a.to_json(), json!({"t":"doc","c":[{"t":"sen","x":"kept"}]}));
        assert_eq!(
            a.to_json(),
            marekvs_core::json::build_doc(&recs).unwrap().value
        );
        let b = Tree::from_json(&a.to_json()).unwrap();
        assert_eq!(a.sid, b.sid);
    }
}

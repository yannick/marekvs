//! Identity and unique subtree correspondence. Bulk matches are transactional.
use crate::{
    budget::{CancelToken, DiffError},
    matching::{Layer, Matching},
    model::{NodeId, Tree},
};
use std::collections::BTreeMap;
#[derive(Clone, Debug, Default)]
pub struct ExactStats {
    pub compared_nodes: usize,
}
fn index(t: &Tree, exact: bool) -> BTreeMap<(u8, u128), Vec<NodeId>> {
    let mut out = BTreeMap::new();
    for (i, n) in t.nodes.iter().enumerate() {
        out.entry((n.kind as u8, if exact { n.h_exact } else { n.h_content }))
            .or_insert_with(Vec::new)
            .push(i as NodeId);
    }
    out
}
fn eids(t: &Tree) -> BTreeMap<[u8; 10], Option<NodeId>> {
    let mut out = BTreeMap::new();
    for (i, n) in t.nodes.iter().enumerate() {
        if let Some(e) = n.eid {
            out.entry(e)
                .and_modify(|v| *v = None)
                .or_insert(Some(i as NodeId));
        }
    }
    out
}
fn bulk(
    a: &Tree,
    b: &Tree,
    x: NodeId,
    y: NodeId,
    m: &mut Matching,
    layer: Layer,
    cancel: &CancelToken,
) -> Result<bool, DiffError> {
    let mut pending = vec![(x, y)];
    let mut pairs = Vec::new();
    while let Some((x, y)) = pending.pop() {
        cancel.check()?;
        let (n, k) = (a.node(x), b.node(y));
        if n.kind != k.kind
            || n.children.len() != k.children.len()
            || m.a2b(x).is_some_and(|z| z != y)
            || m.b2a(y).is_some_and(|z| z != x)
        {
            return Ok(false);
        }
        pairs.push((x, y));
        pending.extend(n.children.iter().copied().zip(k.children.iter().copied()));
    }
    for (x, y) in pairs {
        m.set(x, y, layer);
        if a.node(x).kind.is_leaf() && a.node(x).h_exact != b.node(y).h_exact {
            m.mark_format(x);
        }
    }
    Ok(true)
}
pub fn run(
    a: &Tree,
    b: &Tree,
    m: &mut Matching,
    cancel: &CancelToken,
) -> Result<ExactStats, DiffError> {
    cancel.check()?;
    let (ea, eb) = (eids(a), eids(b));
    // Roots are semantic document anchors, even when everything beneath them changed.
    if a.node(a.root).kind == b.node(b.root).kind {
        let layer =
            if a.node(a.root).eid.is_some_and(|e| {
                ea.get(&e) == Some(&Some(a.root)) && eb.get(&e) == Some(&Some(b.root))
            }) {
                Layer::I0
            } else {
                Layer::I3
            };
        m.set(a.root, b.root, layer);
    }
    for (e, x) in ea {
        cancel.check()?;
        if let (Some(x), Some(Some(y))) = (x, eb.get(&e)) {
            if a.node(x).kind == b.node(*y).kind {
                m.set(x, *y, Layer::I0);
            }
        }
    }
    let (xa, xb, ca, cb) = (
        index(a, true),
        index(b, true),
        index(a, false),
        index(b, false),
    );
    let mut stats = ExactStats::default();
    let mut stack = vec![a.root];
    while let Some(x) = stack.pop() {
        cancel.check()?;
        stats.compared_nodes += 1;
        let n = a.node(x);
        let mut done = false;
        for (left, right, h, layer) in [
            (&xa, &xb, n.h_exact, Layer::I1Exact),
            (&ca, &cb, n.h_content, Layer::I1Content),
        ] {
            let key = (n.kind as u8, h);
            let y = if let Some(y) = m.a2b(x) {
                let k = b.node(y);
                (if layer == Layer::I1Exact {
                    k.h_exact == h
                } else {
                    k.h_content == h
                })
                .then_some(y)
            } else {
                match (left.get(&key), right.get(&key)) {
                    (Some(xs), Some(ys)) if xs.len() == 1 && ys.len() == 1 => Some(ys[0]),
                    _ => None,
                }
            };
            if let Some(y) = y {
                if bulk(a, b, x, y, m, layer, cancel)? {
                    done = true;
                    break;
                }
            }
        }
        if !done {
            stack.extend(n.children.iter().rev().copied());
        }
    }
    Ok(stats)
}
#[cfg(test)]
mod tests {
    use super::*;
    fn tree(xs: &[&str]) -> Tree {
        Tree::from_json(&serde_json::json!({"t":"doc","c":xs.iter().map(|s|serde_json::json!({"t":"sen","x":s})).collect::<Vec<_>>()})).unwrap()
    }
    #[test]
    fn exact_root_prunes_and_pairs_descendants() {
        let a = tree(&["a", "b"]);
        let mut m = Matching::new(&a, &a);
        let s = run(&a, &a, &mut m, &CancelToken::none()).unwrap();
        assert_eq!(m.matched_count(), 3);
        assert_eq!(s.compared_nodes, 1);
    }
    #[test]
    fn identity_does_not_hide_descendants_and_duplicate_eids_are_ignored() {
        let mut a = tree(&["same", "old"]);
        let mut b = tree(&["same", "new"]);
        a.nodes[0].eid = Some([1; 10]);
        b.nodes[0].eid = Some([1; 10]);
        a.nodes[1].eid = Some([2; 10]);
        a.nodes[2].eid = Some([2; 10]);
        b.nodes[2].eid = Some([2; 10]);
        let mut m = Matching::new(&a, &b);
        run(&a, &b, &mut m, &CancelToken::none()).unwrap();
        assert_eq!(m.a2b(0), Some(0));
        assert_eq!(m.a2b(1), Some(1));
        assert_eq!(m.a2b(2), None);
    }
    #[test]
    fn bulk_pairing_never_overwrites_identity() {
        let mut a = tree(&["repeat", "repeat"]);
        let mut b = a.clone();
        a.nodes[1].eid = Some([1; 10]);
        b.nodes[2].eid = Some([1; 10]);
        let mut m = Matching::new(&a, &b);
        run(&a, &b, &mut m, &CancelToken::none()).unwrap();
        assert_eq!(m.a2b(1), Some(2));
        assert_eq!(m.b2a(2), Some(1));
    }
}

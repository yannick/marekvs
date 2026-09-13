#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn tree(v: serde_json::Value) -> Tree {
        Tree::from_json(&v).unwrap()
    }
    fn sentence(s: &str) -> Tree {
        tree(json!({"t":"doc","c":[{"t":"sen","x":s}]}))
    }
    fn result(a: &Tree, m: &MergeGraph) -> Tree {
        crate::apply(
            a,
            &crate::plan(a, &m.graph, &crate::Accepted::all(&m.graph)).unwrap(),
        )
    }
    #[test]
    fn unchanged_side_and_equal_operations() {
        let a = sentence("one two three");
        let l = sentence("one new three");
        let m = merge3(&a, &l, &a, &Options::default()).unwrap();
        assert!(m.conflicts.is_empty());
        assert_eq!(result(&a, &m).sid, l.sid);
        let both = merge3(&a, &l, &l, &Options::default()).unwrap();
        assert_eq!(both.graph.changes.len(), m.graph.changes.len());
        assert!(both.origins.values().all(|s| *s == Side::Both));
        assert_eq!(result(&a, &both).sid, l.sid);
        assert_ne!(
            m.graph.gid,
            merge3(&a, &a, &l, &Options::default()).unwrap().graph.gid
        );
    }
    #[test]
    fn independent_text_edits_and_overlaps() {
        let a = sentence("one two three four");
        let l = sentence("one new three four");
        let r = sentence("one two three last");
        let m = merge3(&a, &l, &r, &Options::default()).unwrap();
        assert!(m.conflicts.is_empty(), "{:?}", m.conflicts);
        assert_eq!(result(&a, &m).sid, sentence("one new three last").sid);
        let r = sentence("one other three four");
        let m = merge3(&a, &l, &r, &Options::default()).unwrap();
        assert!(!m.conflicts.is_empty());
        assert!(crate::plan(&a, &m.graph, &crate::Accepted::all(&m.graph)).is_err());
    }
    #[test]
    fn same_position_insertions_equal_subtrees_collapse() {
        let a = tree(json!({"t":"doc"}));
        let l = tree(json!({"t":"doc","c":[{"t":"sec","c":[{"t":"sen","x":"inserted"}]}]}));
        let r = tree(
            json!({"t":"doc","a":{"right":true},"c":[{"t":"sec","c":[{"t":"sen","x":"inserted"}]}]}),
        );
        let m = merge3(&a, &l, &r, &Options::default()).unwrap();
        assert_eq!(
            m.graph
                .changes
                .iter()
                .filter(|c| matches!(c.op, Op::Insert { .. }))
                .count(),
            2
        );
        assert_eq!(result(&a, &m).sid, r.sid);
        let r = tree(json!({"t":"doc","c":[{"t":"sen","x":"different"}]}));
        let m = merge3(&a, &l, &r, &Options::default()).unwrap();
        assert!(m.conflicts.is_empty());
        assert_eq!(result(&a, &m).nodes.len(), 4);
    }
    #[test]
    fn disjoint_attributes_remain_independently_acceptable() {
        let a = tree(json!({"t":"doc","a":{"x":0,"y":0}}));
        let l = tree(json!({"t":"doc","a":{"x":1,"y":0}}));
        let r = tree(json!({"t":"doc","a":{"x":0,"y":2}}));
        let m = merge3(&a, &l, &r, &Options::default()).unwrap();
        assert!(m.conflicts.is_empty());
        assert_eq!(m.graph.changes.len(), 2);
        assert_eq!(
            result(&a, &m).to_json(),
            json!({"t":"doc","a":{"x":1,"y":2}})
        );
        for c in &m.graph.changes {
            let p = crate::plan(&a, &m.graph, &[c.id.clone()].into_iter().collect()).unwrap();
            assert!([l.sid, r.sid].contains(&crate::apply(&a, &p).sid));
        }
    }
    #[test]
    fn formatting_projects_through_independent_text_and_conflicts_inside_rewritten_word() {
        let a = sentence("alpha beta gamma");
        let l = tree(
            json!({"t":"doc","c":[{"t":"sen","x":"alpha beta gamma","f":[[11,16,{"b":true}]]}]}),
        );
        let r = sentence("alphabet beta gamma");
        let m = merge3(&a, &l, &r, &Options::default()).unwrap();
        assert!(m.conflicts.is_empty(), "{:?}", m.conflicts);
        assert_eq!(
            result(&a, &m).to_json()["c"][0]["f"],
            json!([[14,19,{"b":true}]])
        );
        let l = tree(
            json!({"t":"doc","c":[{"t":"sen","x":"alpha beta gamma","f":[[1,4,{"b":true}]]}]}),
        );
        let m = merge3(&a, &l, &r, &Options::default()).unwrap();
        assert!(!m.conflicts.is_empty());
    }
    fn identified(v: serde_json::Value) -> Tree {
        let mut t = tree(v);
        for n in &mut t.nodes {
            if let Some(id) = n.attrs.0.get("id").and_then(|v| v.as_u64()) {
                let mut eid = [0; 10];
                eid[0] = id as u8;
                n.eid = Some(eid);
            }
        }
        t
    }
    #[test]
    fn opposing_ancestry_moves_keep_cycle_group() {
        let a =
            identified(json!({"t":"doc","c":[{"t":"sec","a":{"id":1}},{"t":"sec","a":{"id":2}}]}));
        let l = identified(
            json!({"t":"doc","c":[{"t":"sec","a":{"id":2},"c":[{"t":"sec","a":{"id":1}}]}]}),
        );
        let r = identified(
            json!({"t":"doc","c":[{"t":"sec","a":{"id":1},"c":[{"t":"sec","a":{"id":2}}]}]}),
        );
        let m = merge3(&a, &l, &r, &Options::default()).unwrap();
        assert_eq!(m.graph.changes.len(), 2);
        assert_eq!(m.conflicts.len(), 1);
        assert!(crate::plan(&a, &m.graph, &crate::Accepted::all(&m.graph)).is_err());
        for c in &m.graph.changes {
            assert!(crate::plan(&a, &m.graph, &[c.id.clone()].into_iter().collect()).is_ok());
        }
    }
    #[test]
    fn identical_insert_text_in_different_parents_stays_distinct() {
        let a =
            identified(json!({"t":"doc","c":[{"t":"sec","a":{"id":1}},{"t":"sec","a":{"id":2}}]}));
        let l = identified(
            json!({"t":"doc","c":[{"t":"sec","a":{"id":1},"c":[{"t":"sen","x":"new"}]},{"t":"sec","a":{"id":2}}]}),
        );
        let r = identified(
            json!({"t":"doc","c":[{"t":"sec","a":{"id":1}},{"t":"sec","a":{"id":2},"c":[{"t":"sen","x":"new"}]}]}),
        );
        let m = merge3(&a, &l, &r, &Options::default()).unwrap();
        assert!(m.conflicts.is_empty());
        assert_eq!(m.graph.changes.len(), 2);
        let out = result(&a, &m);
        assert_eq!(out.to_json()["c"][0]["c"][0]["x"], "new");
        assert_eq!(out.to_json()["c"][1]["c"][0]["x"], "new");
    }
    #[test]
    fn move_and_edit_remain_separately_selectable() {
        let a = identified(
            json!({"t":"doc","c":[{"t":"sec","a":{"id":1},"c":[{"t":"sen","a":{"id":3},"x":"one two three"}]},{"t":"sec","a":{"id":2}}]}),
        );
        let l = identified(
            json!({"t":"doc","c":[{"t":"sec","a":{"id":1}},{"t":"sec","a":{"id":2},"c":[{"t":"sen","a":{"id":3},"x":"one two three"}]}]}),
        );
        let r = identified(
            json!({"t":"doc","c":[{"t":"sec","a":{"id":1},"c":[{"t":"sen","a":{"id":3},"x":"one new three"}]},{"t":"sec","a":{"id":2}}]}),
        );
        let m = merge3(&a, &l, &r, &Options::default()).unwrap();
        assert!(m.conflicts.is_empty());
        assert_eq!(m.graph.changes.len(), 2);
        assert_eq!(
            result(&a, &m).to_json()["c"][1]["c"][0]["x"],
            "one new three"
        );
        for c in &m.graph.changes {
            let p = crate::plan(&a, &m.graph, &[c.id.clone()].into_iter().collect()).unwrap();
            assert!([l.sid, r.sid].contains(&crate::apply(&a, &p).sid));
        }
    }
    #[test]
    fn partially_equal_edit_lists_deduplicate_shared_change() {
        let a = sentence("one two three four five");
        let l = sentence("one new three four last");
        let r = sentence("one new third four five");
        let m = merge3(&a, &l, &r, &Options::default()).unwrap();
        assert!(m.conflicts.is_empty(), "{:?}", m.conflicts);
        assert_eq!(result(&a, &m).sid, sentence("one new third four last").sid);
    }
    #[test]
    fn competing_zero_width_text_insertions_conflict() {
        let a = sentence("abcd");
        let l = sentence("abXcd");
        let r = sentence("abYcd");
        let o = Options {
            level: crate::Level::Char,
            ..Options::default()
        };
        let m = merge3(&a, &l, &r, &o).unwrap();
        assert_eq!(m.conflicts.len(), 1);
    }
    #[test]
    fn carried_formatting_alternatives_are_reported() {
        let a = sentence("one two three four five");
        let l = tree(
            json!({"t":"doc","c":[{"t":"sen","x":"new two three four five","f":[[8,13,{"link":"left"}]]}]}),
        );
        let r = tree(
            json!({"t":"doc","c":[{"t":"sen","x":"one two three four last","f":[[8,13,{"link":"right"}]]}]}),
        );
        let m = merge3(&a, &l, &r, &Options::default()).unwrap();
        assert_eq!(m.conflicts.len(), 1);
    }

    #[test]
    fn carried_intra_word_boundary_conflicts_with_other_character_rewrite() {
        let a = sentence("alphabet beta gamma");
        let l = tree(
            json!({"t":"doc","c":[{"t":"sen","x":"alphabet beta gamma!","f":[[1,4,{"b":true}]]}]}),
        );
        let r = sentence("alPhabet beta gamma");
        let o = Options {
            level: crate::Level::Char,
            ..Options::default()
        };
        let m = merge3(&a, &l, &r, &o).unwrap();
        assert_eq!(m.conflicts.len(), 1);
    }
}

use crate::{
    graph::{Anchor, ChangeId, Graph, NodeRef, Op},
    textdiff::{tokenize, Edit},
    Attrs, DiffError, Options, Run, Sid, Tree,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    L,
    R,
    Both,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeGraph {
    /// Executable graph against `base`. `graph.to` is a synthetic digest of the
    /// ordered merge inputs and their comparison graphs, not a snapshot Sid.
    /// Storage adapters locate source snapshots using `base`, `left`, and
    /// `right` below; never look up `graph.to` as a snapshot. Applying this
    /// self-contained graph requires only the base snapshot.
    pub graph: Graph,
    pub conflicts: Vec<Vec<ChangeId>>,
    pub origins: BTreeMap<ChangeId, Side>,
    pub base: Sid,
    pub left: Sid,
    pub right: Sid,
}

/// Combine independently selectable suggestions in immutable base coordinates.
pub fn merge3(
    base: &Tree,
    left: &Tree,
    right: &Tree,
    o: &Options,
) -> Result<MergeGraph, DiffError> {
    let lg = crate::diff(base, left, o)?;
    let mut rg = crate::diff(base, right, o)?;
    let context = serde_json::to_vec(&(
        "marekvs-diff/merge3/v2",
        base.sid,
        left.sid,
        right.sid,
        &lg.gid,
        &rg.gid,
    ))
    .expect("merge identity");
    let to = Sid(xxhash_rust::xxh3::xxh3_128(&context));
    let mut mapping = BTreeMap::new();
    let left_nodes: BTreeMap<_, _> = left
        .nodes
        .iter()
        .enumerate()
        .map(|(i, n)| (NodeRef::B(n.lid), i))
        .collect();
    let right_nodes: BTreeMap<_, _> = right
        .nodes
        .iter()
        .enumerate()
        .map(|(i, n)| (NodeRef::B(n.lid), i))
        .collect();
    let mut insertion_index: BTreeMap<_, Vec<NodeRef>> = BTreeMap::new();
    for c in &lg.changes {
        if let Op::Insert {
            node,
            parent,
            after,
            ..
        } = &c.op
        {
            let hash = left.nodes[left_nodes[node]].h_exact;
            insertion_index
                .entry((hash, *parent, anchor(after)))
                .or_default()
                .push(*node);
        }
    }
    let mut used = BTreeSet::new();
    for c in &mut rg.changes {
        o.cancel.check()?;
        remap_op(&mut c.op, &mapping);
        if let Op::Insert {
            node,
            parent,
            after,
            ..
        } = &c.op
        {
            if let Some(&ri) = right_nodes.get(node) {
                let key = (right.nodes[ri].h_exact, *parent, anchor(after));
                if let Some(ln) = insertion_index
                    .get(&key)
                    .and_then(|ns| ns.iter().find(|n| !used.contains(*n)))
                    .copied()
                {
                    used.insert(ln);
                    mapping.insert(*node, ln);
                    remap_op(&mut c.op, &mapping);
                }
            }
        }
    }
    // A later match may resolve an anchor used by an earlier operation.
    for c in &mut rg.changes {
        remap_op(&mut c.op, &mapping);
    }
    let mut graph = Graph {
        v: lg.v,
        gid: lg.gid.clone(),
        from: base.sid,
        to,
        algo: lg.algo,
        options: lg.options.clone(),
        changes: Vec::new(),
        deps: Vec::new(),
        conflicts: Vec::new(),
        explicit_conflicts: Vec::new(),
        stats: lg.stats.clone(),
    };
    let mut origins = BTreeMap::new();
    let mut op_index: BTreeMap<Vec<u8>, usize> = BTreeMap::new();
    let mut operation_bytes = 0usize;
    for (g, side) in [(&lg, Side::L), (&rg, Side::R)] {
        for c in &g.changes {
            o.cancel.check()?;
            let bytes = serde_json::to_vec(&c.op).expect("operation serialization");
            if let Some(&i) = op_index.get(&bytes) {
                origins.insert(graph.changes[i].id.clone(), Side::Both);
                continue;
            }
            if graph.changes.len() >= o.budget.max_changes {
                return Err(DiffError::TooBig {
                    bound: "changes",
                    n: graph.changes.len() + 1,
                    limit: o.budget.max_changes,
                });
            }
            operation_bytes = operation_bytes.saturating_add(bytes.len());
            if operation_bytes > o.budget.max_bytes {
                return Err(DiffError::Budget {
                    limit: "merge operation bytes",
                });
            }
            let mut c = c.clone();
            c.id = crate::graph::change_id(base.sid, to, &graph.options, &c.op);
            origins.insert(c.id.clone(), side);
            op_index.insert(bytes, graph.changes.len());
            graph.changes.push(c);
        }
    }
    let mut by_node: BTreeMap<NodeRef, Vec<usize>> = BTreeMap::new();
    for (i, c) in graph.changes.iter().enumerate() {
        by_node.entry(c.op.node()).or_default().push(i);
    }
    let base_nodes: BTreeMap<_, _> = base.nodes.iter().map(|n| (NodeRef::A(n.lid), n)).collect();
    let mut comparisons = 0usize;
    for indexes in by_node.values() {
        for (at, &i) in indexes.iter().enumerate() {
            for &j in &indexes[at + 1..] {
                comparisons += 1;
                if comparisons > o.budget.max_nd {
                    return Err(DiffError::Budget {
                        limit: "merge comparisons",
                    });
                }
                o.cancel.check()?;
                let (a, b) = (&graph.changes[i], &graph.changes[j]);
                if origins[&a.id] != Side::Both && origins[&a.id] == origins[&b.id] {
                    continue;
                }
                let payload_conflict = if matches!(
                    a.op,
                    Op::Modify { .. } | Op::Format { .. } | Op::Attrs { .. }
                ) && matches!(
                    b.op,
                    Op::Modify { .. } | Op::Format { .. } | Op::Attrs { .. }
                ) {
                    let n = base_nodes[&a.op.node()];
                    let mut shell = crate::graph::Shell {
                        kind: n.kind,
                        attrs: n.attrs.clone(),
                        text: n.text.clone(),
                    };
                    crate::plan::compose_shell(&mut shell, &[a, b]).is_err()
                } else {
                    false
                };
                if incompatible(&a.op, &b.op, base) || payload_conflict {
                    graph
                        .explicit_conflicts
                        .push(vec![a.id.clone(), b.id.clone()]);
                }
            }
        }
    }
    let inserts: BTreeMap<_, _> = graph
        .changes
        .iter()
        .filter(|c| matches!(c.op, Op::Insert { .. }))
        .map(|c| (c.op.node(), c.id.clone()))
        .collect();
    graph.deps = graph
        .changes
        .iter()
        .filter_map(|c| {
            c.op.placement()
                .and_then(|(p, _)| inserts.get(&p))
                .map(|id| (c.id.clone(), id.clone()))
        })
        .collect();
    // Structural validation is authoritative even if unrelated leaf alternatives
    // conflict. Leave conditional deletion/escape groups as UI hints only.
    let explicit = std::mem::take(&mut graph.explicit_conflicts);
    if let Err(error) = crate::plan::plan_bounded(
        base,
        &graph,
        &crate::Accepted::all(&graph),
        &o.budget,
        &o.cancel,
    ) {
        match error {
            crate::PlanError::Budget(e) => return Err(e),
            crate::PlanError::Unresolved { groups } => {
                for mut group in groups {
                    group.sort();
                    group.dedup();
                    if group.len() < 2 {
                        continue;
                    }
                    if group.iter().all(|id| {
                        graph
                            .changes
                            .iter()
                            .any(|c| &c.id == id && c.op.placement().is_some())
                    }) {
                        graph.explicit_conflicts.push(group.clone());
                    }
                    graph.conflicts.push(group);
                }
            }
            crate::PlanError::Conflict { .. } | crate::PlanError::InvalidText { .. } => {}
            e => return Err(DiffError::InvalidGraph(e.to_string())),
        }
    }
    graph.explicit_conflicts.extend(explicit);
    canonical_groups(&mut graph.explicit_conflicts);
    graph.conflicts.extend(graph.explicit_conflicts.clone());
    canonical_groups(&mut graph.conflicts);
    graph.stats.i0 += rg.stats.i0;
    graph.stats.i1 += rg.stats.i1;
    graph.stats.i2 += rg.stats.i2;
    graph.stats.i3 += rg.stats.i3;
    graph.stats.candidates += rg.stats.candidates;
    graph.stats.fallbacks.extend(rg.stats.fallbacks);
    graph.ensure_bounded(o)?;
    graph.rehash();
    graph.ensure_bounded(o)?;
    o.cancel.check()?;
    Ok(MergeGraph {
        conflicts: graph.conflicts.clone(),
        graph,
        origins,
        base: base.sid,
        left: left.sid,
        right: right.sid,
    })
}
fn canonical_groups(groups: &mut Vec<Vec<ChangeId>>) {
    for g in groups.iter_mut() {
        g.sort();
        g.dedup();
    }
    groups.sort();
    groups.dedup();
}
fn anchor(after: &[Anchor]) -> Option<NodeRef> {
    after.first().and_then(|a| match a {
        Anchor::Node(n) => Some(*n),
        Anchor::Start => None,
    })
}
fn remap_op(op: &mut Op, map: &BTreeMap<NodeRef, NodeRef>) {
    let remap = |n: &mut NodeRef| {
        if let Some(r) = map.get(n) {
            *n = *r;
        }
    };
    match op {
        Op::Insert {
            node,
            parent,
            after,
            ..
        }
        | Op::Move {
            node,
            parent,
            after,
        } => {
            remap(node);
            remap(parent);
            for a in after {
                if let Anchor::Node(n) = a {
                    remap(n);
                }
            }
        }
        Op::Delete { node }
        | Op::Modify { node, .. }
        | Op::Format { node, .. }
        | Op::Attrs { node, .. } => remap(node),
    }
}
fn edits_conflict(a: &Edit, b: &Edit) -> bool {
    if a == b {
        return false;
    }
    if a.at == b.at {
        return true;
    }
    if a.del == 0 {
        return a.at > b.at && a.at < b.at.saturating_add(b.del);
    }
    if b.del == 0 {
        return b.at > a.at && b.at < a.at.saturating_add(a.del);
    }
    a.at < b.at.saturating_add(b.del) && b.at < a.at.saturating_add(a.del)
}
fn attrs_conflict(old: &Attrs, a: &Attrs, b: &Attrs) -> bool {
    old.0.keys().chain(a.0.keys()).chain(b.0.keys()).any(|k| {
        a.0.get(k) != old.0.get(k) && b.0.get(k) != old.0.get(k) && a.0.get(k) != b.0.get(k)
    })
}
fn format_text_conflict(runs: &[Run], text: &str, edits: &[Edit]) -> bool {
    tokenize(text).iter().any(|token| {
        runs.iter().any(|r| {
            [r.start, r.end]
                .iter()
                .any(|&p| p > token.start && p < token.end)
        }) && edits.iter().any(|e| {
            if e.del == 0 {
                e.at > token.start && e.at < token.end
            } else {
                e.at < token.end && e.at.saturating_add(e.del) > token.start
            }
        })
    })
}
fn formats_conflict(old: &[Run], a: &[Run], b: &[Run]) -> bool {
    let points: BTreeSet<_> = old
        .iter()
        .chain(a)
        .chain(b)
        .flat_map(|r| [r.start, r.end])
        .collect();
    let at = |runs: &[Run], p: u32| {
        runs.iter()
            .find(|r| r.start <= p && p < r.end)
            .map(|r| r.attrs.clone())
            .unwrap_or_default()
    };
    points
        .into_iter()
        .any(|p| attrs_conflict(&at(old, p), &at(a, p), &at(b, p)))
}
// Undo this side's text-coordinate shift before comparing its formatting
// boundaries with another side's edits in the immutable base.
fn base_boundary(point: u32, own: &[Edit]) -> Option<u32> {
    let mut shift = 0i64;
    for e in own {
        let start = i64::from(e.at) + shift;
        let end = start + e.ins.chars().count() as i64;
        let p = i64::from(point);
        if p < start {
            break;
        }
        if p == start {
            return Some(e.at);
        }
        if p < end {
            return None;
        }
        shift += e.ins.chars().count() as i64 - i64::from(e.del);
    }
    u32::try_from(i64::from(point) - shift).ok()
}
fn carried_format_conflict(op: &Op, other: &[Edit]) -> bool {
    let Op::Modify {
        old, new, edits, ..
    } = op
    else {
        return false;
    };
    let mapped: Vec<_> = new
        .f
        .iter()
        .filter_map(|r| {
            Some(Run {
                start: base_boundary(r.start, edits)?,
                end: base_boundary(r.end, edits)?,
                attrs: r.attrs.clone(),
            })
        })
        .collect();
    let changed: Vec<_> = mapped
        .iter()
        .chain(&old.f)
        .filter(|r| !mapped.contains(r) || !old.f.contains(r))
        .cloned()
        .collect();
    format_text_conflict(&changed, &old.x, other)
}
fn incompatible(a: &Op, b: &Op, base: &Tree) -> bool {
    match (a, b) {
        (Op::Move { .. }, Op::Move { .. }) => true,
        (Op::Attrs { old, new: a, .. }, Op::Attrs { new: b, .. }) => attrs_conflict(old, a, b),
        (Op::Modify { edits: ae, .. }, Op::Modify { edits: be, .. }) => {
            ae.iter().any(|x| be.iter().any(|y| edits_conflict(x, y)))
                || carried_format_conflict(a, be)
                || carried_format_conflict(b, ae)
        }
        (
            Op::Format { new, old, .. },
            Op::Modify {
                edits, old: text, ..
            },
        )
        | (
            Op::Modify {
                edits, old: text, ..
            },
            Op::Format { new, old, .. },
        ) => {
            let changed: Vec<_> = new
                .iter()
                .chain(old)
                .filter(|r| !new.contains(r) || !old.contains(r))
                .cloned()
                .collect();
            format_text_conflict(&changed, &text.x, edits)
        }
        (Op::Format { old, new: a, .. }, Op::Format { new: b, .. }) => formats_conflict(old, a, b),
        (Op::Delete { .. }, Op::Delete { .. }) => false,
        // Deletion compatibility depends on the final accepted ancestry.
        (Op::Delete { .. }, _) | (_, Op::Delete { .. }) => false,
        _ => {
            let _ = base;
            false
        }
    }
}

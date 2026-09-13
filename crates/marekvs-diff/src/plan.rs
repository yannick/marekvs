//! Authoritative selection validation and deterministic materialization.
use crate::{graph::*, model::*, textdiff::apply_edits};
use std::collections::{BTreeMap, BTreeSet};
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Accepted(pub BTreeSet<ChangeId>);
impl Accepted {
    pub fn all(g: &Graph) -> Self {
        Self(g.changes.iter().map(|c| c.id.clone()).collect())
    }
    pub fn none() -> Self {
        Self::default()
    }
    pub fn contains(&self, id: &ChangeId) -> bool {
        self.0.contains(id)
    }
}
impl FromIterator<ChangeId> for Accepted {
    fn from_iter<T: IntoIterator<Item = ChangeId>>(i: T) -> Self {
        Self(i.into_iter().collect())
    }
}
#[derive(Clone, Debug)]
pub struct Plan {
    base: Sid,
    result: Tree,
    pub mapping: BTreeMap<NodeRef, u32>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlanError {
    Budget(crate::DiffError),
    BaseMismatch {
        expected: Sid,
        actual: Sid,
    },
    UnknownChange(ChangeId),
    InvalidReference(NodeRef),
    InvalidGraph(String),
    NotClosed {
        change: ChangeId,
        requires: ChangeId,
    },
    Conflict {
        group: Vec<ChangeId>,
    },
    Structural {
        changes: Vec<ChangeId>,
        why: String,
    },
    Unresolved {
        groups: Vec<Vec<ChangeId>>,
    },
    InvalidText {
        changes: Vec<ChangeId>,
        why: String,
    },
}
impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for PlanError {}
impl PlanError {
    pub fn unresolved(&self) -> Vec<Vec<ChangeId>> {
        match self {
            Self::Unresolved { groups } => groups.clone(),
            Self::Conflict { group } => vec![group.clone()],
            Self::Structural { changes, .. } | Self::InvalidText { changes, .. } => {
                vec![changes.clone()]
            }
            Self::NotClosed { change, requires } => vec![vec![change.clone(), requires.clone()]],
            Self::UnknownChange(id) => vec![vec![id.clone()]],
            _ => vec![],
        }
    }
}
#[derive(Clone)]
struct Work {
    shell: Shell,
    parent: Option<NodeRef>,
    children: Vec<NodeRef>,
}
pub fn plan(a: &Tree, g: &Graph, accepted: &Accepted) -> Result<Plan, PlanError> {
    plan_bounded(
        a,
        g,
        accepted,
        &crate::Budget::default(),
        &crate::CancelToken::none(),
    )
}
pub fn plan_bounded(
    a: &Tree,
    g: &Graph,
    accepted: &Accepted,
    budget: &crate::Budget,
    cancel: &crate::CancelToken,
) -> Result<Plan, PlanError> {
    cancel.check().map_err(PlanError::Budget)?;
    if a.nodes.len() > budget.max_nodes || g.changes.len() > budget.max_changes {
        return Err(PlanError::Budget(crate::DiffError::Budget {
            limit: "planner size",
        }));
    }
    if g.v != 2 || g.algo != crate::ALGO_VERSION {
        return Err(PlanError::InvalidGraph("unsupported graph version".into()));
    }
    if a.sid != g.from {
        return Err(PlanError::BaseMismatch {
            expected: g.from,
            actual: a.sid,
        });
    }
    let root = NodeRef::A(a.nodes[a.root as usize].lid);
    let mut nodes: BTreeMap<NodeRef, Work> = a
        .nodes
        .iter()
        .map(|n| {
            (
                NodeRef::A(n.lid),
                Work {
                    shell: Shell {
                        kind: n.kind,
                        attrs: n.attrs.clone(),
                        text: n.text.clone(),
                    },
                    parent: n.parent.map(|p| NodeRef::A(a.nodes[p as usize].lid)),
                    children: n
                        .children
                        .iter()
                        .map(|&c| NodeRef::A(a.nodes[c as usize].lid))
                        .collect(),
                },
            )
        })
        .collect();
    let mut ids = BTreeSet::new();
    let mut inserts = BTreeMap::new();
    for c in &g.changes {
        if !ids.insert(c.id.clone()) {
            return Err(PlanError::InvalidGraph("duplicate change id".into()));
        }
        if let Op::Insert {
            node,
            parent,
            shell,
            ..
        } = &c.op
        {
            if !matches!(node, NodeRef::B(_))
                || nodes.contains_key(node)
                || inserts.insert(*node, c.id.clone()).is_some()
            {
                return Err(PlanError::InvalidGraph(
                    "duplicate or invalid insert identity".into(),
                ));
            }
            nodes.insert(
                *node,
                Work {
                    shell: shell.clone(),
                    parent: Some(*parent),
                    children: vec![],
                },
            );
        }
    }
    for id in &accepted.0 {
        if !ids.contains(id) {
            return Err(PlanError::UnknownChange(id.clone()));
        }
    }
    // Validate every graph reference, including rejected changes; hint arrays cannot authorize anything.
    let mut anchor_work = 0usize;
    for c in &g.changes {
        cancel.check().map_err(PlanError::Budget)?;
        let nr = c.op.node();
        if !nodes.contains_key(&nr) {
            return Err(PlanError::InvalidReference(nr));
        }
        validate_payload(&c.op, &nodes[&nr].shell)?;
        if !matches!(c.op, Op::Insert { .. }) && !matches!(nr, NodeRef::A(_)) {
            return Err(PlanError::InvalidReference(nr));
        }
        if let Some((p, after)) = c.op.placement() {
            anchor_work = anchor_work.saturating_add(after.len());
            if anchor_work > budget.max_nd {
                return Err(PlanError::Budget(crate::DiffError::TooBig {
                    bound: "anchors",
                    n: anchor_work,
                    limit: budget.max_nd,
                }));
            }
            if !nodes.contains_key(&p) {
                return Err(PlanError::InvalidReference(p));
            }
            if nodes[&p].shell.kind.is_leaf() {
                return Err(PlanError::InvalidGraph("leaf destination".into()));
            }
            if after.last() != Some(&Anchor::Start) {
                return Err(PlanError::InvalidGraph(
                    "anchor chain must end at start".into(),
                ));
            }
            for an in after {
                if let Anchor::Node(r) = an {
                    if !nodes.contains_key(r) {
                        return Err(PlanError::InvalidReference(*r));
                    }
                }
            }
        }
        if nr == root
            && matches!(
                c.op,
                Op::Delete { .. } | Op::Move { .. } | Op::Insert { .. }
            )
        {
            return Err(PlanError::InvalidGraph("root structural operation".into()));
        }
    }
    let chosen: Vec<_> = g
        .changes
        .iter()
        .filter(|c| accepted.contains(&c.id))
        .collect();
    let mut groups = Vec::new();
    let mut structural: BTreeMap<NodeRef, Vec<ChangeId>> = BTreeMap::new();
    let mut deleted = BTreeMap::new();
    for group in &g.explicit_conflicts {
        if group.iter().collect::<BTreeSet<_>>().len() < 2 {
            return Err(PlanError::InvalidGraph(
                "conflict group has fewer than two changes".into(),
            ));
        }
        for id in group {
            if !ids.contains(id) {
                return Err(PlanError::UnknownChange(id.clone()));
            }
        }
        if group.iter().all(|id| accepted.contains(id)) {
            groups.push(group.clone());
        }
    }

    for c in &chosen {
        if let Some((p, _)) = c.op.placement() {
            if let Some(req) = inserts.get(&p) {
                if !accepted.contains(req) {
                    groups.push(vec![c.id.clone(), req.clone()]);
                }
            }
            structural
                .entry(c.op.node())
                .or_default()
                .push(c.id.clone());
            nodes.get_mut(&c.op.node()).unwrap().parent = Some(p);
        }
        if let Op::Delete { node } = c.op {
            deleted.insert(node, c.id.clone());
        }
    }
    for group in structural.values() {
        if group.len() > 1 {
            groups.push(group.clone());
        }
    }
    let mut live = BTreeSet::new();
    for &nr in nodes.keys() {
        cancel.check().map_err(PlanError::Budget)?;
        if inserts.get(&nr).is_some_and(|id| !accepted.contains(id)) {
            continue;
        }
        let mut at = Some(nr);
        let mut path = BTreeSet::new();
        let mut dead = false;
        let mut why = Vec::new();
        while let Some(n) = at {
            if !path.insert(n) {
                for p in &path {
                    if let Some(cs) = structural.get(p) {
                        why.extend(cs.clone());
                    }
                }
                groups.push(why);
                dead = true;
                break;
            }
            if deleted.contains_key(&n) || inserts.get(&n).is_some_and(|id| !accepted.contains(id))
            {
                dead = true;
                break;
            }
            if path.len() > budget.max_depth.saturating_add(1) {
                return Err(PlanError::Structural {
                    changes: chosen.iter().map(|c| c.id.clone()).collect(),
                    why: "result exceeds maximum depth".into(),
                });
            }
            at = nodes[&n].parent;
        }
        if !dead {
            live.insert(nr);
        }
    }
    for c in &chosen {
        match &c.op {
            Op::Delete { .. } => {}
            _ if !live.contains(&c.op.node()) => {
                let mut group = vec![c.id.clone()];
                let mut at = Some(c.op.node());
                let mut seen = BTreeSet::new();
                while let Some(n) = at {
                    if !seen.insert(n) {
                        break;
                    }
                    if let Some(id) = deleted.get(&n) {
                        group.push(id.clone());
                    }
                    if let Some(id) = inserts.get(&n) {
                        if !accepted.contains(id) {
                            group.push(id.clone());
                        }
                    }
                    at = nodes[&n].parent;
                }
                groups.push(group);
            }
            _ => {}
        }
    }
    if !groups.is_empty() {
        for g in &mut groups {
            g.sort();
            g.dedup();
        }
        groups.sort();
        groups.dedup();
        return Err(PlanError::Unresolved { groups });
    }
    let moved: BTreeSet<_> = chosen
        .iter()
        .filter(|c| c.op.placement().is_some())
        .map(|c| c.op.node())
        .collect();
    for n in nodes.values_mut() {
        n.children
            .retain(|r| live.contains(r) && !moved.contains(r));
    }
    let mut tails: BTreeMap<(NodeRef, Option<NodeRef>), NodeRef> = BTreeMap::new();
    // The graph's stable B-order sequence is the priority, including when predecessors are rejected.
    for c in &chosen {
        if let Some((p, after)) = c.op.placement() {
            let nr = c.op.node();
            let list = &mut nodes.get_mut(&p).unwrap().children;
            let mut anchor = None;
            for x in after {
                match x {
                    Anchor::Node(r) if list.contains(r) => {
                        anchor = Some(*r);
                        break;
                    }
                    Anchor::Start => break,
                    _ => {}
                }
            }
            let key = (p, anchor);
            let effective = tails
                .get(&key)
                .copied()
                .filter(|r| list.contains(r))
                .or(anchor);
            let idx = effective
                .and_then(|r| list.iter().position(|x| *x == r))
                .map_or(0, |i| i + 1);
            list.insert(idx, nr);
            tails.insert(key, nr);
        }
    }
    let mut by_node: BTreeMap<NodeRef, Vec<&Change>> = BTreeMap::new();
    for &c in &chosen {
        if matches!(
            c.op,
            Op::Modify { .. } | Op::Format { .. } | Op::Attrs { .. }
        ) {
            by_node.entry(c.op.node()).or_default().push(c);
        }
    }
    for (nr, cs) in by_node {
        compose_shell(&mut nodes.get_mut(&nr).unwrap().shell, &cs)?;
    }

    fn emit(
        n: NodeRef,
        nodes: &BTreeMap<NodeRef, Work>,
        mapping: &mut BTreeMap<NodeRef, u32>,
    ) -> serde_json::Value {
        mapping.insert(n, mapping.len() as u32);
        let w = &nodes[&n];
        let mut v = serde_json::Map::new();
        v.insert("t".into(), serde_json::to_value(w.shell.kind).unwrap());
        if !w.shell.attrs.0.is_empty() {
            v.insert("a".into(), serde_json::to_value(&w.shell.attrs).unwrap());
        }
        if let Some(t) = &w.shell.text {
            v.insert("x".into(), t.x.clone().into());
            if !t.f.is_empty() {
                v.insert(
                    "f".into(),
                    serde_json::Value::Array(
                        t.f.iter()
                            .map(|r| serde_json::json!([r.start, r.end, r.attrs]))
                            .collect(),
                    ),
                );
            }
        } else {
            v.insert(
                "c".into(),
                serde_json::Value::Array(
                    w.children
                        .iter()
                        .map(|&c| emit(c, nodes, mapping))
                        .collect(),
                ),
            );
        }
        v.into()
    }
    let mut mapping = BTreeMap::new();
    let json = emit(root, &nodes, &mut mapping);
    let result = Tree::from_json_with_budget(&json, budget)
        .map_err(|e| PlanError::InvalidGraph(e.to_string()))?;
    Ok(Plan {
        base: a.sid,
        result,
        mapping,
    })
}
pub fn apply(a: &Tree, p: &Plan) -> Tree {
    assert_eq!(a.sid, p.base, "plan applied to different base");
    p.result.clone()
}

fn validate_runs(runs: &[Run], text: &str) -> Result<(), PlanError> {
    let len = u32::try_from(text.chars().count())
        .map_err(|_| PlanError::InvalidGraph("format offset overflow".into()))?;
    for (i, r) in runs.iter().enumerate() {
        let invalid_attrs = r.attrs.0.is_empty()
            || r.attrs.0.iter().any(|(key, value)| match key.as_str() {
                "b" | "i" | "u" | "s" | "code" => !value.is_boolean(),
                "link" => !value.is_string(),
                _ => true,
            });
        if r.start >= r.end
            || r.end > len
            || invalid_attrs
            || i > 0
                && (runs[i - 1].end > r.start
                    || runs[i - 1].end == r.start && runs[i - 1].attrs == r.attrs)
        {
            return Err(PlanError::InvalidGraph(
                "noncanonical formatting payload".into(),
            ));
        }
    }
    Ok(())
}
fn validate_payload(op: &Op, base: &Shell) -> Result<(), PlanError> {
    let attrs = |a: &Attrs| {
        if a.0.values().any(|v| v.is_object() || v.is_array()) {
            Err(PlanError::InvalidGraph(
                "non-scalar attribute payload".into(),
            ))
        } else {
            Ok(())
        }
    };
    match op {
        Op::Insert { shell, .. } => {
            attrs(&shell.attrs)?;
            if shell.kind.is_leaf() != shell.text.is_some() {
                return Err(PlanError::InvalidGraph(
                    "insert shell leaf/container mismatch".into(),
                ));
            }
            if let Some(t) = &shell.text {
                validate_runs(&t.f, &t.x)?;
            }
        }
        Op::Modify { old, new, .. } => {
            if !base.kind.is_leaf() {
                return Err(PlanError::InvalidGraph("modify on container".into()));
            }
            validate_runs(&old.f, &old.x)?;
            validate_runs(&new.f, &new.x)?;
        }
        Op::Format { old, new, .. } => {
            let text = base
                .text
                .as_ref()
                .ok_or_else(|| PlanError::InvalidGraph("format on container".into()))?;
            validate_runs(old, &text.x)?;
            validate_runs(new, &text.x)?;
        }
        Op::Attrs { old, new, .. } => {
            attrs(old)?;
            attrs(new)?;
        }
        _ => {}
    }
    Ok(())
}

/// Shared leaf composition used by merge conflict discovery and final planning.
pub(crate) fn compose_shell(w: &mut Shell, cs: &[&Change]) -> Result<(), PlanError> {
    let cids: Vec<_> = cs.iter().map(|c| c.id.clone()).collect();
    for c in cs {
        validate_payload(&c.op, w)?;
    }
    let original = w.text.clone();
    let mut edits = Vec::new();
    let mut fmt_sources: Vec<(Vec<Run>, Vec<crate::textdiff::Edit>, bool)> = Vec::new();
    let mut attr_changes: BTreeMap<String, Option<serde_json::Value>> = BTreeMap::new();
    for c in cs {
        match &c.op {
            Op::Modify {
                edits: es,
                old,
                new,
                ..
            } => {
                if original.as_ref() != Some(old)
                    || apply_edits(&old.x, es).ok().as_deref() != Some(&new.x)
                {
                    return Err(PlanError::InvalidText {
                        changes: cids,
                        why: "modify payload does not reproduce target from base".into(),
                    });
                }
                edits.extend(es.clone());
                let changed = project_runs(&old.f, es).as_ref() != Ok(&new.f);
                fmt_sources.push((new.f.clone(), es.clone(), changed));
            }
            Op::Format { old, new, .. } => {
                if original.as_ref().map(|t| &t.f) != Some(old) {
                    return Err(PlanError::InvalidText {
                        changes: cids,
                        why: "format base mismatch".into(),
                    });
                }
                fmt_sources.push((new.clone(), vec![], true));
            }
            Op::Attrs { old, new, .. } => {
                if &w.attrs != old {
                    return Err(PlanError::InvalidGraph("attribute base mismatch".into()));
                }
                let keys: BTreeSet<_> = old.0.keys().chain(new.0.keys()).collect();
                for key in keys {
                    if old.0.get(key) != new.0.get(key) {
                        let value = new.0.get(key).cloned();
                        if attr_changes.get(key).is_some_and(|v| v != &value) {
                            return Err(PlanError::Conflict { group: cids });
                        }
                        attr_changes.insert(key.clone(), value);
                    }
                }
            }
            _ => {}
        }
    }
    edits.sort_by_key(|e| (e.at, e.del, e.ins.clone()));
    edits.dedup();
    if !edits.is_empty() {
        let t = w.text.as_mut().ok_or_else(|| PlanError::InvalidText {
            changes: cids.clone(),
            why: "modify on container".into(),
        })?;
        t.x = apply_edits(&t.x, &edits).map_err(|e| PlanError::InvalidText {
            changes: cids.clone(),
            why: e.to_string(),
        })?;
    }
    if !fmt_sources.is_empty() {
        let base = original.as_ref().ok_or_else(|| PlanError::InvalidText {
            changes: cids.clone(),
            why: "format on container".into(),
        })?;
        let mut proposals = Vec::new();
        for (runs, own, changed) in fmt_sources {
            if !changed {
                continue;
            }
            let mut external = Vec::new();
            for e in &edits {
                if own.contains(e) {
                    continue;
                }
                let at = project_point(e.at, true, &own).map_err(|why| PlanError::InvalidText {
                    changes: cids.clone(),
                    why: why.into(),
                })?;
                let end = project_point(e.at + e.del, false, &own).map_err(|why| {
                    PlanError::InvalidText {
                        changes: cids.clone(),
                        why: why.into(),
                    }
                })?;
                external.push(crate::textdiff::Edit {
                    at,
                    del: end.saturating_sub(at),
                    ins: e.ins.clone(),
                });
            }
            proposals.push(project_runs(&runs, &external).map_err(|why| {
                PlanError::InvalidText {
                    changes: cids.clone(),
                    why: why.into(),
                }
            })?);
        }
        let baseline = project_runs(&base.f, &edits).unwrap_or_default();
        let runs = merge_run_proposals(&baseline, &proposals).map_err(|_| PlanError::Conflict {
            group: cids.clone(),
        })?;
        w.text.as_mut().unwrap().f = runs;
    }
    for (key, value) in attr_changes {
        if let Some(value) = value {
            w.attrs.0.insert(key, value);
        } else {
            w.attrs.0.remove(&key);
        }
    }

    Ok(())
}

/// Project a code-point boundary through simultaneous base-relative edits.
fn project_point(
    p: u32,
    start: bool,
    edits: &[crate::textdiff::Edit],
) -> Result<u32, &'static str> {
    let mut delta = 0i64;
    for e in edits {
        let end = e.at.checked_add(e.del).ok_or("offset overflow")?;
        if p < e.at {
            break;
        }
        if e.del == 0 && p == e.at {
            if start {
                delta += e.ins.chars().count() as i64;
            }
            continue;
        }
        if p == e.at {
            break;
        }
        if p < end {
            return Err("format boundary rewritten by text edit");
        }
        delta += e.ins.chars().count() as i64 - i64::from(e.del);
    }
    u32::try_from(i64::from(p) + delta).map_err(|_| "offset overflow")
}
fn project_runs(runs: &[Run], edits: &[crate::textdiff::Edit]) -> Result<Vec<Run>, &'static str> {
    let mut out: Vec<Run> = Vec::new();
    for r in runs {
        let start = project_point(r.start, true, edits)?;
        let end = project_point(r.end, false, edits)?;
        if end <= start {
            continue;
        }
        if let Some(last) = out.last_mut() {
            if last.end == start && last.attrs == r.attrs {
                last.end = end;
                continue;
            }
        }
        out.push(Run {
            start,
            end,
            attrs: r.attrs.clone(),
        });
    }
    Ok(out)
}
fn merge_run_proposals(base: &[Run], proposals: &[Vec<Run>]) -> Result<Vec<Run>, ()> {
    let mut points = BTreeSet::new();
    for r in base.iter().chain(proposals.iter().flatten()) {
        points.insert(r.start);
        points.insert(r.end);
    }
    let points: Vec<_> = points.into_iter().collect();
    let empty = Attrs::default();
    let at = |runs: &[Run], p: u32| {
        runs.get(runs.partition_point(|r| r.end <= p))
            .filter(|r| r.start <= p)
            .map(|r| r.attrs.clone())
            .unwrap_or_default()
    };
    let mut out: Vec<Run> = Vec::new();
    for interval in points.windows(2) {
        let start = interval[0];
        let end = interval[1];
        let original = at(base, start);
        let mut delta: BTreeMap<String, Option<serde_json::Value>> = BTreeMap::new();
        for runs in proposals {
            let attrs = at(runs, start);
            for key in original.0.keys().chain(attrs.0.keys()) {
                if original.0.get(key) != attrs.0.get(key) {
                    let v = attrs.0.get(key).cloned();
                    if delta.get(key).is_some_and(|prev| prev != &v) {
                        return Err(());
                    }
                    delta.insert(key.clone(), v);
                }
            }
        }
        let mut attrs = original;
        for (k, v) in delta {
            if let Some(v) = v {
                attrs.0.insert(k, v);
            } else {
                attrs.0.remove(&k);
            }
        }
        if attrs == empty {
            continue;
        }
        if let Some(last) = out.last_mut() {
            if last.end == start && last.attrs == attrs {
                last.end = end;
                continue;
            }
        }
        out.push(Run { start, end, attrs });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn tree(v: serde_json::Value) -> Tree {
        Tree::from_json(&v).unwrap()
    }
    fn base() -> Tree {
        tree(json!({"t":"doc","c":[{"t":"sec","c":[{"t":"sen","x":"one"}]},{"t":"sec","c":[]}]}))
    }
    fn graph(a: &Tree, ops: Vec<Op>) -> Graph {
        Graph {
            v: 2,
            gid: Gid("test".into()),
            from: a.sid,
            to: a.sid,
            algo: 2,
            options: String::new(),
            changes: ops
                .into_iter()
                .enumerate()
                .map(|(i, op)| Change {
                    id: ChangeId(i.to_string()),
                    op,
                })
                .collect(),
            deps: vec![],
            conflicts: vec![],
            explicit_conflicts: vec![],
            stats: Stats::default(),
        }
    }
    fn nr(a: &Tree, i: usize) -> NodeRef {
        NodeRef::A(a.nodes[i].lid)
    }
    #[test]
    fn descendant_escape_makes_edit_independent() {
        let a = base();
        let old = a.nodes[2].text.clone().unwrap();
        let new = Text {
            x: "two".into(),
            f: vec![],
        };
        let g = graph(
            &a,
            vec![
                Op::Delete { node: nr(&a, 1) },
                Op::Move {
                    node: nr(&a, 2),
                    parent: nr(&a, 3),
                    after: vec![Anchor::Start],
                },
                Op::Modify {
                    node: nr(&a, 2),
                    edits: vec![crate::textdiff::Edit {
                        at: 0,
                        del: 3,
                        ins: "two".into(),
                    }],
                    old,
                    new,
                },
            ],
        );
        assert!(plan(&a, &g, &Accepted::all(&g)).is_ok());
        assert!(plan(
            &a,
            &g,
            &Accepted([g.changes[0].id.clone(), g.changes[2].id.clone()].into())
        )
        .is_err());
        let p = plan(&a, &g, &Accepted([g.changes[2].id.clone()].into())).unwrap();
        assert_eq!(apply(&a, &p).to_json()["c"][0]["c"][0]["x"], "two");
    }
    #[test]
    fn inserted_shell_dependency_rederived() {
        let a = base();
        let p = NodeRef::B(Lid(99));
        let g = graph(
            &a,
            vec![
                Op::Insert {
                    node: p,
                    parent: nr(&a, 0),
                    after: vec![Anchor::Start],
                    shell: Shell {
                        kind: Kind::Sec,
                        attrs: Attrs::default(),
                        text: None,
                    },
                },
                Op::Move {
                    node: nr(&a, 2),
                    parent: p,
                    after: vec![Anchor::Start],
                },
            ],
        );
        assert!(plan(&a, &g, &Accepted::all(&g)).is_ok());
        assert!(plan(&a, &g, &Accepted([g.changes[1].id.clone()].into())).is_err());
    }
    #[test]
    fn fallback_does_not_reverse_siblings() {
        let a = base();
        let ops = ["a", "b", "c"]
            .iter()
            .enumerate()
            .map(|(i, x)| Op::Insert {
                node: NodeRef::B(Lid(i as u128)),
                parent: nr(&a, 3),
                after: vec![Anchor::Start],
                shell: Shell {
                    kind: Kind::Sen,
                    attrs: Attrs::default(),
                    text: Some(Text {
                        x: x.to_string(),
                        f: vec![],
                    }),
                },
            })
            .collect();
        let g = graph(&a, ops);
        let result = apply(&a, &plan(&a, &g, &Accepted::all(&g)).unwrap());
        assert_eq!(
            result.to_json()["c"][1]["c"],
            json!([{"t":"sen","x":"a"},{"t":"sen","x":"b"},{"t":"sen","x":"c"}])
        );
    }
    #[test]
    fn opposing_moves_are_unresolved() {
        let a = base();
        let g = graph(
            &a,
            vec![
                Op::Move {
                    node: nr(&a, 1),
                    parent: nr(&a, 3),
                    after: vec![Anchor::Start],
                },
                Op::Move {
                    node: nr(&a, 3),
                    parent: nr(&a, 1),
                    after: vec![Anchor::Start],
                },
            ],
        );
        assert!(matches!(
            plan(&a, &g, &Accepted::all(&g)),
            Err(PlanError::Unresolved { .. })
        ));
    }
    #[test]
    fn rejects_wrong_base_and_unknown_refs() {
        let a = base();
        let mut g = graph(
            &a,
            vec![Op::Delete {
                node: NodeRef::A(Lid(99)),
            }],
        );
        assert!(matches!(
            plan(&a, &g, &Accepted::none()),
            Err(PlanError::InvalidReference(_))
        ));
        g.from = Sid(0);
        assert!(matches!(
            plan(&a, &g, &Accepted::none()),
            Err(PlanError::BaseMismatch { .. })
        ));
    }
}
#[cfg(test)]
mod composition_tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn formatting_projects_and_combines_by_attribute() {
        let edits = vec![crate::textdiff::Edit {
            at: 0,
            del: 5,
            ins: "alphabet".into(),
        }];
        let bold = Run {
            start: 11,
            end: 16,
            attrs: Attrs::from_pairs(&[("b", json!(true))]),
        };
        let projected = project_runs(&[bold], &edits).unwrap();
        assert_eq!(projected[0].start, 14);
        assert_eq!(projected[0].end, 19);
        let italic = Run {
            start: 14,
            end: 19,
            attrs: Attrs::from_pairs(&[("i", json!(true))]),
        };
        let combined = merge_run_proposals(&[], &[projected, vec![italic]]).unwrap();
        assert_eq!(combined[0].attrs.0.len(), 2);
    }
    #[test]
    fn competing_format_values_are_conflicts() {
        let run = |v| Run {
            start: 0,
            end: 4,
            attrs: Attrs::from_pairs(&[("color", json!(v))]),
        };
        assert!(merge_run_proposals(&[], &[vec![run("red")], vec![run("blue")]]).is_err());
    }
    #[test]
    fn version_and_cancellation_are_checked() {
        let a = Tree::from_json(&json!({"t":"doc","c":[]})).unwrap();
        let mut g = crate::diff(&a, &a, &crate::Options::default()).unwrap();
        g.algo = 999;
        assert!(matches!(
            plan(&a, &g, &Accepted::none()),
            Err(PlanError::InvalidGraph(_))
        ));
        let cancel = crate::CancelToken::none();
        cancel.cancel();
        assert!(matches!(
            plan_bounded(
                &a,
                &g,
                &Accepted::none(),
                &crate::Budget::default(),
                &cancel
            ),
            Err(PlanError::Budget(crate::DiffError::Cancelled))
        ));
    }
}

#[cfg(test)]
mod payload_tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn malformed_runs_are_rejected_before_composition_normalizes_them() {
        let bold = Attrs::from_pairs(&[("b", json!(true))]);
        let run = |start, end| Run {
            start,
            end,
            attrs: bold.clone(),
        };
        let variants = vec![
            vec![run(0, 2), run(1, 3)],
            vec![run(2, 3), run(0, 1)],
            vec![run(0, 1), run(1, 2)],
            vec![run(0, 4)],
            vec![run(1, 1)],
            vec![Run {
                start: 0,
                end: 1,
                attrs: Attrs::default(),
            }],
            vec![Run {
                start: 0,
                end: 1,
                attrs: Attrs::from_pairs(&[("b", json!(1))]),
            }],
        ];
        for new in variants {
            let mut shell = Shell {
                kind: Kind::Sen,
                attrs: Attrs::default(),
                text: Some(Text {
                    x: "abc".into(),
                    f: vec![],
                }),
            };
            let change = Change {
                id: ChangeId("test".into()),
                op: Op::Format {
                    node: NodeRef::A(Lid(1)),
                    old: vec![],
                    new,
                },
            };
            assert!(matches!(
                compose_shell(&mut shell, &[&change]),
                Err(PlanError::InvalidGraph(_))
            ));
        }
    }
    #[test]
    fn rejected_malformed_format_is_still_an_invalid_graph() {
        let a = Tree::from_json(&json!({"t":"doc","c":[{"t":"sen","x":"abc"}]})).unwrap();
        let mut g = crate::diff(&a, &a, &crate::Options::default()).unwrap();
        g.changes.push(Change {
            id: ChangeId("test".into()),
            op: Op::Format {
                node: NodeRef::A(a.nodes[1].lid),
                old: vec![],
                new: vec![Run {
                    start: 0,
                    end: 1,
                    attrs: Attrs::default(),
                }],
            },
        });
        assert!(matches!(
            plan(&a, &g, &Accepted::none()),
            Err(PlanError::InvalidGraph(_))
        ));
    }
}

//! Stable graph wire representation and semantic identities.
use crate::{
    classify::{self, Raw},
    matching::Matching,
    model::*,
    textdiff::{diff_text_bounded, Edit},
    DiffError, Options, ALGO_VERSION,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ChangeId(pub String);
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Gid(pub String);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum NodeRef {
    A(Lid),
    B(Lid),
}
pub type Ref = NodeRef;
impl Serialize for NodeRef {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let (p, l) = match self {
            Self::A(l) => ("a", l),
            Self::B(l) => ("b", l),
        };
        s.serialize_str(&format!("{p}:{:032x}", l.0))
    }
}
impl<'de> Deserialize<'de> for NodeRef {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        if s.len() != 34
            || !s.is_ascii()
            || !s[2..]
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(serde::de::Error::custom("invalid node reference"));
        }
        let n = u128::from_str_radix(&s[2..], 16).map_err(serde::de::Error::custom)?;
        match &s[..2] {
            "a:" => Ok(Self::A(Lid(n))),
            "b:" => Ok(Self::B(Lid(n))),
            _ => Err(serde::de::Error::custom("invalid node reference")),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Anchor {
    Node(NodeRef),
    Start,
}
impl Serialize for Anchor {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Node(n) => n.serialize(s),
            Self::Start => s.serialize_str("start"),
        }
    }
}
impl<'de> Deserialize<'de> for Anchor {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        if s == "start" {
            Ok(Self::Start)
        } else {
            serde_json::from_value(serde_json::Value::String(s))
                .map(Self::Node)
                .map_err(serde::de::Error::custom)
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Shell {
    pub kind: Kind,
    pub attrs: Attrs,
    pub text: Option<Text>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase", deny_unknown_fields)]
pub enum Op {
    Move {
        node: NodeRef,
        parent: NodeRef,
        after: Vec<Anchor>,
    },
    Insert {
        node: NodeRef,
        parent: NodeRef,
        after: Vec<Anchor>,
        shell: Shell,
    },
    Delete {
        node: NodeRef,
    },
    Modify {
        node: NodeRef,
        edits: Vec<Edit>,
        old: Text,
        new: Text,
    },
    Format {
        node: NodeRef,
        old: Vec<Run>,
        new: Vec<Run>,
    },
    Attrs {
        node: NodeRef,
        old: Attrs,
        new: Attrs,
    },
}
impl Op {
    pub fn node(&self) -> NodeRef {
        match self {
            Self::Move { node, .. }
            | Self::Insert { node, .. }
            | Self::Delete { node }
            | Self::Modify { node, .. }
            | Self::Format { node, .. }
            | Self::Attrs { node, .. } => *node,
        }
    }
    pub fn placement(&self) -> Option<(NodeRef, &[Anchor])> {
        match self {
            Self::Move { parent, after, .. } | Self::Insert { parent, after, .. } => {
                Some((*parent, after))
            }
            _ => None,
        }
    }
    pub fn is_move(&self) -> bool {
        matches!(self, Self::Move { .. })
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Change {
    pub id: ChangeId,
    #[serde(flatten)]
    pub op: Op,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stats {
    pub i0: usize,
    pub i1: usize,
    pub i2: usize,
    pub i3: usize,
    pub candidates: usize,
    pub fallbacks: Vec<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Graph {
    pub v: u32,
    pub gid: Gid,
    pub from: Sid,
    pub to: Sid,
    pub algo: u32,
    pub options: String,
    pub changes: Vec<Change>,
    pub deps: Vec<(ChangeId, ChangeId)>,
    pub conflicts: Vec<Vec<ChangeId>>,
    #[serde(default)]
    pub explicit_conflicts: Vec<Vec<ChangeId>>,
    pub stats: Stats,
}
fn digest(domain: &str, bytes: &[u8]) -> String {
    let mut v = Vec::new();
    v.extend_from_slice(&(domain.len() as u64).to_le_bytes());
    v.extend_from_slice(domain.as_bytes());
    v.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    v.extend_from_slice(bytes);
    format!("{:032x}", xxhash_rust::xxh3::xxh3_128(&v))
}
impl Graph {
    pub fn rehash(&mut self) {
        self.gid = Gid(String::new());
        self.gid = Gid(digest(
            "marekvs-diff/graph/v2",
            &serde_json::to_vec(self).expect("graph serialization"),
        ));
    }
    pub fn canonical_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("graph serialization")
    }
}
pub fn change_id(from: Sid, to: Sid, options: &str, op: &Op) -> ChangeId {
    ChangeId(format!(
        "c:{}",
        digest(
            "marekvs-diff/change/v2",
            &serde_json::to_vec(&(ALGO_VERSION, from, to, options, op))
                .expect("operation serialization")
        )
    ))
}
/// Count serialized output without allocating its JSON buffer. The limit also
/// bounds the accumulated graph payload before cloning each input field.
fn charge<T: Serialize>(value: &T, used: &mut usize, o: &Options) -> Result<(), DiffError> {
    struct Counter<'a> {
        n: usize,
        limit: usize,
        cancel: &'a crate::CancelToken,
    }
    impl std::io::Write for Counter<'_> {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.cancel.check().map_err(std::io::Error::other)?;
            self.n = self.n.saturating_add(b.len());
            if self.n > self.limit {
                return Err(std::io::Error::other("graph bytes"));
            }
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter {
        n: *used,
        limit: o.budget.max_bytes,
        cancel: &o.cancel,
    };
    if serde_json::to_writer(&mut counter, value).is_err() {
        o.cancel.check()?;
        return Err(DiffError::TooBig {
            bound: "graph_bytes",
            n: counter.n,
            limit: o.budget.max_bytes,
        });
    }
    *used = counter.n;
    Ok(())
}
fn reserve(n: usize, used: &mut usize, o: &Options) -> Result<(), DiffError> {
    o.cancel.check()?;
    *used = used.saturating_add(n);
    if *used > o.budget.max_bytes {
        return Err(DiffError::TooBig {
            bound: "graph_bytes",
            n: *used,
            limit: o.budget.max_bytes,
        });
    }
    Ok(())
}
impl Graph {
    pub fn ensure_bounded(&self, o: &Options) -> Result<(), DiffError> {
        charge(self, &mut 0, o)
    }
}

pub fn build(a: &Tree, b: &Tree, m: &Matching, o: &Options) -> Result<Graph, DiffError> {
    o.cancel.check()?;
    let mut bytes_used = 0usize;
    reserve(512, &mut bytes_used, o)?;
    let options = o.digest();
    let mut raw = classify::run(a, b, m);
    // B document order is a semantic placement priority. Other ops keep A order.
    raw.sort_by_key(|r| match r {
        Raw::Insert { b } | Raw::Move { b, .. } => (0, *b),
        _ => (1, 0),
    });
    if raw.len() > o.budget.max_changes {
        return Err(DiffError::TooBig {
            bound: "changes",
            n: raw.len(),
            limit: o.budget.max_changes,
        });
    }
    let bref = |bi: u32| {
        m.b2a(bi)
            .map(|ai| NodeRef::A(a.nodes[ai as usize].lid))
            .unwrap_or(NodeRef::B(b.nodes[bi as usize].lid))
    };
    let mut positions = vec![0; b.nodes.len()];
    for n in &b.nodes {
        for (i, &c) in n.children.iter().enumerate() {
            positions[c as usize] = i;
        }
    }
    let mut anchors_used = 0usize;
    let mut changes = Vec::new();
    for r in raw {
        o.cancel.check()?;
        reserve(256, &mut bytes_used, o)?;
        let op = match r {
            r @ (Raw::Move { .. } | Raw::Insert { .. }) => {
                let bi = match r {
                    Raw::Move { b, .. } | Raw::Insert { b } => b,
                    _ => unreachable!(),
                };
                let bn = &b.nodes[bi as usize];
                let bp = bn.parent.expect("nonroot structural operation");
                let siblings = &b.nodes[bp as usize].children;
                let count = positions[bi as usize] + 1;
                anchors_used = anchors_used.saturating_add(count);
                if anchors_used > o.budget.max_nd {
                    return Err(DiffError::TooBig {
                        bound: "anchors",
                        n: anchors_used,
                        limit: o.budget.max_nd,
                    });
                }
                reserve(count.saturating_mul(40), &mut bytes_used, o)?;
                if matches!(r, Raw::Insert { .. }) {
                    charge(&bn.attrs, &mut bytes_used, o)?;
                    charge(&bn.text, &mut bytes_used, o)?;
                }
                let mut after: Vec<_> = siblings[..positions[bi as usize]]
                    .iter()
                    .rev()
                    .map(|&x| Anchor::Node(bref(x)))
                    .collect();
                after.push(Anchor::Start);
                match r {
                    Raw::Move { a: ai, .. } => Op::Move {
                        node: NodeRef::A(a.nodes[ai as usize].lid),
                        parent: bref(bp),
                        after,
                    },
                    _ => Op::Insert {
                        node: bref(bi),
                        parent: bref(bp),
                        after,
                        shell: Shell {
                            kind: bn.kind,
                            attrs: bn.attrs.clone(),
                            text: bn.text.clone(),
                        },
                    },
                }
            }
            Raw::Delete { a: ai } => Op::Delete {
                node: NodeRef::A(a.nodes[ai as usize].lid),
            },
            Raw::Modify { a: ai, b: bi } => {
                let old = a.nodes[ai as usize].text.as_ref().unwrap();
                let new = b.nodes[bi as usize].text.as_ref().unwrap();
                charge(old, &mut bytes_used, o)?;
                charge(new, &mut bytes_used, o)?;
                let edits = diff_text_bounded(&old.x, &new.x, o.level, &o.budget, &o.cancel)?;
                charge(&edits, &mut bytes_used, o)?;
                Op::Modify {
                    node: NodeRef::A(a.nodes[ai as usize].lid),
                    edits,
                    old: old.clone(),
                    new: new.clone(),
                }
            }
            Raw::Format { a: ai, b: bi } => {
                charge(
                    &a.nodes[ai as usize].text.as_ref().unwrap().f,
                    &mut bytes_used,
                    o,
                )?;
                charge(
                    &b.nodes[bi as usize].text.as_ref().unwrap().f,
                    &mut bytes_used,
                    o,
                )?;
                Op::Format {
                    node: NodeRef::A(a.nodes[ai as usize].lid),
                    old: a.nodes[ai as usize].text.as_ref().unwrap().f.clone(),
                    new: b.nodes[bi as usize].text.as_ref().unwrap().f.clone(),
                }
            }
            Raw::Attrs { a: ai, b: bi } => {
                charge(&a.nodes[ai as usize].attrs, &mut bytes_used, o)?;
                charge(&b.nodes[bi as usize].attrs, &mut bytes_used, o)?;
                Op::Attrs {
                    node: NodeRef::A(a.nodes[ai as usize].lid),
                    old: a.nodes[ai as usize].attrs.clone(),
                    new: b.nodes[bi as usize].attrs.clone(),
                }
            }
        };
        changes.push(Change {
            id: change_id(a.sid, b.sid, &options, &op),
            op,
        });
    }
    let inserts: BTreeMap<_, _> = changes
        .iter()
        .filter(|c| matches!(c.op, Op::Insert { .. }))
        .map(|c| (c.op.node(), c.id.clone()))
        .collect();
    let mut deps = Vec::new();
    for c in &changes {
        o.cancel.check()?;
        if let Some(id) = c.op.placement().and_then(|(p, _)| inserts.get(&p)) {
            reserve(80, &mut bytes_used, o)?;
            deps.push((c.id.clone(), id.clone()));
        }
    }
    let lookup: BTreeMap<_, _> = a
        .nodes
        .iter()
        .enumerate()
        .map(|(i, n)| (NodeRef::A(n.lid), i))
        .collect();
    let movable: std::collections::BTreeSet<_> = changes
        .iter()
        .filter(|c| c.op.is_move())
        .map(|c| c.op.node())
        .collect();
    let deletions: BTreeMap<_, _> = changes
        .iter()
        .filter(|c| matches!(c.op, Op::Delete { .. }))
        .map(|c| (c.op.node(), &c.id))
        .collect();
    let mut conflicts = Vec::new();
    let mut work = 0usize;
    // Walk each affected node's ancestry once, instead of comparing every
    // deletion to every change. Both ancestry work and emitted bytes are bounded.
    for c in &changes {
        o.cancel.check()?;
        if !matches!(
            c.op,
            Op::Modify { .. } | Op::Format { .. } | Op::Attrs { .. }
        ) {
            continue;
        }
        let mut at = lookup.get(&c.op.node()).copied();
        let mut may_escape = false;
        while let Some(i) = at {
            o.cancel.check()?;
            work = work.saturating_add(1);
            if work > o.budget.max_nd {
                return Err(DiffError::TooBig {
                    bound: "graph_work",
                    n: work,
                    limit: o.budget.max_nd,
                });
            }
            let nr = NodeRef::A(a.nodes[i].lid);
            may_escape |= movable.contains(&nr);
            if let Some(id) = deletions.get(&nr) {
                if !may_escape {
                    reserve(80, &mut bytes_used, o)?;
                    conflicts.push(vec![(*id).clone(), c.id.clone()]);
                }
            }
            at = a.nodes[i].parent.map(|p| p as usize);
        }
    }
    let mut g = Graph {
        v: 2,
        gid: Gid(String::new()),
        from: a.sid,
        to: b.sid,
        algo: ALGO_VERSION,
        options,
        changes,
        deps,
        conflicts,
        explicit_conflicts: vec![],
        stats: Stats::default(),
    };
    g.ensure_bounded(o)?;
    g.rehash();
    g.ensure_bounded(o)?;
    Ok(g)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn graph_wire_and_payload_identity() {
        let a = Tree::from_json(&json!({"t":"doc","c":[{"t":"sen","x":"one"}]})).unwrap();
        let b = Tree::from_json(&json!({"t":"doc","c":[{"t":"sen","x":"two"}]})).unwrap();
        let g = crate::diff(&a, &b, &Options::default()).unwrap();
        let bytes = g.canonical_bytes();
        let decoded: Graph = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(g, decoded);
        assert_eq!(g, crate::diff(&a, &b, &Options::default()).unwrap());
        let mut altered = g.clone();
        altered.stats.i0 += 1;
        altered.rehash();
        assert_ne!(g.gid, altered.gid);
        let left = Op::Attrs {
            node: NodeRef::A(a.nodes[1].lid),
            old: Attrs::default(),
            new: Attrs::from_pairs(&[("x", json!(1))]),
        };
        let mut right = left.clone();
        if let Op::Attrs { new, .. } = &mut right {
            new.0.insert("x".into(), json!(2));
        }
        assert_ne!(
            change_id(a.sid, b.sid, "options", &left),
            change_id(a.sid, b.sid, "options", &right)
        );
    }
    #[test]
    fn malformed_reference_never_panics() {
        for s in [
            "a:xyz",
            "éé000000000000000000000000000000",
            "z:00000000000000000000000000000000",
        ] {
            assert!(serde_json::from_value::<NodeRef>(json!(s)).is_err());
        }
    }
}

#[cfg(test)]
mod budget_tests {
    use super::*;
    use crate::matching::Layer;
    use serde_json::json;
    fn paired(a: &Tree, b: &Tree) -> Matching {
        let mut m = Matching::new(a, b);
        for i in 0..a.nodes.len() {
            assert!(m.set(i as u32, i as u32, Layer::I0));
        }
        m
    }
    #[test]
    fn rejects_graph_payload_before_accumulating_text_copies() {
        let a =
            Tree::from_json(&json!({"t":"doc","c":[{"t":"code","x":"a".repeat(1000)}]})).unwrap();
        let b =
            Tree::from_json(&json!({"t":"doc","c":[{"t":"code","x":"b".repeat(1000)}]})).unwrap();
        let o = Options {
            budget: crate::Budget {
                max_bytes: 1500,
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(matches!(
            build(&a, &b, &paired(&a, &b), &o),
            Err(DiffError::TooBig {
                bound: "graph_bytes",
                ..
            })
        ));
    }
    #[test]
    fn ancestor_hint_work_is_bounded_and_cancellable() {
        let a = Tree::from_json(&json!({"t":"doc","c":[{"t":"sec","c":[{"t":"sen","x":"a"}]}]}))
            .unwrap();
        let b = Tree::from_json(&json!({"t":"doc","c":[{"t":"sec","c":[{"t":"sen","x":"b"}]}]}))
            .unwrap();
        let mut o = Options {
            budget: crate::Budget {
                max_nd: 2,
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(matches!(
            build(&a, &b, &paired(&a, &b), &o),
            Err(DiffError::TooBig {
                bound: "graph_work",
                ..
            })
        ));
        o.cancel.cancel();
        assert!(matches!(
            build(&a, &b, &paired(&a, &b), &o),
            Err(DiffError::Cancelled)
        ));
        o = Options::default();
        let g = build(&a, &b, &paired(&a, &b), &o).unwrap();
        o.budget.max_bytes = g.canonical_bytes().len() - 1;
        assert!(g.ensure_bounded(&o).is_err());
    }
}

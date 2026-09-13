use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

pub type NodeId = u32;

/// Logical id: xxh3-128 of (sid ‖ ordinal path). Stable for a snapshot,
/// identical on every node that reads it (§5.3).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Lid(#[serde(with = "hex128")] pub u128);

/// Semantic snapshot identity (§5.2 step 2).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct Sid(#[serde(with = "hex128")] pub u128);

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Doc,
    Sec,
    Par,
    Sen,
    List,
    Li,
    Tbl,
    Row,
    Cell,
    Code,
}

impl Kind {
    pub fn from_tag(t: &str) -> Option<Kind> {
        Some(match t {
            "doc" => Kind::Doc,
            "sec" => Kind::Sec,
            "par" => Kind::Par,
            "sen" => Kind::Sen,
            "list" => Kind::List,
            "li" => Kind::Li,
            "tbl" => Kind::Tbl,
            "row" => Kind::Row,
            "cell" => Kind::Cell,
            "code" => Kind::Code,
            _ => return None,
        })
    }
    pub fn is_leaf(self) -> bool {
        matches!(self, Kind::Sen | Kind::Code)
    }
}

/// Flat map of scalar attributes, key-sorted (canonical form).
#[derive(Clone, PartialEq, Eq, Debug, Default, Serialize, Deserialize)]
pub struct Attrs(pub BTreeMap<String, serde_json::Value>);

impl Attrs {
    pub fn from_pairs(pairs: &[(&str, serde_json::Value)]) -> Attrs {
        Attrs(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        )
    }
}

/// Formatting run over code points, half-open. Canonical: sorted by start,
/// non-overlapping, equal-attr adjacent runs merged, never empty (§5.1).
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Run {
    pub start: u32,
    pub end: u32,
    pub attrs: Attrs,
}

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct Text {
    pub x: String,
    pub f: Vec<Run>,
}

#[derive(Clone, Debug)]
pub struct Node {
    pub kind: Kind,
    pub lid: Lid,
    /// Source element id when the tree came from marekvs records (I0 signal).
    pub eid: Option<[u8; 10]>,
    pub attrs: Attrs,
    pub text: Option<Text>,
    pub children: SmallVec<[NodeId; 4]>,
    pub parent: Option<NodeId>,
    pub ordinal: u32,
    pub weight: u32,
    pub h_content: u128,
    pub h_exact: u128,
}

#[derive(Clone, Debug)]
pub struct Tree {
    pub nodes: Vec<Node>,
    pub root: NodeId,
    pub sid: Sid,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ModelError {
    Limit { bound: &'static str, limit: usize },
    UnknownKind { path: String, tag: String },
    NotNormalized { path: String, why: &'static str },
    BadRun { path: String },
    Shape { path: String, why: &'static str },
}

impl Tree {
    /// Parse a canonical tree. Validates kinds, whitespace normalization (single
    /// spaces, trimmed; NFC is the converter contract) and run canonical form. Assigns `sid` and `lid`s;
    /// hashes and weights are filled by `hash::compute`, which this calls.
    pub fn from_json(v: &serde_json::Value) -> Result<Tree, ModelError> {
        Self::from_json_with_budget(v, &crate::Budget::default())
    }
    pub fn from_json_with_budget(
        v: &serde_json::Value,
        budget: &crate::Budget,
    ) -> Result<Tree, ModelError> {
        preflight(v, budget)?;
        if v.get("t").and_then(|t| t.as_str()) != Some("doc") {
            return Err(ModelError::Shape {
                path: "$".into(),
                why: "root must be doc",
            });
        }
        let mut nodes = Vec::new();
        parse_into(v, None, 0, "$", &mut nodes)?;
        let mut tree = Tree {
            nodes,
            root: 0,
            sid: Sid(0),
        };
        crate::hash::compute(&mut tree);
        tree.sid = crate::canonical::sid(&tree);
        crate::canonical::assign_lids(&mut tree);
        Ok(tree)
    }

    pub fn approx_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.nodes.capacity() * std::mem::size_of::<Node>()
            + self
                .nodes
                .iter()
                .map(|n| {
                    n.text.as_ref().map_or(0, |t| {
                        t.x.capacity() + t.f.capacity() * std::mem::size_of::<Run>()
                    }) + serde_json::to_vec(&n.attrs).unwrap().len()
                })
                .sum::<usize>()
    }
    pub fn children(&self, id: NodeId) -> &[NodeId] {
        &self.nodes[id as usize].children
    }
    pub fn node(&self, id: NodeId) -> &Node {
        &self.nodes[id as usize]
    }
    pub fn ordinal_path(&self, mut id: NodeId) -> Vec<u32> {
        let mut p = Vec::new();
        while let Some(parent) = self.nodes[id as usize].parent {
            p.push(self.nodes[id as usize].ordinal);
            id = parent;
        }
        p.reverse();
        p
    }
    /// Emit canonical JSON (the inverse of `from_json`).
    pub fn to_json(&self) -> serde_json::Value {
        to_json_at(self, self.root)
    }
}

fn parse_into(
    v: &serde_json::Value,
    parent: Option<NodeId>,
    ordinal: u32,
    path: &str,
    out: &mut Vec<Node>,
) -> Result<NodeId, ModelError> {
    let obj = v.as_object().ok_or(ModelError::Shape {
        path: path.into(),
        why: "node must be an object",
    })?;
    let tag = obj
        .get("t")
        .and_then(|t| t.as_str())
        .ok_or(ModelError::Shape {
            path: path.into(),
            why: "missing t",
        })?;
    let kind = Kind::from_tag(tag).ok_or(ModelError::UnknownKind {
        path: path.into(),
        tag: tag.into(),
    })?;
    if obj
        .keys()
        .any(|k| !["t", "a", "c", "x", "f"].contains(&k.as_str()))
    {
        return Err(ModelError::Shape {
            path: path.into(),
            why: "unknown field",
        });
    }
    if !kind.is_leaf() && (obj.contains_key("x") || obj.contains_key("f")) {
        return Err(ModelError::Shape {
            path: path.into(),
            why: "container has leaf fields",
        });
    }
    let mut attrs = Attrs::default();
    if let Some(a) = obj.get("a") {
        let m = a.as_object().ok_or(ModelError::Shape {
            path: path.into(),
            why: "a must be an object",
        })?;
        for (k, val) in m {
            if val.is_object()
                || val.is_array()
                || val.as_u64().is_some_and(|n| n > i64::MAX as u64)
            {
                return Err(ModelError::Shape {
                    path: path.into(),
                    why: "attrs must be scalars with signed-64-bit integers",
                });
            }
            attrs.0.insert(k.clone(), val.clone());
        }
    }
    let text = if kind.is_leaf() {
        let x = obj
            .get("x")
            .and_then(|x| x.as_str())
            .ok_or(ModelError::Shape {
                path: path.into(),
                why: "leaf needs x",
            })?;
        if kind == Kind::Sen {
            check_normalized(x, path)?;
        }
        let n = u32::try_from(x.chars().count())
            .map_err(|_| ModelError::BadRun { path: path.into() })?;
        let mut runs = Vec::new();
        if let Some(f) = obj.get("f") {
            for r in f
                .as_array()
                .ok_or(ModelError::BadRun { path: path.into() })?
            {
                let arr = r
                    .as_array()
                    .ok_or(ModelError::BadRun { path: path.into() })?;
                if arr.len() != 3 {
                    return Err(ModelError::BadRun { path: path.into() });
                }
                let (s, e) = (
                    arr.first().and_then(|s| s.as_u64()),
                    arr.get(1).and_then(|e| e.as_u64()),
                );
                let (Some(s), Some(e)) = (s, e) else {
                    return Err(ModelError::BadRun { path: path.into() });
                };
                if s >= e || e > u64::from(n) {
                    return Err(ModelError::BadRun { path: path.into() });
                }
                let ra = arr
                    .get(2)
                    .and_then(|a| a.as_object())
                    .ok_or(ModelError::BadRun { path: path.into() })?;
                if ra.is_empty()
                    || ra.iter().any(|(k, v)| match k.as_str() {
                        "b" | "i" | "u" | "s" | "code" => !v.is_boolean(),
                        "link" => !v.is_string(),
                        _ => true,
                    })
                {
                    return Err(ModelError::BadRun { path: path.into() });
                }
                runs.push(Run {
                    start: s as u32,
                    end: e as u32,
                    attrs: Attrs(ra.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
                });
            }
        }
        if !is_canonical_runs(&runs) {
            return Err(ModelError::BadRun { path: path.into() });
        }
        Some(Text {
            x: x.to_string(),
            f: runs,
        })
    } else {
        None
    };
    let id = out.len() as NodeId;
    out.push(Node {
        kind,
        lid: Lid(0),
        eid: None,
        attrs,
        text,
        children: SmallVec::new(),
        parent,
        ordinal,
        weight: 0,
        h_content: 0,
        h_exact: 0,
    });
    if let Some(c) = obj.get("c") {
        if kind.is_leaf() {
            return Err(ModelError::Shape {
                path: path.into(),
                why: "leaf has children",
            });
        }
        for (i, child) in c
            .as_array()
            .ok_or(ModelError::Shape {
                path: path.into(),
                why: "c must be an array",
            })?
            .iter()
            .enumerate()
        {
            let cid = parse_into(child, Some(id), i as u32, &format!("{path}.c[{i}]"), out)?;
            out[id as usize].children.push(cid);
        }
    }
    Ok(id)
}

/// Trim, single spaces and no exotic whitespace are validated here. **NFC is
/// the converter's contract (spec §5.1) and is not validated server-side**:
/// the crate takes no Unicode-normalization dependency. Say so in design/19.
fn check_normalized(x: &str, path: &str) -> Result<(), ModelError> {
    if x.starts_with(' ') || x.ends_with(' ') {
        return Err(ModelError::NotNormalized {
            path: path.into(),
            why: "untrimmed",
        });
    }
    if x.contains("  ") {
        return Err(ModelError::NotNormalized {
            path: path.into(),
            why: "double space",
        });
    }
    if x.chars()
        .any(|c| c != ' ' && c != '\n' && c.is_whitespace())
    {
        return Err(ModelError::NotNormalized {
            path: path.into(),
            why: "non-space whitespace",
        });
    }
    Ok(())
}

fn is_canonical_runs(runs: &[Run]) -> bool {
    runs.windows(2)
        .all(|w| w[0].end <= w[1].start && !(w[0].end == w[1].start && w[0].attrs == w[1].attrs))
}

fn to_json_at(t: &Tree, id: NodeId) -> serde_json::Value {
    let n = t.node(id);
    let mut m = serde_json::Map::new();
    m.insert("t".into(), serde_json::to_value(n.kind).unwrap());
    if !n.attrs.0.is_empty() {
        m.insert(
            "a".into(),
            serde_json::Value::Object(
                n.attrs
                    .0
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            ),
        );
    }
    if let Some(text) = &n.text {
        m.insert("x".into(), text.x.clone().into());
        if !text.f.is_empty() {
            m.insert(
                "f".into(),
                text.f
                    .iter()
                    .map(|r| {
                        serde_json::json!([
                            r.start,
                            r.end,
                            serde_json::Value::Object(
                                r.attrs
                                    .0
                                    .iter()
                                    .map(|(k, v)| (k.clone(), v.clone()))
                                    .collect()
                            )
                        ])
                    })
                    .collect(),
            );
        }
    }
    if !n.children.is_empty() {
        m.insert(
            "c".into(),
            n.children.iter().map(|c| to_json_at(t, *c)).collect(),
        );
    }
    serde_json::Value::Object(m)
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn canonical_tree_and_hashes() {
        let v = json!({"t":"doc","c":[{"t":"sen","x":"Hello world."}]});
        let a = Tree::from_json(&v).unwrap();
        assert_eq!(a.to_json(), v);
        assert_eq!(a.nodes[1].parent, Some(0));
        assert_eq!(a.nodes[0].weight, 2);
        let mut v = v;
        v["c"][0]["f"] = json!([[0,5,{"b":true}]]);
        let b = Tree::from_json(&v).unwrap();
        assert_ne!(a.sid, b.sid);
        assert_eq!(a.nodes[0].h_content, b.nodes[0].h_content);
        assert_ne!(a.nodes[0].h_exact, b.nodes[0].h_exact);
        assert_eq!(
            serde_json::to_string(&Sid(1)).unwrap(),
            "\"00000000000000000000000000000001\""
        );
    }
    #[test]
    fn strict_boundary() {
        for v in [
            json!({"t":"sen","x":"ok"}),
            json!({"t":"doc","z":1}),
            json!({"t":"doc","x":"bad"}),
            json!({"t":"doc","c":[{"t":"sen","x":"a  b"}]}),
            json!({"t":"doc","c":[{"t":"sen","x":"ab","f":[[0,4294967297u64,{}]]}]}),
            json!({"t":"doc","c":[{"t":"sen","x":"ab","f":[[0,1,{"b":1}]]}]}),
            json!({"t":"doc","c":[{"t":"sen","x":"ab","f":[[0,1,{},9]]}]}),
        ] {
            assert!(Tree::from_json(&v).is_err(), "{v}");
        }
        assert!(Tree::from_json(
            &json!({"t":"doc","c":[{"t":"sen","x":"\nHello\n"},{"t":"code","x":"  x\t\n"}]})
        )
        .is_ok());
    }
    #[test]
    fn limits_before_recursion() {
        let v = json!({"t":"doc","c":[{"t":"sen","x":"one two"}]});
        for b in [
            crate::Budget {
                max_depth: 0,
                ..Default::default()
            },
            crate::Budget {
                max_nodes: 1,
                ..Default::default()
            },
            crate::Budget {
                max_bytes: 1,
                ..Default::default()
            },
            crate::Budget {
                max_leaf_tokens: 1,
                ..Default::default()
            },
        ] {
            assert!(Tree::from_json_with_budget(&v, &b).is_err());
        }
    }
}

impl std::fmt::Display for ModelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for ModelError {}
mod hex128 {
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(v: &u128, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&format!("{v:032x}"))
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<u128, D::Error> {
        let s = String::deserialize(d)?;
        if s.len() != 32
            || !s
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(serde::de::Error::custom("expected 32 lowercase hex digits"));
        }
        u128::from_str_radix(&s, 16).map_err(serde::de::Error::custom)
    }
}
/// Iterative preflight bounds arbitrary JSON before serialization or recursive node parsing.
fn preflight(v: &serde_json::Value, b: &crate::Budget) -> Result<(), ModelError> {
    // Semantic traversal follows only `c`; attribute keys may themselves be named `t`.
    let mut semantic = vec![(v, 0usize)];
    let mut nodes = 0usize;
    while let Some((node, depth)) = semantic.pop() {
        nodes += 1;
        if nodes > b.max_nodes || nodes > u32::MAX as usize {
            return Err(ModelError::Limit {
                bound: "nodes",
                limit: b.max_nodes,
            });
        }
        if depth > b.max_depth {
            return Err(ModelError::Limit {
                bound: "depth",
                limit: b.max_depth,
            });
        }
        if let Some(x) = node.get("x").and_then(|x| x.as_str()) {
            if crate::hash::token_count(x) > b.max_leaf_tokens {
                return Err(ModelError::Limit {
                    bound: "leaf_tokens",
                    limit: b.max_leaf_tokens,
                });
            }
        }
        if let Some(children) = node.get("c").and_then(|c| c.as_array()) {
            if children.len() > b.max_nodes.saturating_sub(nodes) {
                return Err(ModelError::Limit {
                    bound: "nodes",
                    limit: b.max_nodes,
                });
            }
            for child in children {
                semantic.push((child, depth + 1));
            }
        }
    }
    let mut stack = vec![(v, 0usize)];
    let mut bytes = 0usize;
    while let Some((value, depth)) = stack.pop() {
        if depth > b.max_depth.saturating_mul(2).saturating_add(8) {
            return Err(ModelError::Limit {
                bound: "depth",
                limit: b.max_depth,
            });
        }
        match value {
            serde_json::Value::Object(o) => {
                for (k, v) in o {
                    bytes = bytes.saturating_add(k.len());
                    stack.push((v, depth + 1));
                }
            }
            serde_json::Value::Array(a) => {
                for v in a {
                    stack.push((v, depth + 1));
                }
            }
            serde_json::Value::String(s) => bytes = bytes.saturating_add(s.len()),
            _ => bytes = bytes.saturating_add(value.to_string().len()),
        }
        if bytes > b.max_bytes {
            return Err(ModelError::Limit {
                bound: "bytes",
                limit: b.max_bytes,
            });
        }
    }
    // Exact serialized byte size, bounded above by six times the preflight estimate.
    if serde_json::to_vec(v).unwrap().len() > b.max_bytes {
        return Err(ModelError::Limit {
            bound: "bytes",
            limit: b.max_bytes,
        });
    }
    Ok(())
}

#[cfg(test)]
mod boundary_regressions {
    use super::*;
    use serde_json::json;
    #[test]
    fn scalar_attribute_named_t_is_not_a_document_node() {
        let v = json!({"t":"doc","a":{"t":"label"}});
        assert!(Tree::from_json_with_budget(
            &v,
            &crate::Budget {
                max_nodes: 1,
                max_depth: 0,
                ..Default::default()
            }
        )
        .is_ok());
    }
    #[test]
    fn exact_serialized_byte_budget_is_accepted() {
        let v = json!({"t":"doc","a":{"x":true,"count":123}});
        let b = crate::Budget {
            max_bytes: serde_json::to_vec(&v).unwrap().len(),
            ..Default::default()
        };
        assert!(Tree::from_json_with_budget(&v, &b).is_ok());
    }
}

#[cfg(test)]
mod validation_matrix {
    use super::*;
    use serde_json::json;
    #[test]
    fn rejects_noncanonical_runs() {
        for f in [
            json!([[0,1,{"b":true}],[1,2,{"b":true}]]),
            json!([[0,2,{"i":true}],[1,3,{"b":true}]]),
            json!([[1, 2, {}], [0, 1, {}]]),
            json!([[0, 0, {}]]),
            json!([[0,1,{"unknown":true}]]),
            json!([[0,1,{"link":false}]]),
        ] {
            assert!(
                Tree::from_json(&json!({"t":"doc","c":[{"t":"sen","x":"abc","f":f}]})).is_err()
            );
        }
    }
    #[test]
    fn digest_wire_roundtrips_and_rejects_noncanonical() {
        assert_eq!(
            serde_json::from_str::<Lid>(&serde_json::to_string(&Lid(u128::MAX)).unwrap()).unwrap(),
            Lid(u128::MAX)
        );
        for s in ["1", "\"1\"", "\"FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\""] {
            assert!(serde_json::from_str::<Sid>(s).is_err());
        }
    }
}

#[cfg(test)]
mod empty_format_regression {
    use super::*;
    #[test]
    fn empty_marks_are_not_canonical() {
        let v = serde_json::json!({"t":"doc","c":[{"t":"sen","x":"abc","f":[[0,1,{}]]}]});
        assert!(Tree::from_json(&v).is_err());
    }
}

#[cfg(test)]
mod numeric_storage_regression {
    use super::*;
    #[test]
    fn out_of_signed_range_integer_is_rejected_without_silent_float_rounding() {
        let v = serde_json::json!({"t":"doc","a":{"count":u64::MAX}});
        assert!(Tree::from_json(&v).is_err());
    }
}

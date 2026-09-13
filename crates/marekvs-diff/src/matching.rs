//! One-to-one correspondence with the evidence layer retained per source node.
use crate::model::{NodeId, Tree};
#[derive(Clone, Copy, PartialEq, Eq, Debug, serde::Serialize, serde::Deserialize)]
pub enum Layer {
    I0,
    I1Exact,
    I1Content,
    I2,
    I3,
}
#[derive(Clone, Debug)]
pub struct Matching {
    a2b: Vec<Option<NodeId>>,
    b2a: Vec<Option<NodeId>>,
    layer: Vec<Option<Layer>>,
    format_changed: Vec<NodeId>,
}
impl Matching {
    pub fn new(a: &Tree, b: &Tree) -> Self {
        Self {
            a2b: vec![None; a.nodes.len()],
            b2a: vec![None; b.nodes.len()],
            layer: vec![None; a.nodes.len()],
            format_changed: Vec::new(),
        }
    }
    /// Refuse conflicting or out-of-bounds evidence without disturbing prior pairs.
    pub fn set(&mut self, x: NodeId, y: NodeId, layer: Layer) -> bool {
        if x as usize >= self.a2b.len() || y as usize >= self.b2a.len() {
            return false;
        }
        if self.a2b[x as usize].is_some() || self.b2a[y as usize].is_some() {
            let same = self.a2b[x as usize] == Some(y) && self.b2a[y as usize] == Some(x);
            if same && self.layer[x as usize].is_some_and(|old| (layer as u8) < (old as u8)) {
                self.layer[x as usize] = Some(layer);
            }
            return same;
        }
        self.a2b[x as usize] = Some(y);
        self.b2a[y as usize] = Some(x);
        self.layer[x as usize] = Some(layer);
        true
    }
    pub fn a2b(&self, x: NodeId) -> Option<NodeId> {
        self.a2b.get(x as usize).copied().flatten()
    }
    pub fn b2a(&self, y: NodeId) -> Option<NodeId> {
        self.b2a.get(y as usize).copied().flatten()
    }
    pub fn layer(&self, x: NodeId) -> Option<Layer> {
        self.layer.get(x as usize).copied().flatten()
    }
    pub fn pairs(&self) -> impl Iterator<Item = (NodeId, NodeId)> + '_ {
        self.a2b
            .iter()
            .enumerate()
            .filter_map(|(i, v)| v.map(|j| (i as NodeId, j)))
    }
    pub fn matched_count(&self) -> usize {
        self.pairs().count()
    }
    pub fn format_changed(&self) -> &[NodeId] {
        &self.format_changed
    }
    pub(crate) fn mark_format(&mut self, x: NodeId) {
        self.format_changed.push(x);
    }
    pub fn unmatched_a(&self) -> Vec<NodeId> {
        self.a2b
            .iter()
            .enumerate()
            .filter_map(|(i, v)| v.is_none().then_some(i as NodeId))
            .collect()
    }
    pub fn unmatched_b(&self) -> Vec<NodeId> {
        self.b2a
            .iter()
            .enumerate()
            .filter_map(|(i, v)| v.is_none().then_some(i as NodeId))
            .collect()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pairing_preserves_bijection() {
        let t = Tree::from_json(&serde_json::json!({"t":"doc","c":[{"t":"sen","x":"x"}]})).unwrap();
        let mut m = Matching::new(&t, &t);
        assert!(m.set(0, 0, Layer::I3));
        assert!(!m.set(1, 0, Layer::I3));
        assert!(!m.set(0, 1, Layer::I3));
        assert_eq!(m.pairs().collect::<Vec<_>>(), vec![(0, 0)]);
    }
}

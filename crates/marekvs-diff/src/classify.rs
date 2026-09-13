//! Structural classification. An LIS retains the maximum ordered sibling subset.
use crate::{
    matching::Matching,
    model::{NodeId, Tree},
};
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Raw {
    Move { a: NodeId, b: NodeId },
    Delete { a: NodeId },
    Insert { b: NodeId },
    Modify { a: NodeId, b: NodeId },
    Format { a: NodeId, b: NodeId },
    Attrs { a: NodeId, b: NodeId },
}

fn lis(xs: &[usize]) -> Vec<usize> {
    // Suffix lengths make the reconstruction prefer the earliest A siblings
    // among equally long subsequences, without a quadratic path comparison.
    let mut sorted = xs.to_vec();
    sorted.sort_unstable();
    sorted.dedup();
    let mut bit = vec![0usize; sorted.len() + 1];
    let mut suffix = vec![0; xs.len()];
    for (i, &x) in xs.iter().enumerate().rev() {
        let rank = sorted.len() - sorted.binary_search(&x).unwrap();
        let mut q = rank - 1;
        let mut best = 0;
        while q > 0 {
            best = best.max(bit[q]);
            q &= q - 1;
        }
        suffix[i] = best + 1;
        let mut q = rank;
        while q < bit.len() {
            bit[q] = bit[q].max(best + 1);
            q += q & (!q + 1);
        }
    }
    let mut remaining = suffix.iter().copied().max().unwrap_or(0);
    let mut last = None;
    let mut out = Vec::new();
    for (i, &x) in xs.iter().enumerate() {
        if remaining > 0 && last.is_none_or(|v| x > v) && suffix[i] >= remaining {
            out.push(i);
            last = Some(x);
            remaining -= 1;
        }
    }
    out
}

pub fn run(a: &Tree, b: &Tree, m: &Matching) -> Vec<Raw> {
    let mut moved = vec![false; a.nodes.len()];
    let mut ord = vec![0; b.nodes.len()];
    for n in &b.nodes {
        for (i, &c) in n.children.iter().enumerate() {
            ord[c as usize] = i;
        }
    }
    for (ai, n) in a.nodes.iter().enumerate() {
        if let Some(bi) = m.a2b(ai as NodeId) {
            if ai != a.root as usize
                && n.parent.and_then(|p| m.a2b(p)) != b.nodes[bi as usize].parent
            {
                moved[ai] = true;
            }
            let kids: Vec<_> = n
                .children
                .iter()
                .filter_map(|&c| {
                    m.a2b(c)
                        .filter(|&bc| b.nodes[bc as usize].parent == Some(bi))
                        .map(|bc| (c, ord[bc as usize]))
                })
                .collect();
            let xs: Vec<_> = kids.iter().map(|x| x.1).collect();
            let stays = lis(&xs);
            for &(c, _) in &kids {
                moved[c as usize] = true;
            }
            for i in stays {
                moved[kids[i].0 as usize] = false;
            }
        }
    }
    let mut out = Vec::new();
    for (ai, n) in a.nodes.iter().enumerate() {
        let ai = ai as NodeId;
        if let Some(bi) = m.a2b(ai) {
            let bn = &b.nodes[bi as usize];
            if moved[ai as usize] {
                out.push(Raw::Move { a: ai, b: bi });
            }
            if n.attrs != bn.attrs {
                out.push(Raw::Attrs { a: ai, b: bi });
            }
            if let (Some(x), Some(y)) = (&n.text, &bn.text) {
                if x.x != y.x {
                    out.push(Raw::Modify { a: ai, b: bi });
                } else if x.f != y.f {
                    out.push(Raw::Format { a: ai, b: bi });
                }
            }
        } else if ai != a.root && n.parent.is_none_or(|p| m.a2b(p).is_some()) {
            out.push(Raw::Delete { a: ai });
        }
    }
    for bi in 0..b.nodes.len() {
        if bi != b.root as usize && m.b2a(bi as NodeId).is_none() {
            out.push(Raw::Insert { b: bi as NodeId });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rotation_keeps_two() {
        assert_eq!(lis(&[1, 2, 0]), vec![0, 1]);
    }
    #[test]
    fn ties_keep_earlier_siblings() {
        assert_eq!(lis(&[1, 0]), vec![0]);
    }
    #[test]
    fn increasing_keeps_all() {
        assert_eq!(lis(&[0, 1, 2]), vec![0, 1, 2]);
    }
}

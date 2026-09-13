//! Bounded patience/Myers sibling alignment and explicit contextual substitutions.
use crate::{
    budget::{Budget, CancelToken, DiffError},
    matching::{Layer, Matching},
    model::{NodeId, Tree},
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
#[derive(Clone, Debug, Default)]
pub struct AlignStats {
    pub fallbacks: Vec<String>,
    pub nd: usize,
}
fn lis(pairs: &[(usize, usize)]) -> Vec<(usize, usize)> {
    let mut tails: Vec<usize> = Vec::new();
    let mut prev = vec![None; pairs.len()];
    for (i, &(_, y)) in pairs.iter().enumerate() {
        let k = tails.partition_point(|&j| pairs[j].1 < y);
        if k > 0 {
            prev[i] = Some(tails[k - 1]);
        }
        if k == tails.len() {
            tails.push(i);
        } else {
            tails[k] = i;
        }
    }
    let mut out = Vec::new();
    let mut at = tails.last().copied();
    while let Some(i) = at {
        out.push(pairs[i]);
        at = prev[i];
    }
    out.reverse();
    out
}
fn equal(a: &Tree, b: &Tree, x: NodeId, y: NodeId) -> bool {
    a.node(x).kind == b.node(y).kind && a.node(x).h_content == b.node(y).h_content
}
fn myers(
    a: &Tree,
    b: &Tree,
    xs: &[NodeId],
    ys: &[NodeId],
    remaining: &mut usize,
    cancel: &CancelToken,
) -> Result<Option<Vec<(usize, usize)>>, DiffError> {
    if xs.is_empty() || ys.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let max = xs.len() + ys.len();
    let offset = max as isize + 1;
    if max.saturating_mul(2).saturating_add(3) > *remaining {
        return Ok(None);
    }
    let mut v = vec![0isize; 2 * max + 3];
    let mut trace = Vec::new();
    for d in 0..=max {
        cancel.check()?;
        // Trace storage, snake steps and diagonal expansions all consume the cap.
        if *remaining < v.len() {
            return Ok(None);
        }
        *remaining -= v.len();
        trace.push(v.clone());
        for k in (-(d as isize)..=d as isize).step_by(2) {
            cancel.check()?;
            if *remaining == 0 {
                return Ok(None);
            }
            *remaining -= 1;
            let idx = (offset + k) as usize;
            let mut x = if k == -(d as isize) || (k != d as isize && v[idx - 1] < v[idx + 1]) {
                v[idx + 1]
            } else {
                v[idx - 1] + 1
            };
            let mut y = x - k;
            while x < xs.len() as isize
                && y < ys.len() as isize
                && equal(a, b, xs[x as usize], ys[y as usize])
            {
                if *remaining == 0 {
                    return Ok(None);
                }
                *remaining -= 1;
                cancel.check()?;
                x += 1;
                y += 1;
            }
            v[idx] = x;
            if x == xs.len() as isize && y == ys.len() as isize {
                let mut result = Vec::new();
                let (mut x, mut y) = (x, y);
                for depth in (0..=d).rev() {
                    let old = &trace[depth];
                    let k = x - y;
                    let idx = (offset + k) as usize;
                    let pk = if k == -(depth as isize)
                        || (k != depth as isize && old[idx - 1] < old[idx + 1])
                    {
                        k + 1
                    } else {
                        k - 1
                    };
                    let px = old[(offset + pk) as usize];
                    let py = px - pk;
                    while x > px && y > py {
                        x -= 1;
                        y -= 1;
                        result.push((x as usize, y as usize));
                    }
                    if depth > 0 {
                        x = px;
                        y = py;
                    }
                }
                result.reverse();
                return Ok(Some(result));
            }
        }
    }
    Ok(Some(Vec::new()))
}
fn substitute(a: &Tree, b: &Tree, xs: &[NodeId], ys: &[NodeId], m: &mut Matching) {
    // A matched-parent gap with equal cardinality has an unambiguous contextual
    // substitution only when the ordered kinds agree. Unequal gaps stay edits.
    let x: Vec<_> = xs.iter().copied().filter(|x| m.a2b(*x).is_none()).collect();
    let y: Vec<_> = ys.iter().copied().filter(|y| m.b2a(*y).is_none()).collect();
    if x.len() == y.len()
        && x.iter()
            .zip(&y)
            .all(|(x, y)| a.node(*x).kind == b.node(*y).kind)
    {
        for (x, y) in x.into_iter().zip(y) {
            m.set(x, y, Layer::I3);
        }
    }
}
pub fn run(
    a: &Tree,
    b: &Tree,
    m: &mut Matching,
    budget: &Budget,
    cancel: &CancelToken,
) -> Result<AlignStats, DiffError> {
    cancel.check()?;
    if a.node(a.root).kind == b.node(b.root).kind {
        m.set(a.root, b.root, Layer::I3);
    }
    let mut stats = AlignStats::default();
    let mut remaining = budget.max_nd;
    let mut queue: VecDeque<_> = m.pairs().collect();
    let mut visited = BTreeSet::new();
    while let Some((p, q)) = queue.pop_front() {
        cancel.check()?;
        if !visited.insert((p, q)) {
            continue;
        }
        let (xs, ys) = (&a.node(p).children, &b.node(q).children);
        if xs.is_empty() || ys.is_empty() {
            continue;
        }
        let positions: BTreeMap<_, _> = ys.iter().enumerate().map(|(i, &y)| (y, i)).collect();
        let mut anchors: Vec<_> = xs
            .iter()
            .enumerate()
            .filter_map(|(i, &x)| m.a2b(x).and_then(|y| positions.get(&y).map(|&j| (i, j))))
            .collect();
        // Patience anchors: exact/content hashes unique among unmatched siblings.
        let mut left: BTreeMap<_, Vec<usize>> = BTreeMap::new();
        let mut right: BTreeMap<_, Vec<usize>> = BTreeMap::new();
        for (i, &x) in xs.iter().enumerate() {
            if m.a2b(x).is_none() {
                left.entry((a.node(x).kind as u8, a.node(x).h_content))
                    .or_default()
                    .push(i);
            }
        }
        for (j, &y) in ys.iter().enumerate() {
            if m.b2a(y).is_none() {
                right
                    .entry((b.node(y).kind as u8, b.node(y).h_content))
                    .or_default()
                    .push(j);
            }
        }
        for (key, is) in left {
            if is.len() == 1 {
                if let Some(js) = right.get(&key) {
                    if js.len() == 1 {
                        anchors.push((is[0], js[0]));
                    }
                }
            }
        }
        anchors.sort_unstable();
        let anchors = lis(&anchors);
        let mut start = (0, 0);
        for end in anchors
            .into_iter()
            .chain(std::iter::once((xs.len(), ys.len())))
        {
            let ax: Vec<_> = xs[start.0..end.0]
                .iter()
                .copied()
                .filter(|x| m.a2b(*x).is_none())
                .collect();
            let by: Vec<_> = ys[start.1..end.1]
                .iter()
                .copied()
                .filter(|y| m.b2a(*y).is_none())
                .collect();
            if let Some(equal_pairs) = myers(a, b, &ax, &by, &mut remaining, cancel)? {
                let mut gap = (0, 0);
                for (i, j) in equal_pairs
                    .into_iter()
                    .chain(std::iter::once((ax.len(), by.len())))
                {
                    substitute(a, b, &ax[gap.0..i], &by[gap.1..j], m);
                    if i < ax.len() && j < by.len() {
                        m.set(ax[i], by[j], Layer::I3);
                    }
                    gap = (i + 1, j + 1);
                }
            } else {
                stats.fallbacks.push("candidate cap reached".into()); /* Keep ambiguous leftovers as insert/delete on exhaustion. */
            }
            if end.0 < xs.len() && end.1 < ys.len() {
                m.set(xs[end.0], ys[end.1], Layer::I3);
            }
            start = (end.0 + 1, end.1 + 1);
        }
        for &x in xs {
            if let Some(y) = m.a2b(x) {
                if !visited.contains(&(x, y)) {
                    queue.push_back((x, y));
                }
            }
        }
    }
    stats.nd = budget.max_nd - remaining;
    Ok(stats)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn edited_gap_is_substituted_but_trailing_insert_is_not() {
        let make = |xs: &[&str]| {
            Tree::from_json(&serde_json::json!({"t":"doc","c":xs.iter().map(|s|serde_json::json!({"t":"sen","x":s})).collect::<Vec<_>>()})).unwrap()
        };
        let a = make(&["first", "old", "last"]);
        let b = make(&["first", "edited", "last", "extra"]);
        let mut m = Matching::new(&a, &b);
        crate::exact::run(&a, &b, &mut m, &CancelToken::none()).unwrap();
        run(&a, &b, &mut m, &Budget::default(), &CancelToken::none()).unwrap();
        assert_eq!(m.a2b(2), Some(2));
        assert_eq!(m.layer(2), Some(Layer::I3));
        assert_eq!(m.b2a(4), None);
    }
}

#[cfg(test)]
mod regression_tests {
    use super::*;
    #[test]
    fn myers_duplicates_and_budget_exhaustion() {
        let t=Tree::from_json(&serde_json::json!({"t":"doc","c":[{"t":"sen","x":"a"},{"t":"sen","x":"b"},{"t":"sen","x":"a"}]})).unwrap();
        let mut budget = 10000;
        let pairs = myers(
            &t,
            &t,
            &[1, 2, 3],
            &[1, 3],
            &mut budget,
            &CancelToken::none(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(pairs, vec![(0, 0), (2, 1)]);
        let mut budget = 0;
        assert!(myers(&t, &t, &[1], &[1], &mut budget, &CancelToken::none())
            .unwrap()
            .is_none());
    }
    #[test]
    fn substitution_respects_kinds_and_nd_cap() {
        let a =
            Tree::from_json(&serde_json::json!({"t":"doc","c":[{"t":"sen","x":"old"}]})).unwrap();
        let b =
            Tree::from_json(&serde_json::json!({"t":"doc","c":[{"t":"code","x":"new"}]})).unwrap();
        let mut m = Matching::new(&a, &b);
        run(&a, &b, &mut m, &Budget::default(), &CancelToken::none()).unwrap();
        assert_eq!(m.a2b(1), None);
        let mut m = Matching::new(&a, &a);
        let budget = Budget {
            max_nd: 0,
            ..Default::default()
        };
        let stats = run(&a, &a, &mut m, &budget, &CancelToken::none()).unwrap();
        assert_eq!(stats.nd, 0);
    }
}

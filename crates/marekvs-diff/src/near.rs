//! Deterministic bounded near matching. Seeds are splitmix64(0x6d6172656b767332 + slot).
use crate::{
    budget::DiffError,
    matching::{Layer, Matching},
    model::{NodeId, Tree},
    Options,
};
use std::collections::{BTreeMap, BTreeSet};
use xxhash_rust::xxh3::xxh3_64;
#[derive(Clone, Debug, Default)]
pub struct NearStats {
    pub candidates: usize,
    pub fallbacks: Vec<String>,
    pub candidates_pairs: Vec<(NodeId, NodeId)>,
}
fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}
/// Actual shingle set, also exposed for independent recall measurements.
pub fn shingles(text: &str) -> BTreeSet<u64> {
    let words: Vec<_> = text.split_whitespace().collect();
    if text.is_empty() {
        return BTreeSet::new();
    }
    if words.len() > 16 {
        words
            .windows(3)
            .map(|w| {
                let mut bytes = Vec::new();
                for s in w {
                    bytes.extend_from_slice(&(s.len() as u64).to_be_bytes());
                    bytes.extend_from_slice(s.as_bytes());
                }
                xxh3_64(&bytes)
            })
            .collect()
    } else {
        let chars: Vec<_> = text.chars().collect();
        if chars.len() < 4 {
            BTreeSet::from([xxh3_64(text.as_bytes())])
        } else {
            chars
                .windows(4)
                .map(|w| xxh3_64(w.iter().collect::<String>().as_bytes()))
                .collect()
        }
    }
}
/// Unbounded reference helper for controlled benchmark inputs; production uses signature_checked.
pub fn signature(text: &str) -> [u64; 128] {
    let s = shingles(text);
    let mut out = [u64::MAX; 128];
    for (i, v) in out.iter_mut().enumerate() {
        let seed = mix(0x6d6172656b767332u64.wrapping_add(i as u64));
        for h in &s {
            *v = (*v).min(mix(h ^ seed));
        }
    }
    out
}
// Production signatures share a deterministic work budget across both trees.
// Charge input bytes before scanning/allocation, then bytes hashed +128 slot
// updates before each streamed shingle. No unbounded shingle set is constructed.
fn signature_checked(
    text: &str,
    remaining: &mut usize,
    mut check: impl FnMut() -> Result<(), DiffError>,
) -> Result<Option<[u64; 128]>, DiffError> {
    check()?;
    if text.len() > *remaining {
        *remaining = 0;
        return Ok(None);
    }
    *remaining -= text.len();
    let mut words = Vec::new();
    let mut start = None;
    for (i, c) in text.char_indices() {
        check()?;
        if c.is_whitespace() {
            if let Some(begin) = start.take() {
                words.push(&text[begin..i]);
            }
        } else if start.is_none() {
            start = Some(i);
        }
    }
    if let Some(begin) = start {
        words.push(&text[begin..]);
    }
    let mut out = [u64::MAX; 128];
    let mut add = |bytes: &[u8], remaining: &mut usize| -> Result<bool, DiffError> {
        check()?;
        let cost = bytes.len().saturating_add(128);
        if cost > *remaining {
            *remaining = 0;
            return Ok(false);
        }
        *remaining -= cost;
        let h = xxh3_64(bytes);
        for (i, v) in out.iter_mut().enumerate() {
            check()?;
            let seed = mix(0x6d6172656b767332u64.wrapping_add(i as u64));
            *v = (*v).min(mix(h ^ seed));
        }
        Ok(true)
    };
    if words.len() > 16 {
        for w in words.windows(3) {
            let len = w.iter().map(|s| s.len().saturating_add(8)).sum::<usize>();
            // Check before allocating the length-delimited shingle buffer.
            if len.saturating_add(128) > *remaining {
                *remaining = 0;
                return Ok(None);
            }
            let mut bytes = Vec::with_capacity(len);
            for word in w {
                bytes.extend_from_slice(&(word.len() as u64).to_be_bytes());
                bytes.extend_from_slice(word.as_bytes());
            }
            if !add(&bytes, remaining)? {
                return Ok(None);
            }
        }
    } else {
        let mut ring = std::collections::VecDeque::with_capacity(4);
        let mut any = false;
        for c in text.chars() {
            ring.push_back(c);
            if ring.len() == 4 {
                let mut bytes = [0u8; 16];
                let mut len = 0;
                for c in &ring {
                    len += c.encode_utf8(&mut bytes[len..]).len();
                }
                if !add(&bytes[..len], remaining)? {
                    return Ok(None);
                }
                any = true;
                ring.pop_front();
            }
        }
        if !any && !text.is_empty() && !add(text.as_bytes(), remaining)? {
            return Ok(None);
        }
    }
    Ok(Some(out))
}
fn similarity(a: &[u64; 128], b: &[u64; 128]) -> f32 {
    if a[0] == u64::MAX || b[0] == u64::MAX {
        0.0
    } else {
        a.iter().zip(b).filter(|(x, y)| x == y).count() as f32 / 128.0
    }
}
fn emit(
    x: NodeId,
    y: NodeId,
    a: &Tree,
    b: &Tree,
    m: &Matching,
    set: &mut BTreeSet<(NodeId, NodeId)>,
    cap: usize,
) -> bool {
    if a.node(x).kind != b.node(y).kind
        || m.a2b(x).is_some()
        || m.b2a(y).is_some()
        || set.contains(&(x, y))
    {
        return true;
    }
    if set.len() >= cap {
        return false;
    }
    set.insert((x, y));
    true
}
fn context(a: &Tree, b: &Tree, x: NodeId, y: NodeId, m: &Matching) -> f32 {
    let mut yes = 0.;
    let mut total = 0.;
    if let (Some(p), Some(q)) = (a.node(x).parent, b.node(y).parent) {
        total += 1.;
        if m.a2b(p) == Some(q) {
            yes += 1.;
        }
        let ax = a.node(x).ordinal as usize;
        let by = b.node(y).ordinal as usize;
        for delta in [-1isize, 1] {
            if let (Some(i), Some(j)) = (ax.checked_add_signed(delta), by.checked_add_signed(delta))
            {
                if let (Some(u), Some(v)) = (a.node(p).children.get(i), b.node(q).children.get(j)) {
                    total += 1.;
                    if m.a2b(*u) == Some(*v) {
                        yes += 1.;
                    }
                }
            }
        }
    }
    if total == 0. {
        0.
    } else {
        yes / total
    }
}
pub fn run(a: &Tree, b: &Tree, m: &mut Matching, o: &Options) -> Result<NearStats, DiffError> {
    o.cancel.check()?;
    let mut s = NearStats::default();
    let mut set = BTreeSet::new();
    let cap = o.budget.max_candidates;
    let mut sa = BTreeMap::new();
    let mut sb = BTreeMap::new();
    let mut signature_work = o.budget.max_nd;
    for x in m.unmatched_a() {
        o.cancel.check()?;
        if let Some(t) = &a.node(x).text {
            match signature_checked(&t.x, &mut signature_work, || o.cancel.check())? {
                Some(sig) => {
                    sa.insert(x, sig);
                }
                None => {
                    if !s.fallbacks.iter().any(|f| f == "near:signature_budget") {
                        s.fallbacks.push("near:signature_budget".into());
                    }
                }
            }
        }
    }
    for y in m.unmatched_b() {
        o.cancel.check()?;
        if let Some(t) = &b.node(y).text {
            match signature_checked(&t.x, &mut signature_work, || o.cancel.check())? {
                Some(sig) => {
                    sb.insert(y, sig);
                }
                None => {
                    if !s.fallbacks.iter().any(|f| f == "near:signature_budget") {
                        s.fallbacks.push("near:signature_budget".into());
                    }
                }
            }
        }
    }
    let rows = usize::from(o.bands.1).max(1);
    let bands = usize::from(o.bands.0).min(128 / rows);
    let mut buckets: BTreeMap<(u8, usize, Vec<u64>), Vec<NodeId>> = BTreeMap::new();
    for (&y, sig) in &sb {
        if sig[0] == u64::MAX {
            continue;
        }
        for band in 0..bands {
            buckets
                .entry((
                    b.node(y).kind as u8,
                    band,
                    sig[band * rows..(band + 1) * rows].to_vec(),
                ))
                .or_default()
                .push(y);
        }
    }
    'lsh: for (&x, sig) in &sa {
        for band in 0..bands {
            o.cancel.check()?;
            if let Some(ys) = buckets.get(&(
                a.node(x).kind as u8,
                band,
                sig[band * rows..(band + 1) * rows].to_vec(),
            )) {
                for &y in ys {
                    if !emit(x, y, a, b, m, &mut set, cap) {
                        s.fallbacks.push("candidate cap reached".into());
                        break 'lsh;
                    }
                }
            }
        }
    }
    // Short-leaf rescue follows matched immediate neighbours across parent boundaries.
    'rescue: for &x in sa.keys() {
        o.cancel.check()?;
        if a.node(x)
            .text
            .as_ref()
            .unwrap()
            .x
            .split_whitespace()
            .count()
            >= 8
        {
            continue;
        }
        if let Some(p) = a.node(x).parent {
            let i = a.node(x).ordinal as usize;
            let mut ys = BTreeSet::new();
            for d in [-1isize, 1] {
                if let Some(n) = i
                    .checked_add_signed(d)
                    .and_then(|j| a.node(p).children.get(j))
                {
                    if let Some(y) = m.a2b(*n) {
                        if let Some(q) = b.node(y).parent {
                            for &v in &b.node(q).children {
                                if m.b2a(v).is_none() && b.node(v).kind == a.node(x).kind {
                                    ys.insert(v);
                                    if ys.len() == 8 {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            for y in ys.into_iter().take(8) {
                if !emit(x, y, a, b, m, &mut set, cap) {
                    s.fallbacks.push("candidate cap reached".into());
                    break 'rescue;
                }
            }
        }
    }
    // Accumulate matched descendant evidence upward; no positional pairing of near containers.
    let mut overlap: BTreeMap<(NodeId, NodeId), u64> = BTreeMap::new();
    'overlap: for (x, y) in m.pairs() {
        o.cancel.check()?;
        if !a.node(x).kind.is_leaf() {
            continue;
        }
        let mut p = a.node(x).parent;
        while let Some(px) = p {
            let mut q = b.node(y).parent;
            while let Some(qy) = q {
                if a.node(px).kind == b.node(qy).kind && m.a2b(px).is_none() && m.b2a(qy).is_none()
                {
                    if !emit(px, qy, a, b, m, &mut set, cap) {
                        s.fallbacks.push("candidate cap reached".into());
                        break 'overlap;
                    }
                    *overlap.entry((px, qy)).or_default() +=
                        u64::from(a.node(x).weight.min(b.node(y).weight));
                }
                q = b.node(qy).parent;
            }
            p = a.node(px).parent;
        }
    }
    s.candidates_pairs = set.iter().copied().collect();
    s.candidates = set.len();
    let w = &o.weights;
    let total = w.text + w.children + w.context + w.format + w.size;
    let mut scored = Vec::new();
    for (x, y) in set {
        o.cancel.check()?;
        let (n, k) = (a.node(x), b.node(y));
        let max = n.weight.max(k.weight).max(1) as f32;
        let text = match (sa.get(&x), sb.get(&y)) {
            (Some(u), Some(v)) => similarity(u, v),
            _ => 0.,
        };
        let child = (*overlap.get(&(x, y)).unwrap_or(&0) as f32 / max).min(1.);
        let fmt = if n.text.as_ref().map(|t| &t.f) == k.text.as_ref().map(|t| &t.f) {
            1.
        } else {
            0.
        };
        // Reviewed calibration correction: <=16 tokens use character 4-grams.
        // Leaves have no child feature; unavailable context is not negative evidence.
        // base=(text*w_text+format*w_format+size*w_size)/(w_text+w_format+w_size)
        // score=base+(1-base)*context*w_context/(active+w_context).
        // Internal nodes normalize all non-text features (no signature exists).
        let size = n.weight.min(k.weight) as f32 / max;
        let ctx = context(a, b, x, y, m);
        let score = if n.kind.is_leaf() {
            let active = w.text + w.format + w.size;
            let base = if active > 0. {
                (w.text * text + w.format * fmt + w.size * size) / active
            } else {
                0.
            };
            let context_weight = if active + w.context > 0. {
                w.context / (active + w.context)
            } else {
                0.
            };
            (base + (1. - base) * context_weight * ctx).clamp(0., 1.)
        } else {
            ((w.children * child + w.context * ctx + w.format * fmt + w.size * size)
                / (total - w.text).max(f32::EPSILON))
            .clamp(0., 1.)
        };
        if score >= o.theta {
            scored.push((x, y, score));
        }
    }
    scored.sort_by(|u, v| {
        v.2.total_cmp(&u.2)
            .then_with(|| u.0.cmp(&v.0))
            .then_with(|| u.1.cmp(&v.1))
    });
    // Sorted exclusion is mutual-best among the remaining endpoints: the next edge
    // is maximal for each endpoint, with document order breaking equal scores.
    for (x, y, _) in scored {
        o.cancel.check()?;
        if m.a2b(x).is_none() && m.b2a(y).is_none() {
            m.set(x, y, Layer::I2);
        }
    }
    Ok(s)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn signatures_are_deterministic_and_empty_has_no_similarity() {
        let a = signature("one two three four five six seven eight");
        assert_eq!(a, signature("one two three four five six seven eight"));
        assert!(similarity(&a, &a) > 0.99);
        assert_eq!(similarity(&signature(""), &signature("")), 0.0);
    }
    #[test]
    fn candidates_are_capped_and_deterministic() {
        let a = Tree::from_json(
            &serde_json::json!({"t":"doc","c":[{"t":"sen","x":"repeat"},{"t":"sen","x":"repeat"}]}),
        )
        .unwrap();
        let mut o = Options::default();
        o.budget.max_candidates = 1;
        let mut m = Matching::new(&a, &a);
        m.set(0, 0, Layer::I3);
        let s = run(&a, &a, &mut m, &o).unwrap();
        assert_eq!(s.candidates_pairs.len(), 1);
        assert!(!s.fallbacks.is_empty());
    }
}

#[cfg(test)]
mod regression_tests {
    use super::*;
    #[test]
    fn internal_near_never_pairs_children_by_position() {
        let a=Tree::from_json(&serde_json::json!({"t":"doc","c":[{"t":"par","c":[{"t":"sen","x":"same"},{"t":"sen","x":"old"}]}]})).unwrap();
        let b=Tree::from_json(&serde_json::json!({"t":"doc","c":[{"t":"par","c":[{"t":"sen","x":"new"},{"t":"sen","x":"same"},{"t":"sen","x":"extra"}]}]})).unwrap();
        let mut m = Matching::new(&a, &b);
        crate::exact::run(&a, &b, &mut m, &crate::CancelToken::none()).unwrap();
        let o = Options::default();
        run(&a, &b, &mut m, &o).unwrap();
        assert_eq!(m.a2b(1), Some(1));
        assert_eq!(m.a2b(2), Some(3));
        assert_eq!(m.a2b(3), None);
    }
    #[test]
    fn short_sentence_rescued_via_neighbour_at_another_parent() {
        let a=Tree::from_json(&serde_json::json!({"t":"doc","c":[{"t":"par","c":[{"t":"sen","x":"anchor"},{"t":"sen","x":"old clause"}]},{"t":"par","c":[]}]})).unwrap();
        let b=Tree::from_json(&serde_json::json!({"t":"doc","c":[{"t":"par","c":[]},{"t":"par","c":[{"t":"sen","x":"anchor"},{"t":"sen","x":"new clause"}]}]})).unwrap();
        let mut m = Matching::new(&a, &b);
        crate::exact::run(&a, &b, &mut m, &crate::CancelToken::none()).unwrap();
        let s = run(&a, &b, &mut m, &Options::default()).unwrap();
        assert!(s.candidates_pairs.contains(&(3, 4)));
    }
}

#[cfg(test)]
mod showcase_diagnostic {
    use super::*;
    #[test]
    fn moved_edited_showcase_is_a_near_pair() {
        let a = Tree::from_json(
            &serde_json::from_str(include_str!("../tests/corpus/showcase/a.json")).unwrap(),
        )
        .unwrap();
        let b = Tree::from_json(
            &serde_json::from_str(include_str!("../tests/corpus/showcase/b.json")).unwrap(),
        )
        .unwrap();
        let mut m = Matching::new(&a, &b);
        crate::exact::run(&a, &b, &mut m, &crate::CancelToken::none()).unwrap();
        let s = run(&a, &b, &mut m, &Options::default()).unwrap();
        let x = a
            .nodes
            .iter()
            .position(|n| {
                n.text
                    .as_ref()
                    .is_some_and(|t| t.x.contains("shall commence"))
            })
            .unwrap() as NodeId;
        let y = b
            .nodes
            .iter()
            .position(|n| {
                n.text
                    .as_ref()
                    .is_some_and(|t| t.x.contains("may commence"))
            })
            .unwrap() as NodeId;
        assert_eq!(
            m.a2b(x),
            Some(y),
            "candidates: {:?}; signature similarity {}",
            s.candidates_pairs,
            similarity(
                &signature(&a.node(x).text.as_ref().unwrap().x),
                &signature(&b.node(y).text.as_ref().unwrap().x)
            )
        );
    }
}

#[cfg(test)]
mod exclusion_tests {
    use super::*;
    #[test]
    fn repeated_clauses_do_not_steal_an_unrelated_rewrite() {
        let make = |s: &str| {
            Tree::from_json(&serde_json::json!({"t":"doc","c":[{"t":"sen","x":"Repeated clause."},{"t":"sen","x":"Repeated clause."},{"t":"sen","x":s}]})).unwrap()
        };
        let a = make("Apples ripen in autumn.");
        let b = make("Quantum waves oscillate rapidly.");
        let mut m = Matching::new(&a, &b);
        m.set(0, 0, Layer::I3);
        m.set(2, 2, Layer::I1Exact);
        let result = run(&a, &b, &mut m, &Options::default()).unwrap();
        assert!(
            result.candidates_pairs.contains(&(3, 3)),
            "context rescue exercises the scorer"
        );
        assert_eq!(m.a2b(1), Some(1));
        assert_eq!(m.a2b(3), None);
        assert_eq!(m.b2a(3), None);
    }
    #[test]
    fn identical_empty_leaves_never_gain_lsh_candidates() {
        let a = Tree::from_json(&serde_json::json!({"t":"doc","c":[{"t":"sen","x":""}]})).unwrap();
        let mut m = Matching::new(&a, &a);
        let s = run(&a, &a, &mut m, &Options::default()).unwrap();
        assert!(s.candidates_pairs.is_empty());
    }
}

#[cfg(test)]
mod signature_budget_tests {
    use super::*;
    #[test]
    fn signature_budget_skips_large_input_and_reports_fallback() {
        let a =
            Tree::from_json(&serde_json::json!({"t":"doc","c":[{"t":"sen","x":"x".repeat(4096)}]}))
                .unwrap();
        let mut m = Matching::new(&a, &a);
        let mut o = Options::default();
        o.budget.max_nd = 64;
        let stats = run(&a, &a, &mut m, &o).unwrap();
        assert!(stats.fallbacks.iter().any(|s| s == "near:signature_budget"));
        assert!(stats.candidates_pairs.is_empty());
        assert_eq!(m.a2b(1), None);
    }
    #[test]
    fn signature_checks_cancellation_during_scan() {
        let mut checks = 0;
        let mut work = 100_000;
        let result = signature_checked(&"x".repeat(4096), &mut work, || {
            checks += 1;
            if checks == 100 {
                Err(DiffError::Cancelled)
            } else {
                Ok(())
            }
        });
        assert_eq!(result, Err(DiffError::Cancelled));
        assert_eq!(checks, 100);
    }
    #[test]
    fn bounded_signature_matches_reference_for_both_shingle_modes() {
        for text in ["This Agreement shall commence on the Effective Date.","one two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen sixteen seventeen eighteen", "🙂ab", ""] {
            let mut work=100_000;
            assert_eq!(signature_checked(text,&mut work,||Ok(())).unwrap(),Some(signature(text)));
        }
    }
}

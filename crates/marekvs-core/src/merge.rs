//! Convergent merge rules (design/02, refined).
//!
//! LWW registers (string, list, head, stream entry): higher `(hlc, origin)`
//! wins, ties keep local (identical write).
//!
//! OR elements (hash field, set member, zset member): per-element ORSWOT.
//! Every element record carries BOTH lattices:
//! * `live` — add-dots with their values (top-`MAX_LIVE_DOTS` by dot)
//! * `covered` — dots removed so far (top-`MAX_TOMB_DOTS` by dot)
//!
//! Merge is the join: `covered' = cov_a ∪ cov_b`,
//! `live' = (live_a ∪ live_b) \ covered'`. The element is dead when `live'`
//! is empty (envelope tombstone flag mirrors this). Keeping the covered set
//! on live records too is what makes the merge associative — a remove that
//! covered a dot we never held must still travel with the record.
//!
//! "Top-N of union" is commutative, associative and idempotent, so the caps
//! preserve the merge laws (a >255-way concurrent remove history per element
//! could in theory resurrect a stale add; accepted and documented).
//!
//! Element payload:
//!   [nlive u8] nlive × [origin u16][hlc u64][vlen varint][value]
//!   [ncov  u8] ncov  × [origin u16][hlc u64]
//! both lists sorted by dot descending; entry 0 of live is the visible value.

use crate::envelope::{Envelope, RecordType, TOMBSTONE};
use crate::NodeId;

pub const MAX_LIVE_DOTS: usize = 4;
pub const MAX_TOMB_DOTS: usize = 255;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Dot {
    pub hlc: u64,
    pub origin: NodeId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeOutcome {
    KeepLocal,
    TakeIncoming,
    /// Neither side equals the join; store these canonical merged bytes.
    Merged(Vec<u8>),
}

// ---------------------------------------------------------------------------
// varint helpers
// ---------------------------------------------------------------------------

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let b = (v & 0x7F) as u8;
        v >>= 7;
        if v == 0 {
            out.push(b);
            return;
        }
        out.push(b | 0x80);
    }
}

fn get_varint(buf: &[u8]) -> Option<(u64, usize)> {
    let mut v = 0u64;
    let mut shift = 0;
    for (i, &b) in buf.iter().enumerate() {
        v |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 {
            return Some((v, i + 1));
        }
        shift += 7;
        if shift > 63 {
            return None;
        }
    }
    None
}

// ---------------------------------------------------------------------------
// element payload codec
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ElementState {
    /// Sorted descending by dot; entry 0 is the visible (value-winning) add.
    pub live: Vec<(Dot, Vec<u8>)>,
    /// Sorted descending; dots covered by removes.
    pub covered: Vec<Dot>,
}

impl ElementState {
    pub fn is_dead(&self) -> bool {
        self.live.is_empty()
    }

    pub fn value(&self) -> Option<&[u8]> {
        self.live.first().map(|(_, v)| v.as_slice())
    }

    pub fn dots(&self) -> Vec<Dot> {
        self.live.iter().map(|(d, _)| *d).collect()
    }

    fn normalize(&mut self) {
        self.covered.sort_unstable_by(|a, b| b.cmp(a));
        self.covered.dedup();
        self.covered.truncate(MAX_TOMB_DOTS);
        self.live.sort_unstable_by_key(|e| std::cmp::Reverse(e.0));
        self.live.dedup_by(|a, b| a.0 == b.0);
        let cov = &self.covered;
        self.live.retain(|(d, _)| !cov.contains(d));
        self.live.truncate(MAX_LIVE_DOTS);
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(
            2 + self.live.iter().map(|(_, v)| 11 + v.len()).sum::<usize>()
                + self.covered.len() * 10,
        );
        out.push(self.live.len() as u8);
        for (dot, val) in &self.live {
            out.extend_from_slice(&dot.origin.to_be_bytes());
            out.extend_from_slice(&dot.hlc.to_be_bytes());
            put_varint(&mut out, val.len() as u64);
            out.extend_from_slice(val);
        }
        out.push(self.covered.len() as u8);
        for d in &self.covered {
            out.extend_from_slice(&d.origin.to_be_bytes());
            out.extend_from_slice(&d.hlc.to_be_bytes());
        }
        out
    }

    pub fn decode(payload: &[u8]) -> Option<ElementState> {
        let nlive = *payload.first()? as usize;
        let mut live = Vec::with_capacity(nlive);
        let mut pos = 1;
        for _ in 0..nlive {
            if payload.len() < pos + 10 {
                return None;
            }
            let origin = u16::from_be_bytes(payload[pos..pos + 2].try_into().unwrap());
            let hlc = u64::from_be_bytes(payload[pos + 2..pos + 10].try_into().unwrap());
            pos += 10;
            let (vlen, adv) = get_varint(&payload[pos..])?;
            pos += adv;
            let vlen = vlen as usize;
            if payload.len() < pos + vlen {
                return None;
            }
            live.push((Dot { hlc, origin }, payload[pos..pos + vlen].to_vec()));
            pos += vlen;
        }
        let ncov = *payload.get(pos)? as usize;
        pos += 1;
        if payload.len() < pos + ncov * 10 {
            return None;
        }
        let mut covered = Vec::with_capacity(ncov);
        for i in 0..ncov {
            let p = pos + i * 10;
            covered.push(Dot {
                origin: u16::from_be_bytes(payload[p..p + 2].try_into().unwrap()),
                hlc: u64::from_be_bytes(payload[p + 2..p + 10].try_into().unwrap()),
            });
        }
        Some(ElementState { live, covered })
    }
}

/// Canonical full record (envelope + payload) for an element state.
/// `version` is the record version carried in the envelope; canonical merges
/// use the symmetric max of both inputs' versions.
fn encode_element(
    rtype: RecordType,
    version: (u64, NodeId),
    ttl: u64,
    mut state: ElementState,
) -> Vec<u8> {
    state.normalize();
    let mut flags = (rtype as u8) << 2;
    if state.is_dead() {
        flags |= TOMBSTONE;
    }
    let env = Envelope {
        flags,
        hlc: version.0,
        origin: version.1,
        ttl_deadline_ms: ttl,
    };
    env.encode_with(&state.encode())
}

/// A fresh single-add element record.
pub fn element_add(rtype: RecordType, hlc: u64, origin: NodeId, value: &[u8]) -> Vec<u8> {
    let dot = Dot { hlc, origin };
    encode_element(
        rtype,
        (hlc, origin),
        0,
        ElementState {
            live: vec![(dot, value.to_vec())],
            covered: vec![],
        },
    )
}

/// A fresh single-add element record with a TTL deadline.
pub fn element_add_ttl(
    rtype: RecordType,
    hlc: u64,
    origin: NodeId,
    value: &[u8],
    ttl_deadline_ms: u64,
) -> Vec<u8> {
    let dot = Dot { hlc, origin };
    encode_element(
        rtype,
        (hlc, origin),
        ttl_deadline_ms,
        ElementState {
            live: vec![(dot, value.to_vec())],
            covered: vec![],
        },
    )
}

/// An element remove covering the `observed` dots.
pub fn element_remove(rtype: RecordType, hlc: u64, origin: NodeId, observed: &[Dot]) -> Vec<u8> {
    encode_element(
        rtype,
        (hlc, origin),
        0,
        ElementState {
            live: vec![],
            covered: observed.to_vec(),
        },
    )
}

/// The visible value of an element record's payload (None when dead).
pub fn element_value(payload: &[u8]) -> Option<Vec<u8>> {
    let st = ElementState::decode(payload)?;
    st.value().map(|v| v.to_vec())
}

/// All live add-dots (what a remove must "observe").
pub fn element_dots(payload: &[u8]) -> Vec<Dot> {
    ElementState::decode(payload).map_or_else(Vec::new, |s| s.dots())
}

// ---------------------------------------------------------------------------
// merge
// ---------------------------------------------------------------------------

fn merge_lww(local: &Envelope, incoming: &Envelope) -> MergeOutcome {
    if incoming.version() > local.version() {
        MergeOutcome::TakeIncoming
    } else {
        MergeOutcome::KeepLocal
    }
}

/// Merge two full stored values (envelope + payload) for the same internal
/// key. `Merged` bytes are canonical: both merge orders yield identical bytes.
pub fn merge_values(local: &[u8], incoming: &[u8]) -> MergeOutcome {
    let Some((lenv, lpay)) = Envelope::decode(local) else {
        return MergeOutcome::TakeIncoming; // local corrupt: replace
    };
    let Some((ienv, ipay)) = Envelope::decode(incoming) else {
        return MergeOutcome::KeepLocal; // incoming corrupt: ignore
    };

    let rtype = ienv.rtype();

    // PN counters (v1.1): counter ⊔ counter joins; counter vs anything else
    // (plain SET, tombstone) is LWW by envelope version — SET/DEL reset.
    if lenv.rtype() == RecordType::Counter
        && rtype == RecordType::Counter
        && !lenv.is_tombstone()
        && !ienv.is_tombstone()
    {
        return merge_counters(&lenv, lpay, &ienv, ipay, local, incoming);
    }

    // HyperLogLog registers (design/02 §HLL): a register is a 1-byte
    // monotone lattice — merge = payload max. Envelope version = symmetric
    // max (anti-entropy digests); TTL follows the version winner like
    // elements. vs tombstone/other types → LWW (head del clock provides
    // resurrection safety, same as set members).
    if lenv.rtype() == RecordType::HllRegister
        && rtype == RecordType::HllRegister
        && !lenv.is_tombstone()
        && !ienv.is_tombstone()
    {
        return merge_hll_registers(&lenv, lpay, &ienv, ipay, local, incoming);
    }

    if ienv.is_head() || lenv.is_head() || !rtype.is_or_element() {
        return merge_lww(&lenv, &ienv);
    }

    let (Some(l), Some(i)) = (ElementState::decode(lpay), ElementState::decode(ipay)) else {
        return merge_lww(&lenv, &ienv); // defensive: undecodable element
    };

    let mut joined = ElementState {
        live: l.live.iter().chain(i.live.iter()).cloned().collect(),
        covered: l.covered.iter().chain(i.covered.iter()).copied().collect(),
    };
    joined.normalize();

    let version = lenv.version().max(ienv.version());
    let ttl = if ienv.version() > lenv.version() {
        ienv.ttl_deadline_ms
    } else {
        lenv.ttl_deadline_ms
    };
    let merged = encode_element(rtype, version, ttl, joined);
    if merged == local {
        MergeOutcome::KeepLocal
    } else if merged == incoming {
        MergeOutcome::TakeIncoming
    } else {
        MergeOutcome::Merged(merged)
    }
}

/// Counter ⊔ counter: PN join on equal base versions, base-winner wholesale
/// otherwise (crate::counter). Envelope version = symmetric max so anti-
/// entropy digests converge; TTL follows the envelope winner.
fn merge_counters(
    lenv: &Envelope,
    lpay: &[u8],
    ienv: &Envelope,
    ipay: &[u8],
    local: &[u8],
    incoming: &[u8],
) -> MergeOutcome {
    use crate::counter::CounterState;
    let (Some(l), Some(i)) = (CounterState::decode(lpay), CounterState::decode(ipay)) else {
        return merge_lww(lenv, ienv); // defensive: undecodable counter
    };
    let joined = CounterState::merge(&l, &i);
    let version = lenv.version().max(ienv.version());
    let ttl = if ienv.version() > lenv.version() {
        ienv.ttl_deadline_ms
    } else {
        lenv.ttl_deadline_ms
    };
    let env = Envelope {
        flags: (RecordType::Counter as u8) << 2,
        hlc: version.0,
        origin: version.1,
        ttl_deadline_ms: ttl,
    };
    let merged = env.encode_with(&joined.encode());
    if merged == local {
        MergeOutcome::KeepLocal
    } else if merged == incoming {
        MergeOutcome::TakeIncoming
    } else {
        MergeOutcome::Merged(merged)
    }
}

/// HLL register ⊔ register: rank = max(ranks); canonical bytes.
fn merge_hll_registers(
    lenv: &Envelope,
    lpay: &[u8],
    ienv: &Envelope,
    ipay: &[u8],
    local: &[u8],
    incoming: &[u8],
) -> MergeOutcome {
    let (Some(&l_rank), Some(&i_rank)) = (lpay.first(), ipay.first()) else {
        return merge_lww(lenv, ienv); // defensive: malformed register
    };
    let _ = (local, incoming);
    // Higher rank wins with its own envelope. EQUAL ranks resolve to the
    // LOWER envelope version — also deterministic/commutative, and it makes
    // a duplicate PFADD (same rank, fresh version) a true no-op: no write,
    // no replication, no anti-entropy digest churn.
    match l_rank.cmp(&i_rank) {
        std::cmp::Ordering::Greater => MergeOutcome::KeepLocal,
        std::cmp::Ordering::Less => MergeOutcome::TakeIncoming,
        std::cmp::Ordering::Equal => {
            if lenv.version() <= ienv.version() {
                MergeOutcome::KeepLocal
            } else {
                MergeOutcome::TakeIncoming
            }
        }
    }
}

/// Apply an outcome: the value that should end up stored.
pub fn resolve<'a>(local: &'a [u8], incoming: &'a [u8], outcome: &'a MergeOutcome) -> &'a [u8] {
    match outcome {
        MergeOutcome::KeepLocal => local,
        MergeOutcome::TakeIncoming => incoming,
        MergeOutcome::Merged(m) => m,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn add(hlc: u64, origin: NodeId, val: &[u8]) -> Vec<u8> {
        element_add(RecordType::SetMember, hlc, origin, val)
    }

    fn full_merge(a: &[u8], b: &[u8]) -> Vec<u8> {
        resolve(a, b, &merge_values(a, b)).to_vec()
    }

    #[test]
    fn lww_higher_wins() {
        let old = Envelope::new(RecordType::String, 100, 1).encode_with(b"old");
        let new = Envelope::new(RecordType::String, 200, 2).encode_with(b"new");
        assert_eq!(merge_values(&old, &new), MergeOutcome::TakeIncoming);
        assert_eq!(merge_values(&new, &old), MergeOutcome::KeepLocal);
    }

    #[test]
    fn concurrent_adds_both_survive_remove_of_one() {
        let a1 = add(100, 1, b"");
        let a2 = add(90, 2, b""); // concurrent, lower hlc
        let rm = element_remove(
            RecordType::SetMember,
            150,
            3,
            &[Dot {
                hlc: 100,
                origin: 1,
            }],
        );

        let s = full_merge(&full_merge(&a1, &rm), &a2);
        let t = full_merge(&full_merge(&a1, &a2), &rm);
        let u = full_merge(&full_merge(&a2, &rm), &a1); // remove seen before a1
        assert_eq!(s, t);
        assert_eq!(s, u);
        let (env, pay) = Envelope::decode(&s).unwrap();
        assert!(!env.is_tombstone(), "a2 was never observed by the remove");
        assert_eq!(element_dots(pay), vec![Dot { hlc: 90, origin: 2 }]);
    }

    #[test]
    fn covered_add_stays_dead() {
        let a1 = add(100, 1, b"");
        let rm = element_remove(
            RecordType::SetMember,
            150,
            3,
            &[Dot {
                hlc: 100,
                origin: 1,
            }],
        );
        let s = full_merge(&rm, &a1); // stale add arrives after remove
        let (env, _) = Envelope::decode(&s).unwrap();
        assert!(env.is_tombstone());
    }

    #[test]
    fn idempotent() {
        let a1 = add(100, 1, b"x");
        let rm = element_remove(
            RecordType::SetMember,
            150,
            3,
            &[Dot {
                hlc: 100,
                origin: 1,
            }],
        );
        let m = full_merge(&a1, &rm);
        assert_eq!(full_merge(&m, &rm), m);
        assert_eq!(full_merge(&m, &a1), m);
        assert_eq!(full_merge(&m, &m), m);
    }

    #[test]
    fn element_roundtrip() {
        let st = ElementState {
            live: vec![(Dot { hlc: 9, origin: 1 }, b"v1".to_vec())],
            covered: vec![Dot { hlc: 5, origin: 0 }],
        };
        assert_eq!(ElementState::decode(&st.encode()).unwrap(), st);
    }
}

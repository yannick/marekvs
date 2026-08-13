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

/// Overwrite: covers the `observed` dots and installs a single fresh add-dot
/// in ONE record — a SET both replaces the value and prevents resurrection of
/// the adds it observed (design/16 JSON path assignment).
pub fn element_set(
    rtype: RecordType,
    hlc: u64,
    origin: NodeId,
    value: &[u8],
    observed: &[Dot],
) -> Vec<u8> {
    let dot = Dot { hlc, origin };
    encode_element(
        rtype,
        (hlc, origin),
        0,
        ElementState {
            live: vec![(dot, value.to_vec())],
            covered: observed.to_vec(),
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

/// A live element value read as PN-counter state.
///
/// Two guards, because a false positive here would render a user's opaque
/// value as a number:
///
/// * a **canonical round-trip**, not merely a successful decode —
///   `CounterState::decode` tolerates trailing bytes, so many byte strings
///   decode without being counters;
/// * **at least one slot**. `CounterState::bump` always inserts the bumping
///   node's slot (even for `HINCRBY … 0`), so every counter this code writes
///   has one; the slotless canonical form is exactly 19 zero bytes, which a
///   user could plausibly store as a value and which no write path produces.
///
/// What remains is deliberate forgery: a value that reproduces the canonical
/// layout including sorted slot ids. The only consequence is that the forger's
/// own field reads back as a number.
fn as_counter(value: &[u8]) -> Option<crate::counter::CounterState> {
    let c = crate::counter::CounterState::decode(value)?;
    (!c.slots.is_empty() && c.encode() == value).then_some(c)
}

/// Fold every live dot's counter state into one (T2-12).
///
/// This is the heart of counter-valued hash fields, and the reason
/// [`element_value`] alone is wrong for them. A hash field is an OR-element,
/// so two concurrent HINCRBYs on different nodes create two *different dots*
/// and BOTH survive the merge — but `element_value` returns `live.first()`,
/// showing one and hiding the other. The lost-increment bug would simply move
/// from LWW to dot-selection.
///
/// Folding with [`CounterState::merge`](crate::counter::CounterState::merge)
/// recovers both: each node's increment lives in its own slot and the join is
/// a pointwise max, so `A+1` and `B+1` applied concurrently yield `+2`.
/// Both nodes hold the identical `live` set after merging, so both fold to the
/// same number — convergence comes from the fold being a pure function of
/// converged bytes.
///
/// Returns `None` when no live dot carries counter state: that is a field a
/// plain HSET has taken over (HSET covers the observed dots, which is exactly
/// Redis's "SET resets the counter"), and the caller falls back to the raw
/// value.
pub fn counter_field_state(payload: &[u8]) -> Option<crate::counter::CounterState> {
    let st = ElementState::decode(payload)?;
    let mut folded: Option<crate::counter::CounterState> = None;
    for (_, v) in &st.live {
        let Some(c) = as_counter(v) else { continue };
        folded = Some(match folded {
            None => c,
            Some(acc) => crate::counter::CounterState::merge(&acc, &c),
        });
    }
    folded
}

pub fn counter_field_value(payload: &[u8]) -> Option<i64> {
    counter_field_state(payload)?.value()
}

/// Keep only the newest live entry per origin.
///
/// `live` is sorted by dot descending, so the first entry seen for an origin
/// is its highest-hlc one. This is what keeps a counter field bounded: each
/// node writes a fresh dot per increment (so a concurrent HDEL that covered
/// the old dot does not silently swallow the new one), and the superseded
/// entries are dropped here instead of being pushed onto `covered`.
///
/// Sound because a node only ever publishes its **own** monotonically growing
/// slot: its newest entry subsumes every older one it wrote, so discarding
/// them loses nothing. Peers' entries are untouched.
fn collapse_per_origin(st: &mut ElementState) {
    let mut seen: Vec<NodeId> = Vec::new();
    st.live.retain(|(d, _)| {
        if seen.contains(&d.origin) {
            false
        } else {
            seen.push(d.origin);
            true
        }
    });
}

/// Write a counter-field record: a fresh dot for this node, collapsed so at
/// most one entry per node survives.
///
/// The obvious implementation — `element_set` covering the dots it observed —
/// is wrong for a counter. A counter is incremented unboundedly often, so
/// every increment would push a dot onto `covered`; that list is capped at
/// [`MAX_TOMB_DOTS`] (255) and truncates the oldest, so a hot counter would
/// sit permanently at the cap. That turns the documented ">255-way remove
/// history can resurrect a stale add" hazard from astronomically unlikely
/// into routine, and bloats every record to ~2.6 KB besides.
///
/// Reusing one dot per node instead does not work either: the merge dedups by
/// exact dot and cannot order two different values on the same dot, so the
/// stale one wins and the increment is lost.
///
/// So: fresh dot, then collapse per origin. `live` holds at most one entry per
/// node that ever incremented, `covered` never grows from incrementing, and a
/// concurrent HDEL stays add-wins exactly like SADD.
///
/// `mine` carries only this node's slot plus the agreed base, because
/// [`counter_field_value`] folds with `CounterState::merge` — a pointwise max
/// — so each node's slot is recovered from its own entry and restating peers'
/// slots would bloat every record to O(nodes) without adding information.
pub fn counter_field_set(hlc: u64, origin: NodeId, prior: Option<&[u8]>, mine: &[u8]) -> Vec<u8> {
    let mut st = prior
        .and_then(ElementState::decode)
        .unwrap_or(ElementState {
            live: Vec::new(),
            covered: Vec::new(),
        });
    st.live.push((Dot { hlc, origin }, mine.to_vec()));
    st.normalize();
    collapse_per_origin(&mut st);
    encode_element(RecordType::CounterField, (hlc, origin), 0, st)
}

/// The value a client should see for an element.
///
/// Use this instead of [`element_value`] anywhere a hash field is rendered:
/// for `CounterField` it folds every live dot (see [`counter_field_value`]),
/// and for everything else it is `element_value`.
pub fn element_display_value(rtype: RecordType, payload: &[u8]) -> Option<Vec<u8>> {
    if rtype == RecordType::CounterField {
        if let Some(n) = counter_field_value(payload) {
            return Some(n.to_string().into_bytes());
        }
    }
    element_value(payload)
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

    // The merged record's rtype must not depend on WHICH side is "incoming",
    // or the two directions of the same merge encode different bytes and the
    // anti-entropy digests never converge. Taking the numerically greater type
    // is commutative and associative, and makes CounterField "sticky" over
    // HashField: once a field is a counter, a concurrent plain HSET does not
    // silently downgrade the record's type on one node only.
    let rtype = RecordType::from_bits((lenv.rtype() as u8).max(rtype as u8));

    let mut joined = ElementState {
        live: l.live.iter().chain(i.live.iter()).cloned().collect(),
        covered: l.covered.iter().chain(i.covered.iter()).copied().collect(),
    };
    joined.normalize();
    if rtype == RecordType::CounterField {
        // One entry per node; see collapse_per_origin. Applied on both merge
        // directions, so the encoded bytes stay identical either way.
        collapse_per_origin(&mut joined);
    }

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
    fn set_replaces_observed_add_in_any_order() {
        let a1 = add(100, 1, b"old");
        let set = element_set(
            RecordType::SetMember,
            150,
            2,
            b"new",
            &[Dot {
                hlc: 100,
                origin: 1,
            }],
        );
        let s = full_merge(&a1, &set);
        let t = full_merge(&set, &a1);
        assert_eq!(s, t);
        let (env, pay) = Envelope::decode(&s).unwrap();
        assert!(!env.is_tombstone());
        assert_eq!(element_value(pay).unwrap(), b"new");
        assert_eq!(
            element_dots(pay),
            vec![Dot {
                hlc: 150,
                origin: 2
            }]
        );
        // the observed add must stay covered even if it re-arrives later
        assert_eq!(full_merge(&s, &a1), s);
    }

    #[test]
    fn set_keeps_unobserved_concurrent_add_alive() {
        let a1 = add(100, 1, b"old");
        let concurrent = add(140, 3, b"other");
        let set = element_set(
            RecordType::SetMember,
            150,
            2,
            b"new",
            &[Dot {
                hlc: 100,
                origin: 1,
            }],
        );
        let s = full_merge(&full_merge(&a1, &set), &concurrent);
        let t = full_merge(&full_merge(&concurrent, &a1), &set);
        assert_eq!(s, t);
        let (env, pay) = Envelope::decode(&s).unwrap();
        assert!(!env.is_tombstone());
        // both survive; the set's dot (150,2) is highest and thus visible
        assert_eq!(element_value(pay).unwrap(), b"new");
        assert_eq!(
            element_dots(pay),
            vec![
                Dot {
                    hlc: 150,
                    origin: 2
                },
                Dot {
                    hlc: 140,
                    origin: 3
                }
            ]
        );
    }

    #[test]
    fn set_with_no_observed_dots_is_plain_add() {
        let set = element_set(RecordType::HashField, 100, 1, b"v", &[]);
        let add = element_add(RecordType::HashField, 100, 1, b"v");
        assert_eq!(set, add);
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

/// T2-12: counter-valued hash fields.
///
/// The invariant under test throughout: a hash field is an OR-element (so
/// HDEL/DEL keep working) whose *value* is a PN-counter lattice, and the
/// visible number is the fold over every live dot — never `live.first()`.
#[cfg(test)]
mod counter_field_tests {
    use super::*;
    use crate::counter::CounterState;

    /// One node's increment, as HINCRBY writes it: fold what is live, bump
    /// our own slot, cover the dots we observed.
    fn incr(payload: Option<&[u8]>, hlc: u64, node: NodeId, delta: i64) -> Vec<u8> {
        let observed = payload.map(element_dots).unwrap_or_default();
        let mut st = payload
            .and_then(|p| {
                let mut acc: Option<CounterState> = None;
                for (_, v) in ElementState::decode(p)?.live {
                    if let Some(c) = as_counter(&v) {
                        acc = Some(match acc {
                            None => c,
                            Some(a) => CounterState::merge(&a, &c),
                        });
                    }
                }
                acc
            })
            .unwrap_or_default();
        st.bump(node, delta);
        element_set(RecordType::CounterField, hlc, node, &st.encode(), &observed)
    }

    fn value(rec: &[u8]) -> Option<i64> {
        let (_, pay) = Envelope::decode(rec)?;
        counter_field_value(pay)
    }

    fn merged(a: &[u8], b: &[u8]) -> Vec<u8> {
        match merge_values(a, b) {
            MergeOutcome::TakeIncoming => b.to_vec(),
            MergeOutcome::KeepLocal => a.to_vec(),
            MergeOutcome::Merged(m) => m,
        }
    }

    /// THE bug T2-12 exists to fix: two nodes each +1 on the same field,
    /// concurrently. Both increments must survive. Before the fold, the
    /// OR-element kept both dots but showed only one → the field read 1.
    #[test]
    fn concurrent_increments_on_two_nodes_both_survive() {
        let a = incr(None, 100, 1, 1);
        let b = incr(None, 101, 2, 1);
        assert_eq!(value(&a), Some(1));
        assert_eq!(value(&b), Some(1));
        assert_eq!(value(&merged(&a, &b)), Some(2), "an increment was lost");
    }

    /// Merge must be commutative *in bytes*, not just in value — the two
    /// directions run on different nodes and anti-entropy hashes the bytes.
    #[test]
    fn merge_is_byte_commutative() {
        let a = incr(None, 100, 1, 5);
        let b = incr(None, 101, 2, 7);
        assert_eq!(merged(&a, &b), merged(&b, &a));
        assert_eq!(value(&merged(&a, &b)), Some(12));
    }

    #[test]
    fn merge_is_associative_and_idempotent() {
        let a = incr(None, 100, 1, 1);
        let b = incr(None, 101, 2, 2);
        let c = incr(None, 102, 3, 4);
        let left = merged(&merged(&a, &b), &c);
        let right = merged(&a, &merged(&b, &c));
        assert_eq!(left, right, "not associative");
        assert_eq!(value(&left), Some(7));
        assert_eq!(merged(&left, &left), left, "not idempotent");
        assert_eq!(merged(&left, &a), left, "re-merging an ancestor changed it");
    }

    /// Repeated increments on one node must not grow the live set: each new
    /// add covers the dots it observed, so the field stays one entry.
    #[test]
    fn repeated_same_node_increments_do_not_accumulate_dots() {
        let mut rec = incr(None, 100, 1, 1);
        for hlc in 101..140 {
            rec = incr(Some(&Envelope::decode(&rec).unwrap().1), hlc, 1, 1);
        }
        let (_, pay) = Envelope::decode(&rec).unwrap();
        let st = ElementState::decode(pay).unwrap();
        assert_eq!(st.live.len(), 1, "live dots accumulated per increment");
        assert_eq!(counter_field_value(pay), Some(40));
    }

    /// Interleaved traffic: each node increments locally, then they sync.
    /// Every acked increment must be in the total.
    #[test]
    fn interleaved_rounds_converge_to_the_full_sum() {
        let mut a = incr(None, 100, 1, 1);
        let mut b = incr(None, 100, 2, 1);
        let mut hlc = 200;
        for _ in 0..10 {
            a = incr(Some(&Envelope::decode(&a).unwrap().1), hlc, 1, 1);
            b = incr(Some(&Envelope::decode(&b).unwrap().1), hlc + 1, 2, 1);
            hlc += 2;
            let m = merged(&a, &b);
            assert_eq!(m, merged(&b, &a));
            a = m.clone();
            b = m;
        }
        assert_eq!(value(&a), Some(22), "some increments were lost");
        assert_eq!(a, b, "replicas diverged");
    }

    /// HSET over a counter resets it — it covers the counter's dots, which is
    /// the element-level form of Redis's "SET resets the counter".
    #[test]
    fn hset_over_a_counter_replaces_it() {
        let c = incr(None, 100, 1, 41);
        let (_, pay) = Envelope::decode(&c).unwrap();
        let observed = element_dots(pay);
        let set = element_set(RecordType::HashField, 200, 1, b"hello", &observed);
        let m = merged(&c, &set);
        let (env, mpay) = Envelope::decode(&m).unwrap();
        assert_eq!(
            counter_field_value(mpay),
            None,
            "the counter should be gone once its dots are covered"
        );
        assert_eq!(
            element_display_value(env.rtype(), mpay),
            Some(b"hello".to_vec())
        );
    }

    /// A plain HSET that did NOT observe the counter is concurrent with it.
    /// The record type is sticky (counter wins), so both nodes agree, and the
    /// counter — the value with a defined join — is what shows.
    #[test]
    fn a_concurrent_hset_does_not_diverge_the_type() {
        let c = incr(None, 100, 1, 9);
        let set = element_set(RecordType::HashField, 101, 2, b"hello", &[]);
        let ab = merged(&c, &set);
        let ba = merged(&set, &c);
        assert_eq!(ab, ba, "merged bytes depend on merge direction");
        let (env, pay) = Envelope::decode(&ab).unwrap();
        assert_eq!(env.rtype(), RecordType::CounterField);
        assert_eq!(counter_field_value(pay), Some(9));
    }

    /// Arbitrary user bytes must never be mistaken for counter state — the
    /// fold requires a canonical round-trip, not just a successful decode.
    #[test]
    fn opaque_values_are_not_read_as_counters() {
        for junk in [
            &b"x"[..],
            &[0u8; 19][..],
            &[7u8; 40][..],
            &b"0123456789012345678901234567890123456789"[..],
        ] {
            let rec = element_set(RecordType::CounterField, 100, 1, junk, &[]);
            let (env, pay) = Envelope::decode(&rec).unwrap();
            assert_eq!(
                counter_field_value(pay),
                None,
                "junk {junk:?} decoded as a counter"
            );
            // and it still renders as the raw value
            assert_eq!(element_display_value(env.rtype(), pay), Some(junk.to_vec()));
        }
    }

    /// Negative deltas (HINCRBY -n) ride the counter's neg slots.
    #[test]
    fn decrements_survive_concurrently_too() {
        let a = incr(None, 100, 1, 10);
        let b = incr(None, 101, 2, -3);
        assert_eq!(value(&merged(&a, &b)), Some(7));
    }

    /// HDEL still works: the field is an OR-element regardless of its value.
    #[test]
    fn a_counter_field_can_still_be_removed() {
        let c = incr(None, 100, 1, 5);
        let (_, pay) = Envelope::decode(&c).unwrap();
        let rm = element_remove(RecordType::CounterField, 200, 1, &element_dots(pay));
        let m = merged(&c, &rm);
        let (_, mpay) = Envelope::decode(&m).unwrap();
        assert!(
            ElementState::decode(mpay).unwrap().is_dead(),
            "HDEL must remove a counter field"
        );
    }
}

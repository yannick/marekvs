//! PN-counter records (v1.1) — convergent Redis counters.
//!
//! A counter is a hybrid register/CRDT:
//! * a **base**: an LWW register `(base_hlc, base_origin, base_value)` — the
//!   value established by the last explicit SET (or 0 for a fresh counter);
//! * per-node **delta slots** `(pos, neg)` — increments/decrements applied
//!   since that base. A node only ever grows its own slot, so slots are
//!   monotonic and merge by **pointwise max** (a PN-counter join).
//!
//! Value = base + Σpos − Σneg.
//!
//! Merge of two counters:
//! * same base version → join the slot maps pointwise (all concurrent
//!   increments survive — the "stable increments" guarantee);
//! * different base versions → the higher base wins wholesale (its slots
//!   replace the loser's). This encodes Redis "SET resets the counter":
//!   increments racing an explicit SET are dropped by design.
//!
//! Interop with plain strings on the same key is LWW by envelope version
//! (a fresh SET beats an older counter; a fresh INCR converts the string it
//! read into its counter base, so it beats the string it was based on).
//!
//! Payload layout:
//! ```text
//! [base_hlc u64 BE][base_origin u16 BE][base i64 BE]
//! [n u8] n × [node u16 BE][pos u64 BE][neg u64 BE]
//! ```
//! Slots sorted by node id ascending (canonical bytes).

use crate::NodeId;

pub const MAX_SLOTS: usize = 255;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CounterState {
    pub base_hlc: u64,
    pub base_origin: NodeId,
    pub base: i64,
    /// Sorted ascending by node id; one slot per node that ever incremented.
    pub slots: Vec<(NodeId, u64, u64)>, // (node, pos, neg)
}

impl CounterState {
    /// Fresh counter on top of a read value (or absent key: version (0,0),
    /// base 0 — every node starts from the identical base, so first-touch
    /// increments from different nodes join instead of racing).
    pub fn on_base(base_hlc: u64, base_origin: NodeId, base: i64) -> CounterState {
        CounterState {
            base_hlc,
            base_origin,
            base,
            slots: Vec::new(),
        }
    }

    pub fn base_version(&self) -> (u64, NodeId) {
        (self.base_hlc, self.base_origin)
    }

    /// Current value; None on i64 overflow (caller surfaces a Redis error,
    /// but stored state stays intact — overflow is a read-side condition).
    pub fn value(&self) -> Option<i64> {
        let mut v = self.base as i128;
        for (_, pos, neg) in &self.slots {
            v += *pos as i128;
            v -= *neg as i128;
        }
        i64::try_from(v).ok()
    }

    /// Apply a local increment (negative delta = decrement) to `node`'s slot.
    pub fn bump(&mut self, node: NodeId, delta: i64) {
        let idx = match self.slots.binary_search_by_key(&node, |s| s.0) {
            Ok(i) => i,
            Err(i) => {
                self.slots.insert(i, (node, 0, 0));
                i
            }
        };
        if delta >= 0 {
            self.slots[idx].1 = self.slots[idx].1.saturating_add(delta as u64);
        } else {
            self.slots[idx].2 = self.slots[idx].2.saturating_add(delta.unsigned_abs());
        }
    }

    /// PN join with `other` (must share the base version): pointwise max.
    fn join_slots(&mut self, other: &CounterState) {
        for &(node, pos, neg) in &other.slots {
            match self.slots.binary_search_by_key(&node, |s| s.0) {
                Ok(i) => {
                    self.slots[i].1 = self.slots[i].1.max(pos);
                    self.slots[i].2 = self.slots[i].2.max(neg);
                }
                Err(i) => self.slots.insert(i, (node, pos, neg)),
            }
        }
        self.slots.truncate(MAX_SLOTS);
    }

    /// Canonical merge of two counter states.
    pub fn merge(a: &CounterState, b: &CounterState) -> CounterState {
        use std::cmp::Ordering;
        match a.base_version().cmp(&b.base_version()) {
            Ordering::Greater => a.clone(),
            Ordering::Less => b.clone(),
            Ordering::Equal => {
                let mut out = a.clone();
                out.join_slots(b);
                out
            }
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(19 + self.slots.len() * 18);
        out.extend_from_slice(&self.base_hlc.to_be_bytes());
        out.extend_from_slice(&self.base_origin.to_be_bytes());
        out.extend_from_slice(&self.base.to_be_bytes());
        out.push(self.slots.len() as u8);
        for (node, pos, neg) in &self.slots {
            out.extend_from_slice(&node.to_be_bytes());
            out.extend_from_slice(&pos.to_be_bytes());
            out.extend_from_slice(&neg.to_be_bytes());
        }
        out
    }

    pub fn decode(payload: &[u8]) -> Option<CounterState> {
        if payload.len() < 19 {
            return None;
        }
        let base_hlc = u64::from_be_bytes(payload[0..8].try_into().unwrap());
        let base_origin = u16::from_be_bytes(payload[8..10].try_into().unwrap());
        let base = i64::from_be_bytes(payload[10..18].try_into().unwrap());
        let n = payload[18] as usize;
        if payload.len() < 19 + n * 18 {
            return None;
        }
        let mut slots = Vec::with_capacity(n);
        for i in 0..n {
            let p = 19 + i * 18;
            slots.push((
                u16::from_be_bytes(payload[p..p + 2].try_into().unwrap()),
                u64::from_be_bytes(payload[p + 2..p + 10].try_into().unwrap()),
                u64::from_be_bytes(payload[p + 10..p + 18].try_into().unwrap()),
            ));
        }
        Some(CounterState {
            base_hlc,
            base_origin,
            base,
            slots,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let mut c = CounterState::on_base(42, 1, 100);
        c.bump(3, 5);
        c.bump(1, -2);
        assert_eq!(CounterState::decode(&c.encode()).unwrap(), c);
        assert_eq!(c.value(), Some(103));
    }

    #[test]
    fn concurrent_increments_survive() {
        let base = CounterState::on_base(0, 0, 0);
        let mut a = base.clone();
        let mut b = base.clone();
        a.bump(1, 10);
        b.bump(2, 5);
        b.bump(2, -1);
        let m1 = CounterState::merge(&a, &b);
        let m2 = CounterState::merge(&b, &a);
        assert_eq!(m1, m2);
        assert_eq!(m1.value(), Some(14));
    }

    #[test]
    fn newer_base_resets() {
        let mut old = CounterState::on_base(10, 0, 0);
        old.bump(1, 100);
        let fresh = CounterState::on_base(20, 2, 7); // explicit SET
        let m = CounterState::merge(&old, &fresh);
        assert_eq!(m.value(), Some(7), "SET resets: racing increments drop");
    }

    #[test]
    fn idempotent_join() {
        let mut a = CounterState::on_base(1, 1, 0);
        a.bump(1, 3);
        let m = CounterState::merge(&a, &a);
        assert_eq!(m, a);
        assert_eq!(m.value(), Some(3));
    }
}

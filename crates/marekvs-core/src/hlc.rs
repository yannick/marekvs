//! Hybrid logical clock (Kulkarni HLC) packed in a u64:
//! `[48-bit physical ms since Unix epoch | 16-bit logical counter]`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Remote timestamps more than this far ahead of the local wall clock are
/// clamped (design/05, `max_clock_drift`).
pub const MAX_CLOCK_DRIFT_MS: u64 = 5_000;

#[inline]
pub fn hlc_pack(phys_ms: u64, logical: u16) -> u64 {
    (phys_ms << 16) | logical as u64
}

#[inline]
pub fn hlc_phys_ms(hlc: u64) -> u64 {
    hlc >> 16
}

fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_millis() as u64
}

/// Process-wide clock. One instance per node, shared by all shards.
#[derive(Debug, Default)]
pub struct Hlc(AtomicU64);

impl Hlc {
    pub fn new() -> Self {
        Self(AtomicU64::new(hlc_pack(wall_ms(), 0)))
    }

    /// Timestamp for a local event (send rule): `max(prev + 1, wall << 16)`.
    pub fn now(&self) -> u64 {
        let wall = hlc_pack(wall_ms(), 0);
        self.0
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |prev| {
                Some(prev.wrapping_add(1).max(wall))
            })
            .map(|prev| prev.wrapping_add(1).max(wall))
            .unwrap()
    }

    /// Receive rule: `max(local, remote) + 1`, with remote clamped to
    /// `wall + MAX_CLOCK_DRIFT_MS` to bound damage from a skewed peer.
    /// Returns the clamped remote value actually observed.
    pub fn observe(&self, remote: u64) -> u64 {
        let limit = hlc_pack(wall_ms() + MAX_CLOCK_DRIFT_MS, 0);
        let remote = remote.min(limit);
        self.0
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |prev| {
                Some(prev.max(remote).wrapping_add(1))
            })
            .ok();
        remote
    }

    /// Whether a remote timestamp would be clamped (callers log this loudly).
    pub fn is_drifted(&self, remote: u64) -> bool {
        hlc_phys_ms(remote) > wall_ms() + MAX_CLOCK_DRIFT_MS
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monotonic_local() {
        let c = Hlc::new();
        let mut prev = 0;
        for _ in 0..10_000 {
            let t = c.now();
            assert!(t > prev);
            prev = t;
        }
    }

    #[test]
    fn observe_advances_past_remote() {
        let c = Hlc::new();
        let remote = c.now() + (10 << 16); // 10ms ahead, within drift
        c.observe(remote);
        assert!(c.now() > remote);
    }

    #[test]
    fn observe_clamps_far_future() {
        let c = Hlc::new();
        let remote = hlc_pack(wall_ms() + 60_000, 0);
        assert!(c.is_drifted(remote));
        let seen = c.observe(remote);
        assert!(hlc_phys_ms(seen) <= wall_ms() + MAX_CLOCK_DRIFT_MS + 1);
    }
}

//! Bounded replication ring (design/04). ondaDB commit hooks append; per-peer
//! sender cursors read. Overrun ⇒ the cursor jumps to the tail and the gap is
//! healed by anti-entropy (no unbounded hint queues).

use std::collections::VecDeque;
use std::sync::Arc;

use marekvs_proto::ReplOp;
use parking_lot::Mutex;
use tokio::sync::Notify;

pub const RING_MAX_BYTES: usize = 128 * 1024 * 1024;
pub const RING_MAX_OPS: usize = 262_144;

#[derive(Debug, Clone)]
pub struct RingEntry {
    pub seq: u64,
    pub origin: u16,
    pub op: ReplOp,
}

struct Inner {
    buf: VecDeque<RingEntry>,
    bytes: usize,
    /// Sequence of the first entry ever dropped past each cursor position is
    /// implied by `first_seq()`; readers detect gaps themselves.
    next_local_seq: u64,
}

pub struct Ring {
    inner: Mutex<Inner>,
    pub notify: Notify,
    /// True when this node is CONFIGURED standalone (no seeds, N=1) — a
    /// static fact, unlike connectivity. The commit hook skips ring
    /// buffering only for standalone nodes that still see no other members.
    /// Gating on runtime connectivity instead (an earlier attempt) raced the
    /// boot window: writes accepted before gossip merged / peers connected
    /// were silently dropped from the push path and had to wait for
    /// anti-entropy — the ring exists precisely to decouple commit from
    /// connect, so cluster-configured nodes always buffer.
    pub standalone_cfg: std::sync::atomic::AtomicBool,
    /// Cluster-member count from the current gossip view (covers the edge of
    /// a standalone-configured node that later gets joined by peers).
    pub members: std::sync::atomic::AtomicUsize,
}

impl Ring {
    pub fn buffering_needed(&self) -> bool {
        !self
            .standalone_cfg
            .load(std::sync::atomic::Ordering::Relaxed)
            || self.members.load(std::sync::atomic::Ordering::Relaxed) > 1
    }
}

impl Ring {
    pub fn new() -> Arc<Ring> {
        Self::new_starting_at(1)
    }

    /// Sequence numbers MUST be strictly increasing across process restarts:
    /// consumers persist "applied up to seq S per origin" and resume with
    /// ResumeFrom{S}. A ring restarting at 1 makes every stale cursor S look
    /// "caught up" (cursor >= last), so the pump silently ships NOTHING this
    /// node accepts until seq passes S again — acked writes strand on the
    /// origin (chaos finding: crash_restart el-3600). The caller seeds the
    /// start from a persisted high-water mark plus a jump that covers the
    /// unpersisted tail.
    pub fn new_starting_at(first_seq: u64) -> Arc<Ring> {
        Arc::new(Ring {
            inner: Mutex::new(Inner {
                buf: VecDeque::new(),
                bytes: 0,
                next_local_seq: first_seq.max(1),
            }),
            notify: Notify::new(),
            standalone_cfg: std::sync::atomic::AtomicBool::new(false),
            members: std::sync::atomic::AtomicUsize::new(1),
        })
    }

    /// Append ops committed locally (or applied from a remote origin — the
    /// origin rides along so senders can route correctly).
    pub fn push(&self, origin: u16, seq_hint: Option<u64>, ops: Vec<ReplOp>) {
        let mut g = self.inner.lock();
        for (i, op) in ops.into_iter().enumerate() {
            let seq = match seq_hint {
                Some(s) => s + i as u64,
                None => {
                    let s = g.next_local_seq;
                    g.next_local_seq += 1;
                    s
                }
            };
            g.next_local_seq = g.next_local_seq.max(seq + 1);
            g.bytes += op.ikey.len() + op.value.len() + 32;
            g.buf.push_back(RingEntry { seq, origin, op });
        }
        while g.bytes > RING_MAX_BYTES || g.buf.len() > RING_MAX_OPS {
            if let Some(e) = g.buf.pop_front() {
                g.bytes -= e.op.ikey.len() + e.op.value.len() + 32;
            } else {
                break;
            }
        }
        drop(g);
        self.notify.notify_waiters();
    }

    /// Read up to `max` entries with seq > `after`. Returns (entries, gap):
    /// gap = true when `after` has fallen off the tail (repair via AE).
    pub fn read_after(&self, after: u64, max: usize) -> (Vec<RingEntry>, bool) {
        let g = self.inner.lock();
        let first = g.buf.front().map(|e| e.seq).unwrap_or(g.next_local_seq);
        let gap = after + 1 < first;
        let out: Vec<RingEntry> = g
            .buf
            .iter()
            .filter(|e| e.seq > after)
            .take(max)
            .cloned()
            .collect();
        (out, gap)
    }

    /// Current occupancy: (ops, bytes) — for the metrics gauges.
    pub fn occupancy(&self) -> (usize, usize) {
        let g = self.inner.lock();
        (g.buf.len(), g.bytes)
    }

    pub fn last_seq(&self) -> u64 {
        let g = self.inner.lock();
        g.buf.back().map(|e| e.seq).unwrap_or(g.next_local_seq - 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn op(n: u8) -> ReplOp {
        ReplOp {
            ikey: vec![n],
            value: vec![n; 4],
        }
    }

    #[test]
    fn push_read() {
        let r = Ring::new();
        r.push(1, None, vec![op(1), op(2)]);
        let (got, gap) = r.read_after(0, 10);
        assert!(!gap);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].seq, 1);
        let (got, _) = r.read_after(1, 10);
        assert_eq!(got.len(), 1);
    }

    #[test]
    fn gap_detection() {
        let r = Ring::new();
        // Overflow by op count
        for i in 0..(RING_MAX_OPS + 10) {
            r.push(1, None, vec![op((i % 250) as u8)]);
        }
        let (_, gap) = r.read_after(0, 1);
        assert!(gap, "cursor at 0 must be reported as gapped");
        let last = r.last_seq();
        let (got, gap) = r.read_after(last - 1, 10);
        assert!(!gap);
        assert_eq!(got.len(), 1);
    }
}

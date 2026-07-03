//! Rendezvous (HRW) placement (design/04 §Placement).
//!
//! `score(node, pid) = xxh3_64(node_le_bytes || pid_le_bytes)`;
//! owners = top-N by score among data-owning members. Pure function of the
//! membership view — no ring state, minimal churn on join/leave.

use marekvs_core::ikey::Pid;
use marekvs_core::NodeId;
use xxhash_rust::xxh3::xxh3_64;

#[inline]
pub fn score(node: NodeId, pid: Pid) -> u64 {
    let mut buf = [0u8; 4];
    buf[..2].copy_from_slice(&node.to_le_bytes());
    buf[2..].copy_from_slice(&pid.to_le_bytes());
    xxh3_64(&buf)
}

/// Top-N owners of `pid`. `candidates` = (node, is_active); Leaving nodes
/// (is_active = false) still own data but rank below no one — they are
/// filtered only when computing NEW ownership in the caller when needed.
pub fn owners_for(candidates: &[(NodeId, bool)], pid: Pid, n: usize) -> Vec<NodeId> {
    let mut scored: Vec<(u64, NodeId)> = candidates
        .iter()
        .map(|(id, _)| (score(*id, pid), *id))
        .collect();
    scored.sort_unstable_by(|a, b| b.cmp(a));
    scored.truncate(n);
    scored.into_iter().map(|(_, id)| id).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cands(ids: &[NodeId]) -> Vec<(NodeId, bool)> {
        ids.iter().map(|i| (*i, true)).collect()
    }

    #[test]
    fn deterministic_and_distinct() {
        let c = cands(&[0, 1, 2, 3, 4]);
        for pid in 0..64 {
            let a = owners_for(&c, pid, 3);
            let b = owners_for(&c, pid, 3);
            assert_eq!(a, b);
            assert_eq!(a.len(), 3);
            let mut d = a.clone();
            d.dedup();
            assert_eq!(d.len(), 3, "owners must be distinct");
        }
    }

    #[test]
    fn minimal_disruption_on_join() {
        // Adding a node must never change the relative order of survivors:
        // a partition's owner set changes only by the newcomer displacing
        // the lowest-ranked owner.
        let before = cands(&[0, 1, 2, 3]);
        let after = cands(&[0, 1, 2, 3, 4]);
        let mut moved = 0;
        for pid in 0..4096u16 {
            let a = owners_for(&before, pid, 3);
            let b = owners_for(&after, pid, 3);
            let stolen: Vec<_> = a.iter().filter(|x| !b.contains(x)).collect();
            assert!(stolen.len() <= 1, "join may displace at most one owner");
            if !stolen.is_empty() {
                assert!(b.contains(&4));
                moved += 1;
            }
        }
        // Newcomer steals roughly 3/5 of partitions' one slot (top-3-of-5);
        // sanity-check the spread is neither zero nor everything.
        assert!(moved > 1500 && moved < 3500, "moved={moved}");
    }

    #[test]
    fn balanced_distribution() {
        let c = cands(&[0, 1, 2, 3, 4, 5, 6, 7]);
        let mut counts = [0usize; 8];
        for pid in 0..4096u16 {
            for id in owners_for(&c, pid, 3) {
                counts[id as usize] += 1;
            }
        }
        let expected = 4096 * 3 / 8;
        for (id, n) in counts.iter().enumerate() {
            let dev = (*n as f64 - expected as f64).abs() / expected as f64;
            assert!(dev < 0.15, "node {id} owns {n}, expected ~{expected}");
        }
    }

    #[test]
    fn fewer_nodes_than_n() {
        let c = cands(&[0, 1]);
        let owners = owners_for(&c, 7, 3);
        assert_eq!(owners.len(), 2, "degrades to available nodes");
    }
}

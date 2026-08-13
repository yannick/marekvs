//! Rendezvous (HRW) placement (design/04 §Placement).
//!
//! `score(node, pid) = xxh3_64(node_le_bytes || pid_le_bytes)`;
//! owners = top-N by score among data-owning members. Pure function of the
//! membership view — no ring state, minimal churn on join/leave.

use marekvs_core::ikey::Pid;
#[cfg(test)]
use marekvs_core::ikey::PARTITIONS;
use marekvs_core::NodeId;
use xxhash_rust::xxh3::xxh3_64;

#[inline]
pub fn score(node: NodeId, pid: Pid) -> u64 {
    let mut buf = [0u8; 4];
    buf[..2].copy_from_slice(&node.to_le_bytes());
    buf[2..].copy_from_slice(&pid.to_le_bytes());
    xxh3_64(&buf)
}

/// A placement candidate.
///
/// Deliberately a struct, not a tuple: adding zone-awareness had to reach
/// **every** path that computes ownership. `View::with_tables` (the cached
/// placement tables) and `Cluster::future_owned_pids` (the join gate) score
/// candidates independently, so a change that touched only one would make the
/// gate and the tables disagree about who owns what. Changing the type makes
/// the compiler find them all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    pub node: NodeId,
    /// Active nodes are eligible to be H1; Leaving nodes still own data.
    pub active: bool,
    /// Failure domain from `MAREKVS_ZONE`, gossiped. `None` = unlabelled,
    /// which degrades to pure HRW.
    pub zone: Option<String>,
}

impl Candidate {
    pub fn new(node: NodeId, active: bool) -> Candidate {
        Candidate {
            node,
            active,
            zone: None,
        }
    }
}

/// Top-N owners of `pid`, highest HRW score first.
///
/// With `zone_spread` off (the default) this is pure HRW: sort by score, take
/// the top N. With it on, the *same* score order is walked greedily, taking a
/// candidate only if its zone is not already represented, then falling back to
/// score order for whatever slots remain.
///
/// Why spreading matters: HRW is topology-blind, so with RF=2 both homes of a
/// partition can land in one zone. Kubernetes pod topology spread constraints
/// spread *pods*, not *partition replicas* — a zone outage then takes both
/// copies of some partitions while the pod count looks balanced.
///
/// The fallback pass is what keeps this safe when the topology is not what you
/// assumed: fewer zones than replicas, partially-labelled nodes, or a whole
/// zone gone. Ownership degrades to plain HRW rather than returning fewer
/// owners than it could.
pub fn owners_for_zoned(
    candidates: &[Candidate],
    pid: Pid,
    n: usize,
    zone_spread: bool,
) -> Vec<NodeId> {
    let mut scored: Vec<(u64, NodeId, Option<&str>)> = candidates
        .iter()
        .map(|c| (score(c.node, pid), c.node, c.zone.as_deref()))
        .collect();
    // Descending by (score, node): the node id breaks score ties so every
    // replica derives the identical order.
    scored.sort_unstable_by(|a, b| (b.0, b.1).cmp(&(a.0, a.1)));

    if !zone_spread {
        scored.truncate(n);
        return scored.into_iter().map(|(_, id, _)| id).collect();
    }

    let mut out: Vec<NodeId> = Vec::with_capacity(n);
    let mut used: Vec<&str> = Vec::new();
    for (_, id, zone) in &scored {
        if out.len() == n {
            break;
        }
        if let Some(z) = zone {
            if !used.contains(z) {
                used.push(z);
                out.push(*id);
            }
        }
    }
    // Remaining slots: unlabelled nodes, or zones already represented because
    // there are fewer zones than replicas.
    for (_, id, _) in &scored {
        if out.len() == n {
            break;
        }
        if !out.contains(id) {
            out.push(*id);
        }
    }
    out
}

/// Pure-HRW placement (no zone spreading) — see [`owners_for_zoned`].
pub fn owners_for(candidates: &[Candidate], pid: Pid, n: usize) -> Vec<NodeId> {
    owners_for_zoned(candidates, pid, n, false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cands(ids: &[NodeId]) -> Vec<Candidate> {
        ids.iter().map(|i| Candidate::new(*i, true)).collect()
    }

    /// `ids[i]` lives in `zones[i]`.
    fn zoned(ids: &[NodeId], zones: &[&str]) -> Vec<Candidate> {
        ids.iter()
            .zip(zones)
            .map(|(id, z)| Candidate {
                node: *id,
                active: true,
                zone: Some((*z).to_string()),
            })
            .collect()
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

    // --- zone-aware placement (T2-11) ---

    /// The regression freeze: with zone spreading OFF, placement must be
    /// byte-identical to pure HRW for every partition. This is what makes the
    /// feature safe to ship dark.
    #[test]
    fn zone_spreading_off_is_identical_to_plain_hrw() {
        let plain = cands(&[0, 1, 2, 3, 4, 5]);
        let labelled = zoned(&[0, 1, 2, 3, 4, 5], &["a", "a", "b", "b", "c", "c"]);
        for pid in 0..PARTITIONS {
            let expect = owners_for(&plain, pid, 3);
            assert_eq!(owners_for(&labelled, pid, 3), expect, "pid {pid}");
            assert_eq!(
                owners_for_zoned(&labelled, pid, 3, false),
                expect,
                "pid {pid}"
            );
        }
    }

    /// The point of the feature: with at least RF zones, the owners of every
    /// partition span RF *distinct* zones, so losing one zone cannot take
    /// every copy.
    #[test]
    fn owners_span_distinct_zones_when_enough_zones_exist() {
        let c = zoned(&[0, 1, 2, 3, 4, 5], &["a", "a", "b", "b", "c", "c"]);
        for pid in 0..PARTITIONS {
            let owners = owners_for_zoned(&c, pid, 3, true);
            assert_eq!(owners.len(), 3, "pid {pid}");
            let mut zs: Vec<&str> = owners
                .iter()
                .map(|o| {
                    c.iter()
                        .find(|x| x.node == *o)
                        .unwrap()
                        .zone
                        .as_deref()
                        .unwrap()
                })
                .collect();
            zs.sort_unstable();
            zs.dedup();
            assert_eq!(zs.len(), 3, "pid {pid}: owners share a zone ({owners:?})");
        }
    }

    /// Losing a whole zone must still produce RF owners — the fallback pass
    /// takes a second node from a zone already represented rather than
    /// under-replicating.
    #[test]
    fn fewer_zones_than_replicas_still_fills_every_slot() {
        let c = zoned(&[0, 1, 2, 3], &["a", "a", "b", "b"]);
        for pid in 0..PARTITIONS {
            let owners = owners_for_zoned(&c, pid, 3, true);
            assert_eq!(owners.len(), 3, "pid {pid} under-replicated");
            let mut d = owners.clone();
            d.sort_unstable();
            d.dedup();
            assert_eq!(d.len(), 3, "pid {pid} has duplicate owners");
        }
    }

    /// Unlabelled nodes must not be excluded — a half-labelled cluster
    /// (mid-rollout) still places every partition on RF nodes.
    #[test]
    fn unlabelled_nodes_are_still_placeable() {
        let mut c = zoned(&[0, 1], &["a", "b"]);
        c.push(Candidate::new(2, true));
        c.push(Candidate::new(3, true));
        for pid in 0..PARTITIONS {
            let owners = owners_for_zoned(&c, pid, 3, true);
            assert_eq!(owners.len(), 3, "pid {pid}");
            let mut d = owners.clone();
            d.sort_unstable();
            d.dedup();
            assert_eq!(d.len(), 3, "pid {pid} duplicates");
        }
    }

    /// Zone spreading must stay a pure function of the inputs: every replica
    /// computes placement independently and they must agree exactly.
    #[test]
    fn zone_spreading_is_deterministic() {
        let c = zoned(&[0, 1, 2, 3, 4, 5], &["a", "b", "c", "a", "b", "c"]);
        for pid in 0..PARTITIONS {
            assert_eq!(
                owners_for_zoned(&c, pid, 3, true),
                owners_for_zoned(&c, pid, 3, true)
            );
        }
    }

    /// Candidate ORDER must not change the answer — the view is assembled
    /// from a gossip map and two nodes may enumerate it differently.
    #[test]
    fn candidate_order_does_not_change_placement() {
        let c = zoned(&[0, 1, 2, 3, 4, 5], &["a", "b", "c", "a", "b", "c"]);
        let mut rev = c.clone();
        rev.reverse();
        for pid in 0..PARTITIONS {
            assert_eq!(
                owners_for_zoned(&c, pid, 3, true),
                owners_for_zoned(&rev, pid, 3, true),
                "pid {pid} depends on candidate order"
            );
        }
    }

    /// Spreading still has to balance: it reorders within score order, it does
    /// not concentrate ownership.
    #[test]
    fn zone_spread_keeps_the_distribution_balanced() {
        let c = zoned(&[0, 1, 2, 3, 4, 5], &["a", "a", "b", "b", "c", "c"]);
        let mut counts = [0usize; 6];
        for pid in 0..PARTITIONS {
            for id in owners_for_zoned(&c, pid, 3, true) {
                counts[id as usize] += 1;
            }
        }
        let expected = PARTITIONS as usize * 3 / 6;
        for (id, n) in counts.iter().enumerate() {
            let dev = (*n as f64 - expected as f64).abs() / expected as f64;
            assert!(dev < 0.2, "node {id} owns {n}, expected ~{expected}");
        }
    }

    #[test]
    fn fewer_nodes_than_n() {
        let c = cands(&[0, 1]);
        let owners = owners_for(&c, 7, 3);
        assert_eq!(owners.len(), 2, "degrades to available nodes");
    }
}

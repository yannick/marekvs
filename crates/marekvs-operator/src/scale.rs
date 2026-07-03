//! Pure scaling decisions — no Kubernetes types, fully unit-tested.
//!
//! Two separate questions, two functions:
//!
//! 1. [`autoscale_target`] — how many nodes *should* exist (the autoscaler's
//!    sizing decision, with hysteresis and time windows).
//! 2. [`next_replicas`] — what the StatefulSet may be set to *right now*
//!    (the safety stepper: up freely, down one node at a time and only
//!    while every partition is fully replicated).

use crate::types::AutoscaleSpec;

/// Everything the decision functions are allowed to look at.
#[derive(Debug, Clone, Copy)]
pub struct Observed {
    /// Current StatefulSet spec.replicas.
    pub current: i32,
    /// Pods passing readiness.
    pub ready: i32,
    /// Worst `marekvs_cluster_underreplicated_partitions` across nodes;
    /// None = metrics could not be scraped.
    pub underreplicated: Option<i64>,
    /// Cluster-wide command rate; None = no valid sample yet.
    pub ops_per_second: Option<f64>,
    /// Unix seconds now / of the last completed scale operation.
    pub now_epoch: i64,
    pub last_scale_epoch: Option<i64>,
}

/// The autoscaler's ideal node count, given the observed load.
///
/// Sizing: `ceil(total_ops / target_ops_per_node)`, clamped to
/// `[max(min_nodes, rf+1), max_nodes]`. Hysteresis:
/// * scale UP whenever the ideal exceeds current and the up-cooldown has
///   passed — undersized clusters hurt immediately;
/// * scale DOWN only one node per stabilization window, and only when the
///   per-node load has real headroom (< 60% of target at `current - 1`
///   nodes), so the cluster doesn't flap around the threshold.
pub fn autoscale_target(spec: &AutoscaleSpec, rf: i32, obs: &Observed) -> i32 {
    let floor = spec.min_nodes.max(rf + 1);
    let ceil = spec.max_nodes.max(floor);
    let clamp = |n: i32| n.clamp(floor, ceil);

    let Some(ops) = obs.ops_per_second else {
        // No load signal (bootstrap, scrape failure): hold position.
        return clamp(obs.current);
    };
    let ideal = (ops / spec.target_ops_per_node).ceil() as i32;

    let since_scale = obs
        .last_scale_epoch
        .map(|t| obs.now_epoch - t)
        .unwrap_or(i64::MAX);

    if ideal > obs.current {
        if since_scale >= spec.scale_up_cooldown_seconds {
            return clamp(ideal);
        }
        return clamp(obs.current);
    }

    if ideal < obs.current && obs.current > floor {
        let after_down = (obs.current - 1).max(1);
        let load_after = ops / after_down as f64;
        if load_after < 0.6 * spec.target_ops_per_node
            && since_scale >= spec.scale_down_stabilization_seconds
        {
            return clamp(obs.current - 1);
        }
    }
    clamp(obs.current)
}

/// What the stepper decided this round.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// Set the StatefulSet to this many replicas.
    Set(i32),
    /// At the target; nothing to do.
    Hold,
    /// Want to shrink but the cluster is not provably safe (reason inside).
    Blocked(BlockReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockReason {
    /// Some partition is below the replication factor.
    Underreplicated,
    /// Not every pod is ready — a member is joining/restarting.
    NotAllReady,
    /// Metrics could not be scraped; refuse blind scale-downs.
    NoMetrics,
}

/// The safety stepper. `target` is where we eventually want to be
/// ([`autoscale_target`] or spec.nodes); the return value is what the
/// StatefulSet may be set to *now*.
///
/// Scale-up jumps straight to the target (adding nodes never removes data
/// copies). Scale-down moves ONE node per call and only while
/// `underreplicated == 0` with all pods ready — the codified version of the
/// runbook in k8s/README.md.
pub fn next_replicas(target: i32, obs: &Observed) -> Step {
    use std::cmp::Ordering::*;
    match target.cmp(&obs.current) {
        Equal => Step::Hold,
        Greater => Step::Set(target),
        Less => match obs.underreplicated {
            None => Step::Blocked(BlockReason::NoMetrics),
            Some(n) if n > 0 => Step::Blocked(BlockReason::Underreplicated),
            Some(_) if obs.ready < obs.current => Step::Blocked(BlockReason::NotAllReady),
            Some(_) => Step::Set(obs.current - 1),
        },
    }
}

/// Command-counter rate between two samples. Counters are per-process, so a
/// pod restart can make the total go DOWN — such samples are discarded
/// (returns None and the caller re-baselines). Degenerate intervals are
/// discarded too.
pub fn ops_rate(
    prev_total: Option<f64>,
    prev_epoch: Option<i64>,
    total: f64,
    now_epoch: i64,
) -> Option<f64> {
    let (pt, pe) = (prev_total?, prev_epoch?);
    let dt = now_epoch - pe;
    if !(1..=900).contains(&dt) || total < pt {
        return None;
    }
    Some((total - pt) / dt as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> AutoscaleSpec {
        AutoscaleSpec {
            min_nodes: 3,
            max_nodes: 10,
            target_ops_per_node: 1000.0,
            scale_down_stabilization_seconds: 300,
            scale_up_cooldown_seconds: 60,
        }
    }

    fn obs(current: i32, ops: Option<f64>) -> Observed {
        Observed {
            current,
            ready: current,
            underreplicated: Some(0),
            ops_per_second: ops,
            now_epoch: 10_000,
            last_scale_epoch: None,
        }
    }

    #[test]
    fn sizes_from_load() {
        // 4500 ops/s at 1000/node → 5 nodes.
        assert_eq!(autoscale_target(&spec(), 2, &obs(3, Some(4500.0))), 5);
    }

    #[test]
    fn respects_ceiling_and_floor() {
        assert_eq!(autoscale_target(&spec(), 2, &obs(3, Some(50_000.0))), 10);
        // rf+1 beats a too-low minNodes.
        let mut s = spec();
        s.min_nodes = 1;
        assert_eq!(autoscale_target(&s, 2, &obs(3, Some(1.0))), 3);
    }

    #[test]
    fn no_signal_holds() {
        assert_eq!(autoscale_target(&spec(), 2, &obs(5, None)), 5);
    }

    #[test]
    fn up_cooldown_defers() {
        let mut o = obs(3, Some(9000.0));
        o.last_scale_epoch = Some(o.now_epoch - 10); // scaled 10s ago
        assert_eq!(autoscale_target(&spec(), 2, &o), 3);
        o.last_scale_epoch = Some(o.now_epoch - 61);
        assert_eq!(autoscale_target(&spec(), 2, &o), 9);
    }

    #[test]
    fn down_needs_headroom_and_stabilization() {
        // 5 nodes, 500 ops/s total → ideal 1, but going 5→4 needs
        // load_after < 600 (0.6 * target): 500/4 = 125 ✓, and the window.
        let mut o = obs(5, Some(500.0));
        o.last_scale_epoch = Some(o.now_epoch - 100); // window not passed
        assert_eq!(autoscale_target(&spec(), 2, &o), 5);
        o.last_scale_epoch = Some(o.now_epoch - 301);
        assert_eq!(autoscale_target(&spec(), 2, &o), 4); // one step only
    }

    #[test]
    fn down_hysteresis_blocks_flapping() {
        // 4 nodes at 2500 total: ideal 3, but 2500/3 = 833 > 600 → hold.
        let mut o = obs(4, Some(2500.0));
        o.last_scale_epoch = Some(o.now_epoch - 1000);
        assert_eq!(autoscale_target(&spec(), 2, &o), 4);
    }

    #[test]
    fn stepper_scales_up_in_one_jump() {
        assert_eq!(next_replicas(7, &obs(3, None)), Step::Set(7));
    }

    #[test]
    fn stepper_scales_down_one_at_a_time() {
        assert_eq!(next_replicas(3, &obs(6, None)), Step::Set(5));
    }

    #[test]
    fn stepper_blocks_when_underreplicated() {
        let mut o = obs(6, None);
        o.underreplicated = Some(17);
        assert_eq!(
            next_replicas(3, &o),
            Step::Blocked(BlockReason::Underreplicated)
        );
    }

    #[test]
    fn stepper_blocks_without_metrics() {
        let mut o = obs(6, None);
        o.underreplicated = None;
        assert_eq!(next_replicas(3, &o), Step::Blocked(BlockReason::NoMetrics));
    }

    #[test]
    fn stepper_blocks_when_pods_unready() {
        let mut o = obs(6, None);
        o.ready = 5;
        assert_eq!(
            next_replicas(3, &o),
            Step::Blocked(BlockReason::NotAllReady)
        );
    }

    #[test]
    fn stepper_holds_at_target() {
        assert_eq!(next_replicas(4, &obs(4, None)), Step::Hold);
    }

    #[test]
    fn rate_discards_restarts_and_bad_intervals() {
        assert_eq!(ops_rate(Some(100.0), Some(0), 700.0, 60), Some(10.0));
        assert_eq!(ops_rate(Some(100.0), Some(0), 50.0, 60), None); // reset
        assert_eq!(ops_rate(Some(100.0), Some(0), 700.0, 0), None); // dt=0
        assert_eq!(ops_rate(Some(100.0), Some(0), 700.0, 9999), None); // stale
        assert_eq!(ops_rate(None, None, 700.0, 60), None); // no baseline
    }
}

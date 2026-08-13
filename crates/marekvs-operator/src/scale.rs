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

/// Observed rollout state, straight off the StatefulSet status.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RolloutObserved {
    /// `spec.replicas` the controller settled on this pass.
    pub replicas: i32,
    /// Pods passing readiness.
    pub ready: i32,
    /// Worst underreplicated count; `None` = metrics unavailable.
    pub underreplicated: Option<i64>,
    /// True when the StatefulSet's update revision differs from its current
    /// revision — i.e. the pod template changed and pods still need replacing.
    pub revision_changed: bool,
    /// Pods already carrying the update revision.
    pub updated: i32,
    /// `rollingUpdate.partition` the controller published last pass.
    pub partition: Option<i32>,
}

/// What to do with `rollingUpdate.partition` this reconcile.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Rollout {
    /// No template change in flight — let the StatefulSet run unpartitioned.
    Idle,
    /// Hold the partition here; the health gate is not satisfied.
    Blocked(i32),
    /// Move the partition down to this value, releasing one more pod.
    Advance(i32),
    /// Every pod carries the new revision.
    Done,
}

/// Health-gated canary walk of `rollingUpdate.partition` (T2-15).
///
/// A plain StatefulSet rolling update is gated only on pod *readiness*, and
/// readiness means "serving", not "the cluster is fully replicated". With
/// RF=2, restarting pod B while pod A's partitions are still under-replicated
/// leaves a single-copy window. So the controller drives the rollout itself:
/// freeze it at `partition == replicas`, then release exactly one pod at a
/// time, and only while every partition is fully replicated and every pod is
/// ready.
///
/// The gate is deliberately the same one scale-down uses — `underreplicated ==
/// 0` and all pods ready — because it is the same question: is it safe to take
/// a node away right now?
///
/// Unknown metrics block: without `underreplicated` we cannot prove safety,
/// and a stuck rollout is visible and recoverable, whereas one that proceeded
/// blind is neither.
pub fn rollout_step(o: &RolloutObserved) -> Rollout {
    if !o.revision_changed {
        return Rollout::Idle;
    }
    if o.updated >= o.replicas {
        return Rollout::Done;
    }
    // First pass of a rollout: freeze before anything is replaced.
    let current = o.partition.unwrap_or(o.replicas).clamp(0, o.replicas);
    if o.partition.is_none() && current > 0 {
        return Rollout::Blocked(current);
    }
    let healthy = o.ready >= o.replicas && o.underreplicated == Some(0);
    if !healthy {
        return Rollout::Blocked(current);
    }
    if current == 0 {
        // Already fully released; waiting for the last pod to report updated.
        return Rollout::Blocked(0);
    }
    Rollout::Advance(current - 1)
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

    fn ro(replicas: i32, partition: Option<i32>) -> RolloutObserved {
        RolloutObserved {
            replicas,
            ready: replicas,
            underreplicated: Some(0),
            revision_changed: true,
            updated: 0,
            partition,
        }
    }

    #[test]
    fn rollout_is_idle_without_a_template_change() {
        let mut o = ro(3, None);
        o.revision_changed = false;
        assert_eq!(rollout_step(&o), Rollout::Idle);
    }

    /// The first pass must freeze the rollout, not let k8s replace a pod
    /// before the controller has looked at cluster health even once.
    #[test]
    fn rollout_freezes_before_replacing_anything() {
        assert_eq!(rollout_step(&ro(3, None)), Rollout::Blocked(3));
    }

    #[test]
    fn rollout_releases_one_pod_at_a_time_while_healthy() {
        assert_eq!(rollout_step(&ro(3, Some(3))), Rollout::Advance(2));
        assert_eq!(rollout_step(&ro(3, Some(2))), Rollout::Advance(1));
        assert_eq!(rollout_step(&ro(3, Some(1))), Rollout::Advance(0));
    }

    /// The whole point: under-replicated means "do not take another node",
    /// which readiness alone would have allowed.
    #[test]
    fn rollout_holds_while_underreplicated() {
        let mut o = ro(3, Some(2));
        o.underreplicated = Some(4);
        assert_eq!(rollout_step(&o), Rollout::Blocked(2));
    }

    #[test]
    fn rollout_holds_while_a_pod_is_unready() {
        let mut o = ro(3, Some(2));
        o.ready = 2;
        assert_eq!(rollout_step(&o), Rollout::Blocked(2));
    }

    /// No metrics = no proof of safety = do not advance.
    #[test]
    fn rollout_holds_without_metrics() {
        let mut o = ro(3, Some(2));
        o.underreplicated = None;
        assert_eq!(rollout_step(&o), Rollout::Blocked(2));
    }

    #[test]
    fn rollout_completes_when_every_pod_is_updated() {
        let mut o = ro(3, Some(0));
        o.updated = 3;
        assert_eq!(rollout_step(&o), Rollout::Done);
    }

    /// At partition 0 the last pod is still being replaced; holding (rather
    /// than reporting Done) keeps the phase honest until the status confirms.
    #[test]
    fn rollout_waits_at_zero_for_the_last_pod() {
        let mut o = ro(3, Some(0));
        o.updated = 2;
        assert_eq!(rollout_step(&o), Rollout::Blocked(0));
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

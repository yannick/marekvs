//! Cluster topology snapshot for the CLUSTER command family (design/15).
//! Provided by the server via [`crate::Engine::set_cluster_topology`] —
//! the same trait-object indirection as the `cluster_info` hook, so this
//! crate does not depend on marekvs-cluster.

use std::net::SocketAddr;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct TopologyNode {
    pub id: u16,
    /// Client-facing RESP endpoint; None for members that don't gossip one
    /// (mixed-version cluster) — omitted from topology replies.
    pub resp_addr: Option<SocketAddr>,
    /// Gossip port, reported as the cluster-bus port in CLUSTER NODES.
    pub gossip_port: u16,
    /// chitchat generation (boot incarnation) — config-epoch.
    pub generation: u64,
    /// Node phase string: "joining" | "active" | "leaving".
    pub state: String,
}

#[derive(Debug, Clone)]
pub struct Topology {
    pub self_id: u16,
    /// Monotonic view epoch (bumps on every membership change).
    pub epoch: u64,
    pub nodes: Vec<TopologyNode>,
    /// Per-pid owners, H1 (primary) first — PARTITIONS entries.
    pub pid_owners: Vec<Vec<u16>>,
}

pub type TopologyFn = Arc<dyn Fn() -> Topology + Send + Sync>;

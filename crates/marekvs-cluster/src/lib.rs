//! marekvs-cluster — gossip membership (chitchat) + HRW placement
//! (design/04 §Placement, design/06).

pub mod placement;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use chitchat::transport::UdpTransport;
use chitchat::{spawn_chitchat, ChitchatConfig, ChitchatHandle, ChitchatId, FailureDetectorConfig};
use marekvs_core::ikey::{Pid, PARTITIONS};
use marekvs_core::NodeId;
use parking_lot::RwLock;
use tokio::sync::watch;

pub use placement::{owners_for, owners_for_zoned, Candidate};

/// This node's failure domain (`MAREKVS_ZONE`), gossiped so peers can spread
/// replicas across zones. Empty/unset = unlabelled.
pub fn self_zone() -> Option<String> {
    static V: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();
    V.get_or_init(|| {
        std::env::var("MAREKVS_ZONE")
            .ok()
            .map(|z| z.trim().to_string())
            .filter(|z| !z.is_empty())
    })
    .clone()
}

/// Whether placement spreads a partition's replicas across zones (T2-11).
///
/// Off by default, and it **must be uniform cluster-wide**: two nodes
/// disagreeing compute different owners for the same partition, which is the
/// same class of split as a REPLICAS_N mismatch. `Cluster::rebuild_view` logs
/// an ERROR when a peer's placement config hash differs from ours.
///
/// Turning it on for a live cluster reshuffles ownership, so the data movement
/// runs through the normal join/bootstrap machinery (rate-limited by
/// `MAREKVS_BOOTSTRAP_RATE_MB`); watch `marekvs_cluster_underreplicated_partitions`
/// for progress. Treat the flip as a maintenance operation.
pub fn zone_spread_enabled() -> bool {
    static V: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        matches!(
            std::env::var("MAREKVS_ZONE_AWARE").as_deref(),
            Ok("1") | Ok("true") | Ok("yes")
        )
    })
}

/// Hash of the settings every node must agree on for placement to converge.
/// Gossiped so a mismatch is loud instead of silently splitting ownership.
fn placement_config_hash(replicas_n: usize) -> String {
    format!("rf{}z{}", replicas_n, u8::from(zone_spread_enabled()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodePhase {
    Joining,
    Active,
    Leaving,
}

impl NodePhase {
    pub fn as_str(&self) -> &'static str {
        match self {
            NodePhase::Joining => "joining",
            NodePhase::Active => "active",
            NodePhase::Leaving => "leaving",
        }
    }

    pub fn parse(s: &str) -> Option<NodePhase> {
        match s {
            "joining" => Some(NodePhase::Joining),
            "active" => Some(NodePhase::Active),
            "leaving" => Some(NodePhase::Leaving),
            _ => None,
        }
    }

    /// Placement-eligible states (design/04): Active and Leaving own data.
    pub fn owns_data(&self) -> bool {
        matches!(self, NodePhase::Active | NodePhase::Leaving)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Member {
    pub node: NodeId,
    pub mesh_addr: SocketAddr,
    /// The peer's gossip endpoint — persisted by the server as a fallback
    /// seed so a restarted node can rejoin even when every configured seed
    /// address went stale (environments without stable IPs or DNS, e.g.
    /// Apple containers give every restart a fresh IP).
    pub gossip_addr: SocketAddr,
    /// The peer's client-facing RESP endpoint, for CLUSTER SLOTS/NODES
    /// (design/15). None while a mixed-version cluster still has members
    /// that don't gossip it — such members are omitted from topology
    /// replies, never guessed.
    pub resp_addr: Option<SocketAddr>,
    /// chitchat generation (boot timestamp) — the member's incarnation,
    /// reported as config-epoch in CLUSTER NODES/INFO.
    pub generation: u64,
    pub phase: NodePhase,
    /// Failure domain from `MAREKVS_ZONE` (k8s downward API from
    /// `topology.kubernetes.io/zone`), gossiped. `None` = unlabelled.
    pub zone: Option<String>,
}

/// Immutable placement view derived from the current gossip state.
///
/// `owners_tbl`/`h1_tbl` are the flat per-pid placement tables from
/// design/04 ("cached as a flat [pid] → [NodeId; N] table") — computed once
/// per membership change so the read/replication hot paths do a lookup
/// instead of re-scoring HRW per operation (profiling: `owners_for` allocs
/// and hashing showed up on every GET via the read-through ownership check).
#[derive(Debug, Default, Clone)]
pub struct View {
    pub members: Vec<Member>,
    pub epoch: u64,
    /// Per-pid top-N owners; empty when the view was built without a
    /// replica count (e.g. `View::default()`), in which case `owners()`
    /// falls back to direct computation.
    owners_tbl: Vec<Vec<NodeId>>,
    h1_tbl: Vec<Option<NodeId>>,
    /// The replica count the tables were built for.
    tbl_n: usize,
}

impl View {
    /// Build a view with precomputed placement tables for `n` replicas.
    pub fn with_tables(members: Vec<Member>, epoch: u64, n: usize) -> View {
        let mut view = View {
            members,
            epoch,
            owners_tbl: Vec::new(),
            h1_tbl: Vec::new(),
            tbl_n: n,
        };
        let candidates = view.owner_candidates();
        let zone_spread = zone_spread_enabled();
        let mut owners_tbl = Vec::with_capacity(PARTITIONS as usize);
        let mut h1_tbl = Vec::with_capacity(PARTITIONS as usize);
        for pid in 0..PARTITIONS {
            let owners = owners_for_zoned(&candidates, pid, n, zone_spread);
            h1_tbl.push(view.h1_uncached(&owners));
            owners_tbl.push(owners);
        }
        view.owners_tbl = owners_tbl;
        view.h1_tbl = h1_tbl;
        view
    }

    pub fn owner_candidates(&self) -> Vec<Candidate> {
        self.members
            .iter()
            .filter(|m| m.phase.owns_data())
            .map(|m| Candidate {
                node: m.node,
                active: m.phase == NodePhase::Active,
                zone: m.zone.clone(),
            })
            .collect()
    }

    /// The N home replicas of a partition (highest HRW scores first).
    pub fn owners(&self, pid: Pid, n: usize) -> Vec<NodeId> {
        if n == self.tbl_n && !self.owners_tbl.is_empty() {
            return self.owners_tbl[pid as usize].clone();
        }
        owners_for_zoned(&self.owner_candidates(), pid, n, zone_spread_enabled())
    }

    /// Table-backed membership test without allocating.
    pub fn is_owner(&self, pid: Pid, n: usize, node: NodeId) -> bool {
        if n == self.tbl_n && !self.owners_tbl.is_empty() {
            return self.owners_tbl[pid as usize].contains(&node);
        }
        self.owners(pid, n).contains(&node)
    }

    fn h1_uncached(&self, owners: &[NodeId]) -> Option<NodeId> {
        owners
            .iter()
            .find(|id| {
                self.members
                    .iter()
                    .any(|m| m.node == **id && m.phase == NodePhase::Active)
            })
            .or(owners.first())
            .copied()
    }

    /// Primary home: top-ranked owner whose phase is Active.
    pub fn h1(&self, pid: Pid, n: usize) -> Option<NodeId> {
        if n == self.tbl_n && !self.h1_tbl.is_empty() {
            return self.h1_tbl[pid as usize];
        }
        let owners = self.owners(pid, n);
        self.h1_uncached(&owners)
    }

    pub fn mesh_addr(&self, node: NodeId) -> Option<SocketAddr> {
        self.members
            .iter()
            .find(|m| m.node == node)
            .map(|m| m.mesh_addr)
    }

    pub fn contains(&self, node: NodeId) -> bool {
        self.members.iter().any(|m| m.node == node)
    }
}

pub struct ClusterConfig {
    pub node_id: NodeId,
    pub cluster_name: String,
    pub gossip_listen: SocketAddr,
    pub gossip_advertise: SocketAddr,
    pub mesh_advertise: SocketAddr,
    pub resp_advertise: SocketAddr,
    pub seeds: Vec<String>,
    pub replicas_n: usize,
    pub gossip_interval: Duration,
}

pub struct Cluster {
    handle: ChitchatHandle,
    pub self_id: NodeId,
    pub replicas_n: usize,
    view: Arc<RwLock<Arc<View>>>,
    view_tx: watch::Sender<u64>,
    /// Local mirror of the gossiped self phase (chitchat has no cheap sync
    /// read of own state; consumers like the alive-heartbeat need one).
    self_phase: RwLock<NodePhase>,
}

impl Cluster {
    pub async fn spawn(cfg: ClusterConfig) -> anyhow::Result<Arc<Cluster>> {
        let generation = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();
        let chitchat_id = ChitchatId {
            node_id: cfg.node_id.to_string(),
            generation_id: generation,
            gossip_advertise_addr: cfg.gossip_advertise,
        };
        let config = ChitchatConfig {
            chitchat_id,
            cluster_id: cfg.cluster_name.clone(),
            gossip_interval: cfg.gossip_interval,
            listen_addr: cfg.gossip_listen,
            seed_nodes: cfg.seeds.clone(),
            failure_detector_config: FailureDetectorConfig {
                initial_interval: cfg.gossip_interval * 4,
                ..Default::default()
            },
            marked_for_deletion_grace_period: Duration::from_secs(3600),
            catchup_callback: None,
            extra_liveness_predicate: None,
        };
        let mut initial_kvs = vec![
            ("mesh_addr".to_string(), cfg.mesh_advertise.to_string()),
            ("resp_addr".to_string(), cfg.resp_advertise.to_string()),
            ("state".to_string(), NodePhase::Joining.as_str().to_string()),
            // Placement settings every node must agree on; a mismatch splits
            // ownership silently, so it is gossiped and checked on every view.
            ("pcfg".to_string(), placement_config_hash(cfg.replicas_n)),
        ];
        if let Some(z) = self_zone() {
            initial_kvs.push(("zone".to_string(), z));
        }
        let handle = spawn_chitchat(config, initial_kvs, &UdpTransport).await?;

        let (view_tx, _) = watch::channel(0u64);
        let cluster = Arc::new(Cluster {
            handle,
            self_id: cfg.node_id,
            replicas_n: cfg.replicas_n,
            view: Arc::new(RwLock::new(Arc::new(View::default()))),
            view_tx,
            self_phase: RwLock::new(NodePhase::Joining),
        });

        // Watch live-node changes and rebuild the placement view.
        let weak = Arc::downgrade(&cluster);
        let mut watcher = {
            let chitchat = cluster.handle.chitchat();
            let guard = chitchat.lock().await;
            guard.live_nodes_watcher()
        };
        tokio::spawn(async move {
            let mut epoch = 0u64;
            loop {
                if watcher.changed().await.is_err() {
                    return;
                }
                let Some(cluster) = weak.upgrade() else {
                    return;
                };
                let snapshot = watcher.borrow_and_update().clone();
                epoch += 1;
                cluster.rebuild_view(snapshot, epoch);
            }
        });

        Ok(cluster)
    }

    fn rebuild_view(&self, nodes: BTreeMap<ChitchatId, chitchat::NodeState>, epoch: u64) {
        let mut members = Vec::with_capacity(nodes.len() + 1);
        for (id, state) in &nodes {
            let Ok(node) = id.node_id.parse::<NodeId>() else {
                continue;
            };
            let Some(addr) = state.get("mesh_addr").and_then(|a| a.parse().ok()) else {
                continue;
            };
            let phase = state
                .get("state")
                .and_then(NodePhase::parse)
                .unwrap_or(NodePhase::Joining);
            members.push(Member {
                node,
                mesh_addr: addr,
                gossip_addr: id.gossip_advertise_addr,
                resp_addr: state.get("resp_addr").and_then(|a| a.parse().ok()),
                generation: id.generation_id,
                phase,
                zone: state
                    .get("zone")
                    .filter(|z| !z.is_empty())
                    .map(String::from),
            });
        }
        members.sort_by_key(|m| m.node);
        members.dedup_by_key(|m| m.node);

        // Placement config must be uniform: a node computing ownership under a
        // different RF or zone-awareness setting disagrees about who owns what,
        // and the disagreement is invisible until reads go missing. Loud, every
        // view change, rather than a startup-only check that a later rollout
        // could slip past.
        let ours = placement_config_hash(self.replicas_n);
        for (id, state) in &nodes {
            let Some(theirs) = state.get("pcfg") else {
                continue;
            };
            if theirs != ours {
                tracing::error!(
                    peer = %id.node_id,
                    ours = %ours,
                    theirs = %theirs,
                    "PLACEMENT CONFIG MISMATCH: this peer computes different \
                     partition owners than we do (check MAREKVS_REPLICAS_N and \
                     MAREKVS_ZONE_AWARE are identical cluster-wide)"
                );
            }
        }

        tracing::info!(?members, epoch, "membership view updated");
        *self.view.write() = Arc::new(View::with_tables(members, epoch, self.replicas_n));
        let _ = self.view_tx.send(epoch);
    }

    /// Current placement view (cheap Arc clone).
    pub fn view(&self) -> Arc<View> {
        self.view.read().clone()
    }

    /// Subscribe to view changes (value = epoch).
    pub fn watch(&self) -> watch::Receiver<u64> {
        self.view_tx.subscribe()
    }

    pub async fn set_phase(&self, phase: NodePhase) {
        let chitchat = self.handle.chitchat();
        let mut guard = chitchat.lock().await;
        guard.self_node_state().set("state", phase.as_str());
        *self.self_phase.write() = phase;
        tracing::info!(phase = phase.as_str(), "node phase changed");
    }

    /// Our own current phase (local mirror of the gossiped state).
    pub fn phase(&self) -> NodePhase {
        *self.self_phase.read()
    }

    pub async fn set_kv(&self, key: &str, value: &str) {
        let chitchat = self.handle.chitchat();
        let mut guard = chitchat.lock().await;
        guard.self_node_state().set(key, value);
    }

    // --- placement conveniences ---

    pub fn owners(&self, pid: Pid) -> Vec<NodeId> {
        self.view().owners(pid, self.replicas_n)
    }

    pub fn h1(&self, pid: Pid) -> Option<NodeId> {
        self.view().h1(pid, self.replicas_n)
    }

    pub fn is_home(&self, pid: Pid) -> bool {
        self.view().is_owner(pid, self.replicas_n, self.self_id)
    }

    /// Partitions this node homes under the current view.
    pub fn owned_pids(&self) -> Vec<Pid> {
        let view = self.view();
        (0..PARTITIONS)
            .filter(|pid| view.is_owner(*pid, self.replicas_n, self.self_id))
            .collect()
    }

    /// Partitions this node WOULD home if it were Active (join planning).
    pub fn future_owned_pids(&self) -> Vec<Pid> {
        let view = self.view();
        let mut candidates = view.owner_candidates();
        if !candidates.iter().any(|c| c.node == self.self_id) {
            candidates.push(Candidate {
                node: self.self_id,
                active: true,
                zone: self_zone(),
            });
        }
        (0..PARTITIONS)
            .filter(|pid| {
                owners_for_zoned(&candidates, *pid, self.replicas_n, zone_spread_enabled())
                    .contains(&self.self_id)
            })
            .collect()
    }

    /// Owners of `pid` under the placement that INCLUDES this node as an
    /// Active candidate (the post-join world), excluding self. For a
    /// rejoiner these are its pre-outage co-owners — the peers that held
    /// the partition's data alongside it.
    pub fn future_co_owners(&self, pid: Pid) -> Vec<NodeId> {
        let view = self.view();
        let mut candidates = view.owner_candidates();
        if !candidates.iter().any(|c| c.node == self.self_id) {
            candidates.push(Candidate {
                node: self.self_id,
                active: true,
                zone: self_zone(),
            });
        }
        owners_for_zoned(&candidates, pid, self.replicas_n, zone_spread_enabled())
            .into_iter()
            .filter(|o| *o != self.self_id)
            .collect()
    }

    pub fn cluster_stats(&self) -> ClusterStats {
        let view = self.view();
        let candidates = view.owner_candidates();
        let mut under = 0usize;
        let mut min_rf = usize::MAX;
        for pid in 0..PARTITIONS {
            let rf =
                owners_for_zoned(&candidates, pid, self.replicas_n, zone_spread_enabled()).len();
            min_rf = min_rf.min(rf);
            if rf < self.replicas_n {
                under += 1;
            }
        }
        ClusterStats {
            members: view.members.len(),
            underreplicated_partitions: under,
            effective_rf_min: if min_rf == usize::MAX { 0 } else { min_rf },
            degraded: under > 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClusterStats {
    pub members: usize,
    pub underreplicated_partitions: usize,
    pub effective_rf_min: usize,
    pub degraded: bool,
}

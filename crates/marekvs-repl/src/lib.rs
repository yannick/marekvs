//! marekvs-repl — asynchronous replication (design/04) and anti-entropy
//! (design/05): commit-hook → ring → per-peer fan-out, interest-based
//! read-through with leases, Merkle repair, partition bootstrap.

pub mod ae;
pub mod mesh;
pub mod ring;

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use marekvs_cluster::{Cluster, NodePhase};
use marekvs_core::envelope::Envelope;
use marekvs_core::ikey::{self, Pid};
use marekvs_core::NodeId;
use marekvs_engine::store::{self, Store};
use marekvs_engine::{BudgetErr, BudgetGrant, BudgetPeer, Engine, ReadThrough};
use marekvs_proto::{BudgetErrKind, BudgetGrantWire, BudgetTokenId, PeerMsg, ReplBatch, ReplOp};
use mesh::Mesh;
use parking_lot::Mutex;
use ring::Ring;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};

pub const INTEREST_LEASE: Duration = Duration::from_secs(60);
pub const FETCH_TIMEOUT: Duration = Duration::from_millis(300);
pub const AE_ROUND: Duration = Duration::from_secs(5);
const BATCH_MAX_OPS: usize = 256;
/// Payload-byte cap per ReplBatch, comfortably under proto MAX_FRAME (8 MiB):
/// oversized frames fail encode in the writer task and are dropped silently.
const BATCH_MAX_BYTES: usize = 1024 * 1024;
/// Seq-space jump applied on restart: must exceed ops accepted between two
/// high-water persists (1s apart) with generous margin.
const RING_SEQ_RESTART_JUMP: u64 = 1_000_000;

type InterestMap = HashMap<Pid, HashMap<Vec<u8>, HashMap<NodeId, Instant>>>;

/// Hard cap on interest-map entries. Design/04 defaults table: 1 M.
fn interest_max_entries() -> usize {
    static V: OnceLock<usize> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("MAREKVS_INTEREST_MAX_ENTRIES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&v| v > 0)
            .unwrap_or(1_000_000)
    })
}

/// The interest map behind a strict mutation API so the live (pid, key,
/// node) leaf count can never drift — the count enforces the hard cap that
/// keeps a redis-cli scanning unique keys through non-home nodes from
/// inflating this table without limit (an OOM you can cause from a client).
///
/// Policy at cap: REJECT new registrations, always allow refreshing an
/// existing leaf. Rejection is correct in the AP model: the subscriber's
/// 60 s lease already bounds staleness and interest pushes only shrink it —
/// a rejected registration degrades that key to worst-case-lease staleness
/// with zero memory growth. (Evict-oldest would need an order structure
/// that is itself unbounded under the scan-storm being defended against.)
struct Interest {
    map: InterestMap,
    total: usize,
    max: usize,
}

impl Interest {
    fn new(max: usize) -> Interest {
        Interest {
            map: HashMap::new(),
            total: 0,
            max,
        }
    }

    /// Register or refresh a lease; false = at cap, registration rejected.
    fn register(&mut self, pid: Pid, userkey: &[u8], node: NodeId, exp: Instant) -> bool {
        if let Some(slot) = self
            .map
            .get_mut(&pid)
            .and_then(|keys| keys.get_mut(userkey))
            .and_then(|subs| subs.get_mut(&node))
        {
            *slot = exp; // refresh never grows the map
            return true;
        }
        if self.total >= self.max {
            return false;
        }
        self.map
            .entry(pid)
            .or_default()
            .entry(userkey.to_vec())
            .or_default()
            .insert(node, exp);
        self.total += 1;
        true
    }

    fn subs(&self, pid: Pid, userkey: &[u8]) -> Option<&HashMap<NodeId, Instant>> {
        self.map.get(&pid).and_then(|keys| keys.get(userkey))
    }

    /// Drop every lease held by `node` (its connection died).
    fn remove_peer(&mut self, node: NodeId) {
        let total = &mut self.total;
        self.map.retain(|_, keys| {
            keys.retain(|_, subs| {
                if subs.remove(&node).is_some() {
                    *total -= 1;
                }
                !subs.is_empty()
            });
            !keys.is_empty()
        });
    }

    /// Drop expired leases (called once per AE round).
    fn gc(&mut self, now: Instant) {
        let total = &mut self.total;
        self.map.retain(|_, keys| {
            keys.retain(|_, subs| {
                subs.retain(|_, exp| {
                    let live = *exp > now;
                    if !live {
                        *total -= 1;
                    }
                    live
                });
                !subs.is_empty()
            });
            !keys.is_empty()
        });
    }
}

/// Warn (once per stall) after a peer's window has been full this long.
const STALL_WARN: Duration = Duration::from_secs(5);

/// Re-request a pending bootstrap with no chunk progress after this long.
const BOOTSTRAP_RETRY: Duration = Duration::from_secs(5);
/// An all-Joining cohort (nobody Active anywhere) is treated as a cluster
/// cold start once this much time has passed since ReplEngine start — there
/// is no data to pull, everyone may flip Active. Distinguishes cold start
/// from "gossip has not yet delivered the peers' Active state".
const COLD_START_SETTLE: Duration = Duration::from_secs(6);
/// Meta key holding the pids with unfinished bootstraps (u16 BE, packed).
/// Survives a crash mid-bootstrap: those pids are re-requested at boot even
/// though locally non-empty (chunks are merge-idempotent).
const JOIN_PENDING_KEY: &[u8] = b"join:pending";
/// Meta key: wall-ms heartbeat proving this node was recently alive.
/// Written only while Active/Leaving — a crash mid-rejoin must keep
/// measuring downtime from the PRE-death timestamp or the restart would
/// skip the rejoin.
const ALIVE_LAST_KEY: &[u8] = b"alive:last";

/// gc_grace pull-only rejoin (assessment Tier-1 #3, Cassandra's oldest
/// rule): a node down longer than gc_grace may hold live records whose
/// delete-tombstones were already purged cluster-wide. Rejoining as an
/// authority would resurrect those deletes. Instead the node stays Joining
/// (via the join gate), pull-syncs each home partition against a healthy
/// owner through the ordinary Merkle machinery, and DROPS the stale extras
/// only it holds (the sync source's RequestKeys enumerates exactly those)
/// rather than serving them. Completion = MerkleRootMatch per partition.
struct RejoinState {
    active: bool,
    /// Scope resolved (requires a view with ≥1 Active other member).
    scoped: bool,
    unsynced: HashSet<Pid>,
}

/// Join-gate state (design/06 §Join): a node holds phase Joining — invisible
/// to HRW placement, /ready 503, RESP not yet listening — until every
/// future-owned partition is bootstrapped, instead of the v1 fixed 2 s sleep
/// that let scale-ups serve empty reads cluster-wide.
#[derive(Default)]
struct JoinGate {
    /// pid → (last BootstrapReq send time, re-request attempts). Attempts
    /// drive exponential backoff: a donor streams its request queue
    /// SEQUENTIALLY, so "no chunk yet" usually means "still queued", not
    /// "lost" — naive fixed-interval retries were measured re-streaming
    /// every partition ~6x during a 100k-key join.
    bootstrap_pending: HashMap<Pid, (Instant, u32)>,
    bootstrap_done: HashSet<Pid>,
    /// pid → last BootstrapChunk arrival (progress signal for retries).
    last_chunk_at: HashMap<Pid, Instant>,
    /// Unfinished bootstraps from a previous incarnation (JOIN_PENDING_KEY):
    /// re-requested even though locally non-empty.
    resume_pids: HashSet<Pid>,
    /// Monotonic count of BootstrapChunks applied; retry sweeps compare it
    /// to `last_sweep_applied` — ANY forward progress defers re-requests.
    /// (Wall-clock staleness checks failed here: duplicate streams flooded
    /// the apply queue, made progress LOOK stale, and re-requested more.)
    chunks_applied: u64,
    last_sweep_applied: u64,
    /// Partitions the gc_grace rejoin still has to full-sync (item #3);
    /// holds the gate exactly like pending bootstraps.
    rejoin_pending: HashSet<Pid>,
    /// A bootstrap sweep completed against a view containing ≥1 Active other
    /// member — required before the gate may open in a populated cluster
    /// (closes the race where the gate is polled before the first sweep).
    swept_active_view: bool,
}

/// Pure gate predicate (unit-tested):
/// - anything pending (bootstrap or rejoin) → not ready
/// - a rejoin that has not resolved its scope yet holds the gate whenever
///   Active others exist (the scope may still turn out non-empty); with no
///   Active others the sole-survivor rules below apply
/// - Active others exist → ready only after a sweep against such a view
/// - only Joining others exist → cold start, ready after the settle window
/// - alone → ready
fn join_ready(
    pending: usize,
    rejoin: usize,
    rejoin_unscoped: bool,
    swept_active_view: bool,
    active_others: bool,
    any_others: bool,
    cold_settled: bool,
) -> bool {
    if pending > 0 || rejoin > 0 {
        return false;
    }
    if active_others {
        return swept_active_view && !rejoin_unscoped;
    }
    if any_others {
        return cold_settled;
    }
    true
}

/// Disk write-stop thresholds: (high, low) used-percent, with hysteresis so
/// the guard cannot flap at the boundary. `low` is clamped below `high`.
fn disk_water_marks() -> (u8, u8) {
    static V: OnceLock<(u8, u8)> = OnceLock::new();
    *V.get_or_init(|| {
        let pct = |name: &str, default: u8| {
            std::env::var(name)
                .ok()
                .and_then(|v| v.parse::<u8>().ok())
                .filter(|&v| v > 0 && v <= 100)
                .unwrap_or(default)
        };
        let high = pct("MAREKVS_DISK_HIGH_WATER_PCT", 90);
        let low = pct("MAREKVS_DISK_LOW_WATER_PCT", 85).min(high.saturating_sub(1));
        (high, low)
    })
}

/// Absolute-headroom gate for the write stop: percent alone misfires on a
/// SHARED filesystem (a dev Docker VM disk routinely runs >90% used while
/// tens of GB remain). ENOSPC is about absolute bytes, so the stop engages
/// only when the percent threshold is crossed AND available space is below
/// this floor. Dedicated volumes (k8s PVCs, the chaos tmpfs) are small
/// enough that the floor is naturally breached at high-water.
fn disk_min_avail_bytes() -> u64 {
    static V: OnceLock<u64> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("MAREKVS_DISK_MIN_AVAIL_MB")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(1024)
            * 1024
            * 1024
    })
}

/// Compaction-debt write-stop thresholds in bytes: (high, low), with the same
/// hysteresis as [`disk_water_marks`] so the guard cannot flap.
///
/// Both sit **below** ondaDB's `hard_pending_compaction_bytes` (default 8 GiB)
/// on purpose. Reaching that ceiling is not an error — it is ondaDB blocking
/// the commit until a compaction finishes — but the block happens on a shard
/// thread and so stalls every key on that shard behind it, with no timeout
/// anywhere in `Store::run`. Refusing the write one step earlier turns an
/// unbounded, unexplained stall into a MISCONF the client can retry and an
/// operator can see. `0` for high disables the guard, leaving ondaDB's own
/// pacing as the only backpressure.
fn compaction_debt_water_marks() -> (u64, u64) {
    static V: OnceLock<(u64, u64)> = OnceLock::new();
    *V.get_or_init(|| {
        let bytes = |name: &str| std::env::var(name).ok().and_then(|v| v.parse::<u64>().ok());
        resolve_debt_water_marks(
            bytes("MAREKVS_COMPACTION_DEBT_HIGH_BYTES"),
            bytes("MAREKVS_COMPACTION_DEBT_LOW_BYTES"),
        )
    })
}

/// The rule behind [`compaction_debt_water_marks`], split out so the clamp is
/// testable without the process environment. Defaults are 6 GiB / 4 GiB.
fn resolve_debt_water_marks(high: Option<u64>, low: Option<u64>) -> (u64, u64) {
    let high = high.unwrap_or(6 << 30);
    if high == 0 {
        return (0, 0); // guard disabled
    }
    // Resume well below the stop so a single compaction completing cannot flap
    // the guard. Clamped under `high` however it is configured: a low-water set
    // at or above high-water would release the guard on the very tick that
    // engaged it, which reads as "the guard is working" while it does nothing.
    (high, low.unwrap_or(4 << 30).min(high - 1))
}

/// Refresh even clean cached AE roots this often: ondadb's TTL backstop
/// purges expired records/tombstones WITHOUT a commit hook, so a purely
/// dirty-driven cache could hold a stale root forever on a quiescent pid.
const AE_ROOT_CACHE_TTL: Duration = Duration::from_secs(600);

/// Cap on owned pids probed per AE round (0 = all): bounds per-round CPU/IO
/// on huge ownership sets; a rotating cursor still covers everything.
fn ae_partitions_per_round() -> usize {
    static V: OnceLock<usize> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("MAREKVS_AE_PARTITIONS_PER_ROUND")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(0)
    })
}

/// Bootstrap stream pacing in bytes/s (design/06 defaults table: 64 MiB/s);
/// MAREKVS_BOOTSTRAP_RATE_MB, 0 = unlimited.
fn bootstrap_rate_bytes_per_sec() -> u64 {
    static V: OnceLock<u64> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("MAREKVS_BOOTSTRAP_RATE_MB")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(64)
            .saturating_mul(1024 * 1024)
    })
}

// ---------------------------------------------------------------------------
// cold purge after ownership loss (T2-9)
// ---------------------------------------------------------------------------

/// `meta` key holding the ms timestamp at which this node stopped owning `pid`.
fn cold_marker_key(pid: Pid) -> Vec<u8> {
    format!("cold:{pid}").into_bytes()
}

/// `meta` key counting clean stranded-AE exchanges for `pid` since the marker.
fn cold_ae_key(pid: Pid) -> Vec<u8> {
    format!("cold_ae:{pid}").into_bytes()
}

/// How long a partition must sit un-owned before its data may be purged.
///
/// Generous by default: the data on an ex-owner is exactly what stranded-record
/// AE uses as its last-copy safety net, so the cost of waiting is disk and the
/// cost of being hasty is deleting the only copy of an unshipped write.
fn cold_purge_delay() -> Duration {
    static V: OnceLock<Duration> = OnceLock::new();
    *V.get_or_init(|| {
        Duration::from_secs(
            std::env::var("MAREKVS_COLD_PURGE_SECS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|&v| v > 0)
                .unwrap_or(900),
        )
    })
}

/// Clean stranded-AE exchanges required before a cold partition may be purged.
/// Each one is positive evidence that a current owner already holds everything
/// we hold for that pid.
fn cold_purge_clean_rounds() -> u64 {
    static V: OnceLock<u64> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("MAREKVS_COLD_PURGE_CLEAN_ROUNDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|&v| v > 0)
            .unwrap_or(3)
    })
}

/// How long a node must be absent from the membership view before its dial
/// loops are torn down (T2-10). Chitchat's own dead-node grace is an hour;
/// this is our shorter, independent timer, kept generous enough that a rolling
/// restart or a brief gossip partition never trips it.
fn mesh_peer_gc_grace() -> Duration {
    static V: OnceLock<Duration> = OnceLock::new();
    *V.get_or_init(|| {
        Duration::from_secs(
            std::env::var("MAREKVS_MESH_PEER_GC_SECS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|&v| v > 0)
                .unwrap_or(300),
        )
    })
}

/// Per-peer unacked send window (bytes). Design/05 defaults table: 4 MiB.
fn repl_window_bytes() -> usize {
    static V: OnceLock<usize> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("MAREKVS_REPL_WINDOW_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&v| v > 0)
            .unwrap_or(4 * 1024 * 1024)
    })
}

/// Per-peer replication flow state. The ring is the retransmit buffer:
/// `sent` advances only on a send that actually entered the writer queue,
/// and a full unacked window stalls THIS peer's lane only — the entries
/// stay in the ring and are re-read once acks drain the window. A peer
/// that outruns the ring entirely hits the existing gap→anti-entropy path.
#[derive(Debug)]
struct PeerFlow {
    /// Ring cursor: highest seq shipped to this peer.
    sent: u64,
    /// Highest seq the peer has acked (AckSeq carries ReplBatch.last_seq).
    acked: u64,
    /// (batch last_seq, batch bytes) awaiting ack, oldest first.
    inflight: VecDeque<(u64, usize)>,
    inflight_bytes: usize,
    stalled_since: Option<Instant>,
    stall_warned: bool,
}

impl PeerFlow {
    fn at(seq: u64) -> PeerFlow {
        PeerFlow {
            sent: seq,
            acked: seq,
            inflight: VecDeque::new(),
            inflight_bytes: 0,
            stalled_since: None,
            stall_warned: false,
        }
    }

    fn window_full(&self, window: usize) -> bool {
        self.inflight_bytes >= window
    }

    fn on_send(&mut self, last_seq: u64, bytes: usize) {
        self.sent = last_seq;
        self.inflight.push_back((last_seq, bytes));
        self.inflight_bytes += bytes;
    }

    fn on_ack(&mut self, seq: u64) {
        // A stale ack from before a ResumeFrom rewind must not run ahead of
        // the rewound cursor.
        let seq = seq.min(self.sent);
        if seq > self.acked {
            self.acked = seq;
        }
        while let Some(&(s, b)) = self.inflight.front() {
            if s <= self.acked {
                self.inflight_bytes -= b;
                self.inflight.pop_front();
            } else {
                break;
            }
        }
        self.stalled_since = None;
        self.stall_warned = false;
    }

    /// Connection died: nothing previously queued can be acked anymore. The
    /// cursor stays — the peer's ResumeFrom on reconnect is authoritative.
    fn clear_inflight(&mut self) {
        self.inflight.clear();
        self.inflight_bytes = 0;
        self.stalled_since = None;
        self.stall_warned = false;
    }
}

pub struct ReplEngine {
    pub store: Arc<Store>,
    pub engine: Arc<Engine>,
    pub cluster: Arc<Cluster>,
    pub mesh: Arc<Mesh>,
    pub ring: Arc<Ring>,
    /// Home side: who is interested in which key (design/04 §Interest).
    interest: Mutex<Interest>,
    /// Subscriber side: freshness leases per user key.
    leases: Mutex<HashMap<Vec<u8>, Instant>>,
    /// In-flight fetch/check requests.
    pending: Mutex<HashMap<u64, oneshot::Sender<PeerMsg>>>,
    next_req: AtomicU64,
    /// Per-peer flow state into OUR ring (cursor + unacked send window;
    /// reset by ResumeFrom).
    flows: Mutex<HashMap<NodeId, PeerFlow>>,
    /// Join gate: bootstrap/rejoin completion tracking (design/06 §Join).
    gate: Mutex<JoinGate>,
    /// gc_grace pull-only rejoin state (Tier-1 #3).
    rejoin: Mutex<RejoinState>,
    /// Donor-side bootstrap dedup: (peer, pid) → last stream start. A
    /// duplicate BootstrapReq within the window is dropped — retry-happy
    /// joiners cannot amplify streams at the source (measured 9x
    /// re-streaming feedback loops before this existed).
    streams_served: Mutex<HashMap<(NodeId, Pid), Instant>>,
    /// AE root cache: pid → (root, computed_at). Entries are invalidated by
    /// the commit hook's dirty set; quiescent partitions cost NO scan per
    /// round (previously the whole keyspace was re-hashed every ~5 s —
    /// linear I/O in data size, Tier-2 #7).
    ae_roots: Mutex<HashMap<Pid, (u64, Instant)>>,
    /// Pids written since their root was last computed (set by the commit
    /// hook on every committed op, including AE repairs and rejoin drops).
    ae_dirty: Arc<Mutex<HashSet<Pid>>>,
    /// This process is a healthy member returning within gc_grace: skip
    /// bootstrapping locally-empty partitions (see ReplEngine::start).
    returning_member: bool,
    started: Instant,
}

impl ReplEngine {
    /// Wire everything and spawn the background tasks. `mesh_listener` is the
    /// bound peer-mesh TCP listener.
    pub async fn start(
        store: Arc<Store>,
        engine: Arc<Engine>,
        cluster: Arc<Cluster>,
        mesh_listener: TcpListener,
        standalone_cfg: bool,
    ) -> Arc<ReplEngine> {
        let (incoming_tx, incoming_rx) = mpsc::channel(8192);
        let (ae_tx, ae_rx) = mpsc::channel(8192);
        let (events_tx, events_rx) = mpsc::unbounded_channel();
        let mesh = Mesh::new(
            store.node_id,
            incoming_tx,
            ae_tx,
            events_tx,
            Some((
                engine.metrics.mesh_input_bytes_total.clone(),
                engine.metrics.mesh_output_bytes_total.clone(),
            )),
            Some(engine.metrics.mesh_conn_timeouts_total.clone()),
        );
        // Resume the ring seq space ABOVE anything consumers may have seen
        // from a previous incarnation (see Ring::new_starting_at). The jump
        // covers ops pushed after the last high-water persist.
        let ring_hw = {
            let store = store.clone();
            store
                .run(0, |ctx| match ctx.db.get(&ctx.meta, b"ring:hw") {
                    Ok(v) if v.len() == 8 => u64::from_be_bytes(v.as_slice().try_into().unwrap()),
                    _ => 0,
                })
                .await
        };
        let ring = Ring::new_starting_at(ring_hw + RING_SEQ_RESTART_JUMP);
        ring.standalone_cfg
            .store(standalone_cfg, std::sync::atomic::Ordering::Relaxed);

        let ae_dirty: Arc<Mutex<HashSet<Pid>>> = Arc::new(Mutex::new(HashSet::new()));

        // Bootstraps left unfinished by a previous incarnation: re-request
        // them even though their partitions are locally non-empty.
        let resume_pids: HashSet<Pid> = {
            let store = store.clone();
            store
                .run(0, |ctx| match ctx.db.get(&ctx.meta, JOIN_PENDING_KEY) {
                    Ok(v) => v
                        .chunks(2)
                        .filter(|c| c.len() == 2)
                        .map(|c| u16::from_be_bytes([c[0], c[1]]))
                        .collect(),
                    _ => HashSet::new(),
                })
                .await
        };
        if !resume_pids.is_empty() {
            tracing::info!(
                pids = resume_pids.len(),
                "resuming bootstraps unfinished at last shutdown"
            );
        }

        // gc_grace rejoin detection: down longer than the tombstone
        // retention → our home partitions may resurrect purged deletes;
        // enter pull-only sync (Tier-1 #3).
        let alive_last = {
            let store = store.clone();
            store
                .run(0, |ctx| match ctx.db.get(&ctx.meta, ALIVE_LAST_KEY) {
                    Ok(v) if v.len() == 8 => u64::from_be_bytes(v.as_slice().try_into().unwrap()),
                    _ => 0,
                })
                .await
        };
        let down_ms = store::now_ms().saturating_sub(alive_last);
        let rejoin_needed =
            !standalone_cfg && alive_last > 0 && down_ms > store::gc_grace().as_millis() as u64;
        // Returning member: was Active (has alive:last) and back within
        // gc_grace. Its data is durable and it was caught up when it left,
        // so its locally-EMPTY partitions are legitimately empty — it must
        // NOT re-bootstrap them (that walks the whole keyspace one empty
        // round-trip at a time; a near-empty cluster made a graceful
        // restart take ~30 s+ and blow the readiness window). Any drift
        // during the brief absence is healed by the ring resume + AE. A
        // fresh/wiped node (no alive:last) still bootstraps, since ITS
        // empty partitions may hold data on donors.
        let returning_member = !standalone_cfg && alive_last > 0 && !rejoin_needed;
        if rejoin_needed {
            tracing::warn!(
                down_secs = down_ms / 1000,
                gc_grace_secs = store::gc_grace().as_secs(),
                "down longer than gc_grace: entering pull-only rejoin — home \
                 partitions sync (and shed stale extras) before serving"
            );
            engine.metrics.rejoin_active.set(1);
        } else if returning_member {
            tracing::info!(
                down_secs = down_ms / 1000,
                "returning member within gc_grace: fast rejoin (data present, \
                 empty partitions not re-bootstrapped; drift healed by AE)"
            );
        }

        let repl = Arc::new(ReplEngine {
            store: store.clone(),
            engine: engine.clone(),
            cluster: cluster.clone(),
            mesh: mesh.clone(),
            ring: ring.clone(),
            interest: Mutex::new(Interest::new(interest_max_entries())),
            leases: Mutex::new(HashMap::new()),
            pending: Mutex::new(HashMap::new()),
            next_req: AtomicU64::new(1),
            flows: Mutex::new(HashMap::new()),
            gate: Mutex::new(JoinGate {
                resume_pids,
                ..JoinGate::default()
            }),
            rejoin: Mutex::new(RejoinState {
                active: rejoin_needed,
                scoped: false,
                unsynced: HashSet::new(),
            }),
            streams_served: Mutex::new(HashMap::new()),
            ae_roots: Mutex::new(HashMap::new()),
            ae_dirty: ae_dirty.clone(),
            returning_member,
            started: Instant::now(),
        });

        // 1. Commit hook: every committed batch enters the ring (skip the
        //    node-local zset score index, tag 'Z').
        {
            let ring = ring.clone();
            let self_node = store.node_id;
            let ae_dirty = ae_dirty.clone();
            store.set_commit_hook(Some(Arc::new(
                move |_seq: u64, ops: &[ondadb::CommitOp]| {
                    // AE root-cache invalidation FIRST — every committed op
                    // (client write, repair, bootstrap chunk, rejoin drop, sweep
                    // tombstone) makes its pid's cached root stale, including
                    // ops the early-returns below skip.
                    {
                        let mut dirty = ae_dirty.lock();
                        for op in ops {
                            if let Some(p) = ikey::parse(&op.key) {
                                dirty.insert(p.pid);
                            }
                        }
                    }
                    // Node-local maintenance writes (rejoin extras deletion)
                    // must not enter the ring.
                    if store::commit_hook_suppressed() {
                        return;
                    }
                    // Configured-standalone with no discovered members → nobody
                    // will ever read the ring; skip the per-record clones. See
                    // Ring::buffering_needed for why this must NOT be gated on
                    // runtime connectivity.
                    if !ring.buffering_needed() {
                        return;
                    }
                    // Attribution comes from the COMMIT CONTEXT (which batch is
                    // being applied on this shard thread), not the record
                    // envelope: merged CRDT records carry the version winner's
                    // origin, which misattributes local commits under clock
                    // skew (see store::set_apply_origin).
                    let origin = marekvs_engine::store::current_apply_origin().unwrap_or(self_node);
                    let mut out: Vec<ReplOp> = Vec::new();
                    for op in ops {
                        if matches!(ikey::parse(&op.key), Some(p) if p.tag == b'Z') {
                            continue;
                        }
                        out.push(ReplOp {
                            ikey: op.key.clone(),
                            value: op.value.clone(),
                        });
                    }
                    if !out.is_empty() {
                        // ondaDB's `seq` is deliberately NOT forwarded: the hook
                        // does not run in commit-seq order (see Ring::push), and
                        // the ring's own counter is what readers assume is sorted.
                        ring.push(origin, out);
                    }
                },
            )));
        }

        // 2. Mesh listener + dialers driven by membership view.
        tokio::spawn(mesh.clone().run_listener(mesh_listener));
        repl.clone().spawn_view_watcher();

        // 3. Incoming message pump.
        repl.clone().spawn_incoming(incoming_rx);
        // Separate pump for the heavy AE/bootstrap lane: digest scans and
        // partition streams must not head-of-line-block fetches.
        repl.clone().spawn_incoming(ae_rx);

        // 4. Peer (re)connect events: send ResumeFrom on connect.
        repl.clone().spawn_peer_events(events_rx);

        // 5. Sender loop: drain the ring to peers.
        repl.clone().spawn_sender();
        repl.clone().spawn_ring_hw();

        // 6. Anti-entropy rounds.
        repl.clone().spawn_ae();

        // 6b. Join-gate bootstrap retry tick.
        repl.clone().spawn_join_retry();
        repl.clone().spawn_cold_purge();

        // 6c. gc_grace rejoin driver (exits immediately when not needed).
        if rejoin_needed {
            repl.clone().spawn_rejoin();
        }

        // 7. Pub/sub cluster fan-out.
        {
            let mesh = mesh.clone();
            engine
                .pubsub
                .set_cluster_hook(Box::new(move |channel, payload| {
                    mesh.broadcast_ctl(&PeerMsg::Publish {
                        channel: channel.to_vec(),
                        payload: payload.to_vec(),
                    });
                }));
        }

        // 8. Read-through hook for the engine.
        engine.set_read_through(repl.clone());

        // 8b. Budget hooks (design/13): peer surface for forwarded grants /
        //     issuer-routed closes, and the manual ring-publish half of the
        //     durable-before-publish pipeline. Manual pushes allocate local
        //     seqs (Ring::push with None) — safe because ondaDB commit seqs
        //     always run at or ahead of ring-local seqs, so ordering never
        //     inverts for resuming cursors.
        engine.set_budget_peer(repl.clone());
        {
            let ring = ring.clone();
            let self_node = store.node_id;
            engine.set_budget_publish(Arc::new(move |ops: Vec<(Vec<u8>, Vec<u8>)>| {
                if !ring.buffering_needed() {
                    return;
                }
                let repl_ops: Vec<ReplOp> = ops
                    .into_iter()
                    .map(|(ikey, value)| ReplOp { ikey, value })
                    .collect();
                if !repl_ops.is_empty() {
                    ring.push(self_node, repl_ops);
                }
            }));
        }

        // 9. Metrics stats task: ring/cluster/mesh gauges every 2 s.
        repl.clone().spawn_stats();

        repl
    }

    fn spawn_stats(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut fs_warned = false;
            let alive_every = store::gc_grace().div_f32(4.0).min(Duration::from_secs(30));
            let mut alive_at: Option<Instant> = None;
            loop {
                tokio::time::sleep(Duration::from_secs(2)).await;
                let m = &self.engine.metrics;
                self.update_disk_guard(&mut fs_warned);
                self.update_counter_field_gate();
                // alive:last heartbeat — only while Active/Leaving: a crash
                // mid-rejoin must keep measuring downtime from the PRE-death
                // timestamp, or a restart during rejoin would skip it.
                if matches!(self.cluster.phase(), NodePhase::Active | NodePhase::Leaving)
                    && alive_at.is_none_or(|t| t.elapsed() >= alive_every)
                {
                    alive_at = Some(Instant::now());
                    let now = store::now_ms();
                    self.store
                        .run(0, move |ctx| {
                            let _ = ctx.db.put(
                                &ctx.meta,
                                ALIVE_LAST_KEY,
                                &now.to_be_bytes(),
                                Duration::ZERO,
                            );
                        })
                        .await;
                    // MUST be durable: with interval fsync, a crash shortly
                    // after going Active destroys the only heartbeat — the
                    // restart then measures no downtime and SKIPS the
                    // gc_grace rejoin, re-opening the delete-resurrection
                    // window. Synced OFF the shard thread: sync_wal inside
                    // a shard closure wedged the shard (observed: total
                    // process silence right after the first heartbeat).
                    let db = self.store.db.clone();
                    let _ = tokio::task::spawn_blocking(move || db.sync_wal()).await;
                }
                let (ops, bytes) = self.ring.occupancy();
                m.ring_ops.set(ops as i64);
                m.ring_bytes.set(bytes as i64);
                m.mesh_peers.set(self.mesh.connected_peers().len() as i64);
                m.repl_inflight_bytes.set(
                    self.flows
                        .lock()
                        .values()
                        .map(|f| f.inflight_bytes)
                        .max()
                        .unwrap_or(0) as i64,
                );
                m.join_gate_pending_pids.set({
                    let g = self.gate.lock();
                    (g.bootstrap_pending.len() + g.rejoin_pending.len()) as i64
                });
                m.interest_entries.set(self.interest.lock().total as i64);
                let stats = self.cluster.cluster_stats();
                m.cluster_members.set(stats.members as i64);
                m.cluster_underreplicated_partitions
                    .set(stats.underreplicated_partitions as i64);
                m.cluster_effective_rf_min
                    .set(stats.effective_rf_min as i64);
                m.cluster_owned_partitions
                    .set(self.cluster.owned_pids().len() as i64);
            }
        });
    }

    /// Disk gauges + write-stop hysteresis (design item #6): stop client
    /// writes at MAREKVS_DISK_HIGH_WATER_PCT used, resume at
    /// MAREKVS_DISK_LOW_WATER_PCT — disk-full is THE unrecoverable LSM
    /// failure (ondadb write errors wedge the node mid-compaction), so it
    /// must become a clean MISCONF error instead. Replication/AE keep
    /// applying (refusing merges = divergence); statvfs failure fails OPEN
    /// with one log line — the gauges going absent is the alert.
    fn update_disk_guard(&self, fs_warned: &mut bool) {
        let m = &self.engine.metrics;
        m.db_total_bytes
            .set(self.store.db.stats().total_bytes as i64);
        // store:: keeps the authoritative count on a plain atomic (it is
        // incremented from shard threads, which hold no Metrics handle); mirror
        // the delta into the registry.
        let scans = store::scan_errors_total();
        let mirrored = m.scan_errors_total.get();
        if scans > mirrored {
            m.scan_errors_total.inc_by(scans - mirrored);
        }

        // ondaDB internals. Resident reader memory is the number that grew to
        // dominate while being invisible before 0.7, and bloom_skips/sst_probes
        // is how an operator can see that filters are actually filtering.
        let (resident, budget) = self.store.db.table_cache_bytes();
        m.db_reader_resident_bytes.set(resident as i64);
        m.db_reader_budget_bytes.set(budget as i64);
        let (open_readers, ..) = self.store.db.table_cache_stats();
        m.db_open_readers.set(open_readers as i64);
        m.db_wal_syncs_total
            .set(self.store.db.wal_sync_count() as i64);
        m.db_l0_files.set(self.store.data.l0_file_count() as i64);
        let cf = self.store.data.stats();
        m.db_bloom_skips_total.set(cf.bloom_skips as i64);
        m.db_sst_probes_total.set(cf.sst_probes as i64);
        // Cold-purge reclamation: one purge is one range delete, and excise is
        // what turns the masked interval back into free disk.
        m.db_range_deletes.set(cf.range_deletes as i64);
        m.db_range_fragments.set(cf.range_fragments as i64);
        m.db_excised_tables.set(cf.excised_tables as i64);
        m.db_excised_bytes.set(cf.excised_bytes as i64);
        m.db_periodic_compactions
            .set(cf.periodic_compactions as i64);
        // `data` only, like the gauges above it: `meta` holds the store epoch
        // and budget slots, so its levels never approach a capacity worth
        // pacing against, and mixing the two would hide which one is behind.
        self.update_compaction_guard(cf.compaction_debt);
        let Some((total, avail)) = store::fs_usage(&self.store.data_dir) else {
            if !*fs_warned {
                *fs_warned = true;
                tracing::warn!(dir = %self.store.data_dir.display(), "statvfs failed; disk guard inactive");
            }
            return;
        };
        m.disk_total_bytes.set(total as i64);
        m.disk_avail_bytes.set(avail as i64);
        if total == 0 {
            return;
        }
        let used_pct = ((total - avail.min(total)) * 100 / total) as u8;
        let stopped = self
            .engine
            .write_stopped
            .load(std::sync::atomic::Ordering::Relaxed);
        let (high, low) = disk_water_marks();
        if !stopped && used_pct >= high && avail < disk_min_avail_bytes() {
            self.engine
                .write_stopped
                .store(true, std::sync::atomic::Ordering::Relaxed);
            m.disk_write_stopped.set(1);
            tracing::error!(
                used_pct,
                high,
                "disk above high-water mark: refusing client write commands"
            );
        } else if stopped && (used_pct <= low || avail >= 2 * disk_min_avail_bytes()) {
            self.engine
                .write_stopped
                .store(false, std::sync::atomic::Ordering::Relaxed);
            m.disk_write_stopped.set(0);
            tracing::info!(
                used_pct,
                low,
                "disk back below low-water mark: accepting writes"
            );
        }
    }

    /// Publish the compaction backlog and stop client writes before ondaDB's
    /// hard pacing ceiling blocks a shard thread (see
    /// [`compaction_debt_water_marks`] and `Engine::compaction_stopped`).
    ///
    /// Deliberately asymmetric in the same way the disk guard is: only client
    /// writes are refused. Replication, anti-entropy and bootstrap apply
    /// through `apply_op_from`, which bypasses `cmd::dispatch` — refusing a
    /// merge is divergence, and a follower must converge even while it is
    /// telling its own clients to back off.
    fn update_compaction_guard(&self, debt: u64) {
        let m = &self.engine.metrics;
        m.db_compaction_debt_bytes.set(debt as i64);
        let (high, low) = compaction_debt_water_marks();
        m.db_compaction_debt_high_bytes.set(high as i64);
        if high == 0 {
            return; // guard disabled; the gauge is still published
        }
        let stopped = self
            .engine
            .compaction_stopped
            .load(std::sync::atomic::Ordering::Relaxed);
        if !stopped && debt >= high {
            self.engine
                .compaction_stopped
                .store(true, std::sync::atomic::Ordering::Relaxed);
            m.db_compaction_write_stopped.set(1);
            tracing::error!(
                debt_bytes = debt,
                high_bytes = high,
                "compaction backlog above high-water mark: refusing client write commands"
            );
        } else if stopped && debt <= low {
            self.engine
                .compaction_stopped
                .store(false, std::sync::atomic::Ordering::Relaxed);
            m.db_compaction_write_stopped.set(0);
            tracing::info!(
                debt_bytes = debt,
                low_bytes = low,
                "compaction backlog back below low-water mark: accepting writes"
            );
        }
    }

    fn req_id(&self) -> u64 {
        self.next_req.fetch_add(1, Ordering::Relaxed)
    }

    fn pseudo_rand(&self) -> u64 {
        // xxh3 of a monotonic value: good enough for peer picking/jitter.
        xxhash_rust::xxh3::xxh3_64(&store::now_ms().to_le_bytes())
            ^ self.next_req.fetch_add(1, Ordering::Relaxed)
    }

    // ------------------------------------------------------------------
    // background tasks
    // ------------------------------------------------------------------

    fn spawn_view_watcher(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut watch = self.cluster.watch();
            let mut dialed: HashMap<NodeId, std::net::SocketAddr> = HashMap::new();
            // First time each dialed peer was seen missing from the view.
            // Cleared the moment it reappears, so a flapping node never
            // accumulates towards the grace.
            let mut absent_since: HashMap<NodeId, Instant> = HashMap::new();
            loop {
                let view = self.cluster.view();
                self.ring
                    .members
                    .store(view.members.len(), std::sync::atomic::Ordering::Relaxed);
                // --- peer GC (T2-10) ---
                // Departed nodes are otherwise redialed until process exit.
                // Gossip's own dead-node grace is an hour; this timer is ours
                // and starts at first absence from the view.
                let present: HashSet<NodeId> = view.members.iter().map(|m| m.node).collect();
                let grace = mesh_peer_gc_grace();
                let mut forget: Vec<NodeId> = Vec::new();
                for peer in dialed.keys().copied() {
                    if present.contains(&peer) {
                        absent_since.remove(&peer);
                    } else {
                        let first = *absent_since.entry(peer).or_insert_with(Instant::now);
                        if first.elapsed() >= grace {
                            forget.push(peer);
                        }
                    }
                }
                for peer in forget {
                    self.mesh.forget_peer(peer);
                    // Drop the replication state that only made sense while
                    // the peer existed. A reappearance re-inits the cursor via
                    // ResumeFrom on reconnect.
                    self.flows.lock().remove(&peer);
                    self.interest.lock().remove_peer(peer);
                    dialed.remove(&peer);
                    absent_since.remove(&peer);
                    self.engine.metrics.mesh_peers_forgotten_total.inc();
                }
                self.track_ownership_loss().await;
                for m in &view.members {
                    // Lower id dials (design/04 §Transport). Re-dial when a
                    // restarted peer gossips a NEW mesh address — the old
                    // reconnect loops dial a dead IP forever otherwise
                    // (chaos finding: mesh-orphaned node on Apple
                    // containers, where every restart changes the IP).
                    if m.node > self.store.node_id && dialed.get(&m.node) != Some(&m.mesh_addr) {
                        dialed.insert(m.node, m.mesh_addr);
                        self.mesh.clone().maintain_peer(m.node, m.mesh_addr).await;
                    }
                }
                self.request_bootstraps(&view).await;
                if watch.changed().await.is_err() {
                    return;
                }
            }
        });
    }

    /// For newly-owned, locally-empty partitions ask an existing owner for a
    /// bootstrap stream (design/06 §Join). Tracks every request in the join
    /// gate and re-requests stalled ones (donor died mid-stream, bulk conn
    /// not up yet at the first view change) — callers re-run this on view
    /// changes and on the 2 s retry tick.
    async fn request_bootstraps(&self, view: &marekvs_cluster::View) {
        let n = self.cluster.replicas_n;
        let self_id = self.store.node_id;
        let mut changed = false;
        let mut re_requests = 0usize;
        // Global progress signal: if ANY chunk was applied since the last
        // sweep, the donor pipeline is alive and merely working through its
        // SEQUENTIAL queue — re-requesting queued pids only duplicates
        // streams (measured up to 9x amplification, a runaway feedback
        // loop). Monotonic counter compare, deliberately NOT wall-clock:
        // a flooded apply queue makes wall-clock progress look stale.
        let pipe_active = {
            let mut g = self.gate.lock();
            let advanced = g.chunks_applied != g.last_sweep_applied;
            g.last_sweep_applied = g.chunks_applied;
            advanced
        };
        let future: HashSet<Pid> = self.cluster.future_owned_pids().into_iter().collect();
        // Prune pending entries for pids that left future-ownership: early
        // partial-view sweeps can request nearly EVERY pid (2 candidates →
        // self is in the top-2 of all of them); the wrongly-picked donors
        // refuse, the view completes, ownership shrinks — and without this
        // the refused orphans stayed pending forever and the gate never
        // opened (flaky cold-start/rejoin stalls).
        {
            let mut g = self.gate.lock();
            let before = g.bootstrap_pending.len() + g.resume_pids.len();
            g.bootstrap_pending.retain(|pid, _| future.contains(pid));
            g.resume_pids.retain(|pid| future.contains(pid));
            if g.bootstrap_pending.len() + g.resume_pids.len() != before {
                changed = true;
            }
        }
        for pid in future.iter().copied() {
            let (skip, resume, attempts) = {
                let now = Instant::now();
                let g = self.gate.lock();
                // Done pids are never re-requested: nothing empties a
                // partition today (no cold purge), so done-and-empty cannot
                // occur; revisit when cold_purge lands.
                let entry = g.bootstrap_pending.get(&pid);
                let attempts = entry.map(|(_, a)| *a).unwrap_or(0);
                // Capped backoff (5/10/20 s): early attempts often hit
                // refusing donors while gossip is still delivering members;
                // punitive exponential tiers stalled joins for minutes.
                let backoff = BOOTSTRAP_RETRY * 2u32.saturating_pow(attempts.min(2));
                let awaiting_req = entry.is_some_and(|(t, _)| now.duration_since(*t) < backoff);
                let progressing = g
                    .last_chunk_at
                    .get(&pid)
                    .is_some_and(|t| now.duration_since(*t) < BOOTSTRAP_RETRY);
                let skip = g.bootstrap_done.contains(&pid)
                    || awaiting_req
                    || progressing
                    || (pipe_active && entry.is_some())
                    // Rejoin-scoped pids are being Merkle-synced by the
                    // rejoin driver; bootstrap-streaming them too would
                    // re-copy data the node already has.
                    || g.rejoin_pending.contains(&pid);
                let next_attempts = if entry.is_some() { attempts + 1 } else { 0 };
                (skip, g.resume_pids.contains(&pid), next_attempts)
            };
            if skip {
                continue;
            }
            // Cap re-requests per sweep: the donor's stream queue is
            // sequential — flooding it with duplicates only slows the join.
            if attempts > 0 {
                re_requests += 1;
                if re_requests > 256 {
                    continue;
                }
            }
            let owners = view.owners(pid, n);
            let Some(source) = owners.iter().find(|o| **o != self_id).copied() else {
                continue;
            };
            let probe = self
                .store
                .run(pid, move |ctx| {
                    let mut any = false;
                    store::scan_prefix(ctx, &ikey::partition_prefix(pid), |_, _| {
                        any = true;
                        false
                    })?;
                    Ok::<bool, store::ScanIncomplete>(!any)
                })
                .await;
            // "Empty" decides whether this partition is bootstrapped at all, so
            // a failed probe must not answer it. Re-evaluate next round.
            let Ok(empty) = probe else {
                self.engine.metrics.scan_errors_total.inc();
                continue;
            };
            // A returning member does NOT bootstrap its empty partitions
            // (they were empty when it left; AE heals any drift). Only fresh
            // nodes pull empty partitions, and only interrupted joins resume.
            if (empty && !self.returning_member) || resume {
                self.gate
                    .lock()
                    .bootstrap_pending
                    .insert(pid, (Instant::now(), attempts));
                changed = true;
                self.mesh
                    .send_bulk(source, PeerMsg::BootstrapReq { pid })
                    .await;
            }
        }
        let active_others = view
            .members
            .iter()
            .any(|m| m.node != self_id && m.phase == NodePhase::Active);
        if active_others {
            self.gate.lock().swept_active_view = true;
        }
        if changed {
            self.persist_join_pending().await;
        }
    }

    /// Persist the set of pids with unfinished bootstraps (crash resume).
    async fn persist_join_pending(&self) {
        let bytes: Vec<u8> = {
            let g = self.gate.lock();
            let mut pids: Vec<Pid> = g
                .bootstrap_pending
                .keys()
                .chain(g.resume_pids.iter())
                .copied()
                .collect();
            pids.sort_unstable();
            pids.dedup();
            pids.iter().flat_map(|p| p.to_be_bytes()).collect()
        };
        self.store
            .run(0, move |ctx| {
                let _ = ctx
                    .db
                    .put(&ctx.meta, JOIN_PENDING_KEY, &bytes, Duration::ZERO);
            })
            .await;
    }

    /// Evaluate the join gate against the current view (see `join_ready`).
    fn join_gate_ready(&self) -> bool {
        let view = self.cluster.view();
        let self_id = self.store.node_id;
        let (pending, rejoin, swept) = {
            let g = self.gate.lock();
            (
                g.bootstrap_pending.len(),
                g.rejoin_pending.len(),
                g.swept_active_view,
            )
        };
        let rejoin_unscoped = {
            let r = self.rejoin.lock();
            r.active && !r.scoped
        };
        let active_others = view
            .members
            .iter()
            .any(|m| m.node != self_id && m.phase == NodePhase::Active);
        let any_others = view.members.iter().any(|m| m.node != self_id);
        join_ready(
            pending,
            rejoin,
            rejoin_unscoped,
            swept,
            active_others,
            any_others,
            self.started.elapsed() >= COLD_START_SETTLE,
        )
    }

    /// Hold until every future-owned partition is bootstrapped (and, after a
    /// gc_grace outage, re-synced). `timeout: None` waits forever — the safe
    /// default: a node that cannot finish must stay Joining (unready) rather
    /// than serve empty reads. Returns false on timeout.
    pub async fn wait_join_ready(&self, timeout: Option<Duration>) -> bool {
        let deadline = timeout.map(|t| Instant::now() + t);
        loop {
            if self.join_gate_ready() {
                return true;
            }
            if deadline.is_some_and(|d| Instant::now() >= d) {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    /// Called once the node reaches Active: the join is complete, so clear
    /// the crash-resume marker. `join:pending` (and its in-memory resume
    /// set) exist ONLY to finish a join interrupted mid-bootstrap; a node
    /// that reached Active owns its data and any LATER restart is a healthy
    /// returning member that recovers via ring resume + anti-entropy, not a
    /// re-bootstrap. Leaving stale entries made a graceful restart
    /// force-re-request partitions it already held — which the donor-side
    /// stream dedup then refused, hanging the join (chaos bank scenario).
    pub async fn mark_join_complete(&self) {
        {
            let mut g = self.gate.lock();
            g.resume_pids.clear();
            g.bootstrap_pending.clear();
        }
        self.store
            .run(0, |ctx| {
                let _ = ctx.db.delete(&ctx.meta, JOIN_PENDING_KEY);
            })
            .await;
    }

    // NOTE: writes committed on donors between a pid's bootstrap scan and
    // this node turning Active are consumed past the donors' cursors (we
    // were not an owner yet) and are healed by the REGULAR AE rounds within
    // ~15 s — bounded staleness on writes-during-join, within the AP
    // contract. An eager "probe every bootstrapped pid at Active" kick was
    // tried and reverted: thousands of simultaneous MerkleRoot probes force
    // cold digest scans on the donors, whose sequential incoming pumps then
    // starve read-through fetches into their 300 ms timeout — turning the
    // join moment into exactly the empty-reads window the gate exists to
    // close (chaos: join_empty_reads, 254/792 nil with the kick in place).

    /// gc_grace rejoin driver: resolve the sync scope once a view with
    /// Active others exists, then probe each unsynced home partition with an
    /// ordinary MerkleRoot every round — the reactive AE machinery pulls
    /// what we lack, and RequestKeys enumerates the stale extras we shed.
    /// MerkleRootMatch per pid marks completion. Exits when done.
    fn spawn_rejoin(self: Arc<Self>) {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(AE_ROUND).await;
                if !self.rejoin.lock().active {
                    return;
                }
                let view = self.cluster.view();
                let self_id = self.store.node_id;
                let active_others = view
                    .members
                    .iter()
                    .any(|m| m.node != self_id && m.phase == NodePhase::Active);
                if !self.rejoin.lock().scoped {
                    if !active_others {
                        // Sole survivor: the join gate's cold/alone rules let
                        // the node go Active with nobody to sync from — and
                        // there is no other copy to protect. Stand down.
                        if self.cluster.phase() == NodePhase::Active {
                            tracing::warn!(
                                "gc_grace rejoin: Active with no Active peers \
                                 (sole survivor) — pull-only sync skipped"
                            );
                            self.finish_rejoin();
                            return;
                        }
                        continue;
                    }
                    // Scope: home partitions with local data need a full
                    // sync; empty ones take the normal bootstrap gate.
                    let mut unsynced: HashSet<Pid> = HashSet::new();
                    for pid in self.cluster.future_owned_pids() {
                        // Err = we could not read the partition. Scope it in:
                        // syncing a partition that turns out to be empty costs
                        // one round, while skipping one that holds data leaves
                        // it permanently unsynced.
                        match self.partition_root_cached(pid).await {
                            Ok(0) => {}
                            Ok(_) | Err(_) => {
                                unsynced.insert(pid);
                            }
                        }
                    }
                    self.gate.lock().rejoin_pending = unsynced.clone();
                    let empty = unsynced.is_empty();
                    {
                        let mut r = self.rejoin.lock();
                        r.unsynced = unsynced;
                        r.scoped = true;
                    }
                    tracing::info!(
                        pids = self.gate.lock().rejoin_pending.len(),
                        "gc_grace rejoin: pull-only sync scoped"
                    );
                    if empty {
                        self.finish_rejoin();
                        return;
                    }
                }
                let pids: Vec<Pid> = self.rejoin.lock().unsynced.iter().copied().collect();
                for pid in pids {
                    // Sync against the pid's CO-OWNER (self-included
                    // placement): the peer that held the data alongside us
                    // before the outage. Syncing against a current-view
                    // owner is WRONG — with us excluded, that set includes
                    // a node that became owner at our crash and may still
                    // be empty (re-replication in progress); its Merkle
                    // want-set is "everything" and the extras-drop would
                    // destroy our valid copy.
                    let co: Vec<NodeId> = self
                        .cluster
                        .future_co_owners(pid)
                        .into_iter()
                        .filter(|o| {
                            view.members
                                .iter()
                                .any(|m| m.node == *o && m.phase == NodePhase::Active)
                        })
                        .collect();
                    if co.is_empty() {
                        // Co-owner down: hold this pid; the driver retries
                        // every round, and the sole-survivor/timeout paths
                        // bound the overall wait.
                        continue;
                    }
                    let peer = co[(self.pseudo_rand() % co.len() as u64) as usize];
                    let Ok(root) = self.partition_root_cached(pid).await else {
                        // Never complete a rejoin pid on a failed scan: that
                        // would declare it synced on the strength of a digest
                        // we never computed. Retry next round.
                        self.engine.metrics.ae_scan_failures_total.inc();
                        continue;
                    };
                    if root == 0 {
                        // Drained empty (all extras dropped, nothing pulled
                        // yet counts as synced-empty for placement purposes).
                        self.complete_rejoin_pid(pid);
                        continue;
                    }
                    self.mesh.send_ctl(peer, PeerMsg::MerkleRoot { pid, root });
                }
            }
        });
    }

    fn finish_rejoin(&self) {
        {
            let mut r = self.rejoin.lock();
            r.active = false;
            r.unsynced.clear();
        }
        self.gate.lock().rejoin_pending.clear();
        self.engine.metrics.rejoin_active.set(0);
        tracing::info!("gc_grace rejoin complete");
    }

    /// Enable counter-valued hash fields only once the whole cluster can read
    /// them (T2-12).
    ///
    /// Stricter than "every connected peer announces the bit": every non-self
    /// member of the current **view** must be connected *and* announce it. A
    /// peer that is in the view but unreachable is exactly the case that would
    /// otherwise slip through — we would start writing counters while it is
    /// partitioned, and it would serve their raw payload to clients on heal.
    ///
    /// This is still a heuristic, not a cluster-wide epoch: a node that is
    /// gone from the view entirely (past the failure detector) cannot be
    /// consulted. The operational rule is the usual one — upgrade every node,
    /// and the feature turns itself on.
    fn update_counter_field_gate(&self) {
        let view = self.cluster.view();
        let connected: HashSet<NodeId> = self.mesh.connected_peers().into_iter().collect();
        let ok = view
            .members
            .iter()
            .filter(|m| m.node != self.store.node_id)
            .all(|m| {
                connected.contains(&m.node)
                    && self
                        .mesh
                        .peer_features(m.node)
                        .is_some_and(|f| f & marekvs_proto::features::COUNTER_FIELD != 0)
            });
        let was = self
            .engine
            .counter_fields
            .swap(ok, std::sync::atomic::Ordering::Relaxed);
        if was != ok {
            tracing::info!(
                enabled = ok,
                "counter-valued hash fields (HINCRBY PN semantics) toggled"
            );
        }
    }

    // -----------------------------------------------------------------
    // cold purge after ownership loss (T2-9)
    // -----------------------------------------------------------------

    /// Stamp partitions this node just stopped owning, and un-stamp ones it
    /// owns again.
    ///
    /// The marker is persisted in `meta` rather than held in memory because the
    /// purge delay must survive restarts — an ex-owner that bounces every few
    /// minutes would otherwise reset its clock forever and never reclaim the
    /// disk, which is the leak this exists to close.
    async fn track_ownership_loss(&self) {
        let owned: HashSet<Pid> = self.cluster.owned_pids().into_iter().collect();
        self.store
            .run(0, move |ctx| {
                for pid in 0..marekvs_core::PARTITIONS as Pid {
                    let key = cold_marker_key(pid);
                    let marked = matches!(ctx.db.get(&ctx.meta, &key), Ok(v) if v.len() == 8);
                    if owned.contains(&pid) {
                        // Owned again: drop the marker AND the evidence count,
                        // so a later loss starts its delay and its clean-round
                        // tally from scratch.
                        if marked {
                            let _ = ctx.db.delete(&ctx.meta, &key);
                            let _ = ctx.db.delete(&ctx.meta, &cold_ae_key(pid));
                        }
                    } else if !marked {
                        let _ = ctx.db.put(
                            &ctx.meta,
                            &key,
                            &store::now_ms().to_be_bytes(),
                            Duration::ZERO,
                        );
                        let _ = ctx.db.delete(&ctx.meta, &cold_ae_key(pid));
                    }
                }
            })
            .await;
    }

    /// Record one clean stranded-AE exchange for a partition we no longer own:
    /// an owner answered `MerkleRootMatch`, i.e. it holds exactly what we hold.
    fn note_clean_cold_round(&self, pid: Pid) {
        self.store.spawn_on(0, move |ctx| {
            let key = cold_marker_key(pid);
            // Only count while the partition is actually marked cold; a match
            // for an owned pid says nothing about safe-to-purge.
            if !matches!(ctx.db.get(&ctx.meta, &key), Ok(v) if v.len() == 8) {
                return;
            }
            let k = cold_ae_key(pid);
            let n = match ctx.db.get(&ctx.meta, &k) {
                Ok(v) if v.len() == 8 => u64::from_be_bytes(v.as_slice().try_into().unwrap()),
                _ => 0,
            };
            let _ = ctx
                .db
                .put(&ctx.meta, &k, &(n + 1).to_be_bytes(), Duration::ZERO);
        });
    }

    /// Slow reclaim loop: drop the local copy of partitions this node has not
    /// owned for a while and that a healthy set of owners provably holds.
    ///
    /// Every condition is a fence against deleting a last copy, which is the
    /// only way this can go wrong:
    ///
    /// * the marker must be older than [`cold_purge_delay`];
    /// * at least [`cold_purge_clean_rounds`] stranded-AE exchanges must have
    ///   come back **matched** since the marker — direct evidence an owner has
    ///   our bytes, not merely time passing;
    /// * the current view must show a full `replicas_n` set of **Active**
    ///   owners, so we never purge into a degraded cluster;
    /// * no rejoin may be in progress — a rejoining node is quarantined and its
    ///   view of ownership is deliberately not trusted (T1-3).
    fn spawn_cold_purge(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(60));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                if self.rejoin.lock().active {
                    continue;
                }
                let delay_ms = cold_purge_delay().as_millis() as u64;
                let need_rounds = cold_purge_clean_rounds();
                let n = self.cluster.replicas_n;
                let view = self.cluster.view();
                let owned: HashSet<Pid> = self.cluster.owned_pids().into_iter().collect();

                for pid in 0..marekvs_core::PARTITIONS as Pid {
                    if owned.contains(&pid) {
                        continue;
                    }
                    // Never purge into a degraded cluster: if the owners are
                    // not all Active we may be holding the redundancy.
                    let owners = view.owners(pid, n);
                    let healthy = owners.len() >= n
                        && owners.iter().all(|o| {
                            view.members.iter().any(|m| {
                                m.node == *o && m.phase == marekvs_cluster::NodePhase::Active
                            })
                        });
                    if !healthy {
                        continue;
                    }

                    let (aged, rounds) = self
                        .store
                        .run(pid, move |ctx| {
                            let marked = match ctx.db.get(&ctx.meta, &cold_marker_key(pid)) {
                                Ok(v) if v.len() == 8 => {
                                    Some(u64::from_be_bytes(v.as_slice().try_into().unwrap()))
                                }
                                _ => None,
                            };
                            let rounds = match ctx.db.get(&ctx.meta, &cold_ae_key(pid)) {
                                Ok(v) if v.len() == 8 => {
                                    u64::from_be_bytes(v.as_slice().try_into().unwrap())
                                }
                                _ => 0,
                            };
                            let aged = marked
                                .is_some_and(|at| store::now_ms().saturating_sub(at) >= delay_ms);
                            (aged, rounds)
                        })
                        .await;
                    if !aged || rounds < need_rounds {
                        continue;
                    }

                    match self.purge_partition(pid).await {
                        Ok(()) => {
                            self.engine.metrics.cold_purged_partitions_total.inc();
                            tracing::info!(
                                pid,
                                rounds,
                                "cold purge: dropped local copy of an un-owned partition"
                            );
                        }
                        Err(e) => {
                            tracing::warn!(pid, error = %e, "cold purge range delete failed");
                        }
                    }
                }
            }
        });
    }

    /// Physically drop this node's records for `pid` with a single range delete.
    ///
    /// Deliberately a *local* drop: this node is discarding its own copy of a
    /// partition it no longer owns, not deleting the records cluster-wide.
    /// ondaDB never surfaces a range delete to a commit hook, so — unlike the
    /// scan-and-tombstone predecessor, which needed an explicit
    /// `suppress_commit_hook()` guard for exactly this — nothing here can reach
    /// the replication ring even by accident.
    ///
    /// There is no chunking and no inter-chunk yield any more, because there is
    /// no per-record work to spread out. The commit does take ondaDB's
    /// database-wide commit lock (a range commit is atomic against every
    /// isolation level), which is why this is only ever called for a whole cold
    /// partition and never on a client path.
    async fn purge_partition(&self, pid: Pid) -> anyhow::Result<()> {
        self.store
            .run(pid, move |ctx| store::delete_partition_range(ctx, pid))
            .await?;

        // Reclaim eagerly. A purge is exactly what delete-only excise is for:
        // a whole interval proven deleted, so a table lying entirely inside it
        // can be unlinked by catalog edit without reading a byte of it. The
        // range delete alone only masks the records; this is what returns the
        // space.
        //
        // Best-effort by design — excise declines whenever a table is busy (an
        // overlapping compaction, an in-flight part move, a foreign mount), and
        // that is a skip rather than an error. A failure here must not fail a
        // purge that has already succeeded.
        let db = self.store.db.clone();
        let data = self.store.data.clone();
        match tokio::task::spawn_blocking(move || db.excise_covered(&data)).await {
            Ok(Ok(n)) if n > 0 => {
                tracing::info!(pid, tables = n, "excise retired tables after purge")
            }
            Ok(Ok(_)) => {}
            Ok(Err(e)) => tracing::warn!(pid, error = ?e, "excise pass failed after purge"),
            Err(e) => tracing::warn!(pid, error = %e, "excise task panicked after purge"),
        }
        Ok(())
    }

    fn complete_rejoin_pid(&self, pid: Pid) {
        let done = {
            let mut r = self.rejoin.lock();
            if !r.active {
                return;
            }
            r.unsynced.remove(&pid);
            tracing::debug!(pid, remaining = r.unsynced.len(), "rejoin pid synced");
            r.scoped && r.unsynced.is_empty()
        };
        self.gate.lock().rejoin_pending.remove(&pid);
        if done {
            self.finish_rejoin();
        }
    }

    /// Retry tick for the join gate: re-run the bootstrap sweep while
    /// anything is pending, so a dead donor or a not-yet-dialed bulk conn
    /// cannot stall the join forever.
    fn spawn_join_retry(self: Arc<Self>) {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(2)).await;
                let idle = {
                    let g = self.gate.lock();
                    g.bootstrap_pending.is_empty() && g.resume_pids.is_empty()
                };
                if idle {
                    continue;
                }
                let view = self.cluster.view();
                self.request_bootstraps(&view).await;
            }
        });
    }

    fn spawn_peer_events(self: Arc<Self>, mut rx: mpsc::UnboundedReceiver<(NodeId, bool)>) {
        tokio::spawn(async move {
            while let Some((peer, connected)) = rx.recv().await {
                if connected {
                    // Tell the peer where we are in THEIR sequence stream.
                    let seq = self.applied_seq(peer).await;
                    self.mesh.send_ctl(
                        peer,
                        PeerMsg::ResumeFrom {
                            origin: self.store.node_id,
                            seq,
                        },
                    );
                } else {
                    // Their leases die with the connection (design/04).
                    self.interest.lock().remove_peer(peer);
                    // Nothing queued on the dead connection can be acked; a
                    // vanished peer must not hold its window full forever.
                    if let Some(flow) = self.flows.lock().get_mut(&peer) {
                        flow.clear_inflight();
                    }
                }
            }
        });
    }

    /// Periodically persist the ring high-water mark so a restart can resume
    /// the seq space above every consumer cursor (paired with the restart
    /// jump in ReplEngine::start).
    fn spawn_ring_hw(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut persisted = 0u64;
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let last = self.ring.last_seq();
                if last != persisted {
                    persisted = last;
                    self.store
                        .run(0, move |ctx| {
                            let _ = ctx.db.put(
                                &ctx.meta,
                                b"ring:hw",
                                &last.to_be_bytes(),
                                Duration::ZERO,
                            );
                        })
                        .await;
                }
            }
        });
    }

    fn spawn_sender(self: Arc<Self>) {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = self.ring.notify.notified() => {}
                    _ = tokio::time::sleep(Duration::from_millis(50)) => {}
                }
                self.pump_peers();
            }
        });
    }

    /// Drain new ring entries to every connected peer under the fan-out rule.
    fn pump_peers(&self) {
        let peers = self.mesh.connected_peers();
        if peers.is_empty() {
            return;
        }
        let view = self.cluster.view();
        let n = self.cluster.replicas_n;
        let last = self.ring.last_seq();
        let window = repl_window_bytes();
        for peer in peers {
            // Window gate + cursor snapshot. A full window = unacked bytes in
            // flight to a slow peer: stall THIS lane only, others unaffected.
            let cursor = {
                let mut flows = self.flows.lock();
                let flow = flows.entry(peer).or_insert_with(|| PeerFlow::at(last));
                if flow.window_full(window) {
                    // Count every skipped pass: trickling acks reset the
                    // stall clock below, so a duration-gated counter would
                    // stay 0 under exactly the slow-peer condition it
                    // exists to expose. The 5 s rule gates the LOG only.
                    self.engine.metrics.repl_window_stalls_total.inc();
                    match flow.stalled_since {
                        None => flow.stalled_since = Some(Instant::now()),
                        Some(t) if !flow.stall_warned && t.elapsed() >= STALL_WARN => {
                            flow.stall_warned = true;
                            tracing::warn!(
                                peer,
                                inflight_bytes = flow.inflight_bytes,
                                acked = flow.acked,
                                sent = flow.sent,
                                "replication window full for >{STALL_WARN:?}: peer slow or not acking"
                            );
                        }
                        _ => {}
                    }
                    continue;
                }
                flow.sent
            };
            if cursor >= last {
                continue;
            }
            let (entries, gap) = self.ring.read_after(cursor, BATCH_MAX_OPS);
            if gap {
                tracing::warn!(peer, "ring gap for peer; relying on anti-entropy");
            }
            if entries.is_empty() {
                continue;
            }
            let first_seq = entries.first().unwrap().seq;
            let interest = self.interest.lock();
            // Walk entries, deciding per entry whether it goes to `peer`.
            // CRITICAL: an entry whose partition has an EMPTY owner set means
            // the membership view has not converged (e.g. peers still gossiped
            // as Joining right after boot) — advancing the cursor past it
            // would drop the push permanently and demote convergence to
            // anti-entropy latency. Defer instead: stop here, keep the cursor
            // before this entry, retry on the next pump with a fresher view.
            let mut ops: Vec<ReplOp> = Vec::new();
            let mut new_cursor = cursor;
            let mut batch_bytes: usize = 0;
            for e in entries {
                let Some(p) = ikey::parse(&e.op.ikey) else {
                    new_cursor = e.seq;
                    continue;
                };
                let owners = view.owners(p.pid, n);
                if owners.is_empty() || (owners.len() == 1 && owners[0] == self.store.node_id) {
                    let others = view.members.iter().any(|m| m.node != self.store.node_id);
                    if others {
                        // Members exist but none placement-eligible yet:
                        // unconverged view → defer this entry.
                        break;
                    }
                    // Genuinely alone: nothing to push, safe to advance.
                    new_cursor = e.seq;
                    continue;
                }
                new_cursor = e.seq;
                let is_h1 = view.h1(p.pid, n) == Some(self.store.node_id);
                // homes: only ops we originated
                let mut send = e.origin == self.store.node_id
                    && owners.contains(&peer)
                    && peer != self.store.node_id;
                // interest fan-out: only H1 forwards, never back to origin
                if !send && is_h1 && e.origin != peer && !owners.contains(&peer) {
                    if let Some(subs) = interest.subs(p.pid, p.userkey) {
                        if subs.get(&peer).is_some_and(|exp| *exp > Instant::now()) {
                            send = true;
                        }
                    }
                }
                if send {
                    batch_bytes += e.op.ikey.len() + e.op.value.len();
                    ops.push(e.op);
                    // Byte cap: a 256-op batch of large values can exceed
                    // MAX_FRAME (8 MiB); encode then fails in the writer task
                    // and the frame is dropped SILENTLY — a hole even the ack
                    // window cannot see (later cumulative acks cover it).
                    // Stop the batch here; the next pump continues after it.
                    if batch_bytes >= BATCH_MAX_BYTES {
                        break;
                    }
                }
            }
            drop(interest);
            if ops.is_empty() {
                // Nothing for this peer in the covered stretch: advance the
                // cursor freely — nothing is in flight, the window is
                // untouched, so filtered-only streams never stall.
                let mut flows = self.flows.lock();
                if let Some(flow) = flows.get_mut(&peer) {
                    if flow.sent == cursor {
                        flow.sent = new_cursor;
                    }
                }
                tracing::debug!(peer, cursor, new_cursor, "pump: entries filtered to zero");
                continue;
            }
            tracing::debug!(peer, n = ops.len(), "pump: sending ReplBatch");
            let op_count = ops.len() as u64;
            let sent_ok = self.mesh.send_ctl(
                peer,
                PeerMsg::Repl(ReplBatch {
                    origin: self.store.node_id,
                    first_seq,
                    last_seq: new_cursor,
                    ops,
                    implicit_sub: false,
                }),
            );
            let mut flows = self.flows.lock();
            let Some(flow) = flows.get_mut(&peer) else {
                continue;
            };
            if sent_ok {
                // Advance only if no ResumeFrom rewound the cursor while we
                // were sending — a rewind is authoritative and the ring will
                // re-serve (merges are idempotent, double delivery is safe).
                if flow.sent == cursor {
                    flow.on_send(new_cursor, batch_bytes);
                }
                self.engine.metrics.repl_batches_sent_total.inc();
                self.engine.metrics.repl_ops_sent_total.inc_by(op_count);
            } else {
                // Writer queue full or peer gone: the cursor did NOT move, so
                // the ring re-serves this stretch on the next pump. This was
                // previously a silent drop demoted to anti-entropy latency.
                self.engine.metrics.repl_send_failures_total.inc();
            }
        }
    }

    /// Max unshipped-or-unacked ring backlog across connected peers
    /// (0 = fully drained). The SIGTERM drain polls this so a leaving node
    /// does not exit with acked-but-unshipped writes only it holds; counting
    /// in-flight batches makes "drained" mean the peer APPLIED them, not
    /// merely that they entered a writer queue.
    pub fn pending_backlog(&self) -> u64 {
        let last = self.ring.last_seq();
        let flows = self.flows.lock();
        self.mesh
            .connected_peers()
            .iter()
            .map(|p| match flows.get(p) {
                Some(f) => last.saturating_sub(f.sent) + u64::from(!f.inflight.is_empty()),
                None => 0,
            })
            .max()
            .unwrap_or(0)
    }

    fn spawn_ae(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut round = 0u64;
            loop {
                let jitter = self.pseudo_rand() % 2000;
                tokio::time::sleep(AE_ROUND + Duration::from_millis(jitter)).await;
                self.gc_interest();
                self.engine.metrics.ae_rounds_total.inc();
                round = round.wrapping_add(1);
                let owned = self.cluster.owned_pids();
                let view = self.cluster.view();
                let n = self.cluster.replicas_n;
                // Optional per-round cap on huge ownership sets: a rotating
                // window still covers every pid across successive rounds.
                let cap = ae_partitions_per_round();
                let probe: Vec<Pid> = if cap > 0 && owned.len() > cap {
                    let start = (round as usize).wrapping_mul(cap) % owned.len();
                    owned
                        .iter()
                        .cycle()
                        .skip(start)
                        .take(cap)
                        .copied()
                        .collect()
                } else {
                    owned.clone()
                };
                for pid in probe {
                    let owners = view.owners(pid, n);
                    let others: Vec<NodeId> = owners
                        .into_iter()
                        .filter(|o| *o != self.store.node_id)
                        .collect();
                    if others.is_empty() {
                        continue;
                    }
                    let peer = others[(self.pseudo_rand() % others.len() as u64) as usize];
                    // Offering a root we could not compute would advertise an
                    // empty partition and invite the peer to "repair" us.
                    let Ok(root) = self.partition_root_cached(pid).await else {
                        self.engine.metrics.ae_scan_failures_total.inc();
                        continue;
                    };
                    self.mesh.send_ctl(peer, PeerMsg::MerkleRoot { pid, root });
                }
                // Stranded-record AE (chaos findings): every few rounds, also
                // exchange roots for pids we hold data for but do NOT own.
                // Owners-only AE has a structural blind spot — when unshipped
                // ring entries die with a crashed process, the owners AGREE
                // with each other and the origin's copy is invisible forever.
                // The exchange is push-only (no_backfill) so a non-owner
                // never accumulates partition data it merely offered to.
                if round % 3 == 0 && !self.rejoin.lock().active {
                    let owned_set: std::collections::HashSet<Pid> = owned.iter().copied().collect();
                    for pid in 0..marekvs_core::PARTITIONS as Pid {
                        if owned_set.contains(&pid) {
                            continue;
                        }
                        let owners = view.owners(pid, n);
                        if owners.is_empty() {
                            continue;
                        }
                        let Ok(root) = self.partition_root_cached(pid).await else {
                            self.engine.metrics.ae_scan_failures_total.inc();
                            continue;
                        };
                        if root == 0 {
                            continue; // no local data for this pid
                        }
                        let peer = owners[(self.pseudo_rand() % owners.len() as u64) as usize];
                        self.mesh.send_ctl(peer, PeerMsg::MerkleRoot { pid, root });
                    }
                }
            }
        });
    }

    fn gc_interest(&self) {
        self.interest.lock().gc(Instant::now());
    }

    /// Cached partition Merkle root: recompute (one partition scan) only
    /// when the pid was written since the last compute, or the entry aged
    /// past AE_ROOT_CACHE_TTL (ondadb's TTL purge bypasses the commit
    /// hook). Quiescent partitions cost no I/O per AE round — previously
    /// the full keyspace was re-hashed every ~5 s.
    async fn partition_root_cached(&self, pid: Pid) -> ae::AeResult<u64> {
        if !self.ae_dirty.lock().contains(&pid) {
            if let Some((root, at)) = self.ae_roots.lock().get(&pid) {
                if at.elapsed() < AE_ROOT_CACHE_TTL {
                    return Ok(*root);
                }
            }
        }
        // Clear BEFORE scanning: writes landing mid-scan re-mark the pid,
        // so an invalidation can never be lost (worst case: one extra scan).
        self.ae_dirty.lock().remove(&pid);
        let root = match ae::partition_root(&self.store, pid).await {
            Ok(root) => root,
            Err(e) => {
                // A root computed from a partial scan must never be cached: it
                // would be trusted for AE_ROOT_CACHE_TTL and, for a scan that
                // failed early, it is exactly the "partition is empty"
                // sentinel. Re-mark the pid so the next round rescans it.
                self.ae_dirty.lock().insert(pid);
                return Err(e);
            }
        };
        self.ae_roots.lock().insert(pid, (root, Instant::now()));
        self.engine.metrics.ae_digest_scans_total.inc();
        Ok(root)
    }

    // ------------------------------------------------------------------
    // incoming messages
    // ------------------------------------------------------------------

    fn spawn_incoming(self: Arc<Self>, mut rx: mpsc::Receiver<(NodeId, PeerMsg)>) {
        tokio::spawn(async move {
            while let Some((peer, msg)) = rx.recv().await {
                self.handle(peer, msg).await;
            }
        });
    }

    async fn handle(self: &Arc<Self>, peer: NodeId, msg: PeerMsg) {
        match msg {
            PeerMsg::Hello { .. } => {}
            PeerMsg::Repl(batch) => {
                tracing::debug!(peer, n = batch.ops.len(), "recv ReplBatch");
                self.engine.metrics.repl_batches_received_total.inc();
                self.engine
                    .metrics
                    .repl_ops_applied_total
                    .inc_by(batch.ops.len() as u64);
                for op in &batch.ops {
                    self.apply_op_from(op.clone(), Some(batch.origin)).await;
                }
                if batch.implicit_sub {
                    self.register_interest_ops(peer, &batch.ops);
                }
                // Ack/persist the seq the batch COVERS (incl. entries the
                // sender filtered out for us), not first_seq + count: the
                // sender's window drains against its cursor, and ResumeFrom
                // must not rewind over filtered-only stretches.
                self.store_applied_seq(peer, batch.last_seq).await;
                self.mesh.send_ctl(
                    peer,
                    PeerMsg::AckSeq {
                        origin: self.store.node_id,
                        seq: batch.last_seq,
                    },
                );
            }
            PeerMsg::AckSeq { seq, .. } => {
                if let Some(flow) = self.flows.lock().get_mut(&peer) {
                    flow.on_ack(seq);
                }
            }
            PeerMsg::ResumeFrom { seq, .. } => {
                // Reconnect: the peer's persisted applied-seq is authoritative;
                // in-flight accounting from the dead connection is void.
                self.flows.lock().insert(peer, PeerFlow::at(seq));
            }
            PeerMsg::Fetch { id, ikey } => {
                let k = ikey.clone();
                let value = self
                    .store
                    .run(ikey::parse(&ikey).map(|p| p.pid).unwrap_or(0), move |ctx| {
                        store::get_raw(ctx, &k)
                    })
                    .await;
                self.mesh.send_ctl(
                    peer,
                    PeerMsg::FetchResp {
                        id,
                        value,
                        lease_ms: INTEREST_LEASE.as_millis() as u64,
                    },
                );
            }
            PeerMsg::FetchCollection { id, userkey } => {
                self.engine.metrics.fetches_served_total.inc();
                // Do not answer with a partial collection: the requester caches
                // the response under an INTEREST_LEASE and would serve the
                // missing elements as absent for the whole lease. Staying
                // silent makes it time out and retry.
                let Ok(ops) = self.collect_userkey_records(&userkey).await else {
                    self.engine.metrics.scan_errors_total.inc();
                    return;
                };
                self.register_interest(peer, &userkey);
                self.mesh.send_ctl(
                    peer,
                    PeerMsg::FetchCollectionResp {
                        id,
                        ops,
                        lease_ms: INTEREST_LEASE.as_millis() as u64,
                    },
                );
            }
            PeerMsg::Check { id, ikey, hlc } => {
                let k = ikey.clone();
                let newer = self
                    .store
                    .run(ikey::parse(&ikey).map(|p| p.pid).unwrap_or(0), move |ctx| {
                        store::get_raw(ctx, &k)
                            .filter(|v| Envelope::decode(v).is_some_and(|(e, _)| e.hlc > hlc))
                    })
                    .await;
                self.mesh.send_ctl(
                    peer,
                    PeerMsg::CheckResp {
                        id,
                        newer,
                        lease_ms: INTEREST_LEASE.as_millis() as u64,
                    },
                );
            }
            PeerMsg::FetchResp { id, .. }
            | PeerMsg::FetchCollectionResp { id, .. }
            | PeerMsg::CheckResp { id, .. }
            | PeerMsg::BudgetReserveResp { id, .. }
            | PeerMsg::BudgetCloseResp { id, .. } => {
                if let Some(tx) = self.pending.lock().remove(&id) {
                    let _ = tx.send(msg);
                }
            }
            PeerMsg::BudgetReserve {
                id,
                key,
                amount,
                ttl_ms,
                reqid,
            } => {
                // Forwarded grant: this node grants from ITS OWN escrow slot,
                // completing the durable-before-publish pipeline before the
                // response leaves (design/13).
                let result = marekvs_engine::cmd::budget::serve_remote_reserve(
                    &self.engine,
                    &key,
                    amount,
                    ttl_ms,
                    reqid,
                )
                .await
                .map(|g| BudgetGrantWire {
                    token: g.token.into_bytes(),
                    amount: g.amount,
                    deadline_ms: g.deadline_ms,
                })
                .map_err(budget_err_to_wire);
                self.mesh
                    .send_ctl(peer, PeerMsg::BudgetReserveResp { id, result });
            }
            PeerMsg::BudgetClose {
                id,
                key,
                token,
                spent,
                draw,
                release,
            } => {
                let tid = marekvs_core::budget::TokenId {
                    gen: token.gen,
                    hlc: token.hlc,
                    node: token.node,
                    epoch: token.epoch,
                };
                let result = marekvs_engine::cmd::budget::serve_remote_close(
                    &self.engine,
                    &key,
                    tid,
                    spent,
                    draw,
                    release,
                )
                .await
                .map_err(budget_err_to_wire);
                self.mesh
                    .send_ctl(peer, PeerMsg::BudgetCloseResp { id, result });
            }
            PeerMsg::InterestRenew { keys, .. } => {
                for k in keys {
                    self.register_interest(peer, &k);
                }
            }
            PeerMsg::MerkleRoot { pid, root } => {
                // Staying silent is the only safe answer to a failed scan here.
                // Replying MerkleRootMatch would let a rejoining peer mark the
                // partition synced against a digest we never computed, and
                // replying with zeroed buckets would invite it to "repair" data
                // we in fact hold. Either way the peer retries next round.
                let Ok(ours) = self.partition_root_cached(pid).await else {
                    self.engine.metrics.ae_scan_failures_total.inc();
                    return;
                };
                if ours != root {
                    let Ok(digests) = ae::bucket_digests(&self.store, pid).await else {
                        self.engine.metrics.ae_scan_failures_total.inc();
                        return;
                    };
                    self.mesh
                        .send_ctl(peer, PeerMsg::MerkleBuckets { pid, digests });
                } else {
                    self.mesh.send_ctl(peer, PeerMsg::MerkleRootMatch { pid });
                }
            }
            PeerMsg::MerkleRootMatch { pid } => {
                // gc_grace rejoin: this partition is confirmed in sync with
                // a healthy owner; no-op outside a rejoin.
                self.complete_rejoin_pid(pid);
                // Cold purge (T2-9): for a partition we no longer own, a match
                // is direct evidence that an owner holds exactly our bytes —
                // the evidence the purge fence requires.
                self.note_clean_cold_round(pid);
            }
            PeerMsg::MerkleBuckets { pid, digests } => {
                let Ok(ours) = ae::bucket_digests(&self.store, pid).await else {
                    self.engine.metrics.ae_scan_failures_total.inc();
                    return;
                };
                let we_own = self.cluster.owned_pids().contains(&pid);
                // A rejoining node owns nothing (it is Joining) but WANTS
                // backfill for the home partitions it is re-syncing.
                let rejoin_wants = {
                    let r = self.rejoin.lock();
                    r.active && r.unsynced.contains(&pid)
                };
                for (bucket, (a, b)) in ours.iter().zip(digests.iter()).enumerate() {
                    if a != b {
                        let Ok(entries) = ae::bucket_entries(&self.store, pid, bucket as u8).await
                        else {
                            self.engine.metrics.ae_scan_failures_total.inc();
                            continue;
                        };
                        self.mesh.send_ctl(
                            peer,
                            PeerMsg::BucketKeys {
                                pid,
                                bucket: bucket as u8,
                                entries,
                                no_backfill: !we_own && !rejoin_wants,
                            },
                        );
                    }
                }
            }
            PeerMsg::BucketKeys {
                pid,
                bucket,
                entries,
                no_backfill,
            } => {
                let Ok((mut push, want)) =
                    ae::diff_bucket(&self.store, pid, bucket, &entries).await
                else {
                    // A partial diff both under-pushes and over-requests; the
                    // next round recomputes it from a complete scan.
                    self.engine.metrics.ae_scan_failures_total.inc();
                    return;
                };
                if no_backfill {
                    // Stranded exchange: never GROW the non-owner's cache,
                    // but DO refresh records it already holds (it offered
                    // them). A stale non-owner cache otherwise serves
                    // lease-valid stale reads for up to the 60 s lease —
                    // e.g. a delete it missed while ownership flapped
                    // through a partition (chaos: partition_no_resurrect).
                    let offered: std::collections::HashSet<u64> =
                        entries.iter().map(|(h, _, _)| *h).collect();
                    push.retain(|op| offered.contains(&xxhash_rust::xxh3::xxh3_64(&op.ikey)));
                }
                if !push.is_empty() {
                    self.mesh
                        .send_ctl(peer, PeerMsg::RepairOps { pid, ops: push });
                }
                if !want.is_empty() {
                    self.mesh.send_ctl(
                        peer,
                        PeerMsg::RequestKeys {
                            pid,
                            bucket,
                            ikey_hashes: want,
                        },
                    );
                }
            }
            PeerMsg::RequestKeys {
                pid,
                bucket,
                ikey_hashes,
            } => {
                // The rejoin branch below DELETES every record in `ops`, and the
                // serve branch treats it as the complete answer to the peer's
                // request. A partial scan must reach neither.
                let Ok(ops) = ae::records_by_hash(&self.store, pid, bucket, &ikey_hashes).await
                else {
                    self.engine.metrics.ae_scan_failures_total.inc();
                    return;
                };
                let (rejoin_active, rejoin_home) = {
                    let r = self.rejoin.lock();
                    (r.active, r.unsynced.contains(&pid))
                };
                if rejoin_active
                    && rejoin_home
                    && self.cluster.future_co_owners(pid).contains(&peer)
                {
                    // gc_grace rejoin: every record our CO-OWNER requests
                    // from an unsynced home partition is dropped
                    // UNCONDITIONALLY (Cassandra's down-past-gc_grace rule).
                    // A node down longer than gc_grace received nothing while
                    // down, so everything it holds predates its crash; the
                    // co-owner had the whole outage to converge and is
                    // authoritative. A record it lacks (hence requests) is
                    // either a replicated-then-deleted resurrection or a
                    // never-shipped write forfeit by the rule — both drop.
                    //
                    // (An earlier HLC-vs-last-heartbeat gate left records
                    // written in the seconds between the final heartbeat and
                    // the crash neither dropped nor served — the partition
                    // root then never re-matched and the rejoin hung forever.
                    // The co-owner restriction is what makes unconditional
                    // drop safe: any OTHER requester — e.g. a crash-era owner
                    // still re-replicating — legitimately lacks most of the
                    // partition and is refused below WITHOUT deletion.)
                    let mut dropped = 0u64;
                    for op in ops {
                        let ik = op.ikey;
                        self.store
                            .run(pid, move |ctx| {
                                let _g = store::suppress_commit_hook();
                                store::del_raw(ctx, &ik);
                            })
                            .await;
                        dropped += 1;
                    }
                    if dropped > 0 {
                        self.engine
                            .metrics
                            .rejoin_dropped_records_total
                            .inc_by(dropped);
                        tracing::info!(pid, dropped, "rejoin: dropped stale extra records");
                    }
                } else if rejoin_active {
                    // While rejoining, NEVER serve records to anyone else:
                    // - home pids to a non-co-owner (e.g. a crash-era owner
                    //   being re-replicated): our records may be stale
                    //   post-gc_grace state — serving them is the
                    //   resurrection vector via a third party. Refuse, but
                    //   do NOT delete (only the co-owner's want-set is
                    //   proof of "extras only we hold").
                    // - non-home stranded data: refuse but keep — it may be
                    //   the last copy of validly-unshipped writes. (After
                    //   Active, stranded-AE resumes; documented residual.)
                } else if !ops.is_empty() {
                    self.mesh.send_ctl(peer, PeerMsg::RepairOps { pid, ops });
                }
            }
            PeerMsg::RepairOps { pid, ops } => {
                self.engine
                    .metrics
                    .ae_repair_ops_total
                    .inc_by(ops.len() as u64);
                self.apply_ops_batch(pid, ops, Some(peer)).await;
            }
            PeerMsg::BootstrapReq { pid } => {
                // Refuse pids we do not own: a joiner acting on a partial
                // early gossip view can pick the wrong donor, and streaming
                // our empty/stray copy + Done would mark the pid
                // bootstrapped FOREVER (chaos join_empty_reads: ~1/3 of a
                // joiner's partitions were "done" from a non-owner and then
                // served empty home reads). No reply = the joiner's pending
                // entry holds its join gate and its backoff re-requests
                // from the right owner once views converge.
                if !self.cluster.owned_pids().contains(&pid) {
                    tracing::info!(pid, peer, "refusing bootstrap request: not an owner");
                } else {
                    // Dedup: a joiner may re-request a pid that is merely
                    // queued behind our other streams — serving duplicates
                    // amplifies (up to 9x measured) and floods its apply
                    // path. One stream per (peer, pid) per window.
                    let dup = {
                        let now = Instant::now();
                        let mut served = self.streams_served.lock();
                        served.retain(|_, t| now.duration_since(*t) < 4 * BOOTSTRAP_RETRY);
                        match served.entry((peer, pid)) {
                            // Do NOT re-stamp an occupied entry: a lost
                            // stream must become re-servable when the
                            // window ends.
                            std::collections::hash_map::Entry::Occupied(_) => true,
                            std::collections::hash_map::Entry::Vacant(slot) => {
                                slot.insert(now);
                                false
                            }
                        }
                    };
                    if dup {
                        tracing::debug!(pid, peer, "duplicate bootstrap request ignored");
                    } else {
                        self.stream_partition(peer, pid).await;
                    }
                }
            }
            PeerMsg::BootstrapChunk { pid, ops } => {
                self.apply_ops_batch(pid, ops, Some(peer)).await;
                let mut g = self.gate.lock();
                g.last_chunk_at.insert(pid, Instant::now());
                g.chunks_applied += 1;
            }
            PeerMsg::BootstrapDone { pid, .. } => {
                tracing::debug!(pid, peer, "partition bootstrap complete");
                {
                    let mut g = self.gate.lock();
                    g.bootstrap_pending.remove(&pid);
                    g.last_chunk_at.remove(&pid);
                    g.resume_pids.remove(&pid);
                    g.bootstrap_done.insert(pid);
                    // Dones count as pipeline progress too: a cold-start
                    // cohort's streams are ALL EMPTY (no chunks), and
                    // without this the retry gate saw a "dead" pipeline and
                    // re-requested thousands of empty partitions each tier.
                    g.chunks_applied += 1;
                }
                self.engine.metrics.bootstraps_completed_total.inc();
                self.persist_join_pending().await;
            }
            PeerMsg::Publish { channel, payload } => {
                self.engine.pubsub.publish_local(&channel, &payload);
            }
            PeerMsg::Ping { nonce } => {
                self.mesh.send_ctl(peer, PeerMsg::Pong { nonce });
            }
            PeerMsg::Pong { .. } => {}
        }
    }

    // ------------------------------------------------------------------
    // apply / fetch plumbing
    // ------------------------------------------------------------------

    /// Merge a remote record into local storage on its shard thread.
    /// Zset member records also rebuild the local score index.
    pub async fn apply_op(&self, op: ReplOp) {
        self.apply_op_from(op, None).await
    }

    /// Apply a replicated record, attributing the resulting commit to
    /// `origin` (the node whose seq space delivered it) for ring
    /// echo-suppression. None = unknown source (counts as remote: u16::MAX
    /// suppresses the origin==self home push without claiming a peer).
    pub async fn apply_op_from(&self, op: ReplOp, origin: Option<NodeId>) {
        let Some(p) = ikey::parse(&op.ikey) else {
            return;
        };
        // HLC receive rule (Kulkarni): observe every ingested record's
        // timestamp so our next local write sorts after everything we have
        // seen. Without this, a peer with a lagging wall clock loses LWW
        // merges against values it causally read first — exposed by Apple
        // containers, where every container is its own VM with its own
        // clock (Docker shares one VM clock and can't catch it).
        if let Some((env, _)) = Envelope::decode(&op.value) {
            if self.store.hlc.is_drifted(env.hlc) {
                tracing::warn!(hlc = env.hlc, "clamping far-future remote HLC");
            }
            self.store.hlc.observe(env.hlc);
        }
        let pid = p.pid;
        let (userkey, suffix, tag) = (p.userkey.to_vec(), p.suffix.to_vec(), p.tag);
        let attr = origin.unwrap_or(u16::MAX);
        self.store
            .run(pid, move |ctx| {
                let _guard = store::set_apply_origin(attr);
                if tag == b'z' {
                    marekvs_engine::cmd::zset::apply_member_record(
                        ctx, &userkey, &suffix, &op.value,
                    );
                } else {
                    store::write_merged(ctx, &op.ikey, &op.value);
                    // List elements carry a node-local head/tail position hint
                    // that write_merged does not maintain; drop it so the next
                    // local list op re-derives the range including this record.
                    if tag == b'q' {
                        marekvs_engine::cmd::list::invalidate_hint(ctx, &userkey);
                    }
                }
            })
            .await;
    }

    /// Apply a same-partition batch of records in ONE shard-thread closure.
    /// Bootstrap chunks and repair batches previously paid one channel
    /// round trip PER RECORD (apply_op_from), which made a 100k-record join
    /// apply-bound: chunks queued for minutes, the join gate's progress
    /// signal looked dead, and retries amplified the streams.
    pub async fn apply_ops_batch(&self, pid: Pid, ops: Vec<ReplOp>, origin: Option<NodeId>) {
        if ops.is_empty() {
            return;
        }
        // HLC receive rule for every record BEFORE the batch lands (same
        // semantics as apply_op_from).
        for op in &ops {
            if let Some((env, _)) = Envelope::decode(&op.value) {
                if self.store.hlc.is_drifted(env.hlc) {
                    tracing::warn!(hlc = env.hlc, "clamping far-future remote HLC");
                }
                self.store.hlc.observe(env.hlc);
            }
        }
        let attr = origin.unwrap_or(u16::MAX);
        self.store
            .run(pid, move |ctx| {
                let _guard = store::set_apply_origin(attr);
                for op in &ops {
                    let Some(p) = ikey::parse(&op.ikey) else {
                        continue;
                    };
                    if p.pid != pid {
                        // Wrong-partition record in a per-pid batch: apply
                        // via the slow path is impossible on this shard —
                        // skip and let AE repair (should not happen).
                        tracing::warn!(pid, "batch record for foreign pid skipped");
                        continue;
                    }
                    if p.tag == b'z' {
                        marekvs_engine::cmd::zset::apply_member_record(
                            ctx, p.userkey, p.suffix, &op.value,
                        );
                    } else {
                        store::write_merged(ctx, &op.ikey, &op.value);
                        if p.tag == b'q' {
                            marekvs_engine::cmd::list::invalidate_hint(ctx, p.userkey);
                        }
                    }
                }
            })
            .await;
    }

    /// All records of one user key (string + list + head + elements),
    /// verbatim — for FetchCollection responses and bootstrap.
    async fn collect_userkey_records(
        &self,
        userkey: &[u8],
    ) -> Result<Vec<ReplOp>, store::ScanIncomplete> {
        let uk = userkey.to_vec();
        self.store
            .run_key(userkey, move |ctx| {
                let mut ops = Vec::new();
                for k in [
                    ikey::string_key(&uk),
                    ikey::list_key(&uk),
                    ikey::head_key(&uk),
                ] {
                    if let Some(v) = store::get_raw(ctx, &k) {
                        ops.push(ReplOp { ikey: k, value: v });
                    }
                }
                for tag in ikey::ELEMENT_TAGS {
                    store::scan_prefix(ctx, &ikey::collection_prefix(tag, &uk), |k, v| {
                        ops.push(ReplOp {
                            ikey: k.to_vec(),
                            value: v.to_vec(),
                        });
                        true
                    })?;
                }
                Ok(ops)
            })
            .await
    }

    fn register_interest(&self, peer: NodeId, userkey: &[u8]) {
        let pid = marekvs_core::pid_of(userkey);
        let ok = self
            .interest
            .lock()
            .register(pid, userkey, peer, Instant::now() + INTEREST_LEASE);
        if !ok {
            // At cap: the subscriber still gets its full lease and simply
            // re-fetches on expiry — worst-case-lease staleness instead of
            // unbounded memory. Counted so operators can size the cap.
            self.engine.metrics.interest_rejected_total.inc();
        }
    }

    fn register_interest_ops(&self, peer: NodeId, ops: &[ReplOp]) {
        for op in ops {
            if let Some(p) = ikey::parse(&op.ikey) {
                self.register_interest(peer, p.userkey);
            }
        }
    }

    async fn applied_seq(&self, origin: NodeId) -> u64 {
        let key = format!("cur:{origin}").into_bytes();
        self.store
            .run(0, move |ctx| match ctx.db.get(&ctx.meta, &key) {
                Ok(v) if v.len() == 8 => u64::from_be_bytes(v.try_into().unwrap()),
                _ => 0,
            })
            .await
    }

    async fn store_applied_seq(&self, origin: NodeId, seq: u64) {
        let key = format!("cur:{origin}").into_bytes();
        self.store
            .run(0, move |ctx| {
                let _ = ctx
                    .db
                    .put(&ctx.meta, &key, &seq.to_be_bytes(), Duration::ZERO);
            })
            .await;
    }

    /// Stream a partition to a joining/repairing peer (design/06 §Bootstrap).
    async fn stream_partition(&self, peer: NodeId, pid: Pid) {
        let mut sent = 0usize;
        // Rate pacing (design/06: 64 MiB/s): unthrottled streaming saturates
        // the donor's disk/network during scale events — exactly when p99
        // matters most. Sleep just enough to hold the configured rate.
        let rate = bootstrap_rate_bytes_per_sec();
        let started = Instant::now();
        let mut streamed_bytes: u64 = 0;
        // Resume cursor: the internal key of the last record emitted. Each
        // chunk seeks straight past it. The offset-skip this replaced re-walked
        // every already-sent record on every chunk, making a partition stream
        // quadratic (O(n²/256) records visited) in partition size.
        let prefix = ikey::partition_prefix(pid);
        let mut cursor: Option<Vec<u8>> = None;
        loop {
            let from = cursor.clone();
            let prefix_for_chunk = prefix.clone();
            let chunk = self
                .store
                .run(pid, move |ctx| {
                    let mut ops: Vec<ReplOp> = Vec::new();
                    // `scan_from` visits `start` itself, so resume at the
                    // immediate successor of the last key emitted.
                    let start = match &from {
                        Some(last) => {
                            let mut s = last.clone();
                            s.push(0);
                            s
                        }
                        None => prefix_for_chunk.clone(),
                    };
                    store::scan_from(ctx, &start, &prefix_for_chunk, |k, v| {
                        if matches!(ikey::parse(k), Some(p) if p.tag == b'Z') {
                            return true;
                        }
                        ops.push(ReplOp {
                            ikey: k.to_vec(),
                            value: v.to_vec(),
                        });
                        ops.len() < 256
                    })?;
                    Ok::<Vec<ReplOp>, store::ScanIncomplete>(ops)
                })
                .await;
            let Ok(chunk) = chunk else {
                // Abandon the stream rather than fall through to
                // BootstrapDone, which would tell the peer it holds a complete
                // partition while everything past the failure is missing. The
                // pid stays bootstrap-pending and the join gate retries.
                self.engine.metrics.scan_errors_total.inc();
                tracing::warn!(
                    pid,
                    peer,
                    records = sent,
                    "bootstrap stream aborted: partition scan incomplete"
                );
                return;
            };
            if chunk.is_empty() {
                break;
            }
            cursor = chunk.last().map(|o| o.ikey.clone());
            sent += chunk.len();
            let chunk_bytes: u64 = chunk
                .iter()
                .map(|o| (o.ikey.len() + o.value.len()) as u64)
                .sum();
            if !self
                .mesh
                .send_bulk(peer, PeerMsg::BootstrapChunk { pid, ops: chunk })
                .await
            {
                return;
            }
            streamed_bytes += chunk_bytes;
            self.engine
                .metrics
                .bootstrap_bytes_sent_total
                .inc_by(chunk_bytes);
            if rate > 0 {
                let due = Duration::from_secs_f64(streamed_bytes as f64 / rate as f64);
                if let Some(ahead) = due.checked_sub(started.elapsed()) {
                    tokio::time::sleep(ahead).await;
                }
            }
        }
        self.mesh
            .send_bulk(
                peer,
                PeerMsg::BootstrapDone {
                    pid,
                    as_of_seq: self.ring.last_seq(),
                },
            )
            .await;
        tracing::info!(pid, peer, records = sent, "partition streamed");
    }

    /// Request/response over ctl with timeout.
    async fn request(&self, peer: NodeId, id: u64, msg: PeerMsg) -> Option<PeerMsg> {
        let (tx, rx) = oneshot::channel();
        self.pending.lock().insert(id, tx);
        if !self.mesh.send_ctl(peer, msg) {
            self.pending.lock().remove(&id);
            return None;
        }
        match tokio::time::timeout(FETCH_TIMEOUT, rx).await {
            Ok(Ok(resp)) => Some(resp),
            _ => {
                self.pending.lock().remove(&id);
                None
            }
        }
    }
}

fn budget_err_to_wire(e: BudgetErr) -> BudgetErrKind {
    match e {
        BudgetErr::Exhausted => BudgetErrKind::Exhausted,
        BudgetErr::NoBudget => BudgetErrKind::NoBudget,
        BudgetErr::TryAgain(_) => BudgetErrKind::TryAgain,
        BudgetErr::TokenExpired => BudgetErrKind::TokenExpired,
        BudgetErr::TokenUsed => BudgetErrKind::TokenUsed,
        BudgetErr::Other(msg) => BudgetErrKind::Other(msg),
    }
}

fn budget_err_from_wire(e: BudgetErrKind) -> BudgetErr {
    match e {
        BudgetErrKind::Exhausted => BudgetErr::Exhausted,
        BudgetErrKind::NoBudget => BudgetErr::NoBudget,
        BudgetErrKind::TryAgain => BudgetErr::TryAgain("remote node asked to retry"),
        BudgetErrKind::TokenExpired => BudgetErr::TokenExpired,
        BudgetErrKind::TokenUsed => BudgetErr::TokenUsed,
        BudgetErrKind::Other(msg) => BudgetErr::Other(msg),
    }
}

impl BudgetPeer for ReplEngine {
    fn reserve_remote<'a>(
        &'a self,
        node: u16,
        key: &'a [u8],
        amount: u64,
        ttl_ms: u64,
        reqid: u64,
    ) -> Pin<Box<dyn Future<Output = Result<BudgetGrant, BudgetErr>> + Send + 'a>> {
        Box::pin(async move {
            let id = self.req_id();
            let msg = PeerMsg::BudgetReserve {
                id,
                key: key.to_vec(),
                amount,
                ttl_ms,
                reqid,
            };
            match self.request(node, id, msg).await {
                Some(PeerMsg::BudgetReserveResp { result, .. }) => match result {
                    Ok(g) => Ok(BudgetGrant {
                        token: String::from_utf8_lossy(&g.token).into_owned(),
                        amount: g.amount,
                        deadline_ms: g.deadline_ms,
                    }),
                    Err(e) => Err(budget_err_from_wire(e)),
                },
                _ => Err(BudgetErr::TryAgain("peer unreachable")),
            }
        })
    }

    fn close_remote<'a>(
        &'a self,
        node: u16,
        key: &'a [u8],
        token: &'a [u8],
        spent: Option<u64>,
        draw: Option<u64>,
        release: bool,
    ) -> Pin<Box<dyn Future<Output = Result<u64, BudgetErr>> + Send + 'a>> {
        Box::pin(async move {
            let Some(tid) = marekvs_core::budget::TokenId::parse(token) else {
                return Err(BudgetErr::Other("ERR invalid token id".into()));
            };
            let id = self.req_id();
            let msg = PeerMsg::BudgetClose {
                id,
                key: key.to_vec(),
                token: BudgetTokenId {
                    gen: tid.gen,
                    hlc: tid.hlc,
                    node: tid.node,
                    epoch: tid.epoch,
                },
                spent,
                draw,
                release,
            };
            match self.request(node, id, msg).await {
                Some(PeerMsg::BudgetCloseResp { result, .. }) => {
                    result.map_err(budget_err_from_wire)
                }
                _ => Err(BudgetErr::TryAgain(
                    "issuer unreachable; retry before the token deadline",
                )),
            }
        })
    }

    /// Boot grant-fence fetch (design/13 fix 4): pull the budget collection
    /// from a reachable owner and MERGE it locally, even when this node is
    /// itself a home for the partition — a fresh-epoch home holds nothing.
    fn fetch_budget<'a>(
        &'a self,
        key: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
        Box::pin(async move {
            let pid = marekvs_core::pid_of(key);
            let view = self.cluster.view();
            let n = self.cluster.replicas_n;
            let mut targets = view.owners(pid, n);
            targets.retain(|t| *t != self.store.node_id);
            if targets.is_empty() {
                // No other owner exists (standalone / N=1): local state is
                // all there is.
                return true;
            }
            for target in targets {
                let id = self.req_id();
                let msg = PeerMsg::FetchCollection {
                    id,
                    userkey: key.to_vec(),
                };
                if let Some(PeerMsg::FetchCollectionResp { ops, .. }) =
                    self.request(target, id, msg).await
                {
                    for op in &ops {
                        self.apply_op_from(op.clone(), Some(target)).await;
                    }
                    return true;
                }
            }
            false
        })
    }

    fn owners_for(&self, key: &[u8]) -> Vec<u16> {
        let pid = marekvs_core::pid_of(key);
        self.cluster.view().owners(pid, self.cluster.replicas_n)
    }
}

impl ReadThrough for ReplEngine {
    /// Read path steps 2–4 of design/04: home → serve local; lease-valid →
    /// serve local; else fetch the whole user key from H1 and subscribe.
    fn fetch<'a>(&'a self, userkey: &'a [u8]) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
        Box::pin(async move {
            let pid = marekvs_core::pid_of(userkey);
            if self.cluster.is_home(pid) {
                return false;
            }
            // Lease still fresh? Serve the local copy.
            if let Some(exp) = self.leases.lock().get(userkey) {
                if *exp > Instant::now() {
                    return false;
                }
            }
            let view = self.cluster.view();
            let n = self.cluster.replicas_n;
            let mut targets = view.owners(pid, n);
            targets.retain(|t| *t != self.store.node_id);
            if let Some(h1) = view.h1(pid, n) {
                if let Some(pos) = targets.iter().position(|t| *t == h1) {
                    targets.swap(0, pos);
                }
            }
            for target in targets {
                let id = self.req_id();
                self.engine.metrics.fetches_issued_total.inc();
                let msg = PeerMsg::FetchCollection {
                    id,
                    userkey: userkey.to_vec(),
                };
                if let Some(PeerMsg::FetchCollectionResp { ops, lease_ms, .. }) =
                    self.request(target, id, msg).await
                {
                    for op in &ops {
                        self.apply_op_from(op.clone(), Some(target)).await;
                    }
                    self.leases.lock().insert(
                        userkey.to_vec(),
                        Instant::now() + Duration::from_millis(lease_ms),
                    );
                    return !ops.is_empty();
                }
            }
            false
        })
    }
}

#[cfg(test)]
mod flow_tests {
    use super::PeerFlow;

    #[test]
    fn send_then_ack_drains_window() {
        let mut f = PeerFlow::at(10);
        f.on_send(20, 1000);
        f.on_send(30, 500);
        assert_eq!(f.sent, 30);
        assert_eq!(f.inflight_bytes, 1500);
        f.on_ack(20);
        assert_eq!(f.acked, 20);
        assert_eq!(f.inflight_bytes, 500);
        f.on_ack(30);
        assert_eq!(f.inflight_bytes, 0);
        assert!(f.inflight.is_empty());
    }

    #[test]
    fn window_full_blocks_until_ack() {
        let mut f = PeerFlow::at(0);
        f.on_send(5, 4096);
        assert!(f.window_full(4096));
        assert!(!f.window_full(8192));
        f.on_ack(5);
        assert!(!f.window_full(4096));
    }

    #[test]
    fn stale_ack_clamped_to_sent_after_rewind() {
        let mut f = PeerFlow::at(100);
        f.on_send(200, 64);
        // ResumeFrom rewind to 150 (fresh state), then a stale ack for 200
        // from the old connection arrives.
        let mut f2 = PeerFlow::at(150);
        f2.on_ack(200);
        assert_eq!(f2.acked, 150, "stale ack must not run ahead of the cursor");
        // On the original flow an ack beyond sent is also clamped.
        f.on_ack(999);
        assert_eq!(f.acked, 200);
        assert_eq!(f.inflight_bytes, 0);
    }

    #[test]
    fn out_of_order_ack_drains_all_covered_batches() {
        let mut f = PeerFlow::at(0);
        f.on_send(10, 100);
        f.on_send(20, 100);
        f.on_send(30, 100);
        f.on_ack(30); // cumulative ack covers everything at once
        assert_eq!(f.inflight_bytes, 0);
        assert!(f.inflight.is_empty());
    }

    #[test]
    fn clear_inflight_keeps_cursor() {
        let mut f = PeerFlow::at(0);
        f.on_send(10, 100);
        f.clear_inflight();
        assert_eq!(
            f.sent, 10,
            "cursor survives disconnect; ResumeFrom resets it"
        );
        assert_eq!(f.inflight_bytes, 0);
        assert!(f.stalled_since.is_none());
    }

    #[test]
    fn duplicate_ack_is_harmless() {
        let mut f = PeerFlow::at(0);
        f.on_send(10, 100);
        f.on_ack(10);
        f.on_ack(10);
        assert_eq!(f.acked, 10);
        assert_eq!(f.inflight_bytes, 0);
    }
}

#[cfg(test)]
mod interest_tests {
    use super::Interest;
    use std::time::{Duration, Instant};

    fn exp() -> Instant {
        Instant::now() + Duration::from_secs(60)
    }

    #[test]
    fn register_counts_and_caps() {
        let mut i = Interest::new(2);
        assert!(i.register(1, b"a", 10, exp()));
        assert!(i.register(1, b"b", 10, exp()));
        assert_eq!(i.total, 2);
        assert!(!i.register(2, b"c", 10, exp()), "cap must reject");
        assert_eq!(i.total, 2);
    }

    #[test]
    fn refresh_is_always_allowed_at_cap() {
        let mut i = Interest::new(1);
        assert!(i.register(1, b"a", 10, exp()));
        assert!(
            i.register(1, b"a", 10, exp()),
            "refresh must not count as growth"
        );
        assert_eq!(i.total, 1);
        assert!(
            !i.register(1, b"a", 11, exp()),
            "same key, NEW node is growth"
        );
    }

    #[test]
    fn gc_and_remove_peer_release_capacity() {
        let mut i = Interest::new(2);
        let past = Instant::now() - Duration::from_secs(1);
        assert!(i.register(1, b"a", 10, past));
        assert!(i.register(1, b"b", 11, exp()));
        i.gc(Instant::now());
        assert_eq!(i.total, 1);
        assert!(i.register(1, b"c", 10, exp()), "gc must free capacity");
        i.remove_peer(11);
        assert_eq!(i.total, 1);
        assert!(i.subs(1, b"b").is_none());
        assert!(i.subs(1, b"c").is_some());
    }
}

#[cfg(test)]
mod join_gate_tests {
    use super::join_ready;

    #[test]
    fn pending_work_holds_the_gate() {
        assert!(!join_ready(1, 0, false, true, true, true, true));
        assert!(!join_ready(0, 1, false, true, true, true, true));
    }

    #[test]
    fn populated_cluster_requires_a_sweep() {
        // Active others visible but no bootstrap sweep ran against that
        // view yet: the gate must hold (the sweep may still find empty
        // future-owned pids).
        assert!(!join_ready(0, 0, false, false, true, true, true));
        assert!(join_ready(0, 0, false, true, true, true, true));
    }

    #[test]
    fn unscoped_rejoin_holds_the_gate_with_active_others() {
        // A gc_grace rejoin whose scope is not yet resolved may still turn
        // out non-empty — the gate must hold while Active others exist.
        assert!(!join_ready(0, 0, true, true, true, true, true));
        // Sole survivor: no Active others → the alone/cold rules apply and
        // the rejoin driver stands down after the gate passes.
        assert!(join_ready(0, 0, true, true, false, false, true));
    }

    #[test]
    fn all_joining_cohort_is_cold_start_after_settle() {
        // Others exist but nobody is Active: either a cluster cold start
        // (no data anywhere — ready) or gossip lag (state keys not yet
        // delivered — hold). The settle window separates the two.
        assert!(!join_ready(0, 0, false, false, false, true, false));
        assert!(join_ready(0, 0, false, false, false, true, true));
    }

    #[test]
    fn alone_is_ready_immediately() {
        assert!(join_ready(0, 0, false, false, false, false, false));
    }
}

#[cfg(test)]
mod compaction_guard_tests {
    use super::resolve_debt_water_marks;

    #[test]
    fn defaults_sit_below_ondadbs_hard_pacing_ceiling() {
        // ondaDB's `hard_pending_compaction_bytes` defaults to 8 GiB, where a
        // commit blocks on a shard thread. Both marks must engage first, or the
        // guard never gets to convert that stall into a MISCONF.
        let (high, low) = resolve_debt_water_marks(None, None);
        assert_eq!(high, 6 << 30);
        assert_eq!(low, 4 << 30);
        assert!(high < 8 << 30);
        assert!(low < high);
    }

    #[test]
    fn low_water_is_clamped_under_high_water() {
        // A low-water at or above high-water would clear the stop on the same
        // tick that set it: the guard would flap instead of holding.
        let (high, low) = resolve_debt_water_marks(Some(1000), Some(1000));
        assert_eq!((high, low), (1000, 999));
        let (high, low) = resolve_debt_water_marks(Some(1000), Some(5000));
        assert_eq!((high, low), (1000, 999));
    }

    #[test]
    fn zero_high_water_disables_the_guard() {
        assert_eq!(resolve_debt_water_marks(Some(0), Some(4 << 30)), (0, 0));
    }

    #[test]
    fn high_water_of_one_still_yields_a_usable_pair() {
        // The clamp must not underflow at the smallest legal high-water.
        assert_eq!(resolve_debt_water_marks(Some(1), None), (1, 0));
    }
}

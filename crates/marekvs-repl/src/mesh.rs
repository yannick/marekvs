//! Peer mesh: two TCP connections (ctl + bulk) per pair, dialed by the lower
//! NodeId (design/04 §Transport). Incoming frames are funneled to the
//! ReplEngine through one mpsc channel; outgoing frames go through per-peer
//! bounded writer queues (backpressure).
//!
//! Liveness is application-level: every connection pings its peer each
//! `MAREKVS_MESH_PING_INTERVAL_MS` and closes after
//! `MAREKVS_MESH_IDLE_TIMEOUT_MS` without inbound bytes. TCP alone cannot
//! detect a wedged-but-open connection (conntrack blackhole — the chaos
//! harness creates exactly these), and gossip phi-accrual detects dead
//! *nodes*, not dead *connections*.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use marekvs_core::NodeId;
use marekvs_proto::{decode, encode, ConnKind, PeerMsg};
use parking_lot::RwLock;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

const WRITER_QUEUE: usize = 4096;

fn env_ms(name: &str, default_ms: u64) -> Duration {
    let ms = std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(default_ms);
    Duration::from_millis(ms)
}

fn ping_interval() -> Duration {
    static V: OnceLock<Duration> = OnceLock::new();
    *V.get_or_init(|| env_ms("MAREKVS_MESH_PING_INTERVAL_MS", 1000))
}

fn idle_timeout() -> Duration {
    static V: OnceLock<Duration> = OnceLock::new();
    *V.get_or_init(|| env_ms("MAREKVS_MESH_IDLE_TIMEOUT_MS", 3000))
}

/// Per-peer sender slots. A slot is `Some` only while its connection's
/// reader/writer pumps are alive — `connected_peers` and `send_*` must never
/// see a ghost handle for a dead connection.
#[derive(Clone, Default)]
pub struct PeerHandle {
    pub ctl: Option<mpsc::Sender<PeerMsg>>,
    pub bulk: Option<mpsc::Sender<PeerMsg>>,
}

pub struct Mesh {
    pub node_id: NodeId,
    /// (bytes in, bytes out) counters from the engine's metrics registry.
    pub traffic: Option<(prometheus::IntCounter, prometheus::IntCounter)>,
    /// Connections closed by heartbeat idle timeout.
    pub conn_timeouts: Option<prometheus::IntCounter>,
    peers: RwLock<HashMap<NodeId, PeerHandle>>,
    /// Current dial target per peer. A restarted peer can come back on a
    /// NEW address (no static IPs / no DNS — chaos finding on Apple
    /// containers); reconnect loops check this slot each iteration and exit
    /// when superseded, and maintain_peer for the new address takes over.
    dial_addrs: parking_lot::Mutex<HashMap<NodeId, SocketAddr>>,
    /// Peers deliberately forgotten by [`forget_peer`]: their dial loops exit
    /// instead of retrying until process exit. A node that reappears in the
    /// view clears its mark in `maintain_peer`, so this is a suppression, not
    /// a tombstone — the apple-container pattern (same id, new address) still
    /// reconnects.
    forgotten: parking_lot::Mutex<HashSet<NodeId>>,
    /// What each peer announced in its Hello: `(proto, features)`. Senders
    /// consult this before emitting anything a peer might not merge (P0).
    caps: parking_lot::Mutex<HashMap<NodeId, (u16, u32)>>,
    /// (peer, msg) stream consumed by the ReplEngine — latency-sensitive
    /// lane (Repl/Ack/Fetch/Check/Interest/Publish).
    pub incoming_tx: mpsc::Sender<(NodeId, PeerMsg)>,
    /// Heavy-work lane: anti-entropy + bootstrap messages. Digest scans and
    /// partition streams must never head-of-line-block read-through
    /// fetches on the latency lane (chaos join_empty_reads: post-join AE
    /// bursts starved fetches into their 300 ms timeout → nil reads).
    pub ae_tx: mpsc::Sender<(NodeId, PeerMsg)>,
    /// Peers connected/disconnected notifications (peer, connected). Fired
    /// on ctl-slot transitions only: bulk connects would trigger redundant
    /// ResumeFrom / interest churn.
    pub events_tx: mpsc::UnboundedSender<(NodeId, bool)>,
}

/// True for messages served on the heavy AE/bootstrap lane. Bootstrap
/// chunk/done ordering is preserved: a connection's frames are routed
/// per-message but every Bootstrap*/AE frame goes to the SAME lane.
fn is_heavy_lane(msg: &PeerMsg) -> bool {
    matches!(
        msg,
        PeerMsg::MerkleRoot { .. }
            | PeerMsg::MerkleRootMatch { .. }
            | PeerMsg::MerkleBuckets { .. }
            | PeerMsg::BucketKeys { .. }
            | PeerMsg::RepairOps { .. }
            | PeerMsg::RequestKeys { .. }
            | PeerMsg::BootstrapReq { .. }
            | PeerMsg::BootstrapChunk { .. }
            | PeerMsg::BootstrapDone { .. }
    )
}

impl Mesh {
    pub fn new(
        node_id: NodeId,
        incoming_tx: mpsc::Sender<(NodeId, PeerMsg)>,
        ae_tx: mpsc::Sender<(NodeId, PeerMsg)>,
        events_tx: mpsc::UnboundedSender<(NodeId, bool)>,
        traffic: Option<(prometheus::IntCounter, prometheus::IntCounter)>,
        conn_timeouts: Option<prometheus::IntCounter>,
    ) -> Arc<Mesh> {
        Arc::new(Mesh {
            node_id,
            traffic,
            conn_timeouts,
            peers: RwLock::new(HashMap::new()),
            dial_addrs: parking_lot::Mutex::new(HashMap::new()),
            forgotten: parking_lot::Mutex::new(HashSet::new()),
            caps: parking_lot::Mutex::new(HashMap::new()),
            incoming_tx,
            ae_tx,
            events_tx,
        })
    }

    pub fn peer(&self, node: NodeId) -> Option<PeerHandle> {
        self.peers.read().get(&node).cloned()
    }

    /// Peers with a live ctl connection (the lane replication rides on).
    pub fn connected_peers(&self) -> Vec<NodeId> {
        self.peers
            .read()
            .iter()
            .filter(|(_, h)| h.ctl.is_some())
            .map(|(n, _)| *n)
            .collect()
    }

    /// Best-effort ctl send; drops when the peer is absent or its queue full.
    pub fn send_ctl(&self, node: NodeId, msg: PeerMsg) -> bool {
        match self.peer(node).and_then(|h| h.ctl) {
            Some(tx) => tx.try_send(msg).is_ok(),
            None => false,
        }
    }

    pub async fn send_bulk(&self, node: NodeId, msg: PeerMsg) -> bool {
        match self.peer(node).and_then(|h| h.bulk) {
            Some(tx) => tx.send(msg).await.is_ok(),
            None => false,
        }
    }

    pub fn broadcast_ctl(&self, msg: &PeerMsg) {
        for (_, h) in self.peers.read().iter() {
            if let Some(tx) = &h.ctl {
                let _ = tx.try_send(msg.clone());
            }
        }
    }

    /// Accept loop for the mesh listener.
    pub async fn run_listener(self: Arc<Self>, listener: TcpListener) {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let mesh = self.clone();
                    tokio::spawn(async move {
                        if let Err(e) = mesh.handle_inbound(stream).await {
                            tracing::debug!(?e, "inbound peer connection ended");
                        }
                    });
                }
                Err(e) => {
                    tracing::warn!(?e, "mesh accept failed");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    async fn handle_inbound(self: &Arc<Self>, mut stream: TcpStream) -> anyhow::Result<()> {
        stream.set_nodelay(true)?;
        // First frame must be Hello. Bounded: a connection that never sends
        // one must not hold the task forever.
        let mut buf = Vec::with_capacity(4096);
        let (peer, kind) = loop {
            let mut chunk = [0u8; 4096];
            let n = tokio::time::timeout(idle_timeout(), stream.read(&mut chunk))
                .await
                .map_err(|_| anyhow::anyhow!("timed out waiting for hello"))??;
            if n == 0 {
                anyhow::bail!("closed before hello");
            }
            buf.extend_from_slice(&chunk[..n]);
            if let Some((msg, consumed)) = decode(&buf)? {
                buf.drain(..consumed);
                match msg {
                    PeerMsg::Hello {
                        node,
                        kind,
                        proto,
                        features,
                    } => {
                        self.note_peer_caps(node, proto, features);
                        break (node, kind);
                    }
                    other => anyhow::bail!("expected Hello, got {other:?}"),
                }
            }
        };
        tracing::info!(peer, ?kind, "peer connected (inbound)");
        self.run_connection(peer, kind, stream, buf).await
    }

    /// Dial loop: keep ctl+bulk connections to `peer` alive while it stays in
    /// the membership view. Only called for peers with id > self (lower dials).
    pub async fn maintain_peer(self: Arc<Self>, peer: NodeId, addr: SocketAddr) {
        // A peer we had forgotten is back in the view: un-forget it before
        // spawning, or the new dial loops would exit on their first iteration.
        self.forgotten.lock().remove(&peer);
        // Supersede any reconnect loops dialing an older address.
        self.dial_addrs.lock().insert(peer, addr);
        for kind in [ConnKind::Ctl, ConnKind::Bulk] {
            let mesh = self.clone();
            tokio::spawn(async move {
                let mut backoff = Duration::from_millis(100);
                loop {
                    if mesh.dial_addrs.lock().get(&peer) != Some(&addr) {
                        tracing::info!(peer, %addr, "dial loop superseded by new address");
                        return;
                    }
                    match TcpStream::connect(addr).await {
                        Ok(mut stream) => {
                            backoff = Duration::from_millis(100);
                            let _ = stream.set_nodelay(true);
                            let hello = encode(&PeerMsg::Hello {
                                node: mesh.node_id,
                                kind,
                                proto: marekvs_proto::PROTO_VERSION,
                                features: marekvs_proto::features::ALL,
                            })
                            .expect("encode hello");
                            if stream.write_all(&hello).await.is_ok() {
                                let _ = mesh.run_connection(peer, kind, stream, Vec::new()).await;
                            }
                        }
                        Err(e) => {
                            tracing::debug!(peer, ?e, "dial failed");
                        }
                    }
                    // Stop redialing once the peer left the mesh registry on
                    // purpose (engine deregisters via `drop_peer`).
                    if mesh.dropped(peer) {
                        return;
                    }
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                }
            });
        }
    }

    fn note_peer_caps(&self, peer: NodeId, proto: u16, features: u32) {
        let prev = self.caps.lock().insert(peer, (proto, features));
        if prev.map(|(p, f)| (p, f)) != Some((proto, features)) {
            tracing::info!(peer, proto, features, "peer capabilities");
        }
    }

    /// Feature bits `peer` announced, or `None` if it has not said Hello yet.
    pub fn peer_features(&self, peer: NodeId) -> Option<u32> {
        self.caps.lock().get(&peer).map(|(_, f)| *f)
    }

    /// True when **every** currently connected peer announces `bit`.
    ///
    /// Conservative by construction: a peer that has connected but not yet
    /// said Hello counts as lacking the feature, so a capability can only be
    /// used once the whole mesh has demonstrably agreed to it.
    pub fn all_peers_support(&self, bit: u32) -> bool {
        let caps = self.caps.lock();
        self.connected_peers()
            .iter()
            .all(|p| caps.get(p).is_some_and(|(_, f)| f & bit != 0))
    }

    fn dropped(&self, peer: NodeId) -> bool {
        self.forgotten.lock().contains(&peer)
    }

    /// Stop maintaining connections to `peer` (T2-10).
    ///
    /// Called by the view watcher once a node has been absent from the
    /// membership view for longer than the GC grace. Without this, a departed
    /// node's two dial loops retry forever: a long-lived cluster that has
    /// scaled up and down accumulates one pair per node it has ever seen,
    /// each logging a failed dial every 5 s.
    ///
    /// Marking comes first so a dial loop currently sleeping in backoff sees
    /// the mark on its next iteration and exits. Dropping the handles stops
    /// `connected_peers`/`send_*` from routing to a peer that is gone.
    ///
    /// This is reversible on purpose: if the node comes back — including on a
    /// new address, which is the norm under Apple containers — the view
    /// watcher calls `maintain_peer` again, which clears the mark.
    pub fn forget_peer(&self, peer: NodeId) {
        self.forgotten.lock().insert(peer);
        self.dial_addrs.lock().remove(&peer);
        let had_conn = self.peers.write().remove(&peer).is_some();
        self.caps.lock().remove(&peer);
        // Let the engine tear down cursors/interest exactly as it would for a
        // disconnect, so a later reappearance re-inits via ResumeFrom.
        if had_conn {
            let _ = self.events_tx.send((peer, false));
        }
        tracing::info!(peer, had_conn, "peer forgotten: dial loops stop");
    }

    /// Peers this mesh has deliberately stopped dialing.
    pub fn forgotten_peers(&self) -> Vec<NodeId> {
        let mut v: Vec<NodeId> = self.forgotten.lock().iter().copied().collect();
        v.sort_unstable();
        v
    }

    /// Shared read/write pump for one established connection.
    async fn run_connection(
        self: &Arc<Self>,
        peer: NodeId,
        kind: ConnKind,
        stream: TcpStream,
        preread: Vec<u8>,
    ) -> anyhow::Result<()> {
        let (mut rd, mut wr) = stream.into_split();
        let (tx, mut rx) = mpsc::channel::<PeerMsg>(WRITER_QUEUE);

        // Register handle (ctl and bulk arrive on separate calls).
        {
            let mut peers = self.peers.write();
            let entry = peers.entry(peer).or_default();
            match kind {
                ConnKind::Ctl => entry.ctl = Some(tx.clone()),
                ConnKind::Bulk => entry.bulk = Some(tx.clone()),
            }
        }
        if kind == ConnKind::Ctl {
            let _ = self.events_tx.send((peer, true));
        }

        let out_counter = self.traffic.as_ref().map(|(_, o)| o.clone());
        let writer = tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                let frame = match encode(&msg) {
                    Ok(f) => f,
                    Err(e) => {
                        tracing::warn!(?e, "encode failed");
                        continue;
                    }
                };
                if let Some(c) = &out_counter {
                    c.inc_by(frame.len() as u64);
                }
                if wr.write_all(&frame).await.is_err() {
                    return;
                }
            }
        });

        // Heartbeat: ping on our own cadence; the PEER's idle timeout is what
        // this feeds. try_send drop-on-full is fine — a full queue means real
        // traffic is flowing (which resets the peer's clock just as well) or
        // the writer is wedged (which the peer's timeout catches).
        let pinger = {
            let tx = tx.clone();
            tokio::spawn(async move {
                let mut nonce: u64 = 0;
                let mut tick = tokio::time::interval(ping_interval());
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tick.tick().await;
                    nonce = nonce.wrapping_add(1);
                    let _ = tx.try_send(PeerMsg::Ping { nonce });
                }
            })
        };

        // Reader loop on this task. Any inbound bytes (data, Ping, Pong)
        // count as liveness; a silent connection is closed after the idle
        // timeout so the dial loop + ResumeFrom machinery can rebuild it.
        let mut buf = preread;
        let mut chunk = vec![0u8; 64 * 1024];
        let result: anyhow::Result<()> = loop {
            match decode(&buf) {
                Ok(Some((msg, consumed))) => {
                    buf.drain(..consumed);
                    // Liveness frames are answered on THIS connection (a bulk
                    // conn's ping answered on ctl would leave the bulk read
                    // side idle) and never forwarded to the engine.
                    match msg {
                        PeerMsg::Ping { nonce } => {
                            let _ = tx.try_send(PeerMsg::Pong { nonce });
                        }
                        PeerMsg::Pong { .. } => {}
                        msg if is_heavy_lane(&msg) => {
                            if self.ae_tx.send((peer, msg)).await.is_err() {
                                break Ok(());
                            }
                        }
                        msg => {
                            if self.incoming_tx.send((peer, msg)).await.is_err() {
                                break Ok(());
                            }
                        }
                    }
                    continue;
                }
                Ok(None) => {}
                Err(e) => break Err(e.into()),
            }
            let n = match tokio::time::timeout(idle_timeout(), rd.read(&mut chunk)).await {
                Ok(io) => io?,
                Err(_) => {
                    if let Some(c) = &self.conn_timeouts {
                        c.inc();
                    }
                    tracing::warn!(peer, ?kind, "mesh connection idle timeout; closing");
                    break Err(anyhow::anyhow!("idle timeout"));
                }
            };
            if n == 0 {
                break Ok(());
            }
            if let Some((i, _)) = &self.traffic {
                i.inc_by(n as u64);
            }
            buf.extend_from_slice(&chunk[..n]);
        };

        writer.abort();
        pinger.abort();

        // Deregister OUR slot only — a reconnect may already have replaced
        // it with a fresh sender, which must survive.
        let cleared_ctl = {
            let mut peers = self.peers.write();
            let mut cleared = false;
            if let Some(entry) = peers.get_mut(&peer) {
                let slot = match kind {
                    ConnKind::Ctl => &mut entry.ctl,
                    ConnKind::Bulk => &mut entry.bulk,
                };
                if slot.as_ref().is_some_and(|s| s.same_channel(&tx)) {
                    *slot = None;
                    cleared = kind == ConnKind::Ctl;
                }
                if entry.ctl.is_none() && entry.bulk.is_none() {
                    peers.remove(&peer);
                }
            }
            cleared
        };
        if cleared_ctl {
            let _ = self.events_tx.send((peer, false));
        }
        tracing::info!(peer, ?kind, "peer connection closed");
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mesh() -> Arc<Mesh> {
        let (incoming_tx, _rx1) = mpsc::channel(8);
        let (ae_tx, _rx2) = mpsc::channel(8);
        let (events_tx, _rx3) = mpsc::unbounded_channel();
        Mesh::new(1, incoming_tx, ae_tx, events_tx, None, None)
    }

    /// A forgotten peer's dial loops must see the mark and exit; a peer that
    /// comes back must be dialable again, or a node that briefly left the view
    /// would be unreachable for the rest of the process's life.
    #[tokio::test]
    async fn forget_peer_suppresses_dialing_until_the_peer_returns() {
        let m = mesh();
        assert!(!m.dropped(7), "a peer starts dialable");

        m.forget_peer(7);
        assert!(m.dropped(7), "forgotten peer must stop its dial loops");
        assert_eq!(m.forgotten_peers(), vec![7]);

        // Reappearance: the view watcher re-dials, which un-forgets.
        let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
        m.clone().maintain_peer(7, addr).await;
        assert!(
            !m.dropped(7),
            "a peer back in the view must be dialable again"
        );
        assert!(m.forgotten_peers().is_empty());
    }

    /// Forgetting is per-peer: it must not disturb other peers' dial state.
    #[tokio::test]
    async fn forget_peer_is_scoped_to_one_peer() {
        let m = mesh();
        m.forget_peer(3);
        m.forget_peer(9);
        assert_eq!(m.forgotten_peers(), vec![3, 9]);
        assert!(!m.dropped(4));
    }
}

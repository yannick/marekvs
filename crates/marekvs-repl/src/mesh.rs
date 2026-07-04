//! Peer mesh: two TCP connections (ctl + bulk) per pair, dialed by the lower
//! NodeId (design/04 §Transport). Incoming frames are funneled to the
//! ReplEngine through one mpsc channel; outgoing frames go through per-peer
//! bounded writer queues (backpressure).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use marekvs_core::NodeId;
use marekvs_proto::{decode, encode, ConnKind, PeerMsg};
use parking_lot::RwLock;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

const WRITER_QUEUE: usize = 4096;

#[derive(Clone)]
pub struct PeerHandle {
    pub ctl: mpsc::Sender<PeerMsg>,
    pub bulk: mpsc::Sender<PeerMsg>,
}

pub struct Mesh {
    pub node_id: NodeId,
    /// (bytes in, bytes out) counters from the engine's metrics registry.
    pub traffic: Option<(prometheus::IntCounter, prometheus::IntCounter)>,
    peers: RwLock<HashMap<NodeId, PeerHandle>>,
    /// Current dial target per peer. A restarted peer can come back on a
    /// NEW address (no static IPs / no DNS — chaos finding on Apple
    /// containers); reconnect loops check this slot each iteration and exit
    /// when superseded, and maintain_peer for the new address takes over.
    dial_addrs: parking_lot::Mutex<HashMap<NodeId, SocketAddr>>,
    /// (peer, msg) stream consumed by the ReplEngine.
    pub incoming_tx: mpsc::Sender<(NodeId, PeerMsg)>,
    /// Peers connected/disconnected notifications (peer, connected).
    pub events_tx: mpsc::UnboundedSender<(NodeId, bool)>,
}

impl Mesh {
    pub fn new(
        node_id: NodeId,
        incoming_tx: mpsc::Sender<(NodeId, PeerMsg)>,
        events_tx: mpsc::UnboundedSender<(NodeId, bool)>,
        traffic: Option<(prometheus::IntCounter, prometheus::IntCounter)>,
    ) -> Arc<Mesh> {
        Arc::new(Mesh {
            node_id,
            traffic,
            peers: RwLock::new(HashMap::new()),
            dial_addrs: parking_lot::Mutex::new(HashMap::new()),
            incoming_tx,
            events_tx,
        })
    }

    pub fn peer(&self, node: NodeId) -> Option<PeerHandle> {
        self.peers.read().get(&node).cloned()
    }

    pub fn connected_peers(&self) -> Vec<NodeId> {
        self.peers.read().keys().copied().collect()
    }

    /// Best-effort ctl send; drops when the peer is absent or its queue full.
    pub fn send_ctl(&self, node: NodeId, msg: PeerMsg) -> bool {
        match self.peer(node) {
            Some(h) => h.ctl.try_send(msg).is_ok(),
            None => false,
        }
    }

    pub async fn send_bulk(&self, node: NodeId, msg: PeerMsg) -> bool {
        match self.peer(node) {
            Some(h) => h.bulk.send(msg).await.is_ok(),
            None => false,
        }
    }

    pub fn broadcast_ctl(&self, msg: &PeerMsg) {
        for (_, h) in self.peers.read().iter() {
            let _ = h.ctl.try_send(msg.clone());
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
        // First frame must be Hello.
        let mut buf = Vec::with_capacity(4096);
        let (peer, kind) = loop {
            let mut chunk = [0u8; 4096];
            let n = stream.read(&mut chunk).await?;
            if n == 0 {
                anyhow::bail!("closed before hello");
            }
            buf.extend_from_slice(&chunk[..n]);
            if let Some((msg, consumed)) = decode(&buf)? {
                buf.drain(..consumed);
                match msg {
                    PeerMsg::Hello { node, kind } => break (node, kind),
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

    fn dropped(&self, _peer: NodeId) -> bool {
        false // v1: redial until process exit; view-driven GC is future work
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
            let entry = peers.entry(peer).or_insert_with(|| PeerHandle {
                ctl: tx.clone(),
                bulk: tx.clone(),
            });
            match kind {
                ConnKind::Ctl => entry.ctl = tx.clone(),
                ConnKind::Bulk => entry.bulk = tx.clone(),
            }
        }
        let _ = self.events_tx.send((peer, true));

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

        // Reader loop on this task.
        let mut buf = preread;
        let mut chunk = vec![0u8; 64 * 1024];
        let result: anyhow::Result<()> = loop {
            match decode(&buf) {
                Ok(Some((msg, consumed))) => {
                    buf.drain(..consumed);
                    if self.incoming_tx.send((peer, msg)).await.is_err() {
                        break Ok(());
                    }
                    continue;
                }
                Ok(None) => {}
                Err(e) => break Err(e.into()),
            }
            let n = rd.read(&mut chunk).await?;
            if n == 0 {
                break Ok(());
            }
            if let Some((i, _)) = &self.traffic {
                i.inc_by(n as u64);
            }
            buf.extend_from_slice(&chunk[..n]);
        };

        writer.abort();
        let _ = self.events_tx.send((peer, false));
        tracing::info!(peer, ?kind, "peer connection closed");
        result
    }
}

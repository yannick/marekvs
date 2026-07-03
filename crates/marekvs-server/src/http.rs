//! Health/readiness probes + Prometheus exposition on :9121 (design/07).
//!
//! A deliberately tiny HTTP/1.1 responder — kubelet probes and Prometheus
//! scrapes are simple GETs; pulling in a web framework for four routes
//! would dominate the dependency tree of the whole server.
//!
//! Routes:
//!   GET /ready   → 200 while the node phase is Active|Leaving, else 503
//!   GET /alive   → 200 while shard threads answer within 500 ms, else 503
//!   GET /drain   → set phase Leaving (preStop hook), 200
//!   GET /metrics → Prometheus text format

use std::sync::Arc;
use std::time::Duration;

use marekvs_cluster::{Cluster, NodePhase};
use marekvs_engine::Engine;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

pub async fn serve(
    listener: TcpListener,
    engine: Arc<Engine>,
    cluster: Arc<Cluster>,
) -> anyhow::Result<()> {
    loop {
        let (socket, _) = listener.accept().await?;
        let engine = engine.clone();
        let cluster = cluster.clone();
        tokio::spawn(async move {
            let _ = handle(socket, engine, cluster).await;
        });
    }
}

async fn handle(
    mut socket: TcpStream,
    engine: Arc<Engine>,
    cluster: Arc<Cluster>,
) -> anyhow::Result<()> {
    let mut buf = vec![0u8; 4096];
    let n = tokio::time::timeout(Duration::from_secs(5), socket.read(&mut buf)).await??;
    let request = String::from_utf8_lossy(&buf[..n]);
    let path = request
        .split_whitespace()
        .nth(1)
        .unwrap_or("/")
        .split('?')
        .next()
        .unwrap_or("/");

    let (status, content_type, body): (&str, &str, String) = match path {
        "/metrics" => (
            "200 OK",
            "text/plain; version=0.0.4",
            engine.metrics.render(
                engine.started_at_ms,
                engine.clients.load(std::sync::atomic::Ordering::Relaxed),
            ),
        ),
        "/ready" => {
            // Ready = this node's phase, as visible in its own gossip view.
            let phase = cluster
                .view()
                .members
                .iter()
                .find(|m| m.node == cluster.self_id)
                .map(|m| m.phase);
            match phase {
                Some(NodePhase::Active) | Some(NodePhase::Leaving) => {
                    ("200 OK", "text/plain", "ready\n".into())
                }
                other => (
                    "503 Service Unavailable",
                    "text/plain",
                    format!("not ready: {other:?}\n"),
                ),
            }
        }
        "/alive" => {
            // Liveness = a shard thread answers a no-op within 500 ms.
            let probe = engine.store.run(0, |_ctx| ());
            match tokio::time::timeout(Duration::from_millis(500), probe).await {
                Ok(()) => ("200 OK", "text/plain", "alive\n".into()),
                Err(_) => (
                    "503 Service Unavailable",
                    "text/plain",
                    "shard threads unresponsive\n".into(),
                ),
            }
        }
        "/drain" => {
            tracing::info!("drain requested via HTTP (preStop)");
            cluster.set_phase(NodePhase::Leaving).await;
            ("200 OK", "text/plain", "draining\n".into())
        }
        _ => ("404 Not Found", "text/plain", "not found\n".into()),
    };

    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    socket.write_all(response.as_bytes()).await?;
    Ok(())
}

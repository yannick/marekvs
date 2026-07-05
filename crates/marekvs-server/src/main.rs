//! marekvs-server — process wiring: config, storage, cluster, replication,
//! RESP frontend (design/01 §Startup sequence).

mod http;
mod redisrepl;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use marekvs_cluster::{Cluster, ClusterConfig, NodePhase};
use marekvs_engine::store::{Store, StoreConfig};
use marekvs_engine::{Engine, Session};
use marekvs_repl::ReplEngine;
use marekvs_resp::{ReplyBuf, RespParser};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

/// Split a `host:port` bootstrap target (IPv4/hostname only, matching the
/// REPLICAOF command surface).
fn parse_host_port(s: &str) -> Option<(String, u16)> {
    let (host, port) = s.rsplit_once(':')?;
    let port: u16 = port.trim().parse().ok()?;
    if host.is_empty() || port == 0 {
        return None;
    }
    Some((host.to_string(), port))
}

fn node_id_from_env() -> u16 {
    if let Ok(v) = std::env::var("MAREKVS_NODE_ID") {
        return v.parse().expect("MAREKVS_NODE_ID must be a u16");
    }
    // StatefulSet convention: hostname "marekvs-3" → ordinal 3 (design/07).
    if let Ok(hostname) = std::env::var("HOSTNAME") {
        if let Some(ord) = hostname.rsplit('-').next().and_then(|s| s.parse().ok()) {
            return ord;
        }
    }
    0
}

/// Local primary-interface IP via the UDP-connect trick (no packet is sent).
fn detect_local_ip() -> anyhow::Result<std::net::IpAddr> {
    let sock = std::net::UdpSocket::bind("0.0.0.0:0")?;
    sock.connect("8.8.8.8:80")?;
    Ok(sock.local_addr()?.ip())
}

async fn resolve(host: &str, port: u16) -> anyhow::Result<SocketAddr> {
    if host == "auto" {
        return Ok(SocketAddr::new(detect_local_ip()?, port));
    }
    if let Ok(addr) = format!("{host}:{port}").parse() {
        return Ok(addr);
    }
    let mut last_err = None;
    for _ in 0..30 {
        match tokio::net::lookup_host((host, port)).await {
            Ok(mut addrs) => {
                if let Some(addr) = addrs.find(|a| a.is_ipv4()).or(None) {
                    return Ok(addr);
                }
            }
            Err(e) => last_err = Some(e),
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    anyhow::bail!("cannot resolve advertise host {host}: {last_err:?}")
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Reloadable log filter: seeded from RUST_LOG, live-adjustable via
    // CONFIG SET loglevel (the hook is installed on the engine below).
    let log_spec = std::env::var("RUST_LOG").unwrap_or_else(|_| "info,chitchat=warn".into());
    let log_filter = tracing_subscriber::EnvFilter::try_new(&log_spec)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,chitchat=warn"));
    let (log_filter, log_reload) = tracing_subscriber::reload::Layer::new(log_filter);
    {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;
        tracing_subscriber::registry()
            .with(log_filter)
            .with(tracing_subscriber::fmt::layer())
            .init();
    }

    let node_id = node_id_from_env();
    let data_dir = env_or("MAREKVS_DATA_DIR", ".data/n0");
    let resp_addr: SocketAddr = env_or("MAREKVS_RESP_ADDR", "0.0.0.0:6379").parse()?;
    let mesh_addr: SocketAddr = env_or("MAREKVS_MESH_ADDR", "0.0.0.0:7373").parse()?;
    let gossip_addr: SocketAddr = env_or("MAREKVS_GOSSIP_ADDR", "0.0.0.0:7946").parse()?;
    let metrics_addr: SocketAddr = env_or("MAREKVS_METRICS_ADDR", "0.0.0.0:9121").parse()?;
    let advertise_ip = env_or("MAREKVS_ADVERTISE_IP", "127.0.0.1");
    let env_seeds: Vec<String> = env_or("MAREKVS_SEEDS", "")
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().to_string())
        .collect();
    let replicas_n: usize = env_or("MAREKVS_REPLICAS_N", "3").parse()?;
    let seeds_empty = env_seeds.is_empty();

    // The advertise address may be a hostname (compose service, pod DNS) —
    // resolve it to an IP once at startup.
    let gossip_advertise = resolve(&advertise_ip, gossip_addr.port()).await?;
    let mesh_advertise = resolve(&advertise_ip, mesh_addr.port()).await?;

    tracing::info!(
        node_id,
        %resp_addr,
        %mesh_advertise,
        %gossip_advertise,
        seeds = ?env_seeds,
        replicas_n,
        "marekvs starting"
    );

    // Storage + engine.
    let mut store_cfg = StoreConfig {
        data_dir,
        node_id,
        ..StoreConfig::default()
    };
    if let Ok(v) = std::env::var("MAREKVS_SHARDS") {
        store_cfg.shard_threads = v.parse().expect("MAREKVS_SHARDS must be a number");
    }
    let store = Store::open(&store_cfg)?;

    // Last-known peer gossip addresses from the previous run: merged into
    // the seed list so a restarted node can rejoin even when the configured
    // seeds are stale (no static IPs / no DNS — e.g. Apple containers hand
    // out a fresh IP on every restart; k8s covers this with DNS seeds).
    let persisted_seeds: Vec<String> = {
        let store = store.clone();
        store
            .run(0, |ctx| match ctx.db.get(&ctx.meta, b"peers:last") {
                Ok(v) => String::from_utf8_lossy(&v)
                    .lines()
                    .map(str::to_owned)
                    .collect(),
                Err(_) => Vec::new(),
            })
            .await
    };
    if !persisted_seeds.is_empty() {
        tracing::info!(
            ?persisted_seeds,
            "merging last-known peer addresses into seeds"
        );
    }
    let mut seeds = env_seeds.clone();
    for p in persisted_seeds {
        if !seeds.contains(&p) {
            seeds.push(p);
        }
    }
    let engine = Engine::new(store.clone());
    engine.set_log_reload(
        log_spec,
        Arc::new(move |spec: &str| {
            let f = tracing_subscriber::EnvFilter::try_new(spec).map_err(|e| e.to_string())?;
            log_reload.reload(f).map_err(|e| e.to_string())
        }),
    );

    // Cluster membership.
    let cluster = Cluster::spawn(ClusterConfig {
        node_id,
        cluster_name: env_or("MAREKVS_CLUSTER", "marekvs"),
        gossip_listen: gossip_addr,
        gossip_advertise,
        mesh_advertise,
        seeds,
        replicas_n,
        gossip_interval: Duration::from_millis(500),
    })
    .await?;

    // Persist every OTHER member's gossip address on view changes — the
    // "peers:last" fallback seeds read at boot (see above).
    {
        let cluster = cluster.clone();
        let store = store.clone();
        tokio::spawn(async move {
            let mut watch = cluster.watch();
            let mut last: String = String::new();
            loop {
                if watch.changed().await.is_err() {
                    return;
                }
                let view = cluster.view();
                let mut addrs: Vec<String> = view
                    .members
                    .iter()
                    .filter(|m| m.node != node_id)
                    .map(|m| m.gossip_addr.to_string())
                    .collect();
                addrs.sort();
                addrs.dedup();
                let joined = addrs.join("\n");
                if joined.is_empty() || joined == last {
                    continue;
                }
                last = joined.clone();
                store
                    .run(0, move |ctx| {
                        let _ =
                            ctx.db
                                .put(&ctx.meta, b"peers:last", joined.as_bytes(), Duration::ZERO);
                    })
                    .await;
            }
        });
    }

    // Replication over the peer mesh.
    let mesh_listener = TcpListener::bind(mesh_addr).await?;
    // Standalone = statically configured single node (no seeds, N=1): only
    // then may the replication ring skip buffering (see ring.rs).
    let standalone_cfg = seeds_empty && replicas_n <= 1;
    let repl = ReplEngine::start(
        store.clone(),
        engine.clone(),
        cluster.clone(),
        mesh_listener,
        standalone_cfg,
    )
    .await;

    // INFO replication section.
    {
        let cluster = cluster.clone();
        engine.set_cluster_info(Arc::new(move || {
            let s = cluster.cluster_stats();
            format!(
                "cluster_enabled:1\r\ncluster_members:{}\r\ncluster_degraded:{}\r\n\
                 underreplicated_partitions:{}\r\neffective_rf_min:{}\r\n",
                s.members, s.degraded as u8, s.underreplicated_partitions, s.effective_rf_min
            )
        }));
    }

    // Upstream-Redis replication (REPLICAOF live-migration path). Install the
    // control hook, then bootstrap from MAREKVS_REPLICAOF="host:port" if set.
    {
        let redis_repl = redisrepl::RedisRepl::new(engine.clone(), resp_addr.port());
        engine.set_replicaof(redis_repl.clone());
        if let Ok(target) = std::env::var("MAREKVS_REPLICAOF") {
            let target = target.trim();
            if !target.is_empty() {
                match parse_host_port(target) {
                    Some((host, port)) => {
                        use marekvs_engine::ReplicaOfCtl;
                        redis_repl.replicaof(host, port);
                    }
                    None => tracing::error!(target, "MAREKVS_REPLICAOF must be host:port"),
                }
            }
        }
    }

    // Join sequence (design/06): give gossip a moment to find peers, then
    // hold phase Joining — invisible to HRW, /ready 503 — until every
    // future-owned partition is bootstrapped (or the cluster is confirmed
    // cold/empty). The old fixed 2 s sleep let a scale-up node go Active
    // with an empty store: HRW immediately routed ~1/n of partitions to it
    // and its reads AND other nodes' read-throughs served nils until AE.
    tokio::time::sleep(Duration::from_secs(2)).await;
    if standalone_cfg {
        cluster.set_phase(NodePhase::Active).await;
    } else {
        // 0 = wait forever (default): a node that cannot finish bootstrap
        // must stay unready rather than serve empty reads. The env is an
        // operator escape hatch, not a normal path.
        let timeout_secs: u64 = env_or("MAREKVS_JOIN_TIMEOUT_SECS", "0").parse()?;
        let timeout = (timeout_secs > 0).then(|| Duration::from_secs(timeout_secs));
        if !repl.wait_join_ready(timeout).await {
            tracing::error!(
                timeout_secs,
                "JOIN GATE TIMEOUT: going Active with incomplete bootstrap — \
                 reads may be empty/stale until anti-entropy converges"
            );
            engine.metrics.join_gate_timeouts_total.inc();
        }
        cluster.set_phase(NodePhase::Active).await;
        // Repair the bootstrap→Active delta window in one reactive AE round.
        repl.kick_ae_bootstrapped().await;
    }

    // Graceful drain on SIGTERM: enter Leaving (gossiped; peers stop
    // targeting us), then hold until the replication ring is fully shipped —
    // exiting with unshipped entries strands acked writes on this node only
    // (chaos finding; the boot re-offer is the backstop, but a removed node
    // never boots again). Bounded by a hard cap for hung peers.
    {
        let cluster = cluster.clone();
        let repl = repl.clone();
        tokio::spawn(async move {
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("sigterm handler");
            sigterm.recv().await;
            tracing::info!("SIGTERM: entering Leaving phase");
            cluster.set_phase(NodePhase::Leaving).await;
            // Minimum window for the phase to gossip out.
            tokio::time::sleep(Duration::from_secs(2)).await;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(7);
            loop {
                let backlog = repl.pending_backlog();
                if backlog == 0 || tokio::time::Instant::now() >= deadline {
                    tracing::info!(backlog, "drain complete; exiting");
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            std::process::exit(0);
        });
    }

    // Probes + Prometheus metrics (design/07): started BEFORE the RESP
    // listener so kubelet can watch readiness during long bootstraps.
    {
        let http_listener = TcpListener::bind(metrics_addr).await?;
        tracing::info!(%metrics_addr, "probes + metrics listening");
        let engine = engine.clone();
        let cluster = cluster.clone();
        tokio::spawn(async move {
            if let Err(e) = http::serve(http_listener, engine, cluster).await {
                tracing::error!(?e, "http probe server exited");
            }
        });
    }

    // RESP frontend — only after Active (readiness = port open).
    let listener = TcpListener::bind(resp_addr).await?;
    engine
        .tcp_port
        .store(resp_addr.port(), std::sync::atomic::Ordering::Relaxed);
    tracing::info!(%resp_addr, "ready to serve");
    let session_ids = Arc::new(AtomicU64::new(1));
    loop {
        let (socket, addr) = listener.accept().await?;
        let engine = engine.clone();
        let id = session_ids.fetch_add(1, Ordering::Relaxed);
        tokio::spawn(async move {
            engine
                .clients
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            engine.metrics.connections_accepted_total.inc();
            if let Err(e) = serve_client(engine.clone(), socket, id).await {
                tracing::debug!(%addr, ?e, "client connection ended");
            }
            engine
                .clients
                .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            engine.metrics.connections_closed_total.inc();
        });
    }
}

/// Batchable = parallel-safe data command (see `Engine::parallel_safe`).
fn is_batchable(args: &[Vec<u8>]) -> bool {
    let Some(name) = args.first() else {
        return false;
    };
    // Uppercase without allocating for the common already-upper case.
    if name.len() > 16 {
        return false;
    }
    let mut upper = [0u8; 16];
    for (i, b) in name.iter().enumerate() {
        upper[i] = b.to_ascii_uppercase();
    }
    let Ok(name) = std::str::from_utf8(&upper[..name.len()]) else {
        return false;
    };
    Engine::parallel_safe(name)
}

async fn serve_client(engine: Arc<Engine>, mut socket: TcpStream, id: u64) -> anyhow::Result<()> {
    socket.set_nodelay(true)?;
    let (push_tx, mut push_rx) = mpsc::unbounded_channel();
    let mut sess = Session::new(id, push_tx);
    sess.authenticated = engine.requirepass.read().is_empty();
    let mut parser = RespParser::new();
    let mut out = ReplyBuf::new(false);
    let mut buf = vec![0u8; 64 * 1024];

    loop {
        tokio::select! {
            n = socket.read(&mut buf) => {
                let n = n?;
                if n == 0 {
                    break;
                }
                engine.metrics.net_input_bytes_total.inc_by(n as u64);
                parser.feed(&buf[..n]);
                // Drain every complete command first so pipelined batches
                // can fan out across shards instead of paying one serial
                // shard round-trip each (the pipeline-16 gap in bench/).
                let mut pending: std::collections::VecDeque<Vec<Vec<u8>>> =
                    std::collections::VecDeque::new();
                loop {
                    match parser.next_command() {
                        Ok(Some(args)) => pending.push_back(args),
                        Ok(None) => break,
                        Err(e) => {
                            out.error(&format!("ERR Protocol error: {e:?}"));
                            { let b = out.take(); engine.metrics.net_output_bytes_total.inc_by(b.len() as u64); socket.write_all(&b).await?; }
                            return Ok(());
                        }
                    }
                }
                while let Some(args) = pending.pop_front() {
                    out.resp3 = sess.resp3;
                    let batchable = sess.authenticated
                        && sess.multi.is_none()
                        && sess.sub_count() == 0
                        && !pending.is_empty()
                        && is_batchable(&args);
                    if !batchable {
                        engine.dispatch(&mut sess, args, &mut out).await;
                        if sess.should_close {
                            { let b = out.take(); engine.metrics.net_output_bytes_total.inc_by(b.len() as u64); socket.write_all(&b).await?; }
                            return Ok(());
                        }
                        continue;
                    }
                    // Greedy batch: consecutive parallel-safe commands whose
                    // argument sets are pairwise disjoint (same-key commands
                    // stay ordered by cutting the batch — per-key ordering
                    // and read-your-writes are preserved).
                    let mut batch = vec![args];
                    let mut seen: std::collections::HashSet<Vec<u8>> =
                        batch[0][1..].iter().cloned().collect();
                    while let Some(next) = pending.front() {
                        if !is_batchable(next)
                            || next[1..].iter().any(|a| seen.contains(a))
                        {
                            break;
                        }
                        seen.extend(next[1..].iter().cloned());
                        batch.push(pending.pop_front().unwrap());
                    }
                    if batch.len() == 1 {
                        engine.dispatch(&mut sess, batch.pop().unwrap(), &mut out).await;
                        continue;
                    }
                    let mut tasks = tokio::task::JoinSet::new();
                    for (i, cmd) in batch.into_iter().enumerate() {
                        let engine = engine.clone();
                        let resp3 = sess.resp3;
                        tasks.spawn(async move { (i, engine.dispatch_data(resp3, cmd).await) });
                    }
                    let mut replies: Vec<Option<Vec<u8>>> = Vec::new();
                    while let Some(Ok((i, bytes))) = tasks.join_next().await {
                        if replies.len() <= i {
                            replies.resize(i + 1, None);
                        }
                        replies[i] = Some(bytes);
                    }
                    for r in replies.into_iter().flatten() {
                        out.extend_raw(&r);
                    }
                }
                if !out.is_empty() {
                    { let b = out.take(); engine.metrics.net_output_bytes_total.inc_by(b.len() as u64); socket.write_all(&b).await?; }
                }
            }
            Some(msg) = push_rx.recv() => {
                let mut push = ReplyBuf::new(sess.resp3);
                match &msg.pattern {
                    None => {
                        push.push(3);
                        push.bulk_str("message");
                        push.bulk(&msg.channel);
                        push.bulk(&msg.payload);
                    }
                    Some(pat) => {
                        push.push(4);
                        push.bulk_str("pmessage");
                        push.bulk(pat);
                        push.bulk(&msg.channel);
                        push.bulk(&msg.payload);
                    }
                }
                { let b = push.take(); engine.metrics.net_output_bytes_total.inc_by(b.len() as u64); socket.write_all(&b).await?; }
            }
        }
    }

    engine.pubsub.drop_session(sess.id, &sess.subs, &sess.psubs);
    Ok(())
}

//! Prometheus metrics registry (design/07 §Observability). One `Metrics`
//! per process, shared by the engine (per-command stats), the server
//! (client connections, RESP throughput), and the replication layer (mesh
//! throughput, ring/cluster gauges — updated by a small stats task).
//!
//! Hot-path cost: one label lookup + atomic add per command, one histogram
//! observe per command, one atomic add per socket read/write.

use prometheus::{
    Encoder, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts, Registry, TextEncoder,
};

/// Latency buckets: 50 µs … 2.5 s (storage ops are µs-class; the long tail
/// captures shard-queue waits and blocking-poll granularity).
const LATENCY_BUCKETS: &[f64] = &[
    0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0,
    2.5,
];

pub struct Metrics {
    pub registry: Registry,

    // --- commands (engine dispatch) ---
    pub commands_total: IntCounterVec,          // {cmd}
    pub command_errors_total: IntCounterVec,    // {cmd}
    pub command_duration_seconds: HistogramVec, // {cmd}

    // --- client connections / RESP traffic (server) ---
    pub connections_accepted_total: IntCounter,
    pub connections_closed_total: IntCounter,
    pub connected_clients: IntGauge,
    pub net_input_bytes_total: IntCounter,
    pub net_output_bytes_total: IntCounter,

    // --- peer mesh traffic (repl) ---
    pub mesh_input_bytes_total: IntCounter,
    pub mesh_output_bytes_total: IntCounter,
    pub mesh_peers: IntGauge,

    // --- replication (repl) ---
    pub repl_batches_sent_total: IntCounter,
    pub repl_ops_sent_total: IntCounter,
    pub repl_batches_received_total: IntCounter,
    pub repl_ops_applied_total: IntCounter,
    pub fetches_served_total: IntCounter,
    pub fetches_issued_total: IntCounter,
    pub ae_rounds_total: IntCounter,
    pub ae_repair_ops_total: IntCounter,
    pub ring_ops: IntGauge,
    pub ring_bytes: IntGauge,

    // --- cluster (repl stats task) ---
    pub cluster_members: IntGauge,
    pub cluster_underreplicated_partitions: IntGauge,
    pub cluster_effective_rf_min: IntGauge,
    pub cluster_owned_partitions: IntGauge,

    // --- process ---
    pub uptime_seconds: IntGauge,
    pub info: IntGaugeVec, // {version, node_id} = 1
}

macro_rules! counter {
    ($reg:expr, $name:expr, $help:expr) => {{
        let c = IntCounter::new($name, $help).unwrap();
        $reg.register(Box::new(c.clone())).unwrap();
        c
    }};
}

macro_rules! gauge {
    ($reg:expr, $name:expr, $help:expr) => {{
        let g = IntGauge::new($name, $help).unwrap();
        $reg.register(Box::new(g.clone())).unwrap();
        g
    }};
}

impl Metrics {
    pub fn new(node_id: u16) -> Metrics {
        let registry = Registry::new();

        let commands_total = IntCounterVec::new(
            Opts::new("marekvs_commands_total", "Commands processed, by command"),
            &["cmd"],
        )
        .unwrap();
        registry.register(Box::new(commands_total.clone())).unwrap();

        let command_errors_total = IntCounterVec::new(
            Opts::new(
                "marekvs_command_errors_total",
                "Commands that replied with an error, by command",
            ),
            &["cmd"],
        )
        .unwrap();
        registry
            .register(Box::new(command_errors_total.clone()))
            .unwrap();

        let command_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "marekvs_command_duration_seconds",
                "Command service time (parse to reply), by command",
            )
            .buckets(LATENCY_BUCKETS.to_vec()),
            &["cmd"],
        )
        .unwrap();
        registry
            .register(Box::new(command_duration_seconds.clone()))
            .unwrap();

        let info = IntGaugeVec::new(
            Opts::new("marekvs_info", "Static build/runtime info (value is 1)"),
            &["version", "node_id"],
        )
        .unwrap();
        registry.register(Box::new(info.clone())).unwrap();
        info.with_label_values(&[env!("CARGO_PKG_VERSION"), &node_id.to_string()])
            .set(1);

        Metrics {
            commands_total,
            command_errors_total,
            command_duration_seconds,

            connections_accepted_total: counter!(
                registry,
                "marekvs_connections_accepted_total",
                "Client TCP connections accepted"
            ),
            connections_closed_total: counter!(
                registry,
                "marekvs_connections_closed_total",
                "Client TCP connections closed"
            ),
            connected_clients: gauge!(
                registry,
                "marekvs_connected_clients",
                "Currently connected clients"
            ),
            net_input_bytes_total: counter!(
                registry,
                "marekvs_net_input_bytes_total",
                "Bytes read from clients (RESP)"
            ),
            net_output_bytes_total: counter!(
                registry,
                "marekvs_net_output_bytes_total",
                "Bytes written to clients (RESP)"
            ),

            mesh_input_bytes_total: counter!(
                registry,
                "marekvs_mesh_input_bytes_total",
                "Bytes read from peer mesh connections"
            ),
            mesh_output_bytes_total: counter!(
                registry,
                "marekvs_mesh_output_bytes_total",
                "Bytes written to peer mesh connections"
            ),
            mesh_peers: gauge!(
                registry,
                "marekvs_mesh_peers",
                "Peer nodes with at least one live mesh connection"
            ),

            repl_batches_sent_total: counter!(
                registry,
                "marekvs_repl_batches_sent_total",
                "Replication batches pushed to peers"
            ),
            repl_ops_sent_total: counter!(
                registry,
                "marekvs_repl_ops_sent_total",
                "Replication ops pushed to peers"
            ),
            repl_batches_received_total: counter!(
                registry,
                "marekvs_repl_batches_received_total",
                "Replication batches received from peers"
            ),
            repl_ops_applied_total: counter!(
                registry,
                "marekvs_repl_ops_applied_total",
                "Remote ops merged into local storage"
            ),
            fetches_served_total: counter!(
                registry,
                "marekvs_fetches_served_total",
                "Fetch/FetchCollection requests served to peers"
            ),
            fetches_issued_total: counter!(
                registry,
                "marekvs_fetches_issued_total",
                "Read-through fetches issued to home replicas"
            ),
            ae_rounds_total: counter!(
                registry,
                "marekvs_ae_rounds_total",
                "Anti-entropy rounds completed"
            ),
            ae_repair_ops_total: counter!(
                registry,
                "marekvs_ae_repair_ops_total",
                "Records pushed or pulled by anti-entropy repair"
            ),
            ring_ops: gauge!(
                registry,
                "marekvs_ring_ops",
                "Replication ring occupancy (ops)"
            ),
            ring_bytes: gauge!(
                registry,
                "marekvs_ring_bytes",
                "Replication ring occupancy (bytes)"
            ),

            cluster_members: gauge!(
                registry,
                "marekvs_cluster_members",
                "Members in the gossip view"
            ),
            cluster_underreplicated_partitions: gauge!(
                registry,
                "marekvs_cluster_underreplicated_partitions",
                "Partitions with fewer than N home replicas"
            ),
            cluster_effective_rf_min: gauge!(
                registry,
                "marekvs_cluster_effective_rf_min",
                "Minimum effective replication factor across partitions"
            ),
            cluster_owned_partitions: gauge!(
                registry,
                "marekvs_cluster_owned_partitions",
                "Partitions this node homes"
            ),

            uptime_seconds: gauge!(registry, "marekvs_uptime_seconds", "Process uptime"),
            info,
            registry,
        }
    }

    /// Render the registry in Prometheus text exposition format.
    pub fn render(&self, started_at_ms: u64, clients: i64) -> String {
        self.uptime_seconds
            .set(((crate::store::now_ms() - started_at_ms) / 1000) as i64);
        self.connected_clients.set(clients.max(0));
        let mut buf = Vec::new();
        let encoder = TextEncoder::new();
        if let Err(e) = encoder.encode(&self.registry.gather(), &mut buf) {
            tracing::error!(?e, "metrics encode failed");
        }
        String::from_utf8(buf).unwrap_or_default()
    }

    /// Per-command instrumentation used by `Engine::dispatch`.
    pub fn observe_command(&self, cmd: &str, seconds: f64, errored: bool) {
        let cmd_lower = cmd.to_ascii_lowercase();
        self.commands_total.with_label_values(&[&cmd_lower]).inc();
        if errored {
            self.command_errors_total
                .with_label_values(&[&cmd_lower])
                .inc();
        }
        self.command_duration_seconds
            .with_label_values(&[&cmd_lower])
            .observe(seconds);
    }
}

/// A `Histogram` alias kept public for future direct use.
pub type LatencyHistogram = Histogram;

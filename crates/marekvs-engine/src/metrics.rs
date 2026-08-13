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
    pub mesh_conn_timeouts_total: IntCounter,
    /// Peers whose dial loops were torn down after they left the membership
    /// view for longer than MAREKVS_MESH_PEER_GC_SECS (T2-10).
    pub mesh_peers_forgotten_total: IntCounter,
    /// Records dropped by cold purge: this node's local copy of a partition it
    /// no longer owns, released after the delay and the clean-round evidence
    /// (T2-9). Reclaims the disk that every scale event used to strand.
    pub cold_purged_records_total: IntCounter,

    // --- replication (repl) ---
    pub repl_batches_sent_total: IntCounter,
    pub repl_ops_sent_total: IntCounter,
    pub repl_send_failures_total: IntCounter,
    pub repl_window_stalls_total: IntCounter,
    pub repl_inflight_bytes: IntGauge,
    pub repl_batches_received_total: IntCounter,
    pub repl_ops_applied_total: IntCounter,
    pub fetches_served_total: IntCounter,
    pub fetches_issued_total: IntCounter,
    pub ae_rounds_total: IntCounter,
    pub ae_repair_ops_total: IntCounter,
    pub ae_digest_scans_total: IntCounter,
    /// Anti-entropy exchanges abandoned because a partition scan did not
    /// complete. Non-zero means repair rounds are being skipped — the safe
    /// outcome, but convergence stalls until the underlying storage recovers.
    pub ae_scan_failures_total: IntCounter,
    /// Storage scans that ended incomplete, process-wide (mirrors
    /// `store::scan_errors_total`). The alert signal for silently-short reads.
    pub scan_errors_total: IntCounter,

    // --- ondaDB engine internals (design/09 §Storage floor) ---
    /// Resident bytes held by SSTable readers the table cache has open — index
    /// blocks plus bloom filters. Before ondaDB 0.7 this was unbounded and
    /// invisible, and it grew with total stored bytes rather than working set.
    pub db_reader_resident_bytes: IntGauge,
    /// The byte ceiling those readers are held under
    /// (`MAREKVS_MAX_OPEN_READER_BYTES`); `0` means the byte bound is off.
    /// Resident approaching budget means eviction pressure, not a leak.
    pub db_reader_budget_bytes: IntGauge,
    /// SSTable readers currently open.
    pub db_open_readers: IntGauge,
    /// L0 files in the `data` CF. A climbing value means compaction is not
    /// keeping up and reads are probing more tables each level.
    pub db_l0_files: IntGauge,
    /// SSTable probes skipped by a bloom-filter negative, and probes actually
    /// issued. The ratio is the direct check that ondaDB 0.7.1's bloom-sizing
    /// fix is live — before it, compacted tables measured **zero** skips.
    pub db_bloom_skips_total: IntGauge,
    pub db_sst_probes_total: IntGauge,
    /// Successful physical WAL `sync_data()` calls. Under `SyncMode::None` this
    /// never advances, which is what makes it a durability assertion rather
    /// than a restatement of config.
    pub db_wal_syncs_total: IntGauge,
    pub ring_ops: IntGauge,
    pub ring_bytes: IntGauge,
    pub join_gate_pending_pids: IntGauge,
    pub bootstrap_bytes_sent_total: IntCounter,
    pub rejoin_active: IntGauge,
    pub rejoin_dropped_records_total: IntCounter,
    pub interest_entries: IntGauge,
    pub interest_rejected_total: IntCounter,
    pub bootstraps_completed_total: IntCounter,
    pub join_gate_timeouts_total: IntCounter,

    // --- cluster (repl stats task) ---
    pub cluster_members: IntGauge,
    pub cluster_underreplicated_partitions: IntGauge,
    pub cluster_effective_rf_min: IntGauge,
    pub cluster_owned_partitions: IntGauge,

    // --- disk (repl stats task; also the operator disk-autoscale signal) ---
    pub disk_total_bytes: IntGauge,
    pub disk_avail_bytes: IntGauge,
    pub db_total_bytes: IntGauge,
    pub disk_write_stopped: IntGauge,

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
            mesh_conn_timeouts_total: counter!(
                registry,
                "marekvs_mesh_conn_timeouts_total",
                "Mesh connections closed by heartbeat idle timeout"
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
            repl_send_failures_total: counter!(
                registry,
                "marekvs_repl_send_failures_total",
                "Replication batches dropped because the peer's writer queue was full or the peer was absent"
            ),
            repl_window_stalls_total: counter!(
                registry,
                "marekvs_repl_window_stalls_total",
                "Pump passes that skipped a peer because its unacked replication window was full"
            ),
            repl_inflight_bytes: gauge!(
                registry,
                "marekvs_repl_inflight_bytes",
                "Largest per-peer unacked replication window (bytes)"
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
            ae_digest_scans_total: counter!(
                registry,
                "marekvs_ae_digest_scans_total",
                "Full partition scans performed to (re)compute a Merkle root (cache misses)"
            ),
            mesh_peers_forgotten_total: counter!(
                registry,
                "marekvs_mesh_peers_forgotten_total",
                "Departed peers whose dial loops were torn down by peer GC"
            ),
            cold_purged_records_total: counter!(
                registry,
                "marekvs_cold_purged_records_total",
                "Records dropped from partitions this node no longer owns"
            ),
            ae_scan_failures_total: counter!(
                registry,
                "marekvs_ae_scan_failures_total",
                "Anti-entropy exchanges abandoned because a partition scan did not complete"
            ),
            scan_errors_total: counter!(
                registry,
                "marekvs_scan_errors_total",
                "Storage scans that ended incomplete (a reply or digest would have been short)"
            ),
            db_reader_resident_bytes: gauge!(
                registry,
                "marekvs_db_reader_resident_bytes",
                "Resident bytes (block index + bloom) across open ondaDB SSTable readers"
            ),
            db_reader_budget_bytes: gauge!(
                registry,
                "marekvs_db_reader_budget_bytes",
                "Byte budget for open ondaDB SSTable readers (0 = byte bound disabled)"
            ),
            db_open_readers: gauge!(
                registry,
                "marekvs_db_open_readers",
                "ondaDB SSTable readers currently open"
            ),
            db_l0_files: gauge!(
                registry,
                "marekvs_db_l0_files",
                "L0 SSTable count in the data column family (compaction backlog)"
            ),
            db_bloom_skips_total: gauge!(
                registry,
                "marekvs_db_bloom_skips_total",
                "SSTable probes skipped by a bloom-filter negative"
            ),
            db_sst_probes_total: gauge!(
                registry,
                "marekvs_db_sst_probes_total",
                "SSTable probes issued"
            ),
            db_wal_syncs_total: gauge!(
                registry,
                "marekvs_db_wal_syncs_total",
                "Successful physical WAL sync_data() calls"
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

            bootstrap_bytes_sent_total: counter!(
                registry,
                "marekvs_bootstrap_bytes_sent_total",
                "Payload bytes streamed to joining peers (rate-paced)"
            ),
            rejoin_active: gauge!(
                registry,
                "marekvs_rejoin_active",
                "1 while the gc_grace pull-only rejoin is syncing home partitions"
            ),
            rejoin_dropped_records_total: counter!(
                registry,
                "marekvs_rejoin_dropped_records_total",
                "Stale extra records shed during a gc_grace rejoin instead of being served"
            ),
            interest_entries: gauge!(
                registry,
                "marekvs_interest_entries",
                "Live (partition, key, subscriber) interest leases held for peers"
            ),
            interest_rejected_total: counter!(
                registry,
                "marekvs_interest_rejected_total",
                "Interest registrations rejected at MAREKVS_INTEREST_MAX_ENTRIES"
            ),
            join_gate_pending_pids: gauge!(
                registry,
                "marekvs_join_gate_pending_pids",
                "Partitions still holding the join gate (bootstrap or rejoin pending)"
            ),
            bootstraps_completed_total: counter!(
                registry,
                "marekvs_bootstraps_completed_total",
                "Partition bootstrap streams completed (BootstrapDone received)"
            ),
            join_gate_timeouts_total: counter!(
                registry,
                "marekvs_join_gate_timeouts_total",
                "Times the join gate was overridden by MAREKVS_JOIN_TIMEOUT_SECS"
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

            disk_total_bytes: gauge!(
                registry,
                "marekvs_disk_total_bytes",
                "Size of the filesystem holding the data directory"
            ),
            disk_avail_bytes: gauge!(
                registry,
                "marekvs_disk_avail_bytes",
                "Available bytes on the filesystem holding the data directory"
            ),
            db_total_bytes: gauge!(
                registry,
                "marekvs_db_total_bytes",
                "On-disk SSTable bytes reported by the storage engine"
            ),
            disk_write_stopped: gauge!(
                registry,
                "marekvs_disk_write_stopped",
                "1 while client write commands are refused (disk above high-water)"
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

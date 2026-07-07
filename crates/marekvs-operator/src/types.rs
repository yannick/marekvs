//! The `MarekvsCluster` custom resource — the operator's declarative API.
//!
//! One CR describes one marekvs cluster; the controller materializes it as
//! a StatefulSet + Services + PodDisruptionBudget and then keeps the
//! observed cluster converged with the spec, applying the safety rules from
//! k8s/README.md (scale down one node at a time, never below RF+1, only
//! while `marekvs_cluster_underreplicated_partitions == 0`).

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "marekvs.io",
    version = "v1alpha1",
    kind = "MarekvsCluster",
    namespaced,
    status = "MarekvsClusterStatus",
    shortname = "mkv",
    printcolumn = r#"{"name":"Desired","type":"integer","jsonPath":".status.desiredNodes"}"#,
    printcolumn = r#"{"name":"Ready","type":"integer","jsonPath":".status.readyNodes"}"#,
    printcolumn = r#"{"name":"Underrep","type":"integer","jsonPath":".status.underreplicatedPartitions"}"#,
    printcolumn = r#"{"name":"Ops/s","type":"string","jsonPath":".status.opsPerSecond"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct MarekvsClusterSpec {
    /// Container image for the marekvs nodes.
    pub image: String,

    /// Desired node count. With `autoscale` set this is only the initial
    /// size; the controller then owns the count within [minNodes, maxNodes].
    #[serde(default = "default_nodes")]
    pub nodes: i32,

    /// MAREKVS_REPLICAS_N: how many nodes hold each partition. The
    /// controller never lets the cluster shrink below replicationFactor+1.
    #[serde(default = "default_rf")]
    pub replication_factor: i32,

    #[serde(default)]
    pub storage: StorageSpec,

    /// Container resources (memory request is also used as the limit — no
    /// burst-OOM surprises; CPU is a request only, never a limit).
    #[serde(default)]
    pub resources: ResourcesSpec,

    /// Enable ops/s-driven autoscaling.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub autoscale: Option<AutoscaleSpec>,

    /// Delete the PVCs of scaled-away ordinals once the cluster is fully
    /// replicated again. Default false: volumes are kept so a later
    /// scale-up resumes from existing data.
    #[serde(default)]
    pub reclaim_pvcs: bool,

    /// Extra environment for the marekvs container (e.g. RUST_LOG).
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub extra_env: std::collections::BTreeMap<String, String>,

    /// Restrict marekvs pods to nodes with these labels.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub node_selector: std::collections::BTreeMap<String, String>,

    /// Tolerations for the marekvs pods (e.g. dedicated node pools).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tolerations: Vec<Toleration>,

    /// `whenUnsatisfiable` for the per-hostname topology spread constraint.
    /// Default is ScheduleAnyway (best-effort one pod per node). Set to
    /// DoNotSchedule to make one-per-node a hard guarantee: excess pods stay
    /// Pending, which lets a cluster-autoscaler add nodes instead of
    /// doubling up. Untolerated-tainted nodes are excluded from the skew
    /// calculation (nodeTaintsPolicy: Honor) so e.g. control-plane nodes
    /// don't count as empty domains and wedge scheduling.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname_spread_when_unsatisfiable: Option<String>,
}

/// core/v1 Toleration, restated locally: the CRD schema needs JsonSchema,
/// which k8s-openapi types don't derive without its `schemars` feature.
/// Field names serialize to the exact core/v1 wire shape.
#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Toleration {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// Equal | Exists
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operator: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    /// NoSchedule | PreferNoSchedule | NoExecute
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effect: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub toleration_seconds: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct StorageSpec {
    /// PVC size per node.
    #[serde(default = "default_storage_size")]
    pub size: String,
    /// StorageClass; cluster default when unset. Prefer local NVMe.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub class_name: Option<String>,
}

impl Default for StorageSpec {
    fn default() -> Self {
        Self {
            size: default_storage_size(),
            class_name: None,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ResourcesSpec {
    /// CPU request (no limit is ever set — throttling hurts tail latency).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<String>,
    /// Memory request AND limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct AutoscaleSpec {
    /// Floor. Clamped up to replicationFactor+1 regardless of this value.
    pub min_nodes: i32,
    /// Ceiling.
    pub max_nodes: i32,
    /// Sustained commands/sec one node should carry. The controller sizes
    /// the cluster as ceil(total_ops / target) with hysteresis.
    pub target_ops_per_node: f64,
    /// How long the load must stay low before removing a node (also the
    /// minimum gap between two scale-downs).
    #[serde(default = "default_stabilization")]
    pub scale_down_stabilization_seconds: i64,
    /// Minimum gap between two scale-ups.
    #[serde(default = "default_up_cooldown")]
    pub scale_up_cooldown_seconds: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MarekvsClusterStatus {
    /// Reconciling | Healthy | ScalingUp | ScalingDown | Blocked
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// The controller's current node-count target (spec.nodes or the
    /// autoscaler's decision).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub desired_nodes: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready_nodes: Option<i32>,
    /// Worst value observed across nodes; 0 = safe to scale down.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub underreplicated_partitions: Option<i64>,
    /// Cluster-wide command rate from the last two metric samples,
    /// stringified to keep the CRD schema integer-free (printer column).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ops_per_second: Option<String>,
    /// Unix seconds of the last completed scale operation; gates the
    /// autoscaler's cooldown/stabilization windows.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_scale_epoch: Option<i64>,
    /// Rate-sample state (survives controller restarts).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_ops_total: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_sample_epoch: Option<i64>,
}

pub fn default_nodes() -> i32 {
    3
}
pub fn default_rf() -> i32 {
    2
}
fn default_storage_size() -> String {
    "10Gi".into()
}
pub fn default_stabilization() -> i64 {
    300
}
pub fn default_up_cooldown() -> i64 {
    60
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The shipped example must always parse with the real types — this is
    /// the drift guard between k8s/operator/example-cluster.yaml and the CRD.
    #[test]
    fn example_cluster_parses() {
        let raw = include_str!("../../../k8s/operator/example-cluster.yaml");
        let doc: serde_yaml::Value = serde_yaml::from_str(raw).unwrap();
        let spec: MarekvsClusterSpec = serde_yaml::from_value(doc["spec"].clone()).unwrap();
        assert_eq!(spec.replication_factor, 2);
        let a = spec.autoscale.expect("example demonstrates autoscaling");
        assert_eq!((a.min_nodes, a.max_nodes), (3, 8));
        assert_eq!(a.target_ops_per_node, 5000.0);
        assert_eq!(a.scale_down_stabilization_seconds, 300);
        assert!(!spec.reclaim_pvcs);
    }
}

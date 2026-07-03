//! Builders for the child objects of a MarekvsCluster. Kept in lockstep
//! with the hand-written example in k8s/ (the operator is its automation).

use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::Service;
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use kube::api::Resource;
use kube::ResourceExt;
use serde_json::json;

use crate::types::MarekvsCluster;

fn owner_ref(cr: &MarekvsCluster) -> serde_json::Value {
    let oref = cr.controller_owner_ref(&()).expect("cr has uid");
    serde_json::to_value(&oref).expect("owner ref serializes")
}

fn labels(cr: &MarekvsCluster) -> serde_json::Value {
    json!({
        "app": cr.name_any(),
        "app.kubernetes.io/name": "marekvs",
        "app.kubernetes.io/instance": cr.name_any(),
        "app.kubernetes.io/managed-by": "marekvs-operator",
    })
}

pub fn statefulset(cr: &MarekvsCluster, replicas: i32) -> StatefulSet {
    let name = cr.name_any();
    let ns = cr.namespace().unwrap_or_default();
    let spec = &cr.spec;
    let mut env = vec![
        json!({"name": "MAREKVS_SEEDS",
               "value": format!("{name}-headless.{ns}.svc.cluster.local:7946")}),
        json!({"name": "MAREKVS_ADVERTISE_IP", "value": "auto"}),
        json!({"name": "MAREKVS_REPLICAS_N",
               "value": spec.replication_factor.to_string()}),
        json!({"name": "MAREKVS_DATA_DIR", "value": "/data"}),
    ];
    for (k, v) in &spec.extra_env {
        env.push(json!({"name": k, "value": v}));
    }
    let memory = spec
        .resources
        .memory
        .clone()
        .unwrap_or_else(|| "1Gi".into());
    let cpu = spec.resources.cpu.clone().unwrap_or_else(|| "500m".into());
    let mut pvc_spec = json!({
        "accessModes": ["ReadWriteOnce"],
        "resources": {"requests": {"storage": spec.storage.size}},
    });
    if let Some(class) = &spec.storage.class_name {
        pvc_spec["storageClassName"] = json!(class);
    }

    serde_json::from_value(json!({
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {
            "name": name,
            "namespace": ns,
            "labels": labels(cr),
            "ownerReferences": [owner_ref(cr)],
        },
        "spec": {
            "serviceName": format!("{name}-headless"),
            "replicas": replicas,
            "podManagementPolicy": "Parallel",
            "selector": {"matchLabels": {"app": name}},
            "template": {
                "metadata": {"labels": labels(cr)},
                "spec": {
                    "terminationGracePeriodSeconds": 60,
                    "securityContext": {
                        "runAsNonRoot": true,
                        "runAsUser": 65534,
                        "runAsGroup": 65534,
                        "fsGroup": 65534,
                    },
                    "topologySpreadConstraints": [
                        {"maxSkew": 1, "topologyKey": "kubernetes.io/hostname",
                         "whenUnsatisfiable": "ScheduleAnyway",
                         "labelSelector": {"matchLabels": {"app": name}}},
                        {"maxSkew": 1, "topologyKey": "topology.kubernetes.io/zone",
                         "whenUnsatisfiable": "ScheduleAnyway",
                         "labelSelector": {"matchLabels": {"app": name}}},
                    ],
                    "containers": [{
                        "name": "marekvs",
                        "image": spec.image,
                        "ports": [
                            {"containerPort": 6379, "name": "resp"},
                            {"containerPort": 7373, "name": "mesh"},
                            {"containerPort": 7946, "name": "gossip", "protocol": "UDP"},
                            {"containerPort": 9121, "name": "metrics"},
                        ],
                        "env": env,
                        "volumeMounts": [{"name": "data", "mountPath": "/data"}],
                        "resources": {
                            "requests": {"cpu": cpu, "memory": memory},
                            "limits": {"memory": memory},
                        },
                        "lifecycle": {"preStop": {"httpGet": {"port": "metrics", "path": "/drain"}}},
                        "startupProbe": {
                            "httpGet": {"port": "metrics", "path": "/ready"},
                            "failureThreshold": 180, "periodSeconds": 2,
                        },
                        "readinessProbe": {
                            "httpGet": {"port": "metrics", "path": "/ready"},
                            "periodSeconds": 2,
                        },
                        "livenessProbe": {
                            "httpGet": {"port": "metrics", "path": "/alive"},
                            "initialDelaySeconds": 10, "periodSeconds": 5,
                        },
                    }],
                },
            },
            "volumeClaimTemplates": [{
                "metadata": {"name": "data", "labels": labels(cr)},
                "spec": pvc_spec,
            }],
        },
    }))
    .expect("statefulset json is valid")
}

pub fn client_service(cr: &MarekvsCluster) -> Service {
    let name = cr.name_any();
    serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": name,
            "namespace": cr.namespace().unwrap_or_default(),
            "labels": labels(cr),
            "ownerReferences": [owner_ref(cr)],
        },
        "spec": {
            "selector": {"app": name},
            "ports": [{"port": 6379, "targetPort": "resp", "name": "resp"}],
            "trafficDistribution": "PreferClose",
        },
    }))
    .expect("service json is valid")
}

pub fn headless_service(cr: &MarekvsCluster) -> Service {
    let name = cr.name_any();
    serde_json::from_value(json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": format!("{name}-headless"),
            "namespace": cr.namespace().unwrap_or_default(),
            "labels": labels(cr),
            "ownerReferences": [owner_ref(cr)],
        },
        "spec": {
            "clusterIP": "None",
            "publishNotReadyAddresses": true,
            "selector": {"app": name},
            "ports": [
                {"port": 7946, "name": "gossip", "protocol": "UDP"},
                {"port": 7373, "name": "mesh"},
                {"port": 9121, "name": "metrics"},
            ],
        },
    }))
    .expect("service json is valid")
}

pub fn pdb(cr: &MarekvsCluster) -> PodDisruptionBudget {
    let name = cr.name_any();
    serde_json::from_value(json!({
        "apiVersion": "policy/v1",
        "kind": "PodDisruptionBudget",
        "metadata": {
            "name": name,
            "namespace": cr.namespace().unwrap_or_default(),
            "labels": labels(cr),
            "ownerReferences": [owner_ref(cr)],
        },
        "spec": {
            "minAvailable": cr.spec.replication_factor,
            "selector": {"matchLabels": {"app": name}},
        },
    }))
    .expect("pdb json is valid")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::MarekvsClusterSpec;

    fn cr() -> MarekvsCluster {
        let mut cr = MarekvsCluster::new(
            "demo",
            MarekvsClusterSpec {
                image: "marekvs:test".into(),
                nodes: 3,
                replication_factor: 2,
                storage: Default::default(),
                resources: Default::default(),
                autoscale: None,
                reclaim_pvcs: false,
                extra_env: Default::default(),
            },
        );
        cr.metadata.namespace = Some("ns1".into());
        cr.metadata.uid = Some("uid-1".into());
        cr
    }

    #[test]
    fn statefulset_wires_identity_and_rf() {
        let sts = statefulset(&cr(), 3);
        let spec = sts.spec.unwrap();
        assert_eq!(spec.replicas, Some(3));
        assert_eq!(spec.service_name.as_deref(), Some("demo-headless"));
        let env = spec.template.spec.unwrap().containers[0]
            .env
            .clone()
            .unwrap();
        let get = |n: &str| {
            env.iter()
                .find(|e| e.name == n)
                .and_then(|e| e.value.clone())
                .unwrap()
        };
        assert_eq!(get("MAREKVS_REPLICAS_N"), "2");
        assert_eq!(
            get("MAREKVS_SEEDS"),
            "demo-headless.ns1.svc.cluster.local:7946"
        );
    }

    #[test]
    fn children_carry_owner_refs() {
        for owners in [
            statefulset(&cr(), 3).metadata.owner_references,
            client_service(&cr()).metadata.owner_references,
            headless_service(&cr()).metadata.owner_references,
            pdb(&cr()).metadata.owner_references,
        ] {
            let o = &owners.unwrap()[0];
            assert_eq!(o.kind, "MarekvsCluster");
            assert!(o.controller.unwrap());
        }
    }

    #[test]
    fn pdb_floor_is_rf() {
        let p = pdb(&cr()).spec.unwrap();
        assert_eq!(
            p.min_available,
            Some(k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(2))
        );
    }
}

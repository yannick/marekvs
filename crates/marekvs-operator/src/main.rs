//! marekvs-operator — Kubernetes controller for `MarekvsCluster` resources.
//!
//! Reconcile loop (design/12):
//! 1. Server-side-apply the child objects (StatefulSet, Services, PDB).
//! 2. Scrape every pod's :9121/metrics — ops rate + worst
//!    underreplicated-partitions value.
//! 3. Decide the node-count target: spec.nodes, or the autoscaler.
//! 4. Step the StatefulSet toward it under the safety rules (up freely,
//!    down one node at a time, only while fully replicated).
//! 5. Optionally reclaim PVCs of retired ordinals; publish status.
//!
//! `marekvs-operator crd` prints the CRD YAML (k8s/operator/crd.yaml is
//! generated from it — `just operator-crd`).

mod promtext;
mod resources;
mod scale;
mod types;

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::{PersistentVolumeClaim, Pod, Service};
use k8s_openapi::api::policy::v1::PodDisruptionBudget;
use kube::api::{Api, ListParams, Patch, PatchParams};
use kube::runtime::controller::{Action, Controller};
use kube::runtime::watcher;
use kube::{Client, CustomResourceExt, ResourceExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use scale::{autoscale_target, next_replicas, ops_rate, BlockReason, Observed, Step};
use types::{MarekvsCluster, MarekvsClusterStatus};

const MANAGER: &str = "marekvs-operator";
const RECONCILE_EVERY: Duration = Duration::from_secs(30);

#[derive(thiserror::Error, Debug)]
enum Error {
    #[error("kube api: {0}")]
    Kube(#[from] kube::Error),
}

struct Ctx {
    client: Client,
}

/// Plain HTTP GET against a pod IP (the image is scratch; the metrics
/// endpoint speaks minimal HTTP/1.1 — no TLS, no client library needed).
async fn fetch_metrics(ip: &str, port: u16) -> Option<String> {
    let fut = async {
        let mut s = tokio::net::TcpStream::connect((ip, port)).await.ok()?;
        s.write_all(
            format!("GET /metrics HTTP/1.1\r\nHost: {ip}\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .await
        .ok()?;
        let mut buf = Vec::with_capacity(64 * 1024);
        s.read_to_end(&mut buf).await.ok()?;
        let text = String::from_utf8_lossy(&buf);
        text.split_once("\r\n\r\n")
            .map(|(_, body)| body.to_string())
    };
    tokio::time::timeout(Duration::from_secs(3), fut)
        .await
        .ok()?
}

struct ClusterMetrics {
    ops_total: Option<f64>,
    /// Worst (max) underreplicated count across responding nodes; None when
    /// no node responded at all.
    underreplicated: Option<i64>,
    scraped: usize,
}

async fn scrape(client: &Client, cr: &MarekvsCluster) -> ClusterMetrics {
    let ns = cr.namespace().unwrap_or_default();
    let pods: Api<Pod> = Api::namespaced(client.clone(), &ns);
    let lp = ListParams::default().labels(&format!("app={}", cr.name_any()));
    let list = match pods.list(&lp).await {
        Ok(l) => l,
        Err(_) => {
            return ClusterMetrics {
                ops_total: None,
                underreplicated: None,
                scraped: 0,
            }
        }
    };
    let mut ops = 0.0;
    let mut got_ops = false;
    let mut under: Option<i64> = None;
    let mut scraped = 0;
    for pod in &list.items {
        let Some(ip) = pod.status.as_ref().and_then(|s| s.pod_ip.clone()) else {
            continue;
        };
        let Some(body) = fetch_metrics(&ip, 9121).await else {
            continue;
        };
        scraped += 1;
        if let Some(v) = promtext::sum_metric(&body, "marekvs_commands_total") {
            ops += v;
            got_ops = true;
        }
        if let Some(u) = promtext::gauge(&body, "marekvs_cluster_underreplicated_partitions") {
            under = Some(under.unwrap_or(0).max(u));
        }
    }
    ClusterMetrics {
        ops_total: got_ops.then_some(ops),
        underreplicated: under,
        scraped,
    }
}

fn now_epoch() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

async fn reconcile(cr: Arc<MarekvsCluster>, ctx: Arc<Ctx>) -> Result<Action, Error> {
    let client = &ctx.client;
    let ns = cr.namespace().unwrap_or_default();
    let name = cr.name_any();
    let ssapply = PatchParams::apply(MANAGER).force();

    let stss: Api<StatefulSet> = Api::namespaced(client.clone(), &ns);
    let svcs: Api<Service> = Api::namespaced(client.clone(), &ns);
    let pdbs: Api<PodDisruptionBudget> = Api::namespaced(client.clone(), &ns);
    let crs: Api<MarekvsCluster> = Api::namespaced(client.clone(), &ns);

    // ── children that don't depend on the scaling decision ──────────────
    let headless = resources::headless_service(&cr);
    svcs.patch(&headless.name_any(), &ssapply, &Patch::Apply(&headless))
        .await?;
    let svc = resources::client_service(&cr);
    svcs.patch(&svc.name_any(), &ssapply, &Patch::Apply(&svc))
        .await?;
    let pdb = resources::pdb(&cr);
    pdbs.patch(&pdb.name_any(), &ssapply, &Patch::Apply(&pdb))
        .await?;

    // ── observe ──────────────────────────────────────────────────────────
    let existing = stss.get_opt(&name).await?;
    let current = existing
        .as_ref()
        .and_then(|s| s.spec.as_ref())
        .and_then(|s| s.replicas)
        .unwrap_or(0);
    let ready = existing
        .as_ref()
        .and_then(|s| s.status.as_ref())
        .and_then(|s| s.ready_replicas)
        .unwrap_or(0);

    let m = scrape(client, &cr).await;
    let now = now_epoch();
    let prev = cr.status.clone().unwrap_or_default();
    let rate = match m.ops_total {
        Some(total) => ops_rate(prev.last_ops_total, prev.last_sample_epoch, total, now),
        None => None,
    };

    let obs = Observed {
        current,
        ready,
        underreplicated: m.underreplicated,
        ops_per_second: rate,
        now_epoch: now,
        last_scale_epoch: prev.last_scale_epoch,
    };

    // ── decide ───────────────────────────────────────────────────────────
    let rf = cr.spec.replication_factor;
    let target = match &cr.spec.autoscale {
        Some(a) => autoscale_target(a, rf, &obs),
        None => cr.spec.nodes.max(rf + 1),
    };
    // First reconcile: no StatefulSet yet → create at the full target.
    let step = if existing.is_none() {
        Step::Set(target)
    } else {
        next_replicas(target, &obs)
    };

    let (replicas, phase, message) = match step {
        Step::Set(n) => {
            let phase = if existing.is_none() {
                "Reconciling"
            } else if n > current {
                "ScalingUp"
            } else {
                "ScalingDown"
            };
            (
                n,
                phase,
                format!("replicas {current} → {n} (target {target})"),
            )
        }
        Step::Hold => (current, "Healthy", format!("at target {target}")),
        Step::Blocked(why) => {
            let msg = match why {
                BlockReason::Underreplicated => format!(
                    "scale-down to {target} deferred: {} partitions under-replicated",
                    m.underreplicated.unwrap_or(-1)
                ),
                BlockReason::NotAllReady => {
                    format!("scale-down to {target} deferred: {ready}/{current} pods ready")
                }
                BlockReason::NoMetrics => format!(
                    "scale-down to {target} deferred: metrics unreachable ({} pods scraped)",
                    m.scraped
                ),
            };
            (current, "Blocked", msg)
        }
    };

    // ── act ──────────────────────────────────────────────────────────────
    let sts = resources::statefulset(&cr, replicas);
    stss.patch(&name, &ssapply, &Patch::Apply(&sts)).await?;

    // Reclaim PVCs of retired ordinals once the cluster is provably whole.
    if cr.spec.reclaim_pvcs
        && matches!(step, Step::Hold)
        && m.underreplicated == Some(0)
        && ready == current
    {
        let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), &ns);
        let lp = ListParams::default().labels(&format!("app={name}"));
        for pvc in pvcs.list(&lp).await?.items {
            let pn = pvc.name_any();
            let ordinal = pn
                .strip_prefix(&format!("data-{name}-"))
                .and_then(|s| s.parse::<i32>().ok());
            if let Some(o) = ordinal {
                if o >= replicas {
                    tracing::info!(pvc = %pn, "reclaiming PVC of retired ordinal");
                    let _ = pvcs.delete(&pn, &Default::default()).await;
                }
            }
        }
    }

    // ── publish status ───────────────────────────────────────────────────
    let scaled = replicas != current;
    let status = MarekvsClusterStatus {
        phase: Some(phase.into()),
        message: Some(message.clone()),
        desired_nodes: Some(target),
        ready_nodes: Some(ready),
        underreplicated_partitions: m.underreplicated,
        ops_per_second: rate.map(|r| format!("{r:.1}")),
        last_scale_epoch: if scaled {
            Some(now)
        } else {
            prev.last_scale_epoch
        },
        last_ops_total: m.ops_total.or(prev.last_ops_total),
        last_sample_epoch: if m.ops_total.is_some() {
            Some(now)
        } else {
            prev.last_sample_epoch
        },
    };
    crs.patch_status(
        &name,
        &PatchParams::default(),
        &Patch::Merge(serde_json::json!({ "status": status })),
    )
    .await?;

    tracing::info!(cluster = %name, phase, %message, "reconciled");
    Ok(Action::requeue(RECONCILE_EVERY))
}

fn error_policy(_cr: Arc<MarekvsCluster>, err: &Error, _ctx: Arc<Ctx>) -> Action {
    tracing::warn!(%err, "reconcile failed");
    Action::requeue(Duration::from_secs(10))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if std::env::args().nth(1).as_deref() == Some("crd") {
        print!("{}", serde_yaml::to_string(&MarekvsCluster::crd())?);
        return Ok(());
    }
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,kube=warn".into()),
        )
        .init();

    let client = Client::try_default().await?;
    let crs: Api<MarekvsCluster> = Api::all(client.clone());
    let stss: Api<StatefulSet> = Api::all(client.clone());
    tracing::info!("marekvs-operator starting");

    Controller::new(crs, watcher::Config::default())
        .owns(stss, watcher::Config::default())
        .shutdown_on_signal()
        .run(reconcile, error_policy, Arc::new(Ctx { client }))
        .for_each(|res| async move {
            if let Err(e) = res {
                tracing::warn!(?e, "controller event error");
            }
        })
        .await;
    Ok(())
}

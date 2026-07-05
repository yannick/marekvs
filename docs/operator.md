---
title: Operator
description: The marekvs-operator and the MarekvsCluster CRD — reconcile loop, safe one-at-a-time scaling, and ops/s-based autoscaling.
status: implemented
---

marekvs ships a Kubernetes operator (`crates/marekvs-operator`, manifests in
[`k8s/operator/`](https://github.com/yannick/marekvs/tree/main/k8s/operator))
that manages clusters through a single custom resource. It reconciles the
Services, PodDisruptionBudget and StatefulSet you would otherwise write by hand
(see [Kubernetes](../kubernetes/)), and — crucially — it encodes the scale-down
runbook that plain YAML cannot express.

## Why an operator at all

marekvs needs *less* from an operator than most stateful systems: gossip
membership, HRW placement and Merkle anti-entropy already self-manage the data
plane, and a bare StatefulSet works. What cannot be expressed in YAML is the
**scale-down runbook**:

> scale down one node at a time, never below `RF + 1`, and only while
> `marekvs_cluster_underreplicated_partitions == 0`.

A human can follow that; an HPA cannot. The operator turns the runbook into a
reconcile loop — which is also the precondition for safe autoscaling, since an
autoscaler is only safe if every step it takes passes those same guards.

## The `MarekvsCluster` CRD

One custom resource describes one cluster. Group/version is
`marekvs.io/v1alpha1`, kind `MarekvsCluster`, short name `mkv`, namespaced. The
CRD YAML is generated from the Rust types (`marekvs-operator crd`, or
`just operator-crd`), so the schema can never drift from the controller.

Here is the shipped example ([`k8s/operator/example-cluster.yaml`](https://github.com/yannick/marekvs/blob/main/k8s/operator/example-cluster.yaml))
— an autoscaling cluster sized between 3 and 8 nodes at ~5000 commands/sec per
node:

```yaml
apiVersion: marekvs.io/v1alpha1
kind: MarekvsCluster
metadata:
  name: demo
spec:
  image: ghcr.io/yannick/marekvs:latest
  replicationFactor: 2
  storage:
    size: 10Gi
    # className: local-nvme
  resources:
    cpu: "1"
    memory: 1Gi
  autoscale:
    minNodes: 3
    maxNodes: 8
    targetOpsPerNode: 5000
    scaleDownStabilizationSeconds: 300
    scaleUpCooldownSeconds: 60
  # Keep volumes of scaled-away nodes (default). Set true to have the
  # operator delete them once the cluster is fully replicated again.
  reclaimPvcs: false
  extraEnv:
    RUST_LOG: info
```

The fields, and how the controller reads them:

| Field | Default | Meaning |
|---|---|---|
| `image` | *(required)* | Container image for the marekvs nodes. |
| `nodes` | `3` | Fixed node count. With `autoscale` set, only the initial size; the controller then owns the count within `[minNodes, maxNodes]`. |
| `replicationFactor` | `2` | `MAREKVS_REPLICAS_N`. The controller never shrinks below `replicationFactor + 1`. |
| `storage.size` / `storage.className` | `10Gi` / cluster default | PVC size and StorageClass per node. |
| `resources.cpu` / `resources.memory` | — | CPU is a **request only** (no limit — throttling hurts tail latency); memory is used as **both request and limit** (no burst-OOM). |
| `autoscale` | *(unset)* | Omit for a fixed-size cluster; set to enable ops/s-based sizing (below). |
| `reclaimPvcs` | `false` | Delete PVCs of scaled-away ordinals once fully replicated again. Default keeps them so a later scale-up resumes from existing data. |
| `extraEnv` | `{}` | Extra environment for the marekvs container (e.g. `RUST_LOG`). |

Apply it and watch the printer columns fed by `.status`:

```sh
just operator-apply                               # CRD + RBAC + controller
kubectl apply -f k8s/operator/example-cluster.yaml
kubectl get mkv
# NAME   DESIRED  READY  UNDERREP  OPS/S    PHASE
# demo   3        3      0         4231.0   Healthy
```

`phase` is one of `Reconciling`, `Healthy`, `ScalingUp`, `ScalingDown`, or
`Blocked`.

## The reconcile loop

The controller reconciles on every CR or StatefulSet change and every 30 s
([`main.rs`](https://github.com/yannick/marekvs/blob/main/crates/marekvs-operator/src/main.rs)):

1. **Apply children.** Server-side apply (field manager `marekvs-operator`) of
   the headless + client Services, the PDB (`minAvailable = replicationFactor`),
   and the StatefulSet. All children carry owner references — deleting the CR
   garbage-collects everything **except** PVCs, so data outlives the API object
   on purpose.
2. **Observe.** Read the StatefulSet's `replicas` / `readyReplicas`, then scrape
   every pod's `:9121/metrics` over plain HTTP against the pod IP (the same
   endpoint the probes use — no TLS, no client library). It sums
   `marekvs_commands_total` across pods for the ops counter and takes the
   **worst** (max) per-node `marekvs_cluster_underreplicated_partitions` — each
   node computes its own view, and the conservative merge is the maximum.
3. **Decide the target.** `spec.nodes` (fixed) or the autoscaler (below). Either
   way the floor is `replicationFactor + 1`.
4. **Step toward the target** — the safety stepper (`scale.rs::next_replicas`,
   unit-tested):
   - target **>** current → jump straight there (adding nodes never removes data
     copies; `podManagementPolicy: Parallel` boots them concurrently);
   - target **<** current → **one** ordinal per reconcile, and only if
     `underreplicated == 0` **and** every pod is ready **and** metrics were
     actually scraped. Otherwise the CR reports `phase: Blocked` with the reason,
     and the next reconcile retries.
5. **Reclaim PVCs** (only when `reclaimPvcs: true`): once the cluster is at
   target, fully ready, and fully replicated, PVCs of ordinals `≥ replicas` are
   deleted. Default keeps them.
6. **Publish status** and requeue.

## Safe scaling

The stepper is a pure function — no Kubernetes types, fully unit-tested — that
answers a single question: *what may the StatefulSet be set to right now?* Its
logic is exactly the runbook:

```rust
pub fn next_replicas(target: i32, obs: &Observed) -> Step {
    match target.cmp(&obs.current) {
        Equal   => Step::Hold,
        Greater => Step::Set(target),              // up in one jump
        Less    => match obs.underreplicated {
            None            => Step::Blocked(NoMetrics),        // no blind scale-down
            Some(n) if n > 0 => Step::Blocked(Underreplicated),
            Some(_) if obs.ready < obs.current => Step::Blocked(NotAllReady),
            Some(_)         => Step::Set(obs.current - 1),      // one node only
        },
    }
}
```

So a scale-down proceeds one ordinal at a time and stalls — visibly, as
`phase: Blocked` with a reason — whenever the cluster is not provably safe:
metrics unreachable, some partition under-replicated, or a pod not yet ready. It
never drops below `replicationFactor + 1`, because the target itself is clamped
to that floor before the stepper ever sees it.

## Autoscaling

With `spec.autoscale` set, the target node count is driven by cluster-wide
command rate, computed operator-side from two consecutive `marekvs_commands_total`
samples. The sample state (`lastOpsTotal`, `lastSampleEpoch`) lives in the CR
**status**, so an operator restart loses at most one interval and never
mis-computes a rate; a counter reset (pod restart makes the sum drop) discards
the sample and re-baselines rather than reporting a bogus negative rate.

The sizing decision (`scale.rs::autoscale_target`, unit-tested):

```text
ideal = ceil(total_ops_per_sec / targetOpsPerNode)
floor = max(minNodes, replicationFactor + 1)
ceil  = maxNodes
```

- **Up:** whenever `ideal > current` and `scaleUpCooldownSeconds` has passed
  since the last scale — undersized clusters hurt immediately, so the jump goes
  straight to `ideal`.
- **Down:** at most one node per `scaleDownStabilizationSeconds` window, and only
  when the load would stay **< 60 % of target after the removal** — the
  hysteresis band that stops the cluster flapping around the threshold. The
  safety stepper then still gates the actual removal on full replication.
- **No signal** (bootstrap, scrape failure): hold position. Autoscaling degrades
  to "do nothing", never to "guess".

Worked example — `targetOpsPerNode: 5000`, 3 nodes: load rises to 22 k ops/s →
`ideal = 5`, cluster scales 3→5 in one step. Load falls to 6 k → `ideal = 2`,
floor = 3; `6000 / 4 = 1500 < 3000` (the 60 % band) → 5→4 after the
stabilization window, then 4→3 after another window, each step waiting for
`underreplicated == 0`.

Without `spec.autoscale` the operator manages a fixed `spec.nodes` — you still
get the safe stepping, PVC handling, and status reporting.

## Failure modes

The controller is designed to fail safe: the data plane never depends on it, and
every uncertain situation blocks a scale-down rather than risking data.

| Failure | Behavior |
|---|---|
| Metrics unreachable | Scale-downs blocked (`Blocked: metrics unreachable`); scale-ups and fixed-size reconciliation unaffected. |
| Operator down | Nothing scales; the data plane is untouched. |
| Operator restarts mid-scale-down | State lives in the CR status + the cluster, not in memory; it re-observes and continues. |
| Pod restart during sampling | Counter sum drops → sample discarded, re-baselined. |
| CR deleted | Children GC'd via owner refs; PVCs survive. Recreate the CR with the same name to re-adopt the data. |
| Two operators running | Server-side apply with one field manager keeps them from fighting — but still run a single replica (see below). |

## RBAC

The operator runs under a dedicated ServiceAccount with a cluster-scoped watch
and namespaced children ([`rbac.yaml`](https://github.com/yannick/marekvs/blob/main/k8s/operator/rbac.yaml)):

- `marekvsclusters` + `marekvsclusters/status` — `get`, `list`, `watch`,
  `patch`, `update`.
- `statefulsets`, `services`, `poddisruptionbudgets` — `get`, `list`, `watch`,
  `create`, `patch`.
- `pods` — `get`, `list`, `watch` (the metrics scrape needs pod IPs).
- `persistentvolumeclaims` — `get`, `list`, `delete` (PVC reclaim).

No secrets, no `exec`, no node access.

## Single replica today

```planned title="Operator HA is future work"
The operator runs as a **single replica** — there is no leader election yet, so
running two replicas is not supported (server-side apply keeps them from
corrupting state, but they would still race). Planned follow-ups:

- Leader election → an HA operator Deployment.
- Disk-fill as a second autoscale signal (needs a data-size gauge in the server
  metrics first).
- Version rollouts gated on cluster health between pods (today: a plain
  StatefulSet rolling update when `spec.image` changes — already drain-safe via
  `preStop`, but not health-gated pod-to-pod).
- `kubectl scale` subresource support.
```

## Where to go next

- The hand-run equivalent, and the manifests the operator generates:
  [Kubernetes](../kubernetes/).
- Every environment variable the container reads: [Build & deploy](../build-deploy/#configuration).

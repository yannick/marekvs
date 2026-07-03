# 12 — Kubernetes Operator (marekvs-operator)

Status: **implemented** (v1: reconcile + safe scaling + ops/s autoscaling).
Crate: `crates/marekvs-operator`; manifests: `k8s/operator/`.

## Why an operator at all

marekvs needs less from an operator than most stateful systems — gossip
membership, HRW placement and Merkle anti-entropy already self-manage the
data plane, and a plain StatefulSet works (that example remains in `k8s/`).
What cannot be expressed in YAML is the **scale-down runbook** from
k8s/README.md:

> scale down one node at a time, never below RF+1, and only while
> `marekvs_cluster_underreplicated_partitions == 0`.

A human can follow it; an HPA cannot. The operator turns the runbook into a
reconcile loop — which is also precisely the prerequisite for autoscaling,
since an autoscaler is only safe if every step it takes goes through those
same guards.

## The API: `MarekvsCluster`

```yaml
apiVersion: marekvs.io/v1alpha1
kind: MarekvsCluster
metadata: { name: demo }
spec:
  image: ghcr.io/yannick/marekvs:latest
  nodes: 3                    # fixed size (ignored while autoscale is set)
  replicationFactor: 2        # MAREKVS_REPLICAS_N; floor = RF+1 nodes
  storage: { size: 10Gi, className: local-nvme }
  resources: { cpu: "1", memory: 1Gi }
  autoscale:                  # optional — omit for a fixed-size cluster
    minNodes: 3
    maxNodes: 8
    targetOpsPerNode: 5000    # sustained commands/sec one node should carry
    scaleDownStabilizationSeconds: 300
    scaleUpCooldownSeconds: 60
  reclaimPvcs: false          # true: delete PVCs of retired ordinals
  extraEnv: { RUST_LOG: info }
status:
  phase: Healthy              # Reconciling|ScalingUp|ScalingDown|Blocked|Healthy
  message: at target 3
  desiredNodes: 3
  readyNodes: 3
  underreplicatedPartitions: 0
  opsPerSecond: "1234.5"
  lastScaleEpoch: 1751500000  # gates the autoscaler's time windows
  lastOpsTotal: 987654        # rate-sample state (survives operator restarts)
  lastSampleEpoch: 1751500030
```

The CRD YAML is generated from the Rust types (`marekvs-operator crd`,
`just operator-crd`) — the schema can never drift from the controller.

## Reconcile loop

Runs on every CR/StatefulSet change and every 30 s:

1. **Apply children** (server-side apply, field manager
   `marekvs-operator`): headless + client Services, PDB
   (`minAvailable = replicationFactor`), StatefulSet. All children carry
   owner references — deleting the CR garbage-collects everything except
   PVCs (Kubernetes semantics; data outlives the API object on purpose).
2. **Observe**: StatefulSet replicas/readyReplicas, plus a metrics scrape
   of every pod's `:9121/metrics` (plain HTTP against pod IPs — the same
   endpoint the probes use). Collected: sum of `marekvs_commands_total`
   (cluster ops counter) and the **worst** per-node
   `marekvs_cluster_underreplicated_partitions` (each node computes its own
   view; taking the max is the conservative merge).
3. **Decide the target**: `spec.nodes` (fixed) or the autoscaler (below).
   Either way the floor is `replicationFactor + 1`.
4. **Step toward the target** — the safety stepper
   (`scale.rs::next_replicas`, unit-tested):
   - target > current → jump straight there (adding nodes never removes
     data copies; `podManagementPolicy: Parallel` boots them concurrently);
   - target < current → **one** ordinal per reconcile, and only if
     `underreplicated == 0` **and** every pod is ready **and** metrics were
     actually scraped (no blind scale-downs). Otherwise the CR reports
     `phase: Blocked` with the reason, and the next reconcile retries.
5. **Reclaim PVCs** (only when `reclaimPvcs: true`): once the cluster is at
   target, fully ready, and fully replicated, PVCs of ordinals ≥ replicas
   are deleted. Defaults to keeping them: a later scale-up then reuses the
   volume and the returning node resumes from its own data.
6. **Publish status** and requeue.

## Autoscaling

Signal: cluster-wide command rate, computed operator-side from two
consecutive `marekvs_commands_total` samples. The sample state
(`lastOpsTotal`, `lastSampleEpoch`) lives in the CR **status**, so an
operator restart loses at most one interval and never mis-computes a rate;
counter resets (pod restarts make the sum go down) discard the sample and
re-baseline.

Decision (`scale.rs::autoscale_target`, unit-tested):

```
ideal = ceil(total_ops_per_sec / targetOpsPerNode)
floor = max(minNodes, replicationFactor + 1);  ceil = maxNodes
```

- **Up**: whenever `ideal > current` and `scaleUpCooldownSeconds` has
  passed since the last scale — undersized clusters hurt immediately, so
  the jump goes straight to `ideal` (the stepper allows it).
- **Down**: at most one node per `scaleDownStabilizationSeconds` window,
  and only when the load would still be **< 60 % of target after the
  removal** — the hysteresis band that prevents flapping around the
  threshold. The safety stepper then still gates the actual removal on full
  replication.
- **No signal** (bootstrap, scrape failure): hold position. Autoscaling
  degrades to "do nothing", never to "guess".

Worked example: `targetOpsPerNode: 5000`, 3 nodes, load rises to 22 k
ops/s → ideal = 5, cluster scales 3→5 in one step. Load falls to 6 k →
ideal = 2, floor = 3; 6000/4 = 1500 < 3000 (60 % band) → 5→4 after the
stabilization window, then 4→3 after another window, each step waiting for
`underreplicated == 0`.

## Failure modes, considered

| Failure | Behavior |
|---|---|
| Metrics unreachable | scale-downs blocked (`Blocked: metrics unreachable`); scale-ups and fixed-size reconciliation unaffected |
| Operator down | nothing scales; the data plane is untouched (it never depends on the operator) |
| Operator restarts mid-scale-down | state is in the CR status + cluster, not in memory; it re-observes and continues |
| Pod restart during sampling | counter sum drops → sample discarded, re-baselined |
| CR deleted | children GC'd via owner refs; PVCs survive (recreate the CR with the same name to readopt the data) |
| Two operators running | server-side apply with one field manager keeps them from fighting; still run 1 replica (no leader election yet) |

## RBAC

Namespaced children + cluster-scoped watch: `marekvsclusters(/status)`
patch/watch, `statefulsets`/`services`/`poddisruptionbudgets`
create/patch/watch, `pods` read (metrics scrape needs pod IPs), `pvcs`
read/delete (reclaim). No secrets, no exec, no node access.

## Future work

- Leader election → HA operator deployment.
- Disk-fill as a second autoscale signal (needs a data-size gauge in the
  server metrics first).
- Version rollouts gated on cluster health (today: plain StatefulSet
  rolling update when `spec.image` changes — already drain-safe via
  preStop, but not health-gated between pods).
- Zone-aware placement hints once HRW scoring learns topology (design/09).
- `kubectl scale` subresource support (scale CRs with the standard verb).

# marekvs on Kubernetes — example deployment

Two ways to run marekvs:

1. **Plain manifests** (this directory) — a 3-node cluster you scale by
   hand, following the runbook below. Zero moving parts beyond Kubernetes.
2. **The operator** ([`operator/`](operator/)) — a `MarekvsCluster` CRD
   whose controller automates that runbook and adds **autoscaling**. See
   [the operator section](#the-operator-automated--autoscaled) below and
   [design/12-operator.md](../design/12-operator.md).

The plain example: a minimal, production-shaped deployment with replication
factor 2, per-pod persistent volumes, graceful drain, and a disruption
budget. Full rationale lives in
[design/07-kubernetes.md](../design/07-kubernetes.md).

```sh
just k8s-apply        # kubectl apply -k k8s
just k8s-status       # pods, PVCs, replication health
just k8s-scale n=5    # scale (read "Scaling" below first!)
just k8s-delete       # remove the deployment, keep the data
```

The image reference in `statefulset.yaml` points at
`ghcr.io/yannick/marekvs:latest`, which CI publishes on every push to main
(`.github/workflows/docker.yml`, linux/amd64 + linux/arm64; also
`sha-<commit>` and `vX.Y.Z` tags — pin one of those for production, and
`:debug` for the chaos-harness image).

For **Flux image automation**, every main push also gets a sortable
`main-<sha7>-<unix-timestamp>` tag (`debug-main-…` for the debug image).
Pair it with:

```yaml
apiVersion: image.toolkit.fluxcd.io/v1beta2
kind: ImagePolicy
metadata:
  name: marekvs
spec:
  imageRepositoryRef:
    name: marekvs        # ImageRepository for ghcr.io/yannick/marekvs
  filterTags:
    pattern: '^main-[a-f0-9]+-(?P<ts>[0-9]+)$'
    extract: '$ts'
  policy:
    numerical:
      order: asc
```

## What's in the box

| File | Contents |
|---|---|
| `statefulset.yaml` | 3 replicas, PVC per pod, probes, preStop drain, spread constraints |
| `services.yaml` | `marekvs` (client entry, :6379) + `marekvs-headless` (gossip discovery) |
| `pdb.yaml` | PodDisruptionBudget: never fewer than 2 pods by voluntary eviction |
| `kustomization.yaml` | `kubectl apply -k k8s` glue |

Key mechanics, all implemented in the server:

- **Identity**: `NodeId` = pod ordinal, parsed from the hostname
  (`marekvs-3` → 3). No per-pod configuration.
- **Discovery**: one DNS seed (`marekvs-headless...:7946`) covers all pods —
  the headless service has `publishNotReadyAddresses: true` so even
  still-joining pods resolve.
- **Data**: each pod persists to its PVC via ondadb. A restarted or
  rescheduled pod resumes from its own data (cursor resume) instead of
  re-replicating from scratch.
- **Replication factor**: `MAREKVS_REPLICAS_N=2` — every partition lives on
  2 nodes; writes replicate to the peer without waiting for confirmation
  (AP design).

## Connecting

Any node serves any key, so clients just use the service:

```sh
kubectl run -it --rm redis-cli --image=redis:alpine --restart=Never -- \
    redis-cli -h marekvs
```

## Routing clients to the nearest node

Because any marekvs node serves any key, *which* pod a client lands on never
affects correctness — only latency. Better: it compounds. The node a client
talks to read-through-caches every key that client touches and subscribes to
its updates (interest leases), so consistently routing a client to the same
nearby node builds a working set exactly where it is used.

The example's client service ships with:

```yaml
trafficDistribution: PreferClose        # same zone first (k8s ≥ 1.31)
```

Three tiers, pick per cluster version and appetite:

| Setting | Semantics | Needs |
|---|---|---|
| `trafficDistribution: PreferClose` | same-zone endpoints first, falls back anywhere | k8s ≥ 1.31 (GA 1.33) |
| `trafficDistribution: PreferSameNode` | same-node first, then closest, falls back anywhere | k8s ≥ 1.34 (beta, on by default) |
| `marekvs-local` service (`internalTrafficPolicy: Local`) | same node **only** — connection refused if the node runs no marekvs pod | any k8s; every client node must run marekvs |

On k8s ≥ 1.34, flip the value in `services.yaml` to `PreferSameNode` — that
is the "client pods use the marekvs on their own node when there is one"
behavior with a safe fallback. The strict `marekvs-local` service exists for
setups where marekvs runs on every node clients run on (replicas ≈ node
count); its failure mode is a refused connection, not a slower one, so
treat it as an opt-in for latency-critical clients.

Two caveats:

- Routing is decided **per TCP connection**. Redis clients pool and hold
  connections, so a pod gets its locality at connect time and keeps it —
  restarts/reconnects re-resolve to the then-closest pod.
- Read-your-writes holds per connection. A client alternating between two
  connections to *different* nodes can briefly read stale values — same AP
  semantics as always, locality routing doesn't change it.

## Scaling — the rules that keep your data

Two invariants do all the work:

1. **Every partition is on `REPLICAS_N` (=2) nodes.** Removing one node
   never removes the last copy — the survivor re-replicates to a new peer
   (Merkle anti-entropy), restoring the replication factor automatically.
2. **PVCs outlive pods.** Scaling down does *not* delete the departed pod's
   volume. Scaling back up reattaches it, and the returning node resumes
   from its own data.

Which yields three operational rules:

- **Scale by 1 at a time.** Two simultaneous removals could take both
  replicas of a partition offline before re-replication finishes.
- **Never scale below `REPLICAS_N + 1` pods** (here: 3). At exactly
  `REPLICAS_N` nodes there is no spare node to re-replicate onto.
- **Wait until the cluster is healthy between steps** — see the check below.

### The health check

The gauge to watch is `marekvs_cluster_underreplicated_partitions` on any
node's `:9121/metrics`. **0 means every partition has `REPLICAS_N` live
copies** and the next scaling step is safe:

```sh
kubectl port-forward marekvs-0 9121 &
curl -s localhost:9121/metrics | grep marekvs_cluster_
# marekvs_cluster_members 3
# marekvs_cluster_effective_rf_min 2
# marekvs_cluster_underreplicated_partitions 0   ← safe to proceed
```

(`just k8s-status` runs this check for you.)

### Scale up

```sh
kubectl scale statefulset marekvs --replicas=5
```

New ordinals boot in the Joining phase, discover the cluster via gossip,
bootstrap every partition they will own (the join gate — `/ready` stays 503
and the pod is invisible to placement until the pull completes; progress is
visible as `marekvs_join_gate_pending_pids` on `/metrics`), and only then
flip to Active. HRW placement shifts a proportional slice of the partitions
(~`4096/n` each) onto the newcomers, sourced evenly from the existing
nodes; anti-entropy heals any writes that raced the join. Nothing needs to
be quiesced — writes continue throughout, and the readiness probe keeps a
joining pod out of the client service until it can serve.

Going up by more than one at a time is safe (adding nodes never removes
copies); `podManagementPolicy: Parallel` lets them all join concurrently.

### Scale down

```sh
# 1. check health first
curl -s localhost:9121/metrics | grep underreplicated   # must be 0

# 2. remove ONE node (StatefulSet removes the highest ordinal)
kubectl scale statefulset marekvs --replicas=4

# 3. wait for underreplicated_partitions to return to 0, then repeat.
```

What happens to the departing pod, in order:

1. **preStop hook** hits `/drain` → the node enters the Leaving phase and
   gossips it. Peers stop counting it as a placement target; it *keeps
   serving reads and writes* while it drains (readiness stays true by
   design).
2. **SIGTERM** → the node flushes its last replication-ring entries to its
   peers and exits.
3. The partitions it owned are still on their other replica
   (`REPLICAS_N=2`). Anti-entropy re-replicates them to a new peer, and
   `underreplicated_partitions` falls back to 0 — typically within seconds.

The PVC of the removed ordinal stays. If you later scale back up, the
returning ordinal reuses it and catches up incrementally. Only delete PVCs
when you are *sure* the ordinal won't return **and** the cluster reports 0
underreplicated partitions:

```sh
kubectl delete pvc data-marekvs-4   # after scaling 5 → 4, once healthy
```

### Involuntary disruptions

The same machinery covers crashes and node failures — gossip marks the node
dead, anti-entropy restores the replication factor, and Kubernetes
reschedules the pod with its PVC where possible. The PDB (`minAvailable: 2`)
prevents *voluntary* evictions (node drains, upgrades) from ever taking out
both copies of a partition at once; combined with the
`topologySpreadConstraints`, the two replicas of a partition generally sit
on different hosts and zones to begin with.

### Rolling updates

`kubectl rollout restart statefulset marekvs` updates one pod at a time.
Each pod drains (preStop → Leaving), restarts with its PVC, and rejoins via
the fast path — no bulk data movement. The readiness probe gates the next
pod, so the update naturally respects the health rule.

## The operator: automated + autoscaled

Everything under "Scaling" above is a runbook — the operator in
[`operator/`](operator/) executes it for you and adds load-based sizing:

```sh
just operator-apply                              # CRD + RBAC + controller
kubectl apply -f k8s/operator/example-cluster.yaml
kubectl get mkv                                  # watch it work
# NAME   DESIRED  READY  UNDERREP  OPS/S    PHASE
# demo   3        3      0         4231.0   Healthy
```

A `MarekvsCluster` resource replaces the hand-written StatefulSet, Services
and PDB (the controller generates them — same shape as this directory's
manifests). With `spec.autoscale` set, the controller sizes the cluster to
`ceil(total_ops / targetOpsPerNode)` within `[minNodes, maxNodes]`:

- **scale-up** happens immediately (one cooldown between steps) and jumps
  straight to the needed size — adding nodes never endangers data;
- **scale-down** removes one node per stabilization window, with hysteresis
  (only when there is real headroom), and each removal passes the same
  gate you would check by hand: all pods ready and
  `underreplicated_partitions == 0`. When the gate fails, the CR reports
  `phase: Blocked` with the reason instead of proceeding.

PVCs of scaled-away nodes are kept by default (scale-up resumes from them);
set `reclaimPvcs: true` to have the controller delete them once the cluster
is provably whole again.

Without `spec.autoscale`, the operator manages a fixed `spec.nodes` — you
still get the safe stepping, PVC handling, and status reporting.

## Caveats

- This is an **AP** store: during a scale event (as at any other time), two
  clients on two nodes can briefly observe different values. Convergence is
  bounded by the anti-entropy round, not by the scaling operation.
- `REPLICAS_N` is a cluster-wide setting; raising it later requires a
  rolling restart with the new value and one full anti-entropy cycle to
  populate the additional copies.
- Sizing: `storage` in the volume claim and the memory request are
  placeholders — set them from your working set. Prefer local NVMe storage
  classes; ondadb is disk-native and the storage latency is the write
  latency.

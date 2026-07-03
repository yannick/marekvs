# marekvs on Kubernetes — example deployment

A minimal, production-shaped deployment: a 3-node marekvs cluster with
replication factor 2, per-pod persistent volumes, graceful drain, and a
disruption budget. Full rationale lives in
[design/07-kubernetes.md](../design/07-kubernetes.md).

```sh
just k8s-apply        # kubectl apply -k k8s
just k8s-status       # pods, PVCs, replication health
just k8s-scale n=5    # scale (read "Scaling" below first!)
just k8s-delete       # remove the deployment, keep the data
```

The image reference in `statefulset.yaml` points at
`ghcr.io/yannick/marekvs:latest` — build your own with `just docker-build`
and retag, or adjust the reference.

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
and flip to Active. HRW placement shifts a proportional slice of the
partitions (~`4096/n` each) onto the newcomers, sourced evenly from the
existing nodes; anti-entropy fills them in the background. Nothing needs to
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

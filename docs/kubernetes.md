---
title: Kubernetes
description: Run marekvs as a StatefulSet — identity from the pod ordinal, headless-Service gossip seeding, graceful drain, and safe scaling.
status: implemented
---

marekvs is Kubernetes-native, but it leans on the apiserver **only for
bootstrapping** — DNS-based seed discovery and pod identity. The data plane
(gossip membership, HRW placement, anti-entropy replication) runs on its own and
survives apiserver outages. The manifests in [`k8s/`](https://github.com/yannick/marekvs/tree/main/k8s)
are a minimal, production-shaped 3-node deployment you scale by hand; the
[operator](../operator/) automates the same runbook and adds autoscaling.

Every pod exposes the same four ports:

| Port | Name | Purpose |
|---|---|---|
| `6379` | `resp` | Redis client protocol (RESP2/RESP3) |
| `7373` | `mesh` | Peer replication mesh |
| `7946/udp` | `gossip` | chitchat gossip |
| `9121` | `metrics` | Health probes + Prometheus metrics |

## The StatefulSet

A StatefulSet, not a Deployment — marekvs needs stable identity and stable
storage:

- **Identity from the ordinal.** `NodeId` is parsed from the pod hostname
  (`marekvs-3` → `3`); no `MAREKVS_NODE_ID` is set in the manifest. Reusing the
  ordinal on restart means a returning pod resumes from its own cursor instead
  of re-replicating.
- **A PVC per pod.** ondaDB data survives restarts and reschedules — a returning
  pod is a cheap resume, not a bootstrap.
- **`podManagementPolicy: Parallel`.** Nodes join and leave concurrently; the
  Joining/Leaving state machine handles ordering, not Kubernetes.

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: marekvs
  labels: { app: marekvs }
spec:
  serviceName: marekvs-headless
  replicas: 3                     # never below MAREKVS_REPLICAS_N + 1
  podManagementPolicy: Parallel   # join/leave ordering is gossip's job
  selector:
    matchLabels: { app: marekvs }
  template:
    metadata:
      labels: { app: marekvs }
    spec:
      terminationGracePeriodSeconds: 60
      securityContext:
        runAsNonRoot: true
        runAsUser: 65534
        runAsGroup: 65534
        fsGroup: 65534            # image is FROM scratch; only /data is writable
      containers:
        - name: marekvs
          image: ghcr.io/yannick/marekvs:latest
          ports:
            - { containerPort: 6379, name: resp }
            - { containerPort: 7373, name: mesh }
            - { containerPort: 7946, name: gossip, protocol: UDP }
            - { containerPort: 9121, name: metrics }
          env:
            # NodeId comes from the pod hostname ordinal (marekvs-3 → 3);
            # no MAREKVS_NODE_ID needed in a StatefulSet.
            - name: NAMESPACE
              valueFrom:
                fieldRef: { fieldPath: metadata.namespace }
            # chitchat re-resolves this DNS name, so one seed entry covers
            # every pod behind the headless service.
            - name: MAREKVS_SEEDS
              value: "marekvs-headless.$(NAMESPACE).svc.cluster.local:7946"
            - name: MAREKVS_ADVERTISE_IP
              value: "auto"        # self-detects the pod IP
            - name: MAREKVS_REPLICAS_N
              value: "2"           # every partition on 2 nodes
            - name: MAREKVS_DATA_DIR
              value: /data
            - name: RUST_LOG
              value: info
          volumeMounts:
            - { name: data, mountPath: /data }
          resources:
            requests: { cpu: "1", memory: 1Gi }
            limits:   { memory: 1Gi }   # request = limit: no burst-OOM; no CPU limit
          lifecycle:
            preStop:
              httpGet: { port: metrics, path: /drain }   # → Leaving before SIGTERM
          startupProbe:
            httpGet: { port: metrics, path: /ready }
            failureThreshold: 180   # up to 6 min for long bootstraps
            periodSeconds: 2
          readinessProbe:
            httpGet: { port: metrics, path: /ready }
            periodSeconds: 2
          livenessProbe:
            httpGet: { port: metrics, path: /alive }
            initialDelaySeconds: 10
            periodSeconds: 5
  volumeClaimTemplates:
    - metadata: { name: data, labels: { app: marekvs } }
      spec:
        accessModes: [ReadWriteOnce]
        resources: { requests: { storage: 10Gi } }
        # storageClassName: local-nvme  # prefer local PVs for performance
```

The `MAREKVS_ADVERTISE_IP: auto` value self-detects the pod IP, and one DNS seed
covers the whole cluster because chitchat re-resolves `marekvs-headless...`
internally. The `10Gi` storage request and `1Gi` memory request are placeholders
— size them from your working set, and prefer a local-NVMe storage class, since
ondaDB is disk-native and storage latency *is* write latency.

```note
The manifest sets `terminationGracePeriodSeconds: 60`. Design note
[07-kubernetes.md](https://github.com/yannick/marekvs/blob/main/design/07-kubernetes.md)
quotes 300 to "match the handoff budget"; the shipped value is 60. On SIGTERM the
node enters Leaving, waits ~2 s for the phase to gossip out, then flushes its
replication ring with a hard 7 s cap — well inside 60 s. Raise the grace period
only if you run peers slow enough that the ring backlog does not clear in time.
```

## Services

Two Services, plus an optional strict node-local variant.

```yaml
# Client entry — one stable name; any node serves any key.
apiVersion: v1
kind: Service
metadata:
  name: marekvs
  labels: { app: marekvs }
spec:
  selector: { app: marekvs }
  ports: [{ port: 6379, targetPort: resp, name: resp }]
  trafficDistribution: PreferClose   # same-zone endpoints first (k8s ≥ 1.31)
---
# Headless — gossip seed discovery + StatefulSet identity.
apiVersion: v1
kind: Service
metadata:
  name: marekvs-headless
  labels: { app: marekvs }
spec:
  clusterIP: None
  publishNotReadyAddresses: true     # CRITICAL: joining pods must resolve seeds
  selector: { app: marekvs }
  ports:
    - { port: 7946, name: gossip, protocol: UDP }
    - { port: 7373, name: mesh }
    - { port: 9121, name: metrics }
```

`publishNotReadyAddresses: true` is load-bearing: a Joining pod is unready *by
design*, but it must still discover peers through the headless name to
bootstrap. Without it, a fresh pod could never resolve a seed and would never
join.

### Routing clients to the nearest node

Because any marekvs node serves any key, *which* pod a client lands on never
affects correctness — only latency, and it compounds: the node a client talks to
read-through-caches every key that client touches and subscribes to its updates
(interest leases), so consistently routing a client to the same nearby node
builds a working set exactly where it is used. The client Service ships with
`trafficDistribution: PreferClose`. Three tiers, pick per cluster version and
appetite:

| Setting | Semantics | Needs |
|---|---|---|
| `trafficDistribution: PreferClose` | same-zone endpoints first, falls back anywhere | k8s ≥ 1.31 (GA 1.33) |
| `trafficDistribution: PreferSameNode` | same-node first, then closest, falls back anywhere | k8s ≥ 1.34 (beta, on by default) |
| `marekvs-local` Service (`internalTrafficPolicy: Local`) | same node **only** — connection refused if the node runs no marekvs pod | any k8s; every client node must run marekvs |

```caution
The `marekvs-local` Service (`internalTrafficPolicy: Local`) has **no fallback**:
if the client's node runs no marekvs pod, the connection is *refused*, not merely
slower. Use it only where marekvs runs on every node its clients run on (replicas
≈ node count). Routing is decided per TCP connection, and read-your-writes holds
per connection — a client alternating between connections to different nodes can
briefly read stale values. That is ordinary AP semantics; locality routing does
not change it.
```

## PodDisruptionBudget

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: marekvs
  labels: { app: marekvs }
spec:
  minAvailable: 3                    # ≥ replication floor; blocks eviction below it
  selector:
    matchLabels: { app: marekvs }
```

The PDB blocks *voluntary* evictions (node drains, cluster upgrades) from taking
the cluster below its safe floor. With `MAREKVS_REPLICAS_N=2`, `minAvailable: 3`
keeps a spare node available to re-replicate onto during any single voluntary
disruption. Paired with the `topologySpreadConstraints` on
`kubernetes.io/hostname` and `topology.kubernetes.io/zone` (both `maxSkew: 1`,
`whenUnsatisfiable: ScheduleAnyway`), the two replicas of a partition generally
sit on different hosts and zones to begin with.

```note
The example manifest ships `minAvailable: 2` (= `REPLICAS_N`, the minimum that
keeps every partition replicated). Design note 07 recommends `minAvailable: 3`
(= `REPLICAS_N + 1`) to guarantee a re-replication target during a voluntary
disruption. Both are safe; 3 is the more conservative floor for a 3-node cluster
that you never want dropping to exactly `REPLICAS_N` under a drain.
```

## Probe semantics

All probes are HTTP against the process itself on `:9121` — no shell is baked
into the scratch image. The endpoints are implemented in the server:

| Endpoint | Used as | Behavior |
|---|---|---|
| `/ready` | startup + readiness probe | 200 when phase ∈ {Active, Leaving}, else 503. A Leaving node stays ready to serve during drain; it flips to 503 at final exit. As a startup probe it bounds bootstrap time (`failureThreshold: 180` × 2 s = up to 6 min). |
| `/alive` | liveness probe | 200 while the process and shard threads are responsive and ondaDB is not erroring — a stuck shard fails liveness and the pod restarts. |
| `/drain` | preStop hook | Sets the node to Leaving and gossips it: peers stop counting it as a placement target while it keeps serving. Returns once the phase is set; the pod then drains and exits within the grace period. |
| `/metrics` | Prometheus scrape | Prometheus text format: per-command counters and latency histograms, RESP + mesh throughput, connection stats, replication-ring and cluster gauges. |

Readiness intentionally stays true through the Leaving phase, so a draining node
keeps serving reads and writes until its final exit rather than being pulled from
the client Service the instant it starts to leave.

## Scaling

Two invariants keep your data safe across any scale event:

1. **Every partition is on `REPLICAS_N` nodes.** Removing one node never removes
   the last copy — the survivor re-replicates to a new peer via Merkle
   anti-entropy, restoring the replication factor automatically.
2. **PVCs outlive pods.** Scaling down does not delete the departed ordinal's
   volume; scaling back up reattaches it and the returning node resumes from its
   own data.

Which yields three operational rules:

- **Scale by one at a time.** Two simultaneous removals could take both replicas
  of a partition offline before re-replication finishes.
- **Never scale below `REPLICAS_N + 1` nodes.** At exactly `REPLICAS_N` there is
  no spare node to re-replicate onto.
- **Wait until healthy between steps** — the check below.

### The gauge to watch

Before every scale-down step, watch
`marekvs_cluster_underreplicated_partitions` on any node's `:9121/metrics`.
**`0` means every partition has `REPLICAS_N` live copies** and the next step is
safe:

```sh
kubectl port-forward marekvs-0 9121 &
curl -s localhost:9121/metrics | grep marekvs_cluster_
# marekvs_cluster_members 3
# marekvs_cluster_effective_rf_min 2
# marekvs_cluster_underreplicated_partitions 0   ← safe to proceed
```

```warning
Never begin a scale-down step while `marekvs_cluster_underreplicated_partitions`
is above 0. Doing so can remove the last live copy of a partition before
anti-entropy has restored the replication factor — an AP store cannot get that
data back. `just k8s-status` runs this check for you.
```

### Scale up

```sh
kubectl scale statefulset marekvs --replicas=5
```

New ordinals boot in the Joining phase, discover the cluster via gossip, and
flip to Active. HRW placement shifts a proportional slice of partitions
(~`4096/n` each) onto the newcomers, sourced evenly from the existing nodes;
anti-entropy fills them in the background. Nothing is quiesced — writes continue
throughout, and the readiness probe keeps a joining pod out of the client Service
until it can serve. Going up by more than one at a time is safe (adding nodes
never removes copies); `podManagementPolicy: Parallel` lets them all join at
once.

### Scale down

```sh
# 1. check health first
curl -s localhost:9121/metrics | grep underreplicated   # must be 0

# 2. remove ONE node (StatefulSet drops the highest ordinal)
kubectl scale statefulset marekvs --replicas=4

# 3. wait for underreplicated_partitions to return to 0, then repeat.
```

The departing pod's `preStop` hook hits `/drain`, it enters Leaving and keeps
serving while it drains, then SIGTERM flushes its last replication-ring entries
and it exits. The partitions it owned are still on their other replica;
anti-entropy re-replicates them to a new peer and the gauge falls back to 0,
typically within seconds. The PVC of the removed ordinal stays — delete it only
once you are sure the ordinal will not return **and** the cluster reports 0
underreplicated partitions.

### Involuntary disruptions and rolling updates

The same machinery covers crashes: gossip marks the node dead, anti-entropy
restores the replication factor, and Kubernetes reschedules the pod with its PVC
where possible. `kubectl rollout restart statefulset marekvs` updates one pod at
a time — each drains via `preStop`, restarts with its PVC, and rejoins the fast
path (no bulk data movement); the readiness probe gates the next pod, so a
rolling update naturally respects the health rule.

## Observability

- **Prometheus** on `:9121/metrics`: the `marekvs_cluster_*` gauges
  (`members`, `effective_rf_min`, `underreplicated_partitions`), a per-partition
  staleness gauge (worst-case age of the last anti-entropy round), per-peer
  cursor lag, interest-table size, fetch/check rates, and per-command RESP
  latencies.
- **`INFO replication`** mirrors the cluster gauges for `redis-cli` users
  (`cluster_members`, `underreplicated_partitions`, `effective_rf_min`).
- **Structured logs** (tracing → JSON) to stdout; the level is
  live-reconfigurable via `CONFIG SET loglevel`.

## Where to go next

- Automate all of the above: the [operator](../operator/) turns this runbook
  into a controller with ops/s-based autoscaling.
- Build the image and see every environment variable: [Build & deploy](../build-deploy/#configuration).
- Understand the guarantees the scaling rules protect: [Consistency](../consistency/).

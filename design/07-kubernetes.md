# 07 — Kubernetes Deployment

marekvs is Kubernetes-native but depends on the apiserver **only for
bootstrapping** (DNS seeding, identity). The data plane — membership,
placement, replication — runs on gossip and survives apiserver outages.

## Workload: StatefulSet

StatefulSet, not Deployment:

- **Stable identity**: `NodeId = pod ordinal` (parsed from hostname
  `marekvs-3` → 3). Ordinals are reused on restart → cursor-resume instead of
  re-replication.
- **PVC per pod**: ondaDB data survives restarts and reschedules; a returning
  pod is a cheap `ResumeFrom`, not a bootstrap.
- `podManagementPolicy: Parallel` — nodes join/leave concurrently; the
  Joining/Leaving state machine handles ordering, not Kubernetes.

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata: { name: marekvs }
spec:
  serviceName: marekvs-headless
  replicas: 5                    # ≥ min_nodes (3)
  podManagementPolicy: Parallel
  template:
    spec:
      terminationGracePeriodSeconds: 300     # matches handoff budget (05)
      containers:
      - name: marekvs
        image: ghcr.io/…/marekvs:<tag>       # FROM scratch, see 08
        ports:
        - {containerPort: 6379, name: resp}
        - {containerPort: 7373, name: mesh}
        - {containerPort: 7946, name: gossip, protocol: UDP}
        - {containerPort: 9121, name: metrics}
        env:
        # v1 implemented names: MAREKVS_SEEDS takes host:port gossip seeds
        # (chitchat re-resolves DNS internally); ADVERTISE_IP=auto self-detects
        # the pod IP (or use the downward API: status.podIP).
        - {name: MAREKVS_SEEDS, value: "marekvs-headless.$(NAMESPACE).svc:7946"}
        - {name: MAREKVS_ADVERTISE_IP, value: "auto"}
        - {name: MAREKVS_REPLICAS_N, value: "3"}
        volumeMounts:
        - {name: data, mountPath: /data}
        # The :9121 endpoints are implemented: /ready (Active|Leaving),
        # /alive (shard-thread responsiveness), /drain (-> Leaving),
        # /metrics (Prometheus text format with per-command counters and
        # latency histograms, RESP + mesh throughput, connection stats,
        # replication ring and cluster gauges).
        lifecycle:
          preStop:
            httpGet: {port: 9121, path: /drain}   # → Leaving + handoff (06)
        readinessProbe:
          httpGet: {port: 9121, path: /ready}     # true iff state == Active|Leaving
          periodSeconds: 2
        livenessProbe:
          httpGet: {port: 9121, path: /alive}     # process + shard threads healthy
          initialDelaySeconds: 10
          periodSeconds: 5
        startupProbe:
          httpGet: {port: 9121, path: /ready}     # allows long bootstraps
          failureThreshold: 180                   # up to 6 min joining
          periodSeconds: 2
  volumeClaimTemplates:
  - metadata: { name: data }
    spec:
      accessModes: [ReadWriteOnce]
      resources: { requests: { storage: 100Gi } }
      # storageClassName: local-nvme (performance: prefer local PVs)
```

## Services

```yaml
# Client entry — one stable endpoint, any node serves any key
apiVersion: v1
kind: Service
metadata: { name: marekvs }
spec:
  selector: { app: marekvs }
  ports: [{ port: 6379, targetPort: resp }]
  # sessionAffinity: ClientIP — optional; TCP connections are already sticky,
  # affinity only helps clients that reconnect often (read-your-writes UX)
---
# Headless — gossip seed discovery + StatefulSet identity
apiVersion: v1
kind: Service
metadata: { name: marekvs-headless }
spec:
  clusterIP: None
  publishNotReadyAddresses: true    # CRITICAL: Joining pods must resolve seeds
  selector: { app: marekvs }
  ports: [{ port: 7946, name: gossip }]
```

`publishNotReadyAddresses: true` matters: a Joining pod is unready by design
but must still discover peers through DNS.

## Disruption & scheduling

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata: { name: marekvs }
spec:
  minAvailable: 3            # = min_nodes = N; blocks voluntary eviction below floor
  selector: { matchLabels: { app: marekvs } }
```

- **topologySpreadConstraints** on `topology.kubernetes.io/zone` and
  `kubernetes.io/hostname` (`maxSkew: 1`, `whenUnsatisfiable: ScheduleAnyway`)
  spread replicas; HRW placement is topology-blind in v1 (zone-aware scoring
  is future work — noted in [09-performance.md](09-performance.md)).
- Resources: request what you mean (`memory` request = limit to avoid burst
  OOM; ondaDB block cache + interest table are the main consumers). CPU limit
  omitted (throttling hurts tail latency).
- `securityContext`: `runAsNonRoot`, read-only root FS (the image is scratch;
  only `/data` is writable).

## Probe semantics

| Probe | Path | True when |
|---|---|---|
| startup | /ready | node reached Active (bounds bootstrap time, not liveness) |
| readiness | /ready | state ∈ {Active, Leaving} — Leaving stays ready to serve during drain; flips false at final exit |
| liveness | /alive | event loops + shard threads responsive; ondaDB not erroring |
| preStop | /drain | triggers Leaving → handoff → blocks until Left or grace expiry |

## Scale operations

- **Scale up**: bump `replicas`; new ordinals boot Joining, bootstrap, flip
  Active. Placement shifts ~P/n partitions to each newcomer, sourced evenly.
- **Scale down**: scale by 1 at a time (StatefulSet removes the highest
  ordinal; preStop drains it). The PDB guards the floor.
- **Rolling update**: default `RollingUpdate` one pod at a time; each pod
  drains (Leaving → handoff) before restart, and re-joins via fast-path
  (PVC + cursor resume) — no bulk data movement in the common case.
- **Node failure**: kubelet/cloud reschedules the pod with its PVC where
  possible; gossip-level crash repair
  ([06](06-cluster-membership.md#crash)) covers the gap regardless of whether
  Kubernetes ever brings the pod back.

## Observability

- Prometheus metrics on :9121 (`/metrics`): staleness gauge (last AE round
  age per pid worst-case), `underreplicated_partitions`, `effective_rf_min`,
  ring occupancy, per-peer cursor lag, interest table size, fetch/check rates,
  RESP command latencies.
- `INFO replication` mirrors the cluster gauges for redis-cli users.
- Structured logs (tracing → JSON) to stdout.

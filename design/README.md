# marekvs — Design Documents

marekvs is a distributed key-value database server with a Redis-compatible API,
written in Rust, storing all data on disk via the [ondaDB](../../ondadb) LSM
engine. It is an **AP system**: eventually consistent, coordination-free,
Kubernetes-native, built for performance.

| Doc | Contents |
|---|---|
| [00-overview.md](00-overview.md) | Goals, non-goals, published guarantees, glossary, literature |
| [01-architecture.md](01-architecture.md) | Process anatomy, runtime/thread model, crate layout |
| [02-data-model.md](02-data-model.md) | Internal key layouts, record envelope, HLC, merge rules per type |
| [03-redis-api.md](03-redis-api.md) | Command coverage matrix, RESP2/RESP3, per-command caveats |
| [04-replication.md](04-replication.md) | Partitions, HRW placement, write/read paths, interest subscriptions |
| [05-consistency-anti-entropy.md](05-consistency-anti-entropy.md) | HLC rules, Merkle anti-entropy, staleness bound, tombstone GC, **defaults table** |
| [06-cluster-membership.md](06-cluster-membership.md) | Gossip, node state machine, join/leave/crash, bootstrap |
| [07-kubernetes.md](07-kubernetes.md) | StatefulSet, probes, drain, PDB, topology |
| [08-build-deploy.md](08-build-deploy.md) | Workspace layout, static musl build, FROM-scratch image |
| [09-performance.md](09-performance.md) | Targets, hot paths, ondadb tuning, benchmark plan |
| [10-testing.md](10-testing.md) | Merge-law property tests, Jepsen, chaos, churn tests |
| [11-lua-scripting.md](11-lua-scripting.md) | EVAL/EVALSHA: atomic same-pid scripts, distributed caveats + solutions |
| [12-operator.md](12-operator.md) | Kubernetes operator: MarekvsCluster CRD, safe scaling, ops/s autoscaling |
| [13-budget.md](13-budget.md) | `BG.*` distributed budgets: escrow protocol, never-overspend invariant, tokens |

Reading order for newcomers: 00 → 01 → 02 → 04 → 05. The single source of
truth for all tunables is the defaults table in
[05-consistency-anti-entropy.md](05-consistency-anti-entropy.md#defaults-table).

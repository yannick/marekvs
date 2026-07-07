# Redis Cluster Protocol Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let cluster-aware Redis clients (redis-rs/sccache, lettuce, go-redis) route commands directly to the owning marekvs node — CRC16 slot-compatible partitioning plus a read-only `CLUSTER` command family.

**Architecture:** Change `pid_of` from xxh3 to `crc16(key) % 16384 >> 2` (4 Redis slots per existing pid; all pid-keyed machinery unchanged). Gossip a new `resp_addr` KV so nodes know each other's client endpoints. Expose topology via a new `Engine::set_cluster_topology` hook (same trait-object pattern as `cluster_info`) consumed by a new `cmd/cluster.rs` family. No MOVED/CROSSSLOT — any node still serves any key (see design/15-cluster-protocol.md, the spec for this plan).

**Tech Stack:** Rust workspace. Crates touched: `marekvs-core`, `marekvs-cluster`, `marekvs-engine`, `marekvs-server`. No new dependencies.

**Worktree:** `/Volumes/HOME/code/storage-engines/marekvs-cluster-protocol` (branch `cluster-protocol`). All commands run from this directory.

**Breaking change note (context for the worker):** changing `pid_of` invalidates existing on-disk data placement. This is accepted and documented in design/15. Do not add a compatibility mode.

---

### Task 1: CRC16 + slot mapping in marekvs-core

**Files:**
- Modify: `crates/marekvs-core/src/lib.rs` (pid_of at lines 23-45, tests at 47-60)
- Modify: `crates/marekvs-core/src/ikey.rs:19` (doc constant note only if needed — PARTITIONS stays 4096)

- [ ] **Step 1: Write the failing tests**

Append to the `hash_tag_tests` module in `crates/marekvs-core/src/lib.rs` (rename module to `partition_tests` in the same edit):

```rust
    #[test]
    fn crc16_xmodem_vector() {
        assert_eq!(crc16(b"123456789"), 0x31C3);
        assert_eq!(crc16(b""), 0);
    }

    #[test]
    fn redis_known_slots() {
        // Vectors from real redis-server CLUSTER KEYSLOT.
        assert_eq!(slot_of(b"foo"), 12182);
        assert_eq!(slot_of(b"bar"), 5061);
        assert_eq!(slot_of(b"hello"), 866);
        // Hash tag: slot of the tag content only.
        assert_eq!(slot_of(b"{user1000}.following"), slot_of(b"user1000"));
    }

    #[test]
    fn pid_is_slot_group() {
        for key in [b"foo".as_slice(), b"bar", b"hello", b"{tag}x"] {
            assert_eq!(pid_of(key), slot_of(key) >> 2);
        }
        assert!(u32::from(slot_of(b"anything")) < 16384);
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p marekvs-core partition -- --nocapture` (also try `cargo test -p marekvs-core 2>&1 | tail -20`)
Expected: compile error "cannot find function `crc16`" / "`slot_of`".

- [ ] **Step 3: Implement crc16 + slot_of, change pid_of**

In `crates/marekvs-core/src/lib.rs`, replace the `pid_of` block (lines 23-34) with:

```rust
/// Redis-Cluster CRC16 (XMODEM: poly 0x1021, init 0), table-driven.
/// Must stay bit-identical to redis `crc16.c` — cluster clients compute
/// slots with it on their side.
const CRC16_TAB: [u16; 256] = {
    let mut tab = [0u16; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = (i as u16) << 8;
        let mut j = 0;
        while j < 8 {
            crc = if crc & 0x8000 != 0 { (crc << 1) ^ 0x1021 } else { crc << 1 };
            j += 1;
        }
        tab[i] = crc;
        i += 1;
    }
    tab
};

pub fn crc16(data: &[u8]) -> u16 {
    let mut crc = 0u16;
    for &b in data {
        crc = (crc << 8) ^ CRC16_TAB[(((crc >> 8) ^ b as u16) & 0xFF) as usize];
    }
    crc
}

/// Redis Cluster slot of a user key: `crc16(hash_slice(key)) % 16384`.
/// Identical to redis so cluster-aware clients route to the right node
/// (design/15).
pub fn slot_of(userkey: &[u8]) -> u16 {
    crc16(hash_slice(userkey)) % 16384
}

/// Partition of a user key: its Redis Cluster slot group — 4 consecutive
/// slots per pid (16384 slots / 4096 partitions), so every pid is the
/// contiguous slot range `[pid*4, pid*4+3]` and `CLUSTER SLOTS` can report
/// exact ranges (design/15).
///
/// Redis Cluster hash tags: when the key contains `{...}` with non-empty
/// content, ONLY that content is hashed — `rate:{user1}:count` and
/// `rate:{user1}:window` land on the same partition (and therefore the
/// same shard thread), which is what makes multi-key atomic Lua scripts
/// and co-located MULTI possible (design/11). Rule matches Redis exactly:
/// first `{`, then the first `}` AFTER it; empty `{}` hashes the whole key.
pub fn pid_of(userkey: &[u8]) -> Pid {
    slot_of(userkey) >> 2
}
```

(Keep `hash_slice` exactly as is. `xxhash_rust` stays a dependency — it is still used by HRW `score()` in marekvs-cluster; do not touch Cargo.toml.)

- [ ] **Step 4: Run the full core test suite**

Run: `cargo test -p marekvs-core 2>&1 | tail -5`
Expected: all pass. If `redis_known_slots` fails, the CRC16 variant is wrong — verify against `redis-cli` if available (`redis-cli CLUSTER KEYSLOT foo` on any real redis) before touching the vectors; the vectors are canonical.

- [ ] **Step 5: Run the whole workspace suite (pid change ripples everywhere)**

Run: `cargo test --workspace 2>&1 | grep -E "test result|error\[" | sort | uniq -c`
Expected: all `ok`. Any failure means a test hardcoded an xxh3-derived pid — fix the test's expectation, not the mapping.

- [ ] **Step 6: Update design/02 partition derivation**

In `design/02-data-model.md:10` replace:
```text
pid: u16 = (xxh3_64(userkey) >> 52) as u16     // top 12 bits → 0..4095
```
with:
```text
slot: u16 = crc16(userkey) % 16384             // Redis Cluster slot (design/15)
pid:  u16 = slot >> 2                          // 4 slots per pid → 0..4095
```
Also update the doc comment in `crates/marekvs-core/src/ikey.rs` header if it mentions xxh3 (it doesn't — verify with grep).

- [ ] **Step 7: Commit**

```bash
git add crates/marekvs-core/src/lib.rs design/02-data-model.md
git commit -m "feat(core): CRC16 slot-compatible partitioning (design/15)

pid = redis_slot >> 2. Breaking on-disk change: keys placed under the
old xxh3 mapping are not found under CRC16; migration = REPLICAOF path."
```

---

### Task 2: Gossip `resp_addr` + generation in marekvs-cluster

**Files:**
- Modify: `crates/marekvs-cluster/src/lib.rs` (Member at 51-61, ClusterConfig at 163-172, spawn initial_kvs at 209-212, rebuild_view at 250-275)
- Modify: `crates/marekvs-server/src/main.rs` (advertise resolution at 110-113, ClusterConfig at 174-184)

No unit test here — `rebuild_view` consumes chitchat-internal types; coverage comes from Task 6's integration script. Mechanical wiring:

- [ ] **Step 1: Extend Member and ClusterConfig**

In `crates/marekvs-cluster/src/lib.rs`:

```rust
pub struct Member {
    pub node: NodeId,
    pub mesh_addr: SocketAddr,
    /// The peer's gossip endpoint — persisted by the server as a fallback
    /// seed so a restarted node can rejoin even when every configured seed
    /// address went stale (environments without stable IPs or DNS, e.g.
    /// Apple containers give every restart a fresh IP).
    pub gossip_addr: SocketAddr,
    /// The peer's client-facing RESP endpoint, for CLUSTER SLOTS/NODES
    /// (design/15). None while a mixed-version cluster still has members
    /// that don't gossip it — such members are omitted from topology
    /// replies, never guessed.
    pub resp_addr: Option<SocketAddr>,
    /// chitchat generation (boot timestamp) — the member's incarnation,
    /// reported as config-epoch in CLUSTER NODES/INFO.
    pub generation: u64,
    pub phase: NodePhase,
}
```

`ClusterConfig` gains `pub resp_advertise: SocketAddr,` after `mesh_advertise`.

In `spawn`, add to `initial_kvs`:
```rust
            ("resp_addr".to_string(), cfg.resp_advertise.to_string()),
```

In `rebuild_view`, populate the new fields:
```rust
            members.push(Member {
                node,
                mesh_addr: addr,
                gossip_addr: id.gossip_advertise_addr,
                resp_addr: state.get("resp_addr").and_then(|a| a.parse().ok()),
                generation: id.generation_id,
                phase,
            });
```

- [ ] **Step 2: Wire the server**

In `crates/marekvs-server/src/main.rs` after the `mesh_advertise` resolution (line 113):
```rust
    let resp_advertise = resolve(&advertise_ip, resp_addr.port()).await?;
```
and pass `resp_advertise,` in the `ClusterConfig { ... }` literal.

- [ ] **Step 3: Build + fix all Member construction sites**

Run: `cargo build --workspace 2>&1 | grep -E "^error" | head`
Expected: errors at any other `Member { ... }` literal (grep `Member {` across crates — chaos/test helpers may build views). Fix each with `resp_addr: None, generation: 0` unless a real address is available.

Run: `cargo test --workspace 2>&1 | grep -E "test result" | sort | uniq -c`
Expected: all ok.

- [ ] **Step 4: Commit**

```bash
git add -A crates/ && git commit -m "feat(cluster): gossip resp_addr + expose member generation"
```

---

### Task 3: Topology hook on the engine

**Files:**
- Create: `crates/marekvs-engine/src/topology.rs`
- Modify: `crates/marekvs-engine/src/lib.rs` (module decl; Engine field next to `cluster_info` at line 196; init at 271; setter next to `set_cluster_info` at 314)

- [ ] **Step 1: Create the types**

`crates/marekvs-engine/src/topology.rs`:

```rust
//! Cluster topology snapshot for the CLUSTER command family (design/15).
//! Provided by the server via [`crate::Engine::set_cluster_topology`] —
//! the same trait-object indirection as the `cluster_info` hook, so this
//! crate does not depend on marekvs-cluster.

use std::net::SocketAddr;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct TopologyNode {
    pub id: u16,
    /// Client-facing RESP endpoint; None for members that don't gossip one
    /// (mixed-version cluster) — omitted from topology replies.
    pub resp_addr: Option<SocketAddr>,
    /// Gossip port, reported as the cluster-bus port in CLUSTER NODES.
    pub gossip_port: u16,
    /// chitchat generation (boot incarnation) — config-epoch.
    pub generation: u64,
    /// Node phase string: "joining" | "active" | "leaving".
    pub state: String,
}

#[derive(Debug, Clone)]
pub struct Topology {
    pub self_id: u16,
    /// Monotonic view epoch (bumps on every membership change).
    pub epoch: u64,
    pub nodes: Vec<TopologyNode>,
    /// Per-pid owners, H1 (primary) first — PARTITIONS entries.
    pub pid_owners: Vec<Vec<u16>>,
}

pub type TopologyFn = Arc<dyn Fn() -> Topology + Send + Sync>;
```

- [ ] **Step 2: Wire into Engine**

In `crates/marekvs-engine/src/lib.rs`: add `pub mod topology;`, re-export (`pub use topology::{Topology, TopologyFn, TopologyNode};`), add field

```rust
    /// Topology provider for the CLUSTER command family (design/15),
    /// installed by the server; None in embedded/single-node use — the
    /// CLUSTER family then answers as a 1-node cluster or errors cleanly.
    pub cluster_topology: parking_lot::RwLock<Option<TopologyFn>>,
```

initializer `cluster_topology: parking_lot::RwLock::new(None),` and setter:

```rust
    pub fn set_cluster_topology(&self, f: TopologyFn) {
        *self.cluster_topology.write() = Some(f);
    }
```

- [ ] **Step 3: Build**

Run: `cargo build -p marekvs-engine 2>&1 | tail -3`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add crates/marekvs-engine && git commit -m "feat(engine): cluster topology provider hook"
```

---

### Task 4: `cmd/cluster.rs` — formatters first (TDD), then handlers

**Files:**
- Create: `crates/marekvs-engine/src/cmd/cluster.rs`
- Modify: `crates/marekvs-engine/src/cmd/mod.rs` (module decl at ~line 3-15; dispatch arm after `"DEBUG"` at line 69)

Design: pure functions take `&Topology` and return `Reply`/`String` — unit-testable without a Store. Handlers are thin wrappers reading the hook.

- [ ] **Step 1: Write the failing formatter tests**

Create `crates/marekvs-engine/src/cmd/cluster.rs` containing ONLY the test module first:

```rust
//! CLUSTER command family — read-only topology introspection (design/15).
//! Placement stays gossip+HRW (design/06); nothing here mutates state.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::{Topology, TopologyNode};

    fn topo() -> Topology {
        // 2 nodes, 4 pids (test-sized; PARTITIONS is not assumed by the
        // formatters), RF2. pid owners H1-first.
        Topology {
            self_id: 0,
            epoch: 7,
            nodes: vec![
                TopologyNode {
                    id: 0,
                    resp_addr: Some("10.0.0.1:6379".parse().unwrap()),
                    gossip_port: 7946,
                    generation: 111,
                    state: "active".into(),
                },
                TopologyNode {
                    id: 1,
                    resp_addr: Some("10.0.0.2:6379".parse().unwrap()),
                    gossip_port: 7946,
                    generation: 222,
                    state: "active".into(),
                },
            ],
            pid_owners: vec![vec![0, 1], vec![0, 1], vec![1, 0], vec![1, 0]],
        }
    }

    #[test]
    fn ranges_merge_adjacent_same_owners() {
        // pids 0,1 → slots 0..7 owner [0,1]; pids 2,3 → slots 8..15 [1,0].
        let r = slot_ranges(&topo().pid_owners);
        assert_eq!(
            r,
            vec![(0u16, 7u16, vec![0u16, 1]), (8, 15, vec![1, 0])]
        );
    }

    #[test]
    fn ranges_skip_ownerless_pids() {
        let r = slot_ranges(&[vec![0], vec![], vec![0]]);
        assert_eq!(r, vec![(0, 3, vec![0]), (8, 11, vec![0])]);
    }

    #[test]
    fn node_id_is_40_hex() {
        assert_eq!(node_hex_id(3).len(), 40);
        assert!(node_hex_id(3).ends_with('3'));
    }

    #[test]
    fn info_renders_redis_fields() {
        let s = info_text(&topo());
        assert!(s.contains("cluster_enabled:1\r\n"));
        assert!(s.contains("cluster_state:ok\r\n"));
        assert!(s.contains("cluster_slots_assigned:16384\r\n"));
        assert!(s.contains("cluster_known_nodes:2\r\n"));
        assert!(s.contains("cluster_current_epoch:7\r\n"));
        assert!(s.contains("cluster_my_epoch:111\r\n"));
    }

    #[test]
    fn nodes_lines_have_redis_shape() {
        let s = nodes_text(&topo());
        let lines: Vec<&str> = s.lines().collect();
        assert_eq!(lines.len(), 2);
        // <40hex> <ip:port@cport> <flags> <master> <ping> <pong> <epoch> <state> <slots...>
        let first: Vec<&str> = lines[0].split(' ').collect();
        assert_eq!(first[0].len(), 40);
        assert_eq!(first[1], "10.0.0.1:6379@7946");
        assert!(first[2].contains("master"));
        assert!(first[2].starts_with("myself,")); // self_id == 0
        assert_eq!(first[3], "-");
        assert_eq!(first[6], "111"); // config-epoch = generation
        assert_eq!(first[7], "connected");
        assert_eq!(first[8], "0-7"); // node 0 is H1 of slots 0..7
    }

    #[test]
    fn slots_reply_shape() {
        use crate::reply::Reply;
        let Reply::Array(entries) = slots_reply(&topo()) else {
            panic!("CLUSTER SLOTS must be an array");
        };
        assert_eq!(entries.len(), 2);
        let Reply::Array(first) = &entries[0] else { panic!() };
        assert_eq!(first[0], Reply::Int(0));
        assert_eq!(first[1], Reply::Int(7));
        let Reply::Array(master) = &first[2] else { panic!() };
        assert_eq!(master[0], Reply::Bulk(b"10.0.0.1".to_vec()));
        assert_eq!(master[1], Reply::Int(6379));
        // replica entry present (RF2)
        assert_eq!(first.len(), 4);
    }

    #[test]
    fn slots_skip_master_without_resp_addr() {
        let mut t = topo();
        t.nodes[0].resp_addr = None;
        let Reply::Array(entries) = slots_reply(&t) else { panic!() };
        // ranges mastered by node 0 are dropped; node-1 ranges survive
        assert_eq!(entries.len(), 1);
    }
}
```

(`use crate::reply::Reply;` at top level of the file as needed.)

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p marekvs-engine cluster:: 2>&1 | tail -5`
Expected: compile errors — `slot_ranges`, `node_hex_id`, `info_text`, `nodes_text`, `slots_reply` not found. (Add `pub mod cluster;` to `cmd/mod.rs` in this step so the module compiles.)

- [ ] **Step 3: Implement formatters + handlers**

Add above the test module in `cluster.rs`:

```rust
use std::sync::Arc;

use crate::reply::Reply;
use crate::topology::{Topology, TopologyNode};
use crate::{Engine, Session};

/// Slots per pid: 16384 Redis slots over PARTITIONS pids (design/15).
const SLOTS_PER_PID: u16 = 16384 / marekvs_core::PARTITIONS;

/// Stable 40-hex node id from the ordinal (Redis clients treat it as an
/// opaque identity; stability across restarts is the useful property).
fn node_hex_id(node: u16) -> String {
    format!("{node:040x}")
}

/// Merge per-pid owner lists into (start_slot, end_slot, owners) runs.
/// Ownerless pids (cluster still forming) yield no range.
fn slot_ranges(pid_owners: &[Vec<u16>]) -> Vec<(u16, u16, Vec<u16>)> {
    let mut out: Vec<(u16, u16, Vec<u16>)> = Vec::new();
    for (pid, owners) in pid_owners.iter().enumerate() {
        if owners.is_empty() {
            continue;
        }
        let start = pid as u16 * SLOTS_PER_PID;
        let end = start + SLOTS_PER_PID - 1;
        match out.last_mut() {
            Some(last) if last.2 == *owners && last.1 + 1 == start => last.1 = end,
            _ => out.push((start, end, owners.clone())),
        }
    }
    out
}

fn node_of(t: &Topology, id: u16) -> Option<&TopologyNode> {
    t.nodes.iter().find(|n| n.id == id)
}

fn info_text(t: &Topology) -> String {
    let assigned: u32 = slot_ranges(&t.pid_owners)
        .iter()
        .map(|(s, e, _)| (e - s + 1) as u32)
        .sum();
    let my_epoch = node_of(t, t.self_id).map(|n| n.generation).unwrap_or(0);
    format!(
        "cluster_enabled:1\r\ncluster_state:ok\r\n\
         cluster_slots_assigned:{assigned}\r\ncluster_slots_ok:{assigned}\r\n\
         cluster_slots_pfail:0\r\ncluster_slots_fail:0\r\n\
         cluster_known_nodes:{}\r\ncluster_size:{}\r\n\
         cluster_current_epoch:{}\r\ncluster_my_epoch:{my_epoch}\r\n",
        t.nodes.len(),
        t.nodes.iter().filter(|n| n.state == "active").count(),
        t.epoch,
    )
}

fn nodes_text(t: &Topology) -> String {
    let ranges = slot_ranges(&t.pid_owners);
    let mut out = String::new();
    for n in &t.nodes {
        let Some(addr) = n.resp_addr else { continue };
        let flags = if n.id == t.self_id { "myself,master" } else { "master" };
        let mut line = format!(
            "{} {}:{}@{} {} - 0 0 {} connected",
            node_hex_id(n.id),
            addr.ip(),
            addr.port(),
            n.gossip_port,
            flags,
            n.generation,
        );
        for (s, e, owners) in &ranges {
            if owners.first() == Some(&n.id) {
                line.push_str(&format!(" {s}-{e}"));
            }
        }
        out.push_str(&line);
        out.push('\n');
    }
    out
}

fn node_entry(t: &Topology, id: u16) -> Option<Reply> {
    let n = node_of(t, id)?;
    let addr = n.resp_addr?;
    Some(Reply::Array(vec![
        Reply::Bulk(addr.ip().to_string().into_bytes()),
        Reply::Int(addr.port() as i64),
        Reply::Bulk(node_hex_id(id).into_bytes()),
    ]))
}

fn slots_reply(t: &Topology) -> Reply {
    let mut entries = Vec::new();
    for (start, end, owners) in slot_ranges(&t.pid_owners) {
        // Master (H1) must have a client address; ranges without one are
        // omitted — clients fall back to any node + read-through.
        let Some(master) = owners.first().and_then(|id| node_entry(t, *id)) else {
            continue;
        };
        let mut entry = vec![Reply::Int(start as i64), Reply::Int(end as i64), master];
        entry.extend(owners[1..].iter().filter_map(|id| node_entry(t, *id)));
        entries.push(Reply::Array(entry));
    }
    Reply::Array(entries)
}

fn shards_reply(t: &Topology) -> Reply {
    let mut shards = Vec::new();
    for (start, end, owners) in slot_ranges(&t.pid_owners) {
        let mut nodes = Vec::new();
        for (i, id) in owners.iter().enumerate() {
            let Some(n) = node_of(t, *id) else { continue };
            let Some(addr) = n.resp_addr else { continue };
            nodes.push(Reply::Map(vec![
                (Reply::bulk_str("id"), Reply::bulk_str(node_hex_id(*id))),
                (Reply::bulk_str("port"), Reply::Int(addr.port() as i64)),
                (Reply::bulk_str("ip"), Reply::bulk_str(addr.ip().to_string())),
                (Reply::bulk_str("endpoint"), Reply::bulk_str(addr.ip().to_string())),
                (
                    Reply::bulk_str("role"),
                    Reply::bulk_str(if i == 0 { "master" } else { "replica" }),
                ),
                (Reply::bulk_str("replication-offset"), Reply::Int(0)),
                (Reply::bulk_str("health"), Reply::bulk_str("online")),
            ]));
        }
        if nodes.is_empty() {
            continue;
        }
        shards.push(Reply::Map(vec![
            (
                Reply::bulk_str("slots"),
                Reply::Array(vec![Reply::Int(start as i64), Reply::Int(end as i64)]),
            ),
            (Reply::bulk_str("nodes"), Reply::Array(nodes)),
        ]));
    }
    Reply::Array(shards)
}

pub fn cluster(engine: &Arc<Engine>, _sess: &mut Session, args: &[Vec<u8>]) -> Reply {
    let Some(sub) = args.get(1) else {
        return Reply::wrong_args("cluster");
    };
    let sub = String::from_utf8_lossy(sub).to_ascii_uppercase();
    // KEYSLOT is pure — works even without a topology hook (embedded use).
    if sub == "KEYSLOT" {
        let Some(key) = args.get(2) else {
            return Reply::wrong_args("cluster|keyslot");
        };
        return Reply::Int(marekvs_core::slot_of(key) as i64);
    }
    let Some(topo_fn) = engine.cluster_topology.read().clone() else {
        return Reply::err("ERR This instance has cluster support disabled");
    };
    let t = topo_fn();
    match sub.as_str() {
        "MYID" => Reply::Bulk(node_hex_id(t.self_id).into_bytes()),
        "INFO" => Reply::Bulk(info_text(&t).into_bytes()),
        "SLOTS" => slots_reply(&t),
        "SHARDS" => shards_reply(&t),
        "NODES" => Reply::Bulk(nodes_text(&t).into_bytes()),
        _ => Reply::err(format!(
            "ERR Unknown CLUSTER subcommand or wrong number of arguments for '{}'",
            String::from_utf8_lossy(&args[1])
        )),
    }
}
```

- [ ] **Step 4: Run the tests**

Run: `cargo test -p marekvs-engine cluster:: 2>&1 | tail -5`
Expected: all formatter tests PASS.

- [ ] **Step 5: Wire dispatch**

In `crates/marekvs-engine/src/cmd/mod.rs`: `pub mod cluster;` (alphabetical, after `budget`), and after the `"DEBUG"` arm (line 69):

```rust
        // --- cluster topology, read-only (design/15) ---
        "CLUSTER" => cluster::cluster(engine, sess, &args),
```

Run: `cargo build -p marekvs-engine 2>&1 | tail -3` — clean.

- [ ] **Step 6: Commit**

```bash
git add crates/marekvs-engine && git commit -m "feat(engine): read-only CLUSTER command family (design/15)"
```

---

### Task 5: COMMAND catalog entry for CLUSTER

**Files:**
- Modify: `crates/marekvs-engine/src/cmd/command_docs.rs`

- [ ] **Step 1: Add the catalog entry**

Find the table of commands (grep for an existing admin entry like `"debug"` or `"info"` to copy the row shape exactly — arity, flags, tips all follow the existing pattern). Add a `cluster` entry with arity `-2`, no key specs, summary "Topology introspection for cluster-aware clients (read-only; design/15)". Follow the file's existing const-table conventions precisely.

- [ ] **Step 2: Check the catalog invariant**

The file header says the table lists exactly what dispatch serves — there may be a test asserting dispatch⊆catalog. Run: `cargo test -p marekvs-engine command 2>&1 | tail -5`
Expected: PASS (if a coverage test exists, it now requires this entry — that's the point).

- [ ] **Step 3: Commit**

```bash
git add crates/marekvs-engine/src/cmd/command_docs.rs && git commit -m "docs(engine): CLUSTER in the COMMAND catalog"
```

---

### Task 6: Server wiring — install the topology hook

**Files:**
- Modify: `crates/marekvs-server/src/main.rs` (next to the `set_cluster_info` block at lines 237-248)

- [ ] **Step 1: Install the hook**

After the existing `set_cluster_info` block:

```rust
    // CLUSTER SLOTS/NODES topology provider (design/15).
    {
        let cluster = cluster.clone();
        engine.set_cluster_topology(Arc::new(move || {
            let view = cluster.view();
            let n = cluster.replicas_n;
            let nodes = view
                .members
                .iter()
                .map(|m| marekvs_engine::TopologyNode {
                    id: m.node,
                    resp_addr: m.resp_addr,
                    gossip_port: m.gossip_addr.port(),
                    generation: m.generation,
                    state: m.phase.as_str().to_string(),
                })
                .collect();
            let pid_owners = (0..marekvs_core::PARTITIONS)
                .map(|pid| {
                    let mut owners = view.owners(pid, n);
                    // H1 (first Active owner) first — it is the range master.
                    if let Some(h1) = view.h1(pid, n) {
                        if let Some(pos) = owners.iter().position(|o| *o == h1) {
                            owners.swap(0, pos);
                        }
                    }
                    owners
                })
                .collect();
            marekvs_engine::Topology {
                self_id: node_id,
                epoch: view.epoch,
                nodes,
                pid_owners,
            }
        }));
    }
```

- [ ] **Step 2: Build + full suite**

Run: `cargo build --workspace 2>&1 | tail -3` then `cargo test --workspace 2>&1 | grep -E "test result" | sort | uniq -c`
Expected: clean, all ok.

- [ ] **Step 3: Commit**

```bash
git add crates/marekvs-server && git commit -m "feat(server): wire cluster topology provider"
```

---

### Task 7: Integration test — real 3-node cluster, real cluster client

**Files:**
- Create: `tests/cluster_protocol.sh` (modeled on `tests/local_cluster.sh` + `tests/wait_ready.sh`)

- [ ] **Step 1: Write the script**

```bash
#!/usr/bin/env bash
# CLUSTER protocol smoke: slot mapping, topology replies, redis-cli -c routing.
# Usage: tests/cluster_protocol.sh [binary]
set -euo pipefail
cd "$(dirname "$0")/.."

BIN=${1:-target/debug/marekvs-server}
[ -x "$BIN" ] || cargo build -p marekvs-server
N=3
DIR=$(mktemp -d)
SEEDS=""
for i in $(seq 0 $((N - 1))); do SEEDS+="127.0.0.1:$((17946 + i)),"; done

pids=()
cleanup() { kill "${pids[@]}" 2>/dev/null || true; rm -rf "$DIR"; }
trap cleanup EXIT

for i in $(seq 0 $((N - 1))); do
  MAREKVS_NODE_ID=$i \
    MAREKVS_DATA_DIR="$DIR/n$i" \
    MAREKVS_RESP_ADDR="127.0.0.1:$((16379 + i))" \
    MAREKVS_MESH_ADDR="127.0.0.1:$((17373 + i))" \
    MAREKVS_GOSSIP_ADDR="127.0.0.1:$((17946 + i))" \
    MAREKVS_METRICS_ADDR="127.0.0.1:$((19121 + i))" \
    MAREKVS_ADVERTISE_IP=127.0.0.1 \
    MAREKVS_SEEDS="${SEEDS%,}" \
    MAREKVS_REPLICAS_N=2 \
    RUST_LOG=warn "$BIN" &
  pids+=($!)
done

for i in $(seq 0 $((N - 1))); do
  for _ in $(seq 1 60); do
    redis-cli -p $((16379 + i)) PING 2>/dev/null | grep -q PONG && break
    sleep 0.5
  done
done
# All RESP ports up ≠ every node's VIEW has all members Active yet (gossip
# interval 500 ms) — poll node 0's member count before asserting topology.
for _ in $(seq 1 30); do
  [ "$(redis-cli -p 16379 CLUSTER NODES | wc -l | tr -d ' ')" = "$N" ] && break
  sleep 0.5
done

fail() { echo "FAIL: $1" >&2; exit 1; }

# 1. Redis-identical slot mapping.
[ "$(redis-cli -p 16379 CLUSTER KEYSLOT foo)" = "12182" ] || fail "KEYSLOT foo"
[ "$(redis-cli -p 16379 CLUSTER KEYSLOT bar)" = "5061" ] || fail "KEYSLOT bar"

# 2. MYID shape and stability across nodes' views.
id0=$(redis-cli -p 16379 CLUSTER MYID)
[ "${#id0}" = "40" ] || fail "MYID length (${#id0})"

# 3. INFO says enabled/ok.
redis-cli -p 16379 CLUSTER INFO | grep -q "cluster_enabled:1" || fail "INFO enabled"
redis-cli -p 16379 CLUSTER INFO | grep -q "cluster_state:ok" || fail "INFO state"

# 4. Full slot coverage == 16384, computed from CLUSTER NODES trailing
# slot-range fields (robust — CLUSTER SLOTS wire shape varies by redis-cli
# version, and the all-digit 40-hex node ids poison numeric line grubbing).
covered=$(redis-cli -p 16379 CLUSTER NODES \
  | awk '{for(i=9;i<=NF;i++){split($i,a,"-"); s+=a[2]-a[1]+1}} END{print s+0}')
[ "$covered" = "16384" ] || fail "slot coverage ($covered != 16384)"

# 5. NODES lists all members.
[ "$(redis-cli -p 16379 CLUSTER NODES | wc -l | tr -d ' ')" = "$N" ] || fail "NODES count"

# 6. The slot map is real: for each key, query the master CLUSTER NODES
# reports for its slot DIRECTLY (no -c, no redirects possible) and expect a
# hit. This proves keys physically live where the topology says they do —
# `redis-cli -c` would prove nothing here (marekvs never sends MOVED, so -c
# degenerates to read-through).
nodes=$(redis-cli -p 16379 CLUSTER NODES)
for k in alpha bravo charlie delta echo foxtrot golf hotel; do
  [ "$(redis-cli -p 16379 SET "k:$k" "v:$k")" = "OK" ] || fail "SET k:$k"
done
sleep 1 # replication settle
for k in alpha bravo charlie delta echo foxtrot golf hotel; do
  slot=$(redis-cli -p 16379 CLUSTER KEYSLOT "k:$k")
  mport=$(echo "$nodes" | awk -v s="$slot" '{
    for(i=9;i<=NF;i++){split($i,a,"-"); if (s>=a[1] && s<=a[2]) {
      split($2,hp,"@"); split(hp[1],ip,":"); print ip[2]; exit }}}')
  [ -n "$mport" ] || fail "no master for slot $slot"
  [ "$(redis-cli -p "$mport" GET "k:$k")" = "v:$k" ] || fail "GET k:$k on master :$mport"
done

echo "cluster_protocol: OK"
```

- [ ] **Step 2: Run it**

```bash
chmod +x tests/cluster_protocol.sh
cargo build -p marekvs-server && tests/cluster_protocol.sh
```
Expected: `cluster_protocol: OK`. Requires `redis-cli` on PATH (present in this environment — verify with `command -v redis-cli`, and if absent report instead of hacking around it).

- [ ] **Step 3: Commit**

```bash
git add tests/cluster_protocol.sh && git commit -m "test: CLUSTER protocol integration smoke (3-node, redis-cli -c)"
```

---

### Task 8: Documentation

**Files:**
- Modify: `design/03-redis-api.md` (lines 122, 146)
- Modify: `docs/redis-api.md` (~line 204: still lists CLUSTER as unsupported) and `docs/data-model.md` (~line 15: still documents the xxh3 pid derivation) — the published docsgen mirror of the design docs; leaving them stale ships wrong user-facing docs
- Check: `docs/_nav.toml` — decide explicitly whether design/15 gets a published page; if the mirror is generated by the `docsgen` crate, regenerate instead of hand-editing (check `crates/docsgen` for how the mirror is produced)
- Already created: `design/15-cluster-protocol.md` (the spec — verify it is committed)
- Modify: `design/README.md` if it indexes the design docs (check)

- [ ] **Step 1: Update the API matrix**

`design/03-redis-api.md:146` — remove `CLUSTER *` from the unsupported row and add a supported row (match table style):

```markdown
| CLUSTER INFO/MYID/KEYSLOT/SLOTS/SHARDS/NODES | ✓ — read-only topology for cluster-aware clients ([15](15-cluster-protocol.md)); no MOVED/CROSSSLOT — any node serves any key |
```
and in the unsupported row keep the write/topology-mutation side: `WAIT, FAILOVER, CLUSTER SETSLOT/FORGET/MEET (topology is gossip-managed), FUNCTION, ACL beyond AUTH`.

Line 122: change `| SPUBLISH/SSUBSCRIBE (sharded) | ✗ (no cluster slots) |` to `| SPUBLISH/SSUBSCRIBE (sharded) | ✗ |` (the "no cluster slots" rationale is now false).

- [ ] **Step 2: Index + commit**

Check `design/README.md` for a doc index; add `15-cluster-protocol.md` if so. Update the published mirror (`docs/redis-api.md`, `docs/data-model.md`, `docs/_nav.toml` — regenerate via docsgen if that's how the mirror is produced, hand-edit only if docsgen doesn't cover these pages).

```bash
git add design/ docs/ && git commit -m "docs: CLUSTER protocol support in the API matrix; design/15"
```

---

### Task 9: Final verification

- [ ] **Step 1: Full workspace suite**

Run: `cargo test --workspace 2>&1 | grep -E "test result" | sort | uniq -c`
Expected: all ok, zero failed.

- [ ] **Step 2: Clippy**

Run: `cargo clippy --workspace 2>&1 | grep -E "^(warning|error)" | head`
Expected: no NEW warnings vs main (`git stash` not needed — compare judgment; pre-existing warnings out of scope).

- [ ] **Step 3: Integration script once more**

Run: `tests/cluster_protocol.sh`
Expected: `cluster_protocol: OK`

- [ ] **Step 4: Verify the branch tells the story**

Run: `git log --oneline main..HEAD`
Expected: the commits from tasks 1-8, in order, nothing stray. Do NOT merge to main — stop and report (superpowers:finishing-a-development-branch decides integration).

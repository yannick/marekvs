//! CLUSTER command family — read-only topology introspection (design/15).
//! Placement stays gossip+HRW (design/06); nothing here mutates state.

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
    // Every node serves every key (design/15): the full 16384-slot Redis
    // keyspace is always considered assigned, independent of PARTITIONS or
    // how many pids the topology snapshot actually reports.
    let my_epoch = node_of(t, t.self_id).map(|n| n.generation).unwrap_or(0);
    format!(
        "cluster_enabled:1\r\ncluster_state:ok\r\n\
         cluster_slots_assigned:16384\r\ncluster_slots_ok:16384\r\n\
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
        // Same fail-safe as slots_reply: without an addressable master (H1)
        // the whole range is omitted — a shard whose nodes list has no
        // "master" role breaks cluster clients' routing.
        let has_master = owners
            .first()
            .and_then(|id| node_of(t, *id))
            .and_then(|n| n.resp_addr)
            .is_some();
        if !has_master {
            continue;
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reply::Reply;
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

    #[test]
    fn shards_skip_master_without_resp_addr() {
        let mut t = topo();
        t.nodes[0].resp_addr = None;
        let Reply::Array(shards) = shards_reply(&t) else { panic!() };
        // Node 0 masters slots 0..7: that shard is dropped entirely (a
        // shard whose nodes list has no master breaks client routing);
        // the node-1-mastered shard survives with node 0 absent from it.
        assert_eq!(shards.len(), 1);
        let Reply::Map(shard) = &shards[0] else { panic!() };
        let Reply::Array(nodes) = &shard[1].1 else { panic!() };
        assert_eq!(nodes.len(), 1); // replica entry for node 0 dropped too
    }
}

//! Server/connection management family.

use std::sync::Arc;

use crate::cmd::{eq_ignore_case, parse_u64};
use crate::reply::Reply;
use crate::store::now_ms;
use crate::{Engine, Session};
use marekvs_resp::ReplyBuf;

pub fn ping(args: &[Vec<u8>]) -> Reply {
    match args.len() {
        1 => Reply::Simple("PONG"),
        2 => Reply::Bulk(args[1].clone()),
        _ => Reply::wrong_args("ping"),
    }
}

pub fn echo(args: &[Vec<u8>]) -> Reply {
    if args.len() != 2 {
        return Reply::wrong_args("echo");
    }
    Reply::Bulk(args[1].clone())
}

pub fn hello(
    engine: &Arc<Engine>,
    sess: &mut Session,
    args: &[Vec<u8>],
    out: &mut ReplyBuf,
) -> Reply {
    let mut i = 1;
    if let Some(ver) = args.get(1) {
        match parse_u64(ver) {
            Some(2) => sess.resp3 = false,
            Some(3) => sess.resp3 = true,
            _ => return Reply::err("NOPROTO unsupported protocol version"),
        }
        i = 2;
    }
    while i < args.len() {
        if eq_ignore_case(&args[i], "AUTH") {
            let (Some(_user), Some(pass)) = (args.get(i + 1), args.get(i + 2)) else {
                return Reply::syntax();
            };
            if !engine.requirepass.is_empty() && pass != engine.requirepass.as_bytes() {
                return Reply::err("WRONGPASS invalid username-password pair or user is disabled.");
            }
            sess.authenticated = true;
            i += 3;
        } else if eq_ignore_case(&args[i], "SETNAME") {
            if let Some(name) = args.get(i + 1) {
                sess.name = String::from_utf8_lossy(name).into_owned();
            }
            i += 2;
        } else {
            return Reply::syntax();
        }
    }
    if !engine.requirepass.is_empty() && !sess.authenticated {
        return Reply::err("NOAUTH HELLO must be called with the client already authenticated");
    }
    out.resp3 = sess.resp3;
    Reply::Map(vec![
        (Reply::bulk_str("server"), Reply::bulk_str("marekvs")),
        (
            Reply::bulk_str("version"),
            Reply::bulk_str(env!("CARGO_PKG_VERSION")),
        ),
        (
            Reply::bulk_str("proto"),
            Reply::Int(if sess.resp3 { 3 } else { 2 }),
        ),
        (Reply::bulk_str("id"), Reply::Int(sess.id as i64)),
        (Reply::bulk_str("mode"), Reply::bulk_str("cluster")),
        (Reply::bulk_str("role"), Reply::bulk_str("master")),
        (Reply::bulk_str("modules"), Reply::Array(vec![])),
    ])
}

pub fn auth(engine: &Arc<Engine>, sess: &mut Session, args: &[Vec<u8>]) -> Reply {
    let pass = match args.len() {
        2 => &args[1],
        3 => &args[2], // AUTH user pass
        _ => return Reply::wrong_args("auth"),
    };
    if engine.requirepass.is_empty() {
        return Reply::err("ERR Client sent AUTH, but no password is set.");
    }
    if pass == engine.requirepass.as_bytes() {
        sess.authenticated = true;
        Reply::ok()
    } else {
        Reply::err("WRONGPASS invalid username-password pair or user is disabled.")
    }
}

pub fn reset(engine: &Arc<Engine>, sess: &mut Session) -> Reply {
    engine.pubsub.drop_session(sess.id, &sess.subs, &sess.psubs);
    sess.subs.clear();
    sess.psubs.clear();
    sess.resp3 = false;
    sess.authenticated = engine.requirepass.is_empty();
    Reply::Simple("RESET")
}

pub fn select(args: &[Vec<u8>]) -> Reply {
    match args.get(1).and_then(|b| parse_u64(b)) {
        Some(0) => Reply::ok(),
        Some(_) => Reply::err("ERR DB index is out of range (marekvs is single-database)"),
        None => Reply::not_int(),
    }
}

pub fn client(sess: &mut Session, args: &[Vec<u8>]) -> Reply {
    if args.len() < 2 {
        return Reply::wrong_args("client");
    }
    let sub = String::from_utf8_lossy(&args[1]).to_uppercase();
    match sub.as_str() {
        "ID" => Reply::Int(sess.id as i64),
        "GETNAME" => Reply::Bulk(sess.name.clone().into_bytes()),
        "SETNAME" => {
            if let Some(name) = args.get(2) {
                sess.name = String::from_utf8_lossy(name).into_owned();
                Reply::ok()
            } else {
                Reply::wrong_args("client")
            }
        }
        "INFO" => Reply::Bulk(
            format!(
                "id={} name={} resp={}",
                sess.id,
                sess.name,
                if sess.resp3 { 3 } else { 2 }
            )
            .into_bytes(),
        ),
        "LIST" => Reply::Bulk(
            format!(
                "id={} name={} resp={}\n",
                sess.id,
                sess.name,
                if sess.resp3 { 3 } else { 2 }
            )
            .into_bytes(),
        ),
        "NO-EVICT" | "NO-TOUCH" | "SETINFO" => Reply::ok(),
        _ => Reply::err(format!("ERR CLIENT subcommand '{sub}' not supported")),
    }
}

/// `COMMAND` and its subcommands, driven by the static [`command_docs`] table.
/// redis-cli 7+/8 fetches `COMMAND DOCS` at connect to build tab-completion and
/// inline hints, and `COMMAND INFO`/`COUNT` back HELP and other tooling.
pub fn command_cmd(args: &[Vec<u8>]) -> Reply {
    use crate::cmd::command_docs;

    // Bare COMMAND → INFO for every command.
    if args.len() < 2 {
        return Reply::Array(
            command_docs::all()
                .iter()
                .map(command_docs::info_entry)
                .collect(),
        );
    }
    let sub = String::from_utf8_lossy(&args[1]).to_uppercase();
    match sub.as_str() {
        "COUNT" => Reply::Int(command_docs::all().len() as i64),
        "LIST" => Reply::Array(
            command_docs::all()
                .iter()
                .map(|d| Reply::bulk_str(d.name))
                .collect(),
        ),
        "INFO" => {
            // No names → all; otherwise one entry per name (unknown → null).
            let entries: Vec<Reply> = if args.len() == 2 {
                command_docs::all()
                    .iter()
                    .map(command_docs::info_entry)
                    .collect()
            } else {
                args[2..]
                    .iter()
                    .map(|n| {
                        let name = String::from_utf8_lossy(n);
                        match command_docs::find(&name) {
                            Some(d) => command_docs::info_entry(d),
                            None => Reply::NullArray,
                        }
                    })
                    .collect()
            };
            Reply::Array(entries)
        }
        "DOCS" => {
            let pairs: Vec<(Reply, Reply)> = if args.len() == 2 {
                command_docs::all()
                    .iter()
                    .map(|d| (Reply::bulk_str(d.name), command_docs::docs_value(d)))
                    .collect()
            } else {
                args[2..]
                    .iter()
                    .filter_map(|n| {
                        let name = String::from_utf8_lossy(n);
                        command_docs::find(&name)
                            .map(|d| (Reply::bulk_str(d.name), command_docs::docs_value(d)))
                    })
                    .collect()
            };
            Reply::Map(pairs)
        }
        "GETKEYS" | "GETKEYSANDFLAGS" => {
            if args.len() < 3 {
                return Reply::err("ERR Unknown subcommand or wrong number of arguments");
            }
            command_docs::getkeys(&args[2..])
        }
        _ => Reply::err(format!(
            "ERR Unknown COMMAND subcommand or wrong number of arguments for '{sub}'"
        )),
    }
}

pub fn config(args: &[Vec<u8>]) -> Reply {
    if args.len() < 2 {
        return Reply::wrong_args("config");
    }
    let sub = String::from_utf8_lossy(&args[1]).to_uppercase();
    match sub.as_str() {
        "GET" => {
            // Answer common probes with sane values; unknown → empty map.
            let mut pairs = Vec::new();
            for pat in &args[2..] {
                let p = String::from_utf8_lossy(pat).to_lowercase();
                let known: &[(&str, &str)] = &[
                    ("maxmemory", "0"),
                    ("maxmemory-policy", "noeviction"),
                    ("appendonly", "no"),
                    ("save", ""),
                    ("databases", "1"),
                ];
                for (k, v) in known {
                    if crate::pubsub::glob_match(p.as_bytes(), k.as_bytes()) {
                        pairs.push((Reply::bulk_str(*k), Reply::bulk_str(*v)));
                    }
                }
            }
            Reply::Map(pairs)
        }
        "SET" => Reply::ok(), // accepted, ignored (env-driven config)
        "RESETSTAT" | "REWRITE" => Reply::ok(),
        _ => Reply::err(format!("ERR CONFIG subcommand '{sub}' not supported")),
    }
}

/// INFO [section ...] — Redis-shaped sections with real values (the first
/// cut hardcoded tcp_port/clients and buried cluster stats in the
/// replication section). Sections: server, clients, persistence,
/// replication, cluster, keyspace; no args / "all" / "default" /
/// "everything" = all of them.
pub async fn info(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    let wanted: Vec<String> = args[1..]
        .iter()
        .map(|a| String::from_utf8_lossy(a).to_lowercase())
        .filter(|s| !matches!(s.as_str(), "all" | "default" | "everything"))
        .collect();
    let want = |s: &str| wanted.is_empty() || wanted.iter().any(|w| w == s);
    let mut out = String::new();

    if want("server") {
        let uptime = (now_ms() - engine.started_at_ms) / 1000;
        out.push_str(&format!(
            "# Server\r\nredis_version:7.4.0\r\nredis_mode:cluster\r\nserver_name:marekvs\r\n\
             marekvs_version:{}\r\nrun_id:{}\r\ntcp_port:{}\r\nuptime_in_seconds:{}\r\n\
             uptime_in_days:{}\r\nnode_id:{}\r\n\r\n",
            env!("CARGO_PKG_VERSION"),
            engine.run_id,
            engine.tcp_port.load(std::sync::atomic::Ordering::Relaxed),
            uptime,
            uptime / 86_400,
            engine.store.node_id,
        ));
    }
    if want("clients") {
        out.push_str(&format!(
            "# Clients\r\nconnected_clients:{}\r\nblocked_clients:0\r\n\r\n",
            engine
                .clients
                .load(std::sync::atomic::Ordering::Relaxed)
                .max(0),
        ));
    }
    if want("persistence") {
        out.push_str(
            "# Persistence\r\nloading:0\r\npersistence_engine:ondadb\r\n\
             sync_mode:interval\r\n\r\n",
        );
    }
    if want("replication") {
        // When following an upstream Redis master (REPLICAOF) we report
        // role:slave and the link details; marekvs stays writable (AP), so
        // slave_read_only is always 0.
        let up = engine.replicaof_info();
        let role = if up.active { "slave" } else { "master" };
        out.push_str(&format!(
            "# Replication\r\nrole:{}\r\n{}connected_slaves:0\r\n\r\n",
            role, up.lines,
        ));
    }
    if want("cluster") {
        let cluster = engine
            .cluster_info
            .read()
            .as_ref()
            .map(|f| f())
            .unwrap_or_else(|| "cluster_enabled:0\r\n".to_string());
        out.push_str(&format!("# Cluster\r\n{cluster}\r\n"));
    }
    if want("keyspace") {
        let keys = keyspace_count(engine).await;
        out.push_str("# Keyspace\r\n");
        if keys > 0 {
            out.push_str(&format!("db0:keys={keys},expires=0,avg_ttl=0\r\n"));
        }
        out.push_str("\r\n");
    }
    Reply::Bulk(out.into_bytes())
}

/// Distinct visible user keys (same walk as DBSIZE; INFO is not a hot path).
async fn keyspace_count(engine: &Arc<Engine>) -> i64 {
    match dbsize(engine).await {
        Reply::Int(n) => n,
        _ => 0,
    }
}

/// REPLICAOF host port | REPLICAOF NO ONE (SLAVEOF alias). Reply is immediate;
/// the actual sync/stream work happens in the background task installed via
/// [`Engine::set_replicaof`].
pub fn replicaof(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 3 {
        return Reply::wrong_args("replicaof");
    }
    let Some(ctl) = engine.replicaof.read().clone() else {
        return Reply::err("ERR upstream replication is not enabled on this node");
    };
    if eq_ignore_case(&args[1], "NO") && eq_ignore_case(&args[2], "ONE") {
        ctl.stop();
        return Reply::ok();
    }
    let host = String::from_utf8_lossy(&args[1]).into_owned();
    let Some(port) = parse_u64(&args[2]).filter(|p| *p > 0 && *p <= u16::MAX as u64) else {
        return Reply::err("ERR Invalid master port");
    };
    ctl.replicaof(host, port as u16);
    Reply::ok()
}

pub async fn dbsize(engine: &Arc<Engine>) -> Reply {
    // Approximate: count distinct visible user keys (bounded walk).
    engine
        .store
        .run(0, |ctx| {
            let mut n = 0i64;
            let mut last: Option<Vec<u8>> = None;
            crate::store::scan_prefix(ctx, &[], |k, v| {
                if let Some(p) = marekvs_core::ikey::parse(k) {
                    if p.tag == b'Z' || p.userkey.first() == Some(&0) {
                        return true;
                    }
                    if last.as_deref() == Some(p.userkey) {
                        return true;
                    }
                    if let Some((env, pay)) = marekvs_core::envelope::Envelope::decode(v) {
                        let now = crate::store::now_ms();
                        let vis = if p.tag == b'M' {
                            !env.is_tombstone() && !env.is_expired(now)
                        } else {
                            crate::store::visible(&env, pay, 0, now).is_some()
                                && (!env.rtype().is_or_element()
                                    || marekvs_core::merge::element_value(pay).is_some())
                        };
                        if vis {
                            last = Some(p.userkey.to_vec());
                            n += 1;
                        }
                    }
                }
                true
            });
            Reply::Int(n)
        })
        .await
}

pub async fn flushall(engine: &Arc<Engine>) -> Reply {
    // Tombstone every visible key (replicates like any delete).
    // Deletions must run on each key's own shard.
    let keys = engine
        .store
        .run(0, |ctx| {
            let mut keys: Vec<Vec<u8>> = Vec::new();
            let mut last: Option<Vec<u8>> = None;
            crate::store::scan_prefix(ctx, &[], |k, _| {
                if let Some(p) = marekvs_core::ikey::parse(k) {
                    if p.tag != b'Z'
                        && p.userkey.first() != Some(&0)
                        && last.as_deref() != Some(p.userkey)
                    {
                        last = Some(p.userkey.to_vec());
                        keys.push(p.userkey.to_vec());
                    }
                }
                true
            });
            keys
        })
        .await;
    for key in keys {
        let k = key.clone();
        engine
            .store
            .run_key(&key, move |ctx| {
                crate::cmd::generic::del_key(ctx, &k);
            })
            .await;
    }
    Reply::ok()
}

pub fn time() -> Reply {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap();
    Reply::Array(vec![
        Reply::bulk_str(now.as_secs().to_string()),
        Reply::bulk_str(now.subsec_micros().to_string()),
    ])
}

pub async fn debug(args: &[Vec<u8>]) -> Reply {
    if args.len() >= 2 && eq_ignore_case(&args[1], "SLEEP") {
        if let Some(secs) = args
            .get(2)
            .and_then(|b| std::str::from_utf8(b).ok()?.parse::<f64>().ok())
        {
            tokio::time::sleep(std::time::Duration::from_secs_f64(secs)).await;
            return Reply::ok();
        }
    }
    Reply::ok()
}

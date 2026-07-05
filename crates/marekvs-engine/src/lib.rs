//! marekvs-engine — command engine: shard-threaded storage over ondaDB plus
//! the Redis command families (design/01, design/03).

pub mod cmd;
pub mod metrics;
pub mod pubsub;
pub mod reply;
pub mod store;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use marekvs_resp::ReplyBuf;
use pubsub::{PubMessage, PubSub};
use store::Store;
use tokio::sync::mpsc::UnboundedSender;

/// Read-through hook installed by the replication layer: fetch `userkey`
/// (string + collection records) from a home replica into the local store.
/// Returns true if anything was fetched (caller re-reads locally).
pub trait ReadThrough: Send + Sync {
    fn fetch<'a>(&'a self, userkey: &'a [u8]) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>>;
}

/// Control surface for following an upstream *Redis* master (the `REPLICAOF`
/// live-migration path). Installed by the server via
/// [`Engine::set_replicaof`], kept as a trait so `marekvs-engine` need not
/// depend on the server's tokio replication code — the same indirection the
/// `cluster_info` hook uses.
pub trait ReplicaOfCtl: Send + Sync {
    /// Start, or restart against a new target, following `host:port`.
    fn replicaof(&self, host: String, port: u16);
    /// Stop following but keep all data (`REPLICAOF NO ONE`).
    fn stop(&self);
    /// Current status for the `INFO replication` section.
    fn info(&self) -> ReplicaOfInfo;
}

/// Log-filter reload hook: applies a tracing filter spec to the live
/// subscriber (`CONFIG SET loglevel`).
pub type LogReloadFn = Arc<dyn Fn(&str) -> Result<(), String> + Send + Sync>;

/// Snapshot of upstream-replication status for `INFO replication`.
#[derive(Default)]
pub struct ReplicaOfInfo {
    /// True while a master is configured (role reports `slave`).
    pub active: bool,
    /// Pre-formatted `key:value\r\n` lines to splice into the section.
    pub lines: String,
}

/// Per-connection state, owned by the server's connection task.
pub struct Session {
    pub id: u64,
    pub resp3: bool,
    pub name: String,
    pub subs: Vec<Vec<u8>>,
    pub psubs: Vec<Vec<u8>>,
    pub push_tx: UnboundedSender<PubMessage>,
    pub authenticated: bool,
    pub should_close: bool,
    /// MULTI queue (v1.1): Some = transaction open; the bool flags a queueing
    /// error (EXEC must abort).
    pub multi: Option<(Vec<Vec<Vec<u8>>>, bool)>,
    /// Internal apply path (REPLICAOF upstream writes): exempt from client
    /// guards like the disk write-stop — refusing applies would silently
    /// diverge a follower, and convergent merges are how the node heals.
    pub internal: bool,
}

impl Session {
    pub fn new(id: u64, push_tx: UnboundedSender<PubMessage>) -> Self {
        Self {
            id,
            resp3: false,
            name: String::new(),
            subs: Vec::new(),
            psubs: Vec::new(),
            push_tx,
            authenticated: true,
            should_close: false,
            multi: None,
            internal: false,
        }
    }

    pub fn sub_count(&self) -> usize {
        self.subs.len() + self.psubs.len()
    }
}

pub struct Engine {
    pub store: Arc<Store>,
    pub pubsub: Arc<PubSub>,
    pub read_through: parking_lot::RwLock<Option<Arc<dyn ReadThrough>>>,
    /// Password for AUTH; empty = auth disabled. Seeded from
    /// `MAREKVS_REQUIREPASS`, live-settable via `CONFIG SET requirepass`
    /// (Redis semantics: already-authenticated sessions stay authenticated;
    /// the env value is re-applied on restart).
    pub requirepass: parking_lot::RwLock<String>,
    /// EVAL/EVALSHA wall-clock budget in ms. Seeded from
    /// `MAREKVS_SCRIPT_TIME_LIMIT_MS`, live-settable via
    /// `CONFIG SET lua-time-limit` (alias `busy-reply-threshold`).
    pub script_time_limit_ms: std::sync::atomic::AtomicU64,
    /// Live log-filter reload hook installed by the server
    /// (`CONFIG SET loglevel`); absent in embedded/test use.
    pub log_reload: parking_lot::RwLock<Option<LogReloadFn>>,
    /// Currently applied log-filter directives (`CONFIG GET loglevel`).
    pub log_filter: parking_lot::RwLock<String>,
    pub started_at_ms: u64,
    /// INFO-visible cluster stats provider installed by the server.
    pub cluster_info: parking_lot::RwLock<Option<Arc<dyn Fn() -> String + Send + Sync>>>,
    /// Upstream-Redis replication control (REPLICAOF), installed by the server.
    pub replicaof: parking_lot::RwLock<Option<Arc<dyn ReplicaOfCtl>>>,
    /// Actual RESP listen port (INFO tcp_port), set by the server at boot.
    pub tcp_port: std::sync::atomic::AtomicU16,
    /// Live client-connection count (INFO connected_clients), maintained by
    /// the server's connection loop.
    pub clients: std::sync::atomic::AtomicI64,
    /// Disk high-water write stop (design item: disk-full is THE
    /// unrecoverable LSM failure — ondadb write errors wedge the node
    /// mid-compaction). Set/cleared with hysteresis by the stats task;
    /// checked in cmd::dispatch for client write commands only — peer
    /// replication, AE and bootstrap apply via `apply_op_from` (bypasses
    /// dispatch) and REPLICAOF applies with `Session.internal`.
    pub write_stopped: std::sync::atomic::AtomicBool,
    /// Stable per-boot run id (40 hex chars, Redis convention).
    pub run_id: String,
    /// Prometheus registry + handles (design/07 §Observability).
    pub metrics: metrics::Metrics,
    /// Lua script cache: sha1 hex → source (design/11; also persisted as
    /// hidden replicated system records for cross-node EVALSHA).
    pub scripts: parking_lot::RwLock<std::collections::HashMap<String, String>>,
}

impl Engine {
    pub fn new(store: Arc<Store>) -> Arc<Engine> {
        // 40-hex run id from boot time + node id (unique enough per boot;
        // Redis semantics only need it stable for the process lifetime).
        let metrics = metrics::Metrics::new(store.node_id);
        let now = store::now_ms();
        let run_id = format!(
            "{:016x}{:016x}{:08x}",
            now,
            now.rotate_left(29) ^ 0x9e37_79b9_7f4a_7c15,
            store.node_id as u32
        );
        Arc::new(Engine {
            store,
            pubsub: PubSub::new(),
            read_through: parking_lot::RwLock::new(None),
            requirepass: parking_lot::RwLock::new(
                std::env::var("MAREKVS_REQUIREPASS").unwrap_or_default(),
            ),
            script_time_limit_ms: std::sync::atomic::AtomicU64::new(
                std::env::var("MAREKVS_SCRIPT_TIME_LIMIT_MS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(20),
            ),
            log_reload: parking_lot::RwLock::new(None),
            log_filter: parking_lot::RwLock::new(String::new()),
            started_at_ms: store::now_ms(),
            cluster_info: parking_lot::RwLock::new(None),
            replicaof: parking_lot::RwLock::new(None),
            write_stopped: std::sync::atomic::AtomicBool::new(false),
            tcp_port: std::sync::atomic::AtomicU16::new(6379),
            clients: std::sync::atomic::AtomicI64::new(0),
            metrics,
            run_id,
            scripts: parking_lot::RwLock::new(std::collections::HashMap::new()),
        })
    }

    pub fn set_read_through(&self, rt: Arc<dyn ReadThrough>) {
        *self.read_through.write() = Some(rt);
    }

    pub fn set_cluster_info(&self, f: Arc<dyn Fn() -> String + Send + Sync>) {
        *self.cluster_info.write() = Some(f);
    }

    pub fn set_replicaof(&self, ctl: Arc<dyn ReplicaOfCtl>) {
        *self.replicaof.write() = Some(ctl);
    }

    /// Install the live log-filter reload hook (server wires this to the
    /// tracing reload handle) and record the initially applied directives.
    pub fn set_log_reload(&self, initial: String, f: LogReloadFn) {
        *self.log_filter.write() = initial;
        *self.log_reload.write() = Some(f);
    }

    /// Status of upstream-Redis replication for `INFO` (default when unset).
    pub fn replicaof_info(&self) -> ReplicaOfInfo {
        self.replicaof
            .read()
            .as_ref()
            .map(|c| c.info())
            .unwrap_or_default()
    }

    /// Give the replication layer a chance to fetch/refresh this key from a
    /// home replica. The hook itself decides based on ownership, local
    /// presence, and lease freshness (design/04 §Read path) — the engine
    /// just offers the opportunity before serving a read.
    pub async fn ensure_local(&self, userkey: &[u8]) {
        let rt = self.read_through.read().clone();
        if let Some(rt) = rt {
            rt.fetch(userkey).await;
        }
    }

    /// Commands refused while the disk write-stop is engaged. ALL mutating
    /// commands, deliberately including DEL/UNLINK/EXPIRE/FLUSHALL: LSM
    /// deletes write tombstones and GROW disk until compaction reclaims —
    /// the escape hatch at high-water is operator action (grow the volume),
    /// not more writes. EVAL/EVALSHA may write (Redis blocks them under OOM
    /// for the same reason).
    pub fn is_write_command(name: &str) -> bool {
        matches!(
            name,
            "SET"
                | "SETNX"
                | "SETEX"
                | "PSETEX"
                | "GETSET"
                | "GETDEL"
                | "GETEX"
                | "APPEND"
                | "INCR"
                | "DECR"
                | "INCRBY"
                | "DECRBY"
                | "INCRBYFLOAT"
                | "MSET"
                | "MSETNX"
                | "SETRANGE"
                | "DEL"
                | "UNLINK"
                | "FLUSHALL"
                | "FLUSHDB"
                | "EXPIRE"
                | "PEXPIRE"
                | "EXPIREAT"
                | "PEXPIREAT"
                | "EXPIREMEMBER"
                | "EXPIREMEMBERAT"
                | "PEXPIREMEMBERAT"
                | "PERSIST"
                | "RENAME"
                | "RENAMENX"
                | "COPY"
                | "HSET"
                | "HSETNX"
                | "HMSET"
                | "HDEL"
                | "HGETDEL"
                | "HEXPIRE"
                | "HPEXPIRE"
                | "HEXPIREAT"
                | "HPEXPIREAT"
                | "HPERSIST"
                | "HGETEX"
                | "HSETEX"
                | "HINCRBY"
                | "HINCRBYFLOAT"
                | "SADD"
                | "SREM"
                | "SPOP"
                | "SMOVE"
                | "SUNIONSTORE"
                | "SINTERSTORE"
                | "SDIFFSTORE"
                | "ZADD"
                | "ZINCRBY"
                | "ZREM"
                | "ZPOPMIN"
                | "ZPOPMAX"
                | "BZPOPMIN"
                | "BZPOPMAX"
                | "ZMPOP"
                | "BZMPOP"
                | "ZRANGESTORE"
                | "ZREMRANGEBYSCORE"
                | "ZREMRANGEBYRANK"
                | "ZREMRANGEBYLEX"
                | "ZUNIONSTORE"
                | "ZINTERSTORE"
                | "ZDIFFSTORE"
                | "LPUSH"
                | "RPUSH"
                | "LPUSHX"
                | "RPUSHX"
                | "LPOP"
                | "RPOP"
                | "LSET"
                | "LREM"
                | "LTRIM"
                | "LINSERT"
                | "LMOVE"
                | "RPOPLPUSH"
                | "LMPOP"
                | "BLPOP"
                | "BRPOP"
                | "BLMOVE"
                | "BRPOPLPUSH"
                | "BLMPOP"
                | "XADD"
                | "XDEL"
                | "XTRIM"
                | "XSETID"
                | "PFADD"
                | "PFMERGE"
                | "EVAL"
                | "EVALSHA"
        )
    }

    /// Whether a command is safe for concurrent pipeline dispatch: pure data
    /// ops that never read or mutate Session state (no MULTI/pub-sub/HELLO/
    /// AUTH/blocking/etc.). The server fans batches of these out across
    /// shards; per-key ordering still holds because the batcher never puts
    /// two commands that share an argument in the same batch.
    pub fn parallel_safe(name: &str) -> bool {
        matches!(
            name,
            "GET"
                | "SET"
                | "SETNX"
                | "SETEX"
                | "PSETEX"
                | "GETSET"
                | "GETDEL"
                | "GETEX"
                | "APPEND"
                | "STRLEN"
                | "INCR"
                | "DECR"
                | "INCRBY"
                | "DECRBY"
                | "INCRBYFLOAT"
                | "MGET"
                | "MSET"
                | "MSETNX"
                | "SETRANGE"
                | "GETRANGE"
                | "SUBSTR"
                | "DEL"
                | "UNLINK"
                | "EXISTS"
                | "TYPE"
                | "TTL"
                | "PTTL"
                | "EXPIRE"
                | "PEXPIRE"
                | "EXPIREAT"
                | "PEXPIREAT"
                | "EXPIRETIME"
                | "PEXPIRETIME"
                | "EXPIREMEMBER"
                | "EXPIREMEMBERAT"
                | "PEXPIREMEMBERAT"
                | "PFADD"
                | "PFCOUNT"
                | "PFMERGE"
                | "PERSIST"
                | "COPY"
                | "TOUCH"
                | "SADD"
                | "SREM"
                | "SCARD"
                | "SISMEMBER"
                | "SMISMEMBER"
                | "SMEMBERS"
                | "SPOP"
                | "SRANDMEMBER"
                | "SSCAN"
                | "SMOVE"
                | "SUNION"
                | "SINTER"
                | "SDIFF"
                | "SUNIONSTORE"
                | "SINTERSTORE"
                | "SDIFFSTORE"
                | "SINTERCARD"
                | "HSET"
                | "HMSET"
                | "HSETNX"
                | "HGET"
                | "HMGET"
                | "HGETALL"
                | "HDEL"
                | "HGETDEL"
                | "HEXPIRE"
                | "HPEXPIRE"
                | "HEXPIREAT"
                | "HPEXPIREAT"
                | "HTTL"
                | "HPTTL"
                | "HEXPIRETIME"
                | "HPEXPIRETIME"
                | "HPERSIST"
                | "HGETEX"
                | "HSETEX"
                | "HEXISTS"
                | "HLEN"
                | "HKEYS"
                | "HVALS"
                | "HSTRLEN"
                | "HINCRBY"
                | "HINCRBYFLOAT"
                | "HRANDFIELD"
                | "HSCAN"
                | "ZADD"
                | "ZSCORE"
                | "ZMSCORE"
                | "ZCARD"
                | "ZINCRBY"
                | "ZREM"
                | "ZRANGE"
                | "ZRANGEBYSCORE"
                | "ZREVRANGEBYSCORE"
                | "ZREVRANGE"
                | "ZRANK"
                | "ZREVRANK"
                | "ZCOUNT"
                | "ZLEXCOUNT"
                | "ZPOPMIN"
                | "ZPOPMAX"
                | "ZMPOP"
                | "ZRANDMEMBER"
                | "ZRANGESTORE"
                | "ZRANGEBYLEX"
                | "ZREVRANGEBYLEX"
                | "ZREMRANGEBYSCORE"
                | "ZREMRANGEBYRANK"
                | "ZREMRANGEBYLEX"
                | "ZUNION"
                | "ZINTER"
                | "ZDIFF"
                | "ZUNIONSTORE"
                | "ZINTERSTORE"
                | "ZDIFFSTORE"
                | "ZINTERCARD"
                | "ZSCAN"
                | "LPUSH"
                | "RPUSH"
                | "LPUSHX"
                | "RPUSHX"
                | "LPOP"
                | "RPOP"
                | "LLEN"
                | "LRANGE"
                | "LINDEX"
                | "LSET"
                | "LREM"
                | "LTRIM"
                | "LINSERT"
                | "LPOS"
                | "LMOVE"
                | "RPOPLPUSH"
                | "LMPOP"
                | "XADD"
                | "XLEN"
                | "XRANGE"
                | "XREVRANGE"
                | "XREAD"
                | "XDEL"
                | "XTRIM"
                | "XSETID"
                | "XINFO"
                | "PING"
                | "ECHO"
        )
    }

    /// Dispatch one data command with a throwaway session (used by the
    /// server's concurrent pipeline batcher for `parallel_safe` commands
    /// only). Returns the serialized reply bytes.
    pub async fn dispatch_data(self: &Arc<Self>, resp3: bool, args: Vec<Vec<u8>>) -> Vec<u8> {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let mut sess = Session::new(u64::MAX, tx);
        sess.resp3 = resp3;
        sess.authenticated = true; // batcher only runs on authed connections
        let mut out = ReplyBuf::new(resp3);
        self.dispatch(&mut sess, args, &mut out).await;
        out.take()
    }

    /// Dispatch one parsed command. Writes the reply (or push frames) to `out`.
    pub async fn dispatch(
        self: &Arc<Self>,
        sess: &mut Session,
        args: Vec<Vec<u8>>,
        out: &mut ReplyBuf,
    ) {
        if args.is_empty() {
            return;
        }
        let name = String::from_utf8_lossy(&args[0]).to_uppercase();

        // AUTH / HELLO must work pre-auth.
        if !sess.authenticated && !matches!(name.as_str(), "AUTH" | "HELLO" | "QUIT" | "RESET") {
            out.error("NOAUTH Authentication required.");
            return;
        }

        // MULTI/EXEC (v1.1): queue commands per connection; EXEC runs them
        // sequentially. No atomicity beyond per-key shard serialization —
        // documented in design/03. WATCH is unsupported (no CAS in AP v1.1).
        match name.as_str() {
            "MULTI" => {
                if sess.multi.is_some() {
                    out.error("ERR MULTI calls can not be nested");
                } else {
                    sess.multi = Some((Vec::new(), false));
                    out.simple("OK");
                }
                return;
            }
            "DISCARD" => {
                if sess.multi.take().is_some() {
                    out.simple("OK");
                } else {
                    out.error("ERR DISCARD without MULTI");
                }
                return;
            }
            "WATCH" | "UNWATCH" => {
                out.error("ERR WATCH is not supported (marekvs is AP; no transactional CAS)");
                return;
            }
            "EXEC" => {
                let Some((queued, aborted)) = sess.multi.take() else {
                    out.error("ERR EXEC without MULTI");
                    return;
                };
                if aborted {
                    out.error("EXECABORT Transaction discarded because of previous errors.");
                    return;
                }
                out.array(queued.len());
                for cmd_args in queued {
                    let cmd_name = String::from_utf8_lossy(&cmd_args[0]).to_uppercase();
                    let reply = cmd::dispatch(self, sess, &cmd_name, cmd_args, out).await;
                    reply.write(out);
                }
                return;
            }
            _ if sess.multi.is_some() => {
                // Queue everything else; refuse commands that need the
                // connection out-of-band (pub/sub frames inside EXEC).
                if matches!(
                    name.as_str(),
                    "SUBSCRIBE" | "UNSUBSCRIBE" | "PSUBSCRIBE" | "PUNSUBSCRIBE" | "RESET"
                ) {
                    sess.multi.as_mut().unwrap().1 = true;
                    out.error(&format!("ERR {name} is not allowed in transactions"));
                } else {
                    sess.multi.as_mut().unwrap().0.push(args);
                    out.simple("QUEUED");
                }
                return;
            }
            _ => {}
        }

        let start = std::time::Instant::now();
        let reply = cmd::dispatch(self, sess, &name, args, out).await;
        let errored = matches!(reply, crate::reply::Reply::Err(_));
        self.metrics
            .observe_command(&name, start.elapsed().as_secs_f64(), errored);
        reply.write(out);
    }
}

#[cfg(test)]
mod write_command_tests {
    use super::Engine;

    #[test]
    fn writes_are_classified() {
        for c in [
            "SET",
            "DEL",
            "UNLINK",
            "FLUSHALL",
            "EXPIRE",
            "SADD",
            "SPOP",
            "HSET",
            "HINCRBY",
            "ZADD",
            "ZPOPMIN",
            "LPUSH",
            "RPOPLPUSH",
            "BLPOP",
            "XADD",
            "XTRIM",
            "PFADD",
            "PFMERGE",
            "EVAL",
            "EVALSHA",
            "INCRBYFLOAT",
            "GETDEL",
            "GETEX",
            "RENAME",
            "COPY",
            "MSETNX",
            "HGETDEL",
            "HEXPIRE",
            "HPERSIST",
            "HGETEX",
            "HSETEX",
            "ZRANGESTORE",
            "ZREMRANGEBYRANK",
            "ZUNIONSTORE",
            "ZMPOP",
            "BZPOPMIN",
            "LMPOP",
            "BLMPOP",
            "XSETID",
        ] {
            assert!(Engine::is_write_command(c), "{c} must be write-gated");
        }
    }

    #[test]
    fn reads_and_admin_pass() {
        for c in [
            "GET",
            "MGET",
            "EXISTS",
            "TTL",
            "SCAN",
            "KEYS",
            "SMEMBERS",
            "HGETALL",
            "ZRANGE",
            "ZRANDMEMBER",
            "ZINTERCARD",
            "LRANGE",
            "XRANGE",
            "XINFO",
            "OBJECT",
            "PFCOUNT",
            "INFO",
            "CONFIG",
            "PING",
            "AUTH",
            "SUBSCRIBE",
            "PUBLISH",
            "DBSIZE",
            "SCRIPT",
            "COMMAND",
            "CLIENT",
            "SHUTDOWN",
            "REPLICAOF",
        ] {
            assert!(!Engine::is_write_command(c), "{c} must not be write-gated");
        }
    }
}

//! Upstream-Redis replication (`REPLICAOF`): make this marekvs node copy from
//! and continuously follow a **normal Redis master**. This is the live
//! migration path — point marekvs at Redis, it seeds its data and then streams
//! every subsequent write off the master's replication link.
//!
//! Two concurrent phases (design note in the accompanying report):
//!
//! 1. **Streaming** — connect and handshake as a replica (`PING` → `REPLCONF
//!    listening-port` → `REPLCONF capa eof capa psync2` → `PSYNC ? -1`). The
//!    master answers `+FULLRESYNC <replid> <offset>` and ships an RDB snapshot
//!    which we **discard** (we only find its end — both the `$<len>` disk form
//!    and the diskless `$EOF:<mark>` form, see [`RdbSink`]). The link then
//!    becomes a continuous RESP command stream that we apply by driving the
//!    engine's normal [`Engine::dispatch`] — so every write gets proper
//!    envelopes/HLC/cluster replication for free. We track the byte offset and
//!    answer `REPLCONF GETACK *` with `REPLCONF ACK <offset>` (plus an
//!    unsolicited ACK every second) so the master keeps us.
//! 2. **Snapshot copy** — in parallel, a `SCAN`-based copier over a second
//!    ordinary client connection walks every key and re-applies it locally.
//!    Because the live stream is already flowing, the copy is a read-behind;
//!    transient ordering races self-heal via marekvs' LWW/merge semantics and
//!    subsequent stream updates.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use marekvs_engine::pubsub::PubMessage;
use marekvs_engine::{Engine, ReplicaOfCtl, ReplicaOfInfo, Session};
use marekvs_resp::{ReplyBuf, RespParser};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// ---------------------------------------------------------------------------
// Link status (INFO replication)
// ---------------------------------------------------------------------------

const LINK_NONE: u8 = 0;
const LINK_CONNECT: u8 = 1;
const LINK_CONNECTING: u8 = 2;
const LINK_SYNC: u8 = 3;
const LINK_CONNECTED: u8 = 4;
const LINK_DOWN: u8 = 5;

fn link_str(v: u8) -> &'static str {
    match v {
        LINK_CONNECT => "connect",
        LINK_CONNECTING => "connecting",
        LINK_SYNC => "sync",
        LINK_CONNECTED => "connected",
        LINK_DOWN => "down",
        _ => "none",
    }
}

// ---------------------------------------------------------------------------
// Public handle + control surface
// ---------------------------------------------------------------------------

struct Shared {
    /// Bumped on every start/stop; a running task whose generation no longer
    /// matches must exit. This is our cancellation primitive.
    generation: AtomicU64,
    link: AtomicU8,
    /// Replication offset: bytes of the master's command stream consumed.
    offset: AtomicU64,
    /// Desired master, or `None` when not following.
    target: Mutex<Option<(String, u16)>>,
    /// Log the "ignoring SELECT n>0" warning only once.
    select_warned: AtomicBool,
}

/// Server-side implementation of the engine's [`ReplicaOfCtl`] hook.
pub struct RedisRepl {
    engine: Arc<Engine>,
    /// Port we advertise to the master as our `REPLCONF listening-port`.
    listening_port: u16,
    shared: Arc<Shared>,
}

impl RedisRepl {
    pub fn new(engine: Arc<Engine>, listening_port: u16) -> Arc<RedisRepl> {
        Arc::new(RedisRepl {
            engine,
            listening_port,
            shared: Arc::new(Shared {
                generation: AtomicU64::new(0),
                link: AtomicU8::new(LINK_NONE),
                offset: AtomicU64::new(0),
                target: Mutex::new(None),
                select_warned: AtomicBool::new(false),
            }),
        })
    }
}

impl ReplicaOfCtl for RedisRepl {
    fn replicaof(&self, host: String, port: u16) {
        // Bump generation first so any in-flight task stops, then install the
        // new target and spawn a fresh follower loop.
        let gen = self.shared.generation.fetch_add(1, Ordering::SeqCst) + 1;
        *self.shared.target.lock().unwrap() = Some((host.clone(), port));
        self.shared.offset.store(0, Ordering::SeqCst);
        self.shared.link.store(LINK_CONNECT, Ordering::SeqCst);
        self.shared.select_warned.store(false, Ordering::SeqCst);
        tracing::info!(%host, port, gen, "REPLICAOF: following upstream Redis master");
        let engine = self.engine.clone();
        let shared = self.shared.clone();
        let listening_port = self.listening_port;
        tokio::spawn(async move {
            follow_loop(engine, shared, host, port, listening_port, gen).await;
        });
    }

    fn stop(&self) {
        self.shared.generation.fetch_add(1, Ordering::SeqCst);
        *self.shared.target.lock().unwrap() = None;
        self.shared.link.store(LINK_NONE, Ordering::SeqCst);
        tracing::info!("REPLICAOF NO ONE: stopped following upstream master");
    }

    fn info(&self) -> ReplicaOfInfo {
        let target = self.shared.target.lock().unwrap().clone();
        match target {
            None => ReplicaOfInfo::default(),
            Some((host, port)) => {
                let link = link_str(self.shared.link.load(Ordering::SeqCst));
                let offset = self.shared.offset.load(Ordering::SeqCst);
                let lines = format!(
                    "master_host:{host}\r\nmaster_port:{port}\r\n\
                     master_link_status:{link}\r\nmaster_sync_in_progress:{}\r\n\
                     slave_read_only:0\r\nslave_repl_offset:{offset}\r\n\
                     master_repl_offset:{offset}\r\n",
                    u8::from(self.shared.link.load(Ordering::SeqCst) == LINK_SYNC),
                );
                ReplicaOfInfo {
                    active: true,
                    lines,
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Follower loop: connect, sync, stream, reconnect with backoff.
// ---------------------------------------------------------------------------

fn cancelled(shared: &Shared, gen: u64) -> bool {
    shared.generation.load(Ordering::SeqCst) != gen
}

async fn follow_loop(
    engine: Arc<Engine>,
    shared: Arc<Shared>,
    host: String,
    port: u16,
    listening_port: u16,
    gen: u64,
) {
    let mut backoff = Duration::from_millis(200);
    let max_backoff = Duration::from_secs(5);
    loop {
        if cancelled(&shared, gen) {
            return;
        }
        shared.link.store(LINK_CONNECTING, Ordering::SeqCst);
        match sync_once(&engine, &shared, &host, port, listening_port, gen).await {
            Ok(()) => {
                if cancelled(&shared, gen) {
                    return;
                }
                tracing::warn!(%host, port, "upstream replication stream closed; reconnecting");
                backoff = Duration::from_millis(200);
            }
            Err(e) => {
                if cancelled(&shared, gen) {
                    return;
                }
                tracing::warn!(%host, port, error = %e, "upstream replication error; retrying");
            }
        }
        shared.link.store(LINK_DOWN, Ordering::SeqCst);
        // Cancellable backoff: wake early if generation changes.
        let deadline = tokio::time::Instant::now() + backoff;
        while tokio::time::Instant::now() < deadline {
            if cancelled(&shared, gen) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100).min(backoff)).await;
        }
        backoff = (backoff * 2).min(max_backoff);
    }
}

async fn sync_once(
    engine: &Arc<Engine>,
    shared: &Arc<Shared>,
    host: &str,
    port: u16,
    listening_port: u16,
    gen: u64,
) -> anyhow::Result<()> {
    let mut conn = Conn::connect(host, port).await?;

    // --- replica handshake ---
    conn.send(&encode(&[b"PING"])).await?;
    let _ = conn.read_line().await?; // +PONG (or -NOAUTH/-ERR, tolerated)

    let lp = listening_port.to_string();
    conn.send(&encode(&[b"REPLCONF", b"listening-port", lp.as_bytes()]))
        .await?;
    let _ = conn.read_line().await?;
    conn.send(&encode(&[b"REPLCONF", b"capa", b"eof", b"capa", b"psync2"]))
        .await?;
    let _ = conn.read_line().await?;

    conn.send(&encode(&[b"PSYNC", b"?", b"-1"])).await?;
    // The master emits `\n` keepalive newlines while it forks to produce the
    // snapshot, before the actual `+FULLRESYNC <replid> <offset>` line.
    let line = loop {
        let l = conn.read_line().await?;
        if !l.is_empty() {
            break l;
        }
    };
    let init_offset = parse_fullresync(&line)?;
    shared.offset.store(init_offset, Ordering::SeqCst);
    shared.link.store(LINK_SYNC, Ordering::SeqCst);
    tracing::info!(%host, port, init_offset, "FULLRESYNC accepted; discarding RDB preamble");

    // --- consume & discard the RDB snapshot ---
    conn.consume_rdb().await?;
    shared.link.store(LINK_CONNECTED, Ordering::SeqCst);
    tracing::info!(%host, port, "RDB consumed; live stream established");

    // --- kick off the parallel SCAN snapshot copy on a second connection ---
    {
        let engine = engine.clone();
        let shared = shared.clone();
        let host = host.to_string();
        tokio::spawn(async move {
            match snapshot_copy(&engine, &shared, &host, port, gen).await {
                Ok(n) => tracing::info!(keys = n, "upstream snapshot copy complete"),
                Err(e) => tracing::warn!(error = %e, "upstream snapshot copy failed"),
            }
        });
    }

    // --- apply the live command stream ---
    let (stream, leftover) = conn.into_parts();
    stream_loop(engine, shared, gen, stream, leftover).await
}

/// Drive the post-RDB command stream: apply writes, track offset, send ACKs.
async fn stream_loop(
    engine: &Arc<Engine>,
    shared: &Arc<Shared>,
    gen: u64,
    stream: TcpStream,
    leftover: Vec<u8>,
) -> anyhow::Result<()> {
    let (mut rd, mut wr) = stream.into_split();
    let mut parser = RespParser::new();
    parser.feed(&leftover);
    let mut sess = apply_session();
    let mut ack = tokio::time::interval(Duration::from_secs(1));
    ack.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut buf = vec![0u8; 64 * 1024];

    loop {
        // Drain every complete command currently buffered.
        loop {
            if cancelled(shared, gen) {
                return Ok(());
            }
            let before = parser.buffered();
            match parser.next_command() {
                Ok(Some(args)) => {
                    // Byte offset advances by exactly what this command (and any
                    // transparently-skipped empty frames) consumed.
                    let consumed = (before - parser.buffered()) as u64;
                    let offset = shared.offset.fetch_add(consumed, Ordering::SeqCst) + consumed;
                    apply_stream_cmd(engine, shared, &mut sess, args, offset, &mut wr).await?;
                }
                Ok(None) => break,
                Err(e) => anyhow::bail!("replication stream protocol error: {e:?}"),
            }
        }

        tokio::select! {
            r = rd.read(&mut buf) => {
                let n = r?;
                if n == 0 {
                    return Ok(()); // master closed the link
                }
                parser.feed(&buf[..n]);
            }
            _ = ack.tick() => {
                let off = shared.offset.load(Ordering::SeqCst).to_string();
                wr.write_all(&encode(&[b"REPLCONF", b"ACK", off.as_bytes()])).await?;
            }
        }
    }
}

/// Apply (or answer) one command from the master's replication stream.
async fn apply_stream_cmd(
    engine: &Arc<Engine>,
    shared: &Shared,
    sess: &mut Session,
    args: Vec<Vec<u8>>,
    offset: u64,
    wr: &mut (impl AsyncWriteExt + Unpin),
) -> anyhow::Result<()> {
    if args.is_empty() {
        return Ok(());
    }
    let name = String::from_utf8_lossy(&args[0]).to_ascii_uppercase();
    match name.as_str() {
        // Keepalive from the master — offset already accounted, nothing to do.
        "PING" => {}
        // The master polls for our position; answer immediately or it drops us.
        "REPLCONF" => {
            if args
                .get(1)
                .is_some_and(|s| s.eq_ignore_ascii_case(b"GETACK"))
            {
                let off = offset.to_string();
                wr.write_all(&encode(&[b"REPLCONF", b"ACK", off.as_bytes()]))
                    .await?;
            }
        }
        // Single logical DB: accept SELECT 0, ignore anything else (log once).
        "SELECT" => {
            let db = args
                .get(1)
                .and_then(|b| std::str::from_utf8(b).ok())
                .and_then(|s| s.parse::<u64>().ok());
            if db != Some(0) && !shared.select_warned.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    ?db,
                    "upstream stream selected a non-zero DB; applying all keys to DB 0"
                );
            }
        }
        // Everything else is a real write. Drive it through normal dispatch so
        // it gets envelopes/HLC/cluster replication like any local write. This
        // also transparently handles MULTI/EXEC batches the master may send.
        _ => {
            let mut out = ReplyBuf::new(false);
            engine.dispatch(sess, args, &mut out).await;
        }
    }
    Ok(())
}

/// A throwaway session used to apply replicated writes. Its push channel is
/// wired to a dropped receiver: applied commands are writes and never push to
/// this session, so nothing is ever sent.
fn apply_session() -> Session {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<PubMessage>();
    drop(rx);
    let mut sess = Session::new(0, tx);
    sess.authenticated = true;
    sess
}

// ---------------------------------------------------------------------------
// Phase 2: SCAN-based snapshot copier (second ordinary client connection).
// ---------------------------------------------------------------------------

async fn snapshot_copy(
    engine: &Arc<Engine>,
    shared: &Shared,
    host: &str,
    port: u16,
    gen: u64,
) -> anyhow::Result<u64> {
    let mut conn = Conn::connect(host, port).await?;
    let mut sess = apply_session();
    let mut cursor = b"0".to_vec();
    let mut total = 0u64;
    loop {
        if cancelled(shared, gen) {
            return Ok(total);
        }
        conn.send(&encode(&[b"SCAN", &cursor, b"COUNT", b"500"]))
            .await?;
        let (next, keys) = parse_scan(conn.read_value().await?)?;
        for key in keys {
            if cancelled(shared, gen) {
                return Ok(total);
            }
            if copy_key(engine, &mut conn, &mut sess, &key).await? {
                total += 1;
            }
        }
        if next == b"0" {
            return Ok(total);
        }
        cursor = next;
    }
}

/// Copy a single key from the master into the local store. Returns whether a
/// value was applied (missing/empty keys are skipped).
async fn copy_key(
    engine: &Arc<Engine>,
    conn: &mut Conn,
    sess: &mut Session,
    key: &[u8],
) -> anyhow::Result<bool> {
    conn.send(&encode(&[b"TYPE", key])).await?;
    let ty = conn.read_value().await?.into_string();
    conn.send(&encode(&[b"PTTL", key])).await?;
    let pttl = conn.read_value().await?.as_int().unwrap_or(-1);

    let mut applied = true;
    match ty.as_deref() {
        Some("string") => {
            conn.send(&encode(&[b"GET", key])).await?;
            match conn.read_value().await?.into_bulk() {
                Some(v) => apply(engine, sess, vec![b"SET".to_vec(), key.to_vec(), v]).await,
                None => applied = false,
            }
        }
        Some("hash") => {
            conn.send(&encode(&[b"HGETALL", key])).await?;
            let flat = conn.read_value().await?.into_flat();
            if flat.is_empty() {
                applied = false;
            } else {
                let mut a = vec![b"HSET".to_vec(), key.to_vec()];
                a.extend(flat);
                apply(engine, sess, a).await;
            }
        }
        Some("set") => {
            conn.send(&encode(&[b"SMEMBERS", key])).await?;
            let members = conn.read_value().await?.into_flat();
            if members.is_empty() {
                applied = false;
            } else {
                let mut a = vec![b"SADD".to_vec(), key.to_vec()];
                a.extend(members);
                apply(engine, sess, a).await;
            }
        }
        Some("zset") => {
            conn.send(&encode(&[b"ZRANGE", key, b"0", b"-1", b"WITHSCORES"]))
                .await?;
            // Reply is member,score,member,score …; ZADD wants score member.
            let flat = conn.read_value().await?.into_flat();
            if flat.len() < 2 {
                applied = false;
            } else {
                let mut a = vec![b"ZADD".to_vec(), key.to_vec()];
                for pair in flat.chunks_exact(2) {
                    a.push(pair[1].clone());
                    a.push(pair[0].clone());
                }
                apply(engine, sess, a).await;
            }
        }
        Some("list") => {
            conn.send(&encode(&[b"LRANGE", key, b"0", b"-1"])).await?;
            let items = conn.read_value().await?.into_flat();
            if items.is_empty() {
                applied = false;
            } else {
                let mut a = vec![b"RPUSH".to_vec(), key.to_vec()];
                a.extend(items);
                apply(engine, sess, a).await;
            }
        }
        Some("stream") => {
            conn.send(&encode(&[b"XRANGE", key, b"-", b"+"])).await?;
            let entries = conn.read_value().await?;
            applied = copy_stream(engine, sess, key, entries).await;
        }
        // "none" (key vanished between SCAN and TYPE) or an unknown type.
        _ => applied = false,
    }

    if applied && pttl > 0 {
        apply(
            engine,
            sess,
            vec![
                b"PEXPIRE".to_vec(),
                key.to_vec(),
                pttl.to_string().into_bytes(),
            ],
        )
        .await;
    }
    Ok(applied)
}

/// Replay an XRANGE reply as XADDs preserving ids. Returns whether anything was
/// applied.
async fn copy_stream(engine: &Arc<Engine>, sess: &mut Session, key: &[u8], entries: Val) -> bool {
    let Val::Array(Some(items)) = entries else {
        return false;
    };
    let mut any = false;
    for item in items {
        // Each entry: [id, [field, value, …]].
        let Val::Array(Some(pair)) = item else {
            continue;
        };
        if pair.len() != 2 {
            continue;
        }
        let Some(id) = pair[0].clone().into_bulk() else {
            continue;
        };
        let fields = pair[1].clone().into_flat();
        if fields.is_empty() {
            continue;
        }
        let mut a = vec![b"XADD".to_vec(), key.to_vec(), id];
        a.extend(fields);
        apply(engine, sess, a).await;
        any = true;
    }
    any
}

/// Apply a locally-built command through the engine, discarding the reply.
async fn apply(engine: &Arc<Engine>, sess: &mut Session, args: Vec<Vec<u8>>) {
    let mut out = ReplyBuf::new(false);
    engine.dispatch(sess, args, &mut out).await;
}

fn parse_scan(v: Val) -> anyhow::Result<(Vec<u8>, Vec<Vec<u8>>)> {
    let Val::Array(Some(mut top)) = v else {
        anyhow::bail!("SCAN: expected array reply");
    };
    if top.len() != 2 {
        anyhow::bail!("SCAN: expected [cursor, keys]");
    }
    let keys = top.pop().unwrap().into_flat();
    let cursor = top
        .pop()
        .unwrap()
        .into_bulk()
        .ok_or_else(|| anyhow::anyhow!("SCAN: missing cursor"))?;
    Ok((cursor, keys))
}

// ---------------------------------------------------------------------------
// RESP request encoding + FULLRESYNC parsing
// ---------------------------------------------------------------------------

/// Encode a command as a RESP multi-bulk array.
fn encode(args: &[&[u8]]) -> Vec<u8> {
    let mut b = Vec::with_capacity(16);
    b.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for a in args {
        b.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        b.extend_from_slice(a);
        b.extend_from_slice(b"\r\n");
    }
    b
}

/// Parse `+FULLRESYNC <replid> <offset>` → the starting offset.
fn parse_fullresync(line: &[u8]) -> anyhow::Result<u64> {
    let s = std::str::from_utf8(line)?.trim();
    let s = s.strip_prefix('+').unwrap_or(s);
    let mut it = s.split_ascii_whitespace();
    match it.next() {
        Some("FULLRESYNC") => {}
        other => anyhow::bail!("expected FULLRESYNC, got {other:?} (line: {s:?})"),
    }
    let _replid = it.next();
    let off = it
        .next()
        .and_then(|o| o.parse::<u64>().ok())
        .ok_or_else(|| anyhow::anyhow!("FULLRESYNC missing offset: {s:?}"))?;
    Ok(off)
}

// ---------------------------------------------------------------------------
// RDB preamble consumer (unit-tested)
// ---------------------------------------------------------------------------

/// Result of feeding a chunk to [`RdbSink`].
#[derive(Debug, PartialEq, Eq)]
pub enum FeedOutcome {
    /// The whole chunk belonged to the RDB; feed more.
    NeedMore,
    /// The RDB ended within this chunk; `consumed` bytes belonged to it and the
    /// caller's leftover (the start of the command stream) begins at `consumed`.
    Done { consumed: usize },
}

#[derive(Debug)]
enum RdbState {
    /// Reading the `$…\r\n` header (skipping pre-header keepalive newlines).
    Header,
    /// Disk RDB: `remaining` raw bytes follow, with no trailing CRLF.
    Bulk { remaining: usize },
    /// Diskless RDB: read until the 40-byte `mark` appears; `carry` holds the
    /// tail bytes retained across chunk boundaries for split-mark detection.
    Eof { mark: Vec<u8>, carry: Vec<u8> },
    /// RDB fully consumed.
    Done,
}

/// Incremental consumer that finds and discards the RDB snapshot the master
/// sends after `+FULLRESYNC`, handling both the `$<len>\r\n<bytes>` disk form
/// and the diskless `$EOF:<40-byte-mark>\r\n…<mark>` form, across arbitrary
/// chunk boundaries.
#[derive(Debug)]
pub struct RdbSink {
    state: RdbState,
    hdr: Vec<u8>,
}

impl RdbSink {
    pub fn new() -> Self {
        RdbSink {
            state: RdbState::Header,
            hdr: Vec::new(),
        }
    }

    /// Feed the next chunk of bytes received from the master.
    pub fn feed(&mut self, data: &[u8]) -> Result<FeedOutcome, String> {
        let mut i = 0;
        loop {
            match std::mem::replace(&mut self.state, RdbState::Done) {
                RdbState::Header => {
                    while i < data.len() {
                        let b = data[i];
                        i += 1;
                        // Skip CR/LF keepalives the master emits while forking,
                        // before the actual `$` header begins.
                        if self.hdr.is_empty() && (b == b'\n' || b == b'\r') {
                            continue;
                        }
                        self.hdr.push(b);
                        if self.hdr.ends_with(b"\r\n") {
                            let line = self.hdr[..self.hdr.len() - 2].to_vec();
                            self.hdr.clear();
                            if line.first() != Some(&b'$') {
                                return Err(format!("bad RDB bulk header: {line:?}"));
                            }
                            let body = &line[1..];
                            if let Some(mark) = body.strip_prefix(b"EOF:") {
                                if mark.is_empty() {
                                    return Err("empty diskless EOF mark".into());
                                }
                                self.state = RdbState::Eof {
                                    mark: mark.to_vec(),
                                    carry: Vec::new(),
                                };
                            } else {
                                self.state = RdbState::Bulk {
                                    remaining: parse_len(body)?,
                                };
                            }
                            break;
                        }
                    }
                    if matches!(self.state, RdbState::Done) {
                        // Header not complete and chunk exhausted.
                        self.state = RdbState::Header;
                        return Ok(FeedOutcome::NeedMore);
                    }
                    // New state established; loop to process the rest of `data`.
                }
                RdbState::Bulk { mut remaining } => {
                    let take = remaining.min(data.len() - i);
                    remaining -= take;
                    i += take;
                    if remaining == 0 {
                        self.state = RdbState::Done;
                        return Ok(FeedOutcome::Done { consumed: i });
                    }
                    self.state = RdbState::Bulk { remaining };
                    return Ok(FeedOutcome::NeedMore);
                }
                RdbState::Eof { mark, mut carry } => {
                    let carry_len = carry.len();
                    carry.extend_from_slice(&data[i..]);
                    if let Some(pos) = find_sub(&carry, &mark) {
                        let consumed_in_data = (pos + mark.len()) - carry_len;
                        i += consumed_in_data;
                        self.state = RdbState::Done;
                        return Ok(FeedOutcome::Done { consumed: i });
                    }
                    // Retain the last mark.len()-1 bytes so a mark split across
                    // this and the next chunk is still detected.
                    let keep = mark.len().saturating_sub(1);
                    let start = carry.len().saturating_sub(keep);
                    let new_carry = carry[start..].to_vec();
                    self.state = RdbState::Eof {
                        mark,
                        carry: new_carry,
                    };
                    return Ok(FeedOutcome::NeedMore);
                }
                RdbState::Done => {
                    self.state = RdbState::Done;
                    return Ok(FeedOutcome::Done { consumed: i });
                }
            }
        }
    }
}

impl Default for RdbSink {
    fn default() -> Self {
        Self::new()
    }
}

fn parse_len(body: &[u8]) -> Result<usize, String> {
    let s = std::str::from_utf8(body).map_err(|_| "non-utf8 RDB length".to_string())?;
    s.trim()
        .parse::<usize>()
        .map_err(|_| format!("bad RDB length: {s:?}"))
}

/// Naive substring search (`needle` is a 40-byte random mark — collisions are
/// negligible, and payloads are discarded so speed is not critical).
fn find_sub(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || hay.len() < needle.len() {
        return None;
    }
    (0..=hay.len() - needle.len()).find(|&i| &hay[i..i + needle.len()] == needle)
}

// ---------------------------------------------------------------------------
// Minimal buffered RESP client connection (handshake + copier + streaming)
// ---------------------------------------------------------------------------

/// A parsed RESP reply value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Val {
    Simple(Vec<u8>),
    Error(Vec<u8>),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Val>>),
}

impl Val {
    fn into_bulk(self) -> Option<Vec<u8>> {
        match self {
            Val::Bulk(b) => b,
            Val::Simple(s) => Some(s),
            _ => None,
        }
    }
    fn into_string(self) -> Option<String> {
        self.into_bulk()
            .map(|b| String::from_utf8_lossy(&b).into_owned())
    }
    fn as_int(&self) -> Option<i64> {
        match self {
            Val::Int(i) => Some(*i),
            Val::Bulk(Some(b)) | Val::Simple(b) => std::str::from_utf8(b).ok()?.parse().ok(),
            _ => None,
        }
    }
    /// Flatten an array of bulk strings (map/set/list replies) into raw bytes.
    fn into_flat(self) -> Vec<Vec<u8>> {
        match self {
            Val::Array(Some(items)) => items.into_iter().filter_map(|v| v.into_bulk()).collect(),
            _ => Vec::new(),
        }
    }
}

struct Conn {
    stream: TcpStream,
    buf: Vec<u8>,
    pos: usize,
}

impl Conn {
    async fn connect(host: &str, port: u16) -> anyhow::Result<Conn> {
        let stream = TcpStream::connect((host, port)).await?;
        stream.set_nodelay(true).ok();
        Ok(Conn {
            stream,
            buf: Vec::with_capacity(64 * 1024),
            pos: 0,
        })
    }

    async fn send(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        self.stream.write_all(bytes).await?;
        Ok(())
    }

    /// Read more bytes from the socket, compacting already-consumed bytes first.
    async fn fill(&mut self) -> anyhow::Result<usize> {
        if self.pos > 0 {
            self.buf.drain(0..self.pos);
            self.pos = 0;
        }
        let mut tmp = [0u8; 64 * 1024];
        let n = self.stream.read(&mut tmp).await?;
        self.buf.extend_from_slice(&tmp[..n]);
        Ok(n)
    }

    /// Read one CRLF-terminated line (returned without the CRLF).
    async fn read_line(&mut self) -> anyhow::Result<Vec<u8>> {
        loop {
            if let Some(rel) = self.buf[self.pos..].iter().position(|&b| b == b'\n') {
                let nl = self.pos + rel;
                let mut end = nl;
                if end > self.pos && self.buf[end - 1] == b'\r' {
                    end -= 1;
                }
                let line = self.buf[self.pos..end].to_vec();
                self.pos = nl + 1;
                return Ok(line);
            }
            if self.fill().await? == 0 {
                anyhow::bail!("connection closed while reading line");
            }
        }
    }

    /// Consume and discard the RDB snapshot that follows `+FULLRESYNC`.
    async fn consume_rdb(&mut self) -> anyhow::Result<()> {
        let mut sink = RdbSink::new();
        loop {
            let avail = &self.buf[self.pos..];
            if !avail.is_empty() {
                match sink.feed(avail).map_err(|e| anyhow::anyhow!(e))? {
                    FeedOutcome::Done { consumed } => {
                        self.pos += consumed;
                        return Ok(());
                    }
                    FeedOutcome::NeedMore => {
                        self.pos = self.buf.len();
                    }
                }
            }
            if self.fill().await? == 0 {
                anyhow::bail!("connection closed during RDB transfer");
            }
        }
    }

    /// Read one complete RESP reply value.
    async fn read_value(&mut self) -> anyhow::Result<Val> {
        loop {
            if let Some((val, consumed)) = parse_value(&self.buf[self.pos..])? {
                self.pos += consumed;
                return Ok(val);
            }
            if self.fill().await? == 0 {
                anyhow::bail!("connection closed while reading reply");
            }
        }
    }

    fn into_parts(self) -> (TcpStream, Vec<u8>) {
        let leftover = self.buf[self.pos..].to_vec();
        (self.stream, leftover)
    }
}

/// Parse one RESP reply from the front of `data`.
///
/// `Ok(Some((val, consumed)))` on a complete value, `Ok(None)` if more bytes
/// are needed, `Err` on a malformed frame.
fn parse_value(data: &[u8]) -> anyhow::Result<Option<(Val, usize)>> {
    let Some((line, mut consumed)) = read_line_at(data, 0) else {
        return Ok(None);
    };
    if line.is_empty() {
        anyhow::bail!("empty RESP frame");
    }
    let (kind, body) = (line[0], &line[1..]);
    match kind {
        b'+' => Ok(Some((Val::Simple(body.to_vec()), consumed))),
        b'-' => Ok(Some((Val::Error(body.to_vec()), consumed))),
        b':' => {
            let n = std::str::from_utf8(body)?.trim().parse::<i64>()?;
            Ok(Some((Val::Int(n), consumed)))
        }
        b'$' => {
            let len = std::str::from_utf8(body)?.trim().parse::<i64>()?;
            if len < 0 {
                return Ok(Some((Val::Bulk(None), consumed)));
            }
            let len = len as usize;
            if data.len() < consumed + len + 2 {
                return Ok(None);
            }
            let payload = data[consumed..consumed + len].to_vec();
            Ok(Some((Val::Bulk(Some(payload)), consumed + len + 2)))
        }
        b'*' | b'~' | b'>' => {
            let count = std::str::from_utf8(body)?.trim().parse::<i64>()?;
            if count < 0 {
                return Ok(Some((Val::Array(None), consumed)));
            }
            let mut items = Vec::with_capacity(count as usize);
            for _ in 0..count {
                match parse_value(&data[consumed..])? {
                    Some((v, used)) => {
                        items.push(v);
                        consumed += used;
                    }
                    None => return Ok(None),
                }
            }
            Ok(Some((Val::Array(Some(items)), consumed)))
        }
        // RESP3 map: %N → 2N elements. Flatten into an array for our purposes.
        b'%' => {
            let pairs = std::str::from_utf8(body)?.trim().parse::<i64>()?;
            let count = pairs.max(0) * 2;
            let mut items = Vec::with_capacity(count as usize);
            for _ in 0..count {
                match parse_value(&data[consumed..])? {
                    Some((v, used)) => {
                        items.push(v);
                        consumed += used;
                    }
                    None => return Ok(None),
                }
            }
            Ok(Some((Val::Array(Some(items)), consumed)))
        }
        other => anyhow::bail!("unexpected RESP type byte: {:?}", other as char),
    }
}

/// Read a CRLF line starting at `start`; returns `(content, consumed_from_start)`.
fn read_line_at(data: &[u8], start: usize) -> Option<(Vec<u8>, usize)> {
    let rel = data[start..].iter().position(|&b| b == b'\n')?;
    let nl = start + rel;
    let mut end = nl;
    if end > start && data[end - 1] == b'\r' {
        end -= 1;
    }
    Some((data[start..end].to_vec(), (nl + 1) - start))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ---- RDB preamble framing ----

    fn drive(chunks: &[&[u8]]) -> (usize, Vec<u8>) {
        // Feed chunk by chunk; return (total RDB bytes consumed, leftover bytes).
        let mut sink = RdbSink::new();
        let mut consumed_total = 0;
        let mut leftover = Vec::new();
        for chunk in chunks {
            match sink.feed(chunk).unwrap() {
                FeedOutcome::NeedMore => consumed_total += chunk.len(),
                FeedOutcome::Done { consumed } => {
                    consumed_total += consumed;
                    leftover.extend_from_slice(&chunk[consumed..]);
                    // Anything after Done in later chunks is pure leftover.
                }
            }
        }
        (consumed_total, leftover)
    }

    #[test]
    fn rdb_disk_form_single_chunk() {
        // $5\r\nHELLO  (no trailing CRLF) then the command stream begins.
        let mut input = b"$5\r\nHELLO".to_vec();
        input.extend_from_slice(b"*1\r\n$4\r\nPING\r\n");
        let (consumed, leftover) = drive(&[&input]);
        assert_eq!(consumed, 9); // "$5\r\n" (4) + "HELLO" (5)
        assert_eq!(leftover, b"*1\r\n$4\r\nPING\r\n");
    }

    #[test]
    fn rdb_disk_form_chunked() {
        // Header and payload split across many small chunks.
        let chunks: &[&[u8]] = &[b"$", b"10\r", b"\n0123", b"456789", b"LEFT"];
        let (_consumed, leftover) = drive(chunks);
        assert_eq!(leftover, b"LEFT");
    }

    #[test]
    fn rdb_disk_form_leading_newlines() {
        // Master emits keepalive newlines before the header while forking.
        let input = b"\n\n$3\r\nabcTAIL";
        let (_c, leftover) = drive(&[input]);
        assert_eq!(leftover, b"TAIL");
    }

    #[test]
    fn rdb_diskless_eof_form() {
        let mark = b"1234567890abcdef1234567890abcdef12345678"; // 40 bytes
        let mut input = Vec::new();
        input.extend_from_slice(b"$EOF:");
        input.extend_from_slice(mark);
        input.extend_from_slice(b"\r\n");
        input.extend_from_slice(b"<rdb-payload-bytes>");
        input.extend_from_slice(mark);
        input.extend_from_slice(b"*1\r\n$4\r\nPING\r\n");
        let (_c, leftover) = drive(&[&input]);
        assert_eq!(leftover, b"*1\r\n$4\r\nPING\r\n");
    }

    #[test]
    fn rdb_diskless_mark_split_across_chunks() {
        let mark = b"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"; // 40 'A'
        let mut header = Vec::new();
        header.extend_from_slice(b"$EOF:");
        header.extend_from_slice(mark);
        header.extend_from_slice(b"\r\n");
        // payload then the mark, but split the mark down the middle.
        let payload = b"some-payload";
        let mut c1 = header.clone();
        c1.extend_from_slice(payload);
        c1.extend_from_slice(&mark[..20]);
        let mut c2 = Vec::new();
        c2.extend_from_slice(&mark[20..]);
        c2.extend_from_slice(b"AFTER");
        let (_c, leftover) = drive(&[&c1, &c2]);
        assert_eq!(leftover, b"AFTER");
    }

    // ---- ACK offset accounting ----
    //
    // The stream applier advances the replication offset by the exact byte
    // length each RespParser command consumed (measured via `buffered()`
    // deltas). This validates that accounting against known frame sizes.

    #[test]
    fn ack_offset_accounting_matches_bytes() {
        let ping = b"*1\r\n$4\r\nPING\r\n".to_vec();
        let set = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n".to_vec();
        let getack = b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n".to_vec();
        let mut stream = Vec::new();
        stream.extend_from_slice(&ping);
        stream.extend_from_slice(&set);
        stream.extend_from_slice(&getack);

        let mut parser = RespParser::new();
        parser.feed(&stream);
        let mut offset = 0u64;
        let mut sizes = Vec::new();
        loop {
            let before = parser.buffered();
            match parser.next_command().unwrap() {
                Some(_args) => {
                    let consumed = before - parser.buffered();
                    sizes.push(consumed);
                    offset += consumed as u64;
                }
                None => break,
            }
        }
        assert_eq!(sizes, vec![ping.len(), set.len(), getack.len()]);
        assert_eq!(offset as usize, stream.len());
    }

    // ---- RESP reply parsing ----

    #[test]
    fn parse_scan_reply() {
        // *2\r\n $1 "0" \r\n *2\r\n $1 a \r\n $1 b \r\n
        let data = b"*2\r\n$1\r\n0\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n";
        let (v, used) = parse_value(data).unwrap().unwrap();
        assert_eq!(used, data.len());
        let (cursor, keys) = parse_scan(v).unwrap();
        assert_eq!(cursor, b"0");
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);
    }

    #[test]
    fn parse_value_incomplete_is_none() {
        assert_eq!(parse_value(b"$5\r\nHEL").unwrap(), None);
        assert_eq!(parse_value(b"*2\r\n$1\r\na\r\n").unwrap(), None);
    }

    #[test]
    fn parse_value_null_and_int() {
        assert_eq!(parse_value(b"$-1\r\n").unwrap().unwrap().0, Val::Bulk(None));
        assert_eq!(parse_value(b":42\r\n").unwrap().unwrap().0, Val::Int(42));
    }

    #[test]
    fn fullresync_offset() {
        let off =
            parse_fullresync(b"+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 1234").unwrap();
        assert_eq!(off, 1234);
    }
}

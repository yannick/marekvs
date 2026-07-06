//! Storage layer: ondaDB behind shard threads (design/01 §Storage layer).
//!
//! All ondaDB access happens on one of S shard threads; a key's shard is
//! `pid % S`, so every operation on one key is serialized on one thread —
//! atomic read-modify-write without locks. The tokio side submits closures
//! and awaits a oneshot.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crossbeam_channel::{Receiver, Sender};
use marekvs_core::envelope::{head, Envelope, RecordType};
use marekvs_core::ikey::{self, Pid, Tag};
use marekvs_core::merge::{merge_values, resolve, MergeOutcome};
use marekvs_core::{Hlc, NodeId};
use ondadb::{ColumnFamily, ColumnFamilyConfig, Compression, Options, SyncMode, DB};

/// Tombstone retention (design/05 `gc_grace`).
/// Tombstone retention (design/05 defaults table). Env-tunable via
/// MAREKVS_GC_GRACE_SECS — required by the gc_grace rejoin chaos scenario,
/// which cannot wait an hour. Must be uniform across the cluster.
pub fn gc_grace() -> Duration {
    static V: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        Duration::from_secs(
            std::env::var("MAREKVS_GC_GRACE_SECS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|&v| v > 0)
                .unwrap_or(3600),
        )
    })
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

pub struct StoreConfig {
    pub data_dir: String,
    pub node_id: NodeId,
    pub shard_threads: usize,
    pub sync_mode: SyncMode,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            data_dir: ".data".into(),
            node_id: 0,
            shard_threads: std::thread::available_parallelism()
                .map(|n| n.get().saturating_sub(2).max(2))
                .unwrap_or(2),
            sync_mode: SyncMode::Interval,
        }
    }
}

type Job = Box<dyn FnOnce(&ShardCtx) + Send>;

/// Everything a storage job can touch. One per shard thread.
pub struct ShardCtx {
    pub db: DB,
    pub data: Arc<ColumnFamily>,
    pub meta: Arc<ColumnFamily>,
    pub hlc: Arc<Hlc>,
    pub node_id: NodeId,
    /// Store epoch (design/13): minted once per empty data directory. Budget
    /// slot keys and token ids embed it so a NodeId reused with a fresh PVC
    /// can never collide with the dead incarnation's escrow records.
    pub epoch: u64,
    pub shard: usize,
    /// Pop-front cursors: collection scan prefix → internal key of the last
    /// popped element. Pops (SPOP/ZPOPMIN) leave element tombstones at the
    /// scan front, so pop #k would otherwise skip k dead records — the LSM
    /// queue anti-pattern. The hint lets the next pop seek past the dead
    /// prefix. Purely an optimization: a stale/wrong hint at worst causes a
    /// wraparound rescan from the prefix start. Single-threaded per shard,
    /// hence RefCell.
    pub pop_hints: std::cell::RefCell<std::collections::HashMap<Vec<u8>, Vec<u8>>>,
}

/// Pop-front cursor state for one collection prefix.
pub enum PopHint {
    /// Resume scanning at this internal key.
    At(Vec<u8>),
    /// A full rescan found nothing: the collection is known-drained; pops
    /// return empty without scanning until an element write clears this.
    /// (Stored as the empty vec — no valid internal key is empty.)
    Empty,
}

pub fn get_pop_hint(ctx: &ShardCtx, prefix: &[u8]) -> Option<PopHint> {
    ctx.pop_hints.borrow().get(prefix).map(|v| {
        if v.is_empty() {
            PopHint::Empty
        } else {
            PopHint::At(v.clone())
        }
    })
}

pub fn set_pop_hint(ctx: &ShardCtx, prefix: &[u8], last_key: &[u8]) {
    ctx.pop_hints
        .borrow_mut()
        .insert(prefix.to_vec(), last_key.to_vec());
}

pub fn set_pop_hint_empty(ctx: &ShardCtx, prefix: &[u8]) {
    ctx.pop_hints
        .borrow_mut()
        .insert(prefix.to_vec(), Vec::new());
}

pub fn clear_pop_hint(ctx: &ShardCtx, prefix: &[u8]) {
    ctx.pop_hints.borrow_mut().remove(prefix);
}

/// Element-write notification: rewind the pop cursor when a new element
/// sorts below it (ordered pops must see it) and clear a known-drained
/// marker. Cheap: one map lookup per element write, only on collections
/// that have been popped from.
pub fn pop_hint_on_insert(ctx: &ShardCtx, prefix: &[u8], element_key: &[u8]) {
    let mut hints = ctx.pop_hints.borrow_mut();
    if let Some(hint) = hints.get_mut(prefix) {
        if hint.is_empty() || element_key < hint.as_slice() {
            *hint = element_key.to_vec();
        }
    }
}

/// Scan forward from `start` while keys still match `prefix` (`start` itself
/// is visited when present — callers filter dead records anyway).
pub fn scan_from(
    ctx: &ShardCtx,
    start: &[u8],
    prefix: &[u8],
    mut f: impl FnMut(&[u8], &[u8]) -> bool,
) {
    let txn = ctx.db.begin();
    let mut it = txn.new_iterator(&ctx.data);
    it.seek(start);
    while it.valid() {
        if !it.key().starts_with(prefix) {
            break;
        }
        if !f(it.key(), it.value()) {
            break;
        }
        it.next();
    }
}

pub struct Store {
    pub db: DB,
    pub data: Arc<ColumnFamily>,
    pub meta: Arc<ColumnFamily>,
    pub hlc: Arc<Hlc>,
    pub node_id: NodeId,
    /// Store epoch (see [`ShardCtx::epoch`]).
    pub epoch: u64,
    /// True when this boot minted the epoch on an EMPTY data directory —
    /// i.e. any earlier incarnation's budget records exist only on replicas.
    /// The budget boot grant-fence keys off this (design/13).
    pub epoch_fresh: bool,
    /// Data directory, kept for filesystem usage stats (disk-full guard).
    pub data_dir: std::path::PathBuf,
    shards: Vec<Sender<Job>>,
    shard_handles: Vec<std::thread::JoinHandle<()>>,
}

/// (total_bytes, available_bytes) of the filesystem holding `path`, via
/// statvfs. `None` on failure or non-unix. Available = f_bavail (space left
/// for unprivileged writers — the number that matters before ENOSPC).
#[cfg(unix)]
pub fn fs_usage(path: &std::path::Path) -> Option<(u64, u64)> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(c.as_ptr(), &mut st) } != 0 {
        return None;
    }
    let frsize = if st.f_frsize > 0 {
        st.f_frsize as u64
    } else {
        st.f_bsize as u64
    };
    Some((st.f_blocks as u64 * frsize, st.f_bavail as u64 * frsize))
}

#[cfg(not(unix))]
pub fn fs_usage(_path: &std::path::Path) -> Option<(u64, u64)> {
    None
}

impl Drop for Store {
    /// Release the shard threads and the ondadb directory lock — ondadb
    /// holds an advisory lock on <dir>/LOCK for the life of the open, so a
    /// process (or test) reopening the same directory needs the previous
    /// instance to close, not merely drop.
    fn drop(&mut self) {
        self.shards.clear(); // closing the channels ends the shard loops
        for h in self.shard_handles.drain(..) {
            let _ = h.join(); // no in-flight job may race db.close()
        }
        let _ = self.db.close();
    }
}

impl Store {
    pub fn open(cfg: &StoreConfig) -> anyhow::Result<Arc<Store>> {
        let opts = Options::new(&cfg.data_dir);
        let db = DB::open(opts)?;
        let cf_config = || ColumnFamilyConfig {
            sync_mode: cfg.sync_mode,
            compression: Compression::Lz4,
            ..ColumnFamilyConfig::default()
        };
        let data = match db.get_column_family("data") {
            Some(cf) => cf,
            None => db.create_column_family("data", cf_config())?,
        };
        let meta = match db.get_column_family("meta") {
            Some(cf) => cf,
            None => db.create_column_family("meta", cf_config())?,
        };
        let hlc = Arc::new(Hlc::new());
        SHARD_TOTAL.store(cfg.shard_threads, std::sync::atomic::Ordering::Relaxed);

        // Store epoch: minted only when absent; persisted in the meta CF so
        // it is stable across restarts of the same data directory.
        const EPOCH_KEY: &[u8] = b"store:epoch";
        let (epoch, epoch_fresh) = match db.get(&meta, EPOCH_KEY) {
            Ok(v) if v.len() >= 8 => (u64::from_be_bytes(v[..8].try_into().unwrap()), false),
            _ => {
                // Fresh only if the data CF is empty too — a pre-epoch data
                // dir upgrading in place keeps everything it ever granted.
                let empty = {
                    let txn = db.begin();
                    let mut it = txn.new_iterator(&data);
                    it.seek_to_first();
                    !it.valid()
                };
                let epoch = now_ms();
                db.put(&meta, EPOCH_KEY, &epoch.to_be_bytes(), Duration::ZERO)?;
                (epoch, empty)
            }
        };

        let mut shards = Vec::with_capacity(cfg.shard_threads);
        let mut shard_handles = Vec::with_capacity(cfg.shard_threads);
        for shard in 0..cfg.shard_threads {
            let (tx, rx): (Sender<Job>, Receiver<Job>) = crossbeam_channel::bounded(4096);
            let ctx = ShardCtx {
                db: db.clone(),
                data: data.clone(),
                meta: meta.clone(),
                hlc: hlc.clone(),
                node_id: cfg.node_id,
                epoch,
                shard,
                pop_hints: std::cell::RefCell::new(std::collections::HashMap::new()),
            };
            let handle = std::thread::Builder::new()
                .name(format!("mkv-shard-{shard}"))
                .spawn(move || shard_loop(ctx, rx))?;
            shard_handles.push(handle);
            shards.push(tx);
        }

        Ok(Arc::new(Store {
            db,
            data,
            meta,
            hlc,
            node_id: cfg.node_id,
            epoch,
            epoch_fresh,
            data_dir: std::path::PathBuf::from(&cfg.data_dir),
            shards,
            shard_handles,
        }))
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub fn shard_of(&self, pid: Pid) -> usize {
        pid as usize % self.shards.len()
    }

    /// Run a storage job on the shard owning `pid` and await its result.
    pub async fn run<T, F>(&self, pid: Pid, f: F) -> T
    where
        T: Send + 'static,
        F: FnOnce(&ShardCtx) -> T + Send + 'static,
    {
        // Inline fast-path: already on the owning shard thread → execute
        // directly (same serialization guarantee, no queue round-trip).
        let shard = self.shard_of(pid);
        let mut f = Some(f);
        if let Some(out) = with_inline_ctx(shard, |ctx| (f.take().unwrap())(ctx)) {
            return out;
        }
        let f = f.take().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        let job: Job = Box::new(move |ctx| {
            let _ = tx.send(f(ctx));
        });
        self.shards[self.shard_of(pid)]
            .send(job)
            .expect("shard thread died");
        rx.await.expect("shard job dropped")
    }

    /// Same, keyed by user key.
    pub async fn run_key<T, F>(&self, userkey: &[u8], f: F) -> T
    where
        T: Send + 'static,
        F: FnOnce(&ShardCtx) -> T + Send + 'static,
    {
        self.run(marekvs_core::pid_of(userkey), f).await
    }

    /// Fire-and-forget job (replication apply path).
    pub fn spawn_on(&self, pid: Pid, f: impl FnOnce(&ShardCtx) + Send + 'static) {
        let _ = self.shards[self.shard_of(pid)].send(Box::new(f));
    }

    /// Install the post-commit hook on the data CF (replication feed).
    pub fn set_commit_hook(&self, hook: Option<ondadb::CommitHookFn>) {
        self.data.set_commit_hook(hook);
    }
}

thread_local! {
    /// The ShardCtx owned by THIS thread, when it is a shard thread.
    /// Enables the inline fast-path in [`Store::run`]: a caller already on
    /// the owning shard executes its job directly instead of round-tripping
    /// through the queue. This is what lets Lua scripts drive the ordinary
    /// async command handlers synchronously (design/11): every same-shard
    /// `run_key` resolves inline, so the handler future completes in one
    /// poll — and anything that would actually suspend (wrong shard,
    /// blocking op, remote fetch) is caught by the script's poll-once
    /// driver as an error instead of a deadlock.
    static CURRENT_SHARD_CTX: std::cell::RefCell<Option<std::rc::Rc<ShardCtx>>> =
        const { std::cell::RefCell::new(None) };
}

/// Run `f` with the current thread's ShardCtx if this thread is the shard
/// that owns `shard_idx`.
pub fn with_inline_ctx<T>(shard_idx: usize, f: impl FnOnce(&ShardCtx) -> T) -> Option<T> {
    CURRENT_SHARD_CTX.with(|c| {
        let borrow = c.borrow();
        match borrow.as_ref() {
            Some(ctx) if ctx.shard == shard_idx => Some(f(ctx)),
            _ => None,
        }
    })
}

fn shard_loop(ctx: ShardCtx, rx: Receiver<Job>) {
    let ctx = std::rc::Rc::new(ctx);
    CURRENT_SHARD_CTX.with(|c| *c.borrow_mut() = Some(ctx.clone()));
    // Expiry sweeping (design/01): incremental cursor walk between jobs.
    let mut sweep_cursor: Vec<u8> = Vec::new();
    loop {
        match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(job) => job(&ctx),
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                sweep_expired(&ctx, &mut sweep_cursor, 128);
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return,
        }
    }
}

/// Walk up to `budget` records from the cursor; write expiry tombstones for
/// records whose TTL deadline passed. Expiry tombstone HLC = deadline<<16 so
/// every node converges on the identical tombstone (design/05).
fn sweep_expired(ctx: &ShardCtx, cursor: &mut Vec<u8>, budget: usize) {
    let now = now_ms();
    let mut expired: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    {
        let txn = ctx.db.begin();
        let mut it = txn.new_iterator(&ctx.data);
        if cursor.is_empty() {
            it.seek_to_first();
        } else {
            it.seek(cursor);
        }
        let mut n = 0;
        while it.valid() && n < budget {
            // Shard ownership check: this thread only touches its own pids.
            if let Some(parsed) = ikey::parse(it.key()) {
                // Budget records (tag 'b') NEVER expire generically: token
                // deadlines live in the payload and only the issuing node
                // may fold them — a replica-sweeper tombstone here would
                // destroy pre-fold state the issuer's escrow credit needs
                // (design/13). They carry no envelope TTL today; the skip is
                // explicit so a future TTL use can't reintroduce the trace.
                if parsed.tag != ikey::Tag::Budget as u8
                    && parsed.pid as usize % shard_total(ctx) == ctx.shard
                {
                    if let Some((env, pay)) = Envelope::decode(it.value()) {
                        if !env.is_tombstone() && env.is_expired(now) {
                            expired.push((it.key().to_vec(), expiry_tombstone(&env, pay)));
                        }
                    }
                }
            }
            n += 1;
            it.next();
        }
        *cursor = if it.valid() {
            it.key().to_vec()
        } else {
            Vec::new()
        };
    }
    for (k, v) in expired {
        // Normal merged write → commit hook fires → expiry replicates.
        write_merged(ctx, &k, &v);
    }
}

fn shard_total(_ctx: &ShardCtx) -> usize {
    // Each ShardCtx knows only its index; total is implied by construction.
    // Stored once at startup in a global to keep ShardCtx Copy-free.
    SHARD_TOTAL.load(std::sync::atomic::Ordering::Relaxed)
}
pub(crate) static SHARD_TOTAL: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(1);

fn expiry_tombstone(env: &Envelope, payload: &[u8]) -> Vec<u8> {
    let rtype = env.rtype();
    if rtype.is_or_element() {
        let dots = marekvs_core::merge::element_dots(payload);
        marekvs_core::merge::element_remove(rtype, env.expiry_hlc(), env.origin, &dots)
    } else {
        Envelope::tombstone(rtype, env.expiry_hlc(), env.origin).encode_with(&[])
    }
}

// ---------------------------------------------------------------------------
// ShardCtx storage helpers (used by command handlers and the apply path)
// ---------------------------------------------------------------------------

thread_local! {
    /// Origin of the replication batch currently being applied on this
    /// shard thread, if any. The ondadb commit hook attributes ring entries
    /// to this — NOT to the record envelope's origin: a merged CRDT record
    /// (PN counter, HLL) keeps the VERSION WINNER's origin in its envelope,
    /// so a node holding a future-stamped record would misattribute all its
    /// own subsequent commits to the skewed peer and the pump's
    /// `origin == self` home-push rule would silently drop them (chaos
    /// clock_bump finding: replication stalled for 100s after a +100s bump).
    static APPLY_ORIGIN: std::cell::Cell<Option<NodeId>> =
        const { std::cell::Cell::new(None) };
}

/// Mark the current shard-thread commit(s) as an apply from `origin`
/// (replication/AE/bootstrap ingest). Cleared by the guard's Drop.
pub fn set_apply_origin(origin: NodeId) -> ApplyOriginGuard {
    APPLY_ORIGIN.with(|c| c.set(Some(origin)));
    ApplyOriginGuard
}

/// The commit attribution for the hook: the applying batch's origin, or
/// None for a locally-initiated command (caller substitutes self).
pub fn current_apply_origin() -> Option<NodeId> {
    APPLY_ORIGIN.with(|c| c.get())
}

pub struct ApplyOriginGuard;
impl Drop for ApplyOriginGuard {
    fn drop(&mut self) {
        APPLY_ORIGIN.with(|c| c.set(None));
    }
}

thread_local! {
    /// True while this shard thread performs node-local maintenance writes
    /// that must NOT enter the replication ring (gc_grace rejoin extras
    /// deletion — feeding those del_raws into the ring would replicate a
    /// local cleanup as if it were data).
    static SUPPRESS_COMMIT_HOOK: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Suppress the commit hook for writes on this shard thread until the guard
/// drops (mirror of `set_apply_origin`).
pub fn suppress_commit_hook() -> SuppressHookGuard {
    SUPPRESS_COMMIT_HOOK.with(|c| c.set(true));
    SuppressHookGuard
}

pub fn commit_hook_suppressed() -> bool {
    SUPPRESS_COMMIT_HOOK.with(|c| c.get())
}

pub struct SuppressHookGuard;
impl Drop for SuppressHookGuard {
    fn drop(&mut self) {
        SUPPRESS_COMMIT_HOOK.with(|c| c.set(false));
    }
}

/// Raw point read; NotFound → None.
pub fn get_raw(ctx: &ShardCtx, ikey: &[u8]) -> Option<Vec<u8>> {
    match ctx.db.get(&ctx.data, ikey) {
        Ok(v) => Some(v),
        Err(ondadb::OndaError::NotFound) => None,
        Err(e) => {
            tracing::error!(?e, "ondadb get failed");
            None
        }
    }
}

/// Raw put with the ondaDB TTL backstop derived from the record.
pub fn put_raw(ctx: &ShardCtx, ikey: &[u8], value: &[u8]) {
    let onda_ttl = onda_ttl_for(value);
    if let Err(e) = ctx.db.put(&ctx.data, ikey, value, onda_ttl) {
        tracing::error!(?e, "ondadb put failed");
    }
}

/// Physical delete — ONLY for node-local derived data (zset score index).
/// User records are never physically deleted outside GC; they get tombstones.
pub fn del_raw(ctx: &ShardCtx, ikey: &[u8]) {
    if let Err(e) = ctx.db.delete(&ctx.data, ikey) {
        if !matches!(e, ondadb::OndaError::NotFound) {
            tracing::error!(?e, "ondadb delete failed");
        }
    }
}

pub(crate) fn onda_ttl_for(value: &[u8]) -> Duration {
    match Envelope::decode(value) {
        Some((env, _)) if env.is_tombstone() => gc_grace(),
        Some((env, _)) if env.ttl_deadline_ms != 0 => {
            let now = now_ms();
            let remain = env.ttl_deadline_ms.saturating_sub(now);
            Duration::from_millis(remain) + gc_grace()
        }
        _ => Duration::ZERO,
    }
}

/// Batched blind LWW puts: one ondadb transaction (one WAL group-commit
/// frame, one commit-hook batch) for many records. ONLY valid for records
/// where a fresh local write is guaranteed to win the merge — LWW string/
/// counter-reset writes with a just-issued HLC: `Hlc::now()` is monotonic
/// past every timestamp this node has stored or observed (receive rule at
/// the apply path), so the stored value would always lose `merge_values`
/// anyway and the read is pure waste. NEVER use for OR-element records
/// (their merge is not last-write-wins).
pub fn put_many_lww(ctx: &ShardCtx, items: &[(Vec<u8>, Vec<u8>)]) {
    let mut txn = ctx.db.begin();
    for (ikey, value) in items {
        let ttl = onda_ttl_for(value);
        if let Err(e) = txn.put(&ctx.data, ikey, value, ttl) {
            tracing::error!(?e, "batched put failed");
        }
    }
    if let Err(e) = txn.commit() {
        tracing::error!(?e, "batched commit failed");
    }
}

/// Merge `incoming` into whatever is stored under `ikey`.
/// Returns true when the stored bytes changed.
pub fn write_merged(ctx: &ShardCtx, ikey: &[u8], incoming: &[u8]) -> bool {
    let changed = match get_raw(ctx, ikey) {
        None => {
            put_raw(ctx, ikey, incoming);
            true
        }
        Some(local) => {
            // Budget elements (tag 'b') have their own merges — slot
            // pointwise-max, token rank lattice — routed by the element kind
            // byte; heads (which carry no element suffix) stay on the
            // ordinary LWW path in merge_values (design/13).
            let outcome = match ikey::parse(ikey) {
                Some(p) if p.tag == Tag::Budget as u8 && !p.suffix.is_empty() => {
                    marekvs_core::budget::merge_budget(p.suffix[0], &local, incoming)
                }
                _ => merge_values(&local, incoming),
            };
            match &outcome {
                MergeOutcome::KeepLocal => false,
                _ => {
                    let winner = resolve(&local, incoming, &outcome);
                    put_raw(ctx, ikey, winner);
                    true
                }
            }
        }
    };
    // Pop-cursor maintenance for set members: a LIVE member landing on a
    // popped-from collection must rewind the cursor / clear the drained
    // marker, whatever its source (local SADD, replication, AE, bootstrap).
    // Zset score-index writes have their own hook (cmd::zset::put_index).
    if changed {
        if let Some(p) = ikey::parse(ikey) {
            if p.tag == Tag::SetMember as u8 {
                let live = Envelope::decode(incoming).is_some_and(|(e, _)| !e.is_tombstone());
                if live {
                    let prefix = ikey::collection_prefix(Tag::SetMember, p.userkey);
                    pop_hint_on_insert(ctx, &prefix, ikey);
                }
            }
        }
    }
    changed
}

/// Collection head lookup: (envelope, ctype, del_hlc).
pub fn get_head(ctx: &ShardCtx, userkey: &[u8]) -> Option<(Envelope, u8, u64)> {
    let v = get_raw(ctx, &ikey::head_key(userkey))?;
    let (env, pay) = Envelope::decode(&v)?;
    let (ctype, del_hlc) = head::decode(pay)?;
    Some((env, ctype, del_hlc))
}

/// A record is visible if it is not a tombstone, not expired, and (for
/// collection elements) newer than the collection's delete clock.
pub fn visible<'a>(env: &Envelope, payload: &'a [u8], del_hlc: u64, now: u64) -> Option<&'a [u8]> {
    if env.is_tombstone() || env.is_expired(now) || env.hlc <= del_hlc {
        return None;
    }
    Some(payload)
}

/// Read a visible LWW record (string / list / head-managed blob).
pub fn read_lww(ctx: &ShardCtx, ikey_bytes: &[u8], del_hlc: u64) -> Option<(Envelope, Vec<u8>)> {
    let v = get_raw(ctx, ikey_bytes)?;
    let (env, pay) = Envelope::decode(&v)?;
    visible(&env, pay, del_hlc, now_ms())?;
    Some((env, pay.to_vec()))
}

/// Read a visible OR-element's current value.
pub fn read_element(ctx: &ShardCtx, ikey_bytes: &[u8], del_hlc: u64) -> Option<Vec<u8>> {
    let v = get_raw(ctx, ikey_bytes)?;
    let (env, pay) = Envelope::decode(&v)?;
    visible(&env, pay, del_hlc, now_ms())?;
    marekvs_core::merge::element_value(pay)
}

/// Prefix scan over the data CF. `f` returns false to stop early.
pub fn scan_prefix(ctx: &ShardCtx, prefix: &[u8], mut f: impl FnMut(&[u8], &[u8]) -> bool) {
    let txn = ctx.db.begin();
    let mut it = txn.new_iterator(&ctx.data);
    it.seek(prefix);
    while it.valid() {
        if !it.key().starts_with(prefix) {
            break;
        }
        if !f(it.key(), it.value()) {
            break;
        }
        it.next();
    }
}

/// Resolve the Redis-visible type of a user key: b's' string, b'l' list, or
/// a head ctype (design/02 §What a TYPE check reads). None = key absent.
pub fn key_type(ctx: &ShardCtx, userkey: &[u8]) -> Option<u8> {
    let now = now_ms();
    if let Some(v) = get_raw(ctx, &ikey::string_key(userkey)) {
        if let Some((env, pay)) = Envelope::decode(&v) {
            if visible(&env, pay, 0, now).is_some() {
                return Some(b's');
            }
        }
    }
    if let Some((env, ctype, del_hlc)) = get_head(ctx, userkey) {
        if !env.is_tombstone()
            && !env.is_expired(now)
            && collection_nonempty(ctx, ctype, userkey, del_hlc)
        {
            return Some(ctype);
        }
    }
    if let Some(v) = get_raw(ctx, &ikey::list_key(userkey)) {
        if let Some((env, pay)) = Envelope::decode(&v) {
            if visible(&env, pay, 0, now).is_some() {
                return Some(b'l');
            }
        }
    }
    None
}

fn collection_nonempty(ctx: &ShardCtx, ctype: u8, userkey: &[u8], del_hlc: u64) -> bool {
    let tag = match ctype {
        head::CTYPE_HASH => Tag::HashField,
        head::CTYPE_SET => Tag::SetMember,
        head::CTYPE_ZSET => Tag::ZsetMember,
        head::CTYPE_STREAM => Tag::StreamEntry,
        head::CTYPE_HLL => Tag::HllRegister,
        head::CTYPE_LIST => Tag::ListElem,
        // A budget exists as long as its head is live — escrow slots and
        // tokens are ledger records, not membership (design/13).
        head::CTYPE_BUDGET => return true,
        _ => return false,
    };
    let now = now_ms();
    let mut found = false;
    scan_prefix(ctx, &ikey::collection_prefix(tag, userkey), |_k, v| {
        if let Some((env, pay)) = Envelope::decode(v) {
            if visible(&env, pay, del_hlc, now).is_some() {
                found = true;
                return false;
            }
        }
        true
    });
    found
}

/// Ensure a collection head exists with `ctype`; returns its del_hlc.
/// Writes the head only when absent (heads are LWW; a newer DEL wins later).
pub fn ensure_head(ctx: &ShardCtx, userkey: &[u8], ctype: u8) -> u64 {
    match get_head(ctx, userkey) {
        Some((env, t, del)) if t == ctype && !env.is_tombstone() && !env.is_expired(now_ms()) => {
            del
        }
        prev => {
            // Recreating a collection after DEL/expiry/type-change: the new
            // head must CARRY FORWARD the previous delete clock, or stale
            // pre-delete elements arriving later (replication, anti-entropy)
            // would resurrect (design/02 §Whole-collection delete).
            let now = now_ms();
            let prev_del = prev.map_or(0, |(env, _, del)| {
                let mut d = del;
                if env.is_tombstone() {
                    d = d.max(env.hlc);
                }
                if env.is_expired(now) {
                    d = d.max(env.expiry_hlc());
                }
                d
            });
            let hlc = ctx.hlc.now();
            let env = Envelope::head(hlc, ctx.node_id);
            let val = env.encode_with(&head::encode(ctype, prev_del));
            write_merged(ctx, &ikey::head_key(userkey), &val);
            prev_del
        }
    }
}

fn record_live(ctx: &ShardCtx, ikey_bytes: &[u8], now: u64) -> bool {
    get_raw(ctx, ikey_bytes)
        .and_then(|v| Envelope::decode(&v).map(|(e, _)| e))
        .is_some_and(|e| !e.is_tombstone() && !e.is_expired(now))
}

/// Cheap type gate for command handlers. `want` is b's', b'l' (legacy blob
/// lists, transitional) or a head ctype constant. Ok(del_hlc) — the delete
/// clock to filter elements with (0 for non-collections). Err(()) —
/// WRONGTYPE.
///
/// Lazy: each gate reads only what can actually block it (profiling showed
/// the old eager version — 3 point reads on every string op — as the top
/// marekvs cost under SET load). Uses head *presence* (not emptiness); an
/// emptied collection keeps blocking other types until DEL/GC (v1 quirk).
#[allow(clippy::result_unit_err)] // Err(()) is the WRONGTYPE sentinel by convention
pub fn check_type(ctx: &ShardCtx, userkey: &[u8], want: u8) -> Result<u64, ()> {
    let now = now_ms();
    match want {
        b's' => {
            // Only a live collection head or legacy list blob blocks strings.
            if get_head(ctx, userkey)
                .is_some_and(|(e, _, _)| !e.is_tombstone() && !e.is_expired(now))
                || record_live(ctx, &ikey::list_key(userkey), now)
            {
                return Err(());
            }
            Ok(0)
        }
        b'l' => {
            if get_head(ctx, userkey)
                .is_some_and(|(e, _, _)| !e.is_tombstone() && !e.is_expired(now))
                || record_live(ctx, &ikey::string_key(userkey), now)
            {
                return Err(());
            }
            Ok(0)
        }
        ctype => {
            if record_live(ctx, &ikey::string_key(userkey), now)
                || record_live(ctx, &ikey::list_key(userkey), now)
            {
                return Err(());
            }
            match get_head(ctx, userkey) {
                Some((env, t, del)) => {
                    let head_live = !env.is_tombstone() && !env.is_expired(now);
                    if head_live && t != ctype {
                        return Err(());
                    }
                    // Tombstoned/expired head: collection was deleted — its
                    // delete clock still gates old elements.
                    let del = if env.is_tombstone() {
                        del.max(env.hlc)
                    } else {
                        del
                    };
                    let del = if env.is_expired(now) {
                        del.max(env.expiry_hlc())
                    } else {
                        del
                    };
                    Ok(del)
                }
                None => Ok(0),
            }
        }
    }
}

/// New LWW record value from this node, now.
pub fn new_lww(ctx: &ShardCtx, rtype: RecordType, payload: &[u8], ttl_deadline_ms: u64) -> Vec<u8> {
    Envelope::new(rtype, ctx.hlc.now(), ctx.node_id)
        .with_ttl(ttl_deadline_ms)
        .encode_with(payload)
}

/// New LWW tombstone from this node, now.
pub fn new_tombstone(ctx: &ShardCtx, rtype: RecordType) -> Vec<u8> {
    Envelope::tombstone(rtype, ctx.hlc.now(), ctx.node_id).encode_with(&[])
}

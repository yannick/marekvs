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

/// `usize` knob from the environment, falling back to ondaDB's default.
fn env_usize(var: &str, default: usize) -> usize {
    std::env::var(var)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(default)
}

/// Byte-valued knob. Unlike [`env_usize`], `0` is meaningful here — ondaDB
/// reads it as "no bound" for `max_open_reader_bytes`.
fn env_bytes(var: &str, default: usize) -> usize {
    std::env::var(var)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default)
}

/// Seconds-valued knob parsed as `u64`. `0` is meaningful — it is how ondaDB
/// spells "disabled" for `periodic_compaction_interval`.
fn env_secs(var: &str, default: u64) -> Duration {
    Duration::from_secs(
        std::env::var(var)
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(default),
    )
}

/// `u64` knob. `0` is meaningful — it is how ondaDB spells "no limit" for the
/// background IO rates.
fn env_u64(var: &str, default: u64) -> u64 {
    std::env::var(var)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

/// Boolean knob. Accepts the usual spellings; anything else keeps `default`
/// rather than silently reading as false — a typo in a durability-adjacent
/// setting must not quietly pick the other behaviour.
fn env_bool(var: &str, default: bool) -> bool {
    match std::env::var(var).ok().as_deref().map(str::trim) {
        Some("1") | Some("true") | Some("TRUE") | Some("yes") | Some("on") => true,
        Some("0") | Some("false") | Some("FALSE") | Some("no") | Some("off") => false,
        _ => default,
    }
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

/// A scan that did not observe the whole range it was asked for.
///
/// ondaDB ≥0.7 bounds the number (and bytes) of open SSTable readers, so a
/// reader is re-opened on the read path and can fail there. `new_iterator`
/// cannot return a `Result`, so such a failure yields an iterator that is
/// simply *invalid* and records the error on `err()` — a bare
/// `while it.valid()` loop therefore reads a **failed scan as an empty one**.
///
/// Every scan primitive here reports completion instead, and no caller may
/// treat an incomplete scan as data: an empty-looking anti-entropy digest or
/// an empty-looking `KEYS` reply is a wrong answer that looks authoritative.
#[derive(Debug, Clone)]
pub struct ScanIncomplete(pub String);

impl std::fmt::Display for ScanIncomplete {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "storage scan incomplete: {}", self.0)
    }
}

impl std::error::Error for ScanIncomplete {}

/// Process-wide count of scans that ended incomplete. Polled into
/// `marekvs_scan_errors_total` by the replication stats task; a non-zero rate
/// means reads are silently short somewhere and must be alerted on.
static SCAN_ERRORS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub fn scan_errors_total() -> u64 {
    SCAN_ERRORS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Translate a finished iterator into a completion verdict.
fn scan_outcome(it: &ondadb::Iterator) -> Result<(), ScanIncomplete> {
    match it.err() {
        Some(e) => {
            SCAN_ERRORS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Err(ScanIncomplete(format!("{e:?}")))
        }
        None => Ok(()),
    }
}

/// [`scan_prefix`] for command handlers.
///
/// Redis command helpers return plain values (`Vec<..>`, `Option<..>`, counts)
/// through many layers, so threading a `Result` from every collection scan up
/// to the reply would touch most of the command surface. Instead an incomplete
/// scan is recorded process-wide by [`scan_outcome`] and converted into a
/// client error centrally by the dispatch fence in `cmd::dispatch`, which
/// covers these call sites — and any added later — uniformly.
///
/// Use [`scan_prefix`] directly anywhere that is **not** behind that fence:
/// anti-entropy, replication, bootstrap and startup all must handle the error
/// themselves.
pub fn scan_prefix_cmd(ctx: &ShardCtx, prefix: &[u8], f: impl FnMut(&[u8], &[u8]) -> bool) {
    let _ = scan_prefix(ctx, prefix, f);
}

/// [`scan_from`] for command handlers — see [`scan_prefix_cmd`].
pub fn scan_from_cmd(
    ctx: &ShardCtx,
    start: &[u8],
    prefix: &[u8],
    f: impl FnMut(&[u8], &[u8]) -> bool,
) {
    let _ = scan_from(ctx, start, prefix, f);
}

/// Exclusive upper bound for a prefix scan: the first key sorting after every
/// key that starts with `prefix`. `None` when no such key exists (an empty
/// prefix, or one that is all `0xFF`) — the scan is then unbounded above.
fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.last_mut() {
        if *last == u8::MAX {
            end.pop();
        } else {
            *last += 1;
            return Some(end);
        }
    }
    None
}

/// Iterator over `[lower, prefix-successor)`, so ondaDB can skip SSTables that
/// provably hold no matching key instead of opening and seeking every table in
/// every level (closes design/02 §"ondaDB has no range-bounded iterator").
fn bounded_iter(
    txn: &ondadb::Txn,
    ctx: &ShardCtx,
    lower: &[u8],
    upper: Option<&[u8]>,
) -> ondadb::Iterator {
    txn.new_iterator_bounded(
        &ctx.data,
        std::ops::Bound::Included(lower),
        match upper {
            Some(u) => std::ops::Bound::Excluded(u),
            None => std::ops::Bound::Unbounded,
        },
    )
}

/// Scan forward from `start` while keys still match `prefix` (`start` itself
/// is visited when present — callers filter dead records anyway).
///
/// Returns `Err` if the scan ended early because storage failed; the callback
/// may then have seen only part of the range.
pub fn scan_from(
    ctx: &ShardCtx,
    start: &[u8],
    prefix: &[u8],
    mut f: impl FnMut(&[u8], &[u8]) -> bool,
) -> Result<(), ScanIncomplete> {
    let upper = prefix_upper_bound(prefix);
    let txn = ctx.db.begin();
    let mut it = bounded_iter(&txn, ctx, start, upper.as_deref());
    it.seek(start);
    while it.valid() {
        // Bounds already stop the walk; the prefix re-check keeps the contract
        // independent of the bound computation.
        if !it.key().starts_with(prefix) {
            break;
        }
        if !f(it.key(), it.value()) {
            break;
        }
        it.next();
    }
    scan_outcome(&it)
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
        // List positions carry the node id in their low bits so concurrent
        // cross-node pushes cannot collide (ikey::LIST_POS_STRIDE). Refuse to
        // start rather than silently alias two nodes onto one salt, which
        // would reintroduce exactly the lost-push bug the salt prevents.
        anyhow::ensure!(
            cfg.node_id <= ikey::LIST_NODE_ID_MAX,
            "MAREKVS_NODE_ID must be <= {} (got {}): list positions encode the \
             node id in {} bits to keep concurrent cross-node pushes from \
             colliding",
            ikey::LIST_NODE_ID_MAX,
            cfg.node_id,
            ikey::LIST_POS_STRIDE.trailing_zeros(),
        );
        let mut opts = Options::new(&cfg.data_dir);
        // Pinned, not inherited. These bound resident memory and background IO,
        // and their ondaDB defaults have moved between releases (0.7.0 added a
        // reader count bound, 0.7.5 a 1 GiB byte bound, and num_flush_threads
        // went 2 → 4). marekvs is disk-native — the memtable, this block cache
        // and the OS page cache are its only memory tiers — so an operator
        // needs these where they can see and tune them.
        opts.block_cache_size = env_bytes("MAREKVS_BLOCK_CACHE_BYTES", opts.block_cache_size);
        opts.max_open_readers = env_usize("MAREKVS_MAX_OPEN_READERS", opts.max_open_readers);
        opts.max_open_reader_bytes =
            env_bytes("MAREKVS_MAX_OPEN_READER_BYTES", opts.max_open_reader_bytes);
        opts.num_flush_threads = env_usize("MAREKVS_FLUSH_THREADS", opts.num_flush_threads);
        // ondaDB 0.8.0 made compaction jobs *bounded* (one source file plus the
        // target files it overlaps) and replaced the CF-wide `compact_mu` with
        // range locks, so jobs on disjoint key ranges now genuinely run at once.
        // Before that, raising this bought little; the default of 2 predates the
        // change and is low for a node whose shard threads are all writing.
        opts.num_compaction_threads =
            env_usize("MAREKVS_COMPACTION_THREADS", opts.num_compaction_threads);
        // Whether `db.close()` (from `Store::drop`) drains the compaction
        // backlog before returning. ondaDB declared this before 0.8.0 but read
        // it nowhere: close always drained, which on a large data directory cost
        // seconds to tens of seconds — against `terminationGracePeriodSeconds:
        // 60` (k8s/statefulset.yaml) that risked a SIGKILL *during* close.
        //
        // Default `false` (ondaDB's): leftover debt is legal LSM state the next
        // open resumes from. The cost lands on the restarted pod, which serves
        // reads over a deeper L0 until compaction catches up — and L0 files
        // overlap, so a point read probes every one of them. Set this true where
        // a fast, fully-merged restart matters more than a fast shutdown.
        // Background IO classes and rate limiter (ondaDB 0.9.0 feature 0.6).
        // Bounds background bandwidth so flush and compaction cannot monopolise
        // the device — the shape of problem a shared k8s PVC has. All default
        // to 0 = off, matching ondaDB, whose own acceptance benchmark for this
        // feature was not validated; turn them on against a measurement, not on
        // principle.
        opts.background_io_bytes_per_second = env_u64(
            "MAREKVS_BACKGROUND_IO_BPS",
            opts.background_io_bytes_per_second,
        );
        opts.background_io_burst_bytes = env_u64(
            "MAREKVS_BACKGROUND_IO_BURST_BYTES",
            opts.background_io_burst_bytes,
        );
        opts.obsolete_delete_bytes_per_second = env_u64(
            "MAREKVS_OBSOLETE_DELETE_BPS",
            opts.obsolete_delete_bytes_per_second,
        );
        opts.finish_compactions_on_close = env_bool(
            "MAREKVS_FINISH_COMPACTIONS_ON_CLOSE",
            opts.finish_compactions_on_close,
        );
        tracing::info!(
            block_cache_bytes = opts.block_cache_size,
            max_open_readers = opts.max_open_readers,
            max_open_reader_bytes = opts.max_open_reader_bytes,
            flush_threads = opts.num_flush_threads,
            compaction_threads = opts.num_compaction_threads,
            finish_compactions_on_close = opts.finish_compactions_on_close,
            background_io_bps = opts.background_io_bytes_per_second,
            obsolete_delete_bps = opts.obsolete_delete_bytes_per_second,
            "ondaDB options"
        );
        let db = DB::open(opts)?;
        // Range deletes (ondaDB 0.9.0 feature 1.2) back the cold-partition
        // purge: a rebalance drops this node's copy of an un-owned partition
        // with one record over `[pid, pid+1)` instead of a scan plus a
        // tombstone per key.
        //
        // The bit is ONE-WAY and changes stored bytes, so a database that has
        // taken it can no longer be opened by ondaDB < 0.9.0. That is the
        // rollback boundary for this feature, which is why it is enabled here
        // explicitly rather than inferred from some config field.
        //
        // `CAP_PERIODIC_AGE` rides along for the same reason: periodic
        // compaction needs a durable `SstMeta::last_compaction_time` to measure
        // a table's age against, so the interval below is a setting with
        // nothing behind it until the bit is taken.
        let mut caps = ondadb::format::CAP_RANGE_DELETES | ondadb::format::CAP_PERIODIC_AGE;
        if env_bool("MAREKVS_PREFIX_DELTA_KEYS", false) {
            caps |= ondadb::format::CAP_PREFIX_DELTA;
        }
        db.enable_format_capabilities(caps)?;
        // Idle families never reclaim on a size trigger, and marekvs has two
        // sources of dead-but-resident data that only compaction removes:
        // gc_grace tombstones, and collection elements shadowed by a head
        // tombstone's del_hlc (design/02) — deleting a collection is O(1)
        // writes precisely because its elements are left in place and masked.
        // On a family that has stopped being written, nothing ever revisits
        // them. A 24 h age trigger bounds how long they stay resident.
        // `0` disables (ondaDB's own default).
        let periodic = env_secs("MAREKVS_PERIODIC_COMPACTION_SECS", 86_400);
        // Prefix-delta data blocks (ondaDB 2.1): store each data-block user key
        // as the bytes it does not share with its predecessor. marekvs keys are
        // unusually redundant for this — every element record of one collection
        // repeats `[pid][tag][varint klen][userkey]` and differs only in the
        // suffix, so a 1000-field hash writes that prefix 1000 times.
        //
        // Off by default: it is a space-for-CPU trade, and CAP_PREFIX_DELTA is
        // ONE-WAY. Taking the bit on every database just in case would strand
        // anyone who wanted to roll back, so it is claimed only when the knob
        // is actually switched on.
        let prefix_delta = env_bool("MAREKVS_PREFIX_DELTA_KEYS", false);
        // vlog value cache (ondaDB 0.9.0 feature 0.5). klog_value_threshold is
        // 512 B, so every larger Redis string lives in the vlog and is re-read
        // per access; this caches the decoded value. Default 0 = off, as
        // ondaDB ships it — its acceptance arm was S3-gated and never run.
        //
        // The correctness fix that shipped alongside it (block-cache keys now
        // name a BlockDomain, so a klog block and a vlog frame at the same
        // offset can no longer alias) is unconditional and arrived with the
        // 0.9.0 upgrade itself, not with this knob.
        let vlog_cache = env_bytes("MAREKVS_VLOG_VALUE_CACHE_BYTES", 0);
        tracing::info!(
            periodic_compaction_secs = periodic.as_secs(),
            "ondaDB column-family options"
        );
        let cf_config = move || ColumnFamilyConfig {
            sync_mode: cfg.sync_mode,
            compression: Compression::Lz4,
            periodic_compaction_interval: periodic,
            enable_prefix_delta_keys: prefix_delta,
            max_cached_vlog_value_bytes: vlog_cache,
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
                //
                // A failed probe must NOT read as "empty": ondaDB yields an
                // invalid iterator carrying the error on `err()`, and minting a
                // fresh epoch over an existing data directory is unrecoverable.
                // Refuse to open instead.
                let empty = {
                    let txn = db.begin();
                    let mut it = txn.new_iterator(&data);
                    it.seek_to_first();
                    let empty = !it.valid();
                    if let Some(e) = it.err() {
                        anyhow::bail!(
                            "cannot determine whether the data directory is empty \
                             (storage scan failed: {e:?}); refusing to mint a store \
                             epoch, which would be unrecoverable over existing data"
                        );
                    }
                    empty
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
                if let Err(e) = sweep_expired(&ctx, &mut sweep_cursor, 128) {
                    // Passive ondaDB TTL is still the backstop, so a failed
                    // sweep delays active expiry rather than losing it.
                    tracing::warn!(shard = ctx.shard, error = %e, "expiry sweep incomplete");
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return,
        }
    }
}

/// Walk up to `budget` records from the cursor; write expiry tombstones for
/// records whose TTL deadline passed. Expiry tombstone HLC = deadline<<16 so
/// every node converges on the identical tombstone (design/05).
fn sweep_expired(
    ctx: &ShardCtx,
    cursor: &mut Vec<u8>,
    budget: usize,
) -> Result<(), ScanIncomplete> {
    let now = now_ms();
    let mut expired: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let outcome;
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
        outcome = scan_outcome(&it);
        // A failed walk leaves the cursor where it was so the next tick retries
        // the same ground. Treating the invalid iterator as "reached the end"
        // would rewind the sweep to the start of the keyspace on every error.
        if outcome.is_ok() {
            *cursor = if it.valid() {
                it.key().to_vec()
            } else {
                Vec::new()
            };
        }
    }
    for (k, v) in expired {
        // Normal merged write → commit hook fires → expiry replicates.
        // These were genuinely observed, so they are written even if the walk
        // was cut short.
        write_merged(ctx, &k, &v);
    }
    outcome
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
    let onda_ttl = onda_ttl_for_keyed(ikey, value);
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

/// Drop every record of one partition with a single range delete.
///
/// Internal keys lead with the partition id big-endian (see the [`ikey`] module
/// docs), so a partition is exactly the half-open interval `[pid, pid + 1)` and
/// one record at one sequence replaces a scan plus a tombstone per key.
///
/// **This never reaches the commit hook.** ondaDB does not surface range
/// deletes to hooks at all — a range delete is two keys and no value, while
/// `CommitOp` is one key and one value — which is what makes it safe for the
/// caller that needs a purely LOCAL drop: discarding this node's copy of a
/// partition it no longer owns must not replicate tombstones to the new owners.
/// The scan-and-tombstone predecessor depended on a `suppress_commit_hook()`
/// guard for that; here it is a property of the operation instead of something
/// a future refactor has to remember.
pub fn delete_partition_range(ctx: &ShardCtx, pid: Pid) -> anyhow::Result<()> {
    // `Pid` is u16 and `ikey::PARTITIONS` is 4096, so `pid + 1` cannot overflow
    // a u16 in practice — but compute in u32 and encode two bytes anyway,
    // because a 4-byte end bound would sort BELOW every 2-byte key and silently
    // delete nothing at all.
    let end = u16::try_from(u32::from(pid) + 1)
        .map_err(|_| anyhow::anyhow!("partition {pid} has no representable upper bound"))?;
    ctx.db
        .delete_range(&ctx.data, &pid.to_be_bytes(), &end.to_be_bytes())
        .map_err(|e| anyhow::anyhow!("range delete for partition {pid} failed: {e:?}"))
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

/// Like [`onda_ttl_for`] but with the RGA-anchor exception (design/16): a
/// JSON array-element tombstone must survive physical GC — dropping one
/// dangles other elements' left refs and reorders the array on rebuild. It
/// is retained (no backstop TTL) until the doc's records are rewritten.
/// Map-entry tombstones keep the normal `gc_grace` window: they are pure OR
/// state with the same resurrection story as hash fields.
pub(crate) fn onda_ttl_for_keyed(ikey_bytes: &[u8], value: &[u8]) -> Duration {
    if let Some((env, _)) = Envelope::decode(value) {
        if env.is_tombstone() && env.ttl_deadline_ms == 0 {
            if let Some(p) = ikey::parse(ikey_bytes) {
                if p.tag == Tag::Json as u8
                    && matches!(
                        marekvs_core::json::split_last(p.suffix),
                        Some((_, marekvs_core::json::Seg::Elem(_)))
                    )
                {
                    return Duration::ZERO;
                }
                // Proto repeated-element tombstones are RGA anchors too (their
                // map-key segment tag differs from JSON, so use pdoc's codec).
                if p.tag == Tag::ProtoField as u8
                    && matches!(
                        marekvs_core::pdoc::split_last(p.suffix),
                        Some((_, marekvs_core::pdoc::PSeg::Elem(_)))
                    )
                {
                    return Duration::ZERO;
                }
            }
        }
    }
    onda_ttl_for(value)
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
        let ttl = onda_ttl_for_keyed(ikey, value);
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

/// [`read_lww`] for many keys of the data CF in one pass.
///
/// ondaDB resolves the batch under a single snapshot, with one block fetch and
/// one decompression per distinct block however many of the keys land in it
/// (0.9.0 feature 0.4). The decode deliberately reuses the same
/// `Envelope::decode` and [`visible`] pair that `read_lww` uses — a second copy
/// of the tombstone and TTL checks is a correctness bug waiting to happen,
/// since a batched read that forgot one would serve deleted or expired records.
///
/// Results are positional: `out[i]` corresponds to `ikeys[i]`, including for a
/// key repeated in the batch.
pub fn read_lww_batch(
    ctx: &ShardCtx,
    ikeys: &[Vec<u8>],
    del_hlc: u64,
) -> Vec<Option<(Envelope, Vec<u8>)>> {
    if ikeys.is_empty() {
        return Vec::new();
    }
    let refs: Vec<&[u8]> = ikeys.iter().map(|k| k.as_slice()).collect();
    let mut txn = ctx.db.begin();
    let now = now_ms();
    txn.multi_get(&ctx.data, &refs)
        .into_iter()
        .map(|r| {
            let raw = match r {
                Ok(v) => v,
                // NotFound is an ordinary miss; anything else is a storage
                // fault, and a batched read must not report it as a miss any
                // more quietly than `get_raw` does.
                Err(ondadb::OndaError::NotFound) => return None,
                Err(e) => {
                    tracing::error!(?e, "ondadb multi_get failed");
                    return None;
                }
            };
            let (env, pay) = Envelope::decode(&raw)?;
            visible(&env, pay, del_hlc, now)?;
            Some((env, pay.to_vec()))
        })
        .collect()
}

/// Read a visible OR-element's current value.
pub fn read_element(ctx: &ShardCtx, ikey_bytes: &[u8], del_hlc: u64) -> Option<Vec<u8>> {
    let v = get_raw(ctx, ikey_bytes)?;
    let (env, pay) = Envelope::decode(&v)?;
    visible(&env, pay, del_hlc, now_ms())?;
    // display_value, not element_value: a counter-valued hash field must
    // render as the fold over every live dot, not one dot's raw state (T2-12).
    marekvs_core::merge::element_display_value(env.rtype(), pay)
}

/// Prefix scan over the data CF. `f` returns false to stop early.
///
/// Returns `Err` if the scan ended early because storage failed; the callback
/// may then have seen only part of the range. Callers must not report a
/// partial result as a complete one — see [`ScanIncomplete`].
pub fn scan_prefix(
    ctx: &ShardCtx,
    prefix: &[u8],
    mut f: impl FnMut(&[u8], &[u8]) -> bool,
) -> Result<(), ScanIncomplete> {
    let upper = prefix_upper_bound(prefix);
    let txn = ctx.db.begin();
    let mut it = bounded_iter(&txn, ctx, prefix, upper.as_deref());
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
    scan_outcome(&it)
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
        // A live JSON doc always has a visible root record (design/16).
        head::CTYPE_JSON => Tag::Json,
        // A budget exists as long as its head is live — escrow slots and
        // tokens are ledger records, not membership (design/13).
        head::CTYPE_BUDGET => return true,
        // A proto value is HEAD-ONLY (design/17): the message lives in the
        // head tail, so a live head IS the value.
        head::CTYPE_PROTO => return true,
        _ => return false,
    };
    let now = now_ms();
    let mut found = false;
    // Reached only from `key_type`, i.e. behind the `cmd::dispatch` fence: an
    // incomplete scan would otherwise report a live collection as absent.
    scan_prefix_cmd(ctx, &ikey::collection_prefix(tag, userkey), |_k, v| {
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

#[cfg(test)]
mod env_knob_tests {
    use super::env_bool;

    /// `env_bool` reads a *process-global*, so these cases share one test to
    /// keep cargo's thread-per-test from racing on the same variable.
    #[test]
    fn unset_and_unparseable_keep_the_default() {
        const VAR: &str = "MAREKVS_TEST_ENV_BOOL";
        std::env::remove_var(VAR);
        assert!(env_bool(VAR, true));
        assert!(!env_bool(VAR, false));

        for on in ["1", "true", "TRUE", "yes", "on", " true "] {
            std::env::set_var(VAR, on);
            assert!(env_bool(VAR, false), "{on:?} should read as true");
        }
        for off in ["0", "false", "FALSE", "no", "off"] {
            std::env::set_var(VAR, off);
            assert!(!env_bool(VAR, true), "{off:?} should read as false");
        }
        // A typo must not silently pick the other behaviour: durability-adjacent
        // knobs fail towards the configured default, not towards false.
        for junk in ["ture", "", "2", "enabled"] {
            std::env::set_var(VAR, junk);
            assert!(env_bool(VAR, true), "{junk:?} should keep default true");
            assert!(!env_bool(VAR, false), "{junk:?} should keep default false");
        }
        std::env::remove_var(VAR);
    }
}

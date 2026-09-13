//! Dedicated CPU workers. Admission belongs to the allocation owner, never to
//! the awaiting connection, and queue sends cannot block a Tokio executor.
use crate::reply::Reply;
use marekvs_diff::{Budget, CancelToken, DiffError};
use parking_lot::Mutex;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Weak,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};
#[derive(Clone, Debug)]
pub struct DiffConfig {
    pub budget: Budget,
    pub max_records: usize,
    pub max_physical: usize,
    pub max_bytes: usize,
    pub threads: usize,
    pub inflight_bytes: usize,
    pub queue_ms: u64,
    pub time_limit_ms: u64,
    pub cache_bytes: usize,
}
impl Default for DiffConfig {
    fn default() -> Self {
        let threads = (std::thread::available_parallelism().map_or(4, usize::from) / 4).clamp(1, 8);
        let budget = Budget::default();
        let max_bytes = budget.max_bytes;
        Self {
            budget,
            max_records: 50_000,
            max_physical: 200_000,
            max_bytes,
            threads,
            inflight_bytes: 3 * max_bytes * threads,
            queue_ms: 2000,
            time_limit_ms: 5000,
            cache_bytes: 256 * 1024 * 1024,
        }
    }
}
impl DiffConfig {
    pub fn from_env() -> Self {
        let mut c = Self::default();
        let n = |key: &str, default: usize| {
            std::env::var(format!("MAREKVS_DIFF_{key}"))
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
                .filter(|&n| n > 0)
                .unwrap_or(default)
        };
        c.threads = n("CONCURRENCY", c.threads).min(256);
        c.max_bytes = n("MAX_BYTES", c.max_bytes);
        c.max_records = n("MAX_RECORDS", c.max_records);
        c.max_physical = n("MAX_PHYSICAL", n("MAX_PHYSICAL_RECORDS", c.max_physical));
        c.budget.max_bytes = c.max_bytes;
        c.budget.max_nodes = n("MAX_NODES", c.budget.max_nodes);
        c.budget.max_depth = n("MAX_DEPTH", c.budget.max_depth);
        c.budget.max_candidates = n("MAX_CANDIDATES", c.budget.max_candidates);
        c.budget.max_nd = n("MAX_ND", c.budget.max_nd);
        c.budget.max_changes = n("MAX_CHANGES", c.budget.max_changes);
        c.budget.max_leaf_tokens = n("MAX_LEAF_TOKENS", c.budget.max_leaf_tokens);
        c.inflight_bytes = n(
            "INFLIGHT_BYTES",
            c.max_bytes
                .checked_mul(3)
                .and_then(|n| n.checked_mul(c.threads))
                .expect("DIFF default inflight bytes overflow"),
        );
        c.queue_ms = n("QUEUE_MS", c.queue_ms as usize) as u64;
        c.time_limit_ms = n("TIME_LIMIT_MS", c.time_limit_ms as usize) as u64;
        c.cache_bytes = n(
            "CACHE_BYTES",
            n("CACHE_MB", c.cache_bytes / (1024 * 1024))
                .checked_mul(1024 * 1024)
                .expect("DIFF cache bytes overflow"),
        );
        c
    }
}
#[derive(Default)]
struct Accounting {
    bytes: usize,
    requests: usize,
    admissions: Vec<Weak<Admission>>,
}
struct Shared {
    metrics: Option<crate::metrics::DiffMetrics>,
    accounting: Mutex<Accounting>,
    stopped: AtomicBool,
    max_bytes: usize,
    max_requests: usize,
}
pub struct Admission {
    pub cancel: CancelToken,
    pub deadline: Instant,
    bytes: usize,
    shared: Arc<Shared>,
}
impl Drop for Admission {
    fn drop(&mut self) {
        let mut a = self.shared.accounting.lock();
        a.bytes -= self.bytes;
        a.requests -= 1;
        if let Some(m) = &self.shared.metrics {
            m.inflight_bytes.sub(self.bytes as i64);
            m.inflight_requests.dec();
        }
    }
}
/// Keep one in the command future across capture, computation and publication.
/// A shard/worker closure separately owns an Arc<Admission> for memory accounting.
pub struct CancelOnDrop(Option<CancelToken>);
impl Admission {
    pub fn cancel_on_drop(&self) -> CancelOnDrop {
        CancelOnDrop(Some(self.cancel.clone()))
    }
}
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if let Some(cancel) = &self.0 {
            cancel.cancel();
        }
    }
}
type Job = Box<dyn FnOnce() + Send + 'static>;
pub struct DiffPool {
    shared: Arc<Shared>,
    sender: Option<crossbeam_channel::Sender<Job>>,
    workers: Vec<JoinHandle<()>>,
    queue_ms: u64,
    time_limit_ms: u64,
}
impl DiffPool {
    pub fn new(config: &DiffConfig) -> Self {
        Self::with_metrics(config, None)
    }
    pub fn with_metrics(config: &DiffConfig, metrics: Option<crate::metrics::DiffMetrics>) -> Self {
        assert!(config.threads > 0);
        let (tx, rx) = crossbeam_channel::bounded::<Job>(config.threads);
        let shared = Arc::new(Shared {
            metrics,
            accounting: Mutex::new(Accounting::default()),
            stopped: AtomicBool::new(false),
            max_bytes: config.inflight_bytes,
            max_requests: config.threads.saturating_mul(2),
        });
        let workers = (0..config.threads)
            .map(|i| {
                let rx = rx.clone();
                std::thread::Builder::new()
                    .name(format!("diff-{i}"))
                    .spawn(move || {
                        while let Ok(job) = rx.recv() {
                            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job));
                        }
                    })
                    .expect("start DIFF worker")
            })
            .collect();
        Self {
            shared,
            sender: Some(tx),
            workers,
            queue_ms: config.queue_ms,
            time_limit_ms: config.time_limit_ms,
        }
    }
    pub fn admit(&self, bytes: usize) -> Result<Arc<Admission>, Reply> {
        let mut a = self.shared.accounting.lock();
        if self.shared.stopped.load(Ordering::Acquire)
            || a.requests >= self.shared.max_requests
            || a.bytes
                .checked_add(bytes)
                .is_none_or(|n| n > self.shared.max_bytes)
        {
            if let Some(m) = &self.shared.metrics {
                m.rejections.with_label_values(&["admission"]).inc();
            }
            return Err(Reply::err(
                "DIFFBUSY request or in-flight byte capacity exhausted",
            ));
        }
        let deadline = Instant::now()
            .checked_add(Duration::from_millis(self.time_limit_ms))
            .ok_or_else(|| Reply::err("DIFFCONFIG deadline overflow"))?;
        let permit = Arc::new(Admission {
            cancel: CancelToken::with_deadline(deadline),
            deadline,
            bytes,
            shared: self.shared.clone(),
        });
        a.bytes += bytes;
        a.requests += 1;
        if let Some(m) = &self.shared.metrics {
            m.inflight_bytes.add(bytes as i64);
            m.inflight_requests.inc();
        }
        a.admissions.retain(|w| w.strong_count() > 0);
        a.admissions.push(Arc::downgrade(&permit));
        Ok(permit)
    }
    pub fn inflight_bytes(&self) -> usize {
        self.shared.accounting.lock().bytes
    }
    pub fn inflight_requests(&self) -> usize {
        self.shared.accounting.lock().requests
    }
    pub async fn run<
        T: Send + 'static,
        F: FnOnce(&CancelToken) -> Result<T, DiffError> + Send + 'static,
    >(
        &self,
        admission: Arc<Admission>,
        work: F,
    ) -> Result<T, Reply> {
        let deadline = admission.deadline;
        let mut cancellation = admission.cancel_on_drop();
        let queued = Instant::now();
        let queue_ms = self.queue_ms;
        let (tx, rx) = tokio::sync::oneshot::channel();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let metrics = self.shared.metrics.clone();
        let job: Job = Box::new(move || {
            let _ = started_tx.send(());
            if let Some(m) = &metrics {
                m.queue_seconds.observe(queued.elapsed().as_secs_f64());
            }
            let start = Instant::now();
            let result = if queued.elapsed() > Duration::from_millis(queue_ms) {
                Err(DiffError::Timeout { stage: "queue" })
            } else {
                admission
                    .cancel
                    .check()
                    .and_then(|()| work(&admission.cancel))
                    .and_then(|v| admission.cancel.check().map(|()| v))
            };
            if let Some(m) = &metrics {
                m.duration
                    .with_label_values(&["worker"])
                    .observe(start.elapsed().as_secs_f64());
            }
            drop(admission);
            let _ = tx.send(result);
        });
        self.sender
            .as_ref()
            .ok_or_else(|| Reply::err("DIFFBUSY worker pool stopped"))?
            .try_send(job)
            .map_err(|_| {
                if let Some(m) = &self.shared.metrics {
                    m.rejections.with_label_values(&["queue_full"]).inc();
                }
                Reply::err("DIFFBUSY worker queue full")
            })?;
        let queue_deadline = queued
            .checked_add(Duration::from_millis(queue_ms))
            .unwrap_or(deadline)
            .min(deadline);
        match tokio::time::timeout_at(queue_deadline.into(), started_rx).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => return Err(Reply::err("DIFFWORKER worker failed")),
            Err(_) => {
                if let Some(m) = &self.shared.metrics {
                    m.rejections.with_label_values(&["queue_timeout"]).inc();
                }
                return Err(Reply::err("DIFFTIMEOUT queue deadline"));
            }
        }
        let result = match tokio::time::timeout_at(deadline.into(), rx).await {
            Ok(Ok(result)) => result.map_err(diff_error),
            Ok(Err(_)) => Err(Reply::err("DIFFWORKER worker failed")),
            Err(_) => {
                if let Some(m) = &self.shared.metrics {
                    m.rejections.with_label_values(&["execution_timeout"]).inc();
                }
                Err(Reply::err("DIFFTIMEOUT execution deadline"))
            }
        };
        if result.is_ok() {
            cancellation.0 = None;
        }
        drop(cancellation);
        result
    }
}
impl Drop for DiffPool {
    fn drop(&mut self) {
        self.shared.stopped.store(true, Ordering::Release);
        // Upgraded admissions may become the final owner concurrently. Release
        // the accounting mutex before dropping them (Admission::drop locks it).
        let admissions: Vec<_> = self
            .shared
            .accounting
            .lock()
            .admissions
            .iter()
            .filter_map(Weak::upgrade)
            .collect();
        for admission in admissions {
            admission.cancel.cancel();
        }
        self.sender.take();
        for worker in self.workers.drain(..) {
            if worker.thread().id() != std::thread::current().id() {
                let _ = worker.join();
            }
        }
    }
}
pub fn diff_error(e: DiffError) -> Reply {
    super::error(e)
}

pub struct DiffState {
    pub metrics: crate::metrics::DiffMetrics,
    pub config: DiffConfig,
    pub pool: DiffPool,
    pub cache: super::cache::TreeCache,
}
impl Default for DiffState {
    fn default() -> Self {
        Self::new(&prometheus::Registry::new())
    }
}
impl DiffState {
    pub fn new(registry: &prometheus::Registry) -> Self {
        let config = DiffConfig::from_env();
        let metrics = crate::metrics::DiffMetrics::new(registry);
        let pool = DiffPool::with_metrics(&config, Some(metrics.clone()));
        let cache =
            super::cache::TreeCache::with_metrics(config.cache_bytes, Some(metrics.clone()));
        Self {
            metrics,
            config,
            pool,
            cache,
        }
    }
}
impl DiffState {
    pub fn stats(&self) -> Reply {
        use prometheus::core::Collector;
        let counters = |metric: &prometheus::IntCounterVec| {
            Reply::Array(
                metric
                    .collect()
                    .into_iter()
                    .flat_map(|family| {
                        family
                            .get_metric()
                            .iter()
                            .map(|metric| {
                                let mut row = metric
                                    .get_label()
                                    .iter()
                                    .flat_map(|label| {
                                        [
                                            Reply::bulk_str(label.name()),
                                            Reply::bulk_str(label.value()),
                                        ]
                                    })
                                    .collect::<Vec<_>>();
                                row.push(Reply::bulk_str("count"));
                                row.push(Reply::Int(metric.get_counter().get_value() as i64));
                                Reply::Array(row)
                            })
                            .collect::<Vec<_>>()
                    })
                    .collect(),
            )
        };
        Reply::Array(vec![
            Reply::bulk_str("inflight_bytes"),
            Reply::Int(self.pool.inflight_bytes() as i64),
            Reply::bulk_str("inflight_requests"),
            Reply::Int(self.pool.inflight_requests() as i64),
            Reply::bulk_str("cache_bytes"),
            Reply::Int(self.cache.bytes() as i64),
            Reply::bulk_str("cache_hits"),
            Reply::Int(self.cache.hits() as i64),
            Reply::bulk_str("operations"),
            counters(&self.metrics.operations),
            Reply::bulk_str("records"),
            counters(&self.metrics.records),
            Reply::bulk_str("rejections"),
            counters(&self.metrics.rejections),
            Reply::bulk_str("queue_wait_seconds"),
            Reply::Double(self.metrics.queue_seconds.get_sample_sum()),
            Reply::bulk_str("queue_samples"),
            Reply::Int(self.metrics.queue_seconds.get_sample_count() as i64),
        ])
    }
}

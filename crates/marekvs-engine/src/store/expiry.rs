//! Advisory, generation-checked TTL discovery. All polling and data writes run
//! on the owning shard. Iterator construction and one seek/next cannot be
//! preempted; the elapsed budget is checked between those storage operations.
use super::{bounded_iter, expiry_tombstone, scan_outcome, write_merged_checked, ShardCtx};
use marekvs_core::{
    envelope::Envelope,
    ikey::{self, Pid},
};
use prometheus::{Histogram, HistogramOpts, IntCounter, Registry};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct ExpiryMetrics {
    pub polls: IntCounter,
    pub due_partitions: IntCounter,
    pub discovery_partitions: IntCounter,
    pub iterator_opens: IntCounter,
    pub records_visited: IntCounter,
    pub partitions_completed: IntCounter,
    pub tombstones_written: IntCounter,
    pub incomplete_scans: IntCounter,
    pub poll_seconds: Histogram,
    pub iterator_seconds: Histogram,
}
impl ExpiryMetrics {
    fn new() -> Self {
        let counter = |name, help| IntCounter::new(name, help).expect("expiry metric");
        Self {
            due_partitions: counter(
                "marekvs_expiry_due_partitions_total",
                "Partition quanta scanned for a known due deadline",
            ),
            discovery_partitions: counter(
                "marekvs_expiry_discovery_partitions_total",
                "Partition quanta scanned for initial or invalidated discovery",
            ),
            polls: counter(
                "marekvs_expiry_passes_total",
                "Bounded expiry maintenance polls",
            ),
            iterator_opens: counter(
                "marekvs_expiry_iterators_total",
                "Partition iterators opened for expiry discovery",
            ),
            records_visited: counter(
                "marekvs_expiry_records_total",
                "Surfaced records visited by expiry discovery",
            ),
            partitions_completed: counter(
                "marekvs_expiry_partitions_completed_total",
                "Complete generation-stable partition discoveries",
            ),
            tombstones_written: counter(
                "marekvs_expiry_tombstones_total",
                "Successfully committed active-expiry tombstones",
            ),
            incomplete_scans: counter(
                "marekvs_expiry_scan_errors_total",
                "Failed expiry scans retried without publishing absence",
            ),
            poll_seconds: Histogram::with_opts(HistogramOpts::new(
                "marekvs_expiry_tick_seconds",
                "Actual bounded expiry poll duration",
            ))
            .unwrap(),
            iterator_seconds: Histogram::with_opts(HistogramOpts::new(
                "marekvs_expiry_iterator_seconds",
                "Actual iterator construction and scan quantum duration",
            ))
            .unwrap(),
        }
    }
    pub fn register(&self, registry: &Registry) {
        for counter in [
            &self.polls,
            &self.due_partitions,
            &self.discovery_partitions,
            &self.iterator_opens,
            &self.records_visited,
            &self.partitions_completed,
            &self.tombstones_written,
            &self.incomplete_scans,
        ] {
            registry
                .register(Box::new(counter.clone()))
                .expect("register expiry counter");
        }
        registry
            .register(Box::new(self.poll_seconds.clone()))
            .unwrap();
        registry
            .register(Box::new(self.iterator_seconds.clone()))
            .unwrap();
    }
}

pub struct MaintenanceState {
    generations: Vec<AtomicU64>,
    dirty: Vec<AtomicBool>,
    pub metrics: ExpiryMetrics,
    #[cfg(debug_assertions)]
    observer_barrier: parking_lot::Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}
impl Default for MaintenanceState {
    fn default() -> Self {
        Self::new()
    }
}
impl MaintenanceState {
    pub fn new() -> Self {
        Self {
            generations: (0..ikey::PARTITIONS).map(|_| AtomicU64::new(0)).collect(),
            dirty: (0..ikey::PARTITIONS)
                .map(|_| AtomicBool::new(true))
                .collect(),
            metrics: ExpiryMetrics::new(),
            #[cfg(debug_assertions)]
            observer_barrier: parking_lot::Mutex::new(None),
        }
    }
    /// Deterministic test rendezvous after visibility but before invalidation.
    #[cfg(debug_assertions)]
    #[doc(hidden)]
    pub fn set_observer_barrier_for_tests(&self, barrier: Option<Arc<dyn Fn() + Send + Sync>>) {
        *self.observer_barrier.lock() = barrier;
    }
    #[cfg(debug_assertions)]
    pub(super) fn before_observer(&self) {
        let barrier = self.observer_barrier.lock().clone();
        if let Some(barrier) = barrier {
            barrier();
        }
    }
    pub fn generation(&self, pid: Pid) -> u64 {
        self.generations[pid as usize].load(Ordering::Acquire)
    }
    /// Point observer and explicit successful range commits share this path.
    /// fetch_add is essential: hook invocation order is not commit order.
    pub fn invalidate(&self, pid: Pid) {
        if let Some(generation) = self.generations.get(pid as usize) {
            generation.fetch_add(1, Ordering::AcqRel);
            self.dirty[pid as usize].store(true, Ordering::Release);
        }
    }
}

#[derive(Debug, Default)]
pub struct ExpiryProgress {
    pub records_visited: usize,
    pub partitions_completed: usize,
    pub iterator_opens: usize,
    pub tombstones_written: usize,
    pub has_more_work: bool,
}
#[derive(Debug)]
pub struct ScanIncomplete {
    pub pid: Pid,
    pub source: super::ScanIncomplete,
}
impl std::fmt::Display for ScanIncomplete {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "partition {}: {}", self.pid, self.source)
    }
}
impl std::error::Error for ScanIncomplete {}

#[derive(Debug)]
enum Discovery {
    Unknown,
    Scanning {
        generation: u64,
        cursor: Vec<u8>,
        deadline_ms: u64,
    },
    NoTtl {
        generation: u64,
    },
    Due {
        generation: u64,
        deadline_ms: u64,
    },
}
struct Partition {
    pid: Pid,
    state: Discovery,
    retry_at: Option<Instant>,
}
pub struct ExpiryScheduler {
    shared: Arc<MaintenanceState>,
    partitions: Vec<Partition>,
    next: usize,
    urgent_next: usize,
    prefer_urgent: bool,
}
impl ExpiryScheduler {
    pub fn new(shared: Arc<MaintenanceState>, shard_index: usize, shard_count: usize) -> Self {
        assert!(shard_count > 0 && shard_index < shard_count);
        Self {
            shared,
            partitions: (shard_index..ikey::PARTITIONS as usize)
                .step_by(shard_count)
                .map(|pid| Partition {
                    pid: pid as Pid,
                    state: Discovery::Unknown,
                    retry_at: None,
                })
                .collect(),
            next: 0,
            urgent_next: 0,
            prefer_urgent: true,
        }
    }
    pub fn invalidate(&mut self, pid: Pid) {
        self.shared.invalidate(pid);
    }
    pub fn next_wait(&self, now_ms: u64) -> Duration {
        let mut wait = Duration::from_secs(1);
        for p in &self.partitions {
            let generation = self.shared.generation(p.pid);
            let armed = match p.state {
                Discovery::NoTtl { generation: g } if g == generation => continue,
                Discovery::Due {
                    generation: g,
                    deadline_ms,
                } if g == generation => Duration::from_millis(deadline_ms.saturating_sub(now_ms)),
                _ => Duration::from_millis(100),
            };
            let armed = p.retry_at.map_or(armed, |at| {
                armed.max(at.saturating_duration_since(Instant::now()))
            });
            wait = wait.min(armed);
        }
        wait
    }
    pub fn poll(
        &mut self,
        ctx: &ShardCtx,
        now_ms: u64,
        record_budget: usize,
        partition_budget: usize,
        elapsed_budget: Duration,
    ) -> Result<ExpiryProgress, ScanIncomplete> {
        let metrics = self.shared.metrics.clone();
        metrics.polls.inc();
        let _timer = metrics.poll_seconds.start_timer();
        let started = Instant::now();
        let mut progress = ExpiryProgress::default();
        let mut transitions = 0;
        let mut failure = None;
        // Due deadlines and mutations of parked partitions must not wait for
        // the initial 4096-partition discovery rotation. Rotate fairly within
        // each class, visiting each partition at most once in this poll.
        let mut urgent = Vec::new();
        let mut ordinary = Vec::new();
        for offset in 0..self.partitions.len() {
            let i = (self.next + offset) % self.partitions.len();
            let p = &self.partitions[i];
            if p.retry_at.is_some_and(|at| at > Instant::now()) {
                continue;
            }
            let generation = self.shared.generation(p.pid);
            let priority = match p.state {
                Discovery::Due {
                    generation: g,
                    deadline_ms,
                } => g != generation || deadline_ms <= now_ms,
                Discovery::NoTtl { generation: g } => g != generation,
                _ => false,
            };
            if priority {
                urgent.push(i);
            } else if matches!(p.state, Discovery::Unknown | Discovery::Scanning { .. }) {
                ordinary.push(i);
            }
        }
        urgent.sort_by_key(|i| {
            (i + self.partitions.len() - self.urgent_next) % self.partitions.len()
        });
        let mut urgent = urgent.into_iter().peekable();
        let mut ordinary = ordinary.into_iter().peekable();
        while transitions < partition_budget
            && progress.records_visited < record_budget
            && (transitions == 0 || started.elapsed() < elapsed_budget)
        {
            let use_urgent =
                (self.prefer_urgent && urgent.peek().is_some()) || ordinary.peek().is_none();
            let Some(i) = (if use_urgent {
                urgent.next()
            } else {
                ordinary.next()
            }) else {
                break;
            };
            self.prefer_urgent = !use_urgent;
            if use_urgent {
                self.urgent_next = (i + 1) % self.partitions.len();
            } else {
                self.next = (i + 1) % self.partitions.len();
            }
            let p = &mut self.partitions[i];
            if p.retry_at.is_some_and(|at| at > Instant::now()) {
                continue;
            }
            let generation = self.shared.generation(p.pid);
            let due = matches!(p.state, Discovery::Due { generation: g, deadline_ms } if g == generation && deadline_ms <= now_ms);
            match &p.state {
                Discovery::NoTtl { generation: g } if *g == generation => continue,
                Discovery::Due {
                    generation: g,
                    deadline_ms,
                } if *g == generation && *deadline_ms > now_ms => continue,
                Discovery::Scanning { generation: g, .. } if *g == generation => {}
                _ => {
                    p.state = Discovery::Scanning {
                        generation,
                        cursor: Vec::new(),
                        deadline_ms: 0,
                    }
                }
            }
            // Safe because this is the only shard that can write this pid.
            // Generation checks remain authoritative, so even a late dirty
            // store cannot erase or manufacture a stable proof.
            self.shared.dirty[p.pid as usize].swap(false, Ordering::AcqRel);
            transitions += 1;
            if due {
                metrics.due_partitions.inc();
            } else {
                metrics.discovery_partitions.inc();
            }
            let Discovery::Scanning {
                cursor,
                deadline_ms,
                generation: scan_generation,
            } = &mut p.state
            else {
                unreachable!()
            };
            let iterator_timer = metrics.iterator_seconds.start_timer();
            let txn = ctx.db.begin();
            let lower = p.pid.to_be_bytes();
            let upper = (p.pid + 1).to_be_bytes();
            let mut it = bounded_iter(&txn, ctx, &lower, Some(&upper));
            progress.iterator_opens += 1;
            metrics.iterator_opens.inc();
            if cursor.is_empty() {
                it.seek_to_first();
            } else {
                it.seek(cursor);
            }
            let mut expected_generation = generation;
            let mut write_error = None;
            let mut quantum = 0;
            while it.valid()
                && progress.records_visited < record_budget
                && quantum < 16
                && (quantum == 0 || started.elapsed() < elapsed_budget)
            {
                if let Some(parsed) = ikey::parse(it.key()) {
                    debug_assert_eq!(parsed.pid, p.pid);
                    if parsed.tag != ikey::Tag::Budget as u8 {
                        if let Some((env, pay)) = Envelope::decode(it.value()) {
                            if !env.is_tombstone() && env.ttl_deadline_ms > 0 {
                                if env.is_expired(now_ms) {
                                    // Commit before advancing the cursor. The
                                    // next budget check occurs after this one
                                    // non-preemptible record, never after an
                                    // unbounded drain of synchronous commits.
                                    let tombstone = expiry_tombstone(&env, pay);
                                    match write_merged_checked(ctx, it.key(), &tombstone) {
                                        Ok(true) => {
                                            progress.tombstones_written += 1;
                                            metrics.tombstones_written.inc();
                                            expected_generation =
                                                expected_generation.wrapping_add(1);
                                        }
                                        Ok(false) => {}
                                        Err(error) => {
                                            write_error =
                                                Some(super::ScanIncomplete(error.to_string()));
                                        }
                                    }
                                } else if *deadline_ms == 0 || env.ttl_deadline_ms < *deadline_ms {
                                    *deadline_ms = env.ttl_deadline_ms;
                                }
                            }
                        }
                    }
                }
                quantum += 1;
                progress.records_visited += 1;
                metrics.records_visited.inc();
                if write_error.is_some() {
                    break;
                }
                it.next();
            }
            let outcome = scan_outcome(&it);
            let complete = !it.valid();
            let next_cursor = if complete {
                Vec::new()
            } else {
                it.key().to_vec()
            };
            drop(it);
            drop(txn);
            drop(iterator_timer);
            let outcome = if let Some(error) = write_error {
                Err(error)
            } else {
                outcome
            };
            // The only intervening writes on this owning shard are the
            // successful one-record commits above. Account for each exact
            // observer increment; any other generation change discards proof.
            *scan_generation = expected_generation;
            match outcome {
                Err(source) => {
                    p.state = Discovery::Unknown;
                    p.retry_at = Some(Instant::now() + Duration::from_millis(100));
                    metrics.incomplete_scans.inc();
                    failure = Some(ScanIncomplete { pid: p.pid, source });
                }
                Ok(()) if expected_generation != self.shared.generation(p.pid) => {
                    p.state = Discovery::Unknown
                }
                Ok(()) if complete => {
                    let deadline = *deadline_ms;
                    p.state = if deadline == 0 {
                        Discovery::NoTtl {
                            generation: expected_generation,
                        }
                    } else {
                        Discovery::Due {
                            generation: expected_generation,
                            deadline_ms: deadline,
                        }
                    };
                    p.retry_at = None;
                    progress.partitions_completed += 1;
                    metrics.partitions_completed.inc();
                }
                Ok(()) => *cursor = next_cursor,
            }
        }
        progress.has_more_work = self.partitions.iter().any(|p| match p.state {
            Discovery::NoTtl { generation } => generation != self.shared.generation(p.pid),
            Discovery::Due {
                generation,
                deadline_ms,
            } => generation != self.shared.generation(p.pid) || deadline_ms <= now_ms,
            _ => true,
        });
        if let Some(error) = failure {
            Err(error)
        } else {
            Ok(progress)
        }
    }
}

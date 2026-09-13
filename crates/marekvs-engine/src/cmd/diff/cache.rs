//! Semantic-only LRU: source record identity is never cached.
use marekvs_diff::{Sid, Tree};
use parking_lot::Mutex;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
struct Entry {
    sid: Sid,
    tree: Arc<Tree>,
    bytes: usize,
}
pub struct TreeCache {
    metrics: Option<crate::metrics::DiffMetrics>,
    limit: usize,
    entries: Mutex<(VecDeque<Entry>, usize)>,
    hits: AtomicU64,
}
impl TreeCache {
    pub fn new(limit: usize) -> Self {
        Self::with_metrics(limit, None)
    }
    pub fn with_metrics(limit: usize, metrics: Option<crate::metrics::DiffMetrics>) -> Self {
        Self {
            metrics,
            limit,
            entries: Mutex::new((VecDeque::new(), 0)),
            hits: AtomicU64::new(0),
        }
    }
    pub fn get(&self, sid: Sid) -> Option<Arc<Tree>> {
        let mut inner = self.entries.lock();
        let i = inner.0.iter().position(|e| e.sid == sid)?;
        let entry = inner.0.remove(i)?;
        let tree = entry.tree.clone();
        inner.0.push_back(entry);
        self.hits.fetch_add(1, Ordering::Relaxed);
        if let Some(m) = &self.metrics {
            m.cache_hits.inc();
        }
        Some(tree)
    }
    pub fn insert(&self, tree: Arc<Tree>) {
        let bytes = tree
            .approx_bytes()
            .saturating_add(tree.to_json().to_string().len().saturating_mul(8))
            .saturating_add(256);
        if bytes > self.limit {
            return;
        }
        let mut semantic = (*tree).clone();
        for n in &mut semantic.nodes {
            n.eid = None;
        }
        let mut inner = self.entries.lock();
        if let Some(i) = inner.0.iter().position(|e| e.sid == tree.sid) {
            let old = inner.0.remove(i).unwrap();
            inner.1 -= old.bytes;
        }
        while inner.1.saturating_add(bytes) > self.limit {
            if let Some(old) = inner.0.pop_front() {
                inner.1 -= old.bytes;
            } else {
                return;
            }
        }
        inner.1 += bytes;
        if let Some(m) = &self.metrics {
            m.cache_bytes.set(inner.1 as i64);
        }
        inner.0.push_back(Entry {
            sid: tree.sid,
            tree: Arc::new(semantic),
            bytes,
        });
    }
    pub fn put(&self, sid: Sid, tree: Arc<Tree>) {
        if sid == tree.sid {
            self.insert(tree);
        }
    }
    pub fn bytes(&self) -> usize {
        self.entries.lock().1
    }
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }
}

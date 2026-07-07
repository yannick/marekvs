//! Protobuf schema registry + typed values (design/17).
//!
//! * `registry` — hidden replicated system records under `\x00proto:*`
//!   (SCRIPT LOAD precedent): per-version schema records, latest pointers,
//!   the name→version index hash and the prefix-binding hash.
//! * `compile` — protox compilation of `.proto` source (always call from
//!   `tokio::task::spawn_blocking`, never on shard threads) and
//!   FileDescriptorSet validation.
//! * `path` — dot-path field access over prost-reflect `DynamicMessage`s.
//! * `err` — the raw-code error surface (NOSCHEMA/SCHEMAERR/…).

pub mod compile;
pub mod err;
pub mod fields;
pub mod path;
pub mod registry;

use prost_reflect::DescriptorPool;

pub use err::ProtoErr;

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// DoS bounds on schema/value inputs (all env-tunable; defaults keep every
/// stored record comfortably under the 8 MiB MAX_FRAME).
#[derive(Debug, Clone, Copy)]
pub struct ProtoLimits {
    /// Max `.proto` source size per file (bytes).
    pub max_source: usize,
    /// Max compiled FileDescriptorSet size (bytes).
    pub max_fds: usize,
    /// Max encoded message value size (bytes).
    pub max_value: usize,
    /// Max files in one compilation unit (main + transitive imports).
    pub max_files: usize,
    /// Max import chain depth.
    pub max_depth: usize,
}

impl ProtoLimits {
    pub fn from_env() -> Self {
        Self {
            max_source: env_u64("MAREKVS_PROTO_MAX_SOURCE", 1024 * 1024) as usize,
            max_fds: env_u64("MAREKVS_PROTO_MAX_FDS", 4 * 1024 * 1024) as usize,
            max_value: env_u64("MAREKVS_PROTO_MAX_VALUE", 4 * 1024 * 1024) as usize,
            max_files: env_u64("MAREKVS_PROTO_MAX_FILES", 64) as usize,
            max_depth: env_u64("MAREKVS_PROTO_MAX_DEPTH", 16) as usize,
        }
    }
}

/// Hand-rolled LRU for compiled descriptor pools, keyed by
/// `(schema, version)`. Entries are IMMUTABLE (per-version schema records
/// never change), so there is no invalidation — only capacity eviction.
/// `DescriptorPool` is Arc'd internally: clones are cheap and Send+Sync.
pub struct PoolLru {
    cap: usize,
    map: std::collections::HashMap<(String, u32), DescriptorPool>,
    order: std::collections::VecDeque<(String, u32)>,
}

impl PoolLru {
    pub fn new(cap: usize) -> Self {
        Self {
            cap: cap.max(1),
            map: std::collections::HashMap::new(),
            order: std::collections::VecDeque::new(),
        }
    }

    pub fn get(&mut self, schema: &str, version: u32) -> Option<DescriptorPool> {
        let k = (schema.to_string(), version);
        let hit = self.map.get(&k).cloned();
        if hit.is_some() {
            // refresh recency
            if let Some(pos) = self.order.iter().position(|e| *e == k) {
                self.order.remove(pos);
                self.order.push_back(k);
            }
        }
        hit
    }

    pub fn put(&mut self, schema: String, version: u32, pool: DescriptorPool) {
        let k = (schema, version);
        if self.map.insert(k.clone(), pool).is_none() {
            self.order.push_back(k);
        }
        while self.order.len() > self.cap {
            if let Some(old) = self.order.pop_front() {
                self.map.remove(&old);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

/// Node-local snapshot of the binding hash, refreshed at most every
/// `ttl_ms` (immediately on local BIND/UNBIND). Staleness ≤ TTL on remote
/// nodes is a documented AP caveat.
#[derive(Default)]
pub struct BindCache {
    pub loaded_ms: u64,
    /// (prefix, binding), sorted by prefix length DESC so the first match
    /// during a scan is the longest-prefix match.
    pub entries: Vec<(Vec<u8>, registry::BindingRecord)>,
}

/// Per-engine proto state: limits + descriptor-pool LRU + binding cache.
pub struct ProtoState {
    pub limits: ProtoLimits,
    pub pools: parking_lot::Mutex<PoolLru>,
    pub bindings: parking_lot::Mutex<Option<BindCache>>,
    /// Binding-cache TTL in ms.
    pub bind_ttl_ms: u64,
}

impl ProtoState {
    pub fn from_env() -> Self {
        Self {
            limits: ProtoLimits::from_env(),
            pools: parking_lot::Mutex::new(PoolLru::new(
                env_u64("MAREKVS_PROTO_POOL_CACHE", 128) as usize
            )),
            bindings: parking_lot::Mutex::new(None),
            bind_ttl_ms: env_u64("MAREKVS_PROTO_BIND_TTL_MS", 2000),
        }
    }

    /// Drop the binding cache (local BIND/UNBIND: next lookup re-reads).
    pub fn invalidate_bindings(&self) {
        *self.bindings.lock() = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_lru_evicts_oldest() {
        let mut lru = PoolLru::new(2);
        let pool = DescriptorPool::new();
        lru.put("a".into(), 1, pool.clone());
        lru.put("b".into(), 1, pool.clone());
        assert!(lru.get("a", 1).is_some()); // refresh a → b is now oldest
        lru.put("c".into(), 1, pool.clone());
        assert_eq!(lru.len(), 2);
        assert!(lru.get("b", 1).is_none(), "b must have been evicted");
        assert!(lru.get("a", 1).is_some());
        assert!(lru.get("c", 1).is_some());
    }

    #[test]
    fn limits_defaults() {
        let l = ProtoLimits::from_env();
        assert_eq!(l.max_source, 1024 * 1024);
        assert_eq!(l.max_fds, 4 * 1024 * 1024);
        assert_eq!(l.max_value, 4 * 1024 * 1024);
        assert_eq!(l.max_files, 64);
        assert_eq!(l.max_depth, 16);
    }
}

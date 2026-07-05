//! Merkle anti-entropy over partitions (design/05 §Layer 2).
//!
//! 256 buckets per partition: `bucket = xxh3(ikey) & 0xFF`. A bucket digest
//! is the XOR-fold of `xxh3(ikey ‖ hlc_be)` over its records — commutative,
//! so it needs no sorting and updates incrementally. The root hashes the 256
//! digests. Only stored bytes matter: version equality ⇒ value equality
//! (envelopes are stamped once at the origin). The local-only zset score
//! index (tag 'Z') is excluded.

use std::sync::Arc;

use marekvs_core::envelope::Envelope;
use marekvs_core::ikey::{self, Pid};
use marekvs_engine::store::{self, Store};
use marekvs_proto::ReplOp;
use xxhash_rust::xxh3::xxh3_64;

pub const BUCKETS: usize = 256;

fn entry_hash(ikey: &[u8], hlc: u64, vhash: u64) -> u64 {
    let mut buf = Vec::with_capacity(ikey.len() + 16);
    buf.extend_from_slice(ikey);
    buf.extend_from_slice(&hlc.to_be_bytes());
    buf.extend_from_slice(&vhash.to_be_bytes());
    xxh3_64(&buf)
}

fn record_hlc(value: &[u8]) -> u64 {
    Envelope::decode(value).map_or(0, |(e, _)| e.hlc)
}

/// Content hash of the whole stored record. Digests and bucket entries must
/// be CONTENT-aware, not just version-aware: merged CRDT records (PN
/// counters, HLL registers) can hold DIFFERENT slot sets under the SAME
/// envelope version (= symmetric max), so an (ikey, hlc)-only digest calls
/// two divergent replicas identical and Merkle repair never fires (chaos
/// clock_bump finding: nodes stuck at different counter values forever).
fn record_vhash(value: &[u8]) -> u64 {
    xxh3_64(value)
}

/// XOR-fold digests of all 256 buckets of a partition.
pub async fn bucket_digests(store: &Arc<Store>, pid: Pid) -> Vec<u64> {
    store
        .run(pid, move |ctx| {
            let mut digests = vec![0u64; BUCKETS];
            store::scan_prefix(ctx, &ikey::partition_prefix(pid), |k, v| {
                if matches!(ikey::parse(k), Some(p) if p.tag == b'Z') {
                    return true;
                }
                let bucket = (xxh3_64(k) & 0xFF) as usize;
                digests[bucket] ^= entry_hash(k, record_hlc(v), record_vhash(v));
                true
            });
            digests
        })
        .await
}

pub async fn partition_root(store: &Arc<Store>, pid: Pid) -> u64 {
    let digests = bucket_digests(store, pid).await;
    // 0 is the documented "no visible records" sentinel (stranded-AE and
    // the rejoin scope rely on it). xxh3 over 256 zero digests is a NONZERO
    // constant, so without this check every empty partition looked
    // data-bearing: stranded-AE probed every non-owned pid each round and
    // a gc_grace rejoin scoped ~all owned pids instead of the few with
    // data. (An XOR-cancelling non-empty bucket set would also hash to the
    // sentinel — that needs colliding entry hash pairs; the consequence is
    // a skipped offer, healed by owner-AE.)
    if digests.iter().all(|d| *d == 0) {
        return 0;
    }
    let mut bytes = Vec::with_capacity(BUCKETS * 8);
    for d in digests {
        bytes.extend_from_slice(&d.to_be_bytes());
    }
    xxh3_64(&bytes)
}

/// (ikey_hash, hlc, value_hash) for every record in one bucket.
pub async fn bucket_entries(store: &Arc<Store>, pid: Pid, bucket: u8) -> Vec<(u64, u64, u64)> {
    store
        .run(pid, move |ctx| {
            let mut entries = Vec::new();
            store::scan_prefix(ctx, &ikey::partition_prefix(pid), |k, v| {
                if matches!(ikey::parse(k), Some(p) if p.tag == b'Z') {
                    return true;
                }
                if (xxh3_64(k) & 0xFF) as u8 == bucket {
                    entries.push((xxh3_64(k), record_hlc(v), record_vhash(v)));
                }
                true
            });
            entries
        })
        .await
}

/// Compare a peer's bucket entries against ours.
/// Returns (records to push to the peer, ikey-hashes we want from the peer).
pub async fn diff_bucket(
    store: &Arc<Store>,
    pid: Pid,
    bucket: u8,
    theirs: &[(u64, u64, u64)],
) -> (Vec<ReplOp>, Vec<u64>) {
    let theirs: std::collections::HashMap<u64, (u64, u64)> = theirs
        .iter()
        .map(|(h, hlc, vh)| (*h, (*hlc, *vh)))
        .collect();
    store
        .run(pid, move |ctx| {
            let mut push = Vec::new();
            let mut mine = std::collections::HashMap::new();
            store::scan_prefix(ctx, &ikey::partition_prefix(pid), |k, v| {
                if matches!(ikey::parse(k), Some(p) if p.tag == b'Z') {
                    return true;
                }
                if (xxh3_64(k) & 0xFF) as u8 != bucket {
                    return true;
                }
                let h = xxh3_64(k);
                let hlc = record_hlc(v);
                let vh = record_vhash(v);
                mine.insert(h, (hlc, vh));
                // Push when we are strictly newer, OR same version but
                // different content (divergent merged CRDT state — both
                // sides push, both merge, digests converge).
                let send = match theirs.get(&h) {
                    None => true,
                    Some((thlc, tvh)) => hlc > *thlc || (hlc == *thlc && vh != *tvh),
                };
                if send {
                    push.push(ReplOp {
                        ikey: k.to_vec(),
                        value: v.to_vec(),
                    });
                }
                true
            });
            let want: Vec<u64> = theirs
                .iter()
                .filter(|(h, (thlc, tvh))| {
                    mine.get(*h)
                        .is_none_or(|(mhlc, mvh)| mhlc < thlc || (mhlc == thlc && mvh != tvh))
                })
                .map(|(h, _)| *h)
                .collect();
            (push, want)
        })
        .await
}

/// Full records whose xxh3(ikey) is in `hashes` (RequestKeys handler).
pub async fn records_by_hash(
    store: &Arc<Store>,
    pid: Pid,
    bucket: u8,
    hashes: &[u64],
) -> Vec<ReplOp> {
    let wanted: std::collections::HashSet<u64> = hashes.iter().copied().collect();
    store
        .run(pid, move |ctx| {
            let mut ops = Vec::new();
            store::scan_prefix(ctx, &ikey::partition_prefix(pid), |k, v| {
                if matches!(ikey::parse(k), Some(p) if p.tag == b'Z') {
                    return true;
                }
                if (xxh3_64(k) & 0xFF) as u8 == bucket && wanted.contains(&xxh3_64(k)) {
                    ops.push(ReplOp {
                        ikey: k.to_vec(),
                        value: v.to_vec(),
                    });
                }
                true
            });
            ops
        })
        .await
}

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

fn entry_hash(ikey: &[u8], hlc: u64) -> u64 {
    let mut buf = Vec::with_capacity(ikey.len() + 8);
    buf.extend_from_slice(ikey);
    buf.extend_from_slice(&hlc.to_be_bytes());
    xxh3_64(&buf)
}

fn record_hlc(value: &[u8]) -> u64 {
    Envelope::decode(value).map_or(0, |(e, _)| e.hlc)
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
                digests[bucket] ^= entry_hash(k, record_hlc(v));
                true
            });
            digests
        })
        .await
}

pub async fn partition_root(store: &Arc<Store>, pid: Pid) -> u64 {
    let digests = bucket_digests(store, pid).await;
    let mut bytes = Vec::with_capacity(BUCKETS * 8);
    for d in digests {
        bytes.extend_from_slice(&d.to_be_bytes());
    }
    xxh3_64(&bytes)
}

/// (ikey_hash, hlc) for every record in one bucket.
pub async fn bucket_entries(store: &Arc<Store>, pid: Pid, bucket: u8) -> Vec<(u64, u64)> {
    store
        .run(pid, move |ctx| {
            let mut entries = Vec::new();
            store::scan_prefix(ctx, &ikey::partition_prefix(pid), |k, v| {
                if matches!(ikey::parse(k), Some(p) if p.tag == b'Z') {
                    return true;
                }
                if (xxh3_64(k) & 0xFF) as u8 == bucket {
                    entries.push((xxh3_64(k), record_hlc(v)));
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
    theirs: &[(u64, u64)],
) -> (Vec<ReplOp>, Vec<u64>) {
    let theirs: std::collections::HashMap<u64, u64> = theirs.iter().copied().collect();
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
                mine.insert(h, hlc);
                match theirs.get(&h) {
                    None => push.push(ReplOp {
                        ikey: k.to_vec(),
                        value: v.to_vec(),
                    }),
                    Some(their_hlc) if hlc > *their_hlc => push.push(ReplOp {
                        ikey: k.to_vec(),
                        value: v.to_vec(),
                    }),
                    _ => {}
                }
                true
            });
            let want: Vec<u64> = theirs
                .iter()
                .filter(|(h, their_hlc)| mine.get(*h).is_none_or(|m| m < their_hlc))
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

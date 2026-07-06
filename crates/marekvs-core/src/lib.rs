//! marekvs-core — pure data-model layer: partitioning, hybrid logical clocks,
//! record envelopes, internal key layouts, and the convergent merge rules.
//!
//! Byte layouts are specified in design/02-data-model.md. Everything here is
//! I/O-free and deterministic so the merge laws can be property-tested.

pub mod budget;
pub mod counter;
pub mod envelope;
pub mod hlc;
pub mod ikey;
pub mod merge;
pub mod score;

pub use envelope::{Envelope, RecordType, COLLECTION_HEAD, ENVELOPE_LEN, TOMBSTONE};
pub use hlc::{hlc_pack, hlc_phys_ms, Hlc};
pub use ikey::{Pid, Tag, PARTITIONS};
pub use merge::{merge_values, MergeOutcome};

/// Cluster-unique node identifier (StatefulSet pod ordinal).
pub type NodeId = u16;

/// Partition of a user key: top 12 bits of xxh3_64 over the key's hash
/// slice.
///
/// Redis Cluster hash tags: when the key contains `{...}` with non-empty
/// content, ONLY that content is hashed — `rate:{user1}:count` and
/// `rate:{user1}:window` land on the same partition (and therefore the
/// same shard thread), which is what makes multi-key atomic Lua scripts
/// and co-located MULTI possible (design/11). Rule matches Redis exactly:
/// first `{`, then the first `}` AFTER it; empty `{}` hashes the whole key.
pub fn pid_of(userkey: &[u8]) -> Pid {
    (xxhash_rust::xxh3::xxh3_64(hash_slice(userkey)) >> 52) as Pid
}

fn hash_slice(key: &[u8]) -> &[u8] {
    if let Some(open) = key.iter().position(|&b| b == b'{') {
        if let Some(close_rel) = key[open + 1..].iter().position(|&b| b == b'}') {
            if close_rel > 0 {
                return &key[open + 1..open + 1 + close_rel];
            }
        }
    }
    key
}

#[cfg(test)]
mod hash_tag_tests {
    use super::*;

    #[test]
    fn hash_tags_colocate() {
        assert_eq!(
            pid_of(b"rate:{user1}:count"),
            pid_of(b"rate:{user1}:window")
        );
        assert_eq!(pid_of(b"{user1}"), pid_of(b"x{user1}y"));
        // Empty braces / no closing brace: whole key hashes (Redis rule).
        assert_ne!(pid_of(b"a{}b"), pid_of(b"c{}d"));
        assert_ne!(pid_of(b"a{open"), pid_of(b"b{open"));
        // First { and first } after it: "{a}{b}" hashes "a".
        assert_eq!(pid_of(b"{a}{b}"), pid_of(b"{a}{zzz}"));
    }
}

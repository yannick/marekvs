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
pub mod json;
pub mod merge;
pub mod pdoc;
pub mod protohead;
pub mod score;

pub use envelope::{Envelope, RecordType, COLLECTION_HEAD, ENVELOPE_LEN, TOMBSTONE};
pub use hlc::{hlc_pack, hlc_phys_ms, Hlc};
pub use ikey::{Pid, Tag, PARTITIONS};
pub use merge::{merge_values, MergeOutcome};

/// Cluster-unique node identifier (StatefulSet pod ordinal).
pub type NodeId = u16;

/// Redis-Cluster CRC16 (XMODEM: poly 0x1021, init 0), table-driven.
/// Must stay bit-identical to redis `crc16.c` — cluster clients compute
/// slots with it on their side.
const CRC16_TAB: [u16; 256] = {
    let mut tab = [0u16; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = (i as u16) << 8;
        let mut j = 0;
        while j < 8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
            j += 1;
        }
        tab[i] = crc;
        i += 1;
    }
    tab
};

pub fn crc16(data: &[u8]) -> u16 {
    let mut crc = 0u16;
    for &b in data {
        crc = (crc << 8) ^ CRC16_TAB[(((crc >> 8) ^ b as u16) & 0xFF) as usize];
    }
    crc
}

/// Redis Cluster slot of a user key: `crc16(hash_slice(key)) % 16384`.
/// Identical to redis so cluster-aware clients route to the right node
/// (design/15).
pub fn slot_of(userkey: &[u8]) -> u16 {
    crc16(hash_slice(userkey)) % 16384
}

/// Partition of a user key: its Redis Cluster slot group — 4 consecutive
/// slots per pid (16384 slots / 4096 partitions), so every pid is the
/// contiguous slot range `[pid*4, pid*4+3]` and `CLUSTER SLOTS` can report
/// exact ranges (design/15).
///
/// Redis Cluster hash tags: when the key contains `{...}` with non-empty
/// content, ONLY that content is hashed — `rate:{user1}:count` and
/// `rate:{user1}:window` land on the same partition (and therefore the
/// same shard thread), which is what makes multi-key atomic Lua scripts
/// and co-located MULTI possible (design/11). Rule matches Redis exactly:
/// first `{`, then the first `}` AFTER it; empty `{}` hashes the whole key.
pub fn pid_of(userkey: &[u8]) -> Pid {
    slot_of(userkey) >> 2
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
mod partition_tests {
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

    #[test]
    fn crc16_xmodem_vector() {
        assert_eq!(crc16(b"123456789"), 0x31C3);
        assert_eq!(crc16(b""), 0);
    }

    #[test]
    fn redis_known_slots() {
        // Vectors from real redis-server CLUSTER KEYSLOT.
        assert_eq!(slot_of(b"foo"), 12182);
        assert_eq!(slot_of(b"bar"), 5061);
        assert_eq!(slot_of(b"hello"), 866);
        // Hash tag: slot of the tag content only.
        assert_eq!(slot_of(b"{user1000}.following"), slot_of(b"user1000"));
    }

    #[test]
    fn pid_is_slot_group() {
        for key in [b"foo".as_slice(), b"bar", b"hello", b"{tag}x"] {
            assert_eq!(pid_of(key), slot_of(key) >> 2);
        }
        assert!(u32::from(slot_of(b"anything")) < 16384);
    }
}

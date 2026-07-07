//! The 19-byte record envelope prefixed to every stored value.
//!
//! ```text
//! offset size field
//! 0      1    flags: bit0 tombstone, bit1 collection-head, bits 2..4 type
//! 1      8    hlc  (big-endian)
//! 9      2    origin NodeId (big-endian)
//! 11     8    ttl_deadline_ms, absolute wall ms; 0 = no TTL (big-endian)
//! 19     …    payload
//! ```

use crate::NodeId;

pub const ENVELOPE_LEN: usize = 19;
pub const TOMBSTONE: u8 = 0b0000_0001;
pub const COLLECTION_HEAD: u8 = 0b0000_0010;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordType {
    String = 0,
    HashField = 1,
    SetMember = 2,
    ZsetMember = 3,
    List = 4,
    StreamEntry = 5,
    /// PN-counter (v1.1): hybrid base-register + per-node delta slots.
    Counter = 6,
    /// One HyperLogLog register (bucket → max rank); payload = 1 byte.
    /// Merge is payload max — the last free slot in the 3-bit type field.
    HllRegister = 7,
}

impl RecordType {
    pub fn from_bits(b: u8) -> RecordType {
        match b {
            1 => RecordType::HashField,
            2 => RecordType::SetMember,
            3 => RecordType::ZsetMember,
            4 => RecordType::List,
            5 => RecordType::StreamEntry,
            6 => RecordType::Counter,
            7 => RecordType::HllRegister,
            _ => RecordType::String,
        }
    }

    /// Element types use dot-based observed-remove merge; the rest are LWW.
    pub fn is_or_element(self) -> bool {
        matches!(
            self,
            RecordType::HashField | RecordType::SetMember | RecordType::ZsetMember
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Envelope {
    pub flags: u8,
    pub hlc: u64,
    pub origin: NodeId,
    pub ttl_deadline_ms: u64,
}

impl Envelope {
    pub fn new(rtype: RecordType, hlc: u64, origin: NodeId) -> Self {
        Self {
            flags: (rtype as u8) << 2,
            hlc,
            origin,
            ttl_deadline_ms: 0,
        }
    }

    pub fn tombstone(rtype: RecordType, hlc: u64, origin: NodeId) -> Self {
        Self {
            flags: ((rtype as u8) << 2) | TOMBSTONE,
            hlc,
            origin,
            ttl_deadline_ms: 0,
        }
    }

    pub fn head(hlc: u64, origin: NodeId) -> Self {
        Self {
            flags: COLLECTION_HEAD,
            hlc,
            origin,
            ttl_deadline_ms: 0,
        }
    }

    pub fn with_ttl(mut self, deadline_ms: u64) -> Self {
        self.ttl_deadline_ms = deadline_ms;
        self
    }

    pub fn is_tombstone(&self) -> bool {
        self.flags & TOMBSTONE != 0
    }

    pub fn is_head(&self) -> bool {
        self.flags & COLLECTION_HEAD != 0
    }

    pub fn rtype(&self) -> RecordType {
        RecordType::from_bits((self.flags >> 2) & 0b111)
    }

    /// LWW total order: `(hlc, origin)`.
    pub fn version(&self) -> (u64, NodeId) {
        (self.hlc, self.origin)
    }

    /// TTL evaluated locally; expiry acts as an implicit tombstone whose HLC
    /// is `deadline << 16` (design/05 "TTL convergence").
    pub fn is_expired(&self, now_ms: u64) -> bool {
        self.ttl_deadline_ms != 0 && now_ms >= self.ttl_deadline_ms
    }

    pub fn expiry_hlc(&self) -> u64 {
        self.ttl_deadline_ms << 16
    }

    pub fn encode_to(&self, out: &mut Vec<u8>) {
        out.push(self.flags);
        out.extend_from_slice(&self.hlc.to_be_bytes());
        out.extend_from_slice(&self.origin.to_be_bytes());
        out.extend_from_slice(&self.ttl_deadline_ms.to_be_bytes());
    }

    /// Build a full stored value: envelope + payload.
    pub fn encode_with(&self, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(ENVELOPE_LEN + payload.len());
        self.encode_to(&mut out);
        out.extend_from_slice(payload);
        out
    }

    pub fn decode(value: &[u8]) -> Option<(Envelope, &[u8])> {
        if value.len() < ENVELOPE_LEN {
            return None;
        }
        let env = Envelope {
            flags: value[0],
            hlc: u64::from_be_bytes(value[1..9].try_into().unwrap()),
            origin: u16::from_be_bytes(value[9..11].try_into().unwrap()),
            ttl_deadline_ms: u64::from_be_bytes(value[11..19].try_into().unwrap()),
        };
        Some((env, &value[ENVELOPE_LEN..]))
    }
}

/// Head-key payload: `[ctype u8][del_hlc u64 BE]` (+ type-specific tail).
pub mod head {
    pub const CTYPE_HASH: u8 = 1;
    pub const CTYPE_SET: u8 = 2;
    pub const CTYPE_ZSET: u8 = 3;
    pub const CTYPE_STREAM: u8 = 4;
    pub const CTYPE_HLL: u8 = 6;
    /// Lists are head-gated collections of position-keyed LWW elements
    /// (design/02 §Lists). The head carries the collection TTL + delete clock.
    pub const CTYPE_LIST: u8 = 5;
    /// Budget collection (design/13): head tail carries the admin config
    /// (`crate::budget::HeadState`); elements are escrow slots and tokens.
    pub const CTYPE_BUDGET: u8 = 7;
    /// RESERVED for JSON documents (Track A: per-path CRDT collection).
    pub const CTYPE_JSON: u8 = 8;
    /// Protobuf typed value (design/17): HEAD-ONLY record — the head tail is
    /// the `crate::protohead` codec (schema/version/type + message bytes).
    /// Whole-message LWW via the ordinary head merge.
    pub const CTYPE_PROTO: u8 = 9;

    pub fn encode(ctype: u8, del_hlc: u64) -> Vec<u8> {
        let mut v = Vec::with_capacity(9);
        v.push(ctype);
        v.extend_from_slice(&del_hlc.to_be_bytes());
        v
    }

    pub fn decode(payload: &[u8]) -> Option<(u8, u64)> {
        if payload.len() < 9 {
            return None;
        }
        Some((
            payload[0],
            u64::from_be_bytes(payload[1..9].try_into().unwrap()),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let e = Envelope::new(RecordType::SetMember, 0xABCD_EF01_2345, 42).with_ttl(99_000);
        let v = e.encode_with(b"payload");
        let (d, p) = Envelope::decode(&v).unwrap();
        assert_eq!(d, e);
        assert_eq!(p, b"payload");
        assert_eq!(d.rtype(), RecordType::SetMember);
        assert!(!d.is_tombstone());
        assert!(d.is_expired(99_000));
        assert!(!d.is_expired(98_999));
    }

    #[test]
    fn flags() {
        let t = Envelope::tombstone(RecordType::HashField, 1, 2);
        assert!(t.is_tombstone());
        assert_eq!(t.rtype(), RecordType::HashField);
        let h = Envelope::head(5, 0);
        assert!(h.is_head());
    }
}

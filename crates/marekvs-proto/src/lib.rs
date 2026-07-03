//! marekvs-proto — peer mesh wire messages and framing (design/04 §Transport).
//!
//! Framing: `[len: u32 LE][postcard body]`, max frame 8 MiB. The body is a
//! single `PeerMsg` enum; postcard varint discriminants replace the manual
//! msg-type byte from the design (one enum = one registry, still compact).
//! `ReplOp` values are raw bytes copied verbatim from/to ondaDB — zero
//! re-encode on the hot path.

use serde::{Deserialize, Serialize};

pub type NodeId = u16;
pub type Pid = u16;

pub const MAX_FRAME: usize = 8 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum ProtoError {
    #[error("frame exceeds MAX_FRAME: {0} bytes")]
    TooLarge(usize),
    #[error("postcard: {0}")]
    Codec(#[from] postcard::Error),
}

/// One replicated operation: full internal key + verbatim stored value
/// (19-byte envelope + payload).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplOp {
    pub ikey: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplBatch {
    pub origin: NodeId,
    /// Origin's ondaDB commit sequence of the first op (cursor resume).
    pub first_seq: u64,
    pub ops: Vec<ReplOp>,
    /// Sender wants to be registered as an interest subscriber for these
    /// keys' partitions (write-implies-subscribe).
    pub implicit_sub: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PeerMsg {
    /// First message on every connection: identify the dialer.
    Hello {
        node: NodeId,
        kind: ConnKind,
    },

    // --- replication (ctl) ---
    Repl(ReplBatch),
    AckSeq {
        origin: NodeId,
        seq: u64,
    },
    ResumeFrom {
        origin: NodeId,
        seq: u64,
    },

    // --- fetch / interest (ctl) ---
    Fetch {
        id: u64,
        ikey: Vec<u8>,
    },
    /// value = None: key unknown to the home either.
    FetchResp {
        id: u64,
        value: Option<Vec<u8>>,
        lease_ms: u64,
    },
    FetchCollection {
        id: u64,
        userkey: Vec<u8>,
    },
    /// Streamed collection fetch: one message with all element records
    /// (head first when present); simple v1, chunking is future work.
    FetchCollectionResp {
        id: u64,
        ops: Vec<ReplOp>,
        lease_ms: u64,
    },
    Check {
        id: u64,
        ikey: Vec<u8>,
        hlc: u64,
    },
    /// newer = None: local copy is fresh; Some(value): merged newer record.
    CheckResp {
        id: u64,
        newer: Option<Vec<u8>>,
        lease_ms: u64,
    },
    InterestRenew {
        pid: Pid,
        keys: Vec<Vec<u8>>,
    },

    // --- anti-entropy (bulk) ---
    MerkleRoot {
        pid: Pid,
        root: u64,
    },
    MerkleBuckets {
        pid: Pid,
        digests: Vec<u64>,
    },
    BucketKeys {
        pid: Pid,
        bucket: u8,
        entries: Vec<(u64, u64)>,
    }, // (ikey_hash, hlc)
    /// Repair payload in either direction.
    RepairOps {
        pid: Pid,
        ops: Vec<ReplOp>,
    },
    /// Ask peer to send full records for these ikey hashes of a bucket.
    RequestKeys {
        pid: Pid,
        bucket: u8,
        ikey_hashes: Vec<u64>,
    },

    // --- bootstrap / handoff (bulk) ---
    BootstrapReq {
        pid: Pid,
    },
    BootstrapChunk {
        pid: Pid,
        ops: Vec<ReplOp>,
    },
    BootstrapDone {
        pid: Pid,
        as_of_seq: u64,
    },
    HandoffAck {
        pid: Pid,
    },

    // --- pub/sub (ctl) ---
    Publish {
        channel: Vec<u8>,
        payload: Vec<u8>,
    },

    // --- liveness (ctl) ---
    Ping {
        nonce: u64,
    },
    Pong {
        nonce: u64,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ConnKind {
    Ctl,
    Bulk,
}

pub fn encode(msg: &PeerMsg) -> Result<Vec<u8>, ProtoError> {
    let body = postcard::to_allocvec(msg)?;
    if body.len() > MAX_FRAME {
        return Err(ProtoError::TooLarge(body.len()));
    }
    let mut frame = Vec::with_capacity(4 + body.len());
    frame.extend_from_slice(&(body.len() as u32).to_le_bytes());
    frame.extend_from_slice(&body);
    Ok(frame)
}

/// Try to decode one frame from `buf`. Returns (msg, consumed) or None if
/// more bytes are needed.
pub fn decode(buf: &[u8]) -> Result<Option<(PeerMsg, usize)>, ProtoError> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let len = u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize;
    if len > MAX_FRAME {
        return Err(ProtoError::TooLarge(len));
    }
    if buf.len() < 4 + len {
        return Ok(None);
    }
    let msg = postcard::from_bytes(&buf[4..4 + len])?;
    Ok(Some((msg, 4 + len)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let msg = PeerMsg::Repl(ReplBatch {
            origin: 3,
            first_seq: 42,
            ops: vec![ReplOp {
                ikey: vec![1, 2, 3],
                value: vec![9; 32],
            }],
            implicit_sub: true,
        });
        let frame = encode(&msg).unwrap();
        let (decoded, consumed) = decode(&frame).unwrap().unwrap();
        assert_eq!(consumed, frame.len());
        assert_eq!(decoded, msg);
    }

    #[test]
    fn partial_frame() {
        let frame = encode(&PeerMsg::Ping { nonce: 7 }).unwrap();
        assert!(decode(&frame[..frame.len() - 1]).unwrap().is_none());
        assert!(decode(&frame[..2]).unwrap().is_none());
    }

    #[test]
    fn two_frames_in_buffer() {
        let mut buf = encode(&PeerMsg::Ping { nonce: 1 }).unwrap();
        buf.extend(encode(&PeerMsg::Pong { nonce: 2 }).unwrap());
        let (m1, n1) = decode(&buf).unwrap().unwrap();
        assert_eq!(m1, PeerMsg::Ping { nonce: 1 });
        let (m2, n2) = decode(&buf[n1..]).unwrap().unwrap();
        assert_eq!(m2, PeerMsg::Pong { nonce: 2 });
        assert_eq!(n1 + n2, buf.len());
    }
}

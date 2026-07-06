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
    /// Highest ring seq this batch COVERS on the sender — including entries
    /// filtered out for this peer. The receiver acks and persists THIS value:
    /// acking `first_seq + ops.len() - 1` would never equal the sender's
    /// cursor under interest-filtered traffic, so flow-control windows would
    /// never drain and ResumeFrom would rewind too far.
    pub last_seq: u64,
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
    /// Reply to a MerkleRoot whose root EQUALS ours: the sender's copy of
    /// `pid` is confirmed in sync. Consumed by the gc_grace rejoin driver
    /// as its per-partition completion signal; harmless elsewhere.
    MerkleRootMatch {
        pid: Pid,
    },
    MerkleBuckets {
        pid: Pid,
        digests: Vec<u64>,
    },
    BucketKeys {
        pid: Pid,
        bucket: u8,
        entries: Vec<(u64, u64, u64)>,
        /// True when the sender is NOT an owner of `pid` (stranded-record
        /// offer): the receiver must only REQUEST what it lacks, never push
        /// backfill — a non-owner's cache must not accumulate the partition.
        no_backfill: bool,
    }, // (ikey_hash, hlc, value_hash)
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

    // --- bootstrap (bulk) ---
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

    // --- budgets (ctl, design/13) ---
    // NOTE: postcard discriminants are positional — this enum is APPEND-ONLY.
    /// Ask the receiver to grant from ITS OWN escrow slot (forwarded
    /// BG.RESERVE). `ttl_ms = 0` = receiver applies its defaults.
    BudgetReserve {
        id: u64,
        key: Vec<u8>,
        amount: u64,
        ttl_ms: u64,
        reqid: u64,
    },
    BudgetReserveResp {
        id: u64,
        result: Result<BudgetGrantWire, BudgetErrKind>,
    },
    /// Forwarded BG.COMMIT / BG.RELEASE / BG.DRAW — the receiver must be the
    /// token's issuer. `draw` Some(n) = incremental draw; `release` =
    /// RELEASE; else COMMIT with `spent` (None = accept the drawn total).
    BudgetClose {
        id: u64,
        key: Vec<u8>,
        token: BudgetTokenId,
        spent: Option<u64>,
        draw: Option<u64>,
        release: bool,
    },
    /// Ok = credited amount (COMMIT/RELEASE) or remaining (DRAW).
    BudgetCloseResp {
        id: u64,
        result: Result<u64, BudgetErrKind>,
    },
}

/// Token identity on the wire (client form is `gen-hlc-node-epoch` hex).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct BudgetTokenId {
    pub gen: u64,
    pub hlc: u64,
    pub node: NodeId,
    pub epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BudgetGrantWire {
    /// Client-facing token id string bytes.
    pub token: Vec<u8>,
    pub amount: u64,
    /// Absolute deadline stamped by the ISSUER's clock.
    pub deadline_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum BudgetErrKind {
    Exhausted,
    NoBudget,
    TryAgain,
    TokenExpired,
    TokenUsed,
    Other(String),
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
            last_seq: 45,
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
    fn budget_roundtrips() {
        for msg in [
            PeerMsg::BudgetReserve {
                id: 9,
                key: b"budget".to_vec(),
                amount: 100,
                ttl_ms: 30_000,
                reqid: 7,
            },
            PeerMsg::BudgetReserveResp {
                id: 9,
                result: Ok(BudgetGrantWire {
                    token: b"1-2-3-4".to_vec(),
                    amount: 100,
                    deadline_ms: 123,
                }),
            },
            PeerMsg::BudgetReserveResp {
                id: 9,
                result: Err(BudgetErrKind::Exhausted),
            },
            PeerMsg::BudgetClose {
                id: 10,
                key: b"budget".to_vec(),
                token: BudgetTokenId {
                    gen: 1,
                    hlc: 2,
                    node: 3,
                    epoch: 4,
                },
                spent: Some(50),
                draw: None,
                release: false,
            },
            PeerMsg::BudgetCloseResp {
                id: 10,
                result: Err(BudgetErrKind::Other("boom".into())),
            },
        ] {
            let frame = encode(&msg).unwrap();
            let (decoded, consumed) = decode(&frame).unwrap().unwrap();
            assert_eq!(consumed, frame.len());
            assert_eq!(decoded, msg);
        }
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

//! Budget records (design/13): codecs and convergent merges for the escrow
//! protocol behind the `BG.*` command family.
//!
//! Three record shapes live under one budget user key (`Tag::Budget`,
//! element kind = first suffix byte, see `ikey`):
//!
//! * **Head** — ordinary collection head; the tail after `[ctype][del_hlc]`
//!   is [`HeadState`], an absolute LWW register written only by the single
//!   logical central actor (`BG.CREATE`/`TOPUP`/`RECLAIM`), guarded by a
//!   monotone `op_seq`.
//! * **Slot** — per `(gen, node, epoch)` escrow ledger `[granted][returned]`,
//!   written ONLY by the live incarnation `(node, epoch)` itself. Merge is
//!   pointwise max: single-writer grow-only counters, so the join can never
//!   lose an acked grant or credit.
//! * **Token** — one reservation. Merge is a **rank lattice**, not LWW:
//!   `open(0) < closing(1) < folded(2)`; a higher rank absorbs regardless of
//!   HLC. This is what makes a fold final — a skewed-clock COMMIT written
//!   elsewhere with a later HLC must not "un-fold" a folded token (that
//!   would double-credit the escrow). Folded records also carry the envelope
//!   TOMBSTONE flag so they inherit gc_grace retention and the rejoin
//!   resurrection machinery.

use crate::envelope::{Envelope, RecordType, TOMBSTONE};
use crate::merge::MergeOutcome;
use crate::NodeId;

pub const MODE_POOL: u8 = 0;
pub const MODE_WINDOW: u8 = 1;

/// Token lattice ranks. `RANK_CLOSING` is reserved for a future queued-commit
/// mode (non-issuer close requests); v1 never writes it, but the lattice
/// keeps the slot so adding it later is not a format change.
pub const RANK_OPEN: u8 = 0;
pub const RANK_CLOSING: u8 = 1;
pub const RANK_FOLDED: u8 = 2;

/// Terminal token states (meaningful at `RANK_FOLDED`).
pub const STATE_OPEN: u8 = 0;
pub const STATE_COMMITTED: u8 = 1;
pub const STATE_RELEASED: u8 = 2;
pub const STATE_EXPIRED: u8 = 3;

const HEAD_VER: u8 = 1;
const TOKEN_VER: u8 = 1;

// ---------------------------------------------------------------------------
// Token id
// ---------------------------------------------------------------------------

/// Cluster-unique token identity; also the token record's key suffix fields.
/// `hlc` is the issuer's HLC at grant time (per-process monotone), `epoch`
/// the issuer's store epoch (minted per empty data dir), so NodeId reuse on
/// a fresh PVC can never collide with a dead incarnation's tokens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenId {
    pub gen: u64,
    pub hlc: u64,
    pub node: NodeId,
    pub epoch: u64,
}

impl TokenId {
    /// Client-facing form: `gen-hlc-node-epoch`, lowercase hex.
    pub fn format(&self) -> String {
        format!(
            "{:x}-{:x}-{:x}-{:x}",
            self.gen, self.hlc, self.node, self.epoch
        )
    }

    pub fn parse(s: &[u8]) -> Option<TokenId> {
        let s = std::str::from_utf8(s).ok()?;
        let mut it = s.split('-');
        let gen = u64::from_str_radix(it.next()?, 16).ok()?;
        let hlc = u64::from_str_radix(it.next()?, 16).ok()?;
        let node = u16::from_str_radix(it.next()?, 16).ok()?;
        let epoch = u64::from_str_radix(it.next()?, 16).ok()?;
        if it.next().is_some() {
            return None;
        }
        Some(TokenId {
            gen,
            hlc,
            node,
            epoch,
        })
    }
}

// ---------------------------------------------------------------------------
// Head state (tail of the head payload, after [ctype][del_hlc])
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HeadState {
    /// Monotone central-actor op sequence; a serving shard rejects/no-ops
    /// admin writes with `seq <= op_seq` (idempotent at-least-once retries).
    pub op_seq: u64,
    /// Budget generation = the head HLC at CREATE; embedded in slot and
    /// token keys so records physically cannot leak across DEL + re-CREATE.
    pub gen: u64,
    pub mode: u8,
    pub period_ms: u64,
    pub capacity: u64,
    /// Per-reservation ceiling (0 = capacity).
    pub max_amount: u64,
    pub default_ttl_ms: u64,
    pub max_ttl_ms: u64,
    /// Escrow allocation per node, absolute values, Σ ≤ capacity (checked at
    /// every admin write). Sorted by node id (canonical).
    pub alloc: Vec<(NodeId, u64)>,
    /// Fenced nodes (RECLAIM): node → fence wall-ms. A fenced node must not
    /// grant. Sorted by node id.
    pub fence: Vec<(NodeId, u64)>,
}

impl HeadState {
    pub fn alloc_for(&self, node: NodeId) -> u64 {
        self.alloc
            .iter()
            .find(|(n, _)| *n == node)
            .map_or(0, |(_, a)| *a)
    }

    pub fn is_fenced(&self, node: NodeId) -> bool {
        self.fence.iter().any(|(n, _)| *n == node)
    }

    pub fn alloc_total(&self) -> u128 {
        self.alloc.iter().map(|(_, a)| *a as u128).sum()
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(60 + self.alloc.len() * 10 + self.fence.len() * 10);
        out.push(HEAD_VER);
        out.extend_from_slice(&self.op_seq.to_be_bytes());
        out.extend_from_slice(&self.gen.to_be_bytes());
        out.push(self.mode);
        out.extend_from_slice(&self.period_ms.to_be_bytes());
        out.extend_from_slice(&self.capacity.to_be_bytes());
        out.extend_from_slice(&self.max_amount.to_be_bytes());
        out.extend_from_slice(&self.default_ttl_ms.to_be_bytes());
        out.extend_from_slice(&self.max_ttl_ms.to_be_bytes());
        debug_assert!(self.alloc.len() <= u8::MAX as usize);
        out.push(self.alloc.len() as u8);
        for (n, a) in &self.alloc {
            out.extend_from_slice(&n.to_be_bytes());
            out.extend_from_slice(&a.to_be_bytes());
        }
        debug_assert!(self.fence.len() <= u8::MAX as usize);
        out.push(self.fence.len() as u8);
        for (n, f) in &self.fence {
            out.extend_from_slice(&n.to_be_bytes());
            out.extend_from_slice(&f.to_be_bytes());
        }
        out
    }

    pub fn decode(tail: &[u8]) -> Option<HeadState> {
        if tail.len() < 58 || tail[0] != HEAD_VER {
            return None;
        }
        let u64at = |p: usize| u64::from_be_bytes(tail[p..p + 8].try_into().unwrap());
        let mut st = HeadState {
            op_seq: u64at(1),
            gen: u64at(9),
            mode: tail[17],
            period_ms: u64at(18),
            capacity: u64at(26),
            max_amount: u64at(34),
            default_ttl_ms: u64at(42),
            max_ttl_ms: u64at(50),
            alloc: vec![],
            fence: vec![],
        };
        let mut pos = 58;
        let nalloc = *tail.get(pos)? as usize;
        pos += 1;
        if tail.len() < pos + nalloc * 10 {
            return None;
        }
        for _ in 0..nalloc {
            let n = u16::from_be_bytes(tail[pos..pos + 2].try_into().unwrap());
            let a = u64::from_be_bytes(tail[pos + 2..pos + 10].try_into().unwrap());
            st.alloc.push((n, a));
            pos += 10;
        }
        let nfence = *tail.get(pos)? as usize;
        pos += 1;
        if tail.len() < pos + nfence * 10 {
            return None;
        }
        for _ in 0..nfence {
            let n = u16::from_be_bytes(tail[pos..pos + 2].try_into().unwrap());
            let f = u64::from_be_bytes(tail[pos + 2..pos + 10].try_into().unwrap());
            st.fence.push((n, f));
            pos += 10;
        }
        Some(st)
    }
}

// ---------------------------------------------------------------------------
// Slot state
// ---------------------------------------------------------------------------

/// Escrow ledger for one `(gen, node, epoch)` (pool) or
/// `(gen, window, node, epoch)` (window mode). Both fields are grow-only and
/// written only by the owning incarnation; outstanding = granted − returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SlotState {
    pub granted: u64,
    pub returned: u64,
}

impl SlotState {
    pub fn outstanding(&self) -> u128 {
        (self.granted as u128).saturating_sub(self.returned as u128)
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(16);
        out.extend_from_slice(&self.granted.to_be_bytes());
        out.extend_from_slice(&self.returned.to_be_bytes());
        out
    }

    pub fn decode(payload: &[u8]) -> Option<SlotState> {
        if payload.len() < 16 {
            return None;
        }
        Some(SlotState {
            granted: u64::from_be_bytes(payload[..8].try_into().unwrap()),
            returned: u64::from_be_bytes(payload[8..16].try_into().unwrap()),
        })
    }

    /// Lattice join: pointwise max. Grow-only single-writer counters — the
    /// join can only move toward the writer's latest value, never below it.
    pub fn join(a: SlotState, b: SlotState) -> SlotState {
        SlotState {
            granted: a.granted.max(b.granted),
            returned: a.returned.max(b.returned),
        }
    }
}

// ---------------------------------------------------------------------------
// Token state
// ---------------------------------------------------------------------------

/// One reservation. `gen`/issuer/epoch live in the KEY (see
/// `ikey::budget_token_key`); the payload carries the lattice rank, the
/// amounts, and the deadline. The deadline is payload-only by design: it
/// must NOT ride the envelope TTL, or every replica's expiry sweeper would
/// tombstone the record at deadline and destroy the state the issuer needs
/// to fold. Only the fold sets an envelope TTL (gc backstop).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenState {
    pub rank: u8,
    pub state: u8,
    pub amount: u64,
    /// Server-tracked incremental spend (BG.DRAW); ≤ amount.
    pub spent: u64,
    /// Escrow credited back at fold (amount − accepted spend); meaningful at
    /// RANK_FOLDED. RECLAIM derives a node's returns from Σ credited of its
    /// folded tokens, never from a replica's slot copy (per-record AE repair
    /// can tear a fold Txn apart on replicas; a token is one record).
    pub credited: u64,
    /// Absolute wall-clock deadline (issuer's clock is the only authority).
    pub deadline_ms: u64,
    /// Window label the grant was charged to (window mode; 0 in pool mode).
    pub window: u64,
    /// Client-supplied reservation dedup id (0 = none).
    pub reqid: u64,
}

impl TokenState {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(3 + 6 * 8);
        out.push(TOKEN_VER);
        out.push(self.rank);
        out.push(self.state);
        out.extend_from_slice(&self.amount.to_be_bytes());
        out.extend_from_slice(&self.spent.to_be_bytes());
        out.extend_from_slice(&self.credited.to_be_bytes());
        out.extend_from_slice(&self.deadline_ms.to_be_bytes());
        out.extend_from_slice(&self.window.to_be_bytes());
        out.extend_from_slice(&self.reqid.to_be_bytes());
        out
    }

    pub fn decode(payload: &[u8]) -> Option<TokenState> {
        if payload.len() < 51 || payload[0] != TOKEN_VER {
            return None;
        }
        let u64at = |p: usize| u64::from_be_bytes(payload[p..p + 8].try_into().unwrap());
        Some(TokenState {
            rank: payload[1],
            state: payload[2],
            amount: u64at(3),
            spent: u64at(11),
            credited: u64at(19),
            deadline_ms: u64at(27),
            window: u64at(35),
            reqid: u64at(43),
        })
    }
}

// ---------------------------------------------------------------------------
// Record constructors (envelope + payload)
// ---------------------------------------------------------------------------

/// Slot records reuse `RecordType::String` in the envelope — the 3-bit type
/// field is full and budget records are merge-routed by the ikey tag, so the
/// envelope type is never consulted for them.
pub fn encode_slot_record(version: (u64, NodeId), ttl: u64, tomb: bool, st: SlotState) -> Vec<u8> {
    let mut flags = (RecordType::String as u8) << 2;
    if tomb {
        flags |= TOMBSTONE;
    }
    let env = Envelope {
        flags,
        hlc: version.0,
        origin: version.1,
        ttl_deadline_ms: ttl,
    };
    env.encode_with(&st.encode())
}

pub fn encode_token_record(version: (u64, NodeId), ttl: u64, st: TokenState) -> Vec<u8> {
    let mut flags = (RecordType::String as u8) << 2;
    // Folded = tombstone-class: gc_grace retention + rejoin resurrection
    // machinery treat it exactly like a delete of the open token.
    if st.rank == RANK_FOLDED {
        flags |= TOMBSTONE;
    }
    let env = Envelope {
        flags,
        hlc: version.0,
        origin: version.1,
        ttl_deadline_ms: ttl,
    };
    env.encode_with(&st.encode())
}

// ---------------------------------------------------------------------------
// merge
// ---------------------------------------------------------------------------

fn lww(lenv: &Envelope, ienv: &Envelope) -> MergeOutcome {
    if ienv.version() > lenv.version() {
        MergeOutcome::TakeIncoming
    } else {
        MergeOutcome::KeepLocal
    }
}

/// Merge two full budget records (envelope + payload) for the same internal
/// key. `kind` is the first byte of the ikey element suffix
/// (`ikey::BUDGET_SLOT` / `BUDGET_WINDOW_SLOT` / `BUDGET_TOKEN`).
/// Unknown kinds and undecodable payloads fall back to envelope LWW
/// (defensive, same pattern as counters).
pub fn merge_budget(kind: u8, local: &[u8], incoming: &[u8]) -> MergeOutcome {
    let Some((lenv, lpay)) = Envelope::decode(local) else {
        return MergeOutcome::TakeIncoming;
    };
    let Some((ienv, ipay)) = Envelope::decode(incoming) else {
        return MergeOutcome::KeepLocal;
    };
    match kind {
        crate::ikey::BUDGET_SLOT | crate::ikey::BUDGET_WINDOW_SLOT => {
            // Slot vs GC tombstone: LWW by envelope (window GC writes the
            // tombstone only once the window is out of grant reach).
            if lenv.is_tombstone() || ienv.is_tombstone() {
                return lww(&lenv, &ienv);
            }
            let (Some(l), Some(i)) = (SlotState::decode(lpay), SlotState::decode(ipay)) else {
                return lww(&lenv, &ienv);
            };
            let joined = SlotState::join(l, i);
            let version = lenv.version().max(ienv.version());
            let ttl = if ienv.version() > lenv.version() {
                ienv.ttl_deadline_ms
            } else {
                lenv.ttl_deadline_ms
            };
            let merged = encode_slot_record(version, ttl, false, joined);
            if merged == local {
                MergeOutcome::KeepLocal
            } else if merged == incoming {
                MergeOutcome::TakeIncoming
            } else {
                MergeOutcome::Merged(merged)
            }
        }
        crate::ikey::BUDGET_TOKEN => {
            let (Some(l), Some(i)) = (TokenState::decode(lpay), TokenState::decode(ipay)) else {
                return lww(&lenv, &ienv);
            };
            // Rank lattice: higher rank absorbs regardless of HLC. Within a
            // rank, LWW by envelope version (ties keep local — identical
            // write). The result is always one input verbatim → canonical.
            match l.rank.cmp(&i.rank) {
                std::cmp::Ordering::Greater => MergeOutcome::KeepLocal,
                std::cmp::Ordering::Less => MergeOutcome::TakeIncoming,
                std::cmp::Ordering::Equal => lww(&lenv, &ienv),
            }
        }
        _ => lww(&lenv, &ienv),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ikey::{BUDGET_SLOT, BUDGET_TOKEN};
    use crate::merge::resolve;

    fn m(kind: u8, a: &[u8], b: &[u8]) -> Vec<u8> {
        resolve(a, b, &merge_budget(kind, a, b)).to_vec()
    }

    #[test]
    fn head_state_roundtrip() {
        let st = HeadState {
            op_seq: 7,
            gen: 0xDEAD,
            mode: MODE_WINDOW,
            period_ms: 60_000,
            capacity: 1000,
            max_amount: 100,
            default_ttl_ms: 30_000,
            max_ttl_ms: 3_600_000,
            alloc: vec![(0, 400), (1, 300), (2, 300)],
            fence: vec![(3, 123_456)],
        };
        assert_eq!(HeadState::decode(&st.encode()).unwrap(), st);
        assert_eq!(st.alloc_for(1), 300);
        assert_eq!(st.alloc_for(9), 0);
        assert!(st.is_fenced(3));
        assert!(!st.is_fenced(0));
        assert_eq!(st.alloc_total(), 1000);
    }

    #[test]
    fn token_id_roundtrip() {
        let id = TokenId {
            gen: 0xABC,
            hlc: 0x1234_5678_9ABC,
            node: 7,
            epoch: 0xFEED,
        };
        assert_eq!(TokenId::parse(id.format().as_bytes()).unwrap(), id);
        assert!(TokenId::parse(b"nope").is_none());
        assert!(TokenId::parse(b"1-2-3").is_none());
        assert!(TokenId::parse(b"1-2-3-4-5").is_none());
    }

    #[test]
    fn slot_join_is_pointwise_max() {
        let a = encode_slot_record(
            (100, 1),
            0,
            false,
            SlotState {
                granted: 50,
                returned: 10,
            },
        );
        let b = encode_slot_record(
            (90, 1),
            0,
            false,
            SlotState {
                granted: 40,
                returned: 30,
            },
        );
        let ab = m(BUDGET_SLOT, &a, &b);
        let ba = m(BUDGET_SLOT, &b, &a);
        assert_eq!(ab, ba);
        let (_, pay) = Envelope::decode(&ab).unwrap();
        assert_eq!(
            SlotState::decode(pay).unwrap(),
            SlotState {
                granted: 50,
                returned: 30
            }
        );
        // idempotent
        assert_eq!(m(BUDGET_SLOT, &ab, &a), ab);
        assert_eq!(m(BUDGET_SLOT, &ab, &b), ab);
    }

    #[test]
    fn folded_token_absorbs_later_hlc_open() {
        let open = TokenState {
            rank: RANK_OPEN,
            state: STATE_OPEN,
            amount: 100,
            spent: 0,
            credited: 0,
            deadline_ms: 1000,
            window: 0,
            reqid: 0,
        };
        let folded = TokenState {
            rank: RANK_FOLDED,
            state: STATE_EXPIRED,
            credited: 100,
            ..open
        };
        // The open rewrite carries an arbitrarily HIGHER hlc — it must lose.
        let open_rec = encode_token_record((999_999, 3), 0, open);
        let folded_rec = encode_token_record((100, 1), 0, folded);
        assert_eq!(
            merge_budget(BUDGET_TOKEN, &folded_rec, &open_rec),
            MergeOutcome::KeepLocal
        );
        assert_eq!(
            merge_budget(BUDGET_TOKEN, &open_rec, &folded_rec),
            MergeOutcome::TakeIncoming
        );
        // Folded records carry the tombstone flag (gc_grace retention).
        let (env, _) = Envelope::decode(&folded_rec).unwrap();
        assert!(env.is_tombstone());
    }

    #[test]
    fn token_equal_rank_is_lww() {
        let t = TokenState {
            rank: RANK_OPEN,
            state: STATE_OPEN,
            amount: 5,
            spent: 0,
            credited: 0,
            deadline_ms: 9,
            window: 0,
            reqid: 0,
        };
        let a = encode_token_record((100, 1), 0, t);
        let b = encode_token_record((200, 2), 0, TokenState { amount: 6, ..t });
        assert_eq!(m(BUDGET_TOKEN, &a, &b), b);
        assert_eq!(m(BUDGET_TOKEN, &b, &a), b);
    }

    #[test]
    fn token_state_roundtrip() {
        let t = TokenState {
            rank: RANK_FOLDED,
            state: STATE_COMMITTED,
            amount: 100,
            spent: 73,
            credited: 27,
            deadline_ms: 42_000,
            window: 17,
            reqid: 0xC0FFEE,
        };
        assert_eq!(TokenState::decode(&t.encode()).unwrap(), t);
    }
}

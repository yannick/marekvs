//! BG.* — distributed budgets (design/13).
//!
//! Escrow protocol: capacity is pre-split into per-node allocations; a node
//! grants reservations (tokens) only against its OWN escrow slot, on its own
//! shard thread — every grant decision has a single writer, which is what
//! makes Σ(outstanding) + Σ(accepted spend) ≤ capacity hold through
//! partitions, crashes and split-brain. Fail closed everywhere.
//!
//! The five local rules (design/13):
//! 1. only the live incarnation (node, epoch) writes its own slots;
//! 2. every acked mutation is WAL-synced BEFORE it is published to the
//!    replication ring ([`budget_write`], durable-before-publish);
//! 3. only the ISSUER transitions token state, each transition carries its
//!    slot credit in the same txn, folded is absorbing + tombstone-class;
//! 4. admin state is one absolute LWW head record with a monotone op_seq;
//! 5. generations live in the keys (gen = head-create HLC), so stale writers
//!    physically cannot leak credits across DEL + re-CREATE.

use std::collections::HashMap;
use std::sync::Arc;

use marekvs_core::budget::{
    encode_slot_record, encode_token_record, HeadState, SlotState, TokenId, TokenState, MODE_POOL,
    MODE_WINDOW, RANK_FOLDED, STATE_COMMITTED, STATE_EXPIRED, STATE_OPEN, STATE_RELEASED,
};
use marekvs_core::envelope::{head, Envelope, COLLECTION_HEAD};
use marekvs_core::{hlc_phys_ms, ikey};

use crate::reply::Reply;
use crate::store::{self, get_raw, now_ms, scan_prefix, ShardCtx};
use crate::{BudgetErr, BudgetGrant, Engine};

/// How many expired own tokens one grant/close opportunistically folds.
const FOLD_BATCH: usize = 16;

/// (internal key, full record bytes) writes of one budget txn — committed
/// suppressed, WAL-synced, then manually published to the ring.
pub(crate) type BudgetOps = Vec<(Vec<u8>, Vec<u8>)>;

// ---------------------------------------------------------------------------
// REQID dedup LRU
// ---------------------------------------------------------------------------

/// Issuer-local BG.RESERVE dedup: (budget key, reqid) → the original grant.
/// In-memory only — a crash forgets it, and the orphaned duplicate token is
/// reclaimed at its deadline (the documented backstop).
pub struct ReqidLru {
    cap: usize,
    map: HashMap<(Vec<u8>, u64), BudgetGrant>,
    order: std::collections::VecDeque<(Vec<u8>, u64)>,
}

impl ReqidLru {
    pub fn new(cap: usize) -> Self {
        Self {
            cap: cap.max(1),
            map: HashMap::new(),
            order: std::collections::VecDeque::new(),
        }
    }

    pub fn get(&self, key: &[u8], reqid: u64) -> Option<BudgetGrant> {
        self.map.get(&(key.to_vec(), reqid)).cloned()
    }

    pub fn put(&mut self, key: Vec<u8>, reqid: u64, grant: BudgetGrant) {
        let k = (key, reqid);
        if self.map.insert(k.clone(), grant).is_none() {
            self.order.push_back(k);
        }
        while self.order.len() > self.cap {
            if let Some(old) = self.order.pop_front() {
                self.map.remove(&old);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Shard-local state access
// ---------------------------------------------------------------------------

struct BudgetHead {
    env: Envelope,
    del_hlc: u64,
    state: HeadState,
}

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";

/// Read the live budget head, distinguishing WRONGTYPE from NOBUDGET.
fn head_or_err(ctx: &ShardCtx, key: &[u8]) -> Result<BudgetHead, BudgetErr> {
    let now = now_ms();
    if let Some(v) = get_raw(ctx, &ikey::string_key(key)) {
        if let Some((env, pay)) = Envelope::decode(&v) {
            if store::visible(&env, pay, 0, now).is_some() {
                return Err(BudgetErr::Other(WRONGTYPE.into()));
            }
        }
    }
    let Some(v) = get_raw(ctx, &ikey::head_key(key)) else {
        return Err(BudgetErr::NoBudget);
    };
    let Some((env, pay)) = Envelope::decode(&v) else {
        return Err(BudgetErr::NoBudget);
    };
    let Some((ctype, del_hlc)) = head::decode(pay) else {
        return Err(BudgetErr::NoBudget);
    };
    if env.is_tombstone() || env.is_expired(now) {
        return Err(BudgetErr::NoBudget);
    }
    if ctype != head::CTYPE_BUDGET {
        return Err(BudgetErr::Other(WRONGTYPE.into()));
    }
    let Some(state) = HeadState::decode(&pay[9..]) else {
        return Err(BudgetErr::Other("ERR corrupt budget head".into()));
    };
    Ok(BudgetHead {
        env,
        del_hlc,
        state,
    })
}

/// Raw head read for CREATE (any liveness, for del-clock carry-forward and
/// op_seq continuity).
fn head_raw(ctx: &ShardCtx, key: &[u8]) -> Option<(Envelope, u8, u64, Option<HeadState>)> {
    let v = get_raw(ctx, &ikey::head_key(key))?;
    let (env, pay) = Envelope::decode(&v)?;
    let (ctype, del_hlc) = head::decode(pay)?;
    let st = if ctype == head::CTYPE_BUDGET {
        HeadState::decode(&pay[9..])
    } else {
        None
    };
    Some((env, ctype, del_hlc, st))
}

fn read_slot(ctx: &ShardCtx, slot_key: &[u8]) -> SlotState {
    let Some(v) = get_raw(ctx, slot_key) else {
        return SlotState::default();
    };
    match Envelope::decode(&v) {
        Some((env, pay)) if !env.is_tombstone() => SlotState::decode(pay).unwrap_or_default(),
        _ => SlotState::default(),
    }
}

/// Slot key for the slot a token was granted from.
fn slot_key_for(key: &[u8], head: &HeadState, window: u64, node: u16, epoch: u64) -> Vec<u8> {
    if head.mode == MODE_WINDOW {
        ikey::budget_window_slot_key(key, head.gen, window, node, epoch)
    } else {
        ikey::budget_slot_key(key, head.gen, node, epoch)
    }
}

/// Current window label, derived from the HLC physical component — monotone
/// per process, unlike raw wall clock (design/13 fix 9). 0 in pool mode.
fn current_window(ctx: &ShardCtx, head: &HeadState) -> u64 {
    if head.mode == MODE_WINDOW && head.period_ms > 0 {
        hlc_phys_ms(ctx.hlc.now()) / head.period_ms
    } else {
        0
    }
}

/// Parse (hlc, node, epoch) from a token key suffix `[T][gen][hlc][node][epoch]`.
fn token_suffix_fields(suffix: &[u8]) -> Option<(u64, u64, u16, u64)> {
    if suffix.len() < 27 || suffix[0] != ikey::BUDGET_TOKEN {
        return None;
    }
    let gen = u64::from_be_bytes(suffix[1..9].try_into().unwrap());
    let hlc = u64::from_be_bytes(suffix[9..17].try_into().unwrap());
    let node = u16::from_be_bytes(suffix[17..19].try_into().unwrap());
    let epoch = u64::from_be_bytes(suffix[19..27].try_into().unwrap());
    Some((gen, hlc, node, epoch))
}

// ---------------------------------------------------------------------------
// The durable-before-publish write pipeline (design/13 fix 1)
// ---------------------------------------------------------------------------

/// Commit `ops` in ONE suppressed ondaDB txn on the shard thread. The commit
/// hook still marks AE-dirty (it does so before checking suppression), but
/// nothing enters the replication ring — [`budget_write`] pushes the ops
/// manually AFTER the WAL sync.
fn commit_ops(ctx: &ShardCtx, ops: &[(Vec<u8>, Vec<u8>)]) -> bool {
    let _g = store::suppress_commit_hook();
    let mut txn = ctx.db.begin();
    for (k, v) in ops {
        if let Err(e) = txn.put(&ctx.data, k, v, store::onda_ttl_for(v)) {
            tracing::error!(?e, "budget txn put failed");
            return false;
        }
    }
    if let Err(e) = txn.commit() {
        tracing::error!(?e, "budget txn commit failed");
        return false;
    }
    true
}

/// Run a budget mutation: shard-atomic txn → `sync_wal` OFF the shard thread
/// (an on-thread sync wedges the shard — see the rejoin heartbeat precedent)
/// → manual ring publish → only then does the caller ack. Anything a peer
/// ever sees is on this node's disk first, so this node's own-slot state can
/// never regress below a replica's copy (the double-grant trace).
pub(crate) async fn budget_write<T, F>(
    engine: &Arc<Engine>,
    key: &[u8],
    f: F,
) -> Result<T, BudgetErr>
where
    T: Send + 'static,
    F: FnOnce(&ShardCtx) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, T), BudgetErr> + Send + 'static,
{
    let (ops, out) = engine
        .store
        .run_key(key, move |ctx| {
            let (ops, out) = f(ctx)?;
            if !ops.is_empty() && !commit_ops(ctx, &ops) {
                return Err(BudgetErr::TryAgain("storage commit failed"));
            }
            Ok((ops, out))
        })
        .await?;
    if !ops.is_empty() {
        let db = engine.store.db.clone();
        match tokio::task::spawn_blocking(move || db.sync_wal()).await {
            Ok(Ok(())) => {}
            _ => {
                // Committed but not durably acked: fail closed. If it was a
                // grant, the unacked token is reclaimed at its deadline.
                return Err(BudgetErr::TryAgain("wal sync failed"));
            }
        }
        if let Some(publish) = engine.budget_publish.read().clone() {
            publish(ops);
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Fold machinery (issuer-only)
// ---------------------------------------------------------------------------

/// Pending writes of one budget txn: slot updates are accumulated so several
/// folds + a grant against the same slot become one record write.
#[derive(Default)]
struct PendingOps {
    slots: HashMap<Vec<u8>, SlotState>,
    other: Vec<(Vec<u8>, Vec<u8>)>,
}

impl PendingOps {
    fn slot<'a>(&'a mut self, ctx: &ShardCtx, key: &[u8]) -> &'a mut SlotState {
        self.slots
            .entry(key.to_vec())
            .or_insert_with(|| read_slot(ctx, key))
    }

    fn into_ops(self, ctx: &ShardCtx) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut ops = self.other;
        for (k, st) in self.slots {
            ops.push((
                k,
                encode_slot_record((ctx.hlc.now(), ctx.node_id), 0, false, st),
            ));
        }
        ops
    }
}

/// Fold one token owned by this node: credit the escrow slot it was granted
/// from and rewrite the token as an absorbing, tombstone-class rank-2 record
/// carrying the outcome — one txn with the slot update (design/13 fixes 2+3).
#[allow(clippy::too_many_arguments)] // one fold = one fully-specified transition
fn fold_token(
    ctx: &ShardCtx,
    key: &[u8],
    bhead: &BudgetHead,
    token_ikey: &[u8],
    node: u16,
    epoch: u64,
    st: &TokenState,
    state: u8,
    spent: u64,
    pending: &mut PendingOps,
) -> u64 {
    let spent = spent.min(st.amount);
    let credited = st.amount - spent;
    let slot_key = slot_key_for(key, &bhead.state, st.window, node, epoch);
    let slot = pending.slot(ctx, &slot_key);
    slot.returned = slot.returned.saturating_add(credited);
    let folded = TokenState {
        rank: RANK_FOLDED,
        state,
        spent,
        credited,
        ..*st
    };
    pending.other.push((
        token_ikey.to_vec(),
        encode_token_record((ctx.hlc.now(), ctx.node_id), 0, folded),
    ));
    credited
}

/// Opportunistic expiry sweep: fold up to `limit` of THIS node's tokens that
/// are past deadline + grace (issuer clock — the only deadline authority).
/// Tokens from older generations are folded without slot credit (their gen's
/// ledger is dead); the rank-2 tombstone is what lets GC drop them.
fn fold_expired_own(
    ctx: &ShardCtx,
    key: &[u8],
    bhead: &BudgetHead,
    grace_ms: u64,
    limit: usize,
    pending: &mut PendingOps,
) {
    let now = now_ms();
    let prefix = ikey::prefixed(ikey::Tag::Budget, key, &[ikey::BUDGET_TOKEN]);
    let mut victims: Vec<(Vec<u8>, u64, u16, u64, TokenState)> = Vec::new();
    scan_prefix(ctx, &prefix, |k, v| {
        let Some(p) = ikey::parse(k) else { return true };
        let Some((gen, _hlc, node, epoch)) = token_suffix_fields(p.suffix) else {
            return true;
        };
        if node != ctx.node_id {
            return true;
        }
        let Some((env, pay)) = Envelope::decode(v) else {
            return true;
        };
        if env.is_tombstone() {
            return true;
        }
        let Some(st) = TokenState::decode(pay) else {
            return true;
        };
        if st.rank >= RANK_FOLDED {
            return true;
        }
        let stale_gen = gen != bhead.state.gen;
        if stale_gen || now > st.deadline_ms.saturating_add(grace_ms) {
            victims.push((k.to_vec(), gen, node, epoch, st));
        }
        victims.len() < limit
    });
    for (k, gen, node, epoch, st) in victims {
        if gen == bhead.state.gen {
            fold_token(
                ctx,
                key,
                bhead,
                &k,
                node,
                epoch,
                &st,
                STATE_EXPIRED,
                st.spent,
                pending,
            );
        } else {
            // Old generation: no live ledger to credit — just seal the token.
            let folded = TokenState {
                rank: RANK_FOLDED,
                state: STATE_EXPIRED,
                credited: 0,
                ..st
            };
            pending.other.push((
                k,
                encode_token_record((ctx.hlc.now(), ctx.node_id), 0, folded),
            ));
        }
    }
}

// ---------------------------------------------------------------------------
// Grant (BG.RESERVE)
// ---------------------------------------------------------------------------

/// This node's outstanding (granted − returned) across ALL its epochs for
/// the current gen (and current window, in window mode), overlay-aware.
fn own_outstanding(
    ctx: &ShardCtx,
    key: &[u8],
    head: &HeadState,
    window: u64,
    pending: &PendingOps,
) -> u128 {
    let (kind, fixed) = if head.mode == MODE_WINDOW {
        (ikey::BUDGET_WINDOW_SLOT, Some(window))
    } else {
        (ikey::BUDGET_SLOT, None)
    };
    let prefix = {
        let mut suffix = Vec::with_capacity(17);
        suffix.push(kind);
        suffix.extend_from_slice(&head.gen.to_be_bytes());
        if let Some(w) = fixed {
            suffix.extend_from_slice(&w.to_be_bytes());
        }
        ikey::prefixed(ikey::Tag::Budget, key, &suffix)
    };
    let mut total: u128 = 0;
    let node_off = if fixed.is_some() { 17 } else { 9 };
    scan_prefix(ctx, &prefix, |k, v| {
        let Some(p) = ikey::parse(k) else { return true };
        if p.suffix.len() < node_off + 10 {
            return true;
        }
        let node = u16::from_be_bytes(p.suffix[node_off..node_off + 2].try_into().unwrap());
        if node != ctx.node_id {
            return true;
        }
        // Overlay wins: this txn's pending value supersedes the stored one.
        if pending.slots.contains_key(k) {
            return true;
        }
        if let Some((env, pay)) = Envelope::decode(v) {
            if !env.is_tombstone() {
                if let Some(st) = SlotState::decode(pay) {
                    total += st.outstanding();
                }
            }
        }
        true
    });
    for (k, st) in &pending.slots {
        if k.starts_with(&prefix) {
            let suffix_start = k.len().checked_sub(10);
            if let (Some(off), Some(p)) = (suffix_start, ikey::parse(k)) {
                let _ = off;
                if p.suffix.len() >= node_off + 10 {
                    let node =
                        u16::from_be_bytes(p.suffix[node_off..node_off + 2].try_into().unwrap());
                    if node == ctx.node_id {
                        total += st.outstanding();
                    }
                }
            }
        }
    }
    total
}

pub(crate) struct GrantCfg {
    pub default_ttl_ms: u64,
    pub max_ttl_ms: u64,
    pub grace_ms: u64,
}

/// Shard-local grant: fold expired own tokens, headroom-check in u128, bump
/// the current-epoch slot, write the open token. Runs entirely on the shard
/// thread that owns the budget's partition — single writer by construction.
pub(crate) fn grant_local(
    ctx: &ShardCtx,
    cfg: &GrantCfg,
    key: &[u8],
    amount: u64,
    ttl_ms: Option<u64>,
    reqid: u64,
) -> Result<(BudgetOps, BudgetGrant), BudgetErr> {
    let bhead = head_or_err(ctx, key)?;
    let hs = &bhead.state;
    if hs.is_fenced(ctx.node_id) {
        return Err(BudgetErr::Exhausted);
    }
    if amount == 0 {
        return Err(BudgetErr::Other(
            "ERR amount must be a positive integer".into(),
        ));
    }
    let per_token_max = if hs.max_amount > 0 {
        hs.max_amount.min(hs.capacity)
    } else {
        hs.capacity
    };
    if amount > per_token_max {
        return Err(BudgetErr::Other(format!(
            "ERR amount exceeds budget maximum of {per_token_max}"
        )));
    }
    let ttl = ttl_ms.unwrap_or(if hs.default_ttl_ms > 0 {
        hs.default_ttl_ms
    } else {
        cfg.default_ttl_ms
    });
    let max_ttl = if hs.max_ttl_ms > 0 {
        hs.max_ttl_ms
    } else {
        cfg.max_ttl_ms
    };
    if ttl == 0 || ttl > max_ttl {
        return Err(BudgetErr::Other(format!(
            "ERR TTL must be in 1..={max_ttl} ms"
        )));
    }

    let mut pending = PendingOps::default();
    fold_expired_own(ctx, key, &bhead, cfg.grace_ms, FOLD_BATCH, &mut pending);

    let window = current_window(ctx, hs);
    let alloc = hs.alloc_for(ctx.node_id) as u128;
    let outstanding = own_outstanding(ctx, key, hs, window, &pending);
    if outstanding + amount as u128 > alloc {
        // Commit the folds we found even when the grant itself fails —
        // they only ever free escrow.
        return if pending.other.is_empty() && pending.slots.is_empty() {
            Err(BudgetErr::Exhausted)
        } else {
            if commit_ops(ctx, &pending.into_ops(ctx)) {
                // Folds are replay-deterministic; publish rides the next
                // successful budget write (AE covers the gap).
            }
            Err(BudgetErr::Exhausted)
        };
    }

    let slot_key = slot_key_for(key, hs, window, ctx.node_id, ctx.epoch);
    let slot = pending.slot(ctx, &slot_key);
    let Some(granted) = slot.granted.checked_add(amount) else {
        return Err(BudgetErr::Other("ERR budget ledger overflow".into()));
    };
    slot.granted = granted;

    let token_hlc = ctx.hlc.now();
    let deadline = now_ms() + ttl;
    let tid = TokenId {
        gen: hs.gen,
        hlc: token_hlc,
        node: ctx.node_id,
        epoch: ctx.epoch,
    };
    let token_key = ikey::budget_token_key(key, tid.gen, tid.hlc, tid.node, tid.epoch);
    let st = TokenState {
        rank: marekvs_core::budget::RANK_OPEN,
        state: STATE_OPEN,
        amount,
        spent: 0,
        credited: 0,
        deadline_ms: deadline,
        window,
        reqid,
    };
    pending.other.push((
        token_key,
        encode_token_record((token_hlc, ctx.node_id), 0, st),
    ));
    Ok((
        pending.into_ops(ctx),
        BudgetGrant {
            token: tid.format(),
            amount,
            deadline_ms: deadline,
        },
    ))
}

/// Boot grant-fence (design/13 fix 4): a fresh-epoch node (empty data dir)
/// may hold none of the grants its earlier incarnation acked — it must merge
/// its own slot/token records back from a home replica before granting.
async fn ensure_grant_ready(engine: &Arc<Engine>, key: &[u8]) -> Result<(), BudgetErr> {
    if !engine.store.epoch_fresh {
        return Ok(());
    }
    if engine.budget_grant_ready.lock().contains(key) {
        return Ok(());
    }
    let peer = engine.budget_peer.read().clone();
    if let Some(peer) = peer {
        if !peer.fetch_budget(key).await {
            return Err(BudgetErr::TryAgain(
                "boot fence: no reachable home to fetch budget state from",
            ));
        }
    }
    // No peer layer = standalone/embedded: local state is all there is.
    engine.budget_grant_ready.lock().insert(key.to_vec());
    Ok(())
}

// ---------------------------------------------------------------------------
// Close (BG.COMMIT / BG.RELEASE / BG.DRAW) — issuer only
// ---------------------------------------------------------------------------

pub(crate) enum CloseOp {
    /// COMMIT with explicit spent (None = whatever was drawn so far).
    Commit(Option<u64>),
    Release,
    Draw(u64),
}

/// Issuer-local close/draw. Returns the credited amount (COMMIT/RELEASE) or
/// the remaining amount (DRAW). Inner Result: an expired token still folds
/// (a write) while the client gets TOKENEXPIRED.
#[allow(clippy::type_complexity)]
pub(crate) fn close_local(
    ctx: &ShardCtx,
    grace_ms: u64,
    key: &[u8],
    tid: TokenId,
    op: CloseOp,
) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, Result<u64, BudgetErr>), BudgetErr> {
    let bhead = head_or_err(ctx, key)?;
    if bhead.state.gen != tid.gen {
        return Err(BudgetErr::TokenExpired);
    }
    let token_key = ikey::budget_token_key(key, tid.gen, tid.hlc, tid.node, tid.epoch);
    let Some(v) = get_raw(ctx, &token_key) else {
        return Err(BudgetErr::TokenExpired);
    };
    let Some((_env, pay)) = Envelope::decode(&v) else {
        return Err(BudgetErr::TokenExpired);
    };
    let Some(st) = TokenState::decode(pay) else {
        return Err(BudgetErr::Other("ERR corrupt token record".into()));
    };
    if st.rank >= RANK_FOLDED {
        return Err(if st.state == STATE_EXPIRED {
            BudgetErr::TokenExpired
        } else {
            BudgetErr::TokenUsed
        });
    }
    let mut pending = PendingOps::default();
    if now_ms() > st.deadline_ms {
        // Past deadline: fold as expired NOW (issuer clock; no grace needed —
        // grace only covers the background sweep racing remote commits, and
        // this IS the issuer deciding). The client's spend is not accepted.
        let _ = grace_ms;
        fold_token(
            ctx,
            key,
            &bhead,
            &token_key,
            tid.node,
            tid.epoch,
            &st,
            STATE_EXPIRED,
            st.spent,
            &mut pending,
        );
        return Ok((pending.into_ops(ctx), Err(BudgetErr::TokenExpired)));
    }
    match op {
        CloseOp::Draw(n) => {
            if n == 0 {
                return Err(BudgetErr::Other(
                    "ERR amount must be a positive integer".into(),
                ));
            }
            let Some(spent) = st.spent.checked_add(n) else {
                return Err(BudgetErr::Other("ERR draw overflows token".into()));
            };
            if spent > st.amount {
                return Err(BudgetErr::Other(format!(
                    "ERR draw exceeds reserved amount ({} remaining)",
                    st.amount - st.spent
                )));
            }
            // Token stays open; same rank → LWW within rank, and this node
            // is the single writer of its own tokens.
            let updated = TokenState { spent, ..st };
            pending.other.push((
                token_key,
                encode_token_record((ctx.hlc.now(), ctx.node_id), 0, updated),
            ));
            Ok((pending.into_ops(ctx), Ok(st.amount - spent)))
        }
        CloseOp::Commit(explicit) => {
            let spent = explicit.unwrap_or(st.spent);
            if spent > st.amount {
                return Err(BudgetErr::Other("ERR spent exceeds reserved amount".into()));
            }
            if spent < st.spent {
                return Err(BudgetErr::Other(
                    "ERR spent below already drawn amount".into(),
                ));
            }
            let credited = fold_token(
                ctx,
                key,
                &bhead,
                &token_key,
                tid.node,
                tid.epoch,
                &st,
                STATE_COMMITTED,
                spent,
                &mut pending,
            );
            Ok((pending.into_ops(ctx), Ok(credited)))
        }
        CloseOp::Release => {
            let credited = fold_token(
                ctx,
                key,
                &bhead,
                &token_key,
                tid.node,
                tid.epoch,
                &st,
                STATE_RELEASED,
                st.spent,
                &mut pending,
            );
            Ok((pending.into_ops(ctx), Ok(credited)))
        }
    }
}

// ---------------------------------------------------------------------------
// Command handlers
// ---------------------------------------------------------------------------

fn parse_u64(b: &[u8]) -> Option<u64> {
    std::str::from_utf8(b).ok()?.parse().ok()
}

fn eq_ignore_case(b: &[u8], s: &str) -> bool {
    b.eq_ignore_ascii_case(s.as_bytes())
}

/// BG.CREATE key capacity [MODE POOL | MODE WINDOW period-ms] [TTL ms]
/// [MAXTTL ms] [MAXAMOUNT n] [NODES id [id ...]] [SEQ n]
pub async fn create(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 3 {
        return Reply::wrong_args("bg.create");
    }
    let key = args[1].clone();
    let Some(capacity) = parse_u64(&args[2]) else {
        return Reply::not_int();
    };
    let mut mode = MODE_POOL;
    let mut period_ms = 0u64;
    let mut default_ttl = 0u64;
    let mut max_ttl = 0u64;
    let mut max_amount = 0u64;
    let mut nodes: Vec<u16> = Vec::new();
    let mut seq: Option<u64> = None;
    let mut i = 3;
    while i < args.len() {
        let a = &args[i];
        if eq_ignore_case(a, "MODE") && i + 1 < args.len() {
            if eq_ignore_case(&args[i + 1], "POOL") {
                mode = MODE_POOL;
                i += 2;
            } else if eq_ignore_case(&args[i + 1], "WINDOW") && i + 2 < args.len() {
                mode = MODE_WINDOW;
                let Some(p) = parse_u64(&args[i + 2]).filter(|&p| p > 0) else {
                    return Reply::err("ERR WINDOW period must be a positive integer (ms)");
                };
                period_ms = p;
                i += 3;
            } else {
                return Reply::syntax();
            }
        } else if eq_ignore_case(a, "TTL") && i + 1 < args.len() {
            let Some(v) = parse_u64(&args[i + 1]) else {
                return Reply::not_int();
            };
            default_ttl = v;
            i += 2;
        } else if eq_ignore_case(a, "MAXTTL") && i + 1 < args.len() {
            let Some(v) = parse_u64(&args[i + 1]) else {
                return Reply::not_int();
            };
            max_ttl = v;
            i += 2;
        } else if eq_ignore_case(a, "MAXAMOUNT") && i + 1 < args.len() {
            let Some(v) = parse_u64(&args[i + 1]) else {
                return Reply::not_int();
            };
            max_amount = v;
            i += 2;
        } else if eq_ignore_case(a, "SEQ") && i + 1 < args.len() {
            let Some(v) = parse_u64(&args[i + 1]) else {
                return Reply::not_int();
            };
            seq = Some(v);
            i += 2;
        } else if eq_ignore_case(a, "NODES") {
            i += 1;
            while i < args.len() {
                match parse_u64(&args[i]) {
                    Some(n) if n <= u16::MAX as u64 => {
                        nodes.push(n as u16);
                        i += 1;
                    }
                    _ => break,
                }
            }
            if nodes.is_empty() {
                return Reply::err("ERR NODES requires at least one node id");
            }
        } else {
            return Reply::syntax();
        }
    }
    if nodes.is_empty() {
        nodes = vec![engine.store.node_id];
    }
    nodes.sort_unstable();
    nodes.dedup();
    if nodes.len() > 255 {
        return Reply::err("ERR at most 255 escrow nodes");
    }

    let res = budget_write(engine, &key.clone(), move |ctx| {
        // A live string record shadows the key: WRONGTYPE, like collections.
        let now = now_ms();
        if let Some(v) = get_raw(ctx, &ikey::string_key(&key)) {
            if let Some((env, pay)) = Envelope::decode(&v) {
                if store::visible(&env, pay, 0, now).is_some() {
                    return Err(BudgetErr::Other(WRONGTYPE.into()));
                }
            }
        }
        let prev = head_raw(ctx, &key);
        if let Some((env, ctype, _, _)) = &prev {
            let live = !env.is_tombstone() && !env.is_expired(now);
            if live && *ctype != head::CTYPE_BUDGET {
                return Err(BudgetErr::Other(WRONGTYPE.into()));
            }
        }
        let prev_state = prev.as_ref().and_then(|(env, _, _, st)| {
            let live = !env.is_tombstone() && !env.is_expired(now);
            if live {
                st.clone()
            } else {
                None
            }
        });
        // Idempotent retry: an op-seq at or below the stored one is a no-op.
        if let (Some(seq), Some(ps)) = (seq, &prev_state) {
            if seq <= ps.op_seq {
                return Ok((Vec::new(), ()));
            }
        }
        // Delete-clock carry-forward, exactly like ensure_head: stale
        // pre-delete records must stay shadowed after a re-create.
        let prev_del = prev.as_ref().map_or(0, |(env, _, del, _)| {
            let mut d = *del;
            if env.is_tombstone() {
                d = d.max(env.hlc);
            }
            if env.is_expired(now) {
                d = d.max(env.expiry_hlc());
            }
            d
        });
        let hlc = prev
            .as_ref()
            .map_or(0, |(env, _, _, _)| env.hlc.wrapping_add(1))
            .max(ctx.hlc.now());
        // Even split; the remainder spreads one unit at a time from the front.
        let n = nodes.len() as u64;
        let base = capacity / n;
        let rem = (capacity % n) as usize;
        let alloc: Vec<(u16, u64)> = nodes
            .iter()
            .enumerate()
            .map(|(i, &node)| (node, base + u64::from(i < rem)))
            .collect();
        let hs = HeadState {
            op_seq: seq.unwrap_or_else(|| prev_state.as_ref().map_or(0, |p| p.op_seq) + 1),
            gen: hlc,
            mode,
            period_ms,
            capacity,
            max_amount,
            default_ttl_ms: default_ttl,
            max_ttl_ms: max_ttl,
            alloc,
            fence: Vec::new(),
        };
        debug_assert!(hs.alloc_total() <= hs.capacity as u128);
        let env = Envelope {
            flags: COLLECTION_HEAD,
            hlc,
            origin: ctx.node_id,
            ttl_deadline_ms: 0,
        };
        let mut payload = head::encode(head::CTYPE_BUDGET, prev_del);
        payload.extend_from_slice(&hs.encode());
        Ok((vec![(ikey::head_key(&key), env.encode_with(&payload))], ()))
    })
    .await;
    match res {
        Ok(()) => Reply::ok(),
        Err(e) => e.reply(),
    }
}

/// BG.TOPUP key amount [NODE id] [SEQ n]
pub async fn topup(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 3 {
        return Reply::wrong_args("bg.topup");
    }
    let key = args[1].clone();
    let Some(amount) = parse_u64(&args[2]).filter(|&a| a > 0) else {
        return Reply::not_int();
    };
    let mut node: Option<u16> = None;
    let mut seq: Option<u64> = None;
    let mut i = 3;
    while i < args.len() {
        if eq_ignore_case(&args[i], "NODE") && i + 1 < args.len() {
            match parse_u64(&args[i + 1]) {
                Some(n) if n <= u16::MAX as u64 => node = Some(n as u16),
                _ => return Reply::not_int(),
            }
            i += 2;
        } else if eq_ignore_case(&args[i], "SEQ") && i + 1 < args.len() {
            let Some(v) = parse_u64(&args[i + 1]) else {
                return Reply::not_int();
            };
            seq = Some(v);
            i += 2;
        } else {
            return Reply::syntax();
        }
    }
    let res = budget_write(engine, &key.clone(), move |ctx| {
        let bhead = head_or_err(ctx, &key)?;
        let mut hs = bhead.state;
        if let Some(seq) = seq {
            if seq <= hs.op_seq {
                // Idempotent retry: report the capacity that op produced.
                return Ok((Vec::new(), hs.capacity as i64));
            }
        }
        let Some(cap) = hs.capacity.checked_add(amount) else {
            return Err(BudgetErr::Other("ERR capacity overflow".into()));
        };
        hs.capacity = cap;
        match node {
            Some(n) => match hs.alloc.iter_mut().find(|(id, _)| *id == n) {
                Some((_, a)) => *a = a.saturating_add(amount),
                None => {
                    if hs.alloc.len() >= 255 {
                        return Err(BudgetErr::Other("ERR at most 255 escrow nodes".into()));
                    }
                    hs.alloc.push((n, amount));
                    hs.alloc.sort_unstable_by_key(|(id, _)| *id);
                }
            },
            None => {
                // Even spread across existing escrow nodes.
                let n = hs.alloc.len().max(1) as u64;
                let base = amount / n;
                let rem = (amount % n) as usize;
                if hs.alloc.is_empty() {
                    hs.alloc.push((ctx.node_id, amount));
                } else {
                    for (i, (_, a)) in hs.alloc.iter_mut().enumerate() {
                        *a = a.saturating_add(base + u64::from(i < rem));
                    }
                }
            }
        }
        if hs.alloc_total() > hs.capacity as u128 {
            return Err(BudgetErr::Other("ERR allocation exceeds capacity".into()));
        }
        hs.op_seq = seq.unwrap_or(hs.op_seq + 1);
        let hlc = bhead.env.hlc.wrapping_add(1).max(ctx.hlc.now());
        let env = Envelope {
            flags: COLLECTION_HEAD,
            hlc,
            origin: ctx.node_id,
            ttl_deadline_ms: 0,
        };
        let mut payload = head::encode(head::CTYPE_BUDGET, bhead.del_hlc);
        payload.extend_from_slice(&hs.encode());
        let cap_out = hs.capacity as i64;
        Ok((
            vec![(ikey::head_key(&key), env.encode_with(&payload))],
            cap_out,
        ))
    })
    .await;
    match res {
        Ok(cap) => Reply::Int(cap),
        Err(e) => e.reply(),
    }
}

/// BG.RESERVE key amount [TTL ms] [REQID id]
pub async fn reserve(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 3 {
        return Reply::wrong_args("bg.reserve");
    }
    let key = args[1].clone();
    let Some(amount) = parse_u64(&args[2]) else {
        return Reply::not_int();
    };
    let mut ttl: Option<u64> = None;
    let mut reqid = 0u64;
    let mut i = 3;
    while i < args.len() {
        if eq_ignore_case(&args[i], "TTL") && i + 1 < args.len() {
            let Some(v) = parse_u64(&args[i + 1]) else {
                return Reply::not_int();
            };
            ttl = Some(v);
            i += 2;
        } else if eq_ignore_case(&args[i], "REQID") && i + 1 < args.len() {
            let Some(v) = parse_u64(&args[i + 1]) else {
                return Reply::not_int();
            };
            reqid = v;
            i += 2;
        } else {
            return Reply::syntax();
        }
    }
    if reqid != 0 {
        if let Some(prev) = engine.budget_reqids.lock().get(&key, reqid) {
            return grant_reply(prev);
        }
    }
    if let Err(e) = ensure_grant_ready(engine, &key).await {
        return e.reply();
    }
    let cfg = GrantCfg {
        default_ttl_ms: engine
            .budget_default_ttl_ms
            .load(std::sync::atomic::Ordering::Relaxed),
        max_ttl_ms: engine
            .budget_max_ttl_ms
            .load(std::sync::atomic::Ordering::Relaxed),
        grace_ms: engine
            .budget_reclaim_grace_ms
            .load(std::sync::atomic::Ordering::Relaxed),
    };
    let k = key.clone();
    let res = budget_write(engine, &key, move |ctx| {
        grant_local(ctx, &cfg, &k, amount, ttl, reqid)
    })
    .await;
    match res {
        Ok(grant) => {
            if reqid != 0 {
                engine
                    .budget_reqids
                    .lock()
                    .put(key.clone(), reqid, grant.clone());
            }
            grant_reply(grant)
        }
        // Local escrow exhausted: forwarding to peers with headroom is the
        // Phase-2 mesh path; without a peer layer the answer is fail-closed.
        Err(BudgetErr::Exhausted) => forward_reserve(engine, &key, amount, ttl, reqid).await,
        Err(e) => e.reply(),
    }
}

async fn forward_reserve(
    engine: &Arc<Engine>,
    key: &[u8],
    amount: u64,
    ttl: Option<u64>,
    reqid: u64,
) -> Reply {
    let peer = engine.budget_peer.read().clone();
    let Some(peer) = peer else {
        return BudgetErr::Exhausted.reply();
    };
    // Candidates: escrow nodes from the head, self excluded. Head is read
    // node-locally; the lease-checked refresh happened in ensure_local.
    let k = key.to_vec();
    let candidates: Vec<u16> = engine
        .store
        .run_key(key, move |ctx| {
            head_or_err(ctx, &k)
                .map(|b| {
                    b.state
                        .alloc
                        .iter()
                        .map(|(n, _)| *n)
                        .filter(|n| *n != ctx.node_id)
                        .collect()
                })
                .unwrap_or_default()
        })
        .await;
    let ttl_ms = ttl.unwrap_or(0); // 0 = peer applies its defaults
    for node in candidates {
        match peer.reserve_remote(node, key, amount, ttl_ms, reqid).await {
            Ok(grant) => {
                if reqid != 0 {
                    engine
                        .budget_reqids
                        .lock()
                        .put(key.to_vec(), reqid, grant.clone());
                }
                return grant_reply(grant);
            }
            Err(BudgetErr::Exhausted) | Err(BudgetErr::TryAgain(_)) => continue,
            Err(e) => return e.reply(),
        }
    }
    BudgetErr::Exhausted.reply()
}

fn grant_reply(g: BudgetGrant) -> Reply {
    Reply::Map(vec![
        (Reply::bulk_str("token"), Reply::Bulk(g.token.into_bytes())),
        (Reply::bulk_str("amount"), Reply::Int(g.amount as i64)),
        (
            Reply::bulk_str("deadline"),
            Reply::Int(g.deadline_ms as i64),
        ),
    ])
}

/// Shared entry for BG.COMMIT / BG.RELEASE / BG.DRAW: parse the token, route
/// to the issuer (self → local; other node → mesh forward; no mesh →
/// fail closed with TRYAGAIN).
async fn close_cmd(
    engine: &Arc<Engine>,
    args: &[Vec<u8>],
    op_name: &str,
    op: CloseOpSpec,
) -> Reply {
    let key = args[1].clone();
    let Some(tid) = TokenId::parse(&args[2]) else {
        return Reply::err("ERR invalid token id");
    };
    let self_node = engine.store.node_id;
    if tid.node != self_node {
        let peer = engine.budget_peer.read().clone();
        let Some(peer) = peer else {
            return BudgetErr::TryAgain("issuer unreachable (token was granted by another node)")
                .reply();
        };
        let (spent, draw) = match op {
            CloseOpSpec::Commit(s) => (s, None),
            CloseOpSpec::Release => (None, None),
            CloseOpSpec::Draw(n) => (None, Some(n)),
        };
        return match peer
            .close_remote(tid.node, &key, &args[2], spent, draw)
            .await
        {
            Ok(v) => Reply::Int(v as i64),
            Err(e) => e.reply(),
        };
    }
    let _ = op_name;
    let grace = engine
        .budget_reclaim_grace_ms
        .load(std::sync::atomic::Ordering::Relaxed);
    let op = match op {
        CloseOpSpec::Commit(s) => CloseOp::Commit(s),
        CloseOpSpec::Release => CloseOp::Release,
        CloseOpSpec::Draw(n) => CloseOp::Draw(n),
    };
    let k = key.clone();
    let res = budget_write(engine, &key, move |ctx| {
        close_local(ctx, grace, &k, tid, op)
    })
    .await;
    match res {
        Ok(Ok(v)) => Reply::Int(v as i64),
        Ok(Err(e)) | Err(e) => e.reply(),
    }
}

pub(crate) enum CloseOpSpec {
    Commit(Option<u64>),
    Release,
    Draw(u64),
}

/// BG.COMMIT key token [spent]
pub async fn commit(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 3 || args.len() > 4 {
        return Reply::wrong_args("bg.commit");
    }
    let spent = if args.len() == 4 {
        match parse_u64(&args[3]) {
            Some(v) => Some(v),
            None => return Reply::not_int(),
        }
    } else {
        None
    };
    close_cmd(engine, args, "bg.commit", CloseOpSpec::Commit(spent)).await
}

/// BG.RELEASE key token
pub async fn release(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 3 {
        return Reply::wrong_args("bg.release");
    }
    close_cmd(engine, args, "bg.release", CloseOpSpec::Release).await
}

/// BG.DRAW key token amount
pub async fn draw(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 4 {
        return Reply::wrong_args("bg.draw");
    }
    let Some(n) = parse_u64(&args[3]) else {
        return Reply::not_int();
    };
    close_cmd(engine, args, "bg.draw", CloseOpSpec::Draw(n)).await
}

/// BG.INFO key — node-local view; may lag replication (documented).
pub async fn info(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 2 {
        return Reply::wrong_args("bg.info");
    }
    engine.ensure_local(&args[1]).await;
    let key = args[1].clone();
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let bhead = match head_or_err(ctx, &key) {
                Ok(b) => b,
                Err(e) => return e.reply(),
            };
            let hs = &bhead.state;
            let window = current_window(ctx, hs);
            // Per-(node, epoch) ledgers of the current gen (current window
            // in window mode).
            let (kind, fixed) = if hs.mode == MODE_WINDOW {
                (ikey::BUDGET_WINDOW_SLOT, Some(window))
            } else {
                (ikey::BUDGET_SLOT, None)
            };
            let prefix = {
                let mut suffix = Vec::with_capacity(17);
                suffix.push(kind);
                suffix.extend_from_slice(&hs.gen.to_be_bytes());
                if let Some(w) = fixed {
                    suffix.extend_from_slice(&w.to_be_bytes());
                }
                ikey::prefixed(ikey::Tag::Budget, &key, &suffix)
            };
            let node_off = if fixed.is_some() { 17 } else { 9 };
            let mut outstanding: u128 = 0;
            let mut nodes: Vec<Reply> = Vec::new();
            scan_prefix(ctx, &prefix, |k, v| {
                let Some(p) = ikey::parse(k) else { return true };
                if p.suffix.len() < node_off + 10 {
                    return true;
                }
                let node = u16::from_be_bytes(p.suffix[node_off..node_off + 2].try_into().unwrap());
                let epoch =
                    u64::from_be_bytes(p.suffix[node_off + 2..node_off + 10].try_into().unwrap());
                if let Some((env, pay)) = Envelope::decode(v) {
                    if !env.is_tombstone() {
                        if let Some(st) = SlotState::decode(pay) {
                            outstanding += st.outstanding();
                            nodes.push(Reply::Array(vec![
                                Reply::Int(node as i64),
                                Reply::Int(epoch as i64),
                                Reply::Int(hs.alloc_for(node) as i64),
                                Reply::Int(st.granted as i64),
                                Reply::Int(st.returned as i64),
                            ]));
                        }
                    }
                }
                true
            });
            // Open tokens of the current gen.
            let tprefix = ikey::budget_kind_prefix(&key, ikey::BUDGET_TOKEN, hs.gen);
            let mut open_tokens = 0i64;
            let mut open_amount: u128 = 0;
            scan_prefix(ctx, &tprefix, |_k, v| {
                if let Some((env, pay)) = Envelope::decode(v) {
                    if !env.is_tombstone() {
                        if let Some(st) = TokenState::decode(pay) {
                            if st.rank < RANK_FOLDED {
                                open_tokens += 1;
                                open_amount += (st.amount - st.spent.min(st.amount)) as u128;
                            }
                        }
                    }
                }
                true
            });
            let available = (hs.capacity as u128).saturating_sub(outstanding);
            let mut pairs = vec![
                (Reply::bulk_str("capacity"), Reply::Int(hs.capacity as i64)),
                (
                    Reply::bulk_str("mode"),
                    Reply::bulk_str(if hs.mode == MODE_WINDOW {
                        "window"
                    } else {
                        "pool"
                    }),
                ),
                (
                    Reply::bulk_str("generation"),
                    Reply::bulk_str(format!("{:x}", hs.gen)),
                ),
                (Reply::bulk_str("op-seq"), Reply::Int(hs.op_seq as i64)),
                (
                    Reply::bulk_str("default-ttl-ms"),
                    Reply::Int(hs.default_ttl_ms as i64),
                ),
                (
                    Reply::bulk_str("max-ttl-ms"),
                    Reply::Int(hs.max_ttl_ms as i64),
                ),
                (
                    Reply::bulk_str("max-amount"),
                    Reply::Int(hs.max_amount as i64),
                ),
                (
                    Reply::bulk_str("outstanding"),
                    Reply::Int(outstanding.min(i64::MAX as u128) as i64),
                ),
                (
                    Reply::bulk_str("available"),
                    Reply::Int(available.min(i64::MAX as u128) as i64),
                ),
                (Reply::bulk_str("open-tokens"), Reply::Int(open_tokens)),
                (
                    Reply::bulk_str("open-amount"),
                    Reply::Int(open_amount.min(i64::MAX as u128) as i64),
                ),
                (Reply::bulk_str("nodes"), Reply::Array(nodes)),
            ];
            if hs.mode == MODE_WINDOW {
                pairs.insert(
                    2,
                    (
                        Reply::bulk_str("period-ms"),
                        Reply::Int(hs.period_ms as i64),
                    ),
                );
                pairs.insert(3, (Reply::bulk_str("window"), Reply::Int(window as i64)));
            }
            Reply::Map(pairs)
        })
        .await
}

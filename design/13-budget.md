# 13 — Distributed Budgets (`BG.*`)

Many workers on many nodes draw from a shared budget and must **never
overspend — even with the cluster in disarray** (partitions, crashes,
split-brain, PVC loss). A central actor creates and tops up budgets; workers
acquire *tokens* (reservations) with deadlines, spend, and give them back.

marekvs is AP and coordination-free: no consensus, no CAS, any node accepts
writes, replication is async. A shared counter therefore cannot enforce a
ceiling — partitioned decrements sum on heal. Budgets instead use the
**escrow (demarcation) method**: capacity is pre-split into per-node
allocations, and every grant decision is made by a *single writer* against
its own ledger. Safety never depends on seeing another node's state in time.

**Invariant.** For each budget generation:
`Σ(outstanding reservations) + Σ(accepted spend) ≤ capacity`, always.
When funds cannot be granted *safely*, commands **fail closed**
(`-BUDGETEXHAUSTED` / `-TRYAGAIN`) rather than guess.

## Command surface

| Command | Reply | Notes |
|---|---|---|
| `BG.CREATE key capacity [MODE POOL \| MODE WINDOW period-ms] [TTL ms] [MAXTTL ms] [MAXAMOUNT n] [NODES id ...] [SEQ n]` | `+OK` | new generation; escrow split across NODES or the partition's home owners |
| `BG.TOPUP key amount [NODE id] [SEQ n]` | `:new-capacity` | adds capacity (even spread or one node) |
| `BG.RESERVE key amount [TTL ms] [REQID id]` | map `{token, amount, deadline}` | grants from local escrow, else forwards to a peer with headroom |
| `BG.COMMIT key token [spent]` | `:remainder-credited` | routed to the token's issuer; default spent = drawn total |
| `BG.RELEASE key token` | `:amount-credited` | returns everything not drawn |
| `BG.DRAW key token amount` | `:remaining` | server-tracked incremental spend inside a token |
| `BG.INFO key` | map | node-local view; may lag replication |
| `BG.RECLAIM key node-id [SEQ n]` | `:amount-redistributed` | admin: fence a dead node, redistribute its unconsumed escrow |

Errors: `-BUDGETEXHAUSTED` (no safely grantable funds — including the
partition answer), `-NOBUDGET`, `-TOKENEXPIRED`, `-TOKENUSED`, `-TRYAGAIN`
(boot fence unsatisfied / issuer unreachable), plain `ERR` for parse/bounds.

Token ids are `gen-hlc-node-epoch` (lowercase hex) — self-describing routing:
any node can tell who must process a COMMIT/DRAW.

## Storage model

All records live under the budget's user key (`Tag::Budget = b'b'`) — one
partition, one shard thread per node, so multi-record updates are node-locally
atomic in one ondaDB txn:

```
head        [pid][b'M'][klen][userkey]                    ctype 7; HeadState tail
slot        [pid][b'b'][klen][userkey][b'L'][gen][node][epoch]
window slot [pid][b'b'][klen][userkey][b'W'][gen][window][node][epoch]
token       [pid][b'b'][klen][userkey][b'T'][gen][hlc][node][epoch]
```

- **Head** (LWW): capacity, mode, per-node alloc map, fence map, per-token
  bounds, a monotone `op_seq`, and the **generation** (= the head HLC at
  CREATE). Written only by the single logical central actor; every admin
  write is an absolute value guarded by `op_seq` (at-least-once retries are
  no-ops). Σ alloc ≤ capacity is checked in u128 at every write.
- **Slot** (`[granted u64][returned u64]`): the escrow ledger of one
  `(gen, node, epoch)`. **Written only by that live incarnation**, so the
  merge — pointwise max — can never lose an acked grant or credit.
  `outstanding = granted − returned`.
- **Token**: rank lattice `open(0) < closing(1) < folded(2)` — a higher rank
  absorbs regardless of HLC (LWW only within a rank). *Folded* carries the
  outcome (state, accepted spend, credited escrow) and sets the envelope
  TOMBSTONE flag, inheriting `gc_grace` retention and the rejoin
  resurrection machinery. The **deadline lives in the payload**, never the
  envelope TTL — replica expiry sweepers must not destroy pre-fold state
  (they skip tag `b'b'` entirely).

Merges are routed by the ikey tag in `write_merged` and property-tested in
`crates/marekvs-core/tests/merge_laws.rs` (commutative/associative/
idempotent; folded absorbs arbitrarily-later open rewrites; permuted slot
snapshots never lose a grant).

## The five local rules

1. **Single-writer slots.** Only the live incarnation `(node, epoch)` writes
   its own slots, on its shard thread, after a u128-checked headroom test
   over all its epochs. Other nodes' replication lag is irrelevant to the
   `≤` direction.
2. **Durable-before-publish.** The ordinary write path feeds the replication
   ring at commit time, *before* the interval fsync — a budget grant that
   escaped to a replica and was then lost in a crash would let the restarted
   node re-grant the same funds (max-merge collapses the two grants into one
   ledger value while both tokens survive). Budget txns therefore commit
   with the hook suppressed, `sync_wal` off the shard thread, then push
   their ops to the ring manually — anything a peer ever sees is on the
   issuer's disk first. There is deliberately **no config toggle** for this.
3. **Issuer-only token transitions.** COMMIT/RELEASE/DRAW route to the
   issuing node (mesh forward; `-TRYAGAIN` if unreachable). Each fold
   credits the slot in the same txn; deadlines are enforced by the issuer's
   clock alone, so clock skew can only move a boundary commit between
   "accepted" and `-TOKENEXPIRED`, never double-credit.
4. **Absolute admin writes.** CREATE/TOPUP/RECLAIM write the whole head with
   a monotone `op_seq` (optional client `SEQ` for exactly-once retries). A
   torn multi-record alloc state is structurally impossible.
5. **Generations live in the keys.** Slot and token keys embed the
   generation, and slot/token keys embed the issuer's **store epoch**
   (minted per empty data dir). A partitioned issuer folding old tokens
   cannot leak credits into a re-created budget; a NodeId reused with a
   fresh PVC cannot collide with its dead incarnation's records.

## Lifecycle

- **RESERVE** at node *i*: opportunistically fold up to 16 of *i*'s expired
  tokens (past `deadline + budget-reclaim-grace-ms`), GC out-of-reach window
  slots, headroom-check, bump `granted`, write the open token — one txn, one
  WAL sync, publish, ack. Insufficient local escrow → forward over the mesh
  to the alloc nodes in turn (each grants from *its own* slot); nobody can →
  `-BUDGETEXHAUSTED`. `REQID` gives at-least-once clients dedup against the
  issuer's in-memory LRU (a crash forgets it; the orphan is reclaimed at its
  deadline — the universal backstop).
- **Boot grant-fence**: a fresh-epoch node (empty data dir) must fetch the
  budget collection from a reachable owner before its first grant — its
  earlier incarnation's grants exist only on replicas. No reachable owner →
  `-TRYAGAIN`, fail closed.
- **COMMIT spent**: issuer folds the token (`committed`), credits
  `amount − spent` back. **RELEASE**: credits everything undrawn.
  **Expiry**: after `deadline + grace` the issuer folds unreturned tokens as
  `expired`, crediting the full undrawn amount back (auto-reclaim policy —
  a crashed worker's actual external spend is unknowable; workers MUST stop
  spending against a token at its deadline). A late COMMIT gets
  `-TOKENEXPIRED` and its spend is NOT accounted.
- **Window mode**: `W = hlc_phys_ms / period` (HLC physical time is
  process-monotone; raw wall clock is not). Ledgers are per
  `(gen, W, node, epoch)`; each window starts fresh. Folds credit the
  token's *grant* window, so late commits never inflate the current window.
  Owners tombstone their own out-of-reach window slots
  (`max_ttl + 2×grace` behind, +2 windows); replica sweepers never touch
  them. Fixed-window boundary semantics: up to 2× the per-window allowance
  can land in a short wall interval straddling a boundary.
- **RECLAIM** (dead node): operator-attested (AP: liveness is unverifiable
  here) after waiting `max token TTL + grace + AE margin`. The target's
  consumption is `max(Σ token-derived, Σ slot-derived)` — tokens are single
  records (never torn by per-record AE repair) and cover stranded-open
  reservations; slots cover folded tokens already GC'd. The target is
  fenced (it can never grant again this generation) and the difference is
  redistributed. **Residual risk** (documented): a live-but-partitioned
  target keeps granting against its stale alloc until the fence replicates —
  bounded operationally by the wait, and by an escrow-lease heartbeat
  (future work, v1.1).

## Interaction with the rest of the surface

- `TYPE` → `budget`; `DEL` works (starts a generation boundary);
  `EXPIRE`-family, `RENAME`, `COPY` → explicit errors (a cloned ledger would
  be an unfoldable orphan that double-counts escrow).
- A plain `SET` shadows the budget like it shadows collections (Redis SET
  replaces any type): budget commands fail closed while shadowed; `DEL` of
  the string un-shadows; the ledger underneath is untouched.
- Budget commands suspend (WAL sync, forwarding) and are therefore rejected
  inside Lua scripts by the poll-once driver.
- `FLUSHALL` wipes budgets like everything else.

## Config

| Env | CONFIG GET/SET | Default |
|---|---|---|
| `MAREKVS_BUDGET_DEFAULT_TTL_MS` | `budget-default-ttl-ms` | 30 000 |
| `MAREKVS_BUDGET_MAX_TTL_MS` | `budget-max-ttl-ms` | 3 600 000 |
| `MAREKVS_BUDGET_RECLAIM_GRACE_MS` | `budget-reclaim-grace-ms` | 5 000 (= max clock drift) |
| `MAREKVS_BUDGET_REQID_LRU` | — | 4 096 entries |

Per-budget `TTL`/`MAXTTL`/`MAXAMOUNT` at CREATE override the node defaults.

## AP caveats (read before depending on it)

- **A single reservation must fit one node's escrow.** Capacity 100 split
  across 3 owners means `BG.RESERVE key 50` always fails; concentrate with
  `NODES` (or top up one node) when workers need large single grants.
- **Fail-closed liveness cost**: during a partition only reachable escrow is
  grantable. `-BUDGETEXHAUSTED` can be returned while the other side of the
  partition has plenty — that is the design, not a bug.
- **Auto-reclaim is a contract with workers**: a worker must stop spending
  against a token at its deadline. A commit that cannot reach the issuer in
  time is refunded in full and the worker is told (`-TOKENEXPIRED`); its
  real-world spend is then unaccounted *by the budget* — the trade the
  auto-reclaim policy makes for crashed workers.
- **Single logical central actor** for CREATE/TOPUP/RECLAIM. Two concurrent
  controllers race whole-head LWW; each result is internally consistent, but
  one controller's op is lost. Use `SEQ` from one durable controller.
- `BG.INFO` is a node-local view and can lag replication by up to the
  anti-entropy bound.
- **Cost**: every acked budget mutation performs one WAL fsync (coalescing
  under concurrency). Budgets are a control-plane primitive, not a
  data-plane counter — use INCR/PN-counters for statistics.
- Escrow on a node that leaves the owner set strands (fail-closed) until
  `BG.RECLAIM` or a topped-up rebalance; live two-phase rebalance is v1.1.

## Testing

- Merge laws: `crates/marekvs-core/tests/merge_laws.rs` (`budget_records`
  module) — lattice laws + the targeted double-credit/tear traces.
- Engine: `crates/marekvs-engine/tests/budget.rs` — full command matrix,
  generation fencing, epoch boot-fence, restart, replay/tear convergence,
  window rollover/GC, RECLAIM accounting.
- Cluster: `tests/chaos/chaos_test.sh budget_no_overspend` — workers hammer
  reserve/commit/release across nodes under SIGKILL rounds; the oracle
  asserts every node's ledger stays within capacity and no accepted spend is
  lost, and that impossible reservations fail closed promptly.

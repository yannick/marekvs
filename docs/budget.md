---
title: Distributed budgets
description: BG.* — escrow-based budgets that many workers draw from without ever overspending, even under partitions and crashes.
status: stable
---

The `BG.*` command family gives you a **shared spending budget** that any
number of workers on any node can draw from, with a hard guarantee:

> Σ(outstanding reservations) + Σ(accepted spend) ≤ capacity — **always**,
> through partitions, crashes, split-brain, and lost disks.

marekvs is AP, so this cannot work like a counter (partitioned decrements
merge additively). Budgets use the **escrow method**: capacity is pre-split
across nodes, and every grant decision is made by exactly one node against
its own share. When funds can't be granted *safely*, you get an error —
never an overspend.

## The worker loop

```sh
# central system, once:
redis-cli BG.CREATE jobs:budget 10000 TTL 30000 MAXAMOUNT 500

# each worker:
redis-cli BG.RESERVE jobs:budget 100            # → token, amount, deadline
# ... do up to 100 units of external work before the deadline ...
redis-cli BG.COMMIT jobs:budget <token> 73      # spent 73 → 27 flows back
```

- `BG.RESERVE key amount [TTL ms] [REQID id]` — returns a `token`, the
  granted `amount`, and an absolute `deadline`. After the deadline the token
  is dead: **stop spending against it**. `REQID` makes retries idempotent.
- `BG.COMMIT key token [spent]` — reports the final spend; the remainder
  returns to the budget. Omit `spent` to accept everything drawn so far.
- `BG.RELEASE key token` — changed your mind; everything undrawn returns.
- `BG.DRAW key token amount` — optional server-tracked incremental spend
  within a token (`ERR` when it would exceed the reservation).
- If a worker crashes, its token expires and the full undrawn reservation
  flows back automatically after a small grace period.

Admin: `BG.TOPUP key amount [NODE id] [SEQ n]` adds funds;
`BG.INFO key` shows the ledger; `BG.RECLAIM key node-id` recovers the escrow
of a permanently dead node (read the preconditions in the design doc).
`MODE WINDOW period-ms` at CREATE makes the budget refill every period
(a distributed rate limiter with the same never-overspend guarantee).

## What "fail closed" means for you

- Reservations only succeed when some *reachable* node can cover the amount
  from its own escrow share. During a partition you may get
  `-BUDGETEXHAUSTED` even though the other side has funds — by design.
- A single reservation must fit within a single node's share: capacity
  10 000 split across 3 nodes means `BG.RESERVE key 5000` fails. Use
  `NODES` at CREATE (or `TOPUP ... NODE`) to concentrate escrow if workers
  need large single grants.
- A `BG.COMMIT` that reaches the issuer after the token's deadline is
  refused (`-TOKENEXPIRED`) and the reservation is treated as returned —
  the flip side of automatic crash recovery. Workers must respect deadlines.

Errors you should handle: `-BUDGETEXHAUSTED` (back off and retry),
`-TRYAGAIN` (transient: issuer unreachable or node still booting),
`-TOKENEXPIRED`, `-TOKENUSED`.

## Configuration

| Env | CONFIG | Default |
|---|---|---|
| `MAREKVS_BUDGET_DEFAULT_TTL_MS` | `budget-default-ttl-ms` | 30 000 |
| `MAREKVS_BUDGET_MAX_TTL_MS` | `budget-max-ttl-ms` | 3 600 000 |
| `MAREKVS_BUDGET_RECLAIM_GRACE_MS` | `budget-reclaim-grace-ms` | 5 000 |

Full protocol, safety arguments, and AP caveats:
[design/13-budget.md](https://github.com/yannick/marekvs/blob/main/design/13-budget.md).

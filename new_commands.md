# New Redis Commands Implementation Plans

These plans cover the five command groups that fit marekvs' design most
naturally, based on `todo.md` ("Redis compat" section),
`design/02-data-model.md`, `design/03-redis-api.md`, and the current command
implementation under `crates/marekvs-engine/src/cmd/`. None of the target
commands below has a dispatch arm in `cmd/mod.rs` today (verified: they all
fall through to "ERR unknown command"), except where noted.

Common work for every group:

- Add dispatch arms in `crates/marekvs-engine/src/cmd/mod.rs`.
- Add command metadata in `crates/marekvs-engine/src/cmd/command_docs.rs`.
- Update `Engine::is_write_command` (lib.rs:221) and `Engine::parallel_safe`
  (lib.rs:304) where needed, **and** the classification tests next to them
  (lib.rs:559/591 assert write-gating per command name).
- Add focused command tests under `crates/marekvs-engine/tests/`.
- Use official Redis command docs as the syntax/reply-shape compatibility
  source, then document any AP or disk-native divergence explicitly.

Suggested order (design/03 tier first, then effort): §3 lists (v1 gap, S),
§5 COPY/OBJECT (v1 gap, S–M), §4 streams (v1 gap, M), §2 sorted sets (v1
subset first, then v1.1 set-ops/blocking, L overall), §1 hash TTL (v1.1, M).

## 1. Hash / Member TTL Commands

**Tier:** v1.1 per design/03:53. **Effort:** M — the per-member TTL lifecycle
already exists end-to-end; this is mostly parsing + wrappers.

Target commands:

- `HEXPIRE`, `HPEXPIRE`, `HEXPIREAT`, `HPEXPIREAT`
- `HTTL`, `HPTTL`, `HEXPIRETIME`, `HPEXPIRETIME`, `HPERSIST`
- `HGETEX`, `HSETEX`
- `HGETDEL` (Redis 8.0, same family — **adopted**: implementation is an
  HMGET-shaped read + per-field observed-remove via the existing `HDEL`
  machinery). First step of this group: add HGETDEL (and the whole HEXPIRE
  family) to the design/03 hash matrix so the contract stays the source of
  truth.

Why this fits:

- Fields and members are already independent records with envelope TTLs
  (`ttl_deadline_ms`, design/02 §Envelope).
- `design/02` §TTL says field-level TTL (HEXPIRE, Redis 7.4) "falls out for
  free: fields are separate records with their own envelopes".
- The code already has generic member TTL machinery in `cmd/generic.rs`:
  `member_element_key`, `set_member_deadline` (restamp envelope / expire-now
  as observed-remove), and `member_ttl` — wired up as `EXPIREMEMBER`,
  `EXPIREMEMBERAT`, `PEXPIREMEMBERAT`, and 3-arg `TTL`/`PTTL key member`
  (`cmd/mod.rs:74-80`; there is no `PEXPIREMEMBER`, matching KeyDB).

Plan:

1. Promote the member TTL helpers in `cmd/generic.rs`
   (`member_element_key`, `set_member_deadline`, the `member_ttl` read path)
   to reusable `pub(crate)` helpers covering:
   - read member envelope
   - set deadline
   - clear deadline (PERSIST = restamp with `ttl_deadline_ms = 0`)
   - expire now (past deadline → observed-remove, already in
     `set_member_deadline`)
   - compute TTL/expiretime
2. Implement hash wrappers in `cmd/hash.rs`, restricted to hash fields
   (`member_element_key` already dispatches on head ctype; the hash variants
   should WRONGTYPE on non-hash instead of returning "no member").
   Reuse existing OR-element visibility checks and dot tombstone behavior.
3. Support conditional TTL update logic:
   - `NX`: only if no TTL exists
   - `XX`: only if a TTL exists
   - `GT`: only if the new deadline is greater
   - `LT`: only if the new deadline is lower
   Verify exact non-volatile-field behavior and the per-field status codes
   (-2 no field, 0 condition failed, 1 set, 2 deleted) against `redis-server`.
4. Implement `HGETEX`:
   - parse optional `EX`, `PX`, `EXAT`, `PXAT`, or `PERSIST`
   - parse `FIELDS numfields field [field ...]`
   - return values in the same shape as `HMGET`
   - apply TTL/PERSIST only to fields that exist
5. Implement `HSETEX`:
   - parse `FNX` / `FXX`
   - parse optional expiration or `KEEPTTL`
   - parse `FVS numfields field value [field value ...]` (Redis 8.0 uses the
     `FVS` token here, not `FIELDS` — `FIELDS` is the HGETEX/HGETDEL form)
   - check all field existence first so `FNX` / `FXX` succeeds or fails as a
     unit — this is genuinely atomic per key, not best-effort: a collection's
     `pid` derives from the user key only (design/02 §Partitioning), so every
     field of one hash lives on one shard thread
   - write each field with the requested TTL behavior
6. AP caveats to document (not blockers — existing precedent):
   - `NX/XX/GT/LT/FNX/FXX` are check-then-act against **locally visible**
     state, like `SETNX`/`HSETNX`/`ZADD NX` today. There is no CAS in AP
     (design/03: WATCH → error); concurrent cross-node writers resolve by
     envelope LWW / OR-element merge.
   - A TTL change restamps the whole element envelope, so it races a
     concurrent cross-node `HSET` of the same field as LWW: the later
     `(hlc, origin)` wins wholesale (either the TTL change or the value
     write is lost). Same semantics as `EXPIREMEMBER` today.
   - `RENAME`/`COPY` currently drop per-member TTLs (see §5, TTL caveat).
     **Decision: fix it in the shared helper as a prerequisite of this
     group** — extend the OR-element branch of `collect_key_records` to
     carry `ttl_deadline_ms` through `element_add` (add a
     `element_add_with_deadline` variant or a deadline parameter). Field
     TTLs become user-visible with this group, so silently dropping them on
     RENAME/COPY would look like a bug, not a limitation. Both commands
     benefit from one fix.
7. Tests:
   - wrong type
   - absent key
   - absent field
   - expired field
   - condition flags
   - multi-field status arrays
   - `HGETEX` return shape
   - `HSETEX` all-or-nothing condition behavior
   - replication convergence of field expiry
   - syntax/reply-shape diff against `redis-server`

## 2. Sorted Set Completion

**Tier:** split. `ZRANDMEMBER`, `ZREMRANGEBYRANK`, `ZREMRANGEBYLEX`,
`ZLEXCOUNT`, `ZRANGESTORE`, `ZMPOP` are **v1 promises** (design/03:72-73,
todo.md); `ZUNION/ZINTER/ZDIFF (+STORE/CARD)` and `BZPOPMIN/BZPOPMAX/BZMPOP`
are v1.1 (design/03:74-75). **Effort:** L for the whole section; the v1
subset alone is M.

Target commands:

- `ZRANDMEMBER`
- `ZREMRANGEBYRANK`, `ZREMRANGEBYLEX`, `ZLEXCOUNT`
- `ZRANGESTORE`
- `ZMPOP`
- `ZUNION`, `ZINTER`, `ZDIFF`
- `ZUNIONSTORE`, `ZINTERSTORE`, `ZDIFFSTORE`
- `ZINTERCARD`
- `BZPOPMIN`, `BZPOPMAX`, `BZMPOP`

(`ZREMRANGEBYSCORE` already exists — `cmd/zset.rs:810`.)

Why this fits:

- Zsets already use per-member OR elements plus a node-locally derived score
  index (`'Z'` keys, design/02 §Internal key layouts).
- `ZPOPMIN`, `ZPOPMAX`, `ZRANGE`, `ZRANK`, `ZCOUNT`, and score scans already
  exist in `cmd/zset.rs`, with the helpers below already present:
  `scored_members`, `scored_members_limited`, `member_score`, `write_member`,
  `remove_member`, `pop_scored_candidates`, `slice_by_index`, `emit`.
- Multi-key commands can follow the existing documented caveat: not atomic
  across shards (design/03 §Cross-cutting semantics).

Plan:

1. Make the internal helpers above reusable where they are still private,
   and add `try_zpop(ctx, key, min_or_max, count)` extracted from `zpop`
   (`cmd/zset.rs:765`) so `ZMPOP`/blocking variants share it.
2. Implement easy wins first (all v1 promises):
   - `ZRANDMEMBER`: mirror `SRANDMEMBER` (`cmd/set.rs:306`, which uses
     `set_members_limited`); use `scored_members_limited` for positive counts
   - `ZREMRANGEBYRANK`: `scored_members` + existing `slice_by_index` logic,
     then `remove_member` per victim
   - `ZRANGESTORE`: reuse `ZRANGE` parsing and write selected members/scores
     into the destination zset
3. Decide `BYLEX` scope:
   - current code explicitly rejects `BYLEX` (`cmd/zset.rs:541`,
     `"ERR BYLEX is not supported"`; also tracked in todo.md)
   - implement lex commands by sorting/filtering visible members by member
     bytes, not by score index
   - accept O(N) behavior and document it
   - note the Redis contract carries over unchanged: BYLEX is only
     well-defined when all members share one score, which is the caller's
     responsibility — no new AP hazard
4. Implement `ZMPOP`:
   - parse `numkeys key [key ...] MIN|MAX [COUNT count]`
   - check keys in user order
   - pop from the first non-empty zset
   - reply `[key, [[member, score]...]]`, nil when nothing popped
5. Implement union/inter/diff:
   - materialize inputs into `HashMap<member, score>`
   - support `WEIGHTS`
   - support `AGGREGATE SUM|MIN|MAX`
   - return arrays with optional `WITHSCORES`
   - store variants must **replace** the destination: clobber via
     `generic::del_key` (as `RENAME` does), then `write_member` per entry so
     the score index is maintained
6. Implement blocking variants:
   - copy the existing list blocking polling pattern (`cmd/list.rs:895-1010`:
     `POLL_MS = 50`, deadline loop, `engine.ensure_local(key)` per iteration
     for interest read-through)
   - never block shard threads (the poll sleeps on the connection task)
   - `BZMPOP` should behave like `ZMPOP` once any input zset is non-empty
   - reply shapes differ: `BZPOPMIN/MAX` → flat `[key, member, score]`;
     `BZMPOP` → `ZMPOP` shape (pin both shapes in tests — an easy one to
     get wrong when sharing the inner pop helper)
7. AP caveats to document:
   - blocking zset pops inherit the BLPOP caveat verbatim (design/03, last
     row): under partition two nodes may both pop the same member —
     racy-by-design, AP
   - `Z*STORE` destination writes are not atomic with the source reads;
     concurrent writers to the destination interleave per ORSWOT/LWW merge
8. Tests:
   - score ordering
   - equal-score member ordering
   - `WITHSCORES`
   - `COUNT`
   - empty and missing keys
   - wrong types
   - multi-key priority order
   - store overwrite behavior
   - cross-shard non-atomic caveat
   - blocking timeout and eventual wakeup through polling
   - `BZPOPMIN/MAX` flat reply shape vs `BZMPOP` nested shape

## 3. List Multi-Pop

**Tier:** `LMPOP` is a v1 promise, `BLMPOP` is "planned" (design/03:85,87,
todo.md). **Effort:** S — the building blocks all exist.

Target commands:

- `LMPOP`
- `BLMPOP`

Why this fits:

- Lists are already per-position element records (design/02 §Lists v1.2).
- `LPOP`, `RPOP`, `BLPOP`, `BRPOP`, `LMOVE`, and `BLMOVE` already exist, and
  `LPOP key count` already loops `do_pop` (`cmd/list.rs:429`) — the multi-pop
  inner logic is essentially written.
- Blocking list commands already use a polling implementation
  (`POLL_MS = 50`, `cmd/list.rs:900`) that never sleeps on shard threads;
  `try_pop_one` (`cmd/list.rs:915`) is the single-key probe to generalize.

Plan:

1. Factor `cmd/list.rs` pop logic into:
   - `try_pop_many(engine, key, left, count) -> Result<Vec<Vec<u8>>, WrongType>`
     (lift the count-loop out of `pop`)
   - keep `do_pop` as the shard-local inner helper
2. Implement `LMPOP`:
   - parse `numkeys`, key list, `LEFT|RIGHT`, optional `COUNT count`
     (Redis rejects `count <= 0` with an error)
   - scan keys in user order, `engine.ensure_local(key)` before each probe
     like `bpop` does
   - return nil (RESP2 null array) if all lists are empty
   - otherwise return `[key, [elements...]]`
3. Implement `BLMPOP`:
   - parse timeout first (`BLMPOP timeout numkeys key ... LEFT|RIGHT [COUNT n]`)
   - reuse the non-blocking `LMPOP` inner logic
   - use the same polling pattern as `BLPOP` / `BRPOP`
   - return nil on timeout
4. Preserve existing AP caveat:
   - cross-node pushes become visible after replication/read-through/polling;
     under partition two nodes may pop the same element (BLPOP caveat,
     design/03)
   - no shard thread blocks
5. Tests:
   - invalid `numkeys`
   - `COUNT 0` and negative count → error
   - key priority order
   - left/right behavior
   - timeout
   - wrong type
   - same key repeated in input list
   - replicated push becoming visible to blocked pop within the polling window

## 4. Stream Metadata Basics

**Tier:** v1 promises (design/03:102, todo.md). **Effort:** M — the commands
are small, but this introduces the versioned head payload.

Target commands:

- `XSETID`
- `XINFO STREAM`

Why this fits:

- Streams already store entries as immutable per-id records.
- `XDEL` and `XTRIM` already tombstone entries.
- `design/02` (head-key payload, ctype 4) reserves stream state — `last_id`,
  max-len config, group-state blob — in the head payload, but the current
  implementation writes heads with empty state (`ensure_head` →
  `head::encode(ctype, 0)`) and derives the last id by scanning entries
  (`stream_last_id`, `cmd/stream.rs:163`, used by `xadd` and `xread`).

Plan:

1. Introduce a versioned stream-head payload codec in `cmd/stream.rs`:
   - `last_id`
   - `entries_added`
   - `max_deleted_id`
   - reserved/future group-state bytes
2. Update `XADD`:
   - read head metadata first
   - fall back to the `stream_last_id` scan for old or empty metadata
   - update `last_id` and `entries_added`
   - keep explicit-id monotonic checks — but check against
     `max(head.last_id, newest visible entry id)`, not the head alone
     (see hazards below)
3. Update `XDEL` / `XTRIM`:
   - update `max_deleted_id` where appropriate
   - preserve existing per-entry tombstone behavior
4. Implement `XSETID`:
   - parse `key last-id`, optional `ENTRIESADDED n`, optional
     `MAXDELETEDID id`
   - validate stream ID syntax
   - error `ERR no such key` if the stream head doesn't exist (Redis
     behavior), and reject an id below the current top entry
   - update head payload via LWW head write
5. Implement `XINFO STREAM`:
   - `length`, `last-generated-id`, `max-deleted-entry-id`, `entries-added`,
     `first-entry`, `last-entry`
   - `radix-tree-keys` / `radix-tree-nodes` placeholders; `groups` = 0 until
     consumer groups land (they are absent from dispatch entirely — todo.md
     §CRDT gaps)
   - compute first/last with current prefix scans
6. AP hazards — the head is a **single LWW register** (design/02 §LWW
   registers), which shapes what this metadata can promise:
   - `entries-added` cannot be an exact cluster-wide counter: concurrent
     XADDs on two nodes each bump their local copy and the LWW merge keeps
     only one. Document it as approximate (or derive length-ish numbers from
     scans); exact would need PN-counter-style head state — out of scope.
   - `last_id` in the head can lag or regress relative to merged entries (a
     stale head write with a later HLC wins). Reads and the XADD monotonic
     guard must treat the head value as a hint:
     `effective_last = max(head.last_id, scan)`. Entry-id **uniqueness** is
     unaffected — auto-ids embed the origin node in the sequence half
     (design/02 §Streams).
   - `XSETID` itself is a legitimate LWW write and merges fine.
   - Cross-cutting: `RENAME`/`COPY` rebuild heads as `head::encode(ctype, 0)`
     (`cmd/generic.rs:collect_key_records`), wiping stream state.
     **Decision: extend `collect_key_records` to copy the head payload
     verbatim** (re-stamped envelope, same bytes) once this group introduces
     meaningful head state — the scan fallback self-heals `last_id` but
     silently zeroes `entries_added`/`max_deleted_id`, which XINFO then
     reports as wrong-looking data. Pin it in §5's tests.
7. Tests:
   - empty stream
   - tombstoned entries
   - `XTRIM` metadata
   - `XSETID` followed by `XADD *`
   - explicit lower ID rejection
   - RESP2/RESP3 reply shape
   - replication of head metadata

## 5. Generic Keyspace Helpers

**Tier:** `COPY` and `OBJECT ENCODING/REFCOUNT/IDLETIME/FREQ` are v1 promises
(design/03:38,40 — OBJECT explicitly as "static/stub answers"); `DUMP`,
`RESTORE`, `MOVE`, `SORT` are v1.1 (design/03:39). **Effort:** S–M for
COPY + OBJECT; the deferred items are separate work.

Target commands:

- `COPY`
- `OBJECT ENCODING`, `OBJECT REFCOUNT`, `OBJECT IDLETIME`, `OBJECT FREQ`
- Later (v1.1): `DUMP`, `RESTORE`, `SORT_RO`, `SORT` — and probably **not**
  `MOVE`: marekvs has a single logical database (SELECT 0 only,
  design/03:150), so MOVE can never succeed; a static error is all it could
  be.

Why this fits:

- `RENAME` already has the core copy machinery in `cmd/generic.rs`:
  `collect_key_records` (line 545) and `rebuild_ikey` (line 615). COPY is
  RENAME minus the source delete; both live in the same module, so the
  private helpers can stay private.
- `OBJECT` is planned as static/stub answers in `design/03`.
- `COPY` improves compatibility without changing the storage model.

Plan:

1. Implement `COPY` by reusing `collect_key_records` / `rebuild_ikey`:
   - read source records on the source shard (values are re-stamped with a
     fresh HLC so the copy wins at the destination — existing behavior)
   - write rebuilt records on the destination shard
   - do not delete source
2. Parse options:
   - `DB destination-db`: accept only `0`, reject other DBs (consistent with
     SELECT)
   - `REPLACE`: if absent and destination exists, return `0`; if present,
     clobber destination first (`del_key`, as RENAME does)
3. TTL behavior — inherit RENAME's, but know what that is:
   - key-level TTLs survive: the head and string/non-OR records are rebuilt
     with their `ttl_deadline_ms`
   - **per-member TTLs are dropped**: OR elements are re-added via
     `element_add`, which takes no deadline (`collect_key_records`,
     OR-element branch). This is a pre-existing RENAME limitation.
     **Decision (see §1.6): fix in the shared helper** — carry the element
     deadline through. Until §1 lands this is invisible to users
     (per-member TTLs only reachable via EXPIREMEMBER), so the fix rides
     with whichever of §1/§5 ships first
   - preserve collection ordering/suffixes (list positions copy intact —
     already handled)
   - keep counter-freezing behavior consistent with `RENAME`
     (counters materialize to a plain string at the destination,
     design/02 §Counters, `collect_key_records` string branch)
   - stream head state: see §4 decision — copy the head payload verbatim
     once §4's versioned head lands
4. Implement `OBJECT` (all static/stub, per design/03):
   - missing key → `ERR no such key` for REFCOUNT/ENCODING/IDLETIME/FREQ
     (Redis errors here; it does **not** return null)
   - `REFCOUNT key`: `1`
   - `ENCODING key`: stable compatibility strings by type
   - `IDLETIME key`: `0`
   - `FREQ key`: **always return the LFU error**
     (`ERR An LFU maxmemory policy is not selected...`) — marekvs never runs
     an LFU policy, and this matches what a default-configured Redis does,
     so probing clients see familiar behavior. Document in design/03.
   - `HELP`: useful static help text
5. Defer `DUMP` / `RESTORE`:
   - design a marekvs-native serialization format (design/03:39 already
     commits to "marekvs-native, not RDB")
   - do not pretend to emit Redis RDB bytes
6. Defer mutating `SORT STORE`:
   - implement `SORT_RO` first if client compatibility requires it
   - materialize list/set/zset values and sort in memory
   - document O(N log N)
7. Tests:
   - cross-type copy
   - TTL preservation (key-level; plus a test pinning the documented
     per-member TTL behavior, whichever way step 3 decides)
   - overwrite behavior
   - `REPLACE` clobber behavior
   - DB rejection
   - HLL, list, stream copy (stream: head-state expectation per §4)
   - `OBJECT` missing-key error behavior
   - command docs and `COMMAND INFO` metadata

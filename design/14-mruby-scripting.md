# 14 — mruby Scripting (Proposal)

**Status: proposal — not implemented.** Adds mruby as a second scripting
language alongside Lua ([11-lua-scripting.md](11-lua-scripting.md)), using
[`mrubyedge`](https://docs.rs/mrubyedge) (v1.1.x), a pure-Rust, Wasm-focused
mruby VM. Lua remains the Redis-compatible default; mruby is an additive,
config-gated surface under new `RUBY.*` command verbs.

## Why mrubyedge (and not the alternatives)

- **Pure Rust** — no C toolchain beyond what vendored Lua already needs; in
  fact none at all for this crate. Memory-safe VM: malformed bytecode can
  error or panic but cannot corrupt the process.
- `mruby-sys`/FFI bindings to the C mruby were rejected: a second C VM with
  its own GC and longjmp-based error handling inside our shard threads is a
  much bigger hazard than a Rust crate.
- Wasm-hosted Ruby (compile scripts to Wasm, run under wasmtime) was
  rejected for v1: it solves sandboxing better but drags in a JIT runtime,
  ~10× the dependency weight, and a worse value-bridge story. Revisit if the
  fuel problem (drawback #2) proves unfixable.

## The one decision everything else hangs on: bytecode, not source

`EVAL` receives Lua *source* and mlua compiles it in-process. mrubyedge
**cannot compile Ruby source** — it executes precompiled RITE bytecode
(`.mrb`, produced offline by `mrbc`; the project's `mec` compiler is
deprecated). There is no maintained pure-Rust mruby compiler.

Options:

| | Approach | Verdict |
|---|---|---|
| A | **Clients ship `.mrb` bytecode**; server never compiles | **Recommended v1.** Zero new server deps. RESP bulk strings are binary-safe, so bytecode travels fine. Burden: users run `mrbc` (pinned version) in their build step — acceptable for the operator-authored scripts this feature targets. |
| B | Server shells out to a bundled `mrbc` binary on `RUBY.LOAD` | Redis-like UX (send source), but adds a platform-specific binary to the image, a subprocess on the load path, and a compiler-version axis to support. Optional Phase 3, config-gated. |
| C | Embed the C mruby compiler via FFI | Rejected — defeats the pure-Rust rationale entirely. |

Consequence: there is no `RUBY.EVAL <source>`. The primary verb takes
bytecode, and the `LOAD`/`EVALSHA` pair is the expected workflow (bytecode
blobs are cached server-side; clients normally send a 40-byte sha).

**Version pinning:** mrubyedge targets a specific RITE format revision
(mruby 3.x lineage). The server MUST reject bytecode whose RITE header
version it doesn't support, with an error naming the expected `mrbc`
version. Pin the supported `mrbc` release in docs and CI.

## Command surface

Mirrors the EVAL family; `EVAL`/`EVALSHA`/`SCRIPT` stay Lua-only for Redis
compatibility.

```
RUBY.EVAL   <mrb-bytecode> numkeys key [key ...] arg [arg ...]
RUBY.LOAD   <mrb-bytecode>                     → sha1 (of the bytecode)
RUBY.EVALSHA <sha1> numkeys key [key ...] arg [arg ...]
RUBY.EXISTS <sha1> [sha1 ...]
RUBY.FLUSH
```

Gated by `MAREKVS_ENABLE_MRUBY=1` (default off): the feature is young and
its execution-limit story (drawback #2) is weaker than Lua's, so opting in
is an operator decision. Disabled → `ERR mruby scripting is disabled`.

## Architecture: reuse the Lua seams

`cmd/script.rs` already separates language-specific from language-agnostic
parts. Refactor the agnostic parts into `cmd/script_common.rs`, then add
`cmd/rubyscript.rs` beside `cmd/script.rs`:

**Shared unchanged** (already language-neutral):
- `bridge_call` — takes `Vec<Vec<u8>>` argv, re-enters `cmd::dispatch` with
  a synthetic authenticated `Session`, `poll_once` + the `CURRENT_SHARD_CTX`
  inline fast-path resolves same-shard calls in one poll; suspension →
  clean error. This is the whole redis-call bridge; mruby gets it for free.
- `script_safe` denylist (`parallel_safe` minus MSET/MGET/blocking/pubsub/
  EVAL-recursion) and `command_docs::extract_keys` undeclared-key
  enforcement.
- The `eval_source` pipeline shape: same-pid CROSSSLOT check over declared
  KEYS (hash-tag discipline), `ensure_local` pre-fetch of every declared
  key *before* entering the shard job, whole script as one `store.run(pid)`
  shard job → same node-local atomicity guarantee as Lua (caveats 1–3 of
  design/11 apply verbatim).
- Script-record storage/replication pattern: raw bytecode stored as an LWW
  string record under a hidden system key, new prefix
  `\x00rbscript:<sha1>`; in-memory `sha → bytecode` map; `RUBY.EVALSHA`
  miss → `ensure_local` the system key → repopulate → else `NOSCRIPT`.
  Identical self-healing story to design/11 caveat 5.

**mruby-specific** (`rubyscript.rs`):

1. **VM lifecycle: fresh VM per invocation** — unlike the reused
   thread-local `SHARD_LUA`. `rite::load(&bytes)` + `VM::open(&mut rite)`
   couples a VM to one bytecode blob, VM construction is cheap pure-Rust
   allocation, and a throwaway VM eliminates cross-script state leakage and
   any GC-growth questions on a long-lived VM. Cache the *validated
   bytecode bytes* per sha (parse/validate once at LOAD time), not VMs.
2. **Bridge injection.** Expose a `Redis` module with
   `mrb_define_module_cmethod(vm, redis, "call", ...)` / `"pcall"`. The
   `RFn` callable coerces `RObject` args (String/Symbol/Integer/Float
   accepted, everything else → type error, matching Lua's coercion rules)
   into `Vec<Vec<u8>>` and calls the shared `bridge_call`. `call` maps
   `Reply::Err` to a raised Ruby `RuntimeError`; `pcall` returns an error
   hash. If `RFn` turns out to be a plain fn pointer that can't capture the
   `Arc<Engine>`, stash it in a thread-local (`CURRENT_SCRIPT_ENGINE`,
   mirroring the existing `CURRENT_SHARD_CTX` pattern) — same trick, same
   file.
3. **KEYS/ARGV**: injected as frozen global constants `KEYS` and `ARGV`
   (Arrays of Strings) before `vm.run()`. **0-indexed, Ruby-idiomatic** —
   `KEYS[0]` is the first key. Deliberately NOT copying Lua's 1-based
   indexing; document loudly, since it's the one porting trap.
4. **Value mapping** (`reply_to_ruby` / `ruby_to_reply`), following the
   spirit of the Redis Lua table:

   | RESP → Ruby | Ruby → RESP |
   |---|---|
   | Int → Integer | Integer → Int |
   | Bulk → String (binary) | String → Bulk |
   | Null/NullArray → nil | nil → Null |
   | Simple "OK" → `"OK"` | true → Int(1), false → Null |
   | Array/Set → Array | Array → Array (stop at first nil) |
   | Err → raised exception (`call`) / `{err: msg}` (`pcall`) | Float → Int (truncate, Redis rule) |
   | Map → flat Array (RESP2 style) | Hash with `:err`/`:ok` key → Err/Simple |

5. **Limits**: wall-clock deadline shares `script_time_limit_ms` (the
   existing `CONFIG SET lua-time-limit` value; add `ruby-time-limit` alias).
   Enforcement is the hard part — see drawback #2.

Dispatch: one new match arm block in `cmd/mod.rs` for the `RUBY.*` verbs;
add them to `is_write_command` (a script may write) exactly as EVAL is
classified today. No server-crate changes.

## Usage sketch

Author and compile (client side, once, in CI or a Justfile recipe):

```ruby
# rate_limiter.rb — sliding-window rate limiter, PN-counter exact
# KEYS[0] = counter key (hash-tagged), ARGV[0] = window seconds, ARGV[1] = limit
count = Redis.call("INCR", KEYS[0])
Redis.call("EXPIRE", KEYS[0], ARGV[0]) if count == 1
count <= ARGV[1].to_i ? 1 : 0
```

```console
$ mrbc -o rate_limiter.mrb rate_limiter.rb        # pinned mrbc version
$ redis-cli -x RUBY.LOAD < rate_limiter.mrb       # -x: last arg from stdin (binary-safe)
"b7a1f9…e3"                                        # sha1 of the bytecode
```

Call it like EVALSHA (keys must share a hash tag, exactly as with Lua):

```console
$ redis-cli RUBY.EVALSHA b7a1f9…e3 1 'rl:{user42}' 60 100
(integer) 1
$ redis-cli RUBY.EVALSHA b7a1f9…e3 1 'rl:{user42}' 60 100
(integer) 1        # …until 100 within the window, then 0
```

A collection-manipulation example, showing the Ruby win (real blocks and
stdlib instead of Lua table loops):

```ruby
# top_k_merge.rb: merge ARGV scores into a zset and trim to K
# KEYS[0] = zset, ARGV = [k, member1, score1, member2, score2, ...]
k = ARGV[0].to_i
ARGV[1..].each_slice(2) do |member, score|
  Redis.call("ZADD", KEYS[0], score, member)
end
Redis.call("ZREMRANGEBYRANK", KEYS[0], 0, -(k + 1))
Redis.call("ZCARD", KEYS[0])
```

`RUBY.EVAL` (inline bytecode, no LOAD) exists for one-shot admin jobs:
`redis-cli -x RUBY.EVAL < job.mrb 1 'key:{tag}' arg1`.

## Issues and drawbacks — honest list

1. **No source-level EVAL.** The `mrbc` build step is a real UX regression
   vs Lua. Every client integration needs a compile step and a pinned
   compiler version; a sha mismatch after an innocent recompile (bytecode
   is not byte-stable across mrbc versions) will surprise people. Docs must
   push the LOAD-once/EVALSHA pattern and CI-compiled artifacts.
2. **No instruction budget or memory cap in mrubyedge — the blocker for
   untrusted use.** mlua gives us a per-10k-instruction hook (20 ms
   deadline) and `set_memory_limit(16 MiB)`. mrubyedge documents neither.
   An infinite loop in Ruby runs unpreemptable *on the shard thread*:
   head-of-line blocking for every key on that shard, indefinitely. This is
   why the feature ships config-gated and documented as
   **trusted-operator-scripts only** until fixed. Fix path: the VM loop is
   plain Rust — contribute a fuel counter (check deadline every N ops)
   upstream, or carry a small vendored fork with one. Phase 2 is exactly
   this work, and GA should be conditional on it.
3. **Panics abort the node.** The workspace builds with `panic = "abort"`.
   A panic inside mrubyedge (immature RITE parser, stdlib edge case) kills
   the whole marekvs process, not just the script. Mitigations: validate
   the RITE header + structural sanity at LOAD time (reject early on the
   client-facing path), fuzz the loader in CI (`cargo fuzz` target on
   `rite::load` + `vm.run`), and treat bytecode as trusted input
   (drawback #2 already forces this posture).
4. **Maturity.** v1.1.x, ~7 % API documentation, stdlib is an explicit
   subset (`COVERAGE.md`), effectively one maintainer, Wasm-first focus (we
   are an embedder, the second-class use case). Behavioral divergence from
   real mruby is possible and only discoverable by testing. Budget for
   reading the source where docs are missing, and for upstreaming fixes.
5. **Two VMs, one semantics document.** Doubled conversion rules, doubled
   test matrix (every design/11 caveat needs a Ruby twin test in
   `cluster_test.sh`), bigger binary, more supply-chain surface. The
   Lua↔Ruby behavioral deltas (0- vs 1-based KEYS, exception-vs-error-table,
   integer truncation of floats) must be documented side by side or users
   porting scripts will be bitten.
6. **Determinism is inherited, not solved.** Same as Lua: scripts replicate
   *effects only* (design/11 caveat 2), so `Random`/`Time` nondeterminism is
   safe cluster-wide. Keep mrubyedge's `Random` feature enabled for parity.
   But the same footguns follow: a GET/SET token bucket races as LWW; the
   PN-counter pattern is the one that's cross-node exact.
7. **GC/allocation behavior unknown.** Fresh-VM-per-call sidesteps
   accumulation, but a single call can still allocate without bound
   (drawback #2's memory half). Until a real cap exists: cap bytecode size
   at LOAD (e.g. 1 MiB), cap `bridge_call` reply sizes into the VM, and
   rely on the time deadline once it exists.
8. **No `RUBY.KILL`.** Same gap as Lua (`SCRIPT KILL` documented in
   design/11 caveat 4, still unimplemented). With drawback #2 unfixed this
   is doubly relevant; land the fuel hook first, then KILL becomes a flag
   the hook checks — do it for both languages at once.

## Phasing

- **Phase 0 — spike (small):** vendored dep behind a cargo feature
  `mruby`; `RUBY.EVAL` bytecode path end-to-end (load → VM → `Redis.call`
  bridge → value mapping); unit tests mirroring the script.rs ones. Proves
  the `RFn`-captures-engine question and the value bridge before anything
  else is built on it.
- **Phase 1 — full surface (medium):** `RUBY.LOAD`/`EVALSHA`/`EXISTS`/
  `FLUSH`, `\x00rbscript:` replicated records + `ensure_local` self-heal,
  config gate, `script_common.rs` refactor, cluster_test.sh section,
  docs page (`docs/mruby-scripting.md`) with the compile workflow and the
  Lua-deltas table.
- **Phase 2 — safety (the gate for defaults-on):** fuel/instruction budget
  in the VM loop (upstream PR, fork fallback), RITE validation + loader
  fuzzing, memory guard, `RUBY.KILL`/`SCRIPT KILL` on top of the hook.
- **Phase 3 — optional UX:** server-side `mrbc` compile-on-LOAD behind a
  config flag (option B), and/or a `FUNCTION`-style multi-engine registry
  if a third language ever appears.

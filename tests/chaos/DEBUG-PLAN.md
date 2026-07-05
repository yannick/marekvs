# Plan — debug-image chaos tests (grudge partitions, netem, clock skew)

The three faults left un-ported from Jepsen (design/10 §10.3) all need
tooling *inside* the node container — `iptables`, `tc`, a settable clock —
that the `FROM scratch` production image deliberately lacks. This plan adds
a **separate debug image** with a busybox/alpine userland and the fault
tools, leaves the production image untouched, and extends the existing
harness with the new nemeses behind a `CHAOS_DEBUG=1` gate.

## Guiding constraints (why the design is shaped this way)

1. **Never bloat the production image.** The scratch image stays exactly as
   is. The debug image is a second final stage over the *same* build stage,
   so the marekvs binary is byte-identical — we test the real artifact,
   just in a container that also has tools.
2. **Static-musl binary ⇒ no LD_PRELOAD.** libfaketime can't intercept a
   static binary, so clock faults must move the real clock (`date -s` /
   `clock_settime`). That only skews *one node* when each node owns its own
   clock → **clock faults run on the Apple backend** (VM per node). On
   Docker every container shares the host VM clock; time namespaces
   virtualize only `CLOCK_MONOTONIC`, not the `CLOCK_REALTIME` the HLC
   reads, so Docker can't do single-node skew. Documented, not worked
   around.
3. **iptables/tc need `NET_ADMIN`; `date -s` needs `SYS_TIME`.** Debug
   containers run with those caps added. This is why they're a separate,
   opt-in image — the production image should never run privileged.
4. **Reuse the existing checkers.** The Jepsen counter/set acceptance logic,
   convergence waits, and `underreplicated_partitions` heal check already
   exist; the new scenarios only add fault injectors, not new oracles.

## Phase 0 — debug images

- `Dockerfile.debug`: identical build stage to `Dockerfile`, final stage
  `FROM alpine:3.20` instead of scratch, `apk add --no-cache iproute2
  iptables coreutils` (tc, iptables, GNU `date -s`), copy `/marekvs` +
  `/marekvs-operator`, same `ENTRYPOINT`. Tag `marekvs:debug`.
- Justfile: `debug-build` (docker) and `debug-apple-build` (Apple CLI),
  both staged through the existing `_stage-ctx` context. No change to
  `docker-build` / `apple-build`.
- Harness: `IMAGE=${CHAOS_DEBUG:+marekvs:debug}` selection; debug runs pass
  `--cap-add NET_ADMIN --cap-add SYS_TIME` (docker) / the Apple equivalent.
  `node_run` already centralizes container creation — one branch.
- **Exit criterion:** `just chaos-docker` still uses the scratch image and
  is unchanged; `CHAOS_DEBUG=1 just chaos-docker` boots the debug image and
  the existing scenarios still pass (proves the tooling userland doesn't
  perturb behavior).

## Phase 1 — grudge partitions (Docker, iptables)

Port Jepsen's grudge model (`nemesis.clj` `complete-grudge` / `bridge` /
`majorities-ring`): a grudge is `node → {nodes it must not talk to}`,
applied as `iptables` DROP rules on the mesh subnet (172.29.10.0/24), healed
by `iptables -F`. Clients on the edge net are never touched, so writes
continue on every side (the split-brain property the current
mesh-disconnect already gives, now with arbitrary topologies).

- `grudge_apply <grudge-spec>` / `grudge_heal`: for each `(a,b)` in the
  grudge, `crt exec chaos-a iptables -A INPUT -s <mesh_ip b> -j DROP` and
  the symmetric OUTPUT rule (drop both directions so half-open links can't
  leak).
- Pure grudge builders (bash/python, unit-testable off-cluster like
  `scale.rs`):
  - `grudge_halves N` — bisect, two mutually-deaf components.
  - `grudge_bridge N` — two halves + one lone node that can still see both
    (Jepsen `bridge`): the classic "does the bridge node cause split-brain
    divergence that later converges" test.
  - `grudge_ring N` — majorities-ring: every node sees a majority but no two
    see the *same* majority (exact ring for N≤5, the case we run).
- **Scenarios:** `bridge_partition` and `majority_ring` — run
  counter+set+register workloads across the grudge for ~15 s, heal, assert
  the existing counter/set checkers + total convergence +
  `underreplicated_partitions → 0`. These stress placement/AE far harder
  than single-node isolation because different nodes disagree about who owns
  what *while both sides accept writes*.
- **Exit criterion:** both topologies converge post-heal with zero lost
  acked ops; if they don't, it's a real placement/AE bug (the whole point).

## Phase 2 — packet faults (Docker, tc netem)

Jepsen's `packet-package`: delay, loss, corruption, duplication, reorder via
`tc qdisc … netem`, applied to one node's mesh interface.

- `net_delay <i> <ms> [jitter]`, `net_loss <i> <pct>`, `net_corrupt`,
  `net_dup`, `net_reorder`, `net_clear <i>` — thin wrappers over
  `crt exec chaos-i tc qdisc add dev eth0 root netem …` (eth0 = mesh nic;
  confirm the interface name in the debug image, may be `eth1`).
- **Scenarios:**
  - `slow_peer` — the design/10.3 case: `net_delay` one node hard enough to
    overrun the 128 MiB replication ring, forcing the pump into a *gap* →
    Merkle AE must repair. Asserts no lost acked ops and that the gauge
    recovers (proves the ring-gap → AE fallback path, which currently only
    has a `tracing::warn`, actually works end to end).
  - `lossy_writes` — 20–30 % `net_loss` during counter+set load; retries
    make ops indeterminate, the checker's acked/indeterminate envelope
    absorbs that, but nothing acked may be lost.
- **Exit criterion:** ring-overrun repair verified; loss/corruption never
  loses an acked write.

## Phase 3 — clock faults (Apple, real clock)

Per-VM clocks make the Apple backend the only place single-node skew is
meaningful. Port Jepsen's bump / strobe / reset (`nemesis/time.clj`).

- `clock_bump <i> <±seconds>` — `container exec chaos-i date -s @<epoch±Δ>`
  (needs `SYS_TIME`; if the Apple runtime refuses in-VM `date -s`, fall
  back to a tiny static `settimeofday` helper baked into the debug image).
  Offsets from Jepsen's distribution: ±(0.1, 1, 10, 100, 288) s, incl.
  negative.
- `clock_strobe <i> <Δ> <period_ms> <duration_s>` — background loop
  alternating the offset, restoring true time at the end (a bash loop
  calling `date -s`).
- `clock_reset <i>` — resync to the harness host's time.
- **Scenarios:**
  - `clock_bump_skew` — bump one node +10 s and −10 s under counter+set
    load. The HLC receive rule must absorb it: no lost/inverted counter
    increments, sets converge, and a bumped-forward node must not make its
    LWW writes win forever (the exact failure the receive rule was added
    for — this is the regression test that never existed).
  - `clock_strobe` — strobe ±4 s for 20 s; same assertions.
- **Exit criterion:** counter exactness and set completeness hold through
  ±288 s skew and strobing; if a bumped node's LWW starves peers, that's a
  receive-rule bug.
- **Offset check:** before asserting, verify the skew actually took effect
  (compare `redis-cli … TIME` across nodes) — a no-op skew would make the
  test silently vacuous (Jepsen's `check-offsets`).

## Phase 4 — wiring, docs, CI

- Justfile: `chaos-debug` (docker: bridge/ring/slow/lossy) and
  `chaos-clock` (apple: bump/strobe), each with preflight + teardown trap,
  same shape as `chaos-docker`. Default `chaos-docker`/`chaos-apple`
  unchanged and still on the scratch image.
- `tests/chaos/lib.sh`: new fault fns gated so a non-debug image gives a
  clear "needs CHAOS_DEBUG=1 / marekvs:debug" error, never a cryptic
  `iptables: not found`.
- Unit-test the pure grudge builders (halves/bridge/ring) the way
  `scale.rs` is tested — they're just set math and must be right before
  they gate a cluster.
- design/10 §10.3: move these three from "not yet ported" to implemented;
  note the debug-image split and the Docker-clock limitation.
- CI: the debug suites stay **opt-in** (privileged caps, slower). `just ci`
  does not run them; a separate `just chaos-debug` is for local/nightly.

## Risks / unknowns to resolve during implementation

1. **Mesh interface name** in the debug container (eth0 vs eth1) — the node
   is on two Docker networks; tc/iptables must target the *mesh* nic. Verify
   with `ip -o addr` at build-bringup; may need to match by subnet.
2. **Apple `date -s` permission** — if the runtime blocks in-VM clock set
   even with a cap, use the static `settimeofday` helper; if that also
   fails, clock tests are Apple-only *and* may need a runtime flag —
   document whatever the constraint turns out to be rather than fake it.
3. **iptables vs nftables** in alpine 3.20 — use `iptables-legacy` if the
   default nft backend misbehaves inside the container.
4. **Cap availability** — `--cap-add` must be honored; if the environment
   forbids `NET_ADMIN`, these tests can't run there and the harness should
   say so, not partially apply.

## What this deliberately does NOT do

- No fault injection in the production image, ever.
- No `--privileged`; least-privilege caps only.
- No attempt to skew clocks on Docker (physically can't per-node) — clock
  faults are Apple-only by construction, and that's stated, not hidden.

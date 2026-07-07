#!/usr/bin/env bash
# Chaos + churn test suite for marekvs — Jepsen-style scenarios (design/10).
#
#   BACKEND=docker tests/chaos/chaos_test.sh [scenario ...]   # full menu
#   BACKEND=apple  tests/chaos/chaos_test.sh [scenario ...]   # no partitions,
#                                                             # real VM clocks
#
# Each scenario follows the Jepsen phase structure: run workloads and the
# nemesis concurrently (sleep → fault → sleep → heal cycles), then stop the
# nemesis, HEAL EVERYTHING, quiesce, take final reads, and run the checkers
# against the logged history. Default: all scenarios valid for the backend.
set -euo pipefail

cd "$(dirname "$0")/../.."
"./tests/preflight.sh"
source tests/chaos/lib.sh

SCENARIOS=("$@")
[ ${#SCENARIOS[@]} -gt 0 ] || {
  SCENARIOS=(crash_restart freeze_thaw rolling_churn wipe_replace membership_churn join_empty_reads interest_flood bank budget_no_overspend budget_pvc_wipe)
  [ "$BACKEND" = docker ] && SCENARIOS+=(disk_guard gc_grace_rejoin)
  [ "$BACKEND" = docker ] && SCENARIOS+=(partition_divergence partition_no_resurrect budget_partition json_convergence proto_partition proto_field_partition)
}

trap cluster_down EXIT

scenario_banner() {
  echo
  echo "━━━ scenario: $1 ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
}

fresh_cluster() {
  cluster_down
  cluster_up
  sleep 2 # membership settle
}

# ── scenario: crash_restart ──────────────────────────────────────────────
# Jepsen kill nemesis: SIGKILL a random node mid-write, revive, repeat.
# Acked writes must survive (they were committed to ondadb before the ack).
crash_restart() {
  fresh_cluster
  counter_workload chaos:cnt 45 & local W1=$!
  set_workload chaos:set 45 & local W2=$!
  local round
  for round in 0 1 2; do
    sleep 5
    crash $(( (round + 1) % N ))
    sleep 5
    revive $(( (round + 1) % N ))
  done
  wait $W1 $W2
  sleep 3 # quiesce
  check_counter chaos:cnt
  check_set chaos:set
  check_converged "counter" 30 get chaos:cnt
  check_replication_healed 60
}

# ── scenario: partition_divergence (docker) ──────────────────────────────
# Jepsen partition-random-node: isolate node 1 from the mesh while clients
# write to BOTH sides (edge network stays up → true split-brain), heal,
# assert convergence and the CRDT merge laws end-to-end.
partition_divergence() {
  fresh_cluster
  counter_workload chaos:cnt 40 & local W1=$!
  set_workload chaos:set 40 & local W2=$!
  register_workload chaos:reg 40 & local W3=$!
  sleep 5
  partition 1
  sleep 12   # divergent writes accumulate on both sides
  heal 1
  sleep 5
  partition 2
  sleep 8
  heal 2
  wait $W1 $W2 $W3
  sleep 3
  check_counter chaos:cnt
  check_set chaos:set
  check_converged "LWW register" 40 get chaos:reg
  check_converged "counter" 40 get chaos:cnt
  check_replication_healed 60
}

# ── scenario: partition_no_resurrect (docker) ────────────────────────────
# Element removed on the majority side while the isolated node still holds
# it: after heal the removal must win everywhere, forever (OR-set delete
# semantics + tombstone gating — resurrection is THE classic AP bug).
partition_no_resurrect() {
  fresh_cluster
  rcli 0 sadd res:s doomed keeper >/dev/null
  check_set_converged "seed set" res:s 20 "doomed keeper"
  partition 1
  sleep 2
  rcli 0 srem res:s doomed >/dev/null            # remove on majority side
  rcli 1 sadd res:s partition-born >/dev/null    # concurrent add on island
  sleep 3
  heal 1
  # 75 s > the 60 s interest lease: a NON-owner that cached the set before
  # the partition serves its (refreshed-in-place, but never grown) copy
  # until lease expiry re-fetches — new members born on the island reach it
  # only then. Bounded staleness is the documented AP contract; asserting
  # convergence inside the lease window was racing it.
  check_set_converged "post-heal set" res:s 75 "keeper partition-born"
  # the removed element must be gone on every node; the island add survives
  local i
  for i in $(seq 0 $((N - 1))); do
    local m; m=$(rcli "$i" smembers res:s | sort | tr '\n' ' ' | sed 's/ *$//')
    if [ "$m" = "keeper partition-born" ]; then
      chk 0 "node $i: removal held, island add survived [$m]"
    else
      chk 1 "node $i set contents" "expected [keeper partition-born] got [$m]"
    fi
  done
  # DEL of a whole key across a partition
  rcli 0 set res:k alive >/dev/null
  check_converged "key seeded" 20 get res:k
  partition 2
  rcli 0 del res:k >/dev/null
  sleep 2
  heal 2
  check_converged "deleted key stays deleted" 40 get res:k
  local v; v=$(rcli 2 get res:k)
  [ -z "$v" ] && chk 0 "node 2: DEL survived its partition" \
              || chk 1 "node 2 resurrected key" "got [$v]"
}

# ── scenario: freeze_thaw ────────────────────────────────────────────────
# Jepsen hammer-time: SIGSTOP a node for 20s under load. On apple the VM
# clock keeps running while the process is frozen → on thaw the node's HLC
# must absorb the jump (receive rule) without losing or inventing updates.
freeze_thaw() {
  fresh_cluster
  counter_workload chaos:cnt 40 & local W1=$!
  set_workload chaos:set 40 & local W2=$!
  sleep 5
  freeze 1
  sleep 20
  thaw 1
  wait $W1 $W2
  sleep 3
  check_counter chaos:cnt
  check_set chaos:set
  check_converged "counter" 40 get chaos:cnt
  check_replication_healed 60
}

# ── scenario: rolling_churn ──────────────────────────────────────────────
# Rolling graceful restarts under load (the k8s rollout path: SIGTERM →
# Leaving → drain → restart with data → cursor resume).
rolling_churn() {
  fresh_cluster
  counter_workload chaos:cnt 50 & local W1=$!
  set_workload chaos:set 50 & local W2=$!
  sleep 5
  local i
  for i in 0 1 2; do
    graceful_restart "$i"
    sleep 4
  done
  wait $W1 $W2
  sleep 3
  check_counter chaos:cnt
  check_set chaos:set
  check_converged "counter" 30 get chaos:cnt
  check_replication_healed 60
}

# ── scenario: wipe_replace ───────────────────────────────────────────────
# Total data loss on one node → fresh replacement bootstraps from peers.
# All acked data must be readable ON THE FRESH NODE (anti-entropy backfill),
# and the replication gauge must recover.
wipe_replace() {
  fresh_cluster
  counter_workload chaos:cnt 20 & local W1=$!
  set_workload chaos:set 20 & local W2=$!
  wait $W1 $W2
  sleep 2 # pump flush
  echo "  --- pre-wipe state (must already satisfy the acked bounds):"
  check_counter chaos:cnt
  check_set chaos:set
  wipe_node 2
  check_replication_healed 120
  # interest-free reads on the fresh node: read-through must fetch or the
  # data must have been re-replicated — either way, nothing may be missing.
  check_counter chaos:cnt
  check_set chaos:set
}

# ── scenario: membership_churn ───────────────────────────────────────────
# Scale up (node 3 joins mid-workload), let it take ownership, then remove
# it gracefully. No acked write may be lost across the ownership moves.
membership_churn() {
  fresh_cluster
  counter_workload chaos:cnt 40 & local W1=$!
  set_workload chaos:set 40 & local W2=$!
  sleep 5
  echo "  nemesis: node 3 joins the cluster"
  if [ "$BACKEND" = docker ]; then
    docker run -d --name chaos-3 \
      --network "$EDGE_NET" --ip "$(edge_ip 3)" \
      -p "$(resp_port 3):6379" -p "$(metrics_port 3):9121" \
      -e MAREKVS_NODE_ID=3 -e MAREKVS_REPLICAS_N=2 \
      -e MAREKVS_DATA_DIR=/data -e MAREKVS_ADVERTISE_IP="$(mesh_ip 3)" \
      -e MAREKVS_SEEDS="$(seeds)" -e RUST_LOG=info,chitchat=warn \
      "$IMAGE" >/dev/null
    docker network connect --ip "$(mesh_ip 3)" "$MESH_NET" chaos-3
  else
    container run -d --name chaos-3 \
      -e MAREKVS_NODE_ID=3 -e MAREKVS_REPLICAS_N=2 \
      -e MAREKVS_DATA_DIR=/data -e MAREKVS_ADVERTISE_IP=auto \
      -e MAREKVS_SEEDS="$(apple_ip chaos-0):7946" -e RUST_LOG=info,chitchat=warn \
      "$IMAGE" >/dev/null
  fi
  N=4 wait_ready 3
  sleep 12   # let placement shift and writes land on the newcomer
  echo "  nemesis: node 3 leaves gracefully"
  crt stop chaos-3 >/dev/null
  crt rm -f chaos-3 >/dev/null 2>&1 || true
  wait $W1 $W2
  sleep 3
  check_counter chaos:cnt
  check_set chaos:set
  check_converged "counter" 40 get chaos:cnt
  check_replication_healed 90
}

# ── scenario: join_empty_reads ───────────────────────────────────────────
# Scale-up must not serve empty reads: pre-join-gate, a new node flipped
# Active after a fixed 2 s sleep, HRW immediately routed ~1/n of partitions
# to it, and both its own home reads AND other nodes' read-throughs hit its
# empty store until anti-entropy filled it. The gate holds the node in
# Joining (RESP not listening, /ready 503) until every future-owned
# partition is bootstrapped — so the moment PING answers, reads must be
# complete everywhere.
join_empty_reads() {
  fresh_cluster
  echo "  seeding 100k keys"
  local seed_cmds
  seed_cmds=$(awk 'BEGIN{for(i=0;i<100000;i++) printf "set seed:%d v%d\r\n", i, i}')
  if [ "$BACKEND" = docker ]; then
    echo "$seed_cmds" | redis-cli -p "$(resp_port 0)" --pipe >/dev/null
  else
    echo "$seed_cmds" | redis-cli -h "$(apple_ip chaos-0)" -p 6379 --pipe >/dev/null
  fi
  check_replication_healed 90
  echo "  nemesis: node 3 joins the cluster"
  if [ "$BACKEND" = docker ]; then
    docker run -d --name chaos-3 \
      --network "$EDGE_NET" --ip "$(edge_ip 3)" \
      -p "$(resp_port 3):6379" -p "$(metrics_port 3):9121" \
      -e MAREKVS_NODE_ID=3 -e MAREKVS_REPLICAS_N=2 \
      -e MAREKVS_DATA_DIR=/data -e MAREKVS_ADVERTISE_IP="$(mesh_ip 3)" \
      -e MAREKVS_SEEDS="$(seeds)" -e RUST_LOG=info,chitchat=warn \
      "$IMAGE" >/dev/null
    docker network connect --ip "$(mesh_ip 3)" "$MESH_NET" chaos-3
  else
    container run -d --name chaos-3 \
      -e MAREKVS_NODE_ID=3 -e MAREKVS_REPLICAS_N=2 \
      -e MAREKVS_DATA_DIR=/data -e MAREKVS_ADVERTISE_IP=auto \
      -e MAREKVS_SEEDS="$(apple_ip chaos-0):7946" -e RUST_LOG=info,chitchat=warn \
      "$IMAGE" >/dev/null
  fi
  N=4 wait_ready 3 450
  # RESP answering == the node went Active with every home partition
  # bootstrapped. Sample immediately: a small transient nil rate (<1%) is
  # tolerated — read-throughs can race the placement flip for the first
  # second (AP view-convergence window, accepted by the assessment) — but
  # the pre-gate empty-store failure was ~35% nils.
  local db3 miss=0 round j k v n
  db3=$(rcli 3 dbsize 2>/dev/null || echo 0)
  for round in 1 2 3; do
    for j in $(seq 1 66); do
      k=$(( (RANDOM * 3 + j) % 100000 ))
      for n in 0 1 2 3; do
        # `|| true`: one transient connection error must count as a miss,
        # not abort the whole suite (set -e).
        v=$(rcli "$n" get "seed:$k" 2>/dev/null || true)
        [ -n "$v" ] || miss=$((miss + 1))
      done
    done
  done
  # Contract at first PONG: the joiner is SUBSTANTIALLY bootstrapped and any
  # residual gap is small and AE-bounded. Pre-gate: ~35% nils on a ~3k-record
  # store with only slow AE to heal it; post-gate runs measure 1-14%
  # (load-dependent read-through races at the placement flip) decaying to
  # ZERO within one AE bound — the deterministic assertions are the dbsize
  # check and the post-settle zero below; this is a loose tripwire.
  chk $((miss > 158)) "join residual bounded (pre-gate was ~35% nils)" \
    "$miss empty replies out of 792 sampled (node3 dbsize=$db3 at ready)"
  # RF=2 over 4 nodes: node 3 homes ~half the 100k keyspace (~50k keys).
  # The pre-gate code showed dbsize ≈ 3k at first PONG (2 s of bootstrap).
  chk $((db3 < 20000)) "joiner bootstrapped before Active" \
    "node3 dbsize=$db3 at first PONG — went Active with a near-empty store?"
  # After one full AE bound (15 s + margin) every read must be complete
  # everywhere — this is the assessment's documented convergence promise.
  sleep 20
  miss=0
  for j in $(seq 1 66); do
    k=$(( (RANDOM * 3 + j) % 100000 ))
    for n in 0 1 2 3; do
      v=$(rcli "$n" get "seed:$k" 2>/dev/null || true)
      [ -n "$v" ] || miss=$((miss + 1))
    done
  done
  chk $((miss > 0)) "zero empty reads after the AE bound (20s settle)" \
    "$miss empty replies out of 264 sampled post-settle"
  crt rm -f chaos-3 >/dev/null 2>&1 || true
  N=3 check_replication_healed 90
}

# ── scenario: interest_flood ─────────────────────────────────────────────
# A client scanning many unique keys through non-home nodes registers one
# interest lease per (key, node) on the homes — previously unbounded: an
# OOM you can cause from a redis-cli. With the cap, marekvs_interest_entries
# must stay ≤ MAREKVS_INTEREST_MAX_ENTRIES (shrunk to 5000 here), rejections
# must be counted, and interest-path reads must keep working.
interest_flood() {
  CHAOS_EXTRA_ARGS="-e MAREKVS_INTEREST_MAX_ENTRIES=5000"
  fresh_cluster
  CHAOS_EXTRA_ARGS=""
  echo "  flooding: 30k unique-key GETs per node (read-through registers interest)"
  local n cmds
  for n in 0 1 2; do
    cmds=$(awk -v n="$n" 'BEGIN{for(i=0;i<30000;i++) printf "get flood:%d:%d\r\n", n, i}')
    if [ "$BACKEND" = docker ]; then
      echo "$cmds" | redis-cli -p "$(resp_port "$n")" --pipe >/dev/null 2>&1 || true
    else
      echo "$cmds" | redis-cli -h "$(apple_ip "chaos-$n")" -p 6379 --pipe >/dev/null 2>&1 || true
    fi
  done
  sleep 4 # let the 2 s stats tick publish the gauge
  local maxi=0 rej=0 v i
  for i in 0 1 2; do
    v=$(metric_value "$i" marekvs_interest_entries); [ "${v:-0}" -gt "$maxi" ] && maxi=$v
    v=$(metric_value "$i" marekvs_interest_rejected_total); rej=$((rej + ${v:-0}))
  done
  chk $((maxi > 5000)) "interest map capped" \
    "max marekvs_interest_entries=$maxi (cap 5000) — unbounded growth?"
  chk $((rej == 0)) "rejections counted at cap" "interest_rejected_total sum=$rej"
  # Interest-path reads must still function at cap.
  set_workload chaos:set 15 & local W=$!
  wait $W
  sleep 3
  check_set chaos:set
  check_converged "set still converges at cap" 45 scard chaos:set
}

# ── scenario: gc_grace_rejoin (docker) ───────────────────────────────────
# Cassandra's oldest rule: a node down longer than gc_grace must NOT rejoin
# as an authority — its live records whose tombstones were purged elsewhere
# would resurrect deletes. With the fix, the rejoiner holds phase Joining,
# pull-syncs each home partition from a healthy owner, DROPS the stale
# extras only it holds (instead of serving them), and goes Active only when
# every partition's Merkle root matches. gc_grace shrunk to 15 s; downtime
# ~60 s with write churn so survivors' tombstones age past grace and get
# physically purged before the rejoin.
gc_grace_rejoin() {
  [ "$BACKEND" = docker ] || { echo "  (skipped: needs the docker backend)"; return 0; }
  CHAOS_EXTRA_ARGS="-e MAREKVS_GC_GRACE_SECS=15"
  fresh_cluster
  CHAOS_EXTRA_ARGS=""
  rcli 0 set doomed:k v0 >/dev/null
  rcli 0 sadd ctl:s a b c >/dev/null
  check_converged "seed key" 20 get doomed:k
  check_set_converged "seed control set" ctl:s 20 "a b c"
  crash 2
  sleep 1
  rcli 0 del doomed:k >/dev/null
  # Churn for ~60 s (4x grace): the delete's tombstone is written, ages past
  # grace, and compaction physically purges it on the survivors.
  register_workload churn:reg 60 & local W=$!
  wait $W
  # revive's default 40 s readiness window is too tight here: the rejoiner
  # holds Joining until ~2700 partitions confirm MerkleRootMatch.
  echo "  nemesis: revive node 2 (rejoin sync may take minutes)"
  crt start chaos-2 >/dev/null
  wait_ready 2 750
  # The delete must hold everywhere — pre-fix, node 2's stale live record
  # re-offers doomed:k via AE and resurrects it once the tombstone is gone.
  check_converged "deleted key stays deleted" 60 get doomed:k
  check_set_converged "control set intact" ctl:s 30 "a b c"
  local rj drops
  rj=$(metric_value 2 marekvs_rejoin_active)
  chk $([ "${rj:-1}" = "0" ]; echo $?) "rejoin completed (gate released)" \
    "marekvs_rejoin_active=${rj:-absent}"
  drops=$(metric_value 2 marekvs_rejoin_dropped_records_total)
  echo "  (node 2 dropped ${drops:-0} stale extra records during rejoin)"
  rcli 2 set post:k v1 >/dev/null 2>&1 || true
  check_converged "fresh write on rejoiner converges" 30 get post:k
}

# ── scenario: disk_guard ─────────────────────────────────────────────────
# Disk-full is THE unrecoverable LSM failure: ondadb write errors wedge the
# node mid-compaction. With the guard, filling one node's (tmpfs) data
# volume must produce a clean MISCONF refusal at the high-water mark while
# reads keep working, the node stays alive, and REPLICATED writes keep
# applying (refusing merges would mean divergence — the guard sheds client
# load only). Thresholds lowered so the 2 s stats poll cannot be outrun.
disk_guard() {
  [ "$BACKEND" = docker ] || { echo "  (skipped: tmpfs mount needs the docker backend)"; return 0; }
  CHAOS_TMPFS="2 134217728" # node 2: 128 MB /data
  CHAOS_EXTRA_ARGS="-e MAREKVS_DISK_HIGH_WATER_PCT=60 -e MAREKVS_DISK_LOW_WATER_PCT=50"
  fresh_cluster
  CHAOS_TMPFS=""
  CHAOS_EXTRA_ARGS=""
  local payload i out misconf=0 writes=0
  payload=$(printf 'x%.0s' $(seq 1 100000)) # 100 KB
  echo "  filling node 2's 128 MB volume with 100 KB values"
  for i in $(seq 1 1200); do
    out=$(rcli 2 set "disk:k$i" "$payload" 2>&1) || true
    writes=$i
    case "$out" in *MISCONF*) misconf=1; break ;; esac
  done
  chk $((1 - misconf)) "clean MISCONF refusal at high-water" \
    "no MISCONF after $writes x 100KB writes (node wedged or guard absent?)"
  chk $([ "$(rcli 2 ping 2>/dev/null)" = "PONG" ]; echo $?) "node 2 still alive after fill"
  chk $([ "$(rcli 2 strlen disk:k1 2>/dev/null)" = "100000" ]; echo $?) \
    "reads still served under write-stop"
  local stopped; stopped=$(metric_value 2 marekvs_disk_write_stopped)
  chk $([ "${stopped:-0}" = "1" ]; echo $?) "marekvs_disk_write_stopped gauge = 1" \
    "gauge=${stopped:-absent}"
  # Peer replication must keep applying on the stopped node.
  local a0 a1
  a0=$(metric_value 2 marekvs_repl_ops_applied_total); a0=${a0:-0}
  for i in $(seq 1 30); do rcli 0 set "probe:$i" v$i >/dev/null 2>&1 || true; done
  sleep 3
  a1=$(metric_value 2 marekvs_repl_ops_applied_total); a1=${a1:-0}
  chk $((a1 <= a0)) "replication still applies onto the write-stopped node" \
    "repl_ops_applied_total $a0 -> $a1 after 30 writes via node 0"
}

# ── scenario: bank ───────────────────────────────────────────────────────
# Jepsen bank test adapted to AP: transfers between two accounts in one
# hash tag run as atomic Lua scripts; reads snapshot both balances in one
# script (same shard → atomic). Total must be conserved on every node's
# final converged read, through graceful churn. (SIGKILL mid-script could
# legitimately half-apply — Redis semantics — so this scenario uses
# graceful restarts only.)
bank() {
  fresh_cluster
  local TOTAL=1000
  rcli 0 mset '{bank}:a' 500 '{bank}:b' 500 >/dev/null
  check_converged "bank seeded" 20 get '{bank}:a'
  local XFER="local amt = tonumber(ARGV[1])
redis.call('DECRBY', KEYS[1], amt)
redis.call('INCRBY', KEYS[2], amt)
return 1"
  local dur=40 t0=$SECONDS n=0
  (
    while [ $((SECONDS - t0)) -lt $dur ]; do
      wi=$((n % N)); amt=$(( (n % 5) + 1 ))
      if [ $((n % 2)) = 0 ]; then
        rcli "$wi" -t 2 eval "$XFER" 2 '{bank}:a' '{bank}:b' "$amt" >/dev/null 2>&1 || true
      else
        rcli "$wi" -t 2 eval "$XFER" 2 '{bank}:b' '{bank}:a' "$amt" >/dev/null 2>&1 || true
      fi
      n=$((n + 1))
    done
  ) & local W=$!
  sleep 5
  graceful_restart 1
  sleep 5
  graceful_restart 2
  wait $W
  sleep 3
  check_converged "account a" 40 get '{bank}:a'
  check_converged "account b" 40 get '{bank}:b'
  local i
  for i in $(seq 0 $((N - 1))); do
    local a b sum
    a=$(rcli "$i" get '{bank}:a'); b=$(rcli "$i" get '{bank}:b')
    sum=$((a + b))
    if [ "$sum" = "$TOTAL" ]; then
      chk 0 "bank total conserved on node $i ($a + $b = $sum)"
    else
      chk 1 "bank total on node $i" "$a + $b = $sum, expected $TOTAL — $((sum - TOTAL)) CREATED/DESTROYED"
    fi
  done
}

# ── budget scenarios (design/13) ─────────────────────────────────────────
# Shared harness: workers hammer BG.RESERVE/COMMIT/RELEASE across all nodes
# (commits land on non-issuers to exercise forwarding), journaling every
# ACCEPTED spend; the nemesis varies per scenario. Oracle after heal +
# max-TTL + folds: every node's ledger view stays within capacity AND no
# accepted spend is ever lost.
BUDGET_KEY='{bgt}:pool'
BUDGET_CAP=1000
BUDGET_TTL=3000

budget_setup() {
  fresh_cluster
  rcli 0 bg.create "$BUDGET_KEY" "$BUDGET_CAP" TTL "$BUDGET_TTL" MAXAMOUNT 50 >/dev/null
}

budget_worker() { # <duration_s> <journal> — run with `&` by the caller
  local dur=$1 J=$2 t0=$SECONDS n=0 wi ci amt tok spent out
  while [ $((SECONDS - t0)) -lt "$dur" ]; do
    wi=$((n % N)); ci=$(((n + 1) % N)); amt=$(( (n % 40) + 1 ))
    # `|| true` covers the whole pipeline: a killed/isolated node makes
    # redis-cli exit nonzero and pipefail would kill the worker.
    tok=$(rcli "$wi" -t 2 bg.reserve "$BUDGET_KEY" "$amt" 2>/dev/null | sed -n 2p || true)
    if [ -n "$tok" ] && [[ "$tok" == *-*-*-* ]]; then
      case $((n % 4)) in
        0|1) # commit a partial spend via a DIFFERENT node (issuer forward)
          spent=$(( (amt + 1) / 2 ))
          out=$(rcli "$ci" -t 2 bg.commit "$BUDGET_KEY" "$tok" "$spent" 2>/dev/null || true)
          case "$out" in
            ''|*[!0-9]*) : ;; # not accepted — nothing journaled
            *) echo "$spent" >>"$J" ;;
          esac ;;
        2) rcli "$ci" -t 2 bg.release "$BUDGET_KEY" "$tok" >/dev/null 2>&1 || true ;;
        3) : ;; # abandon: reclaimed at the deadline
      esac
    fi
    n=$((n + 1))
  done
}

budget_oracle() { # <journal>
  local J=$1
  # Let every open token expire (TTL + reclaim grace 5s + AE margin), then
  # poke each node so issuers fold their own expired tokens.
  sleep $(( (BUDGET_TTL / 1000) + 8 ))
  local i tok2
  for i in $(seq 0 $((N - 1))); do
    tok2=$(rcli "$i" -t 5 bg.reserve "$BUDGET_KEY" 1 2>/dev/null | sed -n 2p || true)
    if [ -n "$tok2" ]; then
      rcli "$i" -t 5 bg.release "$BUDGET_KEY" "$tok2" >/dev/null 2>&1 || true
    fi
  done
  sleep 3 # replicate the folds
  local spent_total=0 v
  while read -r v; do spent_total=$((spent_total + v)); done <"$J"
  rm -f "$J"
  local out
  for i in $(seq 0 $((N - 1))); do
    out=$(rcli "$i" -t 5 bg.info "$BUDGET_KEY" | awk '/^outstanding$/{getline; print; exit}' || true)
    if [ -z "$out" ]; then
      chk 1 "budget INFO on node $i" "no outstanding field"
      continue
    fi
    if [ "$out" -le "$BUDGET_CAP" ] && [ "$out" -ge "$spent_total" ]; then
      chk 0 "node $i ledger within capacity ($spent_total accepted <= $out outstanding <= $BUDGET_CAP)"
    else
      chk 1 "node $i ledger" "accepted=$spent_total outstanding=$out cap=$BUDGET_CAP — INVARIANT VIOLATED"
    fi
  done
  # Fail closed, promptly: an impossible reservation errors instead of hanging.
  local big
  big=$(rcli 0 -t 5 bg.reserve "$BUDGET_KEY" 2000 2>&1 || true)
  case "$big" in
    *BUDGETEXHAUSTED*|*"exceeds budget maximum"*) chk 0 "oversized reserve fails closed" ;;
    *) chk 1 "oversized reserve" "unexpected: $big" ;;
  esac
}

# ── scenario: json_convergence (docker) ──────────────────────────────────
# Per-path CRDT JSON docs (design/16): concurrent field writes, same-array
# appends, and a subtree delete across a partition must converge to the
# byte-identical document on every node, with each append run contiguous.
json_convergence() {
  fresh_cluster
  rcli 0 json.set jc:doc '$' '{"title":"seed","tags":["a"],"meta":{"k":1}}' >/dev/null
  check_converged "json doc seeded" 30 json.get jc:doc .
  partition 1
  sleep 2
  # majority side: field update, append run, subtree delete
  rcli 0 json.set jc:doc '$.title' '"from-majority"' >/dev/null
  rcli 0 json.arrappend jc:doc .tags '"m1"' '"m2"' >/dev/null
  rcli 0 json.del jc:doc '$.meta' >/dev/null
  # island side: fresh field, its own append run
  rcli 1 json.set jc:doc '$.island' 42 >/dev/null
  rcli 1 json.arrappend jc:doc .tags '"i1"' '"i2"' >/dev/null
  sleep 3
  heal 1
  # 75 s > the 60 s interest lease (see partition_no_resurrect)
  check_converged "post-heal json doc" 75 json.get jc:doc .
  local d; d=$(rcli 0 json.get jc:doc .)
  case "$d" in
    *'"title":"from-majority"'*) chk 0 "majority field survived" ;;
    *) chk 1 "majority field" "doc [$d]" ;;
  esac
  case "$d" in
    *'"island":42'*) chk 0 "island field survived" ;;
    *) chk 1 "island field" "doc [$d]" ;;
  esac
  case "$d" in
    *'"meta"'*) chk 1 "json subtree delete" "meta resurrected [$d]" ;;
    *) chk 0 "json subtree delete held everywhere" ;;
  esac
  case "$d" in
    *'"m1","m2"'*) chk 0 "majority append run contiguous" ;;
    *) chk 1 "majority append run" "doc [$d]" ;;
  esac
  case "$d" in
    *'"i1","i2"'*) chk 0 "island append run contiguous" ;;
    *) chk 1 "island append run" "doc [$d]" ;;
  esac
  check_replication_healed 60
}

# ── scenario: proto_partition (docker) ───────────────────────────────────
# Protobuf registry (design/17): a schema uploaded on one side of a
# partition must decode values on nodes that never saw the upload (hidden-key
# read-through), and concurrent PROTO.SET of the same key resolves LWW to one
# whole message everywhere.
proto_partition() {
  fresh_cluster
  local SRC='syntax = "proto3"; package chaos; message V { string who = 1; }'
  rcli 0 proto.schema set chaos/v.proto SOURCE "$SRC" >/dev/null
  rcli 0 proto.bind pv: chaos.V >/dev/null
  rcli 0 proto.setjson pv:k '{"who":"seed"}' >/dev/null
  check_converged "proto value seeded" 30 proto.getjson pv:k
  partition 1
  sleep 2
  rcli 0 proto.setjson pv:k '{"who":"majority"}' >/dev/null
  sleep 1
  rcli 1 proto.setjson pv:k '{"who":"island"}' >/dev/null   # later write wins LWW
  sleep 3
  heal 1
  check_converged "post-heal proto value" 75 proto.getjson pv:k
  local d; d=$(rcli 2 proto.getjson pv:k)
  case "$d" in
    *'"who":"island"'*) chk 0 "LWW winner is the later write" ;;
    *) chk 1 "proto LWW" "expected island write, got [$d]" ;;
  esac
  # schema uploaded during partition decodes everywhere after heal
  partition 2
  sleep 2
  local SRC2='syntax = "proto3"; package chaos; message W { int32 n = 1; }'
  rcli 0 proto.schema set chaos/w.proto SOURCE "$SRC2" >/dev/null
  rcli 0 proto.bind pw: chaos.W >/dev/null
  rcli 0 proto.setjson pw:k '{"n":7}' >/dev/null
  sleep 2
  heal 2
  check_converged "partition-born schema decodes on all nodes" 75 proto.getjson pw:k
  check_replication_healed 60
}

# ── scenario: proto_field_partition (docker) ─────────────────────────────
# Field-level proto CRDT (design/18): PROTO.SETFIELD of DIFFERENT fields on
# both sides of a partition must both survive the heal (the whole-message-LWW
# data-loss case), and concurrent appends to the same repeated field converge
# with both runs present.
proto_field_partition() {
  fresh_cluster
  local SRC='syntax = "proto3"; package chaos; message R { string a = 1; int32 b = 2; repeated string tags = 3; }'
  rcli 0 proto.schema set chaos/r.proto SOURCE "$SRC" >/dev/null
  rcli 0 proto.bind pr: chaos.R >/dev/null
  rcli 0 proto.setjson pr:k '{"a":"seed","tags":["s"]}' >/dev/null
  check_converged "proto field value seeded" 30 proto.getjson pr:k
  partition 1
  sleep 2
  # majority edits field a + appends to tags; island edits field b + appends
  # to tags. Both fields and both appends must survive the heal.
  rcli 0 proto.setfield pr:k a majority >/dev/null
  rcli 0 proto.setfield pr:k tags.1 m1 >/dev/null
  rcli 1 proto.setfield pr:k b 99 >/dev/null
  rcli 1 proto.setfield pr:k tags.1 i1 >/dev/null
  sleep 3
  heal 1
  check_converged "post-heal proto field value" 75 proto.getjson pr:k
  local d; d=$(rcli 2 proto.getjson pr:k)
  case "$d" in
    *'"a":"majority"'*) chk 0 "majority field a survived" ;;
    *) chk 1 "majority field a" "value [$d]" ;;
  esac
  case "$d" in
    *'"b":99'*) chk 0 "island field b survived" ;;
    *) chk 1 "island field b" "value [$d]" ;;
  esac
  case "$d" in
    *m1*) chk 0 "majority append survived" ;;
    *) chk 1 "majority append" "value [$d]" ;;
  esac
  case "$d" in
    *i1*) chk 0 "island append survived" ;;
    *) chk 1 "island append" "value [$d]" ;;
  esac
  check_replication_healed 60
}

# ── scenario: budget_no_overspend — SIGKILL rounds ───────────────────────
budget_no_overspend() {
  budget_setup
  local J; J=$(mktemp)
  budget_worker 40 "$J" & local W=$!
  local round
  for round in 0 1 2; do
    sleep 6
    crash $(( (round + 1) % N ))
    sleep 6
    revive $(( (round + 1) % N ))
  done
  wait $W || true
  budget_oracle "$J"
}

# ── scenario: budget_partition (docker) — true mesh split-brain ──────────
# Nodes keep serving clients while cut from the mesh: grants on the island
# come only from ITS escrow share, forwards to unreachable peers fail
# closed, and commits to an unreachable issuer -TRYAGAIN (later reclaimed).
budget_partition() {
  [ "$BACKEND" = docker ] || { echo "  (skipped: partitions need the docker backend)"; return 0; }
  budget_setup
  local J; J=$(mktemp)
  budget_worker 40 "$J" & local W=$!
  sleep 5
  partition 1
  sleep 12
  heal 1
  sleep 4
  partition 2
  sleep 8
  heal 2
  wait $W || true
  budget_oracle "$J"
}

# ── scenario: budget_pvc_wipe — fresh-PVC epoch fence ────────────────────
# Node 1 is destroyed INCLUDING its data dir mid-run and boots a fresh
# replacement (new store epoch): the boot grant-fence must refuse grants
# until it re-merges its old incarnation's ledgers from a peer, and none of
# the old grants/spends may be double-counted or lost. The worker pauses 2s
# before the wipe so the ring pump ships every acked op — an op acked in
# the same instant the disk is destroyed is unrecoverable by design (async
# replication); that loss window is bounded by pump latency, documented.
budget_pvc_wipe() {
  budget_setup
  local J; J=$(mktemp)
  budget_worker 15 "$J" & local W=$!
  wait $W || true
  sleep 2 # drain the ring pump before the disk dies
  wipe_node 1
  budget_worker 15 "$J" & W=$!
  wait $W || true
  budget_oracle "$J"
}

# ── scenario: budget_clock_skew (debug, apple) — skew vs deadlines ───────
# Clock bumps on two nodes while workers reserve/commit: deadlines are
# issuer-clock-only and folds are absorbing, so skew may shift boundary
# commits between accepted and -TOKENEXPIRED but never double-credit.
budget_clock_skew() {
  require_debug "clock skew"
  [ "$BACKEND" = apple ] || { echo "  (skipped: clock skew needs the apple backend)"; return 0; }
  budget_setup
  local J; J=$(mktemp)
  budget_worker 40 "$J" & local W=$!
  sleep 5
  clock_bump 1 10
  assert_skewed 1 0
  sleep 8
  clock_bump 2 -10
  sleep 8
  clock_reset 1; clock_reset 2
  wait $W || true
  budget_oracle "$J"
}

# ── scenario: bridge_partition (debug, docker) ───────────────────────────
# Jepsen bridge nemesis: two halves that can't see each other, plus a lone
# bridge node that sees both — writes flow through the bridge and diverge on
# the two sides. Heal, assert CRDT convergence + counter/set correctness.
# Needs N=5 for a meaningful bridge (2 | 2 + bridge).
bridge_partition() {
  N=5 fresh_cluster
  N=5 counter_workload chaos:cnt 40 & local W1=$!
  N=5 set_workload chaos:set 40 & local W2=$!
  N=5 register_workload chaos:reg 40 & local W3=$!
  sleep 5
  N=5 grudge_apply bridge
  sleep 15
  N=5 grudge_heal
  wait $W1 $W2 $W3
  sleep 3
  N=5 check_counter chaos:cnt
  N=5 check_set chaos:set
  N=5 check_converged "LWW register" 45 get chaos:reg
  N=5 check_replication_healed 90
}

# ── scenario: majority_ring (debug, docker) ──────────────────────────────
# Jepsen majorities-ring: every node sees a (different) majority; no clean
# two-component split, so placement/AE are maximally stressed. N=5.
majority_ring() {
  N=5 fresh_cluster
  N=5 counter_workload chaos:cnt 40 & local W1=$!
  N=5 set_workload chaos:set 40 & local W2=$!
  sleep 5
  N=5 grudge_apply ring
  sleep 15
  N=5 grudge_heal
  wait $W1 $W2
  sleep 3
  N=5 check_counter chaos:cnt
  N=5 check_set chaos:set
  N=5 check_converged "counter" 45 get chaos:cnt
  N=5 check_replication_healed 90
}

# ── scenario: slow_peer (debug, docker) ──────────────────────────────────
# design/10 §10.3: delay one node's mesh nic hard enough that the 128 MiB
# replication ring overruns and the pump hits a GAP — Merkle anti-entropy
# must then repair. Verifies the ring-gap→AE fallback end to end (today it
# is only a tracing::warn on the push path).
slow_peer() {
  fresh_cluster
  counter_workload chaos:cnt 50 & local W1=$!
  set_workload chaos:set 50 & local W2=$!
  sleep 5
  net_delay 1 800 200   # 800ms±200ms: replication falls far behind
  sleep 25
  net_clear 1
  wait $W1 $W2
  sleep 5
  check_counter chaos:cnt 90       # AE repair is slower than push
  check_set chaos:set 90
  check_converged "counter" 90 get chaos:cnt
  check_replication_healed 120
}

# ── scenario: lossy_writes (debug, docker) ───────────────────────────────
# 25% packet loss on one node's mesh nic during load. Retries make ops
# indeterminate (the checker envelope absorbs that), but nothing acked may
# be lost and the cluster must converge once loss clears.
lossy_writes() {
  fresh_cluster
  counter_workload chaos:cnt 40 & local W1=$!
  set_workload chaos:set 40 & local W2=$!
  sleep 5
  net_loss 1 25
  net_corrupt 2 5
  sleep 18
  net_clear 1; net_clear 2
  wait $W1 $W2
  sleep 5
  check_counter chaos:cnt 90
  check_set chaos:set 90
  check_converged "counter" 90 get chaos:cnt
  check_replication_healed 120
}

# ── scenario: blackhole_conn (debug, docker) ─────────────────────────────
# Wedged-but-open TCP: drop mesh TCP between nodes 0↔1 while gossip UDP
# stays up, so membership keeps BOTH nodes Active while their replication
# link is silently dead (the conntrack-blackhole failure). The mesh
# heartbeat (1 s ping / 3 s idle timeout) must close the dead connections —
# mesh_peers drops — and reconnect + ResumeFrom must recover after heal.
blackhole_conn() {
  require_debug "conntrack blackhole"
  [ "$BACKEND" = docker ] || { echo "  (skipped: blackhole needs the docker backend)"; return 0; }
  fresh_cluster
  counter_workload chaos:cnt 45 & local W1=$!
  set_workload chaos:set 45 & local W2=$!
  sleep 5
  blackhole 0 1
  # Heartbeat must detect the dead connections within ~2× idle timeout.
  local deadline=$((SECONDS + 12)) ok=0 p0 p1
  while [ $SECONDS -lt "$deadline" ]; do
    p0=$(metric_value 0 marekvs_mesh_peers); p1=$(metric_value 1 marekvs_mesh_peers)
    if [ "${p0:-9}" -le 1 ] && [ "${p1:-9}" -le 1 ]; then ok=1; break; fi
    sleep 1
  done
  chk $((1 - ok)) "heartbeat closed wedged connections (mesh_peers dropped)" \
    "mesh_peers node0=${p0:-?} node1=${p1:-?} after 12s (ghost handles?)"
  sleep 8
  blackhole_heal
  deadline=$((SECONDS + 20)); ok=0
  while [ $SECONDS -lt "$deadline" ]; do
    p0=$(metric_value 0 marekvs_mesh_peers); p1=$(metric_value 1 marekvs_mesh_peers)
    if [ "${p0:-0}" -ge 2 ] && [ "${p1:-0}" -ge 2 ]; then ok=1; break; fi
    sleep 1
  done
  chk $((1 - ok)) "mesh reconnected after heal" \
    "mesh_peers node0=${p0:-?} node1=${p1:-?} after 20s"
  wait $W1 $W2
  sleep 3
  check_counter chaos:cnt 90
  check_set chaos:set 90
  check_converged "counter" 90 get chaos:cnt
  check_replication_healed 120
}

# ── scenario: backpressure_no_drop (debug, docker) ───────────────────────
# Delay one node's mesh nic (800 ms ± 200, well under the 3 s heartbeat
# timeout — hard rate caps queue pings behind data and turn this into
# connection churn) with a tiny 4 KB unacked window: in-flight bytes over
# the inflated RTT exceed the window, so senders must SKIP that peer's lane
# (marekvs_repl_window_stalls_total > 0) instead of overrunning it, and no
# batch may be dropped after the cursor moved
# (marekvs_repl_send_failures_total == 0 — the pre-fix silent-drop branch).
# AE may legitimately assist while a lane is stalled; slow_peer remains the
# ring-overrun→gap→AE regression.
backpressure_no_drop() {
  require_debug "netem delay shaping"
  [ "$BACKEND" = docker ] || { echo "  (skipped: needs the docker backend)"; return 0; }
  CHAOS_EXTRA_ARGS="-e MAREKVS_REPL_WINDOW_BYTES=4096"
  fresh_cluster
  CHAOS_EXTRA_ARGS=""
  counter_workload chaos:cnt 45 & local W1=$!
  set_workload chaos:set 45 & local W2=$!
  sleep 5
  net_delay 1 800 200
  local deadline=$((SECONDS + 25)) stalled=0 s0 s2
  while [ $SECONDS -lt "$deadline" ]; do
    s0=$(metric_value 0 marekvs_repl_window_stalls_total)
    s2=$(metric_value 2 marekvs_repl_window_stalls_total)
    if [ "${s0:-0}" -gt 0 ] || [ "${s2:-0}" -gt 0 ]; then stalled=1; break; fi
    sleep 1
  done
  chk $((1 - stalled)) "flow-control window engaged (stalled pump passes counted)" \
    "stalls node0=${s0:-?} node2=${s2:-?} after 25s of 800ms delay"
  net_clear 1
  wait $W1 $W2
  sleep 5
  check_counter chaos:cnt 90
  check_set chaos:set 90
  check_converged "counter" 90 get chaos:cnt
  check_replication_healed 120
  local drops=0 i v
  for i in 0 1 2; do
    v=$(metric_value "$i" marekvs_repl_send_failures_total); drops=$((drops + ${v:-0}))
  done
  chk $((drops > 0)) "no batch dropped after cursor advance" \
    "repl_send_failures_total sum=$drops — writer queues overran the window?"
}

# ── scenario: clock_bump_skew (debug, apple) ─────────────────────────────
# Per-VM clocks (apple) let us skew ONE node's wall clock while it takes
# writes. The HLC receive rule must absorb it: a node bumped +N seconds must
# not make its LWW writes win forever, and counters/sets stay exact. This is
# the regression test the receive rule never had.
clock_bump_skew() {
  require_debug "clock skew"
  [ "$BACKEND" = apple ] || { echo "  (skipped: clock skew needs the apple backend)"; return 0; }
  fresh_cluster
  counter_workload chaos:cnt 45 & local W1=$!
  set_workload chaos:set 45 & local W2=$!
  register_workload chaos:reg 45 & local W3=$!
  sleep 5
  clock_bump 1 10      # node 1 jumps +10s
  assert_skewed 1 0    # fail loudly if the bump was a no-op (vacuous test)
  sleep 5
  clock_bump 2 -10     # node 2 jumps -10s (into the past)
  sleep 8
  clock_bump 1 100     # node 1 far future
  sleep 5
  clock_reset 1; clock_reset 2
  wait $W1 $W2 $W3
  sleep 3
  check_counter chaos:cnt
  check_set chaos:set
  check_converged "LWW register after skew" 45 get chaos:reg
  check_replication_healed 90
}

# ── scenario: clock_strobe (debug, apple) ────────────────────────────────
# Strobe one node's clock ±4s for 20s under load (Jepsen strobe): rapid
# back-and-forth jumps, the harshest test of HLC monotonicity.
clock_strobe() {
  require_debug "clock strobe"
  [ "$BACKEND" = apple ] || { echo "  (skipped: clock strobe needs the apple backend)"; return 0; }
  fresh_cluster
  counter_workload chaos:cnt 45 & local W1=$!
  set_workload chaos:set 45 & local W2=$!
  sleep 5
  clock_bump 1 8       # prove skew works before strobing
  assert_skewed 1 0
  clock_strobe_run 1 4 500 20   # ±4s, toggle every 500ms, for 20s
  wait $W1 $W2
  clock_reset 1
  sleep 3
  check_counter chaos:cnt
  check_set chaos:set
  check_converged "counter after strobe" 45 get chaos:cnt
  check_replication_healed 90
}

# ── run ──────────────────────────────────────────────────────────────────
CHAOS_ROOT=$CHAOS_DIR
for s in "${SCENARIOS[@]}"; do
  scenario_banner "$s"
  CHAOS_DIR="$CHAOS_ROOT/$s"
  mkdir -p "$CHAOS_DIR"
  fail_before=$fail
  "$s"
  [ "$fail" != "$fail_before" ] && capture_logs "$s"
done
CHAOS_DIR=$CHAOS_ROOT

cluster_down
echo
if [ "$fail" = 0 ]; then
  echo "CHAOS TEST PASSED (backend=$BACKEND, scenarios: ${SCENARIOS[*]})"
else
  echo "CHAOS TEST FAILED (history preserved in $CHAOS_DIR)"
  exit 1
fi

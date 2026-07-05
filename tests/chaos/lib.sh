#!/usr/bin/env bash
# Chaos/churn harness core — Jepsen-inspired fault injection for marekvs.
# Sourced by chaos_test.sh. Two backends:
#
#   BACKEND=docker  full fault menu incl. TRUE network partitions: every node
#                   joins two networks — "mesh" (advertised; gossip/mesh/
#                   replication) and "edge" (host-published client ports).
#                   A partition disconnects only the mesh net, so clients
#                   keep writing to BOTH sides of the split (split-brain,
#                   the AP case that matters).
#   BACKEND=apple   Apple `container` CLI: one lightweight VM per node with
#                   its OWN clock (real clock skew — this environment found
#                   the HLC receive-rule bug). No runtime net detach, so
#                   partition scenarios are skipped; crash/freeze/churn run.
#
# History model (jepsen/src/jepsen/checker.clj): every op is acked (:ok),
# failed (:fail, definitely not applied) or indeterminate (:info — timeout/
# connection error, may or may not have applied). Checkers accept exactly
# what Jepsen's counter / set checkers accept:
#   counter  every read r satisfies lower(r) <= value <= upper(r), where
#            lower = acked increments when the read started and upper =
#            acked+indeterminate when it finished (checker.clj:828)
#   set      no acked element may be absent (lost), no element that was
#            never attempted may be present (phantom), no element may
#            appear twice (duplicate — the CRDT merge bug class); elements
#            from indeterminate adds are legal (recovered) (checker.clj:324)

set -euo pipefail

BACKEND=${BACKEND:-docker}
# CHAOS_DEBUG=1: use the debug image (same binary over alpine + iptables/tc/
# GNU date, see Dockerfile.debug) and grant the fault-injection caps. The
# grudge/netem/clock nemeses require it; everything else runs on scratch.
CHAOS_DEBUG=${CHAOS_DEBUG:-0}
if [ "$CHAOS_DEBUG" = 1 ]; then
  IMAGE=${IMAGE:-marekvs:debug}
else
  IMAGE=${IMAGE:-marekvs:dev}
fi
N=${N:-3}
MESH_NET=chaos-mesh
EDGE_NET=chaos-edge
MESH_SUBNET=172.29.10.0/24
EDGE_SUBNET=172.29.11.0/24
CHAOS_DIR=${CHAOS_DIR:-$(mktemp -d /tmp/marekvs-chaos.XXXXXX)}
# Apple containers get a NEW IP on every restart (like k8s pods) and the
# v1.0 CLI offers neither static IPs nor working DNS registration. Initial
# seeding is therefore staged (node 0 boots first, the rest seed off its
# current IP); RESTART survival comes from marekvs itself: every node
# persists its peers'\'' gossip addresses (meta "peers:last") and merges them
# into its seed list at boot, so a revived node re-finds the survivors at
# their unchanged addresses and gossip heals the rest.

mesh_ip() { echo "172.29.10.$((10 + $1))"; }
edge_ip() { echo "172.29.11.$((10 + $1))"; }
resp_port() { echo $((26379 + $1)); }
metrics_port() { echo $((27121 + $1)); }

# ── backend dispatch ─────────────────────────────────────────────────────

crt() { # container runtime verb passthrough
  if [ "$BACKEND" = docker ]; then docker "$@"; else container "$@"; fi
}

apple_ip() { # container name → ip
  container inspect "$1" 2>/dev/null | python3 -c '
import json, sys
try: data = json.load(sys.stdin)
except Exception: sys.exit(0)
item = data[0] if isinstance(data, list) else data
for n in item.get("status", {}).get("networks", []):
    addr = n.get("ipv4Address") or ""
    if addr: print(addr.split("/")[0]); break'
}

# redis-cli against node i (host-published port on docker, VM IP on apple).
rcli() { # <i> <args...>
  local i=$1; shift
  if [ "$BACKEND" = docker ]; then
    redis-cli -p "$(resp_port "$i")" "$@"
  else
    redis-cli -h "$(apple_ip "chaos-$i")" -p 6379 "$@"
  fi
}

metrics() { # <i> → prometheus body ("" on failure)
  local i=$1
  if [ "$BACKEND" = docker ]; then
    curl -s --max-time 2 "http://127.0.0.1:$(metrics_port "$i")/metrics" || true
  else
    curl -s --max-time 2 "http://$(apple_ip "chaos-$i"):9121/metrics" || true
  fi
}

seeds() {
  local out="" i
  for i in $(seq 0 $((N - 1))); do out+="$(mesh_ip "$i"):7946,"; done
  echo "${out%,}"
}

node_run() { # <i> — create + start node i
  local i=$1
  if [ "$BACKEND" = docker ]; then
    local caps=()
    [ "$CHAOS_DEBUG" = 1 ] && caps=(--cap-add NET_ADMIN --cap-add SYS_TIME)
    docker run -d --name "chaos-$i" "${caps[@]}" \
      --network "$EDGE_NET" --ip "$(edge_ip "$i")" \
      -p "$(resp_port "$i"):6379" -p "$(metrics_port "$i"):9121" \
      -e MAREKVS_NODE_ID="$i" -e MAREKVS_REPLICAS_N=2 \
      -e MAREKVS_DATA_DIR=/data -e MAREKVS_ADVERTISE_IP="$(mesh_ip "$i")" \
      -e MAREKVS_SEEDS="$(seeds)" -e RUST_LOG=${CHAOS_LOG:-info},chitchat=warn \
      "$IMAGE" >/dev/null
    docker network connect --ip "$(mesh_ip "$i")" "$MESH_NET" "chaos-$i"
  else
    local aseeds="" caps=()
    if [ "$i" != 0 ]; then aseeds="$(apple_ip chaos-0):7946"; fi
    [ "$CHAOS_DEBUG" = 1 ] && caps=(--cap-add CAP_SYS_TIME --cap-add CAP_NET_ADMIN)
    container run -d --name "chaos-$i" "${caps[@]}" \
      -e MAREKVS_NODE_ID="$i" -e MAREKVS_REPLICAS_N=2 \
      -e MAREKVS_DATA_DIR=/data -e MAREKVS_ADVERTISE_IP=auto \
      -e MAREKVS_SEEDS="$aseeds" -e RUST_LOG=${CHAOS_LOG:-info},chitchat=warn \
      "$IMAGE" >/dev/null
  fi
}

wait_ready() { # <i> [tries=100]
  local i=$1 tries=${2:-100} t
  for t in $(seq 1 "$tries"); do
    if rcli "$i" ping 2>/dev/null | grep -q PONG; then return 0; fi
    if [ "$t" = "$tries" ]; then
      echo "node $i never became ready" >&2
      crt logs "chaos-$i" 2>&1 | tail -10 >&2
      return 1
    fi
    sleep 0.4
  done
}

cluster_up() {
  echo "chaos: starting $N-node cluster (backend=$BACKEND, history=$CHAOS_DIR)"
  local i
  if [ "$BACKEND" = docker ]; then
    docker network create --subnet "$MESH_SUBNET" "$MESH_NET" >/dev/null 2>&1 || true
    docker network create --subnet "$EDGE_SUBNET" "$EDGE_NET" >/dev/null 2>&1 || true
    for i in $(seq 0 $((N - 1))); do node_run "$i"; done
  else
    container system start 2>/dev/null || true
    node_run 0
    sleep 2
    for i in $(seq 1 $((N - 1))); do node_run "$i"; done
  fi
  for i in $(seq 0 $((N - 1))); do wait_ready "$i"; done
  echo "chaos: cluster ready"
}

cluster_down() {
  local i
  for i in $(seq 0 9); do
    crt rm -f "chaos-$i" >/dev/null 2>&1 || true
  done
  # Safety sweep: also remove any container still running a marekvs image.
  # An apple `container run` without --name (e.g. an ad-hoc probe) leaves a
  # UUID-named container the chaos-N loop above can't see; without this it
  # lingers forever (a stray debug cluster was found exactly that way).
  if [ "$BACKEND" = docker ]; then
    local stray
    stray=$(docker ps -aq --filter "ancestor=marekvs:debug" --filter "ancestor=marekvs:dev" 2>/dev/null | sort -u)
    [ -n "$stray" ] && docker rm -f $stray >/dev/null 2>&1 || true
    docker network rm "$MESH_NET" "$EDGE_NET" >/dev/null 2>&1 || true
  else
    local id
    for id in $(container ls -a 2>/dev/null | awk '/marekvs:(debug|dev)/ {print $1}'); do
      container rm -f "$id" >/dev/null 2>&1 || true
    done
  fi
}

# ── debug-image helpers ──────────────────────────────────────────────────

# Run a command inside node i (debug image only — scratch has no shell).
nexec() { # <i> <cmd...>
  crt exec "chaos-$1" "${@:2}"
}

require_debug() { # <what>
  if [ "$CHAOS_DEBUG" != 1 ]; then
    echo "ERROR: $1 needs the debug image — rerun with CHAOS_DEBUG=1 (just chaos-debug / chaos-clock)" >&2
    exit 2
  fi
}

# The interface on the MESH subnet inside node i (nodes have two nics on
# docker; tc/iptables must hit the mesh one, matched by address).
mesh_nic() { # <i>
  nexec "$1" sh -c "ip -o -4 addr show | awk '/172\.29\.10\./ {print \$2; exit}'"
}

# ── nemeses (jepsen/src/jepsen/nemesis.clj) ──────────────────────────────

crash() { # <i> — kill -9; container filesystem (and data) survives
  echo "  nemesis: crash node $1 (SIGKILL)"
  crt kill -s KILL "chaos-$1" >/dev/null 2>&1 || true
}

revive() { # <i> — restart a crashed/stopped node
  echo "  nemesis: revive node $1"
  crt start "chaos-$1" >/dev/null
  wait_ready "$1"
}

freeze() { # <i> — SIGSTOP: silent but alive (Jepsen hammer-time)
  echo "  nemesis: freeze node $1 (SIGSTOP)"
  crt kill -s STOP "chaos-$1" >/dev/null
}

thaw() { # <i>
  echo "  nemesis: thaw node $1 (SIGCONT)"
  crt kill -s CONT "chaos-$1" >/dev/null
}

graceful_restart() { # <i> — SIGTERM drain, then start again
  echo "  nemesis: graceful restart node $1"
  crt stop "chaos-$1" >/dev/null
  crt start "chaos-$1" >/dev/null
  wait_ready "$1"
}

partition() { # <i> — cut node i's mesh link; clients still reach it (docker)
  [ "$BACKEND" = docker ] || { echo "partition: docker only" >&2; return 1; }
  echo "  nemesis: partition node $1 from the mesh"
  docker network disconnect "$MESH_NET" "chaos-$1"
  # verify it actually took effect (docker desktop can lag)
  for _ in 1 2 3 4 5; do
    docker network inspect "$MESH_NET" 2>/dev/null | grep -q "\"chaos-$1\"" || return 0
    sleep 0.5
  done
  echo "  WARNING: partition of node $1 did not take effect" >&2
}

heal() { # <i> — idempotent: docker's disconnect can lag, so tolerate an
  # endpoint that never left and retry once on transient connect errors.
  echo "  nemesis: heal partition of node $1"
  if docker network inspect "$MESH_NET" 2>/dev/null | grep -q "\"chaos-$1\""; then
    return 0
  fi
  docker network connect --ip "$(mesh_ip "$1")" "$MESH_NET" "chaos-$1" 2>/dev/null || {
    sleep 1
    docker network inspect "$MESH_NET" 2>/dev/null | grep -q "\"chaos-$1\"" ||
      docker network connect --ip "$(mesh_ip "$1")" "$MESH_NET" "chaos-$1"
  }
}

# ── grudge partitions (debug image, iptables) ────────────────────────────
# Realize a Jepsen grudge (tests/chaos/grudge.py) as symmetric iptables DROP
# rules on the MESH subnet: node a drops packets from/to node b's mesh IP,
# both directions, for every cut in the grudge. Clients on the edge net are
# untouched, so writes continue on every side of the split.

grudge_apply() { # <topology> — halves|bridge|ring, over N nodes
  require_debug "grudge partitions"
  echo "  nemesis: grudge partition ($1) over $N nodes"
  local a b
  while read -r a b; do
    [ -n "$a" ] || continue
    # a refuses b's mesh address in both directions.
    nexec "$a" iptables -A INPUT  -s "$(mesh_ip "$b")" -j DROP 2>/dev/null || true
    nexec "$a" iptables -A OUTPUT -d "$(mesh_ip "$b")" -j DROP 2>/dev/null || true
  done < <(python3 "$(dirname "${BASH_SOURCE[0]}")/grudge.py" "$1" "$N")
}

grudge_heal() { # flush all grudge rules on every node
  echo "  nemesis: heal grudge partition"
  local i
  for i in $(seq 0 $((N - 1))); do
    nexec "$i" iptables -F 2>/dev/null || true
  done
}

# ── conntrack blackhole (debug image, iptables) ──────────────────────────
# Wedge the ESTABLISHED mesh TCP connections between two nodes without
# closing them: drop mesh-port TCP in both directions on both nodes, but
# leave gossip UDP (7946) untouched. Membership keeps both nodes Active
# while their replication link is silently dead — the exact failure the
# mesh heartbeat exists to detect (phi-accrual can't see it).

blackhole() { # <a> <b>
  require_debug "conntrack blackhole"
  echo "  nemesis: blackhole mesh TCP between node $1 and node $2"
  local a b
  for pair in "$1 $2" "$2 $1"; do
    read -r a b <<<"$pair"
    nexec "$a" iptables -A INPUT  -p tcp -s "$(mesh_ip "$b")" --dport 7373 -j DROP 2>/dev/null || true
    nexec "$a" iptables -A INPUT  -p tcp -s "$(mesh_ip "$b")" --sport 7373 -j DROP 2>/dev/null || true
    nexec "$a" iptables -A OUTPUT -p tcp -d "$(mesh_ip "$b")" --dport 7373 -j DROP 2>/dev/null || true
    nexec "$a" iptables -A OUTPUT -p tcp -d "$(mesh_ip "$b")" --sport 7373 -j DROP 2>/dev/null || true
  done
}

blackhole_heal() { # flush on every node (same rules table as grudge)
  grudge_heal
}

# ── tc-netem packet faults (debug image) ─────────────────────────────────
# Shape the mesh nic of one node. root qdisc, so a second call replaces the
# first; net_clear removes it.

net_netem() { # <i> <netem-args...>
  require_debug "tc-netem packet faults"
  local nic; nic=$(mesh_nic "$1")
  nexec "$1" tc qdisc replace dev "$nic" root netem "${@:2}"
}
net_delay()   { echo "  nemesis: netem delay ${2}ms on node $1";  net_netem "$1" delay "${2}ms" "${3:-0}ms"; }
net_loss()    { echo "  nemesis: netem ${2}% loss on node $1";     net_netem "$1" loss "${2}%"; }
net_corrupt() { echo "  nemesis: netem ${2}% corrupt on node $1";  net_netem "$1" corrupt "${2}%"; }
net_reorder() { echo "  nemesis: netem reorder on node $1";        net_netem "$1" delay 20ms reorder "${2:-25}%"; }
net_clear() { # <i>
  echo "  nemesis: clear netem on node $1"
  local nic; nic=$(mesh_nic "$1")
  nexec "$1" tc qdisc del dev "$nic" root 2>/dev/null || true
}

# ── clock faults (debug image, apple backend) ────────────────────────────
# Each apple container is its own VM with its own clock, so `date -s` inside
# node i skews ONLY that node — the vehicle for real HLC clock-skew tests
# (docker shares one VM clock; can't skew a single node). Needs SYS_TIME /
# VM root. The static-musl marekvs binary rules out libfaketime.

assert_skewed() { # <i> <ref> — fail the test unless node i's clock differs
  local a b
  a=$(nexec "$1" date +%s 2>/dev/null || echo 0)
  b=$(nexec "$2" date +%s 2>/dev/null || echo 0)
  local d=$(( a > b ? a - b : b - a ))
  if [ "$d" -ge 3 ]; then
    chk 0 "clock skew active: node $1 is ${d}s from node $2"
  else
    chk 1 "clock skew injection" "node $1 within ${d}s of node $2 — bump was a NO-OP (missing CAP_SYS_TIME?); test would be vacuous"
  fi
}

clock_offset_ok() { # verify a skew actually took effect: node i vs node 0
  local skew; skew=$(nexec "$1" date +%s 2>/dev/null)
  local base; base=$(nexec 0 date +%s 2>/dev/null)
  [ -n "$skew" ] && [ -n "$base" ] && [ "$skew" != "$base" ]
}

clock_bump() { # <i> <±seconds> — step node i's wall clock by delta
  require_debug "clock skew"
  echo "  nemesis: bump node $1 clock by ${2}s"
  nexec "$1" sh -c "date -s @\$(( \$(date +%s) + ($2) ))" >/dev/null 2>&1 ||
    echo "  WARNING: clock bump on node $1 failed (needs SYS_TIME / VM root)" >&2
}

clock_reset() { # <i> — resync node i to the harness host clock
  echo "  nemesis: reset node $1 clock to true time"
  nexec "$1" sh -c "date -s @$(date +%s)" >/dev/null 2>&1 || true
}

clock_strobe_run() { # <i> <delta_s> <period_ms> <duration_s>
  require_debug "clock strobe"
  echo "  nemesis: strobe node $1 clock ±${2}s every ${3}ms for ${4}s"
  local i=$1 delta=$2 period=$3 dur=$4
  ( local t0=$SECONDS weird=0
    while [ $((SECONDS - t0)) -lt "$dur" ]; do
      if [ "$weird" = 0 ]; then
        nexec "$i" sh -c "date -s @\$(( \$(date +%s) + $delta ))" >/dev/null 2>&1 || true
        weird=1
      else
        nexec "$i" sh -c "date -s @\$(( \$(date +%s) - $delta ))" >/dev/null 2>&1 || true
        weird=0
      fi
      sleep "0.$(printf '%03d' "$period")" 2>/dev/null || sleep 0.5
    done
    nexec "$i" sh -c "date -s @$(date +%s)" >/dev/null 2>&1 || true
  ) &
}

wipe_node() { # <i> — destroy node incl. its data, boot a fresh replacement
  echo "  nemesis: wipe node $1 (data destroyed) and boot a replacement"
  crt rm -f "chaos-$1" >/dev/null
  node_run "$1"
  wait_ready "$1" 300 # fresh node must bootstrap
}

# ── workloads ────────────────────────────────────────────────────────────
# One writer process per workload (histories are single-writer, so the
# Jepsen read-window bookkeeping needs no locking). Ops rotate across nodes
# so faults always hit active writers.

_pick() { echo $(( $1 % N )); }

# INCR loop with interleaved windowed reads (Jepsen counter workload).
# Files: counter.acked (one line per acked +1), counter.indet,
#        counter.reads (lines "lower value upper").
counter_workload() { # <key> <duration_s>
  local key=$1 dur=$2 t0=$SECONDS n=0
  : > "$CHAOS_DIR/counter.acked"
  : > "$CHAOS_DIR/counter.indet"
  : > "$CHAOS_DIR/counter.reads"
  while [ $((SECONDS - t0)) -lt "$dur" ]; do
    local i; i=$(_pick $n); n=$((n + 1))
    local out
    out=$(rcli "$i" -t 2 incr "$key" 2>/dev/null || true)
    case "$out" in
      ''|*[!0-9]*) echo "n$i $out" >> "$CHAOS_DIR/counter.indet" ;;
      *) echo "n$i $out" >> "$CHAOS_DIR/counter.acked" ;;
    esac
    if [ $((n % 20)) = 0 ]; then
      # windowed read: lower at invoke, upper at completion (checker.clj:832)
      local lower value upper ri
      lower=$(wc -l < "$CHAOS_DIR/counter.acked" | tr -d ' ')
      ri=$(_pick $((n / 20)))
      value=$(rcli "$ri" -t 2 get "$key" 2>/dev/null || true)
      upper=$(( $(wc -l < "$CHAOS_DIR/counter.acked" | tr -d ' ') \
              + $(wc -l < "$CHAOS_DIR/counter.indet" | tr -d ' ') ))
      case "$value" in
        ''|*[!0-9]*) : ;; # nil / error / non-numeric: skip this read
        *) echo "$lower $value $upper n$ri" >> "$CHAOS_DIR/counter.reads" ;;
      esac
    fi
  done
}

# SADD of unique elements (Jepsen set workload).
set_workload() { # <key> <duration_s>
  local key=$1 dur=$2 t0=$SECONDS n=0
  : > "$CHAOS_DIR/set.acked"
  : > "$CHAOS_DIR/set.indet"
  while [ $((SECONDS - t0)) -lt "$dur" ]; do
    local i el; i=$(_pick $n); el="el-$n"; n=$((n + 1))
    local out
    out=$(rcli "$i" -t 2 sadd "$key" "$el" 2>/dev/null || true)
    case "$out" in
      ''|*[!0-9]*) echo "$el n$i" >> "$CHAOS_DIR/set.indet" ;;
      *) echo "$el n$i" >> "$CHAOS_DIR/set.acked" ;;
    esac
  done
}

# LWW SET churn (convergence fuel — checked only for total convergence).
register_workload() { # <key> <duration_s>
  local key=$1 dur=$2 t0=$SECONDS n=0
  while [ $((SECONDS - t0)) -lt "$dur" ]; do
    rcli "$(_pick $n)" -t 2 set "$key" "val-$n" >/dev/null 2>&1 || true
    n=$((n + 1))
  done
}

# ── checkers ─────────────────────────────────────────────────────────────

fail=0
chk() { # <ok?> <desc> [detail]
  if [ "$1" = 0 ]; then echo "  ok: $2"; else echo "  FAIL: $2 — ${3:-}"; fail=1; fi
}

# Counter acceptance, adapted to AP semantics the way Jepsen's set-full
# adapts with :linearizable? false — staleness is legal, loss is not:
# (a) mid-run reads assert only the UPPER bound (value <= acked+indet at
#     completion — more increments than were ever sent = invented state);
#     reads below `lower` are bounded staleness on a replica and are
#     reported as a distribution, not failed;
# (b) the FINAL read must satisfy the full Jepsen window
#     [acked, acked+indet] on EVERY node — checked with a convergence
#     retry (AE may still be repairing right after the last fault heals).
check_counter() { # <key> [converge_timeout=45]
  local acked indet timeout=${2:-45}
  acked=$(wc -l < "$CHAOS_DIR/counter.acked" | tr -d ' ')
  indet=$(wc -l < "$CHAOS_DIR/counter.indet" | tr -d ' ')

  local total_reads invented stale max_lag
  total_reads=$(wc -l < "$CHAOS_DIR/counter.reads" | tr -d ' ')
  invented=$(awk '$2 > $3' "$CHAOS_DIR/counter.reads" | wc -l | tr -d ' ')
  stale=$(awk '$2 < $1' "$CHAOS_DIR/counter.reads" | wc -l | tr -d ' ')
  max_lag=$(awk '$2 < $1 { d = $1 - $2; if (d > m) m = d } END { print m + 0 }' "$CHAOS_DIR/counter.reads")
  if [ "$invented" = 0 ]; then
    chk 0 "counter: no invented increments in $total_reads mid-run reads ($stale stale reads, max lag $max_lag — legal AP staleness)"
  else
    chk 1 "counter mid-run reads" "$invented of $total_reads ABOVE upper bound (invented increments); worst: $(awk '$2 > $3' "$CHAOS_DIR/counter.reads" | head -1)"
  fi

  local deadline=$((SECONDS + timeout)) i v ok
  while :; do
    ok=1
    for i in $(seq 0 $((N - 1))); do
      v=$(rcli "$i" get "$1" 2>/dev/null || true)
      case "$v" in ''|*[!0-9]*) ok=0; break ;; esac
      { [ "$v" -ge "$acked" ] && [ "$v" -le $((acked + indet)) ]; } || { ok=0; break; }
    done
    [ "$ok" = 1 ] && break
    [ $SECONDS -lt "$deadline" ] || break
    sleep 1
  done
  for i in $(seq 0 $((N - 1))); do
    v=$(rcli "$i" get "$1" 2>/dev/null || true)
    if [ -z "$v" ]; then
      chk 1 "counter final on node $i" "unreadable"
    elif [ "$v" -ge "$acked" ] && [ "$v" -le $((acked + indet)) ]; then
      chk 0 "counter final on node $i: $v in [$acked, $((acked + indet))]"
    elif [ "$v" -lt "$acked" ]; then
      chk 1 "counter final on node $i" "$v < acked $acked after ${timeout}s: $((acked - v)) acked increments LOST"
    else
      chk 1 "counter final on node $i" "$v > acked+indet $((acked + indet)): $((v - acked - indet)) increments INVENTED (double-merge?)"
    fi
  done
}

# Set acceptance: lost / phantom / duplicate per node (checker.clj:324 +
# the set-full duplicate rule), with a convergence retry — an element
# missing on one node right after heal is AE lag (legal staleness); LOST
# means it never arrives within the timeout.
check_set() { # <key> [converge_timeout=45]
  local timeout=${2:-45}
  awk '{print $1}' "$CHAOS_DIR/set.acked" | sort -u > "$CHAOS_DIR/set.acked.sorted"
  awk '{print $1}' "$CHAOS_DIR/set.acked" "$CHAOS_DIR/set.indet" | sort -u > "$CHAOS_DIR/set.legal.sorted"
  local total i
  total=$(wc -l < "$CHAOS_DIR/set.acked.sorted" | tr -d ' ')

  _set_node_ok() { # <i> — 0 iff node i currently satisfies the checker
    rcli "$1" smembers "$2" 2>/dev/null > "$CHAOS_DIR/set.read.$1.raw"
    sort "$CHAOS_DIR/set.read.$1.raw" > "$CHAOS_DIR/set.read.$1"
    sort -u "$CHAOS_DIR/set.read.$1" > "$CHAOS_DIR/set.read.$1.uniq"
    lost=$(comm -23 "$CHAOS_DIR/set.acked.sorted" "$CHAOS_DIR/set.read.$1.uniq" | wc -l | tr -d ' ')
    phantom=$(comm -13 "$CHAOS_DIR/set.legal.sorted" "$CHAOS_DIR/set.read.$1.uniq" | wc -l | tr -d ' ')
    dups=$(uniq -d "$CHAOS_DIR/set.read.$1" | wc -l | tr -d ' ')
    [ "$lost" = 0 ] && [ "$phantom" = 0 ] && [ "$dups" = 0 ]
  }

  local deadline=$((SECONDS + timeout)) all_ok lost phantom dups
  while :; do
    all_ok=1
    for i in $(seq 0 $((N - 1))); do
      _set_node_ok "$i" "$1" || { all_ok=0; break; }
    done
    [ "$all_ok" = 1 ] && break
    [ $SECONDS -lt "$deadline" ] || break
    sleep 1
  done
  for i in $(seq 0 $((N - 1))); do
    if _set_node_ok "$i" "$1"; then
      chk 0 "set on node $i: all $total acked elements present, no phantoms, no duplicates"
    else
      chk 1 "set on node $i" "$lost acked LOST after ${timeout}s (first: $(comm -23 "$CHAOS_DIR/set.acked.sorted" "$CHAOS_DIR/set.read.$i.uniq" | head -1)), $phantom PHANTOM, $dups DUPLICATED"
      local el
      comm -23 "$CHAOS_DIR/set.acked.sorted" "$CHAOS_DIR/set.read.$i.uniq" | head -8 | while read -r el; do
        local origin present="" j
        origin=$(awk -v e="$el" '$1 == e {print $2; exit}' "$CHAOS_DIR/set.acked")
        for j in $(seq 0 $((N - 1))); do
          present+="n$j=$(rcli "$j" sismember "$1" "$el" 2>/dev/null || echo '?') "
        done
        echo "    post-mortem: $el acked-by $origin, present: $present"
      done
    fi
  done
}

# All nodes converge to the identical reply for <cli-args...>.
check_converged() { # <desc> <timeout_s> <cli-args...>
  local desc=$1 timeout=$2; shift 2
  local deadline=$((SECONDS + timeout)) v0 vi i all_same
  while [ $SECONDS -lt "$deadline" ]; do
    v0=$(rcli 0 "$@" 2>/dev/null || echo "?0")
    all_same=1
    for i in $(seq 1 $((N - 1))); do
      vi=$(rcli "$i" "$@" 2>/dev/null || echo "?$i")
      [ "$vi" = "$v0" ] || { all_same=0; break; }
    done
    if [ "$all_same" = 1 ] && [ "$v0" != "?0" ]; then
      chk 0 "$desc converged on all nodes"
      return 0
    fi
    sleep 0.5
  done
  chk 1 "$desc" "no total convergence within ${timeout}s (node0=[$v0] node$i=[${vi:-}])"
  return 0
}

# Set convergence with order-insensitive comparison (SMEMBERS order is
# arbitrary). expected="" waits for the set to be empty everywhere.
check_set_converged() { # <desc> <key> <timeout_s> [expected space-joined sorted members]
  local desc=$1 key=$2 timeout=$3 expected=${4-__any__}
  local deadline=$((SECONDS + timeout)) v0 vi i all_same
  while [ $SECONDS -lt "$deadline" ]; do
    v0=$(rcli 0 smembers "$key" 2>/dev/null | sort | tr '\n' ' ' | sed 's/ *$//')
    all_same=1
    for i in $(seq 1 $((N - 1))); do
      vi=$(rcli "$i" smembers "$key" 2>/dev/null | sort | tr '\n' ' ' | sed 's/ *$//')
      [ "$vi" = "$v0" ] || { all_same=0; break; }
    done
    if [ "$all_same" = 1 ]; then
      if [ "$expected" = "__any__" ] || [ "$v0" = "$expected" ]; then
        chk 0 "$desc converged on all nodes [$v0]"
        return 0
      fi
    fi
    sleep 0.5
  done
  chk 1 "$desc" "no convergence to [${expected}] within ${timeout}s (node0=[$v0])"
  return 0
}

# Post-mortem: dump every node's logs into the history dir.
capture_logs() { # <label>
  local i
  for i in $(seq 0 $((N))); do
    crt logs "chaos-$i" > "$CHAOS_DIR/logs.$1.node$i" 2>&1 || true
  done
  echo "  (node logs captured to $CHAOS_DIR/logs.$1.*)"
}

# One unlabeled prometheus metric from node i ("" when absent/unreachable).
metric_value() { # <i> <metric_name>
  metrics "$1" | awk -v m="$2" '$1 == m {print $2; exit}'
}

# Every node's underreplicated gauge must return to 0 after faults — this
# is the operator's scale-safety signal.
check_replication_healed() { # <timeout_s>
  local deadline=$((SECONDS + $1)) worst i u
  while [ $SECONDS -lt "$deadline" ]; do
    worst=0
    for i in $(seq 0 $((N - 1))); do
      u=$(metrics "$i" | awk '/^marekvs_cluster_underreplicated_partitions/ {print $2}')
      [ -n "$u" ] || u=99999
      [ "$u" -gt "$worst" ] && worst=$u
    done
    if [ "$worst" = 0 ]; then
      chk 0 "underreplicated_partitions back to 0 on all nodes"
      return 0
    fi
    sleep 1
  done
  chk 1 "replication heal" "underreplicated_partitions stuck at $worst after $1s"
  return 0
}

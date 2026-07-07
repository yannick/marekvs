#!/usr/bin/env bash
# CLUSTER protocol smoke: slot mapping, topology replies, direct-master routing.
# Usage: tests/cluster_protocol.sh [binary]
set -euo pipefail
cd "$(dirname "$0")/.."

BIN=${1:-target/debug/marekvs-server}
[ -x "$BIN" ] || cargo build -p marekvs-server
N=3
DIR=$(mktemp -d)
SEEDS=""
for i in $(seq 0 $((N - 1))); do SEEDS+="127.0.0.1:$((17946 + i)),"; done

pids=()
cleanup() { kill "${pids[@]}" 2>/dev/null || true; rm -rf "$DIR"; }
trap cleanup EXIT

for i in $(seq 0 $((N - 1))); do
  MAREKVS_NODE_ID=$i \
    MAREKVS_DATA_DIR="$DIR/n$i" \
    MAREKVS_RESP_ADDR="127.0.0.1:$((16379 + i))" \
    MAREKVS_MESH_ADDR="127.0.0.1:$((17373 + i))" \
    MAREKVS_GOSSIP_ADDR="127.0.0.1:$((17946 + i))" \
    MAREKVS_METRICS_ADDR="127.0.0.1:$((19121 + i))" \
    MAREKVS_ADVERTISE_IP=127.0.0.1 \
    MAREKVS_SEEDS="${SEEDS%,}" \
    MAREKVS_REPLICAS_N=2 \
    RUST_LOG=warn "$BIN" &
  pids+=($!)
done

for i in $(seq 0 $((N - 1))); do
  for _ in $(seq 1 60); do
    redis-cli -p $((16379 + i)) PING 2>/dev/null | grep -q PONG && break
    sleep 0.5
  done
done
# All RESP ports up ≠ every node's VIEW has all members Active yet (gossip
# interval 500 ms), and full membership ≠ slot ownership assigned yet either
# — slot assignment lags membership convergence by a further beat. Poll for
# both full membership AND full slot coverage before asserting topology.
for _ in $(seq 1 60); do
  nodes_now=$(redis-cli -p 16379 CLUSTER NODES 2>/dev/null || true)
  n=$(echo "$nodes_now" | grep -c .)
  cov=$(echo "$nodes_now" | awk '{for(i=9;i<=NF;i++){split($i,a,"-"); s+=a[2]-a[1]+1}} END{print s+0}')
  [ "$n" = "$N" ] && [ "$cov" = "16384" ] && break
  sleep 0.2
done

fail() { echo "FAIL: $1" >&2; exit 1; }

# 1. Redis-identical slot mapping.
[ "$(redis-cli -p 16379 CLUSTER KEYSLOT foo)" = "12182" ] || fail "KEYSLOT foo"
[ "$(redis-cli -p 16379 CLUSTER KEYSLOT bar)" = "5061" ] || fail "KEYSLOT bar"

# 2. MYID shape and stability across nodes' views.
id0=$(redis-cli -p 16379 CLUSTER MYID)
[ "${#id0}" = "40" ] || fail "MYID length (${#id0})"

# 3. INFO says enabled/ok.
redis-cli -p 16379 CLUSTER INFO | grep -q "cluster_enabled:1" || fail "INFO enabled"
redis-cli -p 16379 CLUSTER INFO | grep -q "cluster_state:ok" || fail "INFO state"

# 4. Full slot coverage == 16384, computed from CLUSTER NODES trailing
# slot-range fields (robust — CLUSTER SLOTS wire shape varies by redis-cli
# version, and the all-digit 40-hex node ids poison numeric line grubbing).
covered=$(redis-cli -p 16379 CLUSTER NODES \
  | awk '{for(i=9;i<=NF;i++){split($i,a,"-"); s+=a[2]-a[1]+1}} END{print s+0}')
[ "$covered" = "16384" ] || fail "slot coverage ($covered != 16384)"

# 5. NODES lists all members.
[ "$(redis-cli -p 16379 CLUSTER NODES | wc -l | tr -d ' ')" = "$N" ] || fail "NODES count"

# 6. The slot map is real: for each key, query the master CLUSTER NODES
# reports for its slot DIRECTLY (no -c, no redirects possible) and expect a
# hit. This proves keys physically live where the topology says they do —
# `redis-cli -c` would prove nothing here (marekvs never sends MOVED, so -c
# degenerates to read-through).
nodes=$(redis-cli -p 16379 CLUSTER NODES)
for k in alpha bravo charlie delta echo foxtrot golf hotel; do
  [ "$(redis-cli -p 16379 SET "k:$k" "v:$k")" = "OK" ] || fail "SET k:$k"
done
sleep 1 # replication settle
for k in alpha bravo charlie delta echo foxtrot golf hotel; do
  slot=$(redis-cli -p 16379 CLUSTER KEYSLOT "k:$k")
  mport=$(echo "$nodes" | awk -v s="$slot" '{
    for(i=9;i<=NF;i++){split($i,a,"-"); if (s>=a[1] && s<=a[2]) {
      split($2,hp,"@"); split(hp[1],ip,":"); print ip[2]; exit }}}')
  [ -n "$mport" ] || fail "no master for slot $slot"
  [ "$(redis-cli -p "$mport" GET "k:$k")" = "v:$k" ] || fail "GET k:$k on master :$mport"
done

echo "cluster_protocol: OK"

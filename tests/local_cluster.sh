#!/usr/bin/env bash
# Run an N-node cluster locally (foreground; ctrl-c stops all).
# Usage: tests/local_cluster.sh <binary> [nodes=3]
set -euo pipefail

BIN=${1:?usage: local_cluster.sh <binary> [nodes]}
N=${2:-3}
BASE_RESP=6379
BASE_MESH=7373
BASE_GOSSIP=7946

SEEDS=""
for i in $(seq 0 $((N - 1))); do
  SEEDS+="127.0.0.1:$((BASE_GOSSIP + i)),"
done

pids=()
cleanup() { kill "${pids[@]}" 2>/dev/null || true; }
trap cleanup EXIT

for i in $(seq 0 $((N - 1))); do
  mkdir -p ".data/n$i"
  MAREKVS_NODE_ID=$i \
    MAREKVS_DATA_DIR=".data/n$i" \
    MAREKVS_RESP_ADDR="127.0.0.1:$((BASE_RESP + i))" \
    MAREKVS_MESH_ADDR="127.0.0.1:$((BASE_MESH + i))" \
    MAREKVS_GOSSIP_ADDR="127.0.0.1:$((BASE_GOSSIP + i))" \
    MAREKVS_METRICS_ADDR="127.0.0.1:$((9121 + i))" \
    MAREKVS_ADVERTISE_IP=127.0.0.1 \
    MAREKVS_SEEDS="${SEEDS%,}" \
    MAREKVS_REPLICAS_N=2 \
    RUST_LOG=${RUST_LOG:-info,chitchat=warn} \
    "$BIN" &
  pids+=($!)
  echo "node $i: resp=:$((BASE_RESP + i)) pid=${pids[-1]}"
done

echo "cluster up — redis-cli -p 6379|6380|6381; ctrl-c to stop"
wait

#!/usr/bin/env bash
# 3-node marekvs cluster on Apple containers (`container` CLI).
# Nodes self-detect their IP (MAREKVS_ADVERTISE_IP=auto); only the seed
# needs to be known, so node 0 starts first and the rest seed off its IP.
# Usage: tests/apple_cluster.sh up [image] | test | down
set -euo pipefail

CMD=${1:?usage: apple_cluster.sh up [image] | test | down}
IMAGE=${2:-marekvs:dev}
NODES=(mkv-0 mkv-1 mkv-2)

container_ip() {
  container inspect "$1" 2>/dev/null | python3 -c '
import json, sys
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(0)
item = data[0] if isinstance(data, list) else data
for n in item.get("status", {}).get("networks", []):
    addr = n.get("ipv4Address") or ""
    if addr:
        print(addr.split("/")[0])
        break
'
}

run_node() { # <ordinal> <seeds>
  local i=$1 seeds=$2
  container run -d --name "mkv-$i" \
    -e MAREKVS_NODE_ID="$i" \
    -e MAREKVS_REPLICAS_N=2 \
    -e MAREKVS_DATA_DIR=/data \
    -e MAREKVS_ADVERTISE_IP=auto \
    -e MAREKVS_SEEDS="$seeds" \
    -e RUST_LOG=info,chitchat=warn \
    "$IMAGE" >/dev/null
}

case "$CMD" in
  up)
    container system start 2>/dev/null || true
    run_node 0 ""
    sleep 2
    IP0=$(container_ip mkv-0)
    [ -n "$IP0" ] || { echo "cannot determine mkv-0 IP"; container ls; exit 1; }
    echo "mkv-0 ip: $IP0"
    run_node 1 "$IP0:7946"
    run_node 2 "$IP0:7946"
    sleep 1
    echo "mkv-1 ip: $(container_ip mkv-1)"
    echo "mkv-2 ip: $(container_ip mkv-2)"

    echo "waiting for readiness..."
    for n in "${NODES[@]}"; do
      ip=$(container_ip "$n")
      for i in $(seq 1 100); do
        if redis-cli -h "$ip" -p 6379 ping 2>/dev/null | grep -q PONG; then
          echo "$n ($ip) ready"
          break
        fi
        [ "$i" = 100 ] && { echo "$n never became ready"; container logs "$n" 2>&1 | tail -20; exit 1; }
        sleep 0.4
      done
    done
    ;;

  test)
    IP0=$(container_ip mkv-0)
    IP1=$(container_ip mkv-1)
    IP2=$(container_ip mkv-2)
    R0="redis-cli -h $IP0 -p 6379"
    R1="redis-cli -h $IP1 -p 6379"
    R2="redis-cli -h $IP2 -p 6379"
    fail=0
    converge() {
      local desc=$1 expected=$2 timeout=$3; shift 3
      local deadline=$((SECONDS + timeout)) got=""
      while [ $SECONDS -lt $deadline ]; do
        got=$("$@" 2>/dev/null || true)
        [ "$got" = "$expected" ] && { echo "ok: $desc"; return 0; }
        sleep 0.3
      done
      echo "FAIL: $desc — expected [$expected] got [$got]"; fail=1
    }
    $R0 set apple:k hello >/dev/null
    converge "replicated read on node1" "hello" 10 $R1 get apple:k
    converge "replicated read on node2" "hello" 10 $R2 get apple:k
    $R1 set apple:k world >/dev/null
    converge "update visible on node0" "world" 20 $R0 get apple:k
    $R0 sadd apple:s x >/dev/null
    $R2 sadd apple:s y >/dev/null
    converge "OR-set union on node1" "x
y" 20 bash -c "$R1 smembers apple:s | sort"
    $R1 hset apple:h f v >/dev/null
    converge "hash visible on node2" "v" 10 $R2 hget apple:h f
    if [ "$fail" = 0 ]; then echo "APPLE CLUSTER TEST PASSED"; else echo "APPLE CLUSTER TEST FAILED"; exit 1; fi
    ;;

  down)
    for n in "${NODES[@]}"; do
      container rm -f "$n" 2>/dev/null || container stop "$n" 2>/dev/null || true
    done
    echo "apple cluster stopped"
    ;;

  *)
    echo "unknown subcommand: $CMD"; exit 1 ;;
esac

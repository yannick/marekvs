#!/usr/bin/env bash
# Run the benchmark workload matrix against one engine; emit CSV rows.
#
# Usage: run_workloads.sh <engine-label> <host> <port> [requests]
# Env:   BENCH_CLIENTS (50), BENCH_THREADS (4)
#
# Output columns:
#   engine,test,data_size,pipeline,clients,rps,avg_ms,p50_ms,p95_ms,p99_ms,max_ms
set -euo pipefail

ENGINE=${1:?usage: run_workloads.sh <engine> <host> <port> [requests]}
HOST=${2:?}
PORT=${3:?}
REQUESTS=${4:-100000}
CLIENTS=${BENCH_CLIENTS:-50}
THREADS=${BENCH_THREADS:-4}

# List tests run at REQUESTS/10: redis-benchmark pushes every request onto
# ONE fixed key (mylist), and marekvs lists are whole-value blobs — an
# N-request list phase costs O(N²) blob-rewrite bytes (see README caveat 3).
# Same reduced n for both engines, so per-op rates stay comparable.
CORE_TESTS="ping,set,get,incr,sadd,hset,spop,zadd,zpopmin,mset"
LIST_TESTS="lpush,rpush,lpop,lrange_100"

# The workload matrix: (data_size, pipeline). Pipeline 1 = per-op latency
# realism; pipeline 16 = throughput ceiling; 1 KiB values = bandwidth-ish.
MATRIX=(
  "100 1"
  "100 16"
  "1024 1"
)

run_tests() { # <data_size> <pipeline> <tests> <requests>
  local dsize=$1 pipeline=$2 tests=$3 requests=$4
  redis-benchmark -h "$HOST" -p "$PORT" \
    -n "$requests" -c "$CLIENTS" --threads "$THREADS" \
    -d "$dsize" -P "$pipeline" -t "$tests" -r 100000 --csv -q 2>/dev/null |
    while IFS= read -r line; do
      # Skip the header row redis-benchmark prints per invocation.
      case "$line" in \"test\"*) continue ;; esac
      # CSV fields: "TEST","rps","avg","min","p50","p95","p99","max"
      local clean=${line//\"/}
      IFS=',' read -r test rps avg _min p50 p95 p99 max <<<"$clean"
      echo "$ENGINE,$test,$dsize,$pipeline,$CLIENTS,$rps,$avg,$p50,$p95,$p99,$max"
    done
}

run_one() { # <data_size> <pipeline>
  local dsize=$1 pipeline=$2
  local list_requests=$((REQUESTS / 10))
  [ "$list_requests" -lt 1000 ] && list_requests=1000
  run_tests "$dsize" "$pipeline" "$CORE_TESTS" "$REQUESTS"
  run_tests "$dsize" "$pipeline" "$LIST_TESTS" "$list_requests"
}

# Fresh keyspace per CONFIG, not just per engine: redis-benchmark's list/
# set/zset tests hammer FIXED keys (mylist/myset/myzset), so collections
# grow across configs. On marekvs that compounds quadratically (whole-blob
# list rewrites) — an early stuck run took minutes on the P=16 config
# because mylist had grown unboundedly. Same wipe for both engines = fair.
for entry in "${MATRIX[@]}"; do
  read -r dsize pipeline <<<"$entry"
  echo "== $ENGINE d=$dsize P=$pipeline" >&2
  redis-cli -h "$HOST" -p "$PORT" flushall >/dev/null
  run_one "$dsize" "$pipeline"
done

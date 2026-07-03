#!/usr/bin/env bash
# Focused reproducer: concurrent cross-node INCR exactness (PN counters).
# Usage: incr_repro.sh <rounds>
set -uo pipefail
ROUNDS=${1:-5}
BIN=target/release/marekvs-server

fails=0
for r in $(seq 1 "$ROUNDS"); do
  pkill -9 -f local_cluster.sh 2>/dev/null; pkill -9 -f marekvs-server 2>/dev/null
  sleep 2; rm -rf .data
  (./tests/local_cluster.sh "$BIN" 3 > "/tmp/incr-r$r.log" 2>&1 &)
  sleep 2
  ./tests/wait_ready.sh 127.0.0.1 6379 6380 6381 > /dev/null || { echo "round $r: cluster failed to start"; fails=$((fails+1)); continue; }
  sleep 2  # get past the boot window; we're testing counters, not startup
  key="cnt:$r"
  loop() { for _ in $(seq 1 20); do redis-cli -p "$1" incr "$key" >/dev/null; done; }
  loop 6379 & p0=$!; loop 6380 & p1=$!; loop 6381 & p2=$!
  wait $p0 $p1 $p2
  ok=0
  for i in $(seq 1 100); do
    a=$(redis-cli -p 6379 get "$key"); b=$(redis-cli -p 6380 get "$key"); c=$(redis-cli -p 6381 get "$key")
    if [ "$a" = "60" ] && [ "$b" = "60" ] && [ "$c" = "60" ]; then ok=1; break; fi
    sleep 0.3
  done
  if [ "$ok" = 1 ]; then echo "round $r: OK (60/60/60)"; else echo "round $r: FAIL ($a/$b/$c)"; fails=$((fails+1)); fi
done
pkill -9 -f local_cluster.sh 2>/dev/null; pkill -9 -f marekvs-server 2>/dev/null; rm -rf .data
echo "failures: $fails/$ROUNDS"
exit "$fails"

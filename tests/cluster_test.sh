#!/usr/bin/env bash
# Cluster convergence test against 3 nodes.
# Usage: tests/cluster_test.sh <host> <port0> <port1> <port2>
set -euo pipefail

HOST=${1:?}; P0=${2:?}; P1=${3:?}; P2=${4:?}
R0="redis-cli -h $HOST -p $P0"
R1="redis-cli -h $HOST -p $P1"
R2="redis-cli -h $HOST -p $P2"
fail=0

# Wait until a command on a node returns the expected value (convergence poll).
converge() { # <desc> <expected> <timeout_s> <cli...>
  local desc=$1 expected=$2 timeout=$3; shift 3
  local deadline=$((SECONDS + timeout))
  while [ $SECONDS -lt $deadline ]; do
    local got
    got=$("$@" 2>/dev/null || true)
    if [ "$got" = "$expected" ]; then
      echo "ok: $desc"
      return 0
    fi
    sleep 0.3
  done
  echo "FAIL: $desc — never converged to [$expected], last [$got]"
  fail=1
}

echo "--- basic replication: write node0, read node1/node2"
$R0 set repl:k hello >/dev/null
converge "GET on node1 (fetch or push)" "hello" 10 $R1 get repl:k
converge "GET on node2" "hello" 10 $R2 get repl:k

echo "--- update propagation to interest replicas"
$R0 set repl:k world >/dev/null
converge "node1 sees update" "world" 20 $R1 get repl:k
converge "node2 sees update" "world" 20 $R2 get repl:k

echo "--- concurrent SADD on different nodes both survive (OR-set)"
$R0 sadd orset a >/dev/null
$R1 sadd orset b >/dev/null
converge "node0 sees both" "a
b" 20 bash -c "$R0 smembers orset | sort"
converge "node1 sees both" "a
b" 20 bash -c "$R1 smembers orset | sort"
converge "node2 sees both" "a
b" 20 bash -c "$R2 smembers orset | sort"

echo "--- SREM does not resurrect"
$R0 srem orset a >/dev/null
converge "node1 sees removal" "b" 20 bash -c "$R1 smembers orset | sort"
converge "node2 sees removal" "b" 20 bash -c "$R2 smembers orset | sort"

echo "--- DEL propagates"
$R0 set del:k v >/dev/null
converge "node1 has it" "v" 10 $R1 get del:k
$R1 del del:k >/dev/null
converge "node0 sees delete" "" 20 $R0 get del:k
converge "node2 sees delete" "" 20 $R2 get del:k

echo "--- hash field updates converge"
$R0 hset ch f base >/dev/null
converge "node2 fetched hash" "base" 10 $R2 hget ch f
$R2 hset ch f2 added >/dev/null
converge "node0 sees new field" "added" 20 $R0 hget ch f2

echo "--- stable increments: concurrent INCR on all nodes loses nothing (v1.1)"
incr_loop() { # <cli...> — 20 increments
  for _ in $(seq 1 20); do "$@" incr cnt:pn >/dev/null; done
}
incr_loop $R0 & p0=$!
incr_loop $R1 & p1=$!
incr_loop $R2 & p2=$!
wait $p0 $p1 $p2
converge "node0 counts 60" "60" 30 $R0 get cnt:pn
converge "node1 counts 60" "60" 30 $R1 get cnt:pn
converge "node2 counts 60" "60" 30 $R2 get cnt:pn

echo "--- HyperLogLog: disjoint PFADDs on all nodes converge to the union"
for i in $(seq 1 100); do echo "pfadd chll a$i"; done | redis-cli -h $HOST -p $P0 > /dev/null
for i in $(seq 1 100); do echo "pfadd chll b$i"; done | redis-cli -h $HOST -p $P1 > /dev/null
for i in $(seq 1 100); do echo "pfadd chll c$i"; done | redis-cli -h $HOST -p $P2 > /dev/null
hll_ok() { local v; v=$($1 pfcount chll 2>/dev/null); [ -n "$v" ] && [ "$v" -ge 291 ] && [ "$v" -le 309 ]; }
deadline=$((SECONDS + 30)); ok=0
while [ $SECONDS -lt $deadline ]; do
  if hll_ok "$R0" && hll_ok "$R1" && hll_ok "$R2"; then ok=1; break; fi
  sleep 0.5
done
if [ "$ok" = 1 ]; then
  echo "ok: HLL union converged (~300 on all nodes: $($R0 pfcount chll)/$($R1 pfcount chll)/$($R2 pfcount chll))"
else
  echo "FAIL: HLL never converged ($($R0 pfcount chll)/$($R1 pfcount chll)/$($R2 pfcount chll))"
  fail=1
fi

echo "--- BLPOP across nodes: push on node0 wakes blocked pop on node1"
(sleep 1; $R0 rpush bl:x hello >/dev/null) &
bl_out=$($R1 blpop bl:x 10)
if [ "$bl_out" = "bl:x
hello" ]; then
  echo "ok: cross-node BLPOP"
else
  echo "FAIL: cross-node BLPOP got [$bl_out]"
  fail=1
fi

echo "--- cross-node pubsub"
sub_out=$(mktemp)
(redis-cli -h "$HOST" -p "$P2" subscribe xchan &) > "$sub_out" 2>&1
sleep 1
$R0 publish xchan crosshello >/dev/null
sleep 1
if grep -q crosshello "$sub_out"; then
  echo "ok: cross-node pubsub"
else
  echo "FAIL: cross-node pubsub"
  fail=1
fi
rm -f "$sub_out"

echo "--- Lua script effects replicate (design/11 caveat 2)"
$R0 eval "redis.call('SET', KEYS[1], ARGV[1]) return 1" 1 script:effect scripted-value >/dev/null
converge "node1 sees script write" "scripted-value" 20 $R1 get script:effect
converge "node2 sees script write" "scripted-value" 20 $R2 get script:effect

echo "--- counter script cross-node exact (PN counter merge under scripts)"
RATE_SCRIPT="return redis.call('INCR', KEYS[1])"
for i in $(seq 1 20); do $R0 eval "$RATE_SCRIPT" 1 'script:{rl}:ctr' >/dev/null; done
for i in $(seq 1 20); do $R1 eval "$RATE_SCRIPT" 1 'script:{rl}:ctr' >/dev/null; done
for i in $(seq 1 20); do $R2 eval "$RATE_SCRIPT" 1 'script:{rl}:ctr' >/dev/null; done
converge "node0 counter exact 60" "60" 30 $R0 get 'script:{rl}:ctr'
converge "node1 counter exact 60" "60" 30 $R1 get 'script:{rl}:ctr'
converge "node2 counter exact 60" "60" 30 $R2 get 'script:{rl}:ctr'

echo "--- divergence trap: math.random writes ONE cluster-wide value"
$R0 eval "redis.call('SET', KEYS[1], tostring(math.random(1000000000))) return 1" 1 script:rand >/dev/null
rand0=$($R0 get script:rand)
converge "node1 sees same random" "$rand0" 20 $R1 get script:rand
converge "node2 sees same random" "$rand0" 20 $R2 get script:rand

echo "--- SCRIPT LOAD replicates via system records (design/11 caveat 5)"
csha=$($R0 script load "return 'from-node0'")
converge "EVALSHA on node2 after LOAD on node0" "from-node0" 30 $R2 evalsha "$csha" 0

echo "--- INFO reports cluster"
if $R0 info cluster | grep -q "cluster_enabled:1"; then
  echo "ok: INFO cluster section"
else
  echo "FAIL: INFO cluster section"
  fail=1
fi

if [ "$fail" = 0 ]; then
  echo "CLUSTER TEST PASSED"
else
  echo "CLUSTER TEST FAILED"
  exit 1
fi

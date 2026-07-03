#!/usr/bin/env bash
# Integration test: marekvs following a NORMAL Redis master via REPLICAOF.
#
# Starts a real redis-server as the master (or falls back to redis in docker),
# pre-populates it, points a marekvs node at it, and asserts that both the
# initial snapshot copy AND the live command stream land on marekvs. Finally
# REPLICAOF NO ONE must detach so later master writes no longer propagate.
#
# Usage: tests/replicaof.sh <path-to-marekvs-server-binary>
set -euo pipefail

BIN=${1:?usage: replicaof.sh <marekvs-server-binary>}
MASTER_PORT=16400
MAREK_PORT=16401
DIR=$(mktemp -d)
DOCKER_MASTER=""

M="redis-cli -p $MASTER_PORT"   # master (redis)
K="redis-cli -p $MAREK_PORT"    # marekvs follower

cleanup() {
  kill "${SRV:-}" 2>/dev/null || true
  if [ -n "$DOCKER_MASTER" ]; then
    docker rm -f "$DOCKER_MASTER" >/dev/null 2>&1 || true
  else
    kill "${REDIS:-}" 2>/dev/null || true
  fi
  rm -rf "$DIR"
}
trap cleanup EXIT

fail=0
expect() { # expect <description> <expected> <actual>
  if [ "$2" != "$3" ]; then
    echo "FAIL: $1 — expected [$2] got [$3]"
    fail=1
  else
    echo "ok: $1"
  fi
}

# poll_eq <description> <expected> <cmd...> — retry up to ~4s for eventual state
poll_eq() {
  local desc=$1 want=$2; shift 2
  local got=""
  for _ in $(seq 1 40); do
    got=$("$@" 2>/dev/null || true)
    [ "$got" = "$want" ] && { echo "ok: $desc"; return; }
    sleep 0.1
  done
  echo "FAIL: $desc — expected [$want] got [$got]"
  fail=1
}

wait_ready() { # wait_ready <cli-prefix...>
  for i in $(seq 1 100); do
    if "$@" ping 2>/dev/null | grep -q PONG; then return; fi
    [ "$i" = 100 ] && { echo "server never became ready: $*"; exit 1; }
    sleep 0.2
  done
}

# ── start the master (redis-server, else docker) ─────────────────────────────
if command -v redis-server >/dev/null 2>&1; then
  echo "== starting redis-server master on :$MASTER_PORT =="
  redis-server --port "$MASTER_PORT" --save '' --appendonly no \
    --repl-ping-replica-period 1 --dir "$DIR" >/dev/null 2>&1 &
  REDIS=$!
else
  echo "== redis-server not found; starting redis in docker on :$MASTER_PORT =="
  DOCKER_MASTER=repl-master
  docker rm -f "$DOCKER_MASTER" >/dev/null 2>&1 || true
  docker run -d --rm --name "$DOCKER_MASTER" -p "$MASTER_PORT":6379 \
    redis:7-alpine redis-server --repl-ping-replica-period 1 >/dev/null
fi
wait_ready $M

# ── pre-populate the master with every data type ─────────────────────────────
echo "== pre-populating master =="
$M set str:plain hello >/dev/null
$M set str:ttl withttl ex 1000 >/dev/null
$M set counter 41 >/dev/null && $M incr counter >/dev/null   # -> 42
$M hset h f1 v1 f2 v2 >/dev/null
$M sadd myset a b c >/dev/null
$M zadd myz 1 one 2 two 3 three >/dev/null
$M rpush mylist x y z >/dev/null
MASTER_XID=$($M xadd mystream '*' field val)

# ── start marekvs and point it at the master ─────────────────────────────────
echo "== starting marekvs follower on :$MAREK_PORT =="
MAREKVS_DATA_DIR="$DIR/marek" MAREKVS_RESP_ADDR="127.0.0.1:$MAREK_PORT" \
  MAREKVS_MESH_ADDR=127.0.0.1:17474 MAREKVS_GOSSIP_ADDR=127.0.0.1:17475 \
  MAREKVS_NODE_ID=0 MAREKVS_REPLICAS_N=1 RUST_LOG=warn \
  MAREKVS_REPLICAOF="127.0.0.1:$MASTER_PORT" "$BIN" &
SRV=$!
wait_ready $K

# ── phase 1: initial snapshot copy must land ─────────────────────────────────
echo "== asserting initial snapshot copy =="
poll_eq "copy string"       "hello"  $K get str:plain
poll_eq "copy string w/ttl" "withttl" $K get str:ttl
poll_eq "copy counter"      "42"     $K get counter
poll_eq "copy hash"         "v1"     $K hget h f1
poll_eq "copy set member"   "1"      $K sismember myset b
poll_eq "copy zset score"   "2"      $K zscore myz two
poll_eq "copy list"         "x y z"  bash -c "$K lrange mylist 0 -1 | tr '\n' ' ' | sed 's/ \$//'"
poll_eq "copy stream len"   "1"      $K xlen mystream
ttl=$($K ttl str:ttl)
{ [ "$ttl" -ge 900 ] && [ "$ttl" -le 1000 ]; } && echo "ok: copied TTL in range ($ttl)" \
  || { echo "FAIL: copied TTL=$ttl"; fail=1; }

# ── phase 2: live stream must follow ─────────────────────────────────────────
echo "== asserting live replication stream =="
$M set live:new freshvalue >/dev/null
poll_eq "live SET appears"   "freshvalue" $K get live:new

$M del str:plain >/dev/null
poll_eq "live DEL removes"   "0"          $K exists str:plain

$M incr counter >/dev/null   # 42 -> 43 on master
poll_eq "live INCR follows"  "43"         $K get counter

$M expire counter 500 >/dev/null
livettl=""
for _ in $(seq 1 40); do
  livettl=$($K ttl counter 2>/dev/null || true)
  { [ "$livettl" -ge 1 ] 2>/dev/null && [ "$livettl" -le 500 ]; } && break
  sleep 0.1
done
{ [ "$livettl" -ge 1 ] 2>/dev/null && [ "$livettl" -le 500 ]; } \
  && echo "ok: live EXPIRE visible ($livettl)" || { echo "FAIL: live TTL=$livettl"; fail=1; }

$M hset h f3 v3 >/dev/null
poll_eq "live HSET appears"  "v3"         $K hget h f3

# INFO replication should reflect the master link.
info=$($K info replication)
echo "$info" | grep -q "role:slave"                && echo "ok: INFO role:slave" || { echo "FAIL: INFO role"; fail=1; }
echo "$info" | grep -q "master_host:127.0.0.1"     && echo "ok: INFO master_host" || { echo "FAIL: INFO master_host"; fail=1; }
echo "$info" | grep -q "master_link_status:connected" && echo "ok: INFO link connected" || { echo "FAIL: INFO link_status [$(echo "$info" | grep master_link_status)]"; fail=1; }
echo "$info" | grep -q "slave_read_only:0"         && echo "ok: INFO slave_read_only:0" || { echo "FAIL: INFO read_only"; fail=1; }

# ── phase 3: REPLICAOF NO ONE detaches ───────────────────────────────────────
echo "== asserting REPLICAOF NO ONE detaches =="
expect "REPLICAOF NO ONE" "OK" "$($K replicaof no one)"
sleep 0.5
$M set after:detach shouldnotappear >/dev/null
sleep 1.5
expect "post-detach write absent" "" "$($K get after:detach)"
# data copied before detach is retained (AP: keep data)
expect "retained after detach" "freshvalue" "$($K get live:new)"
info2=$($K info replication)
echo "$info2" | grep -q "role:master" && echo "ok: INFO role:master after detach" || { echo "FAIL: INFO role after detach"; fail=1; }

if [ "$fail" = 0 ]; then
  echo "REPLICAOF TEST PASSED"
else
  echo "REPLICAOF TEST FAILED"
  exit 1
fi

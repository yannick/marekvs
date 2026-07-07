#!/usr/bin/env bash
# Single-node end-to-end smoke test via redis-cli.
# Usage: tests/smoke.sh <path-to-marekvs-server-binary> [port]
set -euo pipefail

BIN=${1:?usage: smoke.sh <binary> [port]}
PORT=${2:-16379}
DIR=$(mktemp -d)
R="redis-cli -p $PORT"

"$(dirname "$0")/preflight.sh" "$PORT" 17373 19122

cleanup() { kill "$SRV" 2>/dev/null || true; rm -rf "$DIR"; }
trap cleanup EXIT

MAREKVS_DATA_DIR="$DIR" MAREKVS_RESP_ADDR="127.0.0.1:$PORT" \
  MAREKVS_MESH_ADDR=127.0.0.1:17373 MAREKVS_GOSSIP_ADDR=127.0.0.1:17946 \
  MAREKVS_METRICS_ADDR=127.0.0.1:19122 \
  MAREKVS_NODE_ID=0 MAREKVS_REPLICAS_N=1 RUST_LOG=warn "$BIN" &
SRV=$!

for i in $(seq 1 100); do
  if $R ping 2>/dev/null | grep -q PONG; then break; fi
  [ "$i" = 100 ] && { echo "server never became ready"; exit 1; }
  sleep 0.2
done

fail=0
expect() { # expect <description> <expected> <actual>
  if [ "$2" != "$3" ]; then
    echo "FAIL: $1 — expected [$2] got [$3]"
    fail=1
  else
    echo "ok: $1"
  fi
}

# strings
expect "SET" "OK" "$($R set k1 v1)"
expect "GET" "v1" "$($R get k1)"
expect "APPEND" "4" "$($R append k1 "23")"
expect "GET after append" "v123" "$($R get k1)"
expect "INCR" "1" "$($R incr ctr)"
expect "INCRBY" "11" "$($R incrby ctr 10)"
expect "SETNX taken" "0" "$($R setnx k1 other)"
expect "MSET" "OK" "$($R mset a 1 b 2 c 3)"
expect "MGET" "1
2
3" "$($R mget a b c)"
expect "STRLEN" "4" "$($R strlen k1)"
expect "GETRANGE" "12" "$($R getrange k1 1 2)"

# generic
expect "EXISTS" "1" "$($R exists k1)"
expect "TYPE" "string" "$($R type k1)"
expect "DEL" "1" "$($R del k1)"
expect "EXISTS after DEL" "0" "$($R exists k1)"
expect "TTL missing" "-2" "$($R ttl nosuch)"
$R set exp v >/dev/null
expect "EXPIRE" "1" "$($R expire exp 100)"
ttl=$($R ttl exp)
[ "$ttl" -ge 98 ] && [ "$ttl" -le 100 ] && echo "ok: TTL in range" || { echo "FAIL: TTL=$ttl"; fail=1; }
expect "PERSIST" "1" "$($R persist exp)"
expect "TTL after persist" "-1" "$($R ttl exp)"

# expiry actually deletes
$R set gone v px 200 >/dev/null
sleep 0.5
expect "expired GET" "" "$($R get gone)"
expect "expired EXISTS" "0" "$($R exists gone)"

# hashes
expect "HSET" "2" "$($R hset h f1 v1 f2 v2)"
expect "HGET" "v1" "$($R hget h f1)"
expect "HLEN" "2" "$($R hlen h)"
expect "HDEL" "1" "$($R hdel h f1)"
expect "HGET deleted" "" "$($R hget h f1)"
expect "HINCRBY" "5" "$($R hincrby h n 5)"
expect "TYPE hash" "hash" "$($R type h)"

# sets
expect "SADD" "3" "$($R sadd s a b c)"
expect "SCARD" "3" "$($R scard s)"
expect "SISMEMBER" "1" "$($R sismember s a)"
expect "SREM" "1" "$($R srem s a)"
expect "SISMEMBER gone" "0" "$($R sismember s a)"
expect "SMEMBERS" "b
c" "$($R smembers s | sort)"

# zsets
expect "ZADD" "2" "$($R zadd z 1 one 2 two)"
expect "ZSCORE" "2" "$($R zscore z two)"
expect "ZCARD" "2" "$($R zcard z)"
expect "ZRANGE" "one
two" "$($R zrange z 0 -1)"
expect "ZRANGEBYSCORE" "two" "$($R zrangebyscore z 2 3)"
expect "ZREM" "1" "$($R zrem z one)"

# lists
expect "RPUSH" "3" "$($R rpush l a b c)"
expect "LLEN" "3" "$($R llen l)"
expect "LRANGE" "a
b
c" "$($R lrange l 0 -1)"
expect "LPOP" "a" "$($R lpop l)"
expect "RPOP" "c" "$($R rpop l)"

# streams
xid=$($R xadd st '*' field val)
[ -n "$xid" ] && echo "ok: XADD id=$xid" || { echo "FAIL: XADD"; fail=1; }
expect "XLEN" "1" "$($R xlen st)"

# pubsub (subscribe in background, publish, check delivery)
sub_out=$(mktemp)
(redis-cli -p "$PORT" subscribe chan &) > "$sub_out" 2>&1
sleep 0.5
$R publish chan hello >/dev/null
sleep 0.5
grep -q hello "$sub_out" && echo "ok: pubsub delivery" || { echo "FAIL: pubsub"; fail=1; }
rm -f "$sub_out"

# counters (v1.1: PN counters materialize as strings)
$R set pncnt 10 >/dev/null
expect "INCR on SET base" "11" "$($R incr pncnt)"
expect "DECRBY" "6" "$($R decrby pncnt 5)"
expect "GET materializes counter" "6" "$($R get pncnt)"
expect "STRLEN on counter" "1" "$($R strlen pncnt)"
expect "TYPE counter is string" "string" "$($R type pncnt)"
expect "EXPIRE preserves counter" "1" "$($R expire pncnt 100)"
expect "INCR after EXPIRE" "7" "$($R incr pncnt)"
ttl2=$($R ttl pncnt)
[ "$ttl2" -ge 98 ] && [ "$ttl2" -le 100 ] && echo "ok: counter TTL preserved" || { echo "FAIL: counter TTL=$ttl2"; fail=1; }
expect "SET resets counter" "OK" "$($R set pncnt 100)"
expect "INCR after reset" "101" "$($R incr pncnt)"

# MULTI/EXEC (v1.1) — pipe form (redis-cli feeds stdin sequentially)
txout=$(printf 'MULTI\nSET tx:k2 v2\nGET tx:k2\nEXEC\n' | redis-cli -p "$PORT")
echo "$txout" | grep -q "v2" && echo "ok: MULTI/EXEC pipeline" || { echo "FAIL: MULTI/EXEC [$txout]"; fail=1; }
dis=$(printf 'MULTI\nSET d:k v\nDISCARD\nGET d:k\n' | redis-cli -p "$PORT" | tail -1)
[ -z "$dis" ] && echo "ok: DISCARD drops queue" || { echo "FAIL: DISCARD [$dis]"; fail=1; }

# blocking ops (v1.1): BLPOP with data ready, and timeout path
$R rpush bl:q first >/dev/null
expect "BLPOP immediate" "bl:q
first" "$($R blpop bl:q 1)"
t0=$SECONDS
$R blpop bl:empty 1 >/dev/null
[ $((SECONDS - t0)) -ge 1 ] && echo "ok: BLPOP timeout waits" || { echo "FAIL: BLPOP returned early"; fail=1; }

# EXPIREMEMBER (KeyDB extension): per-member TTL on hash/set/zset
$R hset em:h keep v1 drop v2 > /dev/null
expect "EXPIREMEMBER hash field" "1" "$($R expiremember em:h drop 1)"
expect "EXPIREMEMBER missing member" "0" "$($R expiremember em:h nosuch 10)"
emttl=$($R ttl em:h drop)
[ "$emttl" -ge 0 ] && [ "$emttl" -le 1 ] && echo "ok: member TTL query" || { echo "FAIL: member TTL=$emttl"; fail=1; }
expect "member TTL none" "-1" "$($R ttl em:h keep)"
$R sadd em:s stay bye > /dev/null
expect "EXPIREMEMBER set member (ms)" "1" "$($R expiremember em:s bye 300 ms)"
$R zadd em:z 1 zstay 2 zbye > /dev/null
expect "PEXPIREMEMBERAT zset member" "1" "$($R pexpirememberat em:z zbye $(( $(date +%s) * 1000 + 300 )))"
sleep 1.6
expect "expired field gone" "" "$($R hget em:h drop)"
expect "surviving field intact" "v1" "$($R hget em:h keep)"
expect "expired set member gone" "0" "$($R sismember em:s bye)"
expect "surviving set member" "1" "$($R sismember em:s stay)"
expect "expired zset member gone" "" "$($R zscore em:z zbye)"
expect "surviving zset member" "1" "$($R zscore em:z zstay)"

# HyperLogLog (per-register records, design/02)
for i in $(seq 1 500); do echo "pfadd hl e$i"; done | redis-cli -p "$PORT" > /dev/null
pfc=$($R pfcount hl)
[ "$pfc" -ge 485 ] && [ "$pfc" -le 515 ] && echo "ok: PFCOUNT ~500 ($pfc)" || { echo "FAIL: PFCOUNT=$pfc"; fail=1; }
expect "PFADD dup is no-op" "0" "$($R pfadd hl e1)"
expect "PFADD new element" "1" "$($R pfadd hl brand-new)"
$R pfadd hl2 x y z > /dev/null
expect "PFMERGE" "OK" "$($R pfmerge hldst hl hl2)"
pfm=$($R pfcount hldst)
[ "$pfm" -ge 488 ] && [ "$pfm" -le 520 ] && echo "ok: PFMERGE union ($pfm)" || { echo "FAIL: PFMERGE=$pfm"; fail=1; }
pfu=$($R pfcount hl hl2)
[ "$pfu" -ge 488 ] && [ "$pfu" -le 520 ] && echo "ok: PFCOUNT multi-key ($pfu)" || { echo "FAIL: multi PFCOUNT=$pfu"; fail=1; }
expect "DEL hll" "1" "$($R del hl2)"
expect "PFCOUNT after DEL" "0" "$($R pfcount hl2)"

# probes + metrics (design/07)
expect "/ready" "ready" "$(curl -s http://127.0.0.1:19122/ready)"
expect "/alive" "alive" "$(curl -s http://127.0.0.1:19122/alive)"
curl -s http://127.0.0.1:19122/metrics | grep -q 'marekvs_commands_total{cmd="set"}' \
  && echo "ok: per-command metrics" || { echo "FAIL: metrics missing command counters"; fail=1; }
curl -s http://127.0.0.1:19122/metrics | grep -q 'marekvs_net_input_bytes_total' \
  && echo "ok: net throughput metrics" || { echo "FAIL: net metrics"; fail=1; }

# Lua scripting (design/11): atomic same-pid path
expect "EVAL literal" "42" "$($R eval 'return 42' 0)"
expect "EVAL string" "hi" "$($R eval "return 'hi'" 0)"
expect "EVAL KEYS/ARGV" "k1=a1" "$($R eval "return KEYS[1]..'='..ARGV[1]" 1 k1 a1)"
expect "EVAL redis.call SET" "OK" "$($R eval "return redis.call('SET', KEYS[1], ARGV[1])" 1 lua:k v1)"
expect "EVAL redis.call GET" "v1" "$($R eval "return redis.call('GET', KEYS[1])" 1 lua:k)"
expect "EVAL false is nil" "" "$($R eval 'return false' 0)"
expect "EVAL number truncates" "3" "$($R eval 'return 3.9' 0)"
expect "EVAL table" "$(printf 'a\nb')" "$($R eval "return {'a','b'}" 0)"
expect "EVAL status_reply" "GOOD" "$($R eval "return redis.status_reply('GOOD')" 0)"
expect "EVAL sha1hex" "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d" "$($R eval "return redis.sha1hex('hello')" 0)"
expect "EVAL cjson roundtrip" "7" "$($R eval "return cjson.decode(cjson.encode({n=7})).n" 0)"
expect "EVAL bit shim" "12" "$($R eval "return bit.band(12, 13)" 0)"
expect "EVAL sandbox: os is nil" "1" "$($R eval "if os == nil and io == nil then return 1 end return 0" 0)"
# atomic rate limiter — the flagship single-key script
expect "EVAL rate limiter 1st" "1" "$($R eval "local c = redis.call('INCR', KEYS[1]) if c == 1 then redis.call('EXPIRE', KEYS[1], 60) end if c > 3 then return 0 end return 1" 1 'rl:{u1}')"
$R eval "return redis.call('INCR', KEYS[1])" 1 'rl:{u1}' > /dev/null
$R eval "return redis.call('INCR', KEYS[1])" 1 'rl:{u1}' > /dev/null
expect "EVAL rate limiter over" "0" "$($R eval "local c = redis.call('INCR', KEYS[1]) if c > 3 then return 0 end return 1" 1 'rl:{u1}')"
# hash-tag co-located multi-key script runs atomically on one shard
expect "EVAL multi-key same tag" "ab" "$($R eval "redis.call('SET', KEYS[1], 'a') redis.call('SET', KEYS[2], 'b') return redis.call('GET', KEYS[1])..redis.call('GET', KEYS[2])" 2 '{tag}:x' '{tag}:y')"
expect "EVAL cross-slot rejected" "yes" "$($R eval "return 1" 2 aaa zzz 2>&1 | grep -qi 'CROSSSLOT\|same partition' && echo yes)"
expect "EVAL undeclared key rejected" "yes" "$($R eval "return redis.call('GET', 'sneaky')" 0 2>&1 | grep -qi 'declared' && echo yes)"
expect "EVAL budget abort" "yes" "$($R eval 'while true do end' 0 2>&1 | grep -qiE 'time limit|budget|abort' && echo yes)"
# atomicity under fire (design/11 test 1): two INCRs inside one script must
# never be split by the concurrent INCR bombardment
( for i in $(seq 1 300); do $R incr fire:ctr > /dev/null; done ) &
BOMBER=$!
torn=0
for i in $(seq 1 100); do
  out=$($R eval "local a = redis.call('INCR', KEYS[1]) local b = redis.call('INCR', KEYS[1]) if b - a ~= 1 then return 'TORN' end return 'OK'" 1 fire:ctr)
  [ "$out" = "TORN" ] && torn=1
done
wait $BOMBER
expect "script atomicity under fire" "0" "$torn"

# SCRIPT LOAD / EXISTS / EVALSHA / FLUSH
sha=$($R script load "return 'cached'")
expect "SCRIPT LOAD sha len" "40" "${#sha}"
expect "EVALSHA hit" "cached" "$($R evalsha "$sha" 0)"
expect "SCRIPT EXISTS" "1" "$($R script exists "$sha")"
expect "SCRIPT EXISTS miss" "0" "$($R script exists 0000000000000000000000000000000000000000)"
expect "EVALSHA miss is NOSCRIPT" "yes" "$($R evalsha 0000000000000000000000000000000000000000 0 2>&1 | grep -qi noscript && echo yes)"

# JSON documents (JSON.*, design/16)
expect "JSON.SET root" "OK" "$($R json.set jd '$' '{"a":1,"tags":["x","y"],"nest":{"n":2}}')"
expect "JSON.GET root" '{"a":1,"nest":{"n":2},"tags":["x","y"]}' "$($R json.get jd .)"
expect "JSON.TYPE" "object" "$($R json.type jd)"
expect "JSON.TYPE path" "integer" "$($R json.type jd .a)"
expect "TYPE reports module name" "ReJSON-RL" "$($R type jd)"
expect "JSON.SET path" "OK" "$($R json.set jd '$.a' 5)"
expect "JSON.NUMINCRBY" "8" "$($R json.numincrby jd .a 3)"
expect "JSON.ARRAPPEND" "3" "$($R json.arrappend jd .tags '"z"')"
expect "JSON.ARRLEN" "3" "$($R json.arrlen jd .tags)"
expect "JSON.ARRPOP" '"z"' "$($R json.arrpop jd .tags)"
expect "JSON.STRAPPEND" "3" "$($R json.strappend jd '$.tags[0]' '"yz"' | tr -d '\n')"
expect "JSON.OBJKEYS" "a
nest
tags" "$($R json.objkeys jd)"
expect "JSON.DEL path" "1" "$($R json.del jd '$.nest')"
expect "JSON.GET after del" '{"a":8,"tags":["xyz","y"]}' "$($R json.get jd .)"
expect "JSON EXPIRE" "1" "$($R expire jd 100)"
expect "JSON TTL set" "yes" "$([ "$($R ttl jd)" -gt 0 ] && echo yes)"
expect "JSON.DEL root" "1" "$($R json.del jd)"
expect "JSON.GET missing" "" "$($R json.get jd .)"

# Protobuf registry + typed values (PROTO.*, design/17)
PROTO_SRC='syntax = "proto3"; package acme; message User { string name = 1; int32 age = 2; }'
expect "PROTO.SCHEMA SET" "1" "$($R proto.schema set acme/user.proto SOURCE "$PROTO_SRC")"
expect "PROTO.SCHEMA LIST" "acme/user.proto
1" "$($R proto.schema list)"
expect "PROTO.SCHEMA TYPES" "acme.User" "$($R proto.schema types acme/user.proto)"
expect "PROTO.BIND" "OK" "$($R proto.bind user: acme.User)"
expect "PROTO.SETJSON" "OK" "$($R proto.setjson user:1 '{"name":"ada","age":36}')"
expect "PROTO.GETJSON" '{"name":"ada","age":36}' "$($R proto.getjson user:1)"
expect "PROTO.GETFIELD" "ada" "$($R proto.getfield user:1 name)"
expect "PROTO.SETFIELD" "OK" "$($R proto.setfield user:1 age 37)"
expect "PROTO.GETFIELD after set" "37" "$($R proto.getfield user:1 age)"
expect "PROTO TYPE" "proto" "$($R type user:1)"
expect "PROTO OBJECT ENCODING" "acme.User" "$($R object encoding user:1)"
expect "PROTO.INFO has type" "yes" "$($R proto.info user:1 | grep -q acme.User && echo yes)"
expect "PROTO.SET rejects garbage" "yes" "$($R proto.set user:2 not-a-proto 2>&1 | grep -qi 'PROTOVALIDATE\|error' && echo yes)"
expect "PROTO no binding" "yes" "$($R proto.setjson other:1 '{}' 2>&1 | grep -qi nobinding && echo yes)"
expect "PROTO.DEL" "1" "$($R del user:1)"

# keyspace ops
expect "DBSIZE > 0" "yes" "$([ "$($R dbsize)" -gt 0 ] && echo yes)"
expect "SCAN returns keys" "yes" "$([ -n "$($R scan 0 count 100 | tail +2)" ] && echo yes)"
expect "KEYS hides system keys" "0" "$($R keys '*' | { grep -c 'script:' || true; })"

if [ "$fail" = 0 ]; then
  echo "SMOKE TEST PASSED"
else
  echo "SMOKE TEST FAILED"
  exit 1
fi

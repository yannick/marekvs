#!/usr/bin/env bash
# Wait for PING on each port. Usage: wait_ready.sh <host> <port...>
set -euo pipefail
HOST=${1:?}; shift
for port in "$@"; do
  for i in $(seq 1 150); do
    if redis-cli -h "$HOST" -p "$port" ping 2>/dev/null | grep -q PONG; then
      echo "node on :$port ready"
      break
    fi
    [ "$i" = 150 ] && { echo "node on :$port never became ready"; exit 1; }
    sleep 0.4
  done
done

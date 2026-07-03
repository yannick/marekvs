#!/usr/bin/env bash
# Machine sanity before load-sensitive tests.
#
# Convergence tests assert "value X appears within N seconds" — on a machine
# drowning in stray CPU hogs those timeouts fail with lost-update symptoms
# that look exactly like replication bugs (see the stray-`yes` incidents:
# 24 runaway `yes` processes pushed load to ~300 and made exact-counter
# tests read 59/60). This script makes that class of false alarm impossible:
#
#   1. kills stray `yes` processes (never legitimate as background load)
#   2. refuses to run when load average >> core count (PREFLIGHT_FORCE=1
#      overrides), naming the top CPU consumers
#   3. fails fast with a diagnosis when a port the test needs is taken by a
#      leftover process, instead of letting the test inherit its state
#
# Usage: preflight.sh [port ...]
set -euo pipefail

# ── 1. stray `yes` processes ─────────────────────────────────────────────
strays=$(pgrep -x yes || true)
if [ -n "$strays" ]; then
  echo "preflight: killing stray 'yes' process(es): $(echo "$strays" | tr '\n' ' ')" >&2
  pkill -x yes || true
  sleep 1
fi

# ── 2. load sanity ───────────────────────────────────────────────────────
if [ "$(uname)" = "Darwin" ]; then
  cores=$(sysctl -n hw.ncpu)
  load=$(sysctl -n vm.loadavg | awk '{print $2}')
else
  cores=$(nproc)
  load=$(awk '{print $1}' /proc/loadavg)
fi
limit=$((cores * 2))
if awk -v l="$load" -v lim="$limit" 'BEGIN { exit !(l > lim) }'; then
  echo "preflight: load average $load exceeds 2x core count ($cores cores)." >&2
  echo "preflight: timing-sensitive tests would produce noise. Top consumers:" >&2
  ps aux | sort -rk3 | head -5 | awk '{printf "  pid %-8s %5s%%  %s\n", $2, $3, $11}' >&2
  if [ "${PREFLIGHT_FORCE:-0}" != "1" ]; then
    echo "preflight: aborting (set PREFLIGHT_FORCE=1 to run anyway)" >&2
    exit 1
  fi
  echo "preflight: PREFLIGHT_FORCE=1 set — continuing anyway" >&2
fi

# ── 3. required ports must be free ───────────────────────────────────────
# Connect test via /dev/tcp (no lsof permissions needed); lsof only for the
# best-effort "who holds it" diagnosis.
for port in "$@"; do
  if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
    exec 3>&- 3<&- 2>/dev/null || true
    echo "preflight: port $port is already taken — a previous run leaked state:" >&2
    lsof -nP -iTCP:"$port" -sTCP:LISTEN 2>/dev/null | tail -n +2 | sed 's/^/  /' >&2 || true
    echo "preflight: refusing to run against leftover state. Clean up first" >&2
    echo "  (docker: 'just docker-down' · local: kill the listener on :$port)" >&2
    exit 1
  fi
done

echo "preflight: ok (load $load, $cores cores)"

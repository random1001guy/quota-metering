#!/usr/bin/env bash
# Stop everything setup.sh started and delete bench/data (benchmark data only).
set -uo pipefail
cd "$(dirname "$0")"
PGBIN="$(brew --prefix postgresql@17)/bin"
REDIS_BIN="$(brew --prefix redis)/bin"
"$PGBIN/pg_ctl" -D data/pg-standby -m fast stop 2>/dev/null
"$PGBIN/pg_ctl" -D data/pg-primary -m fast stop 2>/dev/null
"$REDIS_BIN/redis-cli" -p 56380 shutdown nosave 2>/dev/null
"$REDIS_BIN/redis-cli" -p 56379 shutdown nosave 2>/dev/null
pkill -f "toxiproxy-server -host 127.0.0.1 -port 8474" 2>/dev/null
rm -rf data
echo "torn down"

#!/usr/bin/env bash
# Run N independent single-process instances of the quota service (ports 8101..8100+N), the way
# the brief's 8 instances would sit behind a load balancer. The load generator spreads requests
# evenly across them (uvicorn --workers on macOS spreads CONNECTIONS unevenly: one worker took
# 45% of them in testing). Uses the native Postgres (:55432, db quota_svc) and Redis (:56379).
cd "$(dirname "$0")/../.."
export DATABASE_URL=${DATABASE_URL:-postgresql://postgres@127.0.0.1:55432/quota_svc}
export REDIS_URL=${REDIS_URL:-redis://127.0.0.1:56379/0}
N=${INSTANCES:-8}
: > bench/service/uvicorn.pid
trap 'kill $(cat bench/service/uvicorn.pid 2>/dev/null) 2>/dev/null; rm -f bench/service/uvicorn.pid' EXIT INT TERM
for i in $(seq 1 "$N"); do
  .venv/bin/uvicorn quota.app:app --host 127.0.0.1 --port $((8100 + i)) \
    --no-access-log --log-level warning &
  echo $! >> bench/service/uvicorn.pid
done
echo "$N instances on ports 8101..$((8100 + N)); Ctrl-C stops them"
wait

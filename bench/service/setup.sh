#!/usr/bin/env bash
# Database for the service benchmark, on the Postgres that bench/stores/setup.sh started (:55432).
#   - db quota_svc with the repo schema and the usual 5,000 seeded orgs
#   - limits x1000, so uniform runs never hit 429
#   - org 9001 on plan 'hot' (feature1 limit 10^12) for --mode hot
set -euo pipefail
cd "$(dirname "$0")/../.."
PGBIN="$(brew --prefix postgresql@17)/bin"
PSQL="$PGBIN/psql -h 127.0.0.1 -p 55432 -U postgres -qtA"
$PSQL -d postgres -c "SELECT 1 FROM pg_database WHERE datname='quota_svc'" | grep -q 1 \
  || "$PGBIN/createdb" -h 127.0.0.1 -p 55432 -U postgres quota_svc
DATABASE_URL=postgresql://postgres@127.0.0.1:55432/quota_svc .venv/bin/python -m quota.seed --force
$PSQL -d quota_svc <<'SQL'
UPDATE plan_limits SET monthly_limit = monthly_limit * 1000;
INSERT INTO plans VALUES ('hot');
INSERT INTO plan_limits VALUES ('hot', 1, 1000000000000);
INSERT INTO orgs (org_id, name, plan_id, anchor_day) VALUES (9001, 'hot-org', 'hot', 1);
SQL
redis-cli -p 56379 flushall >/dev/null
echo "quota_svc ready"

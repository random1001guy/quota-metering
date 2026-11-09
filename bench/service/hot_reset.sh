#!/usr/bin/env bash
# Forget the hot org's lease and period row, so the next run starts from an empty lease with
# whatever LEASE_FRACTION the instances were started with. Prints Postgres's granted total first.
PSQL="$(brew --prefix postgresql@17)/bin/psql -h 127.0.0.1 -p 55432 -U postgres -d quota_svc -qtA"
ORG=${1:-9001}
echo "granted before reset: $($PSQL -c "select coalesce(sum(granted),0) from quota_periods where org_id=$ORG")"
redis-cli -p 56379 --scan --pattern "*{$ORG}*" | xargs -n 500 redis-cli -p 56379 unlink >/dev/null
$PSQL -c "delete from quota_periods where org_id=$ORG"

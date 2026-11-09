#!/usr/bin/env bash
# Bring up the local benchmark stack (native, no Docker):
#   Postgres 17 primary :55432, sync-capable standby :55433 (replicating THROUGH toxiproxy :55443)
#   Redis primary :56379, replica :56380 (replicating THROUGH toxiproxy :56390)
#   toxiproxy API :8474  (inject latency on the replication links only)
# Idempotent-ish: run teardown.sh first for a clean slate.
set -euo pipefail
export LC_ALL=en_US.UTF-8 LANG=en_US.UTF-8   # postgres on macOS refuses to start without a valid locale
cd "$(dirname "$0")"
BENCH=$(pwd)
DATA="$BENCH/data"
PGBIN="$(brew --prefix postgresql@17)/bin"
REDIS_BIN="$(brew --prefix redis)/bin"
TOXI_BIN="$(brew --prefix toxiproxy)/bin"
mkdir -p "$DATA" results

# ---------- python venv ----------
if [ ! -x .venv/bin/python ]; then
  python3 -m venv .venv
  .venv/bin/pip install -q --upgrade pip
  .venv/bin/pip install -q "psycopg[binary,pool]" redis numpy matplotlib psutil
fi

# ---------- toxiproxy ----------
if ! curl -sf localhost:8474/version >/dev/null; then
  nohup "$TOXI_BIN/toxiproxy-server" -host 127.0.0.1 -port 8474 >"$DATA/toxiproxy.log" 2>&1 &
  for _ in $(seq 50); do curl -sf localhost:8474/version >/dev/null && break; sleep 0.1; done
fi
mkproxy() { # name listen upstream
  curl -sf -X DELETE "localhost:8474/proxies/$1" >/dev/null || true
  curl -sf -X POST localhost:8474/proxies \
    -d "{\"name\":\"$1\",\"listen\":\"$2\",\"upstream\":\"$3\",\"enabled\":true}" >/dev/null
}
mkproxy pg_repl    127.0.0.1:55443 127.0.0.1:55432
mkproxy redis_repl 127.0.0.1:56390 127.0.0.1:56379

# ---------- postgres primary ----------
if [ ! -d "$DATA/pg-primary" ]; then
  "$PGBIN/initdb" -D "$DATA/pg-primary" -U postgres --auth=trust -E UTF8 >/dev/null
  cat >>"$DATA/pg-primary/postgresql.conf" <<EOF
port = 55432
listen_addresses = '127.0.0.1'
max_connections = 400
shared_buffers = 2GB
wal_level = replica
max_wal_senders = 5
wal_buffers = 64MB
max_wal_size = 16GB          # keep checkpoints out of 30-60 s runs (stated in results)
checkpoint_timeout = 30min
synchronous_commit = on
synchronous_standby_names = ''
wal_sync_method = fsync
autovacuum_naptime = 10s
EOF
  echo "host replication all 127.0.0.1/32 trust" >>"$DATA/pg-primary/pg_hba.conf"
fi
"$PGBIN/pg_ctl" -D "$DATA/pg-primary" -l "$DATA/pg-primary.log" -w status >/dev/null 2>&1 \
  || "$PGBIN/pg_ctl" -D "$DATA/pg-primary" -l "$DATA/pg-primary.log" -w start >/dev/null

"$PGBIN/psql" -h 127.0.0.1 -p 55432 -U postgres -qtAc "SELECT 1 FROM pg_database WHERE datname='quota'" | grep -q 1 \
  || "$PGBIN/createdb" -h 127.0.0.1 -p 55432 -U postgres quota
"$PGBIN/psql" -h 127.0.0.1 -p 55432 -U postgres -d quota -q -v ON_ERROR_STOP=1 -f schema.sql

# ---------- postgres standby (base backup through the proxy) ----------
if [ ! -d "$DATA/pg-standby" ]; then
  "$PGBIN/pg_basebackup" -h 127.0.0.1 -p 55443 -U postgres -D "$DATA/pg-standby" -R -X stream \
    -d "application_name=standby1" >/dev/null
  sed -i '' 's/^port = 55432/port = 55433/' "$DATA/pg-standby/postgresql.conf"
fi
"$PGBIN/pg_ctl" -D "$DATA/pg-standby" -l "$DATA/pg-standby.log" -w status >/dev/null 2>&1 \
  || "$PGBIN/pg_ctl" -D "$DATA/pg-standby" -l "$DATA/pg-standby.log" -w start >/dev/null

# ---------- redis ----------
mkdir -p "$DATA/redis-primary" "$DATA/redis-replica"
"$REDIS_BIN/redis-cli" -p 56379 ping >/dev/null 2>&1 || "$REDIS_BIN/redis-server" --port 56379 \
  --bind 127.0.0.1 --daemonize yes --dir "$DATA/redis-primary" --save "" --appendonly no \
  --logfile "$DATA/redis-primary.log" --maxclients 2000
"$REDIS_BIN/redis-cli" -p 56380 ping >/dev/null 2>&1 || "$REDIS_BIN/redis-server" --port 56380 \
  --bind 127.0.0.1 --daemonize yes --dir "$DATA/redis-replica" --save "" --appendonly no \
  --replicaof 127.0.0.1 56390 --logfile "$DATA/redis-replica.log"

# ---------- seed ----------
.venv/bin/python seed.py

echo "stack up:"
"$PGBIN/psql" -h 127.0.0.1 -p 55432 -U postgres -qtAc \
  "SELECT 'pg standby: '||application_name||' state='||state||' sync_state='||sync_state FROM pg_stat_replication"
"$REDIS_BIN/redis-cli" -p 56379 info replication | grep -E 'connected_slaves|slave0'

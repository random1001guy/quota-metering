# Quota metering

Per-organization, per-feature monthly quotas, enforced across a horizontally scaled fleet.
Quota is held in Redis as leases granted from a Postgres ledger. See [DESIGN.md](DESIGN.md).

## Run

```bash
docker compose up --build
```

One command brings up the whole system and puts it under load:

| Service | What it is |
|---|---|
| `postgres`, `redis` | The stores: Postgres holds plans, orgs and per-period granted totals; Redis holds the live leases |
| `seed` | One-off: creates the schema, then 5,000 placeholder orgs (`org1`..`org5000`), features `feature1`..`feature30`, plans starter/pro/enterprise |
| `quota` | The quota service, **3 instances** (host ports 8000-8002). Interactive API docs: http://localhost:8000/docs |
| `dummy-tracking-feature` | A demo product service that consumes quota, **2 instances** (ports 9000-9001): `POST /v1/containers` costs 1 unit (`feature1`) per container, `GET /v1/schedules` costs 1 unit (`feature2`). It reserves, does the work, then commits, or releases if the work failed |
| `load-tester-client` | Drives dummy-tracking-feature continuously (~200 ops/s over 100 busy orgs, bursty per org, batches of 1-100, 5% downstream failures, 2% duplicate sends) and prints a line every 5 s: throughput, outcomes, and the time spent waiting on the quota service (p50/p99). Every 30 s it checks a sample of orgs against their limits. Tune with `LOAD_RATE` / `LOAD_ORGS` |

```text
[load-tester-client] 10:26:56  201 ops/s | ok 942 | 429 0 | failed+released 62 | dup 21 | 503 0 | err 0 | quota overhead p50 1.9 p99 5.7 ms | end-to-end p99 16.4 ms
[load-tester-client] 10:27:06 limit check: 50/50 (org, feature) pairs within limit
```

With the stack up, run the end-to-end checks (in another terminal):

```bash
docker compose run --rm verify
```

It creates fresh orgs with the brief's example limits (500 container-tracking units, 1,000
schedule searches) and, through dummy-tracking-feature: fills a quota exactly with concurrent batches,
oversubscribes it 2x with 1,000 concurrent requests and with 300 concurrent mixed-size batches,
checks all-or-nothing batches, release on downstream failure, racing retries, and a 15 s mixed
load across 20 orgs. Every outcome is reconciled three ways (what the client saw = dummy-tracking-feature's
ledger = the quota usage endpoint, and used <= Postgres granted <= limit). Prints PASS/FAIL per
check; exit code 1 on any failure.

Quick manual calls:

```bash
curl -s -XPOST localhost:9000/v1/containers -H 'content-type: application/json' \
  -H 'Idempotency-Key: demo-1' -d '{"org_id": 42, "containers": ["MSCU1234567", "MAEU7654321"]}'
curl -s localhost:8000/v1/usage/42/feature1
```

## API

| Call | Meaning | Responses |
|---|---|---|
| `POST /v1/reserve` `{org_id, feature, units, request_id}` | Hold `units`, all or nothing | 200 reserved (or `duplicate: true` on a retry), 429 not enough quota, 404 unknown org/feature, 503 retry |
| `POST /v1/commit` `{org_id, request_id}` | Work succeeded: held units become used | 200 (idempotent), 404 unknown, 409 released or expired |
| `POST /v1/release` `{org_id, request_id}` | Work failed: held units go back | 200 (idempotent), 404 unknown, 409 already committed |
| `GET /v1/usage/{org_id}/{feature}` | Used this period, and when it resets | 200 `{used, reserved, limit, remaining, period_start, resets_at}` |

The caller makes one `request_id` per operation and reuses it on every retry.

## Tests

Unit tests need nothing running. Integration and concurrency tests use the compose Postgres
and Redis (in a separate `quota_test` database and Redis DB 15), which must be published on the
host ports 5432 / 6379 for this:

```bash
docker compose -f docker-compose.yml -f docker-compose.test.yml up -d postgres redis
python3 -m venv .venv && .venv/bin/pip install -e '.[dev]'
.venv/bin/pytest tests/unit          # pure logic: billing periods, lease sizing
.venv/bin/pytest                     # unit + integration (real Redis/Postgres, one caller)
.venv/bin/pytest -m concurrency      # 2 service instances x 2 workers, hundreds of concurrent clients
docker compose run --rm verify       # end-to-end through the running stack (see Run)
```

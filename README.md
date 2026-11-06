# Quota metering

Per-organization, per-feature monthly quotas, enforced across a horizontally scaled fleet.
Quota is held in Redis as leases granted from a Postgres ledger. See [DESIGN.md](DESIGN.md).

## Run

```bash
docker compose up --build
```

Starts Postgres, Redis, seeds 5,000 placeholder orgs (`org1`..`org5000`, features
`feature1`..`feature30`, plans starter/pro/enterprise), and runs the quota service on
http://localhost:8000. Interactive API docs: http://localhost:8000/docs.

```bash
curl -s -XPOST localhost:8000/v1/reserve -H 'content-type: application/json' \
  -d '{"org_id": 42, "feature": "feature7", "units": 10, "request_id": "demo-1"}'
curl -s -XPOST localhost:8000/v1/commit -H 'content-type: application/json' \
  -d '{"org_id": 42, "request_id": "demo-1"}'
curl -s localhost:8000/v1/usage/42/feature7
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

Unit tests need nothing running; integration and concurrency tests use the compose Postgres
and Redis (`docker compose up -d`), in a separate `quota_test` database and Redis DB 15.

```bash
python3 -m venv .venv && .venv/bin/pip install -e '.[dev]'
.venv/bin/pytest tests/unit          # pure logic: billing periods, lease sizing
.venv/bin/pytest                     # unit + integration (real Redis/Postgres, one caller)
.venv/bin/pytest -m concurrency      # 2 service instances x 2 workers, hundreds of concurrent clients
```

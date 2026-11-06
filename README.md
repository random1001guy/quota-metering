# Quota metering

Per-organization, per-feature monthly quotas, enforced across a horizontally scaled fleet.
Quota is held in Redis as leases granted from a Postgres ledger. See [DESIGN.md](DESIGN.md).

## Run

```bash
docker compose up --build
```

Starts Postgres, Redis and the quota service on http://localhost:8000.

## Develop

```bash
python3 -m venv .venv && .venv/bin/pip install -e '.[dev]'
.venv/bin/pytest
```

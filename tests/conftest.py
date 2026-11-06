"""Shared fixtures for integration and concurrency tests.

They run against the Postgres and Redis started by `docker compose up`, but in a separate
database (quota_test) and Redis DB index (15), so they never touch the service's data.
"""
import os
import time
from pathlib import Path

import psycopg
import pytest
import redis

PG_ADMIN_URL = os.environ.get("TEST_PG_ADMIN_URL", "postgresql://quota:quota@localhost:5432/quota")
TEST_DB = "quota_test"
TEST_PG_URL = PG_ADMIN_URL.rsplit("/", 1)[0] + "/" + TEST_DB
TEST_REDIS_URL = os.environ.get("TEST_REDIS_URL", "redis://localhost:6379/15")
SCHEMA = Path(__file__).resolve().parents[1] / "db" / "schema.sql"

# Small fixed data set. Feature limits for the 'starter' plan:
LIMITS = {"feature1": 100, "feature2": 1000, "feature3": 0}
N_ORGS = 20


@pytest.fixture(scope="session")
def test_db() -> str:
    """Create a fresh quota_test database with the schema and fixed test data."""
    try:
        admin = psycopg.connect(PG_ADMIN_URL, autocommit=True, connect_timeout=3)
        redis.Redis.from_url(TEST_REDIS_URL, socket_connect_timeout=3).ping()
    except Exception as e:
        pytest.skip(f"Postgres/Redis not reachable ({e!r}); start them with `docker compose up -d`")
    with admin:
        admin.execute(f"DROP DATABASE IF EXISTS {TEST_DB} WITH (FORCE)")
        admin.execute(f"CREATE DATABASE {TEST_DB}")
    with psycopg.connect(TEST_PG_URL, autocommit=True) as c:
        c.execute(SCHEMA.read_text())
        c.execute("INSERT INTO plans VALUES ('starter')")
        for fid, (name, limit) in enumerate(LIMITS.items(), start=1):
            c.execute("INSERT INTO features VALUES (%s, %s)", (fid, name))
            c.execute("INSERT INTO plan_limits VALUES ('starter', %s, %s)", (fid, limit))
        for i in range(1, N_ORGS + 1):
            c.execute("INSERT INTO orgs (org_id, name, plan_id, anchor_day) VALUES (%s, %s, 'starter', %s)",
                      (i, f"org{i}", (i % 31) + 1))
        c.execute("INSERT INTO orgs (org_id, name, plan_id, anchor_day, status) "
                  "VALUES (99, 'org99', 'starter', 1, 'suspended')")
    return TEST_PG_URL


def reset_state(pg_url: str) -> None:
    """Empty the grant ledger and the test Redis DB."""
    with psycopg.connect(pg_url, autocommit=True) as c:
        c.execute("TRUNCATE quota_periods")
    redis.Redis.from_url(TEST_REDIS_URL).flushdb()


class FakeClock:
    """Starts at real time (so Redis TTLs stay in the future) and only moves when told."""

    def __init__(self):
        self.t = time.time()

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds

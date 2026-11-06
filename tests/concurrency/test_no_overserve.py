"""Concurrency: many clients, several service processes, one nearly-exhausted quota.

Starts 2 service instances (2 worker processes each, so 4 processes share the Redis lease and
the Postgres ledger) and fires hundreds of concurrent requests at one org, spread across the
instances. A read-then-write implementation fails these tests; they check exact totals, not
just "no errors".

  pytest -m concurrency
"""
import asyncio
import os
import random
import socket
import subprocess
import sys
import time
import uuid
from collections import Counter

import httpx
import psycopg
import pytest
import redis

from tests.conftest import TEST_REDIS_URL, reset_state

pytestmark = pytest.mark.concurrency

INSTANCES = 2
WORKERS_PER_INSTANCE = 2
IN_FLIGHT = 200


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def instances(test_db):
    env = {**os.environ, "DATABASE_URL": test_db, "REDIS_URL": TEST_REDIS_URL, "PG_POOL_SIZE": "5"}
    ports = [_free_port() for _ in range(INSTANCES)]
    procs = [subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "quota.app:app", "--port", str(p),
         "--workers", str(WORKERS_PER_INSTANCE), "--log-level", "warning"], env=env)
        for p in ports]
    bases = [f"http://127.0.0.1:{p}" for p in ports]
    deadline = time.time() + 30
    for base in bases:
        while True:
            try:
                if httpx.get(base + "/healthz").status_code == 200:
                    break
            except httpx.TransportError:
                pass
            if time.time() > deadline:
                pytest.fail(f"{base} did not start")
            time.sleep(0.2)
    yield bases
    for p in procs:
        p.terminate()
    for p in procs:
        p.wait(10)


async def post(client, bases, path, body) -> httpx.Response:
    """POST to a random instance; on 503 (refill in progress) retry, as a real client would."""
    for attempt in range(100):
        r = await client.post(random.choice(bases) + path, json=body)
        if r.status_code != 503:
            return r
        await asyncio.sleep(0.005 * (attempt + 1))
    raise AssertionError(f"{path} still 503 after retries")


def ledger(test_db, org, feature_id=2) -> tuple[int, dict]:
    """(Postgres granted, Redis lease hash) for the org's current period."""
    with psycopg.connect(test_db) as c:
        rows = c.execute("SELECT granted FROM quota_periods WHERE org_id = %s AND feature_id = %s",
                         (org, feature_id)).fetchall()
    assert len(rows) == 1
    r = redis.Redis.from_url(TEST_REDIS_URL, decode_responses=True)
    (lease_key,) = r.keys(f"lease:{{{org}}}:{feature_id}:*")
    return rows[0][0], {k: int(v) for k, v in r.hgetall(lease_key).items()}


def check_invariant(test_db, org, limit):
    pg_granted, lease = ledger(test_db, org)
    assert lease["balance"] >= 0 and lease["reserved"] >= 0
    assert lease["balance"] + lease["reserved"] + lease["used"] == lease["granted_total"]
    assert lease["granted_total"] <= pg_granted <= limit
    return lease


async def test_exactly_the_limit_is_served(instances, test_db):
    """1-unit requests, 3x more demand than quota, all at once: exactly `limit` succeed."""
    reset_state(test_db)
    org, limit, n_requests = 3, 1000, 3000
    sem = asyncio.Semaphore(IN_FLIGHT)
    outcomes = Counter()

    async def one(client):
        rid = uuid.uuid4().hex
        async with sem:
            r = await post(client, instances, "/v1/reserve",
                           {"org_id": org, "feature": "feature2", "units": 1, "request_id": rid})
            if r.status_code == 200:
                c = await post(client, instances, "/v1/commit", {"org_id": org, "request_id": rid})
                assert c.status_code == 200
            outcomes[r.status_code] += 1

    async with httpx.AsyncClient(timeout=30) as client:
        await asyncio.gather(*(one(client) for _ in range(n_requests)))
        usage = (await client.get(f"{instances[0]}/v1/usage/{org}/feature2")).json()

    assert outcomes == {200: limit, 429: n_requests - limit}
    assert (usage["used"], usage["reserved"], usage["remaining"]) == (limit, 0, 0)
    lease = check_invariant(test_db, org, limit)
    assert lease["used"] == limit


async def test_batches_failures_and_retries(instances, test_db):
    """Batches of 1-50 units; every reserve and commit is sent twice at once (a client retry);
    30% of operations fail downstream and are released. Usage must equal exactly the units of
    the requests that were committed, never exceed the limit, and leave nothing reserved."""
    reset_state(test_db)
    org, limit, n_requests = 4, 1000, 600
    rng = random.Random(7)
    plan = [(uuid.uuid4().hex, rng.randint(1, 50), rng.random() < 0.3) for _ in range(n_requests)]
    sem = asyncio.Semaphore(IN_FLIGHT)
    committed_units = 0
    fresh_reservations = Counter()

    async def one(client, rid, units, fails):
        nonlocal committed_units
        body = {"org_id": org, "feature": "feature2", "units": units, "request_id": rid}
        ref = {"org_id": org, "request_id": rid}
        async with sem:
            a, b = await asyncio.gather(post(client, instances, "/v1/reserve", body),
                                        post(client, instances, "/v1/reserve", body))
            for r in (a, b):
                assert r.status_code in (200, 429), r.text
                if r.status_code == 200 and not r.json()["duplicate"]:
                    fresh_reservations[rid] += 1
            if fresh_reservations[rid] == 0:
                return                                   # rejected both times
            path = "/v1/release" if fails else "/v1/commit"
            c, d = await asyncio.gather(post(client, instances, path, ref),
                                        post(client, instances, path, ref))
            assert c.status_code == d.status_code == 200, (c.text, d.text)
            assert sorted([c.json()["duplicate"], d.json()["duplicate"]]) == [False, True]
            if not fails:
                committed_units += units

    async with httpx.AsyncClient(timeout=30) as client:
        await asyncio.gather(*(one(client, *p) for p in plan))
        usage = (await client.get(f"{instances[1]}/v1/usage/{org}/feature2")).json()

    assert all(n == 1 for n in fresh_reservations.values())  # a retry never holds twice
    assert usage["used"] == committed_units                  # every commit counted exactly once
    assert usage["used"] <= limit and usage["reserved"] == 0
    lease = check_invariant(test_db, org, limit)
    assert lease["used"] == committed_units
    # demand (~15k units) far exceeds the limit, so the quota should end up nearly spent
    assert usage["remaining"] < 50

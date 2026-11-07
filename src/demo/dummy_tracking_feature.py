"""dummy-tracking-feature: a stand-in product service that consumes quota.

  POST /v1/containers   track a batch of containers   -> 1 unit per container (feature1)
  GET  /v1/schedules    search sailing schedules      -> 1 unit per search    (feature2)

Each call holds the units with the quota service, does the (fake) work, then commits, or
releases if the work failed. Clients send an Idempotency-Key header and reuse it on retries.
`fail=true` makes the downstream work fail, so failure handling can be exercised on purpose.

Every committed operation is also written to this service's own ledger (demo_ledger), which
the verify step reconciles against the quota service's usage numbers.
"""
import asyncio
import logging
import os
import random
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, Query
from fastapi.responses import JSONResponse
from psycopg_pool import AsyncConnectionPool
from pydantic import BaseModel, Field

from demo.quota_client import AlreadyHandled, QuotaClient, QuotaError, QuotaExceeded, QuotaUnavailable

log = logging.getLogger("dummy-tracking-feature")

QUOTA_URL = os.environ.get("QUOTA_URL", "http://localhost:8000")
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://quota:quota@localhost:5432/quota")
CONTAINERS_FEATURE = os.environ.get("CONTAINERS_FEATURE", "feature1")
SCHEDULES_FEATURE = os.environ.get("SCHEDULES_FEATURE", "feature2")
WORK_MS = tuple(float(x) for x in os.environ.get("WORK_MS", "2,10").split(","))

LEDGER_DDL = """
CREATE TABLE IF NOT EXISTS demo_ledger (
    request_id   text PRIMARY KEY,
    org_id       bigint NOT NULL,
    feature      text NOT NULL,
    units        int NOT NULL,
    committed_at timestamptz NOT NULL DEFAULT now()
)"""


class DownstreamFailed(Exception):
    pass


class TrackIn(BaseModel):
    org_id: int
    containers: list[str] = Field(min_length=1, max_length=1000)
    fail: bool = False   # demo only: make the downstream work fail


@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = AsyncConnectionPool(DATABASE_URL, min_size=1, max_size=5,
                               kwargs={"autocommit": True}, open=False)
    await pool.open()
    async with pool.connection() as c, c.transaction():
        await c.execute("SELECT pg_advisory_xact_lock(7001)")   # replicas start together
        await c.execute(LEDGER_DDL)
    app.state.pool = pool
    app.state.quota = QuotaClient(QUOTA_URL)
    yield
    await app.state.quota.aclose()
    await pool.close()


app = FastAPI(title="dummy-tracking-feature (demo quota consumer)", lifespan=lifespan)


async def fake_work(fail: bool) -> None:
    await asyncio.sleep(random.uniform(*WORK_MS) / 1000)
    if fail:
        raise DownstreamFailed("tracking provider error")


async def metered(org_id: int, feature: str, units: int, key: str | None, fail: bool) -> JSONResponse:
    key = key or uuid.uuid4().hex
    hold = None
    try:
        async with app.state.quota.hold(org_id, feature, units, key) as hold:
            await fake_work(fail)
    except QuotaError as e:
        return refused(e, hold or e.hold, units, key)
    except DownstreamFailed:
        return reply(502, hold, detail="tracking provider failed; quota was released")

    if hold.committed:
        async with app.state.pool.connection() as c:
            await c.execute(
                "INSERT INTO demo_ledger (request_id, org_id, feature, units) VALUES (%s, %s, %s, %s)"
                " ON CONFLICT (request_id) DO NOTHING", (key, org_id, feature, units))
    return reply(200, hold, status="done", units=units, request_id=key, replayed=False)


def refused(e: QuotaError, hold, units: int, key: str) -> JSONResponse:
    if isinstance(e, QuotaExceeded):
        return reply(429, hold, detail="monthly quota exceeded", available=e.available)
    if isinstance(e, AlreadyHandled):
        if e.state == "committed":
            return reply(200, hold, status="done", units=units, request_id=key, replayed=True)
        if e.state == "reserved":
            return reply(409, hold, detail="the same request is still in progress")
        return reply(409, hold, detail=f"an earlier attempt with this key ended as '{e.state}'")
    if isinstance(e, QuotaUnavailable):
        return reply(503, hold, detail="quota service unavailable", headers={"Retry-After": "1"})
    return reply(400, hold, detail=str(e))


def reply(http_status: int, hold, headers: dict | None = None, **body) -> JSONResponse:
    headers = dict(headers or {})
    if hold is not None:
        headers["X-Quota-Overhead-Ms"] = f"{hold.overhead_s * 1000:.3f}"
    return JSONResponse(status_code=http_status, content=body, headers=headers)


@app.get("/healthz")
async def healthz() -> dict:
    return {"status": "ok"}


@app.post("/v1/containers")
async def track_containers(body: TrackIn, idempotency_key: str | None = Header(default=None)):
    return await metered(body.org_id, CONTAINERS_FEATURE, len(body.containers), idempotency_key, body.fail)


@app.get("/v1/schedules")
async def search_schedules(org_id: int, origin: str = "SGSIN", destination: str = "NLRTM",
                           fail: bool = Query(default=False),
                           idempotency_key: str | None = Header(default=None)):
    return await metered(org_id, SCHEDULES_FEATURE, 1, idempotency_key, fail)

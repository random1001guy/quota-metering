"""HTTP API of the quota service.

  POST /v1/reserve   hold units for a request, all or nothing
  POST /v1/commit    the downstream work succeeded: reserved units become used
  POST /v1/release   the downstream work failed: reserved units go back
  GET  /v1/usage/{org_id}/{feature}   used this period and when it resets

Callers generate one request_id per logical operation and reuse it on every retry; reserve,
commit and release are all idempotent on it. If Redis or Postgres can't be reached the service
fails closed (503).
"""
import logging
from contextlib import asynccontextmanager
from datetime import date, datetime

import psycopg
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from psycopg_pool import AsyncConnectionPool, PoolTimeout
from pydantic import BaseModel, Field
from redis.asyncio import BlockingConnectionPool, Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from quota.config import Settings
from quota.pg import PgStore
from quota.redis_store import RedisStore
from quota.service import Busy, NotFound, OrgInactive, QuotaService

RequestId = Field(min_length=1, max_length=128)


class ReserveIn(BaseModel):
    org_id: int
    feature: str
    units: int = Field(gt=0, le=1_000_000)
    request_id: str = RequestId


class RequestRef(BaseModel):
    org_id: int
    request_id: str = RequestId


class ReserveOut(BaseModel):
    result: str          # reserved | committed | released | expired (latter three: duplicates)
    duplicate: bool
    units: int


class SettleOut(BaseModel):
    result: str          # committed | released
    duplicate: bool


class UsageOut(BaseModel):
    org_id: int
    feature: str
    period_start: date
    resets_at: datetime
    limit: int
    used: int
    reserved: int
    remaining: int


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        pool = AsyncConnectionPool(settings.database_url, min_size=1, max_size=settings.pg_pool_size,
                                   kwargs={"autocommit": True}, open=False)
        await pool.open()
        # Blocking pool: with every connection busy a call WAITS (up to 1 s) for one, instead of
        # redis-py's default pool, which raises MaxConnectionsError at once past 100 in flight.
        redis = Redis(connection_pool=BlockingConnectionPool.from_url(
            settings.redis_url, max_connections=settings.redis_pool_size, timeout=1,
            decode_responses=True))
        svc = QuotaService(settings, PgStore(pool), RedisStore(redis, settings.idempotency_window_s))
        await svc.start()
        app.state.svc = svc
        yield
        await svc.stop()
        await redis.aclose()
        await pool.close()

    app = FastAPI(title="Quota service", lifespan=lifespan)

    def error(status: int, detail: str, **extra) -> JSONResponse:
        return JSONResponse(status_code=status, content={"detail": detail, **extra})

    @app.exception_handler(NotFound)
    async def not_found(_: Request, e: NotFound):
        return error(404, str(e))

    @app.exception_handler(OrgInactive)
    async def inactive(_: Request, e: OrgInactive):
        return error(403, str(e))

    @app.exception_handler(Busy)
    async def busy(_: Request, e: Busy):
        return JSONResponse(status_code=503, content={"detail": str(e)}, headers={"Retry-After": "1"})

    async def store_down(_: Request, e: Exception):
        logging.getLogger(__name__).warning("quota store unavailable: %r", e)
        return JSONResponse(status_code=503, content={"detail": "quota store unavailable"},
                            headers={"Retry-After": "1"})

    for exc in (RedisConnectionError, RedisTimeoutError, psycopg.OperationalError, PoolTimeout):
        app.add_exception_handler(exc, store_down)

    def svc(request: Request) -> QuotaService:
        return request.app.state.svc

    @app.get("/healthz")
    async def healthz() -> dict:
        return {"status": "ok"}

    @app.post("/v1/reserve", response_model=ReserveOut,
              responses={429: {"description": "Not enough quota left"}})
    async def reserve(body: ReserveIn, request: Request):
        r = await svc(request).reserve(body.org_id, body.feature, body.units, body.request_id)
        if r.status == "rejected":
            return error(429, "quota exceeded", units=r.units, available=r.available)
        if r.status == "duplicate":
            return ReserveOut(result=r.state, duplicate=True, units=r.units)
        return ReserveOut(result="reserved", duplicate=False, units=r.units)

    @app.post("/v1/commit", response_model=SettleOut)
    async def commit(body: RequestRef, request: Request):
        outcome = await svc(request).commit(body.org_id, body.request_id)
        if outcome == "UNKNOWN":
            return error(404, "no such reservation (never made, or older than the idempotency window)")
        if outcome == "CONFLICT":
            return error(409, "reservation was released")
        if outcome == "EXPIRED":
            return error(409, "reservation expired before commit; units were reclaimed and not charged")
        return SettleOut(result="committed", duplicate=outcome == "ALREADY")

    @app.post("/v1/release", response_model=SettleOut)
    async def release(body: RequestRef, request: Request):
        outcome = await svc(request).release(body.org_id, body.request_id)
        if outcome == "UNKNOWN":
            return error(404, "no such reservation (never made, or older than the idempotency window)")
        if outcome == "CONFLICT":
            return error(409, "reservation was already committed")
        return SettleOut(result="released", duplicate=outcome == "ALREADY")

    @app.get("/v1/usage/{org_id}/{feature}", response_model=UsageOut)
    async def usage(org_id: int, feature: str, request: Request):
        u = await svc(request).usage(org_id, feature)
        return UsageOut(org_id=u.org_id, feature=u.feature, period_start=u.period_start,
                        resets_at=u.resets_at, limit=u.limit, used=u.used, reserved=u.reserved,
                        remaining=u.remaining)

    return app


app = create_app()

"""Quota service against real Redis and Postgres (the Lua scripts and SQL are the logic under
test, so they are not mocked). Single caller; concurrency lives in tests/concurrency."""
import uuid

import pytest
from psycopg_pool import AsyncConnectionPool
from redis.asyncio import Redis

from quota.config import Settings
from quota.pg import PgStore
from quota.redis_store import RedisStore
from quota.service import NotFound, OrgInactive, QuotaService
from tests.conftest import TEST_REDIS_URL, FakeClock, reset_state

ORG = 1
F1 = "feature1"   # limit 100 -> lease chunk 5


@pytest.fixture
async def svc(test_db):
    reset_state(test_db)
    settings = Settings(database_url=test_db, redis_url=TEST_REDIS_URL)
    pool = AsyncConnectionPool(test_db, min_size=1, max_size=4, kwargs={"autocommit": True}, open=False)
    await pool.open()
    redis = Redis.from_url(TEST_REDIS_URL, decode_responses=True)
    service = QuotaService(settings, PgStore(pool), RedisStore(redis, settings.idempotency_window_s),
                           clock=FakeClock())
    await service.start()
    yield service
    await service.stop()
    await redis.aclose()
    await pool.close()


def rid() -> str:
    return uuid.uuid4().hex


async def take(svc, units, org=ORG, feature=F1) -> str:
    """Reserve and commit; returns the request id."""
    r = rid()
    assert (await svc.reserve(org, feature, units, r)).status == "reserved"
    assert await svc.commit(org, r) == "OK"
    return r


async def drain(svc, org=ORG, feature=F1) -> int:
    """Take 1 unit at a time until rejected; returns how many were served."""
    served = 0
    while (await svc.reserve(org, feature, 1, r := rid())).status == "reserved":
        assert await svc.commit(org, r) == "OK"
        served += 1
    return served


async def granted(svc, org=ORG, feature=F1) -> int:
    cfg = await svc._org(org)
    row = await svc.pg.current(org, svc.features[feature], svc._period(cfg, svc.clock()))
    return row[0] if row else 0


# ---- basic flow --------------------------------------------------------------------------

async def test_reserve_then_commit_counts_as_used(svc):
    r = rid()
    assert (await svc.reserve(ORG, F1, 10, r)).status == "reserved"
    u = await svc.usage(ORG, F1)
    assert (u.used, u.reserved, u.remaining) == (0, 10, 90)
    assert await svc.commit(ORG, r) == "OK"
    u = await svc.usage(ORG, F1)
    assert (u.used, u.reserved, u.remaining, u.limit) == (10, 0, 90, 100)


async def test_usage_before_any_request(svc):
    u = await svc.usage(ORG, F1)
    assert (u.used, u.reserved, u.remaining) == (0, 0, 100)
    assert u.period_start < u.resets_at.date()


async def test_postgres_only_sees_lease_sized_grants(svc):
    for _ in range(10):
        await take(svc, 1)
    await svc.stop()   # let background refills finish
    # 10 units used, but Postgres moved them in a few chunks, never past the limit
    assert 10 <= await granted(svc) <= 100


# ---- batch policy: all or nothing --------------------------------------------------------

async def test_batch_larger_than_remaining_is_rejected_whole(svc):
    await take(svc, 60)
    r = await svc.reserve(ORG, F1, 50, rid())
    assert (r.status, r.available) == ("rejected", 40)
    assert (await svc.usage(ORG, F1)).used == 60          # nothing partially taken
    await take(svc, 40)                                    # exactly what's left still fits
    assert (await svc.usage(ORG, F1)).remaining == 0
    assert (await svc.reserve(ORG, F1, 1, rid())).status == "rejected"


async def test_request_bigger_than_the_whole_limit(svc):
    assert (await svc.reserve(ORG, F1, 101, rid())).status == "rejected"


async def test_zero_limit_feature_rejects(svc):
    assert (await svc.reserve(ORG, "feature3", 1, rid())).status == "rejected"


# ---- retries: idempotent on request_id ---------------------------------------------------

async def test_retried_reserve_holds_once(svc):
    r = rid()
    assert (await svc.reserve(ORG, F1, 10, r)).status == "reserved"
    dup = await svc.reserve(ORG, F1, 10, r)
    assert (dup.status, dup.state) == ("duplicate", "reserved")
    assert (await svc.usage(ORG, F1)).reserved == 10


async def test_retried_commit_charges_once(svc):
    r = await take(svc, 10)
    assert await svc.commit(ORG, r) == "ALREADY"
    assert (await svc.reserve(ORG, F1, 10, r)).state == "committed"   # late reserve retry too
    assert (await svc.usage(ORG, F1)).used == 10


# ---- downstream failure ------------------------------------------------------------------

async def test_release_returns_units(svc):
    r = rid()
    await svc.reserve(ORG, F1, 30, r)
    assert await svc.release(ORG, r) == "OK"
    assert await svc.release(ORG, r) == "ALREADY"
    u = await svc.usage(ORG, F1)
    assert (u.used, u.reserved, u.remaining) == (0, 0, 100)
    assert await svc.commit(ORG, r) == "CONFLICT"
    assert await drain(svc) == 100      # released units are usable again


async def test_cannot_release_after_commit(svc):
    r = await take(svc, 10)
    assert await svc.release(ORG, r) == "CONFLICT"
    assert (await svc.usage(ORG, F1)).used == 10


async def test_unknown_reservation(svc):
    assert await svc.commit(ORG, "never-made") == "UNKNOWN"
    assert await svc.release(ORG, "never-made") == "UNKNOWN"


# ---- client crash: reservation never settled ---------------------------------------------

async def test_expired_reservation_is_reclaimed_on_read(svc):
    r = rid()
    await svc.reserve(ORG, F1, 30, r)
    svc.clock.advance(svc.s.reserve_timeout_s + 1)
    u = await svc.usage(ORG, F1)
    assert (u.used, u.reserved, u.remaining) == (0, 0, 100)
    assert await svc.commit(ORG, r) == "EXPIRED"      # too late: not charged
    assert await svc.release(ORG, r) == "ALREADY"


async def test_expired_reservation_is_reclaimed_by_next_reserve(svc):
    await svc.reserve(ORG, F1, 100, rid())            # holds everything, then the client dies
    assert (await svc.reserve(ORG, F1, 1, rid())).status == "rejected"
    svc.clock.advance(svc.s.reserve_timeout_s + 1)
    assert (await svc.reserve(ORG, F1, 100, rid())).status == "reserved"


# ---- crash between the Postgres grant and the Redis apply --------------------------------

async def test_stranded_grant_is_recovered_by_next_refill(svc):
    await take(svc, 10)
    await svc.stop()
    cfg = await svc._org(ORG)
    period = svc._period(cfg, svc.clock())
    # A refill that committed in Postgres but never reached Redis (instance crashed)
    assert await svc.pg.grant(ORG, svc.features[F1], period, 100, 20) is not None
    assert 10 + await drain(svc) == 100               # every unit still usable, none extra


async def test_stranded_final_grant_is_recovered(svc):
    await take(svc, 10)
    await svc.stop()
    cfg = await svc._org(ORG)
    period = svc._period(cfg, svc.clock())
    total, _ = await svc.pg.grant(ORG, svc.features[F1], period, 100, 1000)
    assert total == 100                               # Postgres is fully granted, Redis isn't
    assert 10 + await drain(svc) == 100


# ---- Redis data loss ---------------------------------------------------------------------

async def test_lost_redis_data_under_serves_never_over_serves(svc):
    await take(svc, 50)
    await svc.stop()
    before = await granted(svc)
    await svc.rs.r.flushdb()                          # Redis loses everything
    served_after = await drain(svc)
    assert served_after == 100 - before               # what was in the lost lease is written off
    assert 50 + served_after <= 100


# ---- monthly reset -----------------------------------------------------------------------

async def test_new_period_starts_fresh(svc):
    assert await drain(svc) == 100
    first = await svc.usage(ORG, F1)
    svc.clock.advance(32 * 86400)
    second = await svc.usage(ORG, F1)
    assert second.period_start == first.resets_at.date()
    assert (second.used, second.remaining) == (0, 100)
    assert await drain(svc) == 100


async def test_reservation_settles_in_its_own_period(svc):
    r = rid()
    await svc.reserve(ORG, F1, 10, r)
    svc.clock.advance(32 * 86400)                     # month rolls over before the commit
    assert await svc.commit(ORG, r) == "OK"
    assert (await svc.usage(ORG, F1)).used == 0       # new period untouched


# ---- lookups -----------------------------------------------------------------------------

async def test_unknown_org_and_feature(svc):
    with pytest.raises(NotFound):
        await svc.reserve(12345, F1, 1, rid())
    with pytest.raises(NotFound):
        await svc.usage(ORG, "nope")


async def test_suspended_org_is_refused(svc):
    with pytest.raises(OrgInactive):
        await svc.reserve(99, F1, 1, rid())

"""Quota logic: reserve / commit / release / usage, and refilling leases from Postgres.

Where a unit of quota can be, for one (org, feature, period):
  Postgres, not granted  --refill-->  Redis balance  --reserve-->  reserved  --commit-->  used
                                           ^------------ release / reclaim ----'
Postgres caps the total ever granted at the limit (pg.GRANT_SQL); Redis scripts never let
balance go negative. Together: used + reserved + balance <= granted <= limit, always.
"""
import asyncio
import logging
import math
import secrets
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from quota.config import Settings
from quota.periods import Period, period_for
from quota.pg import OrgConfig, PgStore
from quota.redis_store import LeaseKeys, RedisStore, res_key

log = logging.getLogger(__name__)


class NotFound(Exception):
    pass


class OrgInactive(Exception):
    pass


class Busy(Exception):
    """Another instance is refilling this lease; the client should retry shortly."""


def lease_chunk(limit: int, fraction: float) -> int:
    """Units moved from Postgres to Redis per refill (at least 1)."""
    return max(1, math.ceil(limit * fraction))


@dataclass(frozen=True)
class ReserveResult:
    status: str                    # 'reserved' | 'duplicate' | 'rejected'
    units: int
    state: str | None = None       # duplicate: state of the original reservation
    available: int | None = None   # rejected: units still available, when known


@dataclass(frozen=True)
class Usage:
    org_id: int
    feature: str
    period_start: date
    resets_at: datetime
    limit: int
    used: int
    reserved: int

    @property
    def remaining(self) -> int:
        return max(0, self.limit - self.used - self.reserved)


# Back-off while another instance holds the refill lock (~170 ms in total).
_LOCK_WAITS_S = (0.002, 0.005, 0.01, 0.02, 0.04, 0.09)


class QuotaService:
    def __init__(self, settings: Settings, pg: PgStore, rs: RedisStore, clock=time.time):
        self.s = settings
        self.pg = pg
        self.rs = rs
        self.clock = clock                       # injectable for tests
        self.features: dict[str, int] = {}
        self._cfg: dict[int, tuple[float, OrgConfig]] = {}
        self._tasks: set[asyncio.Task] = set()

    async def start(self) -> None:
        self.features = await self.pg.load_features()

    async def stop(self) -> None:
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

    # ---- config ----------------------------------------------------------------------------

    async def _org(self, org_id: int) -> OrgConfig:
        hit = self._cfg.get(org_id)
        if hit and hit[0] > time.monotonic():
            return hit[1]
        cfg = await self.pg.load_org(org_id)
        if cfg is None:
            raise NotFound(f"unknown org {org_id}")
        self._cfg[org_id] = (time.monotonic() + self.s.config_cache_ttl_s, cfg)
        return cfg

    async def _resolve(self, org_id: int, feature: str) -> tuple[OrgConfig, int, int]:
        feature_id = self.features.get(feature)
        if feature_id is None:
            raise NotFound(f"unknown feature {feature!r}")
        cfg = await self._org(org_id)
        if feature_id not in cfg.limits:
            raise NotFound(f"org {org_id} has no quota for {feature!r}")
        return cfg, feature_id, cfg.limits[feature_id]

    def _period(self, cfg: OrgConfig, now: float) -> Period:
        return period_for(cfg.anchor_day, datetime.fromtimestamp(now, timezone.utc))

    @staticmethod
    def _expire_at_ms(period: Period) -> int:
        # Lease keys outlive their period by 2 days so late commits/releases still land.
        return int((period.resets_at + timedelta(days=2)).timestamp() * 1000)

    # ---- hot path --------------------------------------------------------------------------

    async def reserve(self, org_id: int, feature: str, units: int, request_id: str) -> ReserveResult:
        cfg, feature_id, limit = await self._resolve(org_id, feature)
        if cfg.status != "active":
            raise OrgInactive(f"org {org_id} is {cfg.status}")
        if units > limit:
            return ReserveResult("rejected", units)
        chunk = lease_chunk(limit, self.s.lease_fraction)
        watermark = max(1, math.ceil(chunk * self.s.refill_watermark))
        res = res_key(org_id, request_id)
        waits = iter(_LOCK_WAITS_S)

        for _ in range(len(_LOCK_WAITS_S) + 4):
            now = self.clock()
            period = self._period(cfg, now)
            k = LeaseKeys.of(org_id, feature_id, period.id)
            token = secrets.token_hex(8)
            status, a, b = await self.rs.reserve(
                k, res, units, now_ms=int(now * 1000),
                deadline_ms=int((now + self.s.reserve_timeout_s) * 1000),
                expire_at_ms=self._expire_at_ms(period), watermark=watermark,
                token=token, lock_ms=self.s.refill_lock_ms)

            if status == "OK":
                if b:  # balance fell below the watermark and we took the lock: top up async
                    self._spawn(self._refill(k, org_id, feature_id, period, limit, chunk, 0, token, False))
                return ReserveResult("reserved", units)
            if status == "DUP":
                return ReserveResult("duplicate", int(b), state=a)
            if status == "REJECT":
                return ReserveResult("rejected", units, available=int(a))
            # INIT (no lease yet) or REFILL (balance short by `a`)
            if b:  # we hold the refill lock: refill, then try again
                await self._refill(k, org_id, feature_id, period, limit, chunk, int(a), token,
                                   init=(status == "INIT"))
                continue
            delay = next(waits, None)   # someone else is refilling: wait for them
            if delay is None:
                break
            await asyncio.sleep(delay)
        raise Busy("quota refill in progress, retry")

    async def _refill(self, k: LeaseKeys, org_id: int, feature_id: int, period: Period,
                      limit: int, chunk: int, short: int, token: str, init: bool) -> None:
        """Move quota from Postgres into the lease. Caller holds the refill lock (token)."""
        try:
            if init:
                row = await self.pg.current(org_id, feature_id, period)
                await self.rs.init_lease(k, row[0] if row else 0, self._expire_at_ms(period))
            got = await self.pg.grant(org_id, feature_id, period, limit, max(short, chunk))
            if got is None:
                # Already fully granted. Still apply the current total: if the instance that
                # made the last grant crashed before applying it, this recovers those units.
                total, _ = await self.pg.current(org_id, feature_id, period)
                exhausted = True
            else:
                total, row_limit = got
                exhausted = total >= row_limit
            await self.rs.apply_grant(k, total, exhausted, token)   # also releases the lock
        except BaseException:
            try:
                await self.rs.unlock(k, token)
            except Exception:
                log.exception("could not release refill lock %s", k.lock)
            raise

    def _spawn(self, coro) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)

        def done(t: asyncio.Task) -> None:
            self._tasks.discard(t)
            if not t.cancelled() and t.exception():
                log.error("background refill failed", exc_info=t.exception())

        task.add_done_callback(done)

    async def commit(self, org_id: int, request_id: str) -> str:
        """OK | ALREADY | CONFLICT | EXPIRED | UNKNOWN (see lua/commit.lua)."""
        return await self.rs.commit(res_key(org_id, request_id))

    async def release(self, org_id: int, request_id: str) -> str:
        """OK | ALREADY | CONFLICT | UNKNOWN (see lua/release.lua)."""
        return await self.rs.release(res_key(org_id, request_id))

    # ---- reporting -------------------------------------------------------------------------

    async def usage(self, org_id: int, feature: str) -> Usage:
        cfg, feature_id, limit = await self._resolve(org_id, feature)
        now = self.clock()
        period = self._period(cfg, now)
        state = await self.rs.usage(LeaseKeys.of(org_id, feature_id, period.id), int(now * 1000))
        used, reserved = (state.used, state.reserved) if state else (0, 0)
        return Usage(org_id, feature, period.start, period.resets_at, limit, used, reserved)

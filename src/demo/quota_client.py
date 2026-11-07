"""Client for the quota service, as a product service would use it.

    async with quota.hold(org_id, "feature1", units=len(containers), request_id=key) as hold:
        await do_the_work()          # raises -> the units are released
    # normal exit -> the units are committed (hold.committed tells whether the commit landed)

One request_id per operation, reused on every retry: the quota service turns repeats into
duplicates, so a retried reserve or commit never charges twice.
"""
import asyncio
import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass

import httpx

log = logging.getLogger(__name__)


class QuotaError(Exception):
    """The quota service refused the request (unknown org/feature, inactive org, bad input)."""
    hold: "Hold | None" = None   # set when raised from hold(): carries the time spent so far


class QuotaExceeded(QuotaError):
    def __init__(self, available: int | None):
        super().__init__(f"quota exceeded (available: {available})")
        self.available = available


class QuotaUnavailable(QuotaError):
    """The quota service could not be reached or kept answering 503: fail closed."""


class AlreadyHandled(QuotaError):
    """This request_id was used before. `state`: reserved | committed | released | expired."""

    def __init__(self, state: str):
        super().__init__(f"request already handled ({state})")
        self.state = state


@dataclass
class Hold:
    org_id: int
    request_id: str
    units: int
    overhead_s: float = 0.0   # time spent waiting on the quota service for this operation
    committed: bool = False


class QuotaClient:
    def __init__(self, base_url: str, *, timeout_s: float = 1.0, retries: int = 3,
                 max_connections: int = 200):
        self.http = httpx.AsyncClient(
            base_url=base_url, timeout=timeout_s,
            # idle connections are dropped well before the server's 30 s keep-alive timeout, so a
            # request never goes out on a connection the server is closing at that moment
            limits=httpx.Limits(max_connections=max_connections,
                                max_keepalive_connections=max_connections, keepalive_expiry=10))
        self.retries = retries

    async def aclose(self) -> None:
        await self.http.aclose()

    async def _post(self, path: str, body: dict) -> tuple[httpx.Response, int]:
        """POST, retrying 503s and transport errors with the SAME body (so the same request_id).
        Returns the response and how many attempts it took."""
        for attempt in range(self.retries + 1):
            try:
                r = await self.http.post(path, json=body)
                if r.status_code != 503:
                    return r, attempt + 1
            except httpx.TransportError as e:
                log.warning("quota %s attempt %d: %r", path, attempt + 1, e)
            if attempt < self.retries:
                await asyncio.sleep(0.02 * 2 ** attempt)   # 20, 40, 80 ms
        raise QuotaUnavailable(f"{path} failed after {self.retries + 1} attempts")

    async def _reserve(self, h: Hold, feature: str) -> None:
        r, attempts = await self._post("/v1/reserve", {
            "org_id": h.org_id, "feature": feature, "units": h.units, "request_id": h.request_id})
        if r.status_code == 429:
            raise QuotaExceeded(r.json().get("available"))
        if r.status_code != 200:
            raise QuotaError(f"reserve: HTTP {r.status_code} {r.text}")
        body = r.json()
        # A duplicate on OUR OWN retry means the first attempt's reply was lost: the hold is ours.
        if body["duplicate"] and not (attempts > 1 and body["result"] == "reserved"):
            raise AlreadyHandled(body["result"])

    @asynccontextmanager
    async def hold(self, org_id: int, feature: str, units: int, request_id: str):
        h = Hold(org_id, request_id, units)
        t = time.perf_counter()
        try:
            await self._reserve(h, feature)
        except QuotaError as e:
            e.hold = h
            raise
        finally:
            h.overhead_s += time.perf_counter() - t

        ref = {"org_id": org_id, "request_id": request_id}
        # (after this point exceptions come from the caller's work, not from the quota service)
        try:
            yield h
        except BaseException:
            t = time.perf_counter()
            try:
                await self._post("/v1/release", ref)
            except QuotaError:
                # Not fatal: the reservation is reclaimed when its deadline passes.
                log.warning("release failed for %s; it will be reclaimed at its deadline", request_id)
            h.overhead_s += time.perf_counter() - t
            raise
        t = time.perf_counter()
        try:
            r, _ = await self._post("/v1/commit", ref)
            h.committed = r.status_code == 200
            if not h.committed:
                log.error("commit for %s: HTTP %d %s", request_id, r.status_code, r.text)
        except QuotaUnavailable:
            # The work is done; the hold will be reclaimed (under-charge, never over-serve).
            log.error("commit failed for %s; the reservation will be reclaimed", request_id)
        finally:
            h.overhead_s += time.perf_counter() - t

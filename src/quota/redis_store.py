"""Redis access: key layout and the Lua scripts that move quota between states.

Every key carries the org as a hashtag ({org}), so all of one org's keys live in the same
Redis Cluster slot and a script may touch several of them atomically.

  lease:{org}:<feature>:<period>   hash: balance, reserved, used, granted_total, exhausted
  dl:{org}:<feature>:<period>      sorted set: reservation key -> deadline (ms)
  refill:{org}:<feature>:<period>  string: token of the instance currently refilling
  res:{org}:<request_id>           hash: state, n, lease, dl  (the idempotency record)
"""
from dataclasses import dataclass
from pathlib import Path

from redis.asyncio import Redis

_LUA = Path(__file__).parent / "lua"


def _script(name: str, with_lib: bool = False) -> str:
    src = (_LUA / f"{name}.lua").read_text()
    return (_LUA / "lib.lua").read_text() + "\n" + src if with_lib else src


@dataclass(frozen=True)
class LeaseKeys:
    lease: str
    dl: str
    lock: str

    @classmethod
    def of(cls, org_id: int, feature_id: int, period_id: str) -> "LeaseKeys":
        suffix = f"{{{org_id}}}:{feature_id}:{period_id}"
        return cls(f"lease:{suffix}", f"dl:{suffix}", f"refill:{suffix}")


def res_key(org_id: int, request_id: str) -> str:
    return f"res:{{{org_id}}}:{request_id}"


@dataclass(frozen=True)
class LeaseState:
    used: int
    reserved: int
    balance: int
    granted_total: int
    exhausted: bool


class RedisStore:
    def __init__(self, r: Redis, idempotency_window_s: int):
        self.r = r
        self.window = idempotency_window_s
        self._reserve = r.register_script(_script("reserve", with_lib=True))
        self._usage = r.register_script(_script("usage", with_lib=True))
        self._init = r.register_script(_script("init_lease"))
        self._apply = r.register_script(_script("apply_grant"))
        self._unlock = r.register_script(_script("release_lock"))
        self._commit = r.register_script(_script("commit"))
        self._release = r.register_script(_script("release"))

    async def reserve(self, k: LeaseKeys, res: str, n: int, *, now_ms: int, deadline_ms: int,
                      expire_at_ms: int, watermark: int, token: str, lock_ms: int) -> list:
        return await self._reserve(
            keys=[k.lease, k.dl, k.lock, res],
            args=[n, now_ms, deadline_ms, expire_at_ms, watermark, token, lock_ms, self.window])

    async def init_lease(self, k: LeaseKeys, baseline: int, expire_at_ms: int) -> bool:
        return bool(await self._init(keys=[k.lease], args=[baseline, expire_at_ms]))

    async def apply_grant(self, k: LeaseKeys, total: int, exhausted: bool, token: str) -> int:
        return int(await self._apply(keys=[k.lease, k.lock], args=[total, int(exhausted), token]))

    async def unlock(self, k: LeaseKeys, token: str) -> None:
        await self._unlock(keys=[k.lock], args=[token])

    async def commit(self, res: str) -> str:
        return await self._commit(keys=[res], args=[self.window])

    async def release(self, res: str) -> str:
        return await self._release(keys=[res], args=[self.window])

    async def usage(self, k: LeaseKeys, now_ms: int) -> LeaseState | None:
        out = await self._usage(keys=[k.lease, k.dl], args=[now_ms, self.window])
        if not out:
            return None
        used, reserved, balance, granted_total, exhausted = (int(x) for x in out)
        return LeaseState(used, reserved, balance, granted_total, bool(exhausted))

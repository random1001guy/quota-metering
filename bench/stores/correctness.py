"""Concurrency correctness under real contention, for both stores.

8 processes (= 8 instances) release their requests at the same instant against ONE nearly-exhausted
quota row. Every request is also sent twice concurrently with the same request_id (a client retry
racing the original).

Assertions
  A (1 unit each, 2,400 requests vs 500 left): exactly 500 accepted, used == 500, never above.
  B (1-50 units each): used <= limit; sum of units the clients were told "ok" == used == sum of
    stored charge records; each request_id charged at most once; every rejected request really
    did not fit (n > limit - final_used, valid because used only grows).
"""
from __future__ import annotations

import asyncio
import multiprocessing as mp
import random
import sys
import time
import uuid

import psycopg
import redis

from common import LUA, PERIOD, PG_DSN, REDIS_PORT, qkey, reqkey

WORKERS, CONNS = 8, 16
ORG, FEAT, LIMIT = 4242, 7, 500


def reset(store: str) -> None:
    if store == "pg":
        with psycopg.connect(PG_DSN, autocommit=True) as c:
            c.execute("DELETE FROM charges WHERE org_id = %s", (ORG,))
            c.execute("UPDATE quota SET used = 0, quota_limit = %s WHERE org_id=%s AND feature=%s AND period=%s",
                      (LIMIT, ORG, FEAT, PERIOD))
    else:
        r = redis.Redis(port=REDIS_PORT)
        for k in r.scan_iter(match=reqkey(ORG, "*"), count=10_000):
            r.delete(k)
        r.hset(qkey(ORG, FEAT), mapping={"used": 0, "limit": LIMIT})


def final_state(store: str) -> tuple[int, int]:
    """(used, sum of stored charge records)"""
    if store == "pg":
        with psycopg.connect(PG_DSN) as c:
            used = c.execute("SELECT used FROM quota WHERE org_id=%s AND feature=%s AND period=%s",
                             (ORG, FEAT, PERIOD)).fetchone()[0]
            rec = c.execute("SELECT coalesce(sum(units),0) FROM charges WHERE org_id=%s", (ORG,)).fetchone()[0]
        return used, rec
    r = redis.Redis(port=REDIS_PORT)
    used = int(r.hget(qkey(ORG, FEAT), "used"))
    rec = sum(int(r.get(k)) for k in r.scan_iter(match=reqkey(ORG, "*"), count=10_000))
    return used, rec


async def fire(store: str, reqs: list[tuple[str, int]], start_at: float) -> list[tuple[str, int, int]]:
    """Send each (request_id, units) TWICE concurrently. Returns (rid, units, code) per send."""
    if store == "pg":
        conns = [await psycopg.AsyncConnection.connect(PG_DSN, autocommit=True) for _ in range(CONNS)]
        pool: asyncio.Queue = asyncio.Queue()
        for c in conns:
            pool.put_nowait(c)

        async def call(rid, n):
            c = await pool.get()
            try:
                cur = await c.execute("SELECT charge(%s::uuid, %s::int, %s::smallint, %s, %s::int)",
                                      (rid, ORG, FEAT, PERIOD, n))
                return (await cur.fetchone())[0]
            finally:
                pool.put_nowait(c)
    else:
        import redis.asyncio as aredis
        r = aredis.Redis(connection_pool=aredis.BlockingConnectionPool(port=REDIS_PORT, max_connections=CONNS))
        sha = await r.script_load(LUA)

        async def call(rid, n):
            return await r.execute_command("EVALSHA", sha, 2, reqkey(ORG, rid), qkey(ORG, FEAT), n, 3600)

    await asyncio.sleep(max(0.0, start_at - time.time()))
    jobs = [(rid, n) for rid, n in reqs for _ in range(2)]
    random.shuffle(jobs)
    codes = await asyncio.gather(*(call(rid, n) for rid, n in jobs))
    if store == "pg":
        for c in conns:
            await c.close()
    else:
        await r.aclose()
    return [(rid, n, int(code)) for (rid, n), code in zip(jobs, codes)]


def _worker(store, reqs, start_at, q):
    q.put(asyncio.run(fire(store, reqs, start_at)))


def scenario(store: str, name: str, per_worker: int, units) -> bool:
    reset(store)
    rng = random.Random(42)
    batches = [[(str(uuid.uuid4()), units(rng)) for _ in range(per_worker)] for _ in range(WORKERS)]
    ctx = mp.get_context("spawn")
    q = ctx.Queue()
    start_at = time.time() + 2.0
    procs = [ctx.Process(target=_worker, args=(store, b, start_at, q)) for b in batches]
    for p in procs:
        p.start()
    results = [x for _ in procs for x in q.get(timeout=120)]
    for p in procs:
        p.join()

    used, recorded = final_state(store)
    ok = [(rid, n) for rid, n, code in results if code >= 0]
    ok_sum = sum(n for _, n in ok)
    ok_ids = [rid for rid, _ in ok]
    rejected = [n for _, n, code in results if code == -1]
    dups = sum(1 for *_, code in results if code == -2)
    remaining = LIMIT - used

    checks = {
        "never over limit (used <= limit)": used <= LIMIT,
        "clients told ok == counter (ok_sum == used)": ok_sum == used,
        "stored charge records == counter": recorded == used,
        "each request_id charged at most once": len(ok_ids) == len(set(ok_ids)),
        "every rejection truly did not fit": all(n > remaining for n in rejected),
    }
    if name.startswith("A"):
        checks["exactly 500 accepted, 0 remaining"] = len(ok) == LIMIT and remaining == 0
    passed = all(checks.values())
    print(f"[{store}] {name}: sends={len(results)} ok={len(ok)} rejected={len(rejected)} dup={dups} "
          f"used={used}/{LIMIT} -> {'PASS' if passed else 'FAIL'}")
    for k, v in checks.items():
        print(f"    {'ok ' if v else 'XX '} {k}")
    return passed


def main(stores):
    ok = True
    for store in stores:
        ok &= scenario(store, "A: 2,400 x 1 unit vs 500 left", 300, lambda r: 1)
        ok &= scenario(store, "B: 800 x 1-50 units vs 500 left", 100, lambda r: r.randint(1, 50))
    print("ALL PASS" if ok else "FAILURES")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:] or ["pg", "redis"]))

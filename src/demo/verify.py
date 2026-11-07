"""End-to-end verification through the running stack: dummy-tracking-feature -> quota service -> Redis/Postgres.

    docker compose run --rm verify

Creates fresh orgs on a 'verify' plan with the brief's example limits (500 container-tracking
units, 1,000 schedule searches), so the background load never touches them. Then runs each
scenario through dummy-tracking-feature and checks the outcome three ways:
  what the client saw  ==  dummy-tracking-feature's ledger (demo_ledger)  ==  quota usage endpoint,
and that used + reserved <= Postgres granted <= limit. Prints a PASS/FAIL table; exit code 1 on
any failure.

Env: CONSUMER_URL, QUOTA_URL, DATABASE_URL, MIXED_S (mixed-load duration, default 15)
"""
import asyncio
import os
import random
import sys
import time
import uuid
from collections import defaultdict

import httpx
import psycopg

CONSUMER_URL = os.environ.get("CONSUMER_URL", "http://localhost:9000")
QUOTA_URL = os.environ.get("QUOTA_URL", "http://localhost:8000")
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://quota:quota@localhost:5432/quota")
MIXED_S = float(os.environ.get("MIXED_S", "15"))
CONTAINERS, SCHEDULES = "feature1", "feature2"
LIMITS = {CONTAINERS: 500, SCHEDULES: 1000}


class Verify:
    def __init__(self, http: httpx.AsyncClient, quota: httpx.AsyncClient, db: psycopg.AsyncConnection):
        self.http, self.quota, self.db = http, quota, db
        self.rows: list[tuple[str, bool, str]] = []
        self.next_org = 0

    # ---- plumbing ------------------------------------------------------------------------

    async def provision(self, n: int) -> None:
        """Create n fresh orgs on the 'verify' plan (new ids every run: no cleanup needed)."""
        async with self.db.transaction():
            await self.db.execute("SELECT pg_advisory_xact_lock(7002)")
            await self.db.execute("INSERT INTO plans (plan_id) VALUES ('verify') ON CONFLICT DO NOTHING")
            for name, limit in LIMITS.items():
                await self.db.execute(
                    "INSERT INTO plan_limits (plan_id, feature_id, monthly_limit)"
                    " SELECT 'verify', feature_id, %s FROM features WHERE name = %s"
                    " ON CONFLICT (plan_id, feature_id) DO UPDATE SET monthly_limit = EXCLUDED.monthly_limit",
                    (limit, name))
            cur = await self.db.execute("SELECT greatest(1000000, coalesce(max(org_id), 0) + 1) FROM orgs")
            base = (await cur.fetchone())[0]
            async with self.db.cursor() as c:
                await c.executemany(
                    "INSERT INTO orgs (org_id, name, plan_id, anchor_day) VALUES (%s, %s, 'verify', 1)",
                    [(base + i, f"verify-{base + i}") for i in range(n)])
        self.next_org = base

    def org(self) -> int:
        self.next_org += 1
        return self.next_org - 1

    async def track(self, org: int, n: int, key: str | None = None, fail: bool = False) -> httpx.Response:
        return await self.http.post("/v1/containers", headers={"Idempotency-Key": key or uuid.uuid4().hex},
                                    json={"org_id": org, "containers": [f"C{i}" for i in range(n)], "fail": fail})

    async def search(self, org: int, key: str | None = None, fail: bool = False) -> httpx.Response:
        return await self.http.get("/v1/schedules", params={"org_id": org, "fail": fail},
                                   headers={"Idempotency-Key": key or uuid.uuid4().hex})

    async def usage(self, org: int, feature: str = CONTAINERS) -> dict:
        return (await self.quota.get(f"/v1/usage/{org}/{feature}")).json()

    async def ledger(self, org: int, feature: str = CONTAINERS) -> tuple[int, int]:
        cur = await self.db.execute(
            "SELECT coalesce(sum(units), 0), count(*) FROM demo_ledger WHERE org_id = %s AND feature = %s",
            (org, feature))
        total, count = await cur.fetchone()
        return int(total), int(count)

    async def granted(self, org: int, feature: str = CONTAINERS) -> int:
        cur = await self.db.execute(
            "SELECT coalesce(sum(q.granted), 0) FROM quota_periods q JOIN features f USING (feature_id)"
            " WHERE q.org_id = %s AND f.name = %s", (org, feature))
        return int((await cur.fetchone())[0])

    def check(self, name: str, ok: bool, detail: str) -> None:
        self.rows.append((name, ok, detail))
        print(f"  {'PASS' if ok else 'FAIL'}  {name}: {detail}", flush=True)

    @staticmethod
    def codes(rs: list[httpx.Response]) -> dict[int, int]:
        out: dict[int, int] = defaultdict(int)
        for r in rs:
            out[r.status_code] += 1
        return dict(sorted(out.items()))

    # ---- scenarios -----------------------------------------------------------------------

    async def exact_fill(self):
        """The brief's case: 500 left, work totalling exactly 500 arrives at once."""
        org = self.org()
        rs = await asyncio.gather(*(self.track(org, 10) for _ in range(50)))
        u, (led, _) = await self.usage(org), await self.ledger(org)
        self.check("exact fill: 50 concurrent batches of 10 against 500",
                   self.codes(rs) == {200: 50} and u["used"] == 500 and u["remaining"] == 0 and led == 500,
                   f"responses {self.codes(rs)}, used {u['used']}, remaining {u['remaining']}, ledger {led}")

    async def oversubscribe_units(self):
        """1,000 concurrent single-unit requests against 500: exactly 500 may succeed."""
        org = self.org()
        rs = await asyncio.gather(*(self.track(org, 1) for _ in range(1000)))
        u, (led, _) = await self.usage(org), await self.ledger(org)
        c = self.codes(rs)
        self.check("oversubscribed: 1,000 concurrent 1-unit requests against 500",
                   c == {200: 500, 429: 500} and u["used"] == 500 and u["remaining"] == 0 and led == 500,
                   f"responses {c}, used {u['used']}, remaining {u['remaining']}, ledger {led}")

    async def oversubscribe_batches(self):
        """300 concurrent batches of 1-20 against 500. Never over-served, and nothing rejected
        that would have fitted: every rejected batch is larger than what is finally left."""
        org = self.org()
        sizes = [random.randint(1, 20) for _ in range(300)]
        rs = await asyncio.gather(*(self.track(org, n) for n in sizes))
        u, (led, _) = await self.usage(org), await self.ledger(org)
        ok_units = sum(n for n, r in zip(sizes, rs) if r.status_code == 200)
        rejected = [(n, r.json().get("available")) for n, r in zip(sizes, rs) if r.status_code == 429]
        no_false_reject = all(n > u["remaining"] and (a is None or a < n) for n, a in rejected)
        self.check("oversubscribed: 300 concurrent batches of 1-20 units against 500",
                   ok_units == u["used"] == led <= 500 and u["remaining"] == 500 - u["used"] and no_false_reject,
                   f"requested {sum(sizes)}, accepted {ok_units}, used {u['used']}, ledger {led}, "
                   f"remaining {u['remaining']}, {len(rejected)} rejected (each larger than what was left)")

    async def batch_all_or_nothing(self):
        org = self.org()
        a = await self.track(org, 495)
        b = await self.track(org, 10)
        mid = await self.usage(org)
        c = await self.track(org, 5)
        d = await self.track(org, 1)
        u = await self.usage(org)
        ok = (a.status_code == 200 and b.status_code == 429 and b.json().get("available") == 5
              and mid["used"] == 495 and c.status_code == 200 and d.status_code == 429
              and u["used"] == 500 and u["remaining"] == 0)
        self.check("all-or-nothing: 495 ok, then 10 rejected with 5 left, then 5 ok",
                   ok, f"495->{a.status_code}, 10->{b.status_code} (available {b.json().get('available')}), "
                       f"used after reject {mid['used']}, 5->{c.status_code}, 1->{d.status_code}, final used {u['used']}")

    async def failure_releases(self):
        org = self.org()
        failed = await asyncio.gather(*(self.track(org, 25, fail=True) for _ in range(20)))
        after_fail = await self.usage(org)
        good = await asyncio.gather(*(self.track(org, 25) for _ in range(20)))
        u, (led, _) = await self.usage(org), await self.ledger(org)
        self.check("downstream failure releases: 20 failed batches of 25, then 20 good ones",
                   self.codes(failed) == {502: 20} and after_fail["used"] == 0 and after_fail["reserved"] == 0
                   and self.codes(good) == {200: 20} and u["used"] == 500 and led == 500,
                   f"failed {self.codes(failed)} -> used {after_fail['used']} reserved {after_fail['reserved']}; "
                   f"then {self.codes(good)} -> used {u['used']}, ledger {led}")

    async def retries(self):
        org = self.org()
        keys = [uuid.uuid4().hex for _ in range(50)]
        racing = await asyncio.gather(*(self.track(org, 5, key=k) for k in keys for _ in range(3)))
        later = await asyncio.gather(*(self.track(org, 5, key=k) for k in keys))
        u, (led, rows) = await self.usage(org), await self.ledger(org)
        firsts = sum(1 for r in racing if r.status_code == 200 and not r.json().get("replayed"))
        replays = all(r.status_code == 200 and r.json().get("replayed") for r in later)
        self.check("retries: 50 operations x 3 racing sends, then each sent again",
                   firsts == 50 and replays and u["used"] == 250 and led == 250 and rows == 50,
                   f"racing sends {self.codes(racing)} ({firsts} did the work), later resends all replayed: "
                   f"{replays}; used {u['used']} (expected 250), ledger rows {rows}")

        k = uuid.uuid4().hex
        first = await self.track(org, 5, key=k, fail=True)
        again = await self.track(org, 5, key=k)
        u2 = await self.usage(org)
        self.check("retry of a failed operation is not charged",
                   first.status_code == 502 and again.status_code == 409 and u2["used"] == 250,
                   f"first {first.status_code}, retry {again.status_code} ({again.json().get('detail')}), used {u2['used']}")

    async def mixed_load(self, n_orgs: int = 20, workers: int = 48):
        """Many clients, many orgs, both features, batches, failures and duplicate sends, for a
        while; then every (org, feature) must reconcile exactly."""
        orgs = [self.org() for _ in range(n_orgs)]
        seen: dict[tuple[int, str], int] = defaultdict(int)
        codes: dict[int, int] = defaultdict(int)
        ops = 0
        stop = time.perf_counter() + MIXED_S

        async def one():
            nonlocal ops
            org, key, fail = random.choice(orgs), uuid.uuid4().hex, random.random() < 0.05
            if random.random() < 0.7:
                feature, n = CONTAINERS, random.randint(1, 50)
                send = lambda: self.track(org, n, key=key, fail=fail)  # noqa: E731
            else:
                feature, n = SCHEDULES, 1
                send = lambda: self.search(org, key=key, fail=fail)  # noqa: E731
            rs = await asyncio.gather(*(send() for _ in range(2 if random.random() < 0.05 else 1)))
            for r in rs:
                codes[r.status_code] += 1
                if r.status_code == 200 and not r.json().get("replayed"):
                    seen[(org, feature)] += n
            ops += 1

        async def worker():
            while time.perf_counter() < stop:
                await one()

        t0 = time.perf_counter()
        await asyncio.gather(*(worker() for _ in range(workers)))
        elapsed = time.perf_counter() - t0
        await asyncio.sleep(0.5)

        mismatches, exhausted = [], 0
        for org in orgs:
            for feature, limit in LIMITS.items():
                u, (led, _), g = await self.usage(org, feature), await self.ledger(org, feature), await self.granted(org, feature)
                client = seen.get((org, feature), 0)
                if not (client == led == u["used"] and u["reserved"] == 0 and u["used"] <= g <= limit):
                    mismatches.append(f"org {org} {feature}: client {client} ledger {led} used {u['used']} "
                                      f"reserved {u['reserved']} granted {g} limit {limit}")
                exhausted += u["remaining"] < 50
        for m in mismatches:
            print(f"    {m}", flush=True)
        self.check(f"mixed load: {workers} clients, {n_orgs} orgs x 2 features, {MIXED_S:.0f} s",
                   not mismatches,
                   f"{ops} operations ({ops / elapsed:.0f}/s), responses {dict(sorted(codes.items()))}; "
                   f"{2 * n_orgs - len(mismatches)}/{2 * n_orgs} pairs reconcile (client = ledger = used, "
                   f"reserved 0, used <= granted <= limit); {exhausted} pairs ran (nearly) out")


async def main() -> int:
    limits = httpx.Limits(max_connections=300, max_keepalive_connections=300)
    async with httpx.AsyncClient(base_url=CONSUMER_URL, timeout=30, limits=limits) as http, \
            httpx.AsyncClient(base_url=QUOTA_URL, timeout=10) as quota:
        db = await psycopg.AsyncConnection.connect(DATABASE_URL, autocommit=True)
        v = Verify(http, quota, db)
        await v.provision(40)
        print(f"verify: orgs {v.next_org}..{v.next_org + 39} on plan 'verify' "
              f"(limits: {CONTAINERS}={LIMITS[CONTAINERS]}, {SCHEDULES}={LIMITS[SCHEDULES]})\n", flush=True)
        for scenario in (v.exact_fill, v.oversubscribe_units, v.oversubscribe_batches,
                         v.batch_all_or_nothing, v.failure_releases, v.retries, v.mixed_load):
            await scenario()
        await db.close()
    failed = [r for r in v.rows if not r[1]]
    print(f"\nverify: {len(v.rows) - len(failed)}/{len(v.rows)} checks passed", flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

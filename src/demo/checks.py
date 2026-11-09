"""Correctness checks, run end to end through the live stack:
client -> dummy-tracking-feature -> quota service -> Redis / Postgres.

Every test creates fresh orgs (ids from 1,000,000 up) on a plan with the limits it needs, so the
background load never touches them and runs never interfere. Outcomes are checked three ways
where possible: what the client saw, dummy-tracking-feature's ledger of completed operations
(demo_ledger), and the quota service's usage endpoint; plus used <= Postgres granted <= limit.

Run by load-tester-client (all tests at startup, or one test with your own numbers):
    python -m demo.load_tester_client check            # all tests
    python -m demo.load_tester_client check 2 --requests 2000 --limit 300
"""
import asyncio
import os
import random
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Awaitable, Callable

import aiohttp
import psycopg

CONSUMER_URL = os.environ.get("CONSUMER_URL", "http://localhost:9000")
QUOTA_URL = os.environ.get("QUOTA_URL", "http://localhost:8000")
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://quota:quota@localhost:5432/quota")
RESERVE_TIMEOUT_S = float(os.environ.get("RESERVE_TIMEOUT_S", "30"))  # must match the quota service
CONTAINERS, SCHEDULES = "feature1", "feature2"   # what dummy-tracking-feature meters


# ---- test registry ---------------------------------------------------------------------------

@dataclass
class Result:
    ok: bool = True
    lines: list[tuple[str, str]] = field(default_factory=list)

    def add(self, label: str, value: str) -> "Result":
        self.lines.append((label, value))
        return self

    def expect(self, ok: bool) -> "Result":
        self.ok = self.ok and ok
        return self


@dataclass
class Test:
    num: int
    title: Callable[[dict], str]
    fn: Callable[..., Awaitable[Result]]
    params: dict[str, tuple[int, str]]   # name -> (default, help)


TESTS: dict[int, Test] = {}


def test(num: int, title: Callable[[dict], str], **params: tuple[int, str]):
    def register(fn):
        TESTS[num] = Test(num, title, fn, params)
        return fn
    return register


# ---- plumbing --------------------------------------------------------------------------------

@dataclass
class Resp:
    status_code: int
    body: dict

    def json(self) -> dict:
        return self.body


class Ctx:
    def __init__(self, http: aiohttp.ClientSession, db: psycopg.AsyncConnection):
        self.http, self.db = http, db
        self.resends = 0   # requests re-sent with the same key after a lost reply

    async def call(self, method: str, url: str, **kw) -> Resp:
        async with self.http.request(method, url, **kw) as r:
            return Resp(r.status, await r.json(content_type=None))

    async def qpost(self, path: str, body: dict) -> Resp:
        """Call the quota service directly (as a client that bypasses dummy-tracking-feature)."""
        return await self.call("POST", QUOTA_URL + path, json=body)

    async def orgs(self, n: int, containers_limit: int, searches_limit: int = 1000) -> list[int]:
        """n fresh orgs on a plan with these limits (plan 'verify-<containers>-<searches>')."""
        plan = f"verify-{containers_limit}-{searches_limit}"
        async with self.db.transaction():
            await self.db.execute("SELECT pg_advisory_xact_lock(7002)")
            await self.db.execute("INSERT INTO plans (plan_id) VALUES (%s) ON CONFLICT DO NOTHING", (plan,))
            for name, limit in ((CONTAINERS, containers_limit), (SCHEDULES, searches_limit)):
                await self.db.execute(
                    "INSERT INTO plan_limits (plan_id, feature_id, monthly_limit)"
                    " SELECT %s, feature_id, %s FROM features WHERE name = %s"
                    " ON CONFLICT (plan_id, feature_id) DO NOTHING", (plan, limit, name))
            cur = await self.db.execute("SELECT greatest(1000000, coalesce(max(org_id), 0) + 1) FROM orgs")
            base = (await cur.fetchone())[0]
            async with self.db.cursor() as c:
                await c.executemany(
                    "INSERT INTO orgs (org_id, name, plan_id, anchor_day) VALUES (%s, %s, %s, 1)",
                    [(base + i, f"verify-{base + i}", plan) for i in range(n)])
        return list(range(base, base + n))

    async def send(self, method: str, url: str, key: str | None, **kw) -> Resp:
        """Send like a well-behaved client: if the reply is lost (connection dropped), send again
        with the SAME Idempotency-Key. The first attempt may have done the work; the retry then
        gets the original outcome back (200 replayed) instead of doing it twice. A retry that
        finds the first attempt still running (409 in progress) waits and asks again."""
        headers = {"Idempotency-Key": key or uuid.uuid4().hex}
        lost = False
        for attempt in range(8):
            try:
                r = await self.call(method, CONSUMER_URL + url, headers=headers, **kw)
            except (aiohttp.ClientError, asyncio.TimeoutError):
                self.resends += 1
                lost = True
                await asyncio.sleep(0.05 * (attempt + 1))
                continue
            if lost and r.status_code == 409 and "in progress" in r.json().get("detail", ""):
                await asyncio.sleep(0.05 * (attempt + 1))
                continue
            return r
        raise RuntimeError(f"{method} {url}: no answer after 8 attempts")

    async def track(self, org: int, n: int, key: str | None = None, fail: bool = False) -> Resp:
        return await self.send("POST", "/v1/containers", key,
                               json={"org_id": org, "containers": [f"C{i}" for i in range(n)], "fail": fail})

    async def search(self, org: int, key: str | None = None, fail: bool = False) -> Resp:
        return await self.send("GET", "/v1/schedules", key,
                               params={"org_id": org, "fail": "true" if fail else "false"})

    async def usage(self, org: int, feature: str = CONTAINERS) -> dict:
        return (await self.call("GET", f"{QUOTA_URL}/v1/usage/{org}/{feature}")).json()

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


def codes(rs: list[Resp]) -> str:
    out: dict[int, int] = defaultdict(int)
    for r in rs:
        out[r.status_code] += 1
    return " | ".join(f"{c} x {n}" for c, n in sorted(out.items()))


def usage_line(u: dict) -> str:
    return f"used {u['used']} | reserved {u['reserved']} | remaining {u['remaining']} (limit {u['limit']})"


# ---- the tests -------------------------------------------------------------------------------

@test(1, lambda p: f"Exact fill: {p['batches']} concurrent batches of {p['size']} units against a limit "
                   f"of {p['batches'] * p['size']} (the brief's case: work totalling exactly what is left)",
      batches=(50, "number of concurrent batches"), size=(10, "units per batch (limit = batches x size)"))
async def exact_fill(c: Ctx, batches: int, size: int) -> Result:
    limit = batches * size
    [org] = await c.orgs(1, limit)
    rs = await asyncio.gather(*(c.track(org, size) for _ in range(batches)))
    u, (led, _) = await c.usage(org), await c.ledger(org)
    return (Result().add("responses", codes(rs)).add("usage", usage_line(u)).add("ledger", f"{led} units")
            .expect(all(r.status_code == 200 for r in rs) and u["used"] == limit
                    and u["remaining"] == 0 and led == limit))


@test(2, lambda p: f"Oversubscribed: {p['requests']} concurrent 1-unit requests against a limit of "
                   f"{p['limit']} (exactly {min(p['requests'], p['limit'])} may succeed)",
      requests=(1000, "concurrent 1-unit requests"), limit=(500, "the org's limit"))
async def oversubscribe_units(c: Ctx, requests: int, limit: int) -> Result:
    [org] = await c.orgs(1, limit)
    rs = await asyncio.gather(*(c.track(org, 1) for _ in range(requests)))
    u, (led, _) = await c.usage(org), await c.ledger(org)
    want = min(requests, limit)
    ok_n = sum(r.status_code == 200 for r in rs)
    return (Result().add("responses", codes(rs)).add("usage", usage_line(u)).add("ledger", f"{led} units")
            .expect(ok_n == want and sum(r.status_code == 429 for r in rs) == requests - want
                    and u["used"] == want and led == want and u["remaining"] == limit - want))


@test(3, lambda p: f"Oversubscribed with mixed batches: {p['requests']} concurrent batches of 1-{p['max_units']} "
                   f"units against a limit of {p['limit']}: never over-served, and nothing rejected that "
                   f"would have fitted",
      requests=(300, "concurrent batches"), max_units=(20, "largest batch"), limit=(500, "the org's limit"))
async def oversubscribe_batches(c: Ctx, requests: int, max_units: int, limit: int) -> Result:
    [org] = await c.orgs(1, limit)
    sizes = [random.randint(1, max_units) for _ in range(requests)]
    rs = await asyncio.gather(*(c.track(org, n) for n in sizes))
    u, (led, _) = await c.usage(org), await c.ledger(org)
    accepted = sum(n for n, r in zip(sizes, rs) if r.status_code == 200)
    rejected = [(n, r.json().get("available")) for n, r in zip(sizes, rs) if r.status_code == 429]
    # With no failures, what is left only shrinks; so a fair rejection is larger than the final remainder
    fair = all(n > u["remaining"] and (a is None or a < n) for n, a in rejected)
    return (Result().add("requested", f"{sum(sizes)} units in {requests} batches")
            .add("responses", codes(rs))
            .add("accepted", f"{accepted} units | ledger {led} units")
            .add("usage", usage_line(u))
            .add("rejections", f"{len(rejected)}, each larger than what was left: {fair}")
            .expect(accepted == u["used"] == led <= limit and u["remaining"] == limit - u["used"] and fair))


@test(4, lambda p: f"All-or-nothing batches: limit {p['limit']}; take {p['limit'] - 5}, then ask for 10 "
                   f"(rejected whole, nothing taken), then 5 (fits), then 1 (nothing left)",
      limit=(500, "the org's limit"))
async def all_or_nothing(c: Ctx, limit: int) -> Result:
    [org] = await c.orgs(1, limit)
    a = await c.track(org, limit - 5)
    b = await c.track(org, 10)
    mid = await c.usage(org)
    d = await c.track(org, 5)
    e = await c.track(org, 1)
    u = await c.usage(org)
    avail = b.json().get("available")
    return (Result().add("requests", f"{limit - 5} -> {a.status_code} | 10 -> {b.status_code} (available {avail}) "
                                     f"| 5 -> {d.status_code} | 1 -> {e.status_code}")
            .add("after the rejected 10", usage_line(mid))
            .add("final usage", usage_line(u))
            .expect(a.status_code == 200 and b.status_code == 429 and avail == 5 and mid["used"] == limit - 5
                    and d.status_code == 200 and e.status_code == 429 and u["used"] == limit and u["remaining"] == 0))


@test(5, lambda p: f"Downstream failure releases quota: {p['requests']} concurrent batches of {p['size']} whose "
                   f"work fails (dummy-tracking-feature answers 502 and releases), then the same again succeeding",
      requests=(20, "batches"), size=(25, "units per batch (limit = requests x size)"))
async def failure_releases(c: Ctx, requests: int, size: int) -> Result:
    limit = requests * size
    [org] = await c.orgs(1, limit)
    failed = await asyncio.gather(*(c.track(org, size, fail=True) for _ in range(requests)))
    after = await c.usage(org)
    good = await asyncio.gather(*(c.track(org, size) for _ in range(requests)))
    u, (led, _) = await c.usage(org), await c.ledger(org)
    return (Result().add("failing work", codes(failed))
            .add("usage after failures", usage_line(after))
            .add("succeeding work", codes(good))
            .add("final usage", f"{usage_line(u)} | ledger {led} units")
            .expect(all(r.status_code == 502 for r in failed) and after["used"] == 0 and after["reserved"] == 0
                    and all(r.status_code == 200 for r in good) and u["used"] == limit and led == limit))


@test(6, lambda p: f"Retries of a SUCCESSFUL operation are not charged again: {p['operations']} operations of "
                   f"{p['size']} units, each sent {p['copies']} times at once with the same Idempotency-Key, "
                   f"then each sent once more (expect '200 replayed')",
      operations=(50, "distinct operations"), copies=(3, "simultaneous sends per operation"),
      size=(5, "units per operation"))
async def retries(c: Ctx, operations: int, copies: int, size: int) -> Result:
    [org] = await c.orgs(1, operations * size * 2)
    keys = [uuid.uuid4().hex for _ in range(operations)]
    racing = await asyncio.gather(*(c.track(org, size, key=k) for k in keys for _ in range(copies)))
    later = await asyncio.gather(*(c.track(org, size, key=k) for k in keys))
    u, (led, rows) = await c.usage(org), await c.ledger(org)
    done = len({k for k, r in zip((k for k in keys for _ in range(copies)), racing) if r.status_code == 200})
    replayed = sum(r.status_code == 200 and r.json().get("replayed") is True for r in later)
    want = operations * size
    return (Result().add("racing sends", f"{codes(racing)} (409 = 'same request still in progress')")
            .add("operations done", f"{done}/{operations}")
            .add("later resends", f"{replayed}/{operations} answered '200 replayed'")
            .add("charged", f"used {u['used']} (expected {want}, not {want * copies}) | ledger {rows} operations")
            .expect(done == operations and replayed == operations and u["used"] == want
                    and led == want and rows == operations))


@test(7, lambda p: "A retry of a FAILED operation is not charged and not silently re-run: the work fails "
                   "(502, units released), then the same Idempotency-Key is sent again without failing "
                   "(expect 409: that key's outcome is final; use a new key to try again)",
      size=(5, "units"))
async def retry_failed(c: Ctx, size: int) -> Result:
    [org] = await c.orgs(1, 500)
    k = uuid.uuid4().hex
    first = await c.track(org, size, key=k, fail=True)
    again = await c.track(org, size, key=k)
    u = await c.usage(org)
    return (Result().add("first attempt", f"{first.status_code} ({first.json().get('detail')})")
            .add("retry, same key", f"{again.status_code} ({again.json().get('detail')})")
            .add("usage", usage_line(u))
            .expect(first.status_code == 502 and again.status_code == 409 and u["used"] == 0 and u["reserved"] == 0))


@test(8, lambda p: f"Crashed client: a reservation of the whole limit ({p['limit']}) is made and never committed "
                   f"or released. Requests are rejected while it is held, then succeed once its "
                   f"{RESERVE_TIMEOUT_S:.0f} s timeout passes and the units return to the pool; the crashed "
                   f"client's late commit is refused and not charged",
      limit=(500, "the org's limit"), size=(10, "units in the requests made around it"))
async def crashed_client(c: Ctx, limit: int, size: int) -> Result:
    [org] = await c.orgs(1, limit)
    rid = f"crashed-{uuid.uuid4().hex}"
    held = await c.qpost("/v1/reserve", {"org_id": org, "feature": CONTAINERS,
                                         "units": limit, "request_id": rid})
    t0 = time.monotonic()
    during = await c.track(org, size)
    u_during = await c.usage(org)
    await asyncio.sleep(max(0.0, RESERVE_TIMEOUT_S + 1 - (time.monotonic() - t0)))
    after = await c.track(org, size)
    u_after = await c.usage(org)
    late = await c.qpost("/v1/commit", {"org_id": org, "request_id": rid})
    u_end = await c.usage(org)
    return (Result().add("crashed client", f"reserved {limit} units directly -> {held.status_code}, then nothing")
            .add("while held", f"request for {size} -> {during.status_code} | {usage_line(u_during)}")
            .add(f"after {RESERVE_TIMEOUT_S + 1:.0f} s", f"request for {size} -> {after.status_code} | {usage_line(u_after)}")
            .add("late commit", f"{late.status_code} ({late.json().get('detail')}) | {usage_line(u_end)}")
            .expect(held.status_code == 200 and during.status_code == 429 and u_during["reserved"] == limit
                    and after.status_code == 200 and u_after["used"] == size and u_after["reserved"] == 0
                    and late.status_code == 409 and u_end["used"] == size))


@test(9, lambda p: f"Mixed load: {p['clients']} concurrent clients for {p['seconds']} s over {p['orgs']} orgs x 2 "
                   f"features (limits {p['limit']} / 1000), batches of 1-50, 5% failing work, 5% duplicate "
                   f"sends; afterwards every (org, feature) must reconcile exactly",
      seconds=(15, "duration"), clients=(48, "concurrent clients"), orgs=(20, "orgs"),
      limit=(500, "container-tracking limit per org"))
async def mixed_load(c: Ctx, seconds: int, clients: int, orgs: int, limit: int) -> Result:
    limits = {CONTAINERS: limit, SCHEDULES: 1000}
    ids = await c.orgs(orgs, limit)
    seen: dict[tuple[int, str], int] = defaultdict(int)
    counted: set[str] = set()   # each operation (key) counts once, however many replies say 200
    all_codes: dict[int, int] = defaultdict(int)
    ops = 0
    stop = time.perf_counter() + seconds

    async def one():
        nonlocal ops
        org, key, fail = random.choice(ids), uuid.uuid4().hex, random.random() < 0.05
        if random.random() < 0.7:
            feature, n = CONTAINERS, random.randint(1, 50)
            send = lambda: c.track(org, n, key=key, fail=fail)  # noqa: E731
        else:
            feature, n = SCHEDULES, 1
            send = lambda: c.search(org, key=key, fail=fail)  # noqa: E731
        for r in await asyncio.gather(*(send() for _ in range(2 if random.random() < 0.05 else 1))):
            all_codes[r.status_code] += 1
            if r.status_code == 200 and key not in counted:
                counted.add(key)
                seen[(org, feature)] += n
        ops += 1

    async def client():
        while time.perf_counter() < stop:
            await one()

    t0 = time.perf_counter()
    await asyncio.gather(*(client() for _ in range(clients)))
    elapsed = time.perf_counter() - t0
    await asyncio.sleep(0.5)

    bad, out_of_quota = [], 0
    for org in ids:
        for feature, lim in limits.items():
            u, (led, _), g = await c.usage(org, feature), await c.ledger(org, feature), await c.granted(org, feature)
            client_total = seen.get((org, feature), 0)
            if not (client_total == led == u["used"] and u["reserved"] == 0 and u["used"] <= g <= lim):
                bad.append(f"org {org} {feature}: client {client_total} | ledger {led} | used {u['used']} | "
                           f"reserved {u['reserved']} | granted {g} | limit {lim}")
            out_of_quota += u["remaining"] < 50
    res = (Result().add("operations", f"{ops} in {elapsed:.1f} s ({ops / elapsed:.0f}/s)")
           .add("responses", " | ".join(f"{k} x {v}" for k, v in sorted(all_codes.items())))
           .add("reconciled", f"{2 * orgs - len(bad)}/{2 * orgs} (org, feature) pairs: client = ledger = used, "
                              f"reserved 0, used <= granted <= limit")
           .add("ran (nearly) out", f"{out_of_quota} pairs"))
    for b in bad:
        res.add("MISMATCH", b)
    return res.expect(not bad)


# ---- runner ----------------------------------------------------------------------------------

RULE = "=" * 100

GLOSSARY = f"""GLOSSARY
  limit            units this org may use per month for this feature (set by its plan)
  used             units of operations that finished and were committed, i.e. charged
  reserved         units held right now by operations still in progress (reserved, not yet committed
                   or released)
  remaining        limit - used - reserved: the most a new request can get right now
  granted          units Postgres has handed to Redis so far (quota moves in chunks);
                   always used + reserved <= granted <= limit
  ledger           dummy-tracking-feature's own list of operations it completed and committed. It is
                   kept apart from the quota service, so "ledger = used" shows nothing was lost or
                   charged twice
  Idempotency-Key  one id per operation, reused on every retry of that operation
  replayed         a retry of an operation that already succeeded: the original 200 comes back and
                   nothing is charged again
  released         the work failed, so the operation's reserved units went back
  expired          a reservation neither committed nor released within the reservation timeout
                   ({RESERVE_TIMEOUT_S:.0f} s here): its units are reclaimed
  quota overhead   per operation, the time dummy-tracking-feature spent waiting on the quota service
                   (reserve + commit or release)
  responses        200 done | 429 not enough quota (a batch is all or nothing) |
                   409 duplicate, or this key's outcome is already final |
                   502 the work failed downstream (units released) | 503 quota service unavailable"""

LATENCY_NOTE = ("Latency numbers here are only indicative: they vary a lot with the cores given to the "
                "Docker VM, other load on the machine, and everything sharing those cores (quota service, "
                "dummy-tracking-feature, load tester, Postgres, Redis).")

NEXT = """WHAT NEXT   from the repo folder, in another terminal or after stopping this with Ctrl-C (each command
            starts whatever it needs; when done, `docker compose down -v` stops everything and deletes the data)
  all checks again             docker compose run --rm load-tester-client check
  one check, your own numbers  docker compose run --rm load-tester-client check 2 --requests 2000 --limit 300
  a load run of your own       docker compose run --rm load-tester-client load --rate 1000 --orgs 500 --seconds 60
  commands, every check's parameters, and the glossary:
                               docker compose run --rm load-tester-client help"""


def print_header() -> None:
    print(f"\n{RULE}\nCORRECTNESS CHECKS   client -> dummy-tracking-feature -> quota service -> Redis/Postgres\n"
          f"  reservation timeout: {RESERVE_TIMEOUT_S:.0f} s (RESERVE_TIMEOUT_S; set to 10 s for this demo in "
          f"docker-compose.yml, service default 30 s)\n"
          f"  each test uses fresh orgs, so tests never affect each other or the background load\n"
          f"{RULE}\n{GLOSSARY}\n  (all of this, plus every command and check parameter: "
          f"docker compose run --rm load-tester-client help)\n{RULE}", flush=True)


def print_result(t: Test, params: dict, res: Result, elapsed: float) -> None:
    print(f"\nTEST {t.num}  {t.title(params)}", flush=True)
    width = max(len(label) for label, _ in res.lines) if res.lines else 0
    for label, value in res.lines:
        print(f"        {label.ljust(width)}   {value}", flush=True)
    print(f"        {'PASS' if res.ok else 'FAIL'}  ({elapsed:.1f} s)", flush=True)


def print_footer(results: list[tuple[int, bool]], resends: int) -> None:
    passed = sum(ok for _, ok in results)
    failed = [n for n, ok in results if not ok]
    note = f"; {resends} lost replies were re-sent with the same key" if resends else ""
    print(f"\n{RULE}\nRESULT  {passed}/{len(results)} passed"
          + (f"   FAILED: {', '.join(f'test {n}' for n in failed)}" if failed else "") + note
          + f"\n{RULE}\n{NEXT}\n{RULE}\n", flush=True)


async def run(only: int | None = None, overrides: dict | None = None) -> bool:
    overrides = overrides or {}
    conn = aiohttp.TCPConnector(limit=300, keepalive_timeout=10)
    async with aiohttp.ClientSession(connector=conn, timeout=aiohttp.ClientTimeout(total=30)) as http:
        db = await psycopg.AsyncConnection.connect(DATABASE_URL, autocommit=True)
        ctx = Ctx(http, db)
        print_header()
        results = []
        for t in ([TESTS[only]] if only else TESTS.values()):
            params = {k: overrides.get(k, default) for k, (default, _) in t.params.items()}
            t0 = time.perf_counter()
            try:
                res = await t.fn(ctx, **params)
            except Exception as e:  # report and carry on with the other tests
                res = Result(ok=False).add("error", repr(e))
            print_result(t, params, res, time.perf_counter() - t0)
            results.append((t.num, res.ok))
        await db.close()
        print_footer(results, ctx.resends)
    return all(ok for _, ok in results)

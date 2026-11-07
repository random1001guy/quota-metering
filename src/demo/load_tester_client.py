"""load-tester-client: keeps dummy-tracking-feature (and through it the quota service) busy, printing live stats.

Traffic follows the brief: bursty per org. Bursts arrive at random; each burst is one org firing
5-40 operations within ~1 s, then going quiet. 80% of operations track containers (batches of
1-100, so 1-100 units), 20% are schedule searches (1 unit). A share of operations fail
downstream (released), and a share are sent twice with the same Idempotency-Key (retries).
Seeded limits are small enough that busy orgs run out, so 429s appear over time, as they should.

Every REPORT_S seconds it prints one line:
    ops/s, outcomes (200 / 429 / 502 failed+released / 409 duplicate / 503 / errors),
    quota overhead p50/p99 (time dummy-tracking-feature spent waiting on the quota service, from the
    X-Quota-Overhead-Ms header), and end-to-end p99.
Every CHECK_S seconds it samples orgs it has touched and checks, via the usage endpoint, that
used + reserved never exceeds the limit.

Env: CONSUMER_URL, QUOTA_URL, RATE (ops/s, default 200), ORGS (orgs 1..ORGS, default 5000),
     FAIL_RATE (0.05), DUP_RATE (0.02), DURATION_S (0 = run forever), REPORT_S (5), CHECK_S (30)
"""
import asyncio
import os
import random
import time
import uuid

import httpx

CONSUMER_URL = os.environ.get("CONSUMER_URL", "http://localhost:9000")
QUOTA_URL = os.environ.get("QUOTA_URL", "http://localhost:8000")
RATE = float(os.environ.get("RATE", "200"))
ORGS = int(os.environ.get("ORGS", "5000"))
FAIL_RATE = float(os.environ.get("FAIL_RATE", "0.05"))
DUP_RATE = float(os.environ.get("DUP_RATE", "0.02"))
DURATION_S = float(os.environ.get("DURATION_S", "0"))
REPORT_S = float(os.environ.get("REPORT_S", "5"))
CHECK_S = float(os.environ.get("CHECK_S", "30"))
MAX_INFLIGHT = 2000
BURST = (5, 40)           # operations per burst
FEATURES = {"/v1/containers": "feature1", "/v1/schedules": "feature2"}


def pct(xs: list[float], p: float) -> str:
    if not xs:
        return "-"
    xs = sorted(xs)
    return f"{xs[min(len(xs) - 1, int(len(xs) * p / 100))]:.1f}"


class Stats:
    def __init__(self):
        self.reset()

    def reset(self):
        self.codes: dict[str, int] = {}
        self.overhead: list[float] = []
        self.e2e: list[float] = []
        self.sent = 0

    def line(self, window_s: float) -> str:
        c = self.codes
        return (f"{self.sent / window_s:6.0f} ops/s | ok {c.get('200', 0):5} | 429 {c.get('429', 0):4}"
                f" | failed+released {c.get('502', 0):4} | dup {c.get('409', 0):3}"
                f" | 503 {c.get('503', 0):3} | err {sum(v for k, v in c.items() if k not in ('200', '429', '502', '409', '503')):3}"
                f" | quota overhead p50 {pct(self.overhead, 50)} p99 {pct(self.overhead, 99)} ms"
                f" | end-to-end p99 {pct(self.e2e, 99)} ms")


async def one_op(http: httpx.AsyncClient, st: Stats, org: int, inflight: list[int]) -> None:
    key = uuid.uuid4().hex
    fail = random.random() < FAIL_RATE
    if random.random() < 0.8:
        n = random.randint(1, 100) if random.random() < 0.5 else random.randint(1, 5)
        req = dict(method="POST", url="/v1/containers",
                   json={"org_id": org, "containers": [f"C{i}" for i in range(n)], "fail": fail})
    else:
        req = dict(method="GET", url="/v1/schedules", params={"org_id": org, "fail": fail})
    sends = 2 if random.random() < DUP_RATE else 1   # a retry racing the original

    async def send():
        t = time.perf_counter()
        try:
            r = await http.request(**req, headers={"Idempotency-Key": key})
            code = str(r.status_code)
            if "X-Quota-Overhead-Ms" in r.headers:
                st.overhead.append(float(r.headers["X-Quota-Overhead-Ms"]))
        except httpx.HTTPError:
            code = "transport"
        st.e2e.append((time.perf_counter() - t) * 1000)
        st.codes[code] = st.codes.get(code, 0) + 1

    try:
        await asyncio.gather(*(send() for _ in range(sends)))
    finally:
        inflight[0] -= 1


async def burst(http, st, inflight, touched):
    org = random.randint(1, ORGS)
    touched.append(org)
    k = random.randint(*BURST)
    times = sorted(random.uniform(0, 1.0) for _ in range(k))
    start = time.perf_counter()
    tasks = []
    for t in times:
        await asyncio.sleep(max(0.0, start + t - time.perf_counter()))
        if inflight[0] >= MAX_INFLIGHT:
            st.codes["shed"] = st.codes.get("shed", 0) + 1
            continue
        inflight[0] += 1
        st.sent += 1
        tasks.append(asyncio.create_task(one_op(http, st, org, inflight)))
    await asyncio.gather(*tasks)


async def limit_check(quota: httpx.AsyncClient, touched: list[int]) -> str:
    sample = random.sample(touched, min(25, len(touched)))
    ok = bad = 0
    for org in sample:
        for feature in FEATURES.values():
            u = (await quota.get(f"/v1/usage/{org}/{feature}")).json()
            if u["used"] + u["reserved"] <= u["limit"] and u["remaining"] >= 0:
                ok += 1
            else:
                bad += 1
                print(f"  !! org {org} {feature}: {u}", flush=True)
    return f"limit check: {ok}/{ok + bad} (org, feature) pairs within limit" + ("" if not bad else "  <-- FAIL")


async def reporter(st: Stats) -> None:
    while True:
        t = time.perf_counter()
        await asyncio.sleep(REPORT_S)
        print(f"[load-tester-client] {time.strftime('%H:%M:%S')} {st.line(time.perf_counter() - t)}", flush=True)
        st.reset()


async def checker(quota: httpx.AsyncClient, touched: list[int]) -> None:
    while True:
        await asyncio.sleep(CHECK_S)
        if touched:
            print(f"[load-tester-client] {time.strftime('%H:%M:%S')} {await limit_check(quota, touched)}", flush=True)
            del touched[:-2000]


async def main() -> None:
    limits = httpx.Limits(max_connections=400, max_keepalive_connections=400)
    async with httpx.AsyncClient(base_url=CONSUMER_URL, timeout=10, limits=limits) as http, \
            httpx.AsyncClient(base_url=QUOTA_URL, timeout=10) as quota:
        print(f"[load-tester-client] driving {CONSUMER_URL} at ~{RATE:.0f} ops/s over {ORGS} orgs "
              f"(fail {FAIL_RATE:.0%}, duplicate sends {DUP_RATE:.0%})", flush=True)
        st, inflight, touched = Stats(), [0], []
        side = [asyncio.create_task(reporter(st)), asyncio.create_task(checker(quota, touched))]
        bursts: set[asyncio.Task] = set()
        mean_burst = sum(BURST) / 2
        t0 = time.perf_counter()
        while not DURATION_S or time.perf_counter() - t0 < DURATION_S:
            await asyncio.sleep(random.expovariate(RATE / mean_burst))
            task = asyncio.create_task(burst(http, st, inflight, touched))
            bursts.add(task)
            task.add_done_callback(bursts.discard)
        await asyncio.gather(*bursts)
        for t in side:
            t.cancel()


if __name__ == "__main__":
    asyncio.run(main())

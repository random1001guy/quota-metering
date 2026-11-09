"""load-tester-client: runs the correctness checks, then keeps dummy-tracking-feature (and through
it the quota service) under sustained load, printing live stats.

    python -m demo.load_tester_client                 # checks (all, default numbers), then load forever
    python -m demo.load_tester_client check           # all checks, then exit (exit code 1 on failure)
    python -m demo.load_tester_client check 3 --requests 1000 --max-units 50 --limit 2000
    python -m demo.load_tester_client help            # commands, every check's parameters, glossary
    python -m demo.load_tester_client load --rate 1000 --orgs 500 --seconds 60   # load only

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

Env: CONSUMER_URL, QUOTA_URL, RATE (ops/s, default 2000), ORGS (orgs 1..ORGS, default 1000),
     FAIL_RATE (0.05), DUP_RATE (0.02), DURATION_S (0 = run forever), REPORT_S (5), CHECK_S (30)
"""
import argparse
import asyncio
import os
import sys
import random
import time
import uuid

import aiohttp

CONSUMER_URL = os.environ.get("CONSUMER_URL", "http://localhost:9000")
QUOTA_URL = os.environ.get("QUOTA_URL", "http://localhost:8000")
RATE = float(os.environ.get("RATE", "2000"))
ORGS = int(os.environ.get("ORGS", "1000"))
FAIL_RATE = float(os.environ.get("FAIL_RATE", "0.05"))
DUP_RATE = float(os.environ.get("DUP_RATE", "0.02"))
DURATION_S = float(os.environ.get("DURATION_S", "0"))
REPORT_S = float(os.environ.get("REPORT_S", "5"))
CHECK_S = float(os.environ.get("CHECK_S", "30"))
MAX_INFLIGHT = 2000
BURST = (5, 40)           # operations per burst
FEATURES = {"/v1/containers": "feature1", "/v1/schedules": "feature2"}
CONTAINER_IDS = [f"C{i}" for i in range(100)]


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


async def one_op(http: aiohttp.ClientSession, st: Stats, org: int, inflight: list[int]) -> None:
    key = uuid.uuid4().hex
    fail = random.random() < FAIL_RATE
    if random.random() < 0.8:
        n = random.randint(1, 100) if random.random() < 0.5 else random.randint(1, 5)
        req = dict(method="POST", url=f"{CONSUMER_URL}/v1/containers",
                   json={"org_id": org, "containers": CONTAINER_IDS[:n], "fail": fail})
    else:
        req = dict(method="GET", url=f"{CONSUMER_URL}/v1/schedules",
                   params={"org_id": org, "fail": "true" if fail else "false"})
    sends = 2 if random.random() < DUP_RATE else 1   # a retry racing the original

    async def send():
        t = time.perf_counter()
        try:
            async with http.request(**req, headers={"Idempotency-Key": key}) as r:
                await r.read()
                code = str(r.status)
                if "X-Quota-Overhead-Ms" in r.headers:
                    st.overhead.append(float(r.headers["X-Quota-Overhead-Ms"]))
        except (aiohttp.ClientError, asyncio.TimeoutError):
            code = "transport"
        st.e2e.append((time.perf_counter() - t) * 1000)
        st.codes[code] = st.codes.get(code, 0) + 1

    try:
        await asyncio.gather(*(send() for _ in range(sends)))
    finally:
        inflight[0] -= 1


async def burst(http, st, inflight, touched):
    org = random.randint(1, st.orgs)
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


async def limit_check(http: aiohttp.ClientSession, touched: list[int]) -> str:
    sample = random.sample(touched, min(25, len(touched)))
    ok = bad = 0
    for org in sample:
        for feature in FEATURES.values():
            async with http.get(f"{QUOTA_URL}/v1/usage/{org}/{feature}") as r:
                u = await r.json()
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


async def checker(http: aiohttp.ClientSession, touched: list[int]) -> None:
    while True:
        await asyncio.sleep(CHECK_S)
        if touched:
            print(f"[load-tester-client] {time.strftime('%H:%M:%S')} {await limit_check(http, touched)}", flush=True)
            del touched[:-2000]


async def load(rate: float, orgs: int, seconds: float) -> None:
    # aiohttp, not httpx, here: at ~2k ops/s httpx's pool alone saturated a core. Idle connections
    # are dropped after 10 s, before the servers' 30 s keep-alive timeout.
    conn = aiohttp.TCPConnector(limit=400, keepalive_timeout=10)
    async with aiohttp.ClientSession(connector=conn, timeout=aiohttp.ClientTimeout(total=10)) as http:
        from demo.checks import LATENCY_NOTE
        print(f"[load-tester-client] SUSTAINED LOAD: ~{rate:.0f} ops/s on dummy-tracking-feature over orgs "
              f"1..{orgs} ({'for %.0f s' % seconds if seconds else 'until stopped'}; bursty per org, "
              f"{FAIL_RATE:.0%} failing work, {DUP_RATE:.0%} duplicate sends). A line every {REPORT_S:.0f} s; "
              f"a limit check every {CHECK_S:.0f} s.\n[load-tester-client] {LATENCY_NOTE}", flush=True)
        st, inflight, touched = Stats(), [0], []
        st.orgs = orgs
        side = [asyncio.create_task(reporter(st)), asyncio.create_task(checker(http, touched))]
        bursts: set[asyncio.Task] = set()
        mean_burst = sum(BURST) / 2
        t0 = time.perf_counter()
        while not seconds or time.perf_counter() - t0 < seconds:
            await asyncio.sleep(random.expovariate(rate / mean_burst))
            task = asyncio.create_task(burst(http, st, inflight, touched))
            bursts.add(task)
            task.add_done_callback(bursts.discard)
        await asyncio.gather(*bursts)
        for t in side:
            t.cancel()


def help_text() -> str:
    from demo.checks import GLOSSARY, TESTS

    run = "docker compose run --rm load-tester-client"
    tests = "\n".join(
        f"  {t.num}  {t.fn.__name__.replace('_', ' '):22} "
        + ", ".join(f"--{k.replace('_', '-')} [{d}] {h}" for k, (d, h) in t.params.items())
        for t in TESTS.values())
    return f"""load-tester-client: correctness checks and load for the quota service demo

COMMANDS   (from the repo folder: {run} <command>)
  (no command)                      all checks, then sustained load until stopped (what `docker compose up` runs)
  check                             all {len(TESTS)} checks, then exit (exit code 1 if any fails)
  check N [--param value ...]       one check with your own numbers, e.g.  check 2 --requests 2000 --limit 300
  load [--rate R] [--orgs N] [--seconds S]
                                    load only; defaults: {RATE:.0f} ops/s, orgs 1..{ORGS}, until stopped (Ctrl-C)
  help                              this text (also -h / --help)

CHECKS AND THEIR PARAMETERS   [default]
{tests}

{GLOSSARY}
"""


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        sys.exit(f"load-tester-client: {message}\n(run 'help' for the commands, checks and parameters)")


def parse(argv: list[str]) -> argparse.Namespace:
    from demo.checks import TESTS

    ap = _Parser(prog="load-tester-client", add_help=False)
    sub = ap.add_subparsers(dest="cmd", parser_class=_Parser)
    sub.add_parser("help", add_help=False)
    ld = sub.add_parser("load", add_help=False)
    ld.add_argument("--rate", type=float, default=RATE)
    ld.add_argument("--orgs", type=int, default=ORGS)
    ld.add_argument("--seconds", type=float, default=DURATION_S)
    chk = sub.add_parser("check", add_help=False)
    chk.add_argument("test", nargs="?", type=int, choices=sorted(TESTS))
    for name in sorted({k for t in TESTS.values() for k in t.params}):
        chk.add_argument(f"--{name.replace('_', '-')}", dest=name, type=int)
    args = ap.parse_args(argv)
    if args.cmd == "check":
        given = {k: v for k, v in vars(args).items() if k not in ("cmd", "test") and v is not None}
        if given and args.test is None:
            ap.error("parameters need a test number, e.g. 'check 2 --requests 2000'")
        if args.test is not None and (unknown := set(given) - set(TESTS[args.test].params)):
            ap.error(f"check {args.test} has no parameter(s) {', '.join(sorted(unknown))}")
        args.overrides = given
    return args


def main(argv: list[str]) -> int:
    from demo.checks import run as run_checks

    if {"-h", "--help"} & set(argv):
        print(help_text())
        return 0
    args = parse(argv)
    if args.cmd == "help":
        print(help_text())
        return 0
    if args.cmd == "check":
        return 0 if asyncio.run(run_checks(args.test, args.overrides)) else 1
    if args.cmd is None:   # default: checks first, then sustained load whatever their outcome
        if not asyncio.run(run_checks()):
            print("[load-tester-client] CHECKS FAILED (see above); starting the load anyway", flush=True)
    if args.cmd == "load":
        asyncio.run(load(args.rate, args.orgs, args.seconds))
    else:
        asyncio.run(load(RATE, ORGS, DURATION_S))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

"""Open-loop load generator for the quota check-and-deduct op.

Open loop: every op has a SCHEDULED start time (Poisson arrivals at the target rate). Latency is
measured from the scheduled time, so time spent queued behind a slow DB / exhausted pool counts
(no coordinated omission). N worker processes stand in for the N service instances, each with its
own connection pool.

Modes
  uniform  every op picks a random (org, feature) out of 150k
  bursty   arrivals come in clumps: one org+feature fires 20-50 ops within ~200 ms, then goes quiet
  hot      every op hits one (org, feature) row -> per-row ceiling

Example
  .venv/bin/python loadgen.py --store pg --rate 10000 --mode uniform --label P1
"""
from __future__ import annotations

import argparse
import asyncio
import gc
import json
import multiprocessing as mp
import os
import platform
import time
import uuid

import numpy as np
import psutil

from common import IDEMPOTENCY_TTL_S, LUA, N_FEATURES, N_ORGS, PERIOD, PG_DSN, REDIS_PORT, RESULTS, qkey, reqkey

OK, REJECTED, DUP, ERROR, UNACKED, SHED, TIMEOUT = range(7)
STATUS_NAMES = ["ok", "rejected", "dup", "error", "unacked", "shed", "timeout"]
MAX_INFLIGHT = 4000  # per worker; beyond this we shed (the DB is clearly saturated)


# ---------------------------------------------------------------- schedule
def build_schedule(rate: float, seconds: float, mode: str, rng: np.random.Generator):
    """Return (t, org, feat, units) arrays, t = seconds from start."""
    if mode == "bursty":
        mean_k = 35
        n_clumps = int(round(rate / mean_k * seconds))
        starts = np.sort(rng.uniform(0, seconds, n_clumps))
        ks = rng.integers(20, 51, n_clumps)
        t = np.concatenate([s + np.sort(rng.uniform(0, 0.2, k)) for s, k in zip(starts, ks)]) if n_clumps else np.empty(0)
        org = np.repeat(rng.integers(0, N_ORGS, n_clumps), ks)
        feat = np.repeat(rng.integers(0, N_FEATURES, n_clumps), ks)
        order = np.argsort(t, kind="stable")
        t, org, feat = t[order], org[order], feat[order]
    else:
        n = int(round(rate * seconds))
        t = np.sort(rng.uniform(0, seconds, n))
        if mode == "hot":
            org = np.zeros(n, dtype=np.int64)
            feat = np.zeros(n, dtype=np.int64)
        else:
            org = rng.integers(0, N_ORGS, n)
            feat = rng.integers(0, N_FEATURES, n)
    n = len(t)
    # half single-unit ops, half batches of 1-100 units (a 100-container upload is ONE op)
    units = np.where(rng.random(n) < 0.5, 1, rng.integers(1, 101, n))
    return t, org, feat, units


# ---------------------------------------------------------------- store clients
class PgClient:
    def __init__(self, op: str, pool: int):
        self.op, self.size = op, pool

    async def open(self):
        import psycopg
        self.q: asyncio.Queue = asyncio.Queue()
        for _ in range(self.size):
            c = await psycopg.AsyncConnection.connect(PG_DSN, autocommit=True, prepare_threshold=0)
            await self.q.put(c)

    async def call(self, org, feat, units) -> int:
        c = await self.q.get()
        try:
            if self.op == "noop":
                await (await c.execute("SELECT 1")).fetchone()
                return OK
            cur = await c.execute(
                "SELECT charge(%s, %s::int, %s::smallint, %s, %s::int)",
                (uuid.uuid4(), org, feat, PERIOD, units),
            )
            v = (await cur.fetchone())[0]
            return OK if v >= 0 else (REJECTED if v == -1 else DUP if v == -2 else ERROR)
        finally:
            self.q.put_nowait(c)

    async def close(self):
        while not self.q.empty():
            await (self.q.get_nowait()).close()


class RedisClient:
    def __init__(self, op: str, pool: int, wait: str, wait_timeout_ms: int):
        self.op, self.size, self.wait, self.wto = op, pool, wait, wait_timeout_ms

    async def open(self):
        import redis.asyncio as aredis
        self.r = aredis.Redis(
            port=REDIS_PORT,
            connection_pool=aredis.BlockingConnectionPool(port=REDIS_PORT, max_connections=self.size, timeout=None),
        )
        self.sha = await self.r.script_load(LUA)

    async def call(self, org, feat, units) -> int:
        if self.op == "noop":
            await self.r.ping()
            return OK
        rid = uuid.uuid4().hex
        args = (self.sha, 2, reqkey(org, rid), qkey(org, feat), units, IDEMPOTENCY_TTL_S)
        if self.wait == "none":
            v = await self.r.execute_command("EVALSHA", *args)
            acked = True
        else:
            p = self.r.pipeline(transaction=False)
            p.execute_command("EVALSHA", *args)
            if self.wait == "wait":
                p.execute_command("WAIT", 1, self.wto)
            else:  # waitaof: fsync'd locally AND on 1 replica
                p.execute_command("WAITAOF", 1, 1, self.wto)
            v, ack = await p.execute()
            acked = (ack >= 1) if self.wait == "wait" else (ack[0] >= 1 and ack[1] >= 1)
        if v < 0:
            return REJECTED if v == -1 else DUP if v == -2 else ERROR
        return OK if acked else UNACKED

    async def close(self):
        await self.r.aclose()


# ---------------------------------------------------------------- worker
async def _run_worker(cfg: dict, sync, wid: int):
    rng = np.random.default_rng(cfg["seed"] * 1000 + wid)
    total_s = cfg["warmup"] + cfg["duration"]
    t, org, feat, units = build_schedule(cfg["rate"] / cfg["workers"], total_s, cfg["mode"], rng)
    n = len(t)
    lat = np.full(n, np.nan)
    lag = np.full(n, np.nan)  # how late the GENERATOR was in launching (its own saturation)
    status = np.full(n, TIMEOUT, dtype=np.int8)

    if cfg["store"] == "pg":
        client = PgClient(cfg["op"], cfg["pool"])
    else:
        client = RedisClient(cfg["op"], cfg["pool"], cfg["wait"], cfg["wait_timeout_ms"])
    await client.open()
    # all workers connected -> parent hands out ONE shared start time
    ready, go, start_val = sync
    ready.wait()
    go.wait()
    start_wall = start_val.value

    inflight = 0
    max_inflight = 0
    pending: set = set()

    async def one(i: int, launched_at: float):
        nonlocal inflight
        try:
            s = await client.call(int(org[i]), int(feat[i]), int(units[i]))
        except Exception:
            s = ERROR
        lat[i] = time.perf_counter() - t0 - t[i]
        status[i] = s
        inflight -= 1

    # synchronized start across processes
    await asyncio.sleep(max(0.0, start_wall - time.time()))
    t0 = time.perf_counter()
    t0_wall = time.time()
    i = 0
    while i < n:
        now = time.perf_counter() - t0
        while i < n and t[i] <= now:
            if inflight >= MAX_INFLIGHT:
                status[i] = SHED
            else:
                inflight += 1
                max_inflight = max(max_inflight, inflight)
                lag[i] = now - t[i]
                task = asyncio.create_task(one(i, now))
                pending.add(task)
                task.add_done_callback(pending.discard)
            i += 1
        if i < n:
            await asyncio.sleep(max(0.0, t[i] - (time.perf_counter() - t0)))
    if pending:
        await asyncio.wait(pending, timeout=10)
    for task in list(pending):
        task.cancel()
    await client.close()

    win = (t >= cfg["warmup"]) & (t < total_s)
    return {
        "lat": lat[win & (status <= UNACKED) & ~np.isnan(lat)].astype(np.float32),
        "lag": lag[win & ~np.isnan(lag)].astype(np.float32),
        # service time = completion - actual launch (excludes the generator's own wake-up lag)
        "svc": (lat - lag)[win & (status <= UNACKED) & ~np.isnan(lat)].astype(np.float32),
        "counts": np.bincount(status[win].astype(np.int64), minlength=len(STATUS_NAMES)).tolist(),
        "max_inflight": max_inflight,
        "tl": (np.stack([t0_wall + t[win], lat[win]]).astype(np.float64) if cfg.get("timeline") else None),
    }


def _worker_entry(cfg, sync, wid, q):
    gc.disable()  # cyclic-GC pauses showed up as 50-600 ms generator stalls; runs are short-lived
    try:
        q.put((wid, asyncio.run(_run_worker(cfg, sync, wid))))
    except Exception as e:  # surface failures to the parent instead of hanging it
        q.put((wid, {"error": repr(e)}))


# ---------------------------------------------------------------- parent
def _power_source() -> str:
    """'AC' or 'Battery' (macOS Low Power Mode on battery throttles the CPU and skews results)."""
    try:
        import subprocess
        out = subprocess.run(["pmset", "-g", "batt"], capture_output=True, text=True, timeout=5).stdout
        return "AC" if "AC Power" in out else "Battery"
    except Exception:  # noqa: BLE001
        return "unknown"


def run(cfg: dict) -> dict:
    ctx = mp.get_context("spawn")
    q = ctx.Queue()
    # workers spawn + build schedules + connect at their own pace, then wait at a barrier; only then
    # is the common start time fixed (spawning 8 interpreters can take >5 s on a busy laptop)
    ready, go, start_val = ctx.Barrier(cfg["workers"] + 1), ctx.Event(), ctx.Value("d", 0.0)
    procs = [ctx.Process(target=_worker_entry, args=(cfg, (ready, go, start_val), w, q)) for w in range(cfg["workers"])]
    for p in procs:
        p.start()
    ready.wait(timeout=120)
    start_wall = time.time() + 0.5
    start_val.value = start_wall
    go.set()

    # CPU used during the measured window, in cores (cpu-seconds / wall-seconds), split into
    # benchmark processes (generator, DB, proxy) vs EVERYTHING ELSE on the machine. "other" is what
    # lets the matrix flag runs disturbed by unrelated load (e.g. a desktop app spiking).
    time.sleep(max(0.0, start_wall + cfg["warmup"] - time.time()))
    gen_pids = {p.pid for p in procs}
    bench_names = {"postgres", "redis-server", "toxiproxy-server"}

    def snapshot():
        snap = {}
        for p in psutil.process_iter(["name"]):
            try:
                ct = p.cpu_times()
                snap[p.pid] = (p.info["name"] or "?", ct.user + ct.system)
            except psutil.Error:
                pass
        return snap

    t_before = time.time()
    if os.environ.get("NO_CPU_SAMPLING"):
        snapshot = lambda: {}  # noqa: E731  (diagnostic: rule out the sampler itself)
    before = snapshot()
    # stop sampling a little before the end: workers close their connections (and the DB backends
    # exit) right at the end of the run, which would drop them from the second snapshot
    time.sleep(max(0.5, cfg["duration"] - 1.0))
    after = snapshot()
    window = time.time() - t_before
    groups = {"gen": 0.0, "db": 0.0, "other": 0.0}
    others: dict = {}
    for pid, (name, secs) in after.items():
        if pid not in before or before[pid][0] != name:
            continue  # count only processes alive for the whole window (no pid reuse confusion)
        d = max(0.0, secs - before[pid][1])
        if pid in gen_pids:
            groups["gen"] += d
        elif name in bench_names:
            groups["db"] += d
        else:
            groups["other"] += d
            others[name] = others.get(name, 0.0) + d
    cpu = {k: round(v / window, 2) for k, v in groups.items()}
    cpu["sampled_at"] = [round(t_before, 2), round(t_before + window, 2)]
    cpu["top_other"] = {n: round(v / window, 2) for n, v in sorted(others.items(), key=lambda kv: -kv[1])[:3]}

    results = [q.get(timeout=cfg["warmup"] + cfg["duration"] + 120) for _ in procs]
    for p in procs:
        p.join()
    errs = [r["error"] for _, r in results if "error" in r]
    if errs:
        raise RuntimeError(f"worker failed: {errs[0]}")

    lat = np.concatenate([r["lat"] for _, r in results]) * 1000.0
    lag = np.concatenate([r["lag"] for _, r in results]) * 1000.0
    svc = np.concatenate([r["svc"] for _, r in results]) * 1000.0
    counts = np.sum([r["counts"] for _, r in results], axis=0).tolist()
    timeline = None
    if cfg.get("timeline"):  # 100 ms buckets by scheduled wall time: (start, n, max_ms, p99_ms)
        tl = np.concatenate([r["tl"] for _, r in results], axis=1)
        tl = tl[:, ~np.isnan(tl[1])]
        b = np.floor(tl[0] * 10) / 10
        timeline = []
        for bs in np.unique(b):
            v = tl[1][b == bs] * 1000
            timeline.append([float(bs), int(len(v)), round(float(v.max()), 2), round(float(np.percentile(v, 99)), 2)])
    pct = (lambda a, p: round(float(np.percentile(a, p)), 3) if len(a) else None)
    completed = counts[OK] + counts[REJECTED] + counts[DUP] + counts[UNACKED]
    return {
        **{k: v for k, v in cfg.items() if k != "seed"},
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "achieved_rate": round(completed / cfg["duration"], 1),
        "p50_ms": pct(lat, 50), "p95_ms": pct(lat, 95), "p99_ms": pct(lat, 99),
        "p999_ms": pct(lat, 99.9), "max_ms": round(float(lat.max()), 3) if len(lat) else None,
        "gen_lag_p99_ms": pct(lag, 99),
        "svc_p50_ms": pct(svc, 50), "svc_p99_ms": pct(svc, 99), "svc_p999_ms": pct(svc, 99.9),
        "counts": dict(zip(STATUS_NAMES, counts)),
        "max_inflight_per_worker": max(r["max_inflight"] for _, r in results),
        "cpu_cores": cpu,
        "host": f"{platform.machine()} {os.cpu_count()} cpu",
        "power": _power_source(),
        **({"timeline": timeline} if timeline is not None else {}),
    }


def parse(argv=None) -> dict:
    ap = argparse.ArgumentParser()
    ap.add_argument("--store", choices=["pg", "redis"], required=True)
    ap.add_argument("--op", choices=["charge", "noop"], default="charge")
    ap.add_argument("--rate", type=float, required=True, help="target ops/sec across all workers")
    ap.add_argument("--duration", type=float, default=30)
    ap.add_argument("--warmup", type=float, default=5)
    ap.add_argument("--mode", choices=["uniform", "bursty", "hot"], default="uniform")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--pool", type=int, default=8, help="connections per worker")
    ap.add_argument("--wait", choices=["none", "wait", "waitaof"], default="none")
    ap.add_argument("--wait-timeout-ms", type=int, default=50)
    ap.add_argument("--label", default="")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--timeline", action="store_true", help="also return per-100ms latency buckets")
    ap.add_argument("--out", default=str(RESULTS / "runs.jsonl"))
    return vars(ap.parse_args(argv))


def main(argv=None):
    cfg = parse(argv)
    out = cfg.pop("out")
    res = run(cfg)
    with open(out, "a") as f:
        f.write(json.dumps(res) + "\n")
    c = res["counts"]
    print(f"[{res['label']}] {res['store']}/{res['op']}/{res['mode']} target={res['rate']:.0f} "
          f"achieved={res['achieved_rate']:.0f}/s p50={res['p50_ms']} p99={res['p99_ms']} "
          f"p99.9={res['p999_ms']} svc99={res['svc_p99_ms']} genlag99={res['gen_lag_p99_ms']} "
          f"err={c['error']} unacked={c['unacked']} shed={c['shed']} timeout={c['timeout']} "
          f"cpu={res['cpu_cores']}", flush=True)
    return res


if __name__ == "__main__":
    main()

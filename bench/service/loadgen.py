"""Open-loop HTTP load generator for the quota service.

One OP = what a consumer does per metered request: POST /v1/reserve, then (if reserved)
POST /v1/commit. Arrivals are Poisson at the target rate and every op has a SCHEDULED start;
latency is measured from that scheduled time, so queueing behind a saturated service counts
(no coordinated omission). N worker processes stand in for the consumer fleet.

Reported per run
  reserve_*   scheduled start -> reserve response   (what sits on the consumer's critical path)
  commit_*    commit sent     -> commit response
  op_*        scheduled start -> commit response    (whole metering overhead for one op)
  counts by outcome, achieved rate, generator lag, CPU by group (generator / service / stores)

Flows
  op        reserve + commit (default)
  healthz   GET /healthz only: calibrates the generator + HTTP stack ceiling
Modes
  uniform   random org (1..5000) x feature (1..30)
  hot       every op hits one (org --hot-org, feature1)
  every     each (org, feature) exactly once over the run (cold start: --rate is ignored)

  .venv/bin/python bench/service/loadgen.py --rate 5000 --label S1
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
from pathlib import Path

import numpy as np
import psutil

RESULTS = Path(__file__).resolve().parent / "results"
OK, REJECTED, BUSY, ERROR, SHED, TIMEOUT = range(6)
NAMES = ["ok", "rejected", "busy", "error", "shed", "timeout"]
MAX_INFLIGHT = 3000  # per worker; beyond this the service is clearly saturated -> shed


def build_schedule(rate, seconds, mode, n_orgs, n_feat, rng, hot_org=1):
    if mode == "every":  # each (org, feature) exactly once, shuffled: first touch of every lease
        pairs = rng.permutation(n_orgs * n_feat)
        n = len(pairs)
        t = np.sort(rng.uniform(0, seconds, n))
        org, feat = pairs // n_feat + 1, pairs % n_feat + 1
        units = np.ones(n, dtype=np.int64)
        return t, org, feat, units
    n = int(round(rate * seconds))
    t = np.sort(rng.uniform(0, seconds, n))
    if mode == "hot":
        org = np.full(n, hot_org, dtype=np.int64)
        feat = np.ones(n, dtype=np.int64)
    else:
        org = rng.integers(1, n_orgs + 1, n)
        feat = rng.integers(1, n_feat + 1, n)
    # half single-unit ops, half batches of 1-100 units (a 100-container upload is ONE op)
    units = np.where(rng.random(n) < 0.5, 1, rng.integers(1, 101, n))
    return t, org, feat, units


async def _run_worker(cfg, sync, wid):
    import aiohttp

    # 'every' needs the SAME permutation in every worker, so the split below is a partition
    rng = np.random.default_rng(cfg["seed"] if cfg["mode"] == "every" else cfg["seed"] * 1000 + wid)
    total_s = cfg["warmup"] + cfg["duration"]
    t, org, feat, units = build_schedule(cfg["rate"] / cfg["workers"], total_s, cfg["mode"],
                                         cfg["orgs"], cfg["features"], rng, cfg["hot_org"])
    if cfg["mode"] == "every":  # split the pairs between workers instead of each doing all
        keep = (np.arange(len(t)) % cfg["workers"]) == wid
        t, org, feat, units = t[keep], org[keep], feat[keep], units[keep]
    n = len(t)
    r_lat = np.full(n, np.nan)
    c_lat = np.full(n, np.nan)
    op_lat = np.full(n, np.nan)
    lag = np.full(n, np.nan)
    status = np.full(n, TIMEOUT, dtype=np.int8)
    codes: dict[int, int] = {}

    urls = [f"http://127.0.0.1:{p}" for p in range(cfg["port"], cfg["port"] + cfg["instances"])]
    per = max(1, cfg["conns"] // len(urls))
    conn = aiohttp.TCPConnector(limit=per * len(urls), limit_per_host=per, force_close=False)
    timeout = aiohttp.ClientTimeout(total=10)
    session = aiohttp.ClientSession(connector=conn, timeout=timeout, json_serialize=json.dumps)
    # open the connections before the clock starts
    await asyncio.gather(*(session.get(u + "/healthz") for u in urls for _ in range(per)))

    ready, go, start_val = sync
    ready.wait()
    go.wait()
    inflight = max_inflight = 0
    pending: set = set()

    def code(c):
        codes[c] = codes.get(c, 0) + 1

    async def one(i):
        nonlocal inflight
        s = ERROR
        base = urls[i % len(urls)]  # round-robin per op = a per-request load balancer
        try:
            if cfg["flow"] == "healthz":
                async with session.get(base + "/healthz") as r:
                    await r.read()
                    code(r.status)
                    s = OK if r.status == 200 else ERROR
                r_lat[i] = op_lat[i] = time.perf_counter() - t0 - t[i]
            else:
                o, rid = int(org[i]), uuid.uuid4().hex
                async with session.post(base + "/v1/reserve", json={
                        "org_id": o, "feature": f"feature{int(feat[i])}",
                        "units": int(units[i]), "request_id": rid}) as r:
                    await r.read()
                    code(r.status)
                now = time.perf_counter()
                r_lat[i] = now - t0 - t[i]
                if r.status == 200:
                    async with session.post(base + "/v1/commit",
                                            json={"org_id": o, "request_id": rid}) as c:
                        await c.read()
                        code(1000 + c.status)  # 1xxx = commit status codes
                    end = time.perf_counter()
                    c_lat[i] = end - now
                    op_lat[i] = end - t0 - t[i]
                    s = OK if c.status == 200 else ERROR
                else:
                    op_lat[i] = r_lat[i]
                    s = REJECTED if r.status == 429 else BUSY if r.status == 503 else ERROR
        except Exception:
            s = ERROR
        status[i] = s
        inflight -= 1

    await asyncio.sleep(max(0.0, start_val.value - time.time()))
    t0 = time.perf_counter()
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
                task = asyncio.create_task(one(i))
                pending.add(task)
                task.add_done_callback(pending.discard)
            i += 1
        if i < n:
            await asyncio.sleep(max(0.0, t[i] - (time.perf_counter() - t0)))
    if pending:
        await asyncio.wait(pending, timeout=12)
    await session.close()

    win = (t >= cfg["warmup"]) & (t < total_s)
    done = win & (status <= BUSY)
    pick = lambda a, m: a[m & ~np.isnan(a)].astype(np.float32)  # noqa: E731
    return {
        "reserve": pick(r_lat, done), "commit": pick(c_lat, done & (status == OK)),
        "op": pick(op_lat, done), "lag": pick(lag, win),
        "counts": np.bincount(status[win].astype(np.int64), minlength=len(NAMES)).tolist(),
        "codes": codes, "max_inflight": max_inflight,
    }


def _entry(cfg, sync, wid, q):
    gc.disable()  # cyclic-GC pauses show up as generator stalls; runs are short-lived
    try:
        q.put((wid, asyncio.run(_run_worker(cfg, sync, wid))))
    except Exception as e:  # noqa: BLE001
        q.put((wid, {"error": repr(e)}))


def _svc_pids():
    """The uvicorn parent (pid in bench/service/uvicorn.pid) and all its worker processes."""
    try:
        out = set()
        for line in (Path(__file__).parent / "uvicorn.pid").read_text().split():
            root = psutil.Process(int(line))
            out |= {root.pid} | {c.pid for c in root.children(recursive=True)}
        return out
    except (OSError, ValueError, psutil.Error):
        return set()


def _cpu_groups(gen_pids):
    svc = _svc_pids()
    snap = {}
    for p in psutil.process_iter(["name"]):
        try:
            ct = p.cpu_times()
        except psutil.Error:
            continue
        name = p.info["name"] or "?"
        if p.pid in gen_pids:
            g = "gen"
        elif name in ("postgres", "redis-server"):
            g = "stores"
        elif p.pid in svc:
            g = "svc"
        else:
            g = "other"
        snap[p.pid] = (name, g, ct.user + ct.system)
    return snap


def run(cfg):
    ctx = mp.get_context("spawn")
    q = ctx.Queue()
    ready, go, start_val = ctx.Barrier(cfg["workers"] + 1), ctx.Event(), ctx.Value("d", 0.0)
    procs = [ctx.Process(target=_entry, args=(cfg, (ready, go, start_val), w, q))
             for w in range(cfg["workers"])]
    for p in procs:
        p.start()
    ready.wait(timeout=120)
    start = time.time() + 0.5
    start_val.value = start
    go.set()

    time.sleep(max(0.0, start + cfg["warmup"] - time.time()))
    gen = {p.pid for p in procs}
    t_b, before = time.time(), _cpu_groups(gen)
    time.sleep(max(0.5, cfg["duration"] - 1.0))
    after, window = _cpu_groups(gen), time.time() - t_b
    cpu = {"gen": 0.0, "svc": 0.0, "stores": 0.0, "other": 0.0}
    for pid, (name, g, secs) in after.items():
        if pid in before and before[pid][0] == name:
            cpu[g] += max(0.0, secs - before[pid][2])
    cpu = {k: round(v / window, 2) for k, v in cpu.items()}

    results = [q.get(timeout=cfg["warmup"] + cfg["duration"] + 60) for _ in procs]
    for p in procs:
        p.join()
    errs = [r["error"] for _, r in results if "error" in r]
    if errs:
        raise RuntimeError(f"worker failed: {errs[0]}")

    cat = lambda k: np.concatenate([r[k] for _, r in results]) * 1000.0  # noqa: E731
    pct = lambda a, p: round(float(np.percentile(a, p)), 2) if len(a) else None  # noqa: E731
    counts = np.sum([r["counts"] for _, r in results], axis=0).tolist()
    codes: dict = {}
    for _, r in results:
        for k, v in r["codes"].items():
            codes[str(k)] = codes.get(str(k), 0) + v
    out = {**cfg, "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
           "achieved_rate": round((counts[OK] + counts[REJECTED] + counts[BUSY]) / cfg["duration"], 1)}
    for k in ("reserve", "commit", "op"):
        a = cat(k)
        out.update({f"{k}_p50": pct(a, 50), f"{k}_p99": pct(a, 99), f"{k}_p999": pct(a, 99.9),
                    f"{k}_max": round(float(a.max()), 2) if len(a) else None})
    out.update({"gen_lag_p99": pct(cat("lag"), 99), "counts": dict(zip(NAMES, counts)),
                "codes": codes, "max_inflight_per_worker": max(r["max_inflight"] for _, r in results),
                "cpu_cores": cpu, "host": f"{platform.machine()} {os.cpu_count()} cpu"})
    return out


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8101, help="first instance port")
    ap.add_argument("--instances", type=int, default=8, help="service instances on consecutive ports")
    ap.add_argument("--flow", choices=["op", "healthz"], default="op")
    ap.add_argument("--rate", type=float, required=True, help="target ops/s across all workers")
    ap.add_argument("--duration", type=float, default=20)
    ap.add_argument("--warmup", type=float, default=4)
    ap.add_argument("--mode", choices=["uniform", "hot", "every"], default="uniform")
    ap.add_argument("--orgs", type=int, default=5000)
    ap.add_argument("--hot-org", type=int, default=9001)
    ap.add_argument("--features", type=int, default=30)
    ap.add_argument("--workers", type=int, default=4, help="generator processes")
    ap.add_argument("--conns", type=int, default=64, help="HTTP connections per generator process")
    ap.add_argument("--label", default="")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--out", default=str(RESULTS / "service.jsonl"))
    cfg = vars(ap.parse_args(argv))
    out_path = Path(cfg.pop("out"))
    res = run(cfg)
    out_path.parent.mkdir(exist_ok=True)
    with out_path.open("a") as f:
        f.write(json.dumps(res) + "\n")
    c = res["counts"]
    print(f"[{res['label']}] {res['flow']}/{res['mode']} target={res['rate']:.0f} "
          f"achieved={res['achieved_rate']:.0f}/s | reserve p50={res['reserve_p50']} "
          f"p99={res['reserve_p99']} p99.9={res['reserve_p999']} | commit p99={res['commit_p99']} | "
          f"op p99={res['op_p99']} | genlag99={res['gen_lag_p99']} | ok={c['ok']} rej={c['rejected']} "
          f"busy={c['busy']} err={c['error']} codes={res['codes']} shed={c['shed']} timeout={c['timeout']} | "
          f"cpu={res['cpu_cores']}", flush=True)


if __name__ == "__main__":
    main()

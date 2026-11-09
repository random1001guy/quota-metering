"""Run the benchmark matrix. Each series = one store configuration swept over increasing rates.
Each (series, rate) cell runs REPEATS times; the report uses the median, raw runs are all kept.
A series stops once it is clearly past its knee (median p99 > STOP_P99_MS, rate not achieved, or errors).

  .venv/bin/python matrix.py                 # everything
  .venv/bin/python matrix.py P1 P2-1ms       # only these series
  .venv/bin/python matrix.py --list
  .venv/bin/python matrix.py P1 --rates 1000      # ONE cell (preferred: run and check row by row)
  .venv/bin/python matrix.py --smoke [series...]   # ~1 min/series pre-flight; run before the full sweep
"""
from __future__ import annotations

import json
import statistics
import sys
import time
import urllib.request

import psycopg
import redis

import loadgen
import seed
from common import NOISE_GEN_LAG_MS, NOISE_OTHER_CORES, PG_DSN, REDIS_PORT, REDIS_REPLICA_PORT, RESULTS, TOXI, is_noisy

REPEATS = 3
MAX_EXTRA = 3          # extra attempts per cell to replace runs disturbed by unrelated load
DURATION, WARMUP = 15, 4
STOP_P99_MS = 50
RATES = [1000, 2000, 5000, 10000, 15000, 20000, 30000]
HOT_RATES = [100, 250, 500, 750, 1000, 2000]
OUT = RESULTS / "matrix.jsonl"


# ------------------------------------------------------------------ config knobs
def toxi_latency(proxy: str, ms: int) -> None:
    """Add `ms` of one-way delay on the primary->replica stream = +ms to every replication round trip."""
    try:
        urllib.request.urlopen(urllib.request.Request(f"{TOXI}/proxies/{proxy}/toxics/lat", method="DELETE"))
    except urllib.error.HTTPError:
        pass
    if ms > 0:
        body = json.dumps({"name": "lat", "type": "latency", "stream": "downstream",
                           "attributes": {"latency": ms, "jitter": 0}}).encode()
        urllib.request.urlopen(urllib.request.Request(f"{TOXI}/proxies/{proxy}/toxics", data=body, method="POST"))


def pg_config(sync_commit: str, wal_sync: str, standby: bool, latency_ms: int) -> dict:
    toxi_latency("pg_repl", latency_ms)
    with psycopg.connect(PG_DSN, autocommit=True) as c:
        c.execute(f"ALTER SYSTEM SET synchronous_commit = '{sync_commit}'")
        c.execute(f"ALTER SYSTEM SET wal_sync_method = '{wal_sync}'")
        c.execute(f"ALTER SYSTEM SET synchronous_standby_names = '{'standby1' if standby else ''}'")
        c.execute("SELECT pg_reload_conf()")
    time.sleep(1.5)
    with psycopg.connect(PG_DSN, autocommit=True) as c:
        got = {k: c.execute(f"SHOW {k}").fetchone()[0]
               for k in ("synchronous_commit", "wal_sync_method", "synchronous_standby_names")}
        rep = c.execute("SELECT state, sync_state FROM pg_stat_replication").fetchall()
    got["replication"] = [f"{s}/{ss}" for s, ss in rep]
    if standby:
        assert ("streaming", "sync") in rep, f"standby not sync: {rep}"
    return got


def pg_reset() -> None:
    seed.seed_pg()
    with psycopg.connect(PG_DSN, autocommit=True) as c:
        c.execute("CHECKPOINT")


def redis_config(appendonly: bool, fsync: str, replica_aof: bool, latency_ms: int, replica: bool = True) -> dict:
    toxi_latency("redis_repl", latency_ms)
    # attach or detach the replica (it replicates through toxiproxy :56390)
    redis.Redis(port=REDIS_REPLICA_PORT).execute_command(*(("REPLICAOF", "127.0.0.1", "56390") if replica else ("REPLICAOF", "NO", "ONE")))
    for _ in range(300):
        n = redis.Redis(port=REDIS_PORT).info("replication")["connected_slaves"]
        if (n >= 1) == replica:
            break
        time.sleep(0.2)
    for port, aof in ((REDIS_PORT, appendonly), (REDIS_REPLICA_PORT, replica_aof)):
        r = redis.Redis(port=port)
        r.config_set("appendfsync", fsync)
        r.config_set("appendonly", "yes" if aof else "no")
        for _ in range(300):  # enabling AOF triggers a rewrite; wait for it
            if r.info("persistence").get("aof_rewrite_in_progress", 0) == 0:
                break
            time.sleep(0.2)
    r = redis.Redis(port=REDIS_PORT)
    info = r.info("replication")
    if replica:
        assert info["connected_slaves"] >= 1, "redis replica not connected"
    else:
        assert info["connected_slaves"] == 0, "redis replica still connected"
    return {"appendonly": r.config_get("appendonly")["appendonly"],
            "appendfsync": r.config_get("appendfsync")["appendfsync"],
            "replica_appendonly": redis.Redis(port=REDIS_REPLICA_PORT).config_get("appendonly")["appendonly"],
            "replica_link": info.get("slave0", "")}


def redis_reset() -> None:
    seed.seed_redis()
    time.sleep(1)  # let the replica catch up with the reseed


# ------------------------------------------------------------------ series
def P(sync_commit="on", wal="fsync", standby=False, lat=0, mode="uniform", pool=8, rates=RATES, desc=""):
    return dict(store="pg", rates=rates, mode=mode, pool=pool, wait="none", desc=desc,
                setup=lambda: pg_config(sync_commit, wal, standby, lat), reset=pg_reset)


def R(aof=False, fsync="everysec", replica_aof=False, wait="none", lat=0, mode="uniform", pool=8, rates=RATES, desc="",
      replica=True):
    return dict(store="redis", rates=rates, mode=mode, pool=pool, wait=wait, desc=desc,
                setup=lambda: redis_config(aof, fsync, replica_aof, lat, replica), reset=redis_reset)


SERIES = {
    # ---- Postgres
    "P0":           P(sync_commit="off", desc="no commit fsync (upper bound)"),
    "P1":           P(desc="single node, WAL fsync (macOS fsync = not truly durable)"),
    "P1-wt":        P(wal="fsync_writethrough", desc="single node, truly durable flush (F_FULLFSYNC)"),
    "P2-0ms":       P(standby=True, lat=0, desc="sync standby, loopback"),
    "P2-1ms":       P(standby=True, lat=1, desc="sync standby, +1 ms RTT (same region)"),
    "P2-2ms":       P(standby=True, lat=2, desc="sync standby, +2 ms RTT (cross-AZ, pessimistic)"),
    "P2-1ms-wt":    P(standby=True, lat=1, wal="fsync_writethrough", desc="sync standby +1 ms, truly durable local flush"),
    "P2-1ms-burst": P(standby=True, lat=1, mode="bursty", desc="sync standby +1 ms, bursty per-org traffic"),
    "P2-1ms-pool4": P(standby=True, lat=1, pool=4, desc="sync standby +1 ms, 4 conns/instance"),
    "P2-1ms-pool16": P(standby=True, lat=1, pool=16, desc="sync standby +1 ms, 16 conns/instance"),
    "P3-hot":       P(standby=True, lat=1, mode="hot", rates=HOT_RATES, desc="sync standby +1 ms, ONE row"),
    # ---- Redis
    "R0":           R(desc="no persistence (upper bound)"),
    "R1":           R(aof=True, fsync="everysec", desc="AOF everysec (<=1 s loss on crash)"),
    "R1-norepl":    R(aof=True, fsync="everysec", replica=False, desc="AOF everysec, no replica"),
    "R2":           R(aof=True, fsync="always", desc="AOF always (F_FULLFSYNC on macOS); same-node crash only"),
    "R3-0ms":       R(aof=True, wait="wait", lat=0, desc="AOF everysec + WAIT 1, loopback"),
    "R3-1ms":       R(aof=True, wait="wait", lat=1, desc="AOF everysec + WAIT 1, +1 ms RTT"),
    "R3-2ms":       R(aof=True, wait="wait", lat=2, desc="AOF everysec + WAIT 1, +2 ms RTT"),
    "R4-1ms":       R(aof=True, replica_aof=True, wait="waitaof", lat=1, desc="WAITAOF 1 1 (local+replica fsync), +1 ms RTT"),
    "R4-1ms-burst": R(aof=True, replica_aof=True, wait="waitaof", lat=1, mode="bursty", desc="WAITAOF 1 1, +1 ms, bursty"),
    "R3-hot":       R(aof=True, wait="wait", lat=1, mode="hot", rates=HOT_RATES, desc="WAIT 1 +1 ms, ONE key"),
}


def run_series(name: str, s: dict, rates=None) -> None:
    applied = s["setup"]()
    print(f"\n=== {name}: {s['desc']}\n    config: {applied}", flush=True)
    for rate in rates or s["rates"]:
        runs, attempts = [], 0
        while len(runs) < REPEATS and attempts < REPEATS + MAX_EXTRA:
            s["reset"]()
            cfg = loadgen.parse(["--store", s["store"], "--rate", str(rate), "--mode", s["mode"],
                                 "--pool", str(s["pool"]), "--wait", s["wait"], "--label", name,
                                 "--duration", str(DURATION), "--warmup", str(WARMUP), "--seed", str(attempts + 1)])
            cfg.pop("out")
            res = loadgen.run(cfg)
            # flagged ONLY on an external measurement (other processes' CPU), never on the latency itself
            noisy = is_noisy(res)
            res.update(series=name, desc=s["desc"], repeat=attempts, noisy=noisy, applied_config=applied)
            with open(OUT, "a") as f:
                f.write(json.dumps(res, default=str) + "\n")
            attempts += 1
            if noisy:
                print(f"    (noisy run discarded: genlag99={res['gen_lag_p99_ms']} ms, other={res['cpu_cores']['other']} cores "
                      f"{res['cpu_cores']['top_other']}, p99={res['p99_ms']})", flush=True)
            else:
                runs.append(res)
        if not runs:
            # repeatedly unable to drive this rate cleanly -> that is the LAPTOP's limit, not a DB number
            print(f"  {name} rate={rate}: no clean run in {attempts} attempts -> generator/machine limit, stopping", flush=True)
            break
        med = lambda k: statistics.median(r[k] for r in runs if r[k] is not None)
        bad = sum(r["counts"]["error"] + r["counts"]["shed"] + r["counts"]["timeout"] + r["counts"]["unacked"] for r in runs)
        total = sum(sum(r["counts"].values()) for r in runs)
        c = runs[0]["cpu_cores"]
        print(f"  {name:14s} rate={rate:>6} achieved={med('achieved_rate'):>8.0f} "
              f"p50={med('p50_ms'):7.2f} p99={med('p99_ms'):8.2f} (runs: "
              f"{', '.join(f'{r['p99_ms']:.2f}' for r in runs)}) svc99={med('svc_p99_ms'):7.2f} "
              f"bad={bad} cpu gen/db/other={c['gen']}/{c['db']}/{c['other']}", flush=True)
        if med("p99_ms") > STOP_P99_MS or med("achieved_rate") < 0.95 * rate or bad > 0.005 * total:
            print(f"  {name}: stopping, past the knee", flush=True)
            break


SMOKE_CHECKS = {
    # series -> extra sanity check on (rate -> result). Proves each knob actually took effect.
    "P1-wt": lambda r: r[2000]["p50_ms"] > 2.0,            # F_FULLFSYNC must be visibly slower
    "P2-1ms": lambda r: r[2000]["p50_ms"] > 1.0,           # +1 ms replication RTT must show up in p50
    "P2-2ms": lambda r: r[2000]["p50_ms"] > 2.0,
    "R2": lambda r: r[2000]["p50_ms"] > 2.0,               # AOF always on macOS = F_FULLFSYNC
    "R3-1ms": lambda r: r[2000]["p50_ms"] > 1.0,
    "R4-1ms": lambda r: r[2000]["p50_ms"] > 1.0,
}


def smoke(names):
    """Fast pre-flight: every series, 1 short run at 2 rates. Checks config applied, no errors, rate
    achieved, and that latency knobs (fsync mode, injected RTT) visibly took effect."""
    rows = []
    for name in names:
        s = SERIES[name]
        try:
            applied = s["setup"]()
        except Exception as e:  # noqa: BLE001
            rows.append((name, "FAIL", f"setup: {e!r}"))
            continue
        rates = [500, 2000] if s["mode"] == "hot" else [2000, 10000]
        res = {}
        for rate in rates:
            s["reset"]()
            cfg = loadgen.parse(["--store", s["store"], "--rate", str(rate), "--mode", s["mode"],
                                 "--pool", str(s["pool"]), "--wait", s["wait"], "--label", name,
                                 "--duration", "6", "--warmup", "2"])
            cfg.pop("out")
            res[rate] = loadgen.run(cfg)
            res[rate]["applied_config"] = applied
            with open(RESULTS / "smoke.jsonl", "a") as f:
                f.write(json.dumps({**res[rate], "series": name}, default=str) + "\n")
        # Setup is judged at the LOW rate only: there everything must be clean. The high rate is
        # informational and classified: saturated (DB-side: big latency, generator on schedule),
        # noisy (generator fell behind = machine stall), or clean.
        problems = []
        low = min(res)
        r = res[low]
        c = r["counts"]
        if c["error"] or c["timeout"] or c["shed"] or c["unacked"]:
            problems.append(f"{low}/s bad ops {c}")
        if r["achieved_rate"] < 0.95 * low:
            problems.append(f"{low}/s achieved only {r['achieved_rate']}")
        if c["rejected"] or c["dup"]:
            problems.append(f"{low}/s unexpected rejected/dup {c}")

        def classify(r):
            if (r["gen_lag_p99_ms"] or 0) > NOISE_GEN_LAG_MS or r["cpu_cores"]["other"] > NOISE_OTHER_CORES:
                return "noisy"
            if r["p99_ms"] > STOP_P99_MS or sum(r["counts"][k] for k in ("timeout", "shed", "error")):
                return "SATURATED"
            return "clean"
        check = SMOKE_CHECKS.get(name)
        if check and 2000 in res and not check(res):
            problems.append(f"knob not visible: p50@2k={res[2000]['p50_ms']} ms")
        summary = "  ".join(f"{rate}/s [{classify(r)}] p50={r['p50_ms']:.2f} p99={r['p99_ms']:.2f} genlag99={r['gen_lag_p99_ms']:.2f}"
                            for rate, r in res.items())
        rows.append((name, "FAIL" if problems else "ok", summary + ("  <- " + "; ".join(problems) if problems else "")))
        print(f"{name:14s} {rows[-1][1]:4s} {rows[-1][2]}", flush=True)
    print("\nSMOKE:", "ALL OK" if all(r[1] == "ok" for r in rows) else "FAILURES: " + ", ".join(r[0] for r in rows if r[1] != "ok"))


def main(argv):
    if "--smoke" in argv:
        smoke([a for a in argv if not a.startswith("-")] or list(SERIES))
        return
    if "--list" in argv:
        for k, s in SERIES.items():
            print(f"{k:14s} {s['desc']}")
        return
    rates = None
    if "--rates" in argv:  # run only these cells, e.g. --rates 1000 or --rates 1000,2000
        i = argv.index("--rates")
        rates = [int(x) for x in argv[i + 1].split(",")]
        argv = argv[:i] + argv[i + 2:]
    names = [a for a in argv if not a.startswith("-")] or list(SERIES)
    for n in names:
        run_series(n, SERIES[n], rates)
    # leave the stack in a neutral state
    toxi_latency("pg_repl", 0)
    toxi_latency("redis_repl", 0)


if __name__ == "__main__":
    main(sys.argv[1:])

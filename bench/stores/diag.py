"""Diagnose latency stalls: run one loadgen cell while sampling Postgres wait events and the
per-100ms latency timeline, so a stall can be matched to what backends were waiting on.

  .venv/bin/python diag.py --rate 10000 --series P0
"""
from __future__ import annotations

import argparse
import collections
import json
import threading
import time

import numpy as np
import psutil
import psycopg

import loadgen
import matrix
from common import PG_DSN, RESULTS


def sampler(stop: threading.Event, out: list):
    with psycopg.connect(PG_DSN, autocommit=True) as c:
        while not stop.is_set():
            t = time.time()
            rows = c.execute(
                "SELECT backend_type, coalesce(wait_event_type,'CPU'), coalesce(wait_event,'-'), state "
                "FROM pg_stat_activity WHERE pid <> pg_backend_pid()").fetchall()
            out.append((t, rows, psutil.cpu_percent(None)))
            time.sleep(0.02)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=int, default=10000)
    ap.add_argument("--series", default="P0")
    ap.add_argument("--duration", type=int, default=15)
    a = ap.parse_args()

    s = matrix.SERIES[a.series]
    print("config:", s["setup"]())
    s["reset"]()
    samples: list = []
    stop = threading.Event()
    th = threading.Thread(target=sampler, args=(stop, samples), daemon=True)
    th.start()
    cfg = loadgen.parse(["--store", "pg", "--rate", str(a.rate), "--duration", str(a.duration),
                         "--warmup", "4", "--label", "diag", "--timeline"])
    cfg.pop("out")
    res = loadgen.run(cfg)
    stop.set()
    th.join()
    print("cpu sampled at:", [time.strftime("%H:%M:%S", time.localtime(x)) + f".{int(x*1000)%1000:03d}" for x in res["cpu_cores"]["sampled_at"]])
    print({k: res[k] for k in ("achieved_rate", "p50_ms", "p99_ms", "p999_ms", "svc_p99_ms", "gen_lag_p99_ms", "cpu_cores")})

    # timeline: worst 100ms buckets
    tl = res.pop("timeline")
    worst = sorted(tl, key=lambda b: -b[2])[:8]
    print("\nworst 100ms buckets (t_wall, n_ops, max_ms, p99_ms):")
    for b in sorted(worst):
        print("  ", time.strftime("%H:%M:%S", time.localtime(b[0])) + f".{int(b[0] * 1000) % 1000:03d}", b[1:])

    # wait events near the worst buckets vs overall
    def summarize(rows_list):
        cnt = collections.Counter()
        for rows in rows_list:
            for bt, wt, we, st in rows:
                if bt == "client backend" and st != "idle":
                    cnt[f"{wt}:{we}"] += 1
                elif bt != "client backend" and wt != "Activity":
                    cnt[f"[{bt}] {wt}:{we}"] += 1
        tot = len(rows_list) or 1
        return {k: round(v / tot, 2) for k, v in cnt.most_common(12)}

    print("\navg active backends by wait event, whole run:", json.dumps(summarize([r for _, r, _ in samples]), indent=1))
    stall_ts = [b[0] for b in worst if b[2] > 20]
    near = [r for t, r, _ in samples if any(abs(t - st) < 0.15 for st in stall_ts)]
    print(f"\nduring stalls ({len(near)} samples):", json.dumps(summarize(near), indent=1))
    cpu_near = [c for t, _, c in samples if any(abs(t - st) < 0.15 for st in stall_ts)]
    print("machine cpu% overall:", round(np.mean([c for *_, c in samples]), 1),
          " during stalls:", round(np.mean(cpu_near), 1) if cpu_near else None)
    with open(RESULTS / "diag.jsonl", "a") as f:
        f.write(json.dumps({"series": a.series, "rate": a.rate, **{k: v for k, v in res.items() if k != "applied_config"}}, default=str) + "\n")


if __name__ == "__main__":
    main()

"""Summarise results/matrix.jsonl -> results/summary.md + results/latency_vs_rate.png.
Every number is the MEDIAN of the repeats for that (series, rate) cell; raw runs stay in the jsonl."""
from __future__ import annotations

import json
import statistics
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from common import RESULTS, is_noisy  # noqa: E402

BUDGET_MS = 10
# Fixed categorical order (reference palette slots 1-5) + a distinct marker each, so identity
# never rides on color alone.
SLOTS = [("#2a78d6", "o"), ("#eb6834", "s"), ("#1baf7a", "^"), ("#eda100", "D"), ("#e87ba4", "v"), ("#008300", "P")]
CHART = {
    # one shared axis: what was measured row by row on AC power (P0 was on battery -> left out)
    "Postgres vs Redis, measured": ["P1", "P1-wt", "P2-1ms", "P2-1ms-wt", "R1", "R1-norepl"],
}
SHORT = {
    "P0": "no commit fsync", "P1": "Postgres single node", "P1-wt": "Postgres single node, durable flush",
    "P2-1ms": "Postgres + sync standby (+1 ms)", "P2-1ms-wt": "Postgres + sync standby (+1 ms), durable flush on primary only",
    "R0": "no persistence", "R1": "Redis Lua, AOF everysec + async replica (no WAIT)",
    "R1-norepl": "Redis Lua, AOF everysec, no replica", "R2": "AOF always",
    "R3-1ms": "WAIT 1, +1 ms", "R4-1ms": "WAITAOF 1 1, +1 ms",
}


def load():
    cells = defaultdict(list)
    desc, order = {}, []
    for line in open(RESULTS / "matrix.jsonl"):
        r = json.loads(line)
        if is_noisy(r):  # re-evaluated with the current rule, not the flag stored at run time
            continue  # disturbed by unrelated load on the machine (kept in the jsonl, excluded here)
        cells[(r["series"], r["rate"])].append(r)
        desc[r["series"]] = r["desc"]
        if r["series"] not in order:
            order.append(r["series"])
    med = {}
    for (s, rate), runs in cells.items():
        m = lambda k: statistics.median(x[k] for x in runs if x[k] is not None)
        bad = sum(x["counts"][k] for x in runs for k in ("error", "shed", "timeout", "unacked"))
        med[(s, rate)] = {
            "rate": rate, "n": len(runs), "achieved": m("achieved_rate"),
            "p50": m("p50_ms"), "p99": m("p99_ms"), "p999": m("p999_ms"), "svc99": m("svc_p99_ms"),
            "p99_runs": [round(x["p99_ms"], 2) for x in runs], "bad": bad,
            "cpu_machine": m_cpu(runs),
        }
    return med, desc, order


def m_cpu(runs):
    return statistics.median(x["cpu_cores"].get("db", 0) for x in runs)


def healthy(c):
    return c["achieved"] >= 0.95 * c["rate"] and c["bad"] == 0


def max_rate_within(med, s, limit_ms):
    ok = [c["rate"] for (ss, _), c in med.items() if ss == s and healthy(c) and c["p99"] <= limit_ms]
    return max(ok) if ok else None


def write_md(med, desc, order):
    out = ["# Quota check-and-deduct: local benchmark results", "",
           "Machine: Apple M4 (10 cores, 16 GB), macOS 26; Postgres 17.11, Redis 8.10; generator = "
           "8 Python processes (stand-ins for 8 instances) on the SAME machine as the DB.",
           "Open-loop Poisson arrivals; latency measured from each op's scheduled start (queueing included). "
           "Every cell = median of its clean runs (15 s measured after 4 s warmup). A run is discarded as noisy only on "
           "measurements independent of DB latency: processes OUTSIDE the benchmark used > 1 core, or the load generator "
           "fell > 5 ms (p99) behind its own launch schedule (a machine stall). All runs stay in `matrix.jsonl`.",
           "Injected latency = toxiproxy delay on the replication link only (app <-> DB is loopback).", "",
           "## Headline: highest tested rate that stays within budget", "",
           "| series | what | max rate p99 <= 5 ms | max rate p99 <= 10 ms | p99 @ 2k | p99 @ 10k | p99 @ 20k |",
           "|---|---|---:|---:|---:|---:|---:|"]
    fmt = lambda v: "-" if v is None else f"{v:,}"
    for s in order:
        at = lambda r: (f"{med[(s, r)]['p99']:.2f}" + ("" if healthy(med[(s, r)]) else " (!)")) if (s, r) in med else "-"
        out.append(f"| {s} | {desc[s]} | {fmt(max_rate_within(med, s, 5))} | {fmt(max_rate_within(med, s, 10))} "
                   f"| {at(2000)} | {at(10000)} | {at(20000)} |")
    out += ["", "(!) = rate not achieved or errors/timeouts. '-' = not reached (series stopped earlier) or not run.",
            "", "## All cells", "",
            "| series | target/s | achieved/s | p50 ms | p99 ms | p99.9 ms | svc p99 ms | p99 per run | bad ops | DB CPU (cores) |",
            "|---|---:|---:|---:|---:|---:|---:|---|---:|---:|"]
    for s in order:
        for (ss, rate), c in sorted(med.items(), key=lambda kv: kv[0][1]):
            if ss != s:
                continue
            out.append(f"| {s} | {rate:,} | {c['achieved']:,.0f} | {c['p50']:.2f} | {c['p99']:.2f} | {c['p999']:.2f} "
                       f"| {c['svc99']:.2f} | {c['p99_runs']} | {c['bad']} | {c['cpu_machine']:.1f} |")
    (RESULTS / "summary.md").write_text("\n".join(out) + "\n")


def chart(med):
    plt.rcParams.update({"font.size": 10, "axes.edgecolor": "#c3c2b7", "axes.labelcolor": "#3d3d3a",
                         "xtick.color": "#5f5e5a", "ytick.color": "#5f5e5a"})
    fig, axes = plt.subplots(1, len(CHART), figsize=(9.5, 5.6), sharey=True, facecolor="#fcfcfb", squeeze=False)
    axes = axes[0]
    for ax, (title, series) in zip(axes, CHART.items()):
        ax.set_facecolor("#fcfcfb")
        for (color, marker), s in zip(SLOTS, series):
            pts = sorted((c["rate"], c["p99"]) for (ss, _), c in med.items() if ss == s)
            if not pts:
                continue
            xs, ys = zip(*pts)
            ax.plot(xs, ys, color=color, lw=2, marker=marker, ms=6, label=SHORT[s],
                    markeredgecolor="#fcfcfb", markeredgewidth=1.5)
        ax.axhline(BUDGET_MS, color="#5f5e5a", lw=1, ls="--")
        ax.text(950, BUDGET_MS * 0.85, "10 ms budget", color="#5f5e5a", fontsize=9, ha="left", va="top")
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_xticks([1000, 2000, 5000, 10000, 20000, 30000])
        ax.set_xticklabels(["1k", "2k", "5k", "10k", "20k", "30k"])
        ax.set_xlabel("offered rate (ops/s)")
        ax.set_title(title, loc="left", fontsize=11, color="#1f1f1d")
        ax.grid(True, which="major", color="#e8e7e2", lw=0.8)
        ax.spines[["top", "right"]].set_visible(False)
        ax.legend(frameon=False, fontsize=9, loc="upper left")
    axes[0].set_ylabel("p99 latency, ms (log)")
    fig.suptitle("p99 of one check-and-deduct vs offered rate (median of clean runs, M4 laptop, AC power)", x=0.01, ha="left",
                 fontsize=12, color="#1f1f1d")
    fig.tight_layout()
    fig.savefig(RESULTS / "latency_vs_rate.png", dpi=150, facecolor=fig.get_facecolor())


if __name__ == "__main__":
    med, desc, order = load()
    write_md(med, desc, order)
    chart(med)
    print((RESULTS / "summary.md").read_text())

"""p99 against offered rate for the warm service runs -> results/latency_vs_rate.png.

Uses the last uniform `op` run labelled W at each rate in results/service.jsonl (the rows of
RESULTS.md's warm table).

  .venv/bin/python bench/service/plot.py
"""
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

RESULTS = Path(__file__).resolve().parent / "results"
BUDGET_MS = 10

runs = {}
for line in open(RESULTS / "service.jsonl"):
    r = json.loads(line)
    if r.get("label") == "W" and r["flow"] == "op" and r["mode"] == "uniform":
        runs[int(r["rate"])] = r          # later runs replace earlier ones
rates = sorted(runs)

plt.rcParams.update({"font.size": 10, "axes.edgecolor": "#c3c2b7", "axes.labelcolor": "#3d3d3a",
                     "xtick.color": "#5f5e5a", "ytick.color": "#5f5e5a"})
fig, ax = plt.subplots(figsize=(9.5, 5.2), facecolor="#fcfcfb")
ax.set_facecolor("#fcfcfb")
for key, label, color, marker in (("reserve_p99", "reserve", "#2a78d6", "o"),
                                  ("op_p99", "reserve + commit (whole operation)", "#eb6834", "s")):
    ax.plot(rates, [runs[x][key] for x in rates], color=color, lw=2, marker=marker, ms=6, label=label,
            markeredgecolor="#fcfcfb", markeredgewidth=1.5)
top = max(rates)
if runs[top]["achieved_rate"] < 0.95 * top:
    ax.annotate(f"only {runs[top]['achieved_rate'] / 1000:.1f}k achieved:\nlaptop out of CPU",
                (top, runs[top]["op_p99"]), textcoords="offset points", xytext=(-70, -20),
                ha="right", va="top", fontsize=9, color="#5f5e5a")
ax.axhline(BUDGET_MS, color="#5f5e5a", lw=1, ls="--")
ax.text(rates[0], BUDGET_MS * 1.12, "10 ms budget", color="#5f5e5a", fontsize=9, va="bottom")
ax.set_xscale("log")
ax.set_yscale("log")
ax.set_xticks(rates)
ax.set_xticklabels([f"{x // 1000}k" for x in rates])
ax.minorticks_off()
ax.set_xlabel("offered rate (ops/s)")
ax.set_ylabel("p99 latency, ms (log)")
ax.grid(True, which="major", color="#e8e7e2", lw=0.8)
ax.spines[["top", "right"]].set_visible(False)
ax.legend(frameon=False, fontsize=9, loc="upper left")
ax.set_title("Quota service: p99 against offered rate (8 instances, M4 laptop, everything on one machine)",
             loc="left", fontsize=11, color="#1f1f1d")
fig.tight_layout()
fig.savefig(RESULTS / "latency_vs_rate.png", dpi=150, facecolor=fig.get_facecolor())
print(RESULTS / "latency_vs_rate.png")

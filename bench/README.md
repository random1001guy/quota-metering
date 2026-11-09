# Benchmarks

Two benchmarks, both native on macOS (Homebrew), not Docker. Docker Desktop runs a Linux VM
that distorts fsync and network numbers.

- `stores/`: Postgres vs Redis for one check-and-deduct. Used to choose the source of truth.
  Results: `stores/results/summary.md`, graph `stores/results/latency_vs_rate.png`.
- `service/`: the quota service itself (reserve + commit over HTTP, 8 instances).
  Results: `service/RESULTS.md`.

Numbers in DESIGN.md come from an M4 MacBook Air (10 cores, 16 GB), on AC power, Low Power Mode
off, browser closed. Raw runs are in the `results/*.jsonl` files.

Run everything from a normal terminal. Processes started from some tools run at a lower priority
(nice 5), land on the efficiency cores and give much worse tails.

## Setup

```bash
brew install postgresql@17 redis toxiproxy
bench/stores/setup.sh        # Postgres :55432 (+ standby :55433), Redis :56379 (+ replica :56380), toxiproxy :8474
```

`setup.sh` makes its own venv in `bench/stores/.venv`. The service benchmark uses the repo venv:

```bash
python3 -m venv .venv && .venv/bin/pip install -e '.[bench]'
bench/service/setup.sh       # db quota_svc: seeded orgs, limits x1000, hot org 9001
```

## Stores: Postgres vs Redis

From `bench/stores/`:

```bash
.venv/bin/python correctness.py                    # both stores, concurrent oversubscription; must print ALL PASS
.venv/bin/python matrix.py --list                  # series (store configs)
.venv/bin/python matrix.py R1 --rates 2000         # one cell: one series at one rate, 3 runs
.venv/bin/python report.py                         # -> results/summary.md, results/latency_vs_rate.png
```

Series in the graph: `P1`, `P1-wt`, `P2-1ms`, `P2-1ms-wt`, `R1`, `R1-norepl`, each at
1k, 2k, 5k, 10k, 15k, 20k, 30k ops/s (a series stops once it is past its knee). Run one cell at a
time and check it before the next.

- Each cell: 3 runs of 15 s after 4 s warm-up; the report uses the median.
- Load is open loop (Poisson arrivals), latency measured from each op's scheduled start.
- 8 generator processes stand in for 8 instances; 5,000 orgs x 30 features.
- Replication delay is injected with toxiproxy on the replication link only.
- macOS `fsync()` doesn't flush the drive cache; `F_FULLFSYNC` does (`results/pg_test_fsync.txt`).
  So `fsync` series are optimistic and `-wt` series pessimistic. A cloud SSD sits between them.

`./teardown.sh` stops everything and deletes `bench/stores/data`.

## Service

From the repo root. Start the 8 instances in one terminal, run the load in another:

```bash
bench/service/serve.sh                                                    # ports 8101..8108
.venv/bin/python bench/service/loadgen.py --rate 2000 --workers 8 --conns 32 --label W-2k
```

| Result in DESIGN.md | Command |
|---|---|
| p99 against rate (graph) | `loadgen.py --rate R --workers 8 --conns 32 --label W`, R = 1000, 2000, 3000, 4000, 5000, 10000 |
| generator ceiling | `loadgen.py --flow healthz --rate 20000 --workers 8 --conns 32` |
| cold start | `bench/service/setup.sh`, then `loadgen.py --mode every --rate 2500 --duration 60 --workers 8 --conns 32` (every pair once; `--rate` is ignored) |
| hot key, chunk size | restart `serve.sh` with `LEASE_FRACTION` (e.g. `2.5e-10` = chunk 250, `2.5e-11` = chunk 25 on the 10^12 limit), run `bench/service/hot_reset.sh`, then `loadgen.py --mode hot --rate 2000 --workers 8 --conns 32` |

Each run appends one line to `service/results/service.jsonl`.
`.venv/bin/python bench/service/plot.py` redraws `service/results/latency_vs_rate.png` (copied to `docs/service-latency-vs-rate.png`).

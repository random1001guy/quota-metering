# Service load test: results (7 Nov 2026)

**Setup**
- **Machine:** MacBook Air M4, 10 cores (4 performance, 6 efficiency), 16 GB, on AC power, Low Power Mode off, Chrome closed.
- **Everything on one machine:**
  - the load generator;
  - 8 single-process service instances (`bench/service/serve.sh`, ports 8101–8108);
  - native Postgres 17 (fsync on, single node);
  - native Redis 8 (AOF everysec).
- **One op** = `POST /v1/reserve`, then `POST /v1/commit`, so each op is two HTTP requests.
- **Load:**
  - 8 generator processes, 32 connections each, spread round-robin across the instances (a stand-in for a load balancer);
  - Poisson arrivals, with latency timed from each op's scheduled start;
  - uniform over 5,000 orgs × 30 features;
  - half the ops are 1 unit, half are 1–100 units;
  - limits are scaled ×1000, so nothing hits 429.
- **Run length:** 20 s measured, after 4 s of warm-up.
- **Warm:** every lease already existed when the run started.

## Warm: p99 against rate (ms)

| ops/s | achieved | reserve p50 | reserve p99 | reserve p99.9 | commit p99 | op p99 | service CPU (cores) | stores CPU | generator CPU |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1,000 | 1,001 | 1.12 | 3.12 | 3.62 | 0.90 | 3.76 | 0.55 | 0.32 | 0.37 |
| 2,000 | 2,004 | 0.94 | **2.20** | 26.8 | 1.21 | 3.07 | 1.19 | 0.59 | 0.74 |
| 3,000 | 3,006 | 1.08 | **2.31** (one earlier run: 10.4) | 3.09 | 1.56 | 3.52 | 2.03 | 0.94 | 1.38 |
| 4,000 | 4,009 | 1.64 | 12.2 | 19.4 | 5.85 | 17.3 | 3.48 | 1.25 | 2.15 |
| 5,000 | 5,015 | 1.44 | 15.2 (repeat: 14.2) | 33.6 | 11.2 | 25.1 | 3.56 | 1.19 | 2.32 |
| 10,000 | **7,507** | 3,376 | 4,050 | 4,162 | 40 | 4,071 | 4.87 | 1.00 | 2.84 |

The 1k row was measured with the earlier single-socket setup: one uvicorn with `--workers 8`. Its numbers match the 8-instance setup at 2k.

15k/s was not run. At 10k/s the laptop already saturates at about 7.5k ops/s (15k HTTP requests/s), shedding load and with a p99 of seconds.

## Cold start: the first touch of every lease (empty Redis, empty `quota_periods`)

| ops/s | reserve p50 | reserve p99 | op p99 | service CPU | stores CPU |
|---:|---:|---:|---:|---:|---:|
| 2,500 (all 150k pairs once, over 60 s) | 4.78 | 126 | 135 | 3.69 | 1.96 |
| 5,000 (uniform, mostly cold) | 305 | 7,650 | 7,680 | 4.73 | 1.28 |

## Hot partition: all traffic on one (org, feature)

**Setup**
- **Org 9001, `feature1`**, with a limit of 10¹² so it never runs out mid-run.
- **Chunk size:** set with `LEASE_FRACTION` (default 5% of the limit), so the chunk can vary while the limit stays fixed.
- **Load:** 2k ops/s, about 51.5k units/s (mean 25.75 units per op).
- **Fresh lease each run** (`bench/service/hot_reset.sh`).
- **Grants per second** = Postgres `granted` ÷ chunk ÷ 24 s.

| chunk (units) | Postgres grants/s | reserve p50 | reserve p99 | reserve p99.9 | 503 Busy | result |
|---:|---:|---:|---:|---:|---:|---|
| no refills (5% of 10¹²) | 0 | 0.80 | 1.83 | 2.22 | 0 | same as uniform |
| no refills, 3k ops/s | 0 | 0.86 | 2.17 (one earlier run: 6.49) | 32.7 | 0 | |
| 50,000 | ~1 | 0.79 | 1.83 | 2.21 | 0 | no change |
| 5,000 | ~10 | 0.81 | 2.28 | 10.3 | 0 | no change |
| 1,000 | ~52 | 0.79 | 1.88 | 3.61 | 0 | no change |
| 250 | ~200 | 0.92 | 4.60 | 15.7 | 0 | tail starts to grow |
| 25 (smaller than most batches) | most ops refill | 1.39 | **174** | 182 | **1,921 (4%)** | **falls over** |

**Takeaways**
- **One hot key is not a Redis problem.** It is as fast as spread-out traffic at the same rate.
- **Refills keep up until the chunk covers only about 5 ms of the key's own consumption** (250 units at 51.5k units/s). Below that, ops queue behind the single refill in flight. They wait through the backoff schedule (`_LOCK_WAITS_S`, about 170 ms in total) and then get 503.
- **The 5% default leaves a very large margin.** A chunk is 5% of the monthly limit, about 1.5 days of the org's average monthly rate. Refill starvation would need a burst more than 25,000× that average.
- **The realistic risk is a small-limit org bursting with big batches.** For example, a 1,000-unit limit has a chunk of 50, smaller than one 100-unit batch. Every batch then needs a refill, and those refills run one at a time. The total is still capped exactly at the limit (correctness holds), but that org sees 503s instead of 200s. Fix if needed: a minimum chunk (for example, at least the largest batch, or a few seconds of recent use).

## What the numbers say

- **Within budget at the brief's sustained rate.** At 2k ops/s, reserve p99 is 2.2 ms and the whole op (reserve + commit) is 3.1 ms, inside the 10 ms budget. On this laptop the p99 stays within 10 ms up to about 3k ops/s.
- **The machine is the bottleneck, not Redis or Postgres.** At 4–5k ops/s the generator, the 8 instances and the stores use about 7–8 cores, but there are only 4 performance cores. The service's CPU grows faster than its rate once work spills onto the efficiency cores. The stores never use more than 1.3 cores.
- **Service cost: about 0.6 ms of CPU per op** (about 0.3 ms per HTTP request, in FastAPI + Pydantic + redis-py), measured at 2k/s. So one core serves roughly 1.6k ops/s flat out. For 10k/s at about 50% utilisation that means roughly 12 service cores; the brief's 8 instances would each run at about 80%. In production, the generator and the stores wouldn't share the box.
- **Cold start is about 10× a warm op**, from config loads, lease setup, and the grant through Postgres. A cold storm at 5k/s collapses. Two things contain it:
  - per-org anchor days already spread period resets: about 1/31 of orgs reset on a given day;
  - pre-creating leases ahead of the reset would remove it; pre-creating only the Postgres rows would remove just the INSERT.

## Issues found while load testing

- **The Redis client pool failed instead of waiting.** redis-py 8's default pool raises `MaxConnectionsError` as soon as more than 100 calls are in flight. Under overload that returned 503s and left refill locks held until their 2 s expiry. Fixed: the service now uses a `BlockingConnectionPool` (`REDIS_POOL_SIZE=64`, 1 s wait).
- **`uvicorn --workers` on macOS spreads connections unevenly.** One worker took 45% of the connections, which made p99 at 5k ops/s 69 ms instead of 14 ms. The load test therefore runs 8 separate instances, with requests spread across them.
- **Occasional tail spikes at a fixed rate** (3k/s: 10.4 ms once, 2.3 ms on the repeat). The suspected cause, not yet confirmed, is the 60 s org-config cache expiring in lockstep across all orgs. The likely fix is a jittered TTL or refreshing entries just before they expire.

## Measurement notes

- Each process started from Claude's tool shell ran at nice 5, which put it on the efficiency cores. With that, even `/healthz` had a p99 of 345 ms at 20k req/s. All results above were run from a normal-priority terminal.
- **Calibration:** `/healthz` (no quota work) gives p99 0.96 ms at 20k req/s, and 18.6 ms at 30k req/s, where the machine runs out of CPU.
- Raw runs are in `results/service.jsonl`.

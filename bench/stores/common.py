"""Shared constants for the benchmark scripts."""
from pathlib import Path

BENCH = Path(__file__).resolve().parent
RESULTS = BENCH / "results"

PG_DSN = "host=127.0.0.1 port=55432 user=postgres dbname=quota"
PG_ADMIN_DSN = "host=127.0.0.1 port=55432 user=postgres dbname=postgres"
REDIS_PORT = 56379
REDIS_REPLICA_PORT = 56380
TOXI = "http://127.0.0.1:8474"

N_ORGS = 5_000
N_FEATURES = 30
PERIOD = "2026-11"
BIG_LIMIT = 10**15  # latency runs: never reject
IDEMPOTENCY_TTL_S = 3600

LUA = (BENCH / "quota.lua").read_text()


def qkey(org: int, feat: int, period: str = PERIOD) -> str:
    return f"q:{{{org}}}:{feat}:{period}"


def reqkey(org: int, req: str) -> str:
    return f"req:{{{org}}}:{req}"


# A run is NOISY (machine disturbed the measurement, not the DB) if either:
#  - processes outside the benchmark used > 1 core during it, or
#  - the generator fell behind its own launch schedule: p99 lag > 5 ms. Observed this session:
#    clean runs 0.2-2.2 ms (higher at low rates / slow ops: longer sleeps), machine stalls >= 6 ms.
NOISE_OTHER_CORES = 1.0
NOISE_GEN_LAG_MS = 5.0


def is_noisy(res: dict) -> bool:
    return (res["cpu_cores"].get("other", 0) > NOISE_OTHER_CORES
            or (res.get("gen_lag_p99_ms") or 0) > NOISE_GEN_LAG_MS)

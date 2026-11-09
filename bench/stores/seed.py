"""Seed 5,000 orgs x 30 features = 150k counters in Postgres and Redis (huge limits -> no rejections).
Also clears previous charges / idempotency keys so every run starts from the same state."""
import psycopg
import redis

from common import BIG_LIMIT, N_FEATURES, N_ORGS, PERIOD, PG_DSN, REDIS_PORT, qkey


def seed_pg() -> None:
    with psycopg.connect(PG_DSN, autocommit=True) as c:
        c.execute("TRUNCATE quota, charges")
        with c.cursor() as cur:
            with cur.copy("COPY quota (org_id, feature, period, quota_limit, used) FROM STDIN") as cp:
                for org in range(N_ORGS):
                    for f in range(N_FEATURES):
                        cp.write_row((org, f, PERIOD, BIG_LIMIT, 0))
        c.execute("VACUUM ANALYZE quota")
        c.execute("VACUUM ANALYZE charges")
        n = c.execute("SELECT count(*) FROM quota").fetchone()[0]
    print(f"postgres: {n} quota rows")


def seed_redis() -> None:
    r = redis.Redis(port=REDIS_PORT)
    r.flushall()
    p = r.pipeline(transaction=False)
    for org in range(N_ORGS):
        for f in range(N_FEATURES):
            p.hset(qkey(org, f), mapping={"used": 0, "limit": BIG_LIMIT})
        if org % 200 == 0:
            p.execute()
    p.execute()
    print(f"redis: {r.dbsize()} keys")


if __name__ == "__main__":
    seed_pg()
    seed_redis()

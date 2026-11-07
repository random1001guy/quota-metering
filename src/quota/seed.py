"""Seed placeholder plans, features and orgs.

  python -m quota.seed            # seeds once; does nothing if orgs already exist
  python -m quota.seed --force    # wipe and reseed

3 plans; 30 features (feature1..feature30), each with its own base limit; pro gets 10x and
enterprise 100x the starter limit. Orgs org1..orgN get a random plan (70/25/5) and a random
anchor day 1..31 (29-31 exercise the month-end clamp). Deterministic (fixed RNG seed).
"""
import os
import random
import sys

import psycopg

PLANS = {"starter": 1, "pro": 10, "enterprise": 100}  # plan -> multiplier on the base limit
PLAN_WEIGHTS = {"starter": 70, "pro": 25, "enterprise": 5}


def seed(conn: psycopg.Connection, n_orgs: int = 5000, n_features: int = 30, rng_seed: int = 42) -> None:
    rng = random.Random(rng_seed)
    with conn.transaction():
        conn.execute("TRUNCATE quota_periods, orgs, plan_limits, features, plans")
        with conn.cursor() as cur:
            cur.executemany("INSERT INTO plans (plan_id) VALUES (%s)", [(p,) for p in PLANS])
            cur.executemany("INSERT INTO features (feature_id, name) VALUES (%s, %s)",
                            [(f, f"feature{f}") for f in range(1, n_features + 1)])
            base = {f: rng.randrange(100, 5001, 50) for f in range(1, n_features + 1)}
            cur.executemany(
                "INSERT INTO plan_limits (plan_id, feature_id, monthly_limit) VALUES (%s, %s, %s)",
                [(p, f, base[f] * mult) for p, mult in PLANS.items() for f in base])
            plans = rng.choices(list(PLAN_WEIGHTS), weights=list(PLAN_WEIGHTS.values()), k=n_orgs)
            with cur.copy("COPY orgs (org_id, name, plan_id, anchor_day) FROM STDIN") as cp:
                for i in range(1, n_orgs + 1):
                    cp.write_row((i, f"org{i}", plans[i - 1], rng.randint(1, 31)))


def ensure_schema(conn: psycopg.Connection, path: str) -> None:
    """Create the tables on first start (the schema file ships inside the image)."""
    if conn.execute("SELECT to_regclass('public.orgs')").fetchone()[0] is None:
        with open(path) as f:
            conn.execute(f.read())
        print(f"seed: created schema from {path}")


def main() -> None:
    url = os.environ.get("DATABASE_URL", "postgresql://quota:quota@localhost:5432/quota")
    n_orgs = int(os.environ.get("SEED_ORGS", "5000"))
    with psycopg.connect(url, autocommit=True) as conn:
        ensure_schema(conn, os.environ.get("SCHEMA_PATH", "db/schema.sql"))
        existing = conn.execute("SELECT count(*) FROM orgs").fetchone()[0]
        if existing and "--force" not in sys.argv:
            print(f"seed: {existing} orgs already present, skipping (use --force to reseed)")
            return
        seed(conn, n_orgs)
        print(f"seed: {n_orgs} orgs, 30 features, {len(PLANS)} plans")


if __name__ == "__main__":
    main()

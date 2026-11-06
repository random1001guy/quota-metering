"""Postgres access: org config and the per-period grant ledger."""
from dataclasses import dataclass

from psycopg_pool import AsyncConnectionPool

from quota.periods import Period


@dataclass(frozen=True)
class OrgConfig:
    org_id: int
    anchor_day: int
    status: str
    limits: dict[int, int]  # feature_id -> monthly limit


# Create the period's row if needed and move up to `want` units into the lease, never past the
# row's limit. The row lock taken by ON CONFLICT DO UPDATE serialises concurrent refills, so two
# instances can never both grant the last units. Returns the new running total; no row is
# returned when the limit was already fully granted.
GRANT_SQL = """
INSERT INTO quota_periods AS q
       (org_id, feature_id, period_start, period_end, quota_limit, granted)
VALUES (%(org)s, %(feat)s, %(start)s, %(end)s, %(limit)s, LEAST(%(want)s, %(limit)s))
ON CONFLICT (org_id, feature_id, period_start) DO UPDATE
   SET granted = LEAST(q.quota_limit, q.granted + %(want)s)
 WHERE q.granted < q.quota_limit
RETURNING granted, quota_limit
"""

ORG_SQL = """
SELECT o.anchor_day, o.status, pl.feature_id, pl.monthly_limit
  FROM orgs o
  LEFT JOIN plan_limits pl USING (plan_id)
 WHERE o.org_id = %s
"""


class PgStore:
    def __init__(self, pool: AsyncConnectionPool):
        self.pool = pool

    async def load_features(self) -> dict[str, int]:
        async with self.pool.connection() as c:
            rows = await (await c.execute("SELECT name, feature_id FROM features")).fetchall()
        return {name: fid for name, fid in rows}

    async def load_org(self, org_id: int) -> OrgConfig | None:
        async with self.pool.connection() as c:
            rows = await (await c.execute(ORG_SQL, (org_id,))).fetchall()
        if not rows:
            return None
        anchor_day, status = rows[0][0], rows[0][1]
        limits = {feat: limit for _, _, feat, limit in rows if feat is not None}
        return OrgConfig(org_id, anchor_day, status, limits)

    async def grant(self, org_id: int, feature_id: int, period: Period, limit: int,
                    want: int) -> tuple[int, int] | None:
        """Grant up to `want` units. Returns (granted_total, quota_limit), or None if exhausted."""
        params = {"org": org_id, "feat": feature_id, "start": period.start, "end": period.end,
                  "limit": limit, "want": want}
        async with self.pool.connection() as c:
            return await (await c.execute(GRANT_SQL, params)).fetchone()

    async def current(self, org_id: int, feature_id: int, period: Period) -> tuple[int, int] | None:
        """(granted_total, quota_limit) for the period, or None if the row doesn't exist yet."""
        async with self.pool.connection() as c:
            cur = await c.execute(
                "SELECT granted, quota_limit FROM quota_periods "
                "WHERE org_id = %s AND feature_id = %s AND period_start = %s",
                (org_id, feature_id, period.start))
            return await cur.fetchone()

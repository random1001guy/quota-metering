-- Quota metering schema (Postgres).
-- Postgres holds configuration and the per-period grant ledger. Live usage (balance, reserved,
-- used) lives in Redis; Postgres only sees quota move into Redis in lease-sized chunks.

CREATE TABLE plans (
    plan_id text PRIMARY KEY                      -- 'starter', 'pro', 'enterprise'
);

CREATE TABLE features (
    feature_id smallint PRIMARY KEY,              -- 1..30
    name       text UNIQUE NOT NULL               -- 'feature1'..'feature30'
);

-- What each plan gives per feature per period.
CREATE TABLE plan_limits (
    plan_id       text     NOT NULL REFERENCES plans,
    feature_id    smallint NOT NULL REFERENCES features,
    monthly_limit bigint   NOT NULL CHECK (monthly_limit >= 0),
    PRIMARY KEY (plan_id, feature_id)
);

CREATE TABLE orgs (
    org_id     int PRIMARY KEY,
    name       text UNIQUE NOT NULL,              -- 'org1'..'org5000'
    plan_id    text NOT NULL REFERENCES plans,
    anchor_day smallint NOT NULL CHECK (anchor_day BETWEEN 1 AND 31),  -- billing day, UTC
    status     text NOT NULL DEFAULT 'active',
    created_at timestamptz NOT NULL DEFAULT now()
);

-- One row per (org, feature, period), created lazily on the first lease grant of the period.
--   quota_limit: copied from plan_limits at creation, frozen for the period.
--   granted:     total units moved into the Redis lease so far. Only goes up. Not usage.
CREATE TABLE quota_periods (
    org_id       int      NOT NULL REFERENCES orgs,
    feature_id   smallint NOT NULL REFERENCES features,
    period_start date     NOT NULL,
    period_end   date     NOT NULL,
    quota_limit  bigint   NOT NULL,
    granted      bigint   NOT NULL DEFAULT 0,
    PRIMARY KEY (org_id, feature_id, period_start),
    CHECK (granted >= 0 AND granted <= quota_limit)
);

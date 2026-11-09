-- Quota counter + idempotency record. One logical op = one call to charge() = one commit.
CREATE TABLE IF NOT EXISTS quota (
    org_id      int      NOT NULL,
    feature     smallint NOT NULL,
    period      text     NOT NULL,          -- '2026-11' (calendar month UTC; new month = new row)
    quota_limit bigint   NOT NULL,
    used        bigint   NOT NULL DEFAULT 0 CHECK (used >= 0 AND used <= quota_limit),
    PRIMARY KEY (org_id, feature, period)
) WITH (fillfactor = 70);                   -- room for HOT updates (counter churn)

CREATE TABLE IF NOT EXISTS charges (
    request_id uuid     PRIMARY KEY,        -- client idempotency key
    org_id     int      NOT NULL,
    feature    smallint NOT NULL,
    period     text     NOT NULL,
    units      int      NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Returns: used-after-charge (>=0) on success,
--          -1 rejected (would exceed limit; nothing recorded),
--          -2 duplicate request_id (already charged; caller returns stored result),
--          -3 no quota row.
-- INSERT first: a concurrent duplicate blocks on the PK until the first commits, then sees the conflict.
CREATE OR REPLACE FUNCTION charge(p_req uuid, p_org int, p_feat smallint, p_period text, p_n int)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE v_used bigint;
BEGIN
    INSERT INTO charges (request_id, org_id, feature, period, units)
    VALUES (p_req, p_org, p_feat, p_period, p_n)
    ON CONFLICT (request_id) DO NOTHING;
    IF NOT FOUND THEN
        RETURN -2;
    END IF;

    UPDATE quota SET used = used + p_n
     WHERE org_id = p_org AND feature = p_feat AND period = p_period
       AND used + p_n <= quota_limit
    RETURNING used INTO v_used;

    IF NOT FOUND THEN
        DELETE FROM charges WHERE request_id = p_req;   -- same txn: rejected ops leave no record
        IF EXISTS (SELECT 1 FROM quota WHERE org_id = p_org AND feature = p_feat AND period = p_period) THEN
            RETURN -1;
        END IF;
        RETURN -3;
    END IF;
    RETURN v_used;
END $$;

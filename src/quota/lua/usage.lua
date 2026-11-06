-- Reclaim overdue reservations, then read the lease, so a reader never sees stale holds.
-- KEYS: 1 lease, 2 deadline set    ARGV: 1 now_ms, 2 idempotency window s
-- Returns {used, reserved, balance, granted_total, exhausted}, or {} if there is no lease.
local lease, dl = KEYS[1], KEYS[2]
if redis.call('EXISTS', lease) == 0 then return {} end
reclaim(lease, dl, ARGV[1], 1000, ARGV[2])
local f = redis.call('HMGET', lease, 'used', 'reserved', 'balance', 'granted_total', 'exhausted')
return {tonumber(f[1]), tonumber(f[2]), tonumber(f[3]), tonumber(f[4]), tonumber(f[5])}

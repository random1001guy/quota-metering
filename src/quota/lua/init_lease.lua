-- Create the period's lease if it doesn't exist. `baseline` is Postgres's granted total at
-- this moment: anything granted before the lease existed (e.g. a lease lost with Redis data)
-- is written off, never re-added, so a lost lease can only under-serve.
-- KEYS: 1 lease    ARGV: 1 baseline, 2 expire_at_ms
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
redis.call('HSET', KEYS[1], 'balance', 0, 'reserved', 0, 'used', 0,
           'granted_total', ARGV[1], 'exhausted', 0)
redis.call('PEXPIREAT', KEYS[1], ARGV[2])
return 1

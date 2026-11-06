-- Bring the lease up to Postgres's granted total. Takes a total, not a delta, so it is
-- idempotent and order-independent: a stale or repeated apply is a no-op, and a grant
-- stranded by a crash is picked up by the next apply.
-- KEYS: 1 lease, 2 refill lock    ARGV: 1 total, 2 exhausted (0/1), 3 lock token
-- Returns the new balance, or -1 if the lease doesn't exist.
local lease, lock = KEYS[1], KEYS[2]
local result = -1
if redis.call('EXISTS', lease) == 1 then
  local total = tonumber(ARGV[1])
  local have = tonumber(redis.call('HGET', lease, 'granted_total'))
  if total > have then
    redis.call('HINCRBY', lease, 'balance', total - have)
    redis.call('HSET', lease, 'granted_total', total)
  end
  if ARGV[2] == '1' then redis.call('HSET', lease, 'exhausted', 1) end
  result = tonumber(redis.call('HGET', lease, 'balance'))
end
if redis.call('GET', lock) == ARGV[3] then redis.call('DEL', lock) end
return result

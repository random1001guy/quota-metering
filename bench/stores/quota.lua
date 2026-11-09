-- Atomic check-and-deduct + idempotency record in ONE script (Redis runs scripts to completion,
-- one at a time, so no other command interleaves).
-- KEYS[1] = req:{org}:<request_id>     idempotency record (same hash slot as the counter)
-- KEYS[2] = q:{org}:<feature>:<period> hash {used, limit}
-- ARGV[1] = units, ARGV[2] = idempotency TTL seconds
-- Returns same codes as Postgres charge(): used-after (>=0), -1 rejected, -2 duplicate, -3 no quota.
local n = tonumber(ARGV[1])
if redis.call('EXISTS', KEYS[1]) == 1 then
  return -2
end
local h = redis.call('HMGET', KEYS[2], 'used', 'limit')
local limit = tonumber(h[2])
if not limit then
  return -3
end
local used = tonumber(h[1]) or 0
if used + n > limit then
  return -1
end
local new_used = redis.call('HINCRBY', KEYS[2], 'used', n)
redis.call('SET', KEYS[1], n, 'EX', tonumber(ARGV[2]))
return new_used

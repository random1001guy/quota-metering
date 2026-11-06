-- Hold n units from the lease, all or nothing.
-- KEYS: 1 lease, 2 deadline set, 3 refill lock, 4 reservation
-- ARGV: 1 n, 2 now_ms, 3 deadline_ms, 4 expire_at_ms (lease lifetime), 5 watermark,
--       6 lock token, 7 lock ttl ms, 8 idempotency window s
-- Returns one of:
--   {'DUP', state, n}         request_id seen before: nothing changes
--   {'INIT', n, got_lock}     no lease for this period yet
--   {'REFILL', short, got_lock} balance too small, Postgres may have more
--   {'REJECT', balance, 0}    balance too small and Postgres has nothing left
--   {'OK', balance, hint}     held; hint=1 means caller took the lock to refill in background
local lease, dl, lock, res = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local n = tonumber(ARGV[1])

local function try_lock()
  if redis.call('SET', lock, ARGV[6], 'NX', 'PX', ARGV[7]) then return 1 end
  return 0
end

local state = redis.call('HGET', res, 'state')
if state then
  return {'DUP', state, tonumber(redis.call('HGET', res, 'n'))}
end
if redis.call('EXISTS', lease) == 0 then
  return {'INIT', n, try_lock()}
end

reclaim(lease, dl, ARGV[2], 20, ARGV[8])

local f = redis.call('HMGET', lease, 'balance', 'exhausted')
local balance, exhausted = tonumber(f[1]), f[2] == '1'
if balance < n then
  if exhausted then return {'REJECT', balance, 0} end
  return {'REFILL', n - balance, try_lock()}
end

redis.call('HINCRBY', lease, 'balance', -n)
redis.call('HINCRBY', lease, 'reserved', n)
redis.call('HSET', res, 'state', 'reserved', 'n', n, 'lease', lease, 'dl', dl)
redis.call('PEXPIREAT', res, ARGV[4])   -- kept for the lease's lifetime while open
redis.call('ZADD', dl, ARGV[3], res)
redis.call('PEXPIREAT', dl, ARGV[4])

local hint = 0
if not exhausted and balance - n < tonumber(ARGV[5]) then hint = try_lock() end
return {'OK', balance - n, hint}

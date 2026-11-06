-- Reserved units go back to the balance (downstream failed).
-- KEYS: 1 reservation    ARGV: 1 idempotency window s
-- Returns OK | ALREADY (released or reclaimed before) | CONFLICT (was committed) | UNKNOWN
local res = KEYS[1]
local f = redis.call('HMGET', res, 'state', 'n', 'lease', 'dl')
local state = f[1]
if not state then return 'UNKNOWN' end
if state == 'released' or state == 'expired' then return 'ALREADY' end
if state == 'committed' then return 'CONFLICT' end
local n = tonumber(f[2])
redis.call('HINCRBY', f[3], 'reserved', -n)
redis.call('HINCRBY', f[3], 'balance', n)
redis.call('HSET', res, 'state', 'released')
redis.call('EXPIRE', res, ARGV[1])
redis.call('ZREM', f[4], res)
return 'OK'

-- Reserved units become used.
-- KEYS: 1 reservation    ARGV: 1 idempotency window s
-- The lease and deadline-set key names are read from the reservation (same {org} slot).
-- Returns OK | ALREADY (committed before) | CONFLICT (was released) |
--         EXPIRED (deadline passed, units reclaimed, not charged) | UNKNOWN
local res = KEYS[1]
local f = redis.call('HMGET', res, 'state', 'n', 'lease', 'dl')
local state = f[1]
if not state then return 'UNKNOWN' end
if state == 'committed' then return 'ALREADY' end
if state == 'released' then return 'CONFLICT' end
if state == 'expired' then return 'EXPIRED' end
local n = tonumber(f[2])
redis.call('HINCRBY', f[3], 'reserved', -n)
redis.call('HINCRBY', f[3], 'used', n)
redis.call('HSET', res, 'state', 'committed')
redis.call('EXPIRE', res, ARGV[1])
redis.call('ZREM', f[4], res)
return 'OK'

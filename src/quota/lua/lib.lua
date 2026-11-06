-- Shared helper, prepended to scripts that need it.
-- Return every reservation on this lease whose deadline has passed to the balance.
-- Members of the deadline set are reservation keys (same {org} hash tag, so same slot).
local function reclaim(lease, dl, now_ms, max, window_s)
  local due = redis.call('ZRANGEBYSCORE', dl, '-inf', now_ms, 'LIMIT', 0, max)
  for _, rk in ipairs(due) do
    if redis.call('HGET', rk, 'state') == 'reserved' then
      local n = tonumber(redis.call('HGET', rk, 'n'))
      redis.call('HINCRBY', lease, 'reserved', -n)
      redis.call('HINCRBY', lease, 'balance', n)
      redis.call('HSET', rk, 'state', 'expired')
      redis.call('EXPIRE', rk, window_s)
    end
    redis.call('ZREM', dl, rk)
  end
  return #due
end

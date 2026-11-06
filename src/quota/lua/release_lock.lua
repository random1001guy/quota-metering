-- Release the refill lock only if we still hold it.
-- KEYS: 1 lock    ARGV: 1 token
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0

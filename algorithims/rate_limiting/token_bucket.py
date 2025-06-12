"""
* Token Bucket Rate Limiter using Redis
* DRY, SOLID, CI/CD, and type safety best practices
"""
import logging
import time
from app.core.redis.redis_cache import RedisCache

# ! Uses atomic Lua script for token refill and consume
# todo: Add fail-open logic and Prometheus metrics if needed

TOKEN_BUCKET_LUA = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local interval = tonumber(ARGV[3])
local now = tonumber(ARGV[4])
local bucket = redis.call('HMGET', key, 'tokens', 'last')
local tokens = tonumber(bucket[1]) or capacity
local last = tonumber(bucket[2]) or now
local delta = math.max(0, now - last)
local refill = math.floor(delta / interval) * refill_rate
local new_tokens = math.min(capacity, tokens + refill)
if new_tokens > 0 then
  new_tokens = new_tokens - 1
  redis.call('HMSET', key, 'tokens', new_tokens, 'last', now)
  redis.call('EXPIRE', key, interval * 2)
  return 1
else
  redis.call('HMSET', key, 'tokens', new_tokens, 'last', now)
  redis.call('EXPIRE', key, interval * 2)
  return 0
end
"""

async def is_allowed_token_bucket(
    cache: RedisCache, key: str, capacity: int, refill_rate: int, interval: int
) -> bool:
    """
    * Token Bucket Rate Limiter
    Args:
        key (str): Unique identifier for the bucket (user ID, IP, etc.)
        capacity (int): Max tokens in bucket
        refill_rate (int): Tokens added per interval
        interval (int): Interval in seconds
    Returns:
        bool: True if allowed, False if rate limited
    """
    try:
        now = int(time.time())
        allowed = await cache._client.eval(
            TOKEN_BUCKET_LUA,
            1,
            key,
            capacity,
            refill_rate,
            interval,
            now,
        )
        return allowed == 1
    except Exception as e:
        # ! Fail-open: If Redis is unavailable, allow the event and log a warning
        logging.warning(f"[token_bucket] Redis unavailable, allowing event (fail-open): {e}")
        return True

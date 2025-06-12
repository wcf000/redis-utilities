"""
* Throttle utility using Redis
* DRY, SOLID, CI/CD, and type safety best practices
"""
import logging
import time
from app.core.redis.redis_cache import RedisCache

# ! Throttle: allow one event per interval
# todo: Add fail-open logic and Prometheus metrics if needed

async def is_allowed_throttle(
    cache: RedisCache, key: str, interval: int
) -> bool:
    """
    * Throttle: allow only one event per interval
    Args:
        key (str): Unique identifier for the throttle (user ID, IP, etc.)
        interval (int): Interval in seconds
    Returns:
        bool: True if allowed, False if throttled
    """
    try:
        now = int(time.time())
        result = await cache._client.setnx(key, now)
        if result:
            await cache._client.expire(key, interval)
            return True
        return False
    except Exception as e:
        # ! Fail-open: If Redis is unavailable, allow the event and log a warning
        logging.warning(f"[throttle] Redis unavailable, allowing event (fail-open): {e}")
        return True

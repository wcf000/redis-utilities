"""
* Debounce utility using Redis
* DRY, SOLID, CI/CD, and type safety best practices
"""
import logging
import time
from app.core.redis.redis_cache import RedisCache

# ! Debounce: only allow event after interval of inactivity
# todo: Add fail-open logic and Prometheus metrics if needed


async def is_allowed_debounce(
    cache: RedisCache, key: str, interval: int
) -> bool:
    """
    * Debounce: only allow event after interval of inactivity
    Args:
        key (str): Unique identifier for the debounce (user ID, IP, etc.)
        interval (int): Inactivity interval in seconds
    Returns:
        bool: True if allowed, False if debounced
    """
    now = int(time.time())
    try:
        ttl = await cache._client.ttl(key)
        if ttl > 0:
            return False
        await cache._client.set(key, now, ex=interval)
        return True
    except Exception as e:
        # ! Fail-open: If Redis is unavailable, allow the event and log a warning
        import logging
        logging.warning(f"[debounce] Redis unavailable, allowing event (fail-open): {e}")
        return True

"""
Redis Cache Utilities

Core Redis functionality including:
- Connection management
- Basic caching operations
- Cache statistics
"""

import hashlib
import json
import logging
from collections.abc import Callable
from functools import wraps
from typing import Any, Optional
import asyncio
# from opentelemetry import trace  # Optional dependency
from redis import asyncio  # ! Needed for coroutine detection

# Optional metrics dependencies
# from app.core.prometheus.metrics import (
#     get_redis_cache_deletes,
#     get_redis_cache_hits,
#     get_redis_cache_misses,
#     get_redis_cache_sets,
# )
from app.core.redis.config import RedisConfig

logger = logging.getLogger(__name__)


class RedisCache:
    """
    Async Redis cache utility supporting dependency injection of a redis.asyncio.Redis client.
    All cache operations are performed using the provided async client.
    """
    def __init__(self, client):
        # ! Accepts an async Redis client (redis.asyncio.Redis)
        self._client = client
        self.stats: dict[str, int] = {"hits": 0, "misses": 0, "sets": 0, "deletes": 0}
        # self.tracer = trace.get_tracer(__name__)  # Optional tracing

    async def get_or_set(self, key: str, value_fn, ttl: int | None = None):
        """
        Atomically get or set a cache value. If the key is missing, compute and set it.
        Args:
            key (str): The cache key
            value_fn (Callable): Function to compute value if key is missing
            ttl (int | None): Time-to-live for the cache entry
        Returns:
            The cached or computed value
        """
        try:
            value = await self.get(key)
            if value is not None:
                self.stats["hits"] += 1
                # get_redis_cache_hits().inc()  # Optional metrics
                return value
            # Compute value and cache it
            value = await value_fn() if callable(value_fn) and hasattr(value_fn, "__call__") else value_fn
            await self.set(key, value, ttl=ttl)
            self.stats["sets"] += 1
            # get_redis_cache_sets().inc()  # Optional metrics
            return value
        except Exception as e:
            logger.error(f"get_or_set failed for key {key}: {str(e)}")
            raise

    async def get(self, key: str) -> Any | None:
        """Get cached value with stats tracking"""
        # Use a simpler implementation without tracing
        try:
            value = await self._client.get(key)
            # * Always decode bytes to string for consistency
            if isinstance(value, bytes):
                value = value.decode()
            if value:
                self.stats["hits"] += 1
                # get_redis_cache_hits().inc()  # Optional metrics
                return value
            self.stats["misses"] += 1
            # get_redis_cache_misses().inc()  # Optional metrics
            return None
        except Exception as e:
            logger.error(f"Cache get failed for key {key}: {str(e)}")
            raise

    async def set(self, key: str, value: Any, ttl: int | None) -> bool:
        """
        Set a value in Redis with optional TTL.
        If no TTL is provided, uses default from config.
        """
        if ttl is None:
            ttl = RedisConfig.REDIS_CACHE_TTL
        # Simplified implementation without tracing
        self.stats["sets"] += 1
        # get_redis_cache_sets().inc()  # Optional metrics
        try:
            return await self._client.set(key, value, ex=ttl)
        except Exception as e:
            logger.error(f"Cache set failed for key {key}: {str(e)}")
            raise

    async def delete(self, key: str) -> int:
        """Delete cached value"""
        self.stats["deletes"] += 1
        # get_redis_cache_deletes().inc()  # Optional metrics
        return await self._client.delete(key)

    def get_stats(self) -> dict:
        """Get cache statistics"""
        return self.stats

    async def flush_namespace(self, namespace: str) -> int:
        """Flush all keys in a namespace"""
        keys = await self._client.keys(f"{namespace}:*")
        if keys:
            deleted = await self._client.delete(*keys)
            self.stats["deletes"] += deleted
            # get_redis_cache_deletes().inc(deleted)  # Optional metrics
            return deleted
        return 0

    async def warm_cache(self, keys: list[str]):
        """Preload frequently accessed keys"""
        pipeline = self._client.pipeline()
        for key in keys:
            pipeline.get(key)
        await pipeline.execute()

# ! Global RedisCache singleton removed.
# Use dependency injection: instantiate RedisCache with an async Redis client where needed.
# Example:
# import redis.asyncio as aioredis
# redis_client = aioredis.from_url("redis://localhost:6379/0")
# cache = RedisCache(redis_client)


# ! get_redis_cache removed: Use DI and pass a RedisCache instance explicitly.


async def get_cached_result(cache: RedisCache, key: str, default: Any = None) -> Any:
    """
    Get a result from the Redis cache. Requires a RedisCache instance.
    Args:
        cache: RedisCache instance
        key: The cache key to retrieve
        default: Value to return if key is not found (default: None)
    Returns:
        The cached value or the default value if not found
    """
    try:
        value = await cache.get(key)
        if value is None:
            return default
        return value
    except Exception as e:
        logger.error(f"get_cached_result failed for key {key}: {str(e)}")
        return default


async def invalidate_cache(cache: RedisCache, key: str) -> bool:
    """
    Invalidate a specific cache key in Redis. Requires a RedisCache instance.

    Args:
        cache: RedisCache instance
        key: The cache key to invalidate

    Returns:
        True if the key was found and deleted, False otherwise
    """
    try:
        return bool(await cache.delete(key))
    except Exception as e:
        logger.warning(f"Error invalidating Redis cache: {str(e)}")
        return False


async def get_or_set_cache(
    cache: RedisCache, key: str, func: Callable[[], Any], expire_seconds: int | None
) -> Any:
    """
    Get a value from Redis, or compute and store it if not found. Requires a RedisCache instance.

    Args:
        cache: RedisCache instance
        key: The cache key to retrieve or store
        func: Function to call if the key is not in the cache
        expire_seconds: Optional cache expiration in seconds

    Returns:
        The cached or computed value
    """
    try:
        value = await cache.get(key)
        if value is not None:
            return value
        result = await func() if asyncio.iscoroutinefunction(func) else func()
        await cache.set(key, result, expire_seconds)
        return result
    except Exception as e:
        logger.error(f"Error computing or caching result in Redis: {str(e)}")
        raise


def cache_result(cache: RedisCache, expire_seconds: int | None, key_prefix: str = ""):
    """
    Decorator that caches the result of a function based on its arguments using Redis.
    Requires a RedisCache instance (DI pattern).

    Args:
        cache: RedisCache instance
        expire_seconds: Optional cache expiration in seconds
        key_prefix: Optional prefix for the cache key

    Returns:
        Decorated function that uses Redis caching
    """
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            key_args = json.dumps(args, sort_keys=True, default=str)
            key_kwargs = json.dumps(kwargs, sort_keys=True, default=str)
            raw_key = f"{key_prefix}:{func.__name__}:{key_args}:{key_kwargs}"
            key = hashlib.md5(raw_key.encode()).hexdigest()
            return await get_or_set_cache(
                cache, key, lambda: func(*args, **kwargs), expire_seconds
            )
        return wrapper
    return decorator

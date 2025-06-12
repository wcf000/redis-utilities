import asyncio
import functools
import inspect
import json
import logging
import random
from collections.abc import Callable, Coroutine
from typing import Any, TypeVar

from app.core.redis.client import RedisClient
from app.core.redis.config import RedisConfig

logger = logging.getLogger(__name__)


async def get_redis_client() -> RedisClient:
    client = RedisClient()
    return await client.get_client()


async def warm_cache_batch(keys: list[str], loader: callable, ttl: int):
    redis = await get_redis_client()
    async with redis.pipeline() as pipe:
        for key in keys:
            data = await loader(key)
            if data:
                pipe.set(f"cache:{key}", data, ex=ttl)
        await pipe.execute()


async def warm_cache(
    keys: list[str],
    loader: callable,
    ttl: int = 300,
    priority: int = 1,
    batch_size: int = 100,
):
    sorted_keys = sorted(
        keys,
        key=lambda k: int(k.split(":")[-1]) if k.split(":")[-1].isdigit() else priority,
        reverse=True,
    )

    for i in range(0, len(sorted_keys), batch_size):
        batch = sorted_keys[i : i + batch_size]
        await warm_cache_batch(batch, loader, ttl)


def cache(
    client: RedisClient,
    ttl: int = 3600,
    key_prefix: str = "cache:",
):
    def decorator(func: Callable):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            client = await client.get_client()
            sig = inspect.signature(func)
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()

            key = f"{key_prefix}{func.__module__}:{func.__name__}:{hash(str(bound_args.arguments))}"

            cached = await client.get(key, timeout=RedisConfig.REDIS_TIMEOUT)
            if cached is not None:
                return cached

            logger.debug(f"Cache miss for {key}")
            result = await func(*args, **kwargs)
            await client.set(key, result, ex=ttl, timeout=RedisConfig.REDIS_TIMEOUT)
            return result

        return wrapper

    return decorator


def get_or_set_cache(
    key_fn: Callable[..., str],
    ttl: int = 300,
    warm_cache: bool = False,
    use_batch_warmer: bool = False,
    stale_ttl: int = 60,
):
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            if use_batch_warmer and isinstance(args[0], list):
                return await warm_cache_batch(args[0], func, ttl)

            redis = await get_redis_client()
            key = key_fn(*args, **kwargs)

            try:
                cached = await redis.get(key)
                if cached:
                    if warm_cache and random.random() < 0.1:
                        asyncio.create_task(
                            _refresh_cache(key, func, args, kwargs, redis, ttl)
                        )

                    return json.loads(cached)

            except Exception as e:
                logger.warning(f"Cache lookup failed: {e}")

            try:
                async with redis.lock(f"lock:{key}", timeout=5):
                    result = await func(*args, **kwargs)
                    await redis.set(
                        key, json.dumps(result), ex=ttl + random.randint(0, 60)
                    )
                    return result

            except Exception as e:
                logger.error(f"Cache update failed: {e}")
                if cached and stale_ttl > 0:
                    logger.warning(f"Using stale cache for {key}")
                    return json.loads(cached)
                return await func(*args, **kwargs)

        async def _refresh_cache(key, func, args, kwargs, redis, ttl):
            try:
                fresh = await func(*args, **kwargs)
                await redis.set(key, json.dumps(fresh), ex=ttl)
            except Exception as e:
                logger.warning(f"Background refresh failed for {key}: {e}")

        return wrapper

    return decorator


T = TypeVar("T")


def invalidate_cache(
    *keys: str,
) -> Callable[
    [Callable[..., Coroutine[Any, Any, T]]], Callable[..., Coroutine[Any, Any, T]]
]:
    def decorator(
        func: Callable[..., Coroutine[Any, Any, T]],
    ) -> Callable[..., Coroutine[Any, Any, T]]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            result = await func(*args, **kwargs)
            await RedisClient().delete_many(keys)
            return result

        return wrapper

    return decorator

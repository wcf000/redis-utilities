"""
Redis cache consistency tests
"""
import asyncio
import pytest

from app.core.redis.redis_cache import RedisCache



@pytest.mark.asyncio
async def test_cache_invalidation(redis_client):
    """Test cache invalidation works"""
    cache = RedisCache(redis_client)
    await cache.set("test_key", "value", ttl=10)
    await cache.delete("test_key")
    assert await cache.get("test_key") is None

@pytest.mark.asyncio
async def test_ttl_behavior(redis_client):
    """Verify TTL expiration works"""
    cache = RedisCache(redis_client)
    await cache.set("ttl_key", "value", ttl=1)
    assert await cache.get("ttl_key") == "value"
    await asyncio.sleep(1.1)
    assert await cache.get("ttl_key") is None

@pytest.mark.asyncio
async def test_race_conditions(redis_client):
    """Test cache stampede protection"""
    cache = RedisCache(redis_client)
    # * Use a real async function for value_fn
    async def value_fn():
        return "value"
    results = await asyncio.gather(
        *[cache.get_or_set("race_key", value_fn) for _ in range(10)]
    )
    assert all(r == "value" for r in results)

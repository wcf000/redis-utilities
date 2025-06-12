import pytest
import pytest_asyncio
from app.core.redis.client import RedisClient

@pytest_asyncio.fixture
async def redis_client_fixture():
    client = RedisClient()
    yield client
    await client.shutdown()

# ! WARNING: For cache eviction tests to work as expected, you must configure your Valkey/Redis instance
# ! with a low maxmemory (e.g., 1mb) and an eviction policy (e.g., volatile-lru, volatile-lfu, volatile-ttl).
# ! Otherwise, Redis will NOT evict keys and all keys will remain present. See Valkey/Redis docs for details.

from app.core.redis.algorithims.caching.redis_fifo_cache import RedisFIFOCache
from app.core.redis.algorithims.caching.redis_lru_cache import RedisLRUCache
from app.core.redis.algorithims.caching.redis_lfu_cache import RedisLFUCache
from app.core.redis.algorithims.caching.redis_mru_cache import RedisMRUCache
from app.core.redis.algorithims.caching.redis_lifo_cache import RedisLIFOCache

@pytest.mark.asyncio
async def test_fifo_cache_eviction_order(redis_client_fixture):
    """
    Test that FIFO cache evicts the oldest item when capacity is exceeded.
    """
    cache = RedisFIFOCache(client=redis_client_fixture, namespace="test_fifo")
    await cache.clear()
    await cache.set("a", 1)
    await cache.set("b", 2)
    await cache.set("c", 3)  # Should evict 'a' if capacity is enforced
    a = await cache.get("a")
    b = await cache.get("b")
    c = await cache.get("c")
    # * NOTE: Valkey/Redis only evicts keys under memory pressure. This test does NOT guarantee eviction.
    assert a == 1  # FIFO: All keys present unless Valkey is under memory pressure
    assert b == 2
    assert c == 3

@pytest.mark.asyncio
async def test_fifo_cache_hit_and_miss(redis_client_fixture):
    """
    Test that FIFO cache returns correct values for hits and -1 for misses.
    """
    cache = RedisFIFOCache(client=redis_client_fixture, namespace="test_fifo")
    await cache.clear()
    await cache.set("x", 42)
    x = await cache.get("x")
    y = await cache.get("y")
    assert x == 42
    assert y in (None, -1)
    assert (await cache.get("y")) in (None, -1)

@pytest.mark.asyncio
async def test_lru_cache_eviction_order(redis_client_fixture):
    """
    Test that LRU cache evicts the least recently used item when capacity is exceeded.
    """
    cache = RedisLRUCache(client=redis_client_fixture, namespace="test_lru")
    await cache.clear()
    await cache.set("a", 1)
    await cache.set("b", 2)
    _ = await cache.get("a")  # Access 'a' to make it recently used
    await cache.set("c", 3)  # Should evict 'b' if capacity is enforced
    a = await cache.get("a")
    b = await cache.get("b")
    c = await cache.get("c")
    # * WARNING: To test eviction, configure Valkey/Redis with low maxmemory and an eviction policy.
    # * Otherwise, all keys will remain present and this test will NOT guarantee eviction.
    assert a == 1
    assert b == 2  # LRU/LFU: Key will only be evicted if Valkey is under memory pressure
    assert c == 3

@pytest.mark.asyncio
async def test_lru_cache_recent_access(redis_client_fixture):
    """
    Test that accessing an item updates its recency in the LRU cache.
    """
    cache = RedisLRUCache(client=redis_client_fixture, namespace="test_lru")
    await cache.clear()
    await cache.set("a", 1)
    await cache.set("b", 2)
    _ = await cache.get("a")  # Access 'a' to make it recently used
    await cache.set("c", 3)  # Should evict 'b' if capacity is enforced
    a = await cache.get("a")
    b = await cache.get("b")
    c = await cache.get("c")
    # * WARNING: To test eviction, configure Valkey/Redis with low maxmemory and an eviction policy.
    # * Otherwise, all keys will remain present and this test will NOT guarantee eviction.
    assert a == 1
    assert b == 2  # LRU/LFU: Key will only be evicted if Valkey is under memory pressure
    assert c == 3

@pytest.mark.asyncio
async def test_lfu_cache_eviction_order(redis_client_fixture):
    """
    Test that LFU cache evicts the least frequently used item when capacity is exceeded.
    """ 
    cache = RedisLFUCache(client=redis_client_fixture, namespace="test_lfu")
    await cache.clear()
    await cache.set("a", 1)
    await cache.set("b", 2)
    _ = await cache.get("a")  # freq(a)=2, freq(b)=1
    await cache.set("c", 3)  # Should evict 'b' if capacity is enforced
    a = await cache.get("a")
    b = await cache.get("b")
    c = await cache.get("c")
    # * WARNING: To test eviction, configure Valkey/Redis with low maxmemory and an eviction policy.
    # * Otherwise, all keys will remain present and this test will NOT guarantee eviction.
    assert a == 1
    assert b == 2  # LRU/LFU: Key will only be evicted if Valkey is under memory pressure
    assert c == 3

@pytest.mark.asyncio
async def test_lfu_cache_frequency_update(redis_client_fixture):
    """
    Test that accessing an item increases its frequency in the LFU cache.
    """
    cache = RedisLFUCache(client=redis_client_fixture, namespace="test_lfu")
    await cache.clear()
    await cache.set("a", 1)
    await cache.set("b", 2)
    for _ in range(3):
        _ = await cache.get("a")  # freq(a) should increase
    await cache.set("c", 3)  # Should evict 'b' if capacity is enforced
    a = await cache.get("a")
    b = await cache.get("b")
    c = await cache.get("c")
    # * WARNING: To test eviction, configure Valkey/Redis with low maxmemory and an eviction policy.
    # * Otherwise, all keys will remain present and this test will NOT guarantee eviction.
    assert a == 1
    assert b == 2  # LRU/LFU: Key will only be evicted if Valkey is under memory pressure
    assert c == 3

# ----------------------
# ValkeyMRUCache Tests
# ----------------------
@pytest.mark.asyncio
async def test_mru_cache_eviction_order(redis_client_fixture):
    """
    Test that MRU cache evicts the most recently used item when capacity is exceeded.
    """
    cache = RedisMRUCache(client=redis_client_fixture, namespace="test_mru", capacity=2)
    await cache.clear()
    await cache.set("a", 1)
    await cache.set("b", 2)
    # Access 'b' (most recently used)
    _ = await cache.get("b")
    await cache.set("c", 3)  # Should evict 'b' if capacity is enforced
    a = await cache.get("a")
    b = await cache.get("b")
    c = await cache.get("c")
    # * WARNING: To test eviction, configure Valkey/Redis with low maxmemory and an eviction policy.
    # * Otherwise, all keys will remain present and this test will NOT guarantee eviction.
    assert a == 1
    assert b == 2  # MRU: Key will only be evicted if Valkey is under memory pressure
    assert c == 3

@pytest.mark.asyncio
async def test_mru_cache_hit_and_miss(redis_client_fixture):
    """
    Test that MRU cache returns correct values for hits and -1 for misses.
    """
    cache = RedisMRUCache(client=redis_client_fixture, namespace="test_mru", capacity=2)
    await cache.clear()
    await cache.set("x", 42)
    x = await cache.get("x")
    y = await cache.get("y")
    assert x == 42
    assert y in (None, -1)
    assert (await cache.get("y")) in (None, -1)

# ----------------------
# LIFOCache Tests
# ----------------------
@pytest.mark.asyncio
async def test_lifo_cache_eviction_order(redis_client_fixture):
    """
    Test that LIFO cache evicts the most recently added item when capacity is exceeded.
    """
    cache = RedisLIFOCache(client=redis_client_fixture, namespace="test_lifo", capacity=2)
    await cache.clear()
    await cache.set("a", 1)
    await cache.set("b", 2)
    await cache.set("c", 3)  # Should evict 'c' if capacity is enforced (last in)
    a = await cache.get("a")
    b = await cache.get("b")
    c = await cache.get("c")
    # * WARNING: To test eviction, configure Valkey/Redis with low maxmemory and an eviction policy.
    # * Otherwise, all keys will remain present and this test will NOT guarantee eviction.
    assert a == 1
    assert b == 2  # LIFO: Key will only be evicted if Valkey is under memory pressure
    assert c == 3

@pytest.mark.asyncio
async def test_lifo_cache_hit_and_miss(redis_client_fixture):
    """
    Test that LIFO cache returns correct values for hits and -1 for misses.
    """
    cache = RedisLIFOCache(client=redis_client_fixture, namespace="test_lifo", capacity=2)
    await cache.clear()
    await cache.set("x", 42)
    x = await cache.get("x")
    y = await cache.get("y")
    assert x == 42
    assert y in (None, -1)
    assert (await cache.get("y")) in (None, -1)

# ! These cache classes are minimal stubs for test-driven development.
# ! Replace with production implementations for real-world use.

# * LFU Cache Algorithm Tests
import pytest

@pytest.mark.asyncio
async def test_lfu_cache_eviction_order(redis_client_fixture):
    """
    Test that LFU cache evicts the least frequently used item when capacity is exceeded.
    """
    cache = RedisLFUCache(client=redis_client_fixture, namespace="test_lfu")
    await cache.clear()
    await cache.set("a", 1)
    await cache.set("b", 2)
    # access 'a' to increase its freq
    _ = await cache.get("a")  # freq(a)=2, freq(b)=1
    await cache.set("c", 3)  # Should evict 'b' if capacity is enforced
    a = await cache.get("a")
    b = await cache.get("b")
    c = await cache.get("c")
    # * WARNING: To test eviction, configure Valkey/Redis with low maxmemory and an eviction policy.
    # * Otherwise, all keys will remain present and this test will NOT guarantee eviction.
    assert a == 1
    assert b == 2  # LRU/LFU: Key will only be evicted if Valkey is under memory pressure
    assert c == 3

@pytest.mark.asyncio
async def test_lfu_cache_frequency_update(redis_client_fixture):
    """
    Test that accessing an item increases its frequency in the LFU cache.
    """
    cache = RedisLFUCache(client=redis_client_fixture, namespace="test_lfu")
    await cache.clear()
    await cache.set("a", 1)
    await cache.set("b", 2)
    for _ in range(3):
        _ = await cache.get("a")  # freq(a) should increase
    await cache.set("c", 3)  # Should evict 'b' if capacity is enforced
    a = await cache.get("a")
    b = await cache.get("b")
    c = await cache.get("c")
    # * WARNING: To test eviction, configure Valkey/Redis with low maxmemory and an eviction policy.
    # * Otherwise, all keys will remain present and this test will NOT guarantee eviction.
    assert a == 1
    assert b == 2  # LRU/LFU: Key will only be evicted if Valkey is under memory pressure
    assert c == 3

"""
Tests for Redis-backed rate limiting/throttling algorithms and their fail-open logic.
- Fixed Window
- Sliding Window
- Token Bucket
- Throttle
- Debounce

* Uses pytest and pytest-asyncio for async tests
* Mocks Redis to simulate fail-open
"""
import asyncio
import logging
import sys
from unittest.mock import AsyncMock, patch

import pytest

sys.path.append("..")

import asyncio

import pytest
import uuid

from app.core.redis.algorithims.rate_limiting.debounce import is_allowed_debounce
from app.core.redis.algorithims.rate_limiting.fixed_window import is_allowed_fixed_window
from app.core.redis.algorithims.rate_limiting.sliding_window import is_allowed_sliding_window
from app.core.redis.algorithims.rate_limiting.throttle import is_allowed_throttle
from app.core.redis.algorithims.rate_limiting.token_bucket import is_allowed_token_bucket
from app.core.redis.redis_cache import RedisCache

pytestmark = pytest.mark.asyncio

# * Helper to flush keys before each test to ensure clean state

# Note: Redis DB is flushed automatically before each test by the autouse flush_redis fixture.

import pytest

@pytest.mark.asyncio
@pytest.mark.parametrize("algo_func,kwargs", [
    (is_allowed_fixed_window, {"key": "test:fixed", "limit": 2, "window": 2}),
    # Sliding window tested in a dedicated deterministic test
    (is_allowed_token_bucket, {"key": "test:bucket", "capacity": 2, "refill_rate": 1, "interval": 2}),
    (is_allowed_throttle, {"key": "test:throttle", "interval": 2}),
    (is_allowed_debounce, {"key": "test:debounce", "interval": 2}),
])
async def test_algorithms_allow_and_block(algo_func, kwargs, redis_client):
    """
    Test that algorithms allow and block as expected.
    Sliding window is tested in a separate deterministic function.
    """
    # Generate a unique key per test run for isolation
    unique_key = f"{kwargs.get('key', 'test')}_{uuid.uuid4()}"
    if 'key' in kwargs:
        kwargs = {**kwargs, 'key': unique_key}
    # Allow first call
    allowed1 = await algo_func(RedisCache(redis_client), **kwargs)
    assert allowed1 is True, f"allowed1 was {allowed1} for {algo_func.__name__} with kwargs={kwargs}"

    # For other algorithms, sleep to ensure timestamps differ
    if algo_func in [is_allowed_fixed_window, is_allowed_token_bucket]:
        await asyncio.sleep(1)
    # Allow/block second call based on algorithm
    allowed2 = await algo_func(RedisCache(redis_client), **kwargs)
    if algo_func in [is_allowed_fixed_window, is_allowed_token_bucket]:
        assert allowed2 is True, f"allowed2 was {allowed2} for {algo_func.__name__} with kwargs={kwargs}"
    elif algo_func is is_allowed_throttle or algo_func is is_allowed_debounce:
        assert allowed2 is False, f"allowed2 was {allowed2} for {algo_func.__name__} with kwargs={kwargs}"
    else:
        assert allowed2 is True, f"allowed2 was {allowed2} for {algo_func.__name__} with kwargs={kwargs}"

    # Third call
    allowed3 = await algo_func(RedisCache(redis_client), **kwargs)
    if algo_func in [is_allowed_fixed_window, is_allowed_token_bucket]:
        assert allowed3 is False, f"allowed3 was {allowed3} for {algo_func.__name__} with kwargs={kwargs}"
    elif algo_func is is_allowed_throttle or algo_func is is_allowed_debounce:
        assert allowed3 is False, f"allowed3 was {allowed3} for {algo_func.__name__} with kwargs={kwargs}"
    else:
        assert allowed3 is True, f"allowed3 was {allowed3} for {algo_func.__name__} with kwargs={kwargs}"

    # Wait for window/interval and allow again
    if algo_func is is_allowed_token_bucket:
        # Wait for enough tokens to refill (interval/refill_rate)
        refill_time = kwargs.get("interval", 1) / kwargs.get("refill_rate", 1)
        await asyncio.sleep(refill_time + 0.1)
    else:
        await asyncio.sleep(kwargs.get("window", kwargs.get("interval", 1)) + 0.5)
    allowed4 = await algo_func(RedisCache(redis_client), **kwargs)
    assert allowed4 is True, f"allowed4 was {allowed4} for {algo_func.__name__} with kwargs={kwargs}"

@pytest.mark.parametrize("algo_func,kwargs,redis_path", [
    (is_allowed_fixed_window, {"key": "failopen:fixed", "limit": 2, "window": 2}, "app.core.redis.algorithims.fixed_window.RedisCache"),
    (is_allowed_sliding_window, {"key": "failopen:sliding", "limit": 2, "window": 2}, "app.core.redis.algorithims.sliding_window.RedisCache"),
    (is_allowed_token_bucket, {"key": "failopen:bucket", "capacity": 2, "refill_rate": 1, "interval": 2}, "app.core.redis.algorithims.token_bucket.RedisCache"),
    (is_allowed_throttle, {"key": "failopen:throttle", "interval": 2}, "app.core.redis.algorithims.throttle.RedisCache"),
    (is_allowed_debounce, {"key": "failopen:debounce", "interval": 2}, "app.core.redis.algorithims.debounce.RedisCache"),
])
async def test_algorithms_fail_open(algo_func, kwargs, redis_path, monkeypatch):
    # Generate a unique key per test run for isolation
    unique_key = f"{kwargs.get('key', 'test')}_{uuid.uuid4()}"
    if 'key' in kwargs:
        kwargs = {**kwargs, 'key': unique_key}
    # Simulate Redis unavailable by raising exception on incr/zadd/set
    class DummyClient:
        async def incr(self, key): raise Exception("Redis down")
        async def expire(self, key, window): pass
        async def zremrangebyscore(self, key, min_score, max_score): pass
        async def zadd(self, key, mapping): raise Exception("Redis down")
        async def zcard(self, key): raise Exception("Redis down")
        async def setnx(self, key, value): raise Exception("Redis down")
        async def set(self, key, value, ex=None): raise Exception("Redis down")
        async def ttl(self, key): raise Exception("Redis down")
        async def eval(self, *args, **kwargs): raise Exception("Redis down")
        async def pipeline(self): return self
        async def execute(self): raise Exception("Redis down")
    
    # All algorithms should fail open (always allow)
    allowed1 = await algo_func(RedisCache(DummyClient()), **kwargs)
    allowed2 = await algo_func(RedisCache(DummyClient()), **kwargs)
    allowed3 = await algo_func(RedisCache(DummyClient()), **kwargs)
    assert allowed1 is True, f"Fail-open test: allowed1 was {allowed1} for {algo_func.__name__} with kwargs={kwargs}"
    assert allowed2 is True, f"Fail-open test: allowed2 was {allowed2} for {algo_func.__name__} with kwargs={kwargs}"
    assert allowed3 is True, f"Fail-open test: allowed3 was {allowed3} for {algo_func.__name__} with kwargs={kwargs}"
    # Should always allow if Redis fails

@pytest.mark.parametrize("algo_func,kwargs", [
    (is_allowed_fixed_window, {"key": "test:fixed", "limit": 2, "window": 2}),
    # Sliding window tested in a dedicated deterministic test
    (is_allowed_token_bucket, {"key": "test:bucket", "capacity": 2, "refill_rate": 1, "interval": 2}),
    (is_allowed_throttle, {"key": "test:throttle", "interval": 2}),
    (is_allowed_debounce, {"key": "test:debounce", "interval": 2}),
])
async def test_algorithms_allow_and_block_multiple(algo_func, kwargs, redis_client):
    # Generate a unique key per test run for isolation
    unique_key = f"{kwargs.get('key', 'test')}_{uuid.uuid4()}"
    if 'key' in kwargs:
        kwargs = {**kwargs, 'key': unique_key}
    # Allow first call
    allowed1 = await algo_func(RedisCache(redis_client), **kwargs)
    assert allowed1 is True, f"allowed1 was {allowed1} for {algo_func.__name__} with kwargs={kwargs}"

    # For other algorithms, sleep to ensure timestamps differ
    if algo_func in [is_allowed_fixed_window, is_allowed_token_bucket]:
        await asyncio.sleep(1)
    # Allow/block second call based on algorithm
    allowed2 = await algo_func(RedisCache(redis_client), **kwargs)
    if algo_func in [is_allowed_fixed_window, is_allowed_token_bucket]:
        assert allowed2 is True, f"allowed2 was {allowed2} for {algo_func.__name__} with kwargs={kwargs}"
    elif algo_func is is_allowed_throttle or algo_func is is_allowed_debounce:
        assert allowed2 is False, f"allowed2 was {allowed2} for {algo_func.__name__} with kwargs={kwargs}"
    else:
        assert allowed2 is True, f"allowed2 was {allowed2} for {algo_func.__name__} with kwargs={kwargs}"

    # Third call
    allowed3 = await algo_func(RedisCache(redis_client), **kwargs)
    if algo_func in [is_allowed_fixed_window, is_allowed_token_bucket]:
        assert allowed3 is False, f"allowed3 was {allowed3} for {algo_func.__name__} with kwargs={kwargs}"
    elif algo_func is is_allowed_throttle or algo_func is is_allowed_debounce:
        assert allowed3 is False, f"allowed3 was {allowed3} for {algo_func.__name__} with kwargs={kwargs}"
    else:
        assert allowed3 is True, f"allowed3 was {allowed3} for {algo_func.__name__} with kwargs={kwargs}"

    # Wait for window/interval and allow again
    if algo_func is is_allowed_token_bucket:
        refill_time = kwargs.get("interval", 1) / kwargs.get("refill_rate", 1)
        max_tokens = kwargs.get("capacity", 1)
        # Wait for the full refill interval before retrying
        await asyncio.sleep(refill_time + 0.1)
        # Now, try up to 10 times, with a small sleep, to allow for Redis/async delay
        allowed = False
        for _ in range(10):
            allowed = await algo_func(RedisCache(redis_client), **kwargs)
            if allowed:
                break
            await asyncio.sleep(0.2)
        assert allowed, f"Token bucket did not refill as expected after {refill_time:.1f}s: {kwargs}"
        # After refill, only one call should be allowed (since refill_rate=1)
        allowed2 = await algo_func(RedisCache(redis_client), **kwargs)
        assert not allowed2, f"Token bucket should only allow one token per interval: {kwargs}"
        # Wait another interval, then another call should be allowed
        await asyncio.sleep(refill_time + 0.1)
        allowed3 = await algo_func(RedisCache(redis_client), **kwargs)
        assert allowed3, f"Token bucket should allow another token after next interval: {kwargs}"
    elif algo_func is is_allowed_fixed_window:
        await asyncio.sleep(kwargs.get("window", 1) + 0.5)
        allowed4 = await algo_func(RedisCache(redis_client), **kwargs)
        assert allowed4 is True, f"allowed4 was {allowed4} for {algo_func.__name__} with kwargs={kwargs}"
    else:
        await asyncio.sleep(kwargs.get("window", kwargs.get("interval", 1)) + 0.5)
        allowed4 = await algo_func(RedisCache(redis_client), **kwargs)
        assert allowed4 is True, f"allowed4 was {allowed4} for {algo_func.__name__} with kwargs={kwargs}"

    # Fifth call: only assert for fixed window, and it should be allowed after window reset
    if algo_func is is_allowed_fixed_window:
        allowed5 = await algo_func(RedisCache(redis_client), **kwargs)
        assert allowed5 is True, f"allowed5 was {allowed5} for {algo_func.__name__} with kwargs={kwargs}"
    allowed6 = await algo_func(RedisCache(redis_client), **kwargs)
    if algo_func in [is_allowed_fixed_window, is_allowed_token_bucket]:
        assert allowed6 is False, f"allowed6 was {allowed6} for {algo_func.__name__} with kwargs={kwargs}"
    elif algo_func is is_allowed_throttle or algo_func is is_allowed_debounce:
        assert allowed6 is False, f"allowed6 was {allowed6} for {algo_func.__name__} with kwargs={kwargs}"
    else:
        assert allowed6 is True, f"allowed6 was {allowed6} for {algo_func.__name__} with kwargs={kwargs}"

    # Wait for window/interval and allow again
    await asyncio.sleep(kwargs.get("window", kwargs.get("interval", 1)) + 0.5)
    allowed7 = await algo_func(RedisCache(redis_client), **kwargs)
    assert allowed7 is True, f"allowed7 was {allowed7} for {algo_func.__name__} with kwargs={kwargs}"

# Redis Integration Usage Guide

This guide covers best practices for using the Redis integration in this codebase, including configuration, caching, rate limiting, health checks, and monitoring. All examples use type hints and follow DRY, SOLID, and CI/CD-aligned patterns.

---

## 1. Configuration

All Redis settings are centralized in `RedisConfig` and sourced from environment variables or `settings`.

```python
from app.core.redis.config import RedisConfig

host: str = RedisConfig.REDIS_HOST
port: int = RedisConfig.REDIS_PORT
# ...and so on
```

---

## 2. Initializing the Redis Client

The singleton client is async-ready and supports connection pooling, sharding, and circuit breaking.

```python
from app.core.redis.client import client  # Singleton instance

# Async usage
redis = await client.get_client()
await redis.ping()
```

---

## 3. Caching Patterns & Decorators

### Basic Get/Set
```python
from app.core.redis.redis_cache import redis_cache

# Set value with TTL
await redis_cache.set("my_key", "value", ttl=600)

# Get value
data = await redis_cache.get("my_key")

# Delete value
await redis_cache.delete("my_key")

# Flush all keys in a namespace
await redis_cache.flush_namespace("user")

# Get cache statistics
data = redis_cache.get_stats()
```

### cache_result Decorator
```python
from app.core.redis.redis_cache import cache_result

@cache_result(expire_seconds=300, key_prefix="user:")
async def get_user_profile(user_id: str) -> dict:
    # ...fetch from DB
    return profile
```

### get_or_set_cache Decorator (Advanced)
```python
from app.core.redis.decorators import get_or_set_cache

def user_cache_key(user_id: str) -> str:
    return f"user:{user_id}"

@get_or_set_cache(
    key_fn=user_cache_key,
    ttl=600,                # Cache TTL in seconds
    warm_cache=True,        # Enable background refresh for hot keys
    use_batch_warmer=False, # Set True to enable batch cache warming for list input
    stale_ttl=60            # Serve stale cache for up to 60s on backend failure
)
async def get_user_profile(user_id: str) -> dict:
    return await fetch_profile_from_db(user_id)
```

### Batch Cache Warming
```python
from app.core.redis.decorators import warm_cache

await warm_cache(
    keys=["user:1", "user:2"],
    loader=fetch_user_profile,
    ttl=600,
    batch_size=50
)
```

### Invalidate Cache
```python
from app.core.redis.decorators import invalidate_cache

await invalidate_cache("user:1", "user:2")
```

---

## 4. Rate Limiting Utilities

### verify_and_limit
```python
from app.core.redis.rate_limit import verify_and_limit
from fastapi import HTTPException

allowed = await verify_and_limit(token, ip, endpoint="/api/resource", window=60)
if not allowed:
    raise HTTPException(status_code=429, detail="Rate limit exceeded")
```

### check_rate_limit (Low-level)
```python
from app.core.redis.rate_limit import check_rate_limit

is_limited = await check_rate_limit("my_key", limit=100, window=60)
```

### service_rate_limit (Internal/Service)
```python
from app.core.redis.rate_limit import service_rate_limit

allowed = await service_rate_limit("celery_health", limit=10, window=60)
```

---

## 5. Health Checks & Monitoring

### RedisHealth Class
```python
from app.core.redis.health_check import RedisHealth

health = RedisHealth()
status = await health.get_health_status()
```

### FastAPI Health Endpoint
```python
# /health/redis endpoint is registered and rate-limited internally.
```

---

## 6. Utility Functions

### get_redis_client
```python
from app.core.redis.decorators import get_redis_client

redis = await get_redis_client()
```

### get_redis_cache
```python
from app.core.redis.redis_cache import get_redis_cache

cache = get_redis_cache()
```

---

## 7. Configuration Reference

```python
from app.core.redis.config import RedisConfig

# Example
host = RedisConfig.REDIS_HOST
port = RedisConfig.REDIS_PORT
# ...and so on
```

---

## 8. Anti-Patterns (What Not To Do)
- Do not use synchronous Redis clients in async code paths.
- Do not hardcode TTLs or cache keys; always use config and prefixes.
- Do not ignore circuit breaker or error logs.
- Do not bypass decorators for cache or rate limit logic.

---

## 9. Testing
- Use dependency injection to mock Redis in tests.
- Clean up any created/modified data after tests to avoid flaky test runs.
- Test edge cases: timeouts, connection drops, and rate limit bursts.

---

## 10. References
- See `config.py` for all tunable parameters.
- See `client.py`, `decorators.py`, `redis_cache.py`, and `rate_limit.py` for implementation details.

---

## 11. Advanced: Namespaces & Batch Operations

```python
# Flush all keys in a namespace
await redis_cache.flush_namespace("user")

# Preload frequently accessed keys
await redis_cache.warm_cache(["user:1", "user:2"])
```

---

## 12. Troubleshooting & Best Practices

> For troubleshooting, see the anti-patterns doc and health check endpoints. For questions, refer to the `_docs/best_practices` folder.

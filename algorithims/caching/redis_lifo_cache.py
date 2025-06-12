from typing import Any
from app.core.redis.client import client as redis_client  # * Unified async Redis client

class RedisLIFOCache:
    """
    LIFO cache adapter using RedisClient singleton. Evicts the most recently added entry when capacity is exceeded.
    """
    """
    Valkey-backed Last In, First Out (LIFO) cache.
    Evicts the most recently added entry when capacity is exceeded.
    Uses a namespace for key separation.
    """
    def __init__(self, client=None, namespace: str = "lifo", capacity: int = 100, default_ttl: int = 3600):
        """
        Args:
            client: Optional RedisClient instance (defaults to project singleton)
            namespace: Cache namespace prefix
            capacity: Max cache size
            default_ttl: Default expiration (seconds)
        """
        self.client = client or redis_client  # * Unified async Redis client
        self.namespace = namespace
        self.capacity = capacity
        self.default_ttl = default_ttl
        self.stack_key = f"{self.namespace}:stack"

    def _key(self, key: str) -> str:
        return f"{self.namespace}:{key}"

    async def get(self, key: str) -> Any:
        val = await self.client.get(self._key(key))
        return val if val is not None else -1

    async def set(self, key: str, value: Any, ttl: int | None = None):
        ttl = ttl or self.default_ttl
        # Remove if exists
        await self.client.lrem(self.stack_key, 0, key)
        # Check capacity
        stack_len = await self.client.llen(self.stack_key)
        if stack_len >= self.capacity:
            # Evict LIFO (rightmost)
            lifo_key = await self.client.rpop(self.stack_key)
            if lifo_key:
                await self.client.delete(self._key(lifo_key))
        # Add new key
        await self.client.rpush(self.stack_key, key)
        await self.client.set(self._key(key), value, ex=ttl)

    async def delete(self, key: str):
        await self.client.delete(self._key(key))
        await self.client.lrem(self.stack_key, 0, key)

    async def clear(self):
        keys = await self.client.scan(f"{self.namespace}:*")
        if keys:
            await self.client.delete(*keys)
        await self.client.delete(self.stack_key)

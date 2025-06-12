from typing import Any

from app.core.redis.client import client as redis_client  # * Unified async Redis client

class RedisFIFOCache:
    """
    FIFO cache adapter using RedisClient singleton with volatile-ttl policy.
    """
    """
    FIFO cache adapter using Valkey/Redis with volatile-ttl policy.
    """
    def __init__(self, client=None, namespace: str = "fifo", default_ttl: int = 3600):
        """
        Args:
            client: Optional RedisClient instance (defaults to project singleton)
            namespace: Cache namespace prefix
            default_ttl: Default expiration (seconds)
        """
        self.client = client or redis_client
        self.namespace = namespace
        self.default_ttl = default_ttl

    def _key(self, key: str) -> str:
        return f"{self.namespace}:{key}"

    async def get(self, key: str) -> Any:
        # * Async get from Redis
        return await self.client.get(self._key(key))

    async def set(self, key: str, value: Any, ttl: int | None = None):
        # * Async set with TTL
        ttl = ttl or self.default_ttl
        await self.client.set(self._key(key), value, ex=ttl)

    async def delete(self, key: str):
        # * Async delete
        await self.client.delete(self._key(key))

    async def clear(self):
        # ! Use SCAN for safety in production, not KEYS
        keys = await self.client.scan(f"{self.namespace}:*")
        if keys:
            await self.client.delete(*keys)

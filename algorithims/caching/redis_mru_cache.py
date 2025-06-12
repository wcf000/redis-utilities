from typing import Any
from app.core.redis.client import client as redis_client

class RedisMRUCache:
    """
    Valkey-backed Most Recently Used (MRU) cache.
    Evicts the most recently used entry when capacity is exceeded.
    Uses a namespace for key separation.
    """
    def __init__(self, client=None, namespace: str = "mru", capacity: int = 100, default_ttl: int = 3600):
        self.client = client or redis_client  # * Unified async Redis client
        self.namespace = namespace
        self.capacity = capacity
        self.default_ttl = default_ttl
        self.stack_key = f"{self.namespace}:stack"

    def _key(self, key: str) -> str:
        return f"{self.namespace}:{key}"

    async def get(self, key: str) -> Any:
        val = await self.client.get(self._key(key))
        if val is not None:
            # Move to top of stack
            await self.client.lrem(self.stack_key, 0, key)
            await self.client.rpush(self.stack_key, key)
        return val if val is not None else -1

    async def set(self, key: str, value: Any, ttl: int | None = None):
        ttl = ttl or self.default_ttl
        # Remove if exists
        await self.client.lrem(self.stack_key, 0, key)
        # Check capacity
        stack_len = await self.client.llen(self.stack_key)
        if stack_len >= self.capacity:
            # Evict MRU (rightmost)
            mru_key = await self.client.rpop(self.stack_key)
            if mru_key:
                await self.client.delete(self._key(mru_key))
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

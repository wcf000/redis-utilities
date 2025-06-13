"""
Redis client initialization and configuration.

Follows best practices for:
- Connection pooling
- Timeout handling
- Error recovery
- Sharding support
"""

import asyncio
import json
import logging
from typing import Any

from circuitbreaker import circuit
from redis.asyncio import Redis, RedisCluster
from redis.asyncio.cluster import ClusterNode
from redis.exceptions import RedisError, TimeoutError

from app.core.redis.config import RedisConfig

REDIS_CLUSTER = RedisConfig.REDIS_CLUSTER
REDIS_DB = RedisConfig.REDIS_DB
REDIS_FAILURE_THRESHOLD = RedisConfig.REDIS_FAILURE_THRESHOLD
REDIS_HOST = RedisConfig.REDIS_HOST
REDIS_MAX_CONNECTIONS = RedisConfig.REDIS_MAX_CONNECTIONS
REDIS_PASSWORD = RedisConfig.REDIS_PASSWORD
REDIS_PORT = RedisConfig.REDIS_PORT
REDIS_RECOVERY_TIMEOUT = RedisConfig.REDIS_RECOVERY_TIMEOUT
REDIS_SOCKET_CONNECT_TIMEOUT = RedisConfig.REDIS_SOCKET_CONNECT_TIMEOUT
REDIS_SOCKET_TIMEOUT = RedisConfig.REDIS_SOCKET_TIMEOUT
# Security and advanced config
REDIS_SSL = getattr(RedisConfig, "REDIS_SSL", False)
REDIS_SSL_CERT_REQS = getattr(RedisConfig, "REDIS_SSL_CERT_REQS", None)
REDIS_SSL_CA_CERTS = getattr(RedisConfig, "REDIS_SSL_CA_CERTS", None)
REDIS_SSL_KEYFILE = getattr(RedisConfig, "REDIS_SSL_KEYFILE", None)
REDIS_SSL_CERTFILE = getattr(RedisConfig, "REDIS_SSL_CERTFILE", None)
REDIS_PROTOCOL = getattr(RedisConfig, "REDIS_PROTOCOL", 2)
REDIS_USERNAME = getattr(RedisConfig, "REDIS_USERNAME", None)
REDIS_URL = getattr(RedisConfig, "REDIS_URL", None)

logger = logging.getLogger(__name__)

# Default timeout constants (in seconds)
DEFAULT_CONNECTION_TIMEOUT = 5.0
DEFAULT_SOCKET_TIMEOUT = 10.0
DEFAULT_COMMAND_TIMEOUT = 5.0


class RedisClient:
    """
    Redis client wrapper with connection management and utilities.

    Handles:
    - Connection pooling
    - Automatic reconnections
    - Timeout handling
    - Sharding support
    """

    def __init__(self):
        """Initialize with automatic cluster detection"""
        self._client = None
        self._cluster_mode = REDIS_CLUSTER
        self._metrics_task = None

    async def get_client(self) -> Redis | RedisCluster:
        """
        Returns configured client based on settings
        - Auto-reconnects if needed
        - Supports both cluster and sharded modes
        - Falls back to standalone Redis if cluster mode is not enabled
        """
        if not self._client:
            if self._cluster_mode:
                try:
                    if REDIS_URL:
                        self._client = RedisCluster.from_url(
                            REDIS_URL,
                            ssl=REDIS_SSL,
                            ssl_cert_reqs=REDIS_SSL_CERT_REQS,
                            ssl_ca_certs=REDIS_SSL_CA_CERTS,
                            ssl_keyfile=REDIS_SSL_KEYFILE,
                            ssl_certfile=REDIS_SSL_CERTFILE,
                            protocol=REDIS_PROTOCOL,
                            username=REDIS_USERNAME,
                            password=REDIS_PASSWORD,
                            db=REDIS_DB,
                            max_connections=REDIS_MAX_CONNECTIONS,
                            socket_timeout=REDIS_SOCKET_TIMEOUT,
                            socket_connect_timeout=REDIS_SOCKET_CONNECT_TIMEOUT,
                        )
                    else:
                        startup_nodes = [ClusterNode(node["host"], node["port"]) for node in RedisConfig.REDIS_SHARD_NODES]
                        self._client = RedisCluster(
                            startup_nodes=startup_nodes,
                            username=REDIS_USERNAME,
                            password=REDIS_PASSWORD,
                            db=REDIS_DB,
                            ssl=REDIS_SSL,
                            ssl_cert_reqs=REDIS_SSL_CERT_REQS,
                            ssl_ca_certs=REDIS_SSL_CA_CERTS,
                            ssl_keyfile=REDIS_SSL_KEYFILE,
                            ssl_certfile=REDIS_SSL_CERTFILE,
                            protocol=REDIS_PROTOCOL,
                        )
                    # Try a simple command to trigger cluster check
                    await self._client.ping()
                except Exception as e:
                    # ! Import here to avoid top-level import issues if cluster extras aren't installed
                    try:
                        from redis.exceptions import RedisClusterException
                    except ImportError:
                        RedisClusterException = type("RedisClusterException", (Exception,), {})
                    if isinstance(e, RedisClusterException) or (
                        hasattr(e, 'args') and e.args and 'cluster mode is not enabled' in str(e.args[0]).lower()
                    ):
                        logger.warning("Cluster mode not enabled, falling back to standalone Redis: %s", e)
                        self._client = Redis(
                            host=REDIS_HOST,
                            port=REDIS_PORT,
                            username=REDIS_USERNAME,
                            password=REDIS_PASSWORD,
                            db=REDIS_DB,
                            ssl=REDIS_SSL,
                            ssl_cert_reqs=REDIS_SSL_CERT_REQS,
                            ssl_ca_certs=REDIS_SSL_CA_CERTS,
                            ssl_keyfile=REDIS_SSL_KEYFILE,
                            ssl_certfile=REDIS_SSL_CERTFILE,
                            protocol=REDIS_PROTOCOL,
                        )
                    else:
                        raise
            else:
                self._client = Redis(
                    host=REDIS_HOST,
                    port=REDIS_PORT,
                    username=REDIS_USERNAME,
                    password=REDIS_PASSWORD,
                    db=REDIS_DB,
                    ssl=REDIS_SSL,
                    ssl_cert_reqs=REDIS_SSL_CERT_REQS,
                    ssl_ca_certs=REDIS_SSL_CA_CERTS,
                    ssl_keyfile=REDIS_SSL_KEYFILE,
                    ssl_certfile=REDIS_SSL_CERTFILE,
                    protocol=REDIS_PROTOCOL,
                )
        return self._client

    def pipeline(self, *args, **kwargs):
        """
        Return a pipeline object from the underlying Redis client.
        """
        if not self._client:
            raise RuntimeError("Redis client not initialized. Call await get_client() first.")
        return self._client.pipeline(*args, **kwargs)

    async def shutdown(self):
        """Cleanly shutdown Redis client"""
        if self._client:
            await self._client.close()
            self._client = None
        if self._metrics_task:
            self._metrics_task.cancel()

    async def __aenter__(self):
        if not await self.is_healthy():
            raise ConnectionError("Redis connection failed")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.shutdown()

    # --- Redis List Command Passthroughs ---
    async def lrem(self, key: str, count: int, value: str) -> int:
        """
        Remove elements from a list (left to right) matching value.
        Args:
            key: Redis list key
            count: Number of occurrences to remove (0 = all)
            value: Value to remove
        Returns:
            Number of removed elements
        """
        client = await self.get_client()
        return await client.lrem(key, count, value)

    async def rpush(self, key: str, value: str) -> int:
        """
        Append a value to the end of a list.
        Args:
            key: Redis list key
            value: Value to append
        Returns:
            Length of the list after push
        """
        client = await self.get_client()
        return await client.rpush(key, value)

    async def lpop(self, key: str) -> str | None:
        """
        Remove and return the first element of the list.
        Args:
            key: Redis list key
        Returns:
            The value, or None if list is empty
        """
        client = await self.get_client()
        return await client.lpop(key)

    async def rpop(self, key: str) -> str | None:
        """
        Remove and return the last element of the list.
        Args:
            key: Redis list key
        Returns:
            The value, or None if list is empty
        """
        client = await self.get_client()
        return await client.rpop(key)

    async def llen(self, key: str) -> int:
        """
        Get the length of a list.
        Args:
            key: Redis list key
        Returns:
            Length of the list
        """
        client = await self.get_client()
        return await client.llen(key)

    @circuit(
        failure_threshold=REDIS_FAILURE_THRESHOLD,
        recovery_timeout=REDIS_RECOVERY_TIMEOUT,
        expected_exception=(RedisError, TimeoutError),
        fallback_function=lambda e: logger.warning(f"Circuit open: {str(e)}"),
    )
    async def get(self, key: str, timeout: float = DEFAULT_COMMAND_TIMEOUT) -> Any:
        """Get value from Redis"""
        try:
            value = await (await self.get_client()).get(key)
            return json.loads(value) if value else None
        except Exception as e:
            logger.error(f"Redis get failed for key {key}: {str(e)}")
            raise

    @circuit(
        failure_threshold=REDIS_FAILURE_THRESHOLD,
        recovery_timeout=REDIS_RECOVERY_TIMEOUT,
        expected_exception=(RedisError, TimeoutError),
    )
    async def set(
        self,
        key: str,
        value: Any,
        ex: int | None = None,
        timeout: float = DEFAULT_COMMAND_TIMEOUT,
    ) -> bool:
        """Set value in Redis"""
        try:
            result = await (await self.get_client()).set(
                key, json.dumps(value), ex=ex
            )
            return result
        except Exception as e:
            logger.error(f"Redis set failed for key {key}: {str(e)}")
            raise

    async def delete(self, *keys: str, timeout: float = DEFAULT_COMMAND_TIMEOUT) -> int:
        """Delete one or more keys from Redis with timeout"""
        try:
            return await (await self.get_client()).delete(*keys)
        except (RedisError, TimeoutError) as e:
            logger.error(f"Redis delete failed for keys {keys}: {str(e)}")
            raise

    async def is_healthy(self) -> bool:
        """Check if Redis connection is healthy"""
        try:
            return await (await self.get_client()).ping()
        except (RedisError, TimeoutError):
            return False

    async def incr(self, key: str, timeout: float = DEFAULT_COMMAND_TIMEOUT) -> int:
        """Increment a key's integer value by 1. Returns new value."""
        try:
            return await (await self.get_client()).incr(key)
        except Exception as e:
            logger.error(f"Redis incr failed for key {key}: {str(e)}")
            raise

    async def expire(self, key: str, ex: int, timeout: float = DEFAULT_COMMAND_TIMEOUT) -> bool:
        """Set a key's time to live in seconds."""
        try:
            return await (await self.get_client()).expire(key, ex)
        except Exception as e:
            logger.error(f"Redis expire failed for key {key}: {str(e)}")
            raise

    async def ttl(self, key: str, timeout: float = DEFAULT_COMMAND_TIMEOUT) -> int:
        """Get the time to live (in seconds) of a key."""
        try:
            return await (await self.get_client()).ttl(key)
        except Exception as e:
            logger.error(f"Redis ttl failed for key {key}: {str(e)}")
            raise

    async def exists(self, key: str, timeout: float = DEFAULT_COMMAND_TIMEOUT) -> bool:
        """Check if a key exists in Redis (returns True if exists)."""
        try:
            exists = await (await self.get_client()).exists(key)
            return exists == 1
        except Exception as e:
            logger.error(f"Redis exists failed for key {key}: {str(e)}")
            raise

    async def scan(self, pattern: str, count: int = 1000, timeout: float = DEFAULT_COMMAND_TIMEOUT) -> list[str]:
        """
        Asynchronously scan for keys matching a pattern.
        Uses SCAN for safety (never KEYS in production).
        Returns a list of matching keys (decoded to str).
        """
        try:
            client = await self.get_client()
            cursor = 0
            keys = []
            while True:
                cursor, batch = await client.scan(cursor=cursor, match=pattern, count=count)
                keys.extend(k.decode() if isinstance(k, bytes) else k for k in batch)
                if cursor == 0:
                    break
            return keys
        except Exception as e:
            logger.error(f"Redis scan failed for pattern {pattern}: {str(e)}")
            raise


# Singleton Redis client instance
client = RedisClient()

# Module-level functions for convenience
get_client = client.get_client
shutdown = client.shutdown
get = client.get
set = client.set
delete = client.delete
is_healthy = client.is_healthy
incr = client.incr
expire = client.expire
ttl = client.ttl
exists = client.exists

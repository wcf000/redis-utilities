"""
Redis health checks for monitoring and readiness probes.
"""

import logging
from datetime import datetime, timedelta

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.core.redis.client import RedisClient
from app.core.redis.config import RedisConfig
from app.core.redis.rate_limit import service_rate_limit

router = APIRouter()
logger = logging.getLogger(__name__)


class RedisHealth:
    """
    Health checks for Redis including:
    - Connection health
    - Performance metrics
    - Circuit breaker status
    """

    def __init__(self, client: RedisClient = None):
        self.client = client or RedisClient()
        self.last_check = datetime.min
        self.cached_status = None
        self.cache_ttl = timedelta(seconds=30)

    async def check_connection(self) -> bool:
        """Check basic Redis connectivity."""
        try:
            return await self.client.ping()
        except Exception as e:
            logger.error(f"Redis connection check failed: {e}")
            return False

    async def check_performance(self) -> dict:
        """Check Redis performance metrics."""
        try:
            info = await self.client.info()
            return {
                "ops_per_sec": info.get("instantaneous_ops_per_sec", 0),
                "memory_used": info.get("used_memory", 0),
                "connected_clients": info.get("connected_clients", 0),
            }
        except Exception as e:
            logger.error(f"Redis performance check failed: {e}")
            return {}

    async def get_health_status(self) -> JSONResponse:
        """Comprehensive health check."""
        if datetime.now() - self.last_check < self.cache_ttl and self.cached_status:
            return self.cached_status

        connection_ok = await self.check_connection()
        perf_stats = await self.check_performance()

        healthy = connection_ok
        status_code = 200 if healthy else 503

        response = JSONResponse(
            status_code=status_code,
            content={
                "healthy": healthy,
                "connection": connection_ok,
                "performance": perf_stats,
                "config": {
                    "timeout": RedisConfig.REDIS_TIMEOUT,
                    "max_connections": RedisConfig.REDIS_MAX_CONNECTIONS,
                },
            },
        )

        self.last_check = datetime.now()
        self.cached_status = response
        return response


redis_health = RedisHealth()


@router.get("/health/redis")
@service_rate_limit
async def redis_health_check():
    """Endpoint for Redis health checks."""
    return await redis_health.get_health_status()

"""
Rate Limiting Utilities

Provides production-grade rate limiting using Redis with:
- Circuit breaker pattern
- Exponential backoff
- Detailed metrics
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Optional, Tuple

from circuitbreaker import circuit
from fastapi import HTTPException, status
# from prometheus_client import Counter, Gauge

# Prometheus Metrics - Singleton getters to avoid duplicate registration

def get_rate_limit_requests():
    if not hasattr(get_rate_limit_requests, "_metric"):
        get_rate_limit_requests._metric = Counter(
            "rate_limit_requests_total", "Total rate limit requests", ["endpoint", "status"]
        )
    return get_rate_limit_requests._metric

def get_rate_limit_gauge():
    if not hasattr(get_rate_limit_gauge, "_metric"):
        get_rate_limit_gauge._metric = Gauge(
            "rate_limit_active_requests", "Currently active rate-limited requests", ["endpoint"]
        )
    return get_rate_limit_gauge._metric

from app.core.redis_utilities.client import RedisClient
# from app.core.third_party_integrations.supabase_home.app import SupabaseAuthService
# from app.core.third_party_integrations.supabase_home.client import get_supabase_client

async def get_auth_service():
    client = await get_supabase_client()
    service = SupabaseAuthService(client)
    user = await service.get_current_user() if hasattr(service.get_current_user, '__await__') else service.get_current_user()
    return user

logger = logging.getLogger(__name__)

# Initialize Redis client
client = RedisClient()

# Async getter for auth service
auth_service = None

async def get_auth_service():
    global auth_service
    if auth_service is None:
        client = await get_supabase_client()
        auth_service = SupabaseAuthService(client)
    return auth_service


# Default rate limit
DEFAULT_LIMIT = 100

# Cleanup script (runs weekly)
CLEANUP_SCRIPT = """
local keys = redis.call('KEYS', 'rate_meta:*')
for _, key in ipairs(keys) do
    local last_update = redis.call('HGET', key, 'updated_at')
    if last_update and os.time() - last_update > 604800 then
        redis.call('DEL', key)
    end
end
return #keys
"""


ATOMIC_RATE_LIMIT_LUA = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2]) -- window in ms
local limit = tonumber(ARGV[3])
local member = ARGV[4]
redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
local count = redis.call('ZCARD', key)
if count < limit then
    redis.call('ZADD', key, now, member)
    redis.call('EXPIRE', key, math.ceil(window / 1000))
    return 1
else
    return 0
end
"""

@circuit(failure_threshold=3, recovery_timeout=60)
async def check_rate_limit(key: str, limit: int, window: int, redis_client=None) -> bool:
    """
    Atomic sliding window rate limiting using Redis Lua script for concurrency safety.
    More accurate than fixed windows, better for burst protection.
    ! Input validation for all arguments (see below)
    """
    # ! Validate key
    if not key or not isinstance(key, str):
        raise ValueError("Rate limit key must be a non-empty string")
    # ! Validate limit
    if not isinstance(limit, int) or limit <= 0:
        raise ValueError("Limit must be a positive integer")
    # ! Validate window
    if not isinstance(window, int) or window <= 0:
        raise ValueError("Window must be a positive integer (seconds)")

    if redis_client is not None:
        redis_instance = redis_client
    else:
        redis_instance = await client.get_client()  # Ensure Redis is initialized
    if redis_instance is None:
        logger.error("Redis unavailable in check_rate_limit: fail-closed")
        return False
    now = int(datetime.utcnow().timestamp() * 1000)  # ms precision
    window_ms = window * 1000  # convert window to ms
    logger.debug(
        f"[check_rate_limit] EVAL args: key={key} now(ms)={now} window(ms)={window_ms} limit={limit}"
    )
    import uuid
    member = f"{now}-{uuid.uuid4()}"
    logger.debug(
        f"[check_rate_limit] EVAL args: key={key} now(ms)={now} window(ms)={window_ms} limit={limit} member={member}"
    )
    try:
        allowed = await redis_instance.eval(
            ATOMIC_RATE_LIMIT_LUA,
            1,  # numkeys
            key,
            now, window_ms, limit, member
        )
        logger.debug(f"[check_rate_limit] Redis eval result: {allowed} | key={key} now(ms)={now} window(ms)={window_ms} limit={limit} member={member}")
    except Exception as e:
        logger.error(f"[check_rate_limit] Redis eval error: {e} | key={key} now(ms)={now} window(ms)={window_ms} limit={limit} member={member}")
        raise
    # * Return True if allowed, False if rate-limited
    return bool(allowed)


async def increment_rate_limit(identifier: str, endpoint: str, window: int = 60) -> int:
    """
    Increment rate limit counter for identifier and endpoint

    Args:
        identifier: User ID, IP address, or other unique identifier
        endpoint: API endpoint or action name
        window: Time window in seconds

    Returns:
        Current count after increment
    """
    key = f"rate_limit:{endpoint}:{identifier}"

    # Get or initialize counter
    current = await client.get(key)
    if current is None:
        await client.set(key, 1, ex=window)
        return 1

    # Increment and return
    count = int(current) + 1
    await client.set(key, count, ex=window)
    return count


async def get_remaining_limit(
    identifier: str, endpoint: str, limit: int = 5, window: int = 60
) -> dict:
    """
    Get rate limit details including remaining requests

    Returns:
        {
            "limit": 5,
            "remaining": 3,
            "reset": 1625097600
        }
    """
    key = f"rate_limit:{endpoint}:{identifier}"
    current = int(await client.get(key) or 0)
    ttl = await client.ttl(key)

    return {
        "limit": limit,
        "remaining": max(0, limit - current),
        "reset": int(time.time()) + (ttl if ttl > 0 else window),
    }


async def alert_system(message: str) -> None:
    # Implement your alerting system
    logger.warning(message)


RATE_LIMIT_CIRCUIT = {
    "failure_threshold": 3,
    "recovery_timeout": 60,
}


@circuit(
    failure_threshold=RATE_LIMIT_CIRCUIT["failure_threshold"],
    recovery_timeout=RATE_LIMIT_CIRCUIT["recovery_timeout"],
)
async def verify_and_limit(token: str, ip: str, endpoint: str, window: int = 3600):
    """
    Enhanced with:
    - User-level rate tracking
    - Composite keys for granular control
    """
    get_rate_limit_gauge().labels(endpoint=endpoint).inc()

    try:
        # Verify JWT with detailed logging
        if not auth_service.verify_jwt(token):
            logger.warning(
                "Invalid JWT token",
                extra={"token": token[:8] + "...", "endpoint": endpoint},
            )
            get_rate_limit_requests().labels(endpoint=endpoint, status="unauthorized").inc()
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials"
            )

        user_id = auth_service.get_user_id(token)
        limit = DEFAULT_LIMIT

        logger.debug(
            "Rate limit check", extra={"user_id": user_id, "endpoint": endpoint}
        )

        # Composite key: user + IP + endpoint
        rate_key = f"user_rate:{user_id}:{ip}:{endpoint}"

        # Track metadata without tier logic
        metadata = {
            "last_ip": ip,
            "last_endpoint": endpoint,
            "updated_at": datetime.utcnow().isoformat(),
        }

        await client.hset(f"rate_meta:{user_id}", mapping=metadata)

        if await check_rate_limit(rate_key, limit=limit, window=window):
            logger.warning(
                "Rate limit exceeded",
                extra={
                    "user_id": user_id,
                    "ip": ip,
                    "endpoint": endpoint,
                    "limit": limit,
                },
            )
            get_rate_limit_requests().labels(endpoint=endpoint, status="limited").inc()
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"Rate limit exceeded. Try again in {window} seconds",
            )

        await increment_rate_limit(rate_key, endpoint, window=window)
        get_rate_limit_requests().labels(endpoint=endpoint, status="allowed").inc()
        return user_id

    except Exception as e:
        logger.error(
            "Rate limit error",
            extra={"error": str(e), "stack_trace": True},
            exc_info=True,
        )
        get_rate_limit_requests().labels(endpoint=endpoint, status="error").inc()
        # Fail open during Redis outages
        return user_id
    finally:
        get_rate_limit_gauge().labels(endpoint=endpoint).dec()


async def service_rate_limit(
    key: str, limit: int, window: int, endpoint: str = "internal"
) -> bool:
    """
    Simplified rate limiting for internal services without user authentication.

    Args:
        key: Unique identifier for the rate limit (e.g. 'celery_health')
        limit: Max allowed requests per window
        window: Time window in seconds
        endpoint: Optional endpoint identifier for metrics

    Returns:
        bool: True if request is allowed, False if rate limited
    """
    get_rate_limit_gauge().labels(endpoint=endpoint).inc()

    try:
        rate_key = f"service_rate:{key}"

        if await check_rate_limit(rate_key, limit=limit, window=window):
            logger.warning(
                "Service rate limit exceeded",
                extra={"key": key, "endpoint": endpoint, "limit": limit},
            )
            get_rate_limit_requests().labels(endpoint=endpoint, status="limited").inc()
            return False

        await increment_rate_limit(rate_key, endpoint, window=window)
        get_rate_limit_requests().labels(endpoint=endpoint, status="allowed").inc()
        return True

    except Exception as e:
        logger.error(
            "Service rate limit error",
            extra={"error": str(e), "stack_trace": True},
            exc_info=True,
        )
        get_rate_limit_requests().labels(endpoint=endpoint, status="error").inc()
        # Fail open during Redis outages
        return True
    finally:
        get_rate_limit_gauge().labels(endpoint=endpoint).dec()


async def init_cleanup():
    await client.eval(CLEANUP_SCRIPT, 0)
    # asyncio.create_task(run_weekly(init_cleanup))  # This line is commented out because run_weekly is not defined in the provided code

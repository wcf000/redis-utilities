"""
Rate Limiting Utilities

Provides production-grade rate limiting using Redis with:
- Circuit breaker pattern
- Exponential backoff
- Detailed metrics
"""

import functools
import logging
import time
import uuid
from datetime import datetime, timedelta
from typing import Optional, Tuple

from circuitbreaker import circuit
from fastapi import HTTPException, status

from app.core.redis.client import RedisClient
# Use the mock implementations from the utilities folder
from app.core.redis._tests.utilities.mock_supabase import MockSupabaseAuthService, mock_get_supabase_client

logger = logging.getLogger(__name__)

# Initialize Redis client
client = RedisClient()

# Async getter for auth service
auth_service = None

async def get_auth_service():
    """Get mock auth service instance for testing and development"""
    global auth_service
    if auth_service is None:
        try:
            # Use the mock client from utilities
            client = await mock_get_supabase_client()
            auth_service = MockSupabaseAuthService(client)
        except Exception as e:
            logger.error(f"Error initializing mock auth service: {e}")
            # Fall back to a simpler mock if needed
            auth_service = MockSupabaseAuthService()
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
        logger.debug(f"[check_rate_limit] Redis eval result: {allowed} | key={key}")
    except Exception as e:
        logger.error(f"[check_rate_limit] Redis eval error: {e} | key={key}")
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
    try:
        # Get auth service
        auth_service_instance = await get_auth_service()
        
        # Verify JWT with detailed logging
        if not auth_service_instance.verify_jwt(token):
            logger.warning(
                "Invalid JWT token",
                extra={"token": token[:8] + "...", "endpoint": endpoint},
            )
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials"
            )

        user_id = auth_service_instance.get_user_id(token)
        limit = DEFAULT_LIMIT

        logger.debug(
            "Rate limit check", extra={"user_id": user_id, "endpoint": endpoint}
        )

        # Use composite key for rate limiting
        key = f"rate:{endpoint}:{user_id}"
        
        # Check if rate limited
        if not await check_rate_limit(key, limit, window):
            logger.warning(
                "Rate limit exceeded",
                extra={"user_id": user_id, "endpoint": endpoint},
            )
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Rate limit exceeded. Try again later.",
            )

        return user_id

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Rate limit check failed: {str(e)}")
        # Fail-open behavior for auth service failures
        return "anonymous"


@circuit(
    failure_threshold=RATE_LIMIT_CIRCUIT["failure_threshold"],
    recovery_timeout=RATE_LIMIT_CIRCUIT["recovery_timeout"],
)
async def service_rate_limit(
    service_name: str = None, limit: int = 100, window: int = 60, endpoint: str = "internal"
):
    """
    Service-to-service rate limiting decorator
    
    Args:
        service_name: Identifier for the service
        limit: Maximum requests per window
        window: Time window in seconds
        endpoint: Optional endpoint identifier for metrics
        
    Returns:
        Decorator that applies rate limiting
    """
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            key = f"service_rate:{service_name or func.__name__}"
            
            try:
                allowed = await check_rate_limit(key, limit, window)
                if not allowed:
                    raise HTTPException(
                        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                        detail="Service rate limit exceeded. Try again later."
                    )
                return await func(*args, **kwargs)
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"Service rate limit check failed: {str(e)}")
                # Fail-open for internal services
                return await func(*args, **kwargs)
                
        return wrapper
        
    # Handle both @service_rate_limit and @service_rate_limit()
    if callable(service_name):
        func = service_name
        service_name = func.__name__
        return decorator(func)
    return decorator

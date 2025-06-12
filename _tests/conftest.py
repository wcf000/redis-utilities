"""
Shared pytest fixtures and configuration for Redis tests
"""

import asyncio
import time
import logging
import socket
import subprocess
import pytest
from app.core.redis.config import RedisConfig
from unittest.mock import patch, MagicMock
from .utilities.mock_supabase import MockSupabaseAuthService, mock_get_supabase_client

@pytest.fixture(scope="session", autouse=True)
def ensure_redis_running():
    """
    Ensure a local Redis server is running for tests. If not, start it with Docker Compose.
    Make sure your docker-compose file exposes port 6379:6379 for the redis service.
    """
    host = getattr(RedisConfig, "REDIS_HOST", "localhost")
    port = getattr(RedisConfig, "REDIS_PORT", 6379)
    max_attempts = 30  # Increased attempts for slow startup
    wait_seconds = 2   # Increased wait time per attempt

    def redis_available():
        try:
            with socket.create_connection((host, port), timeout=2):
                return True
        except OSError as e:
            logging.info(f"Redis connection failed: {e}")
            return False

    if not redis_available():
        logging.info(f"Redis not running at {host}:{port}, attempting to start with Docker Compose...")
        try:
            result = subprocess.run([
                "docker-compose",
                "-f",
                "app/core/redis/docker/docker-compose.redis.yml",
                "up",
                "-d"
            ], capture_output=True, text=True)
            logging.info(f"Docker Compose output: {result.stdout}\n{result.stderr}")
        except Exception as e:
            logging.error(f"Could not start Redis with Docker Compose: {e}")
            raise
        # Wait for Redis to become available
        for attempt in range(1, max_attempts + 1):
            if redis_available():
                logging.info(f"Redis became available after {attempt} attempts.")
                break
            logging.info(f"Waiting for Redis... attempt {attempt}/{max_attempts}")
            time.sleep(wait_seconds)
        else:
            raise RuntimeError(f"Redis did not start after {max_attempts * wait_seconds} seconds.\n"
                               f"Check that your docker-compose file exposes port 6379:6379.")
    yield

# The old ensure_redis_container fixture is now redundant and removed for clarity.

import asyncio
import subprocess
import sys
import time
from unittest.mock import AsyncMock, patch

import pytest

from app.core.redis.client import RedisClient
from app.core.redis.config import RedisConfig
from app.core.redis.rate_limit import check_rate_limit
from app.core.redis.redis_cache import RedisCache


@pytest.fixture(scope="session", autouse=True)
def ensure_redis_container():
    """
    Ensure the Redis Docker container is running for integration tests.
    If not running, start it using docker-compose.redis.yml.
    """
    import logging
    import os
    # ! Use minimal local Docker Compose file for Redis in tests
    # This avoids password/persistence issues and is CI/dev safe
    compose_file = os.path.join(
        os.path.dirname(__file__),
        "..",
        "docker",
        "docker-compose.local.yml",
    )
    container_name = "redis"
    try:
        # Check if container is running
        ps = subprocess.run(["docker", "ps", "--filter", f"name={container_name}", "--format", "{{.Names}}"], capture_output=True, text=True)
        running = container_name in ps.stdout
        if not running:
            logging.info("[pytest] Redis container not running, starting via docker-compose...")
            subprocess.run(["docker-compose", "-f", compose_file, "up", "-d", "redis"], check=True)
            # Wait for container to be healthy (ping)
            for _ in range(20):
                health = subprocess.run(["docker", "inspect", "--format", "{{.State.Health.Status}}", container_name], capture_output=True, text=True)
                if "healthy" in health.stdout:
                    break
                time.sleep(2)
            else:
                raise RuntimeError("Redis container did not become healthy in time.")
        else:
            logging.info("[pytest] Redis container already running.")
    except Exception as e:
        logging.warning(f"[pytest] Could not ensure Redis container is running: {e}")
    yield

# --- Real async Redis client fixture for integration tests ---
import pytest_asyncio
import redis.asyncio as aioredis


@pytest_asyncio.fixture(autouse=True)
async def flush_redis(redis_client):
    """
    ! Flush Redis before each test for isolation
    """
    await redis_client.flushdb()

@pytest_asyncio.fixture
async def redis_client():
    """
    * Provides a real async Redis client for integration tests
    * Uses RedisConfig for connection parameters
    * Ensures proper cleanup after each test
    """
    client = aioredis.Redis(
        host=getattr(RedisConfig, "REDIS_HOST", "127.0.0.1"),
        port=getattr(RedisConfig, "REDIS_PORT", 6379),
        db=getattr(RedisConfig, "REDIS_DB", 0),
        decode_responses=True,
    )
    try:
        yield client
    finally:
        await client.close()
# ! This fixture is required for all integration tests using Redis

@pytest.fixture
def redis_cache():
    """Pre-configured RedisCache instance"""
    cache = RedisCache()
    yield cache
    # Cleanup any test data
    asyncio.run(cache.clear_namespace("test_"))


@pytest.fixture
def rate_limit_checker():
    """Rate limit checker function with default values"""

    async def checker(endpoint, identifier, limit=10, window=60):
        return await check_rate_limit(endpoint, identifier, limit, window)

    return checker


@pytest.fixture(autouse=True)
def reset_redis_config():
    """Reset RedisConfig between tests"""
    original_attrs = {k: v for k, v in vars(RedisConfig).items() if not k.startswith('__')}
    yield
    # Restore only the attributes that were present originally
    for k in list(vars(RedisConfig).keys()):
        if not k.startswith('__'):
            if k in original_attrs:
                setattr(RedisConfig, k, original_attrs[k])
            else:
                delattr(RedisConfig, k)


@pytest.fixture
def mock_time():
    """Mock time module for time-sensitive tests"""
    with patch("time.time") as mock_time:
        mock_time.return_value = 0
        yield mock_time


# Add these fixtures to your conftest.py file
@pytest.fixture
def mock_supabase_auth_service():
    """Return a mock Supabase auth service"""
    return MockSupabaseAuthService()


@pytest.fixture
def mock_supabase_client():
    """Return a mock Supabase client"""
    return mock_get_supabase_client()


@pytest.fixture
def patch_supabase():
    """
    Patch Supabase dependencies for tests
    Returns a context manager that can be used in tests
    """
    # Create patches for Supabase imports
    app_patch = patch('app.core.third_party_integrations.supabase_home.app.SupabaseAuthService', 
                      return_value=MockSupabaseAuthService())
    
    client_patch = patch('app.core.third_party_integrations.supabase_home.client.get_supabase_client', 
                         side_effect=mock_get_supabase_client)
    
    # Start and stop patches
    app_patch.start()
    client_patch.start()
    yield
    app_patch.stop()
    client_patch.stop()

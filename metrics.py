"""
Redis metrics utilities for monitoring and test instrumentation.
Provides record_metrics and other helpers for Prometheus or logging-based metrics.
"""
import logging

# from prometheus_client import REGISTRY, Counter, Gauge, Histogram

# Define a counter metric to track Redis operations
REDIS_OPERATIONS = Counter('redis_operations', 'Number of Redis operations')

def record_metrics(event: str, value: int = 1, **kwargs) -> None:
    """
    * Record a metric event for Redis operations using Prometheus counter
    Args:
        event (str): Name of the event/metric
        value (int): Value to record (default 1)
        kwargs: Additional context (e.g., shard, key, status)
    """
    REDIS_OPERATIONS.inc(value)
    logging.info(f"[metrics] Event: {event}, Value: {value}, Context: {kwargs}")

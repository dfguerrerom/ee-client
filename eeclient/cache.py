import asyncio
import hashlib
import json
import time
from collections import OrderedDict
from typing import Any, Optional


class CacheEntry:
    """Represents a cached response with TTL and task tracking."""

    def __init__(
        self,
        value: Any = None,
        error: Optional[Exception] = None,
        task: Optional[asyncio.Task] = None,
    ):
        self.value = value
        self.error = error
        self.task = task
        self.timestamp = time.time()

    def is_expired(self, ttl: float) -> bool:
        """Check if the cache entry has exceeded its TTL."""
        return (time.time() - self.timestamp) > ttl

    def get_result(self):
        """Get the cached result or raise the cached error."""
        if self.error:
            raise self.error
        return self.value


class ResponseCache:
    """Simple LRU cache with TTL for async function responses.

    Features:
    - Fixed TTL (10 seconds by default)
    - LRU eviction at max size (100 entries by default)
    - Concurrent request deduplication (singleflight)
    - Error caching to prevent retry storms
    """

    def __init__(self, ttl: float = 10.0, max_size: int = 100):
        self.ttl = ttl
        self.max_size = max_size
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._lock: Optional[asyncio.Lock] = None

    def make_cache_key(self, *args, **kwargs) -> str:
        """Generate a stable cache key from function arguments."""
        sorted_kwargs = sorted(kwargs.items())
        key_data = {"args": args, "kwargs": sorted_kwargs}
        key_json = json.dumps(key_data, sort_keys=True, default=str)
        return hashlib.sha256(key_json.encode()).hexdigest()

    async def get_or_fetch(self, key: str, fetch_func, *args, **kwargs) -> Any:
        """Get cached result or fetch new one, with concurrent request deduplication.

        Args:
            key: Cache key
            fetch_func: Async function to call if cache miss
            *args, **kwargs: Arguments to pass to fetch_func

        Returns:
            The cached or freshly fetched result
        """
        # Check cache and get task reference if needed
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            if key in self._cache:
                entry = self._cache[key]

                # If there's an in-flight task, we'll wait for it
                if entry.task and not entry.task.done():
                    in_flight_task = entry.task
                elif not entry.is_expired(self.ttl):
                    # Valid cached result - return it
                    self._cache.move_to_end(key)
                    return entry.get_result()
                else:
                    # Expired entry - remove it and fetch new
                    del self._cache[key]
                    in_flight_task = None
            else:
                in_flight_task = None

            # No in-flight task - create a new one
            if in_flight_task is None:
                task = asyncio.create_task(fetch_func(*args, **kwargs))
                self._cache[key] = CacheEntry(task=task)

                # LRU eviction
                while len(self._cache) > self.max_size:
                    self._cache.popitem(last=False)
            else:
                # Use the existing in-flight task
                task = in_flight_task

        # Wait for the task outside the lock to avoid deadlock
        try:
            result = await task
            async with self._lock:
                if key in self._cache:
                    self._cache[key] = CacheEntry(value=result)
            return result
        except Exception as e:
            async with self._lock:
                if key in self._cache:
                    self._cache[key] = CacheEntry(error=e)
            raise

    def has_pending_tasks(self) -> bool:
        """Return True if any cache entry has an in-flight task."""
        return any(
            entry.task is not None and not entry.task.done()
            for entry in self._cache.values()
        )

    def _rebind(self):
        """Create fresh loop-bound state for the current event loop."""
        self._lock = asyncio.Lock()
        self._cache.clear()

    def clear(self):
        """Clear all cached entries."""
        self._cache.clear()

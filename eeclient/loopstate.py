"""Event-loop scoping for the asyncio objects a session owns.

An :class:`~eeclient.client.EESession` lives as long as the process, but every
asyncio primitive binds to the first event loop that touches it and raises on
any other: locks and semaphores through ``_LoopBoundMixin``, tasks through the
loop that scheduled them, and an ``httpx.AsyncClient`` through the transports
in its connection pool. A session therefore cannot own any of them directly.

Two shapes make this concrete. A host that gives each user session its own loop
and closes it when that session restarts leaves a process-scoped session holding
a pool of dead connections. And a sync-to-async bridge drives one session from
two loops at once — a private ``run_forever`` loop in a background thread for
the blocking API, the caller's loop for the ``*_async`` API — so "rebuild when
the old loop closed" is not enough either; there both loops are open.

What is left on the session is what is safe to share: the credentials, and the
rate limiter, which meters a per-user Earth Engine quota rather than per-loop
concurrency and so must stay loop-free by construction.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
import weakref
from typing import List, Optional, Tuple

import httpx

from eeclient.cache import ResponseCache

logger = logging.getLogger("eeclient")

INFLIGHT_LIMIT = 30
QPS_LIMIT = 60
CACHE_TTL = 10.0
CACHE_SIZE = 100


class SimpleRateLimiter:
    """A process-wide QPS gate, deliberately free of any event loop.

    Earth Engine meters quota per user, so one session needs one gate however
    many loops drive it; an ``asyncio.Lock`` would bind the gate to whichever
    loop reached it first. The slot is reserved under a plain lock with no
    ``await`` inside it and the wait happens after the lock is released, so
    callers queue on the clock rather than behind each other.
    """

    def __init__(self, qps: Optional[float]):
        self.qps = qps
        self._lock = threading.Lock()
        self._next = 0.0

    async def acquire(self) -> None:
        if not self.qps or self.qps <= 0:
            return
        with self._lock:
            now = time.monotonic()
            start = max(now, self._next)
            self._next = start + 1.0 / self.qps
        delay = start - now
        if delay > 0:
            await asyncio.sleep(delay)


class LoopResources:
    """Everything a session owns that belongs to exactly one event loop."""

    __slots__ = ("inflight", "auth_refresh_lock", "client_lock", "cache", "client")

    def __init__(self) -> None:
        self.inflight = asyncio.BoundedSemaphore(INFLIGHT_LIMIT)
        self.auth_refresh_lock = asyncio.Lock()
        self.client_lock = asyncio.Lock()
        self.cache = ResponseCache(ttl=CACHE_TTL, max_size=CACHE_SIZE)
        self.client: Optional[httpx.AsyncClient] = None


def release_sockets(client: httpx.AsyncClient) -> None:
    """Free a pool whose loop is gone, without touching the dead loop.

    ``aclose()`` is not usable here — it drives transports that belong to a loop
    which can no longer run, which is the ``write_eof()`` on a closed transport
    reported in issue #37. Closing the raw sockets instead hands the descriptors
    back now rather than whenever the cyclic collector next reaches them; uvloop
    already released them at ``loop.close()``, plain asyncio did not.

    The walk reads httpcore and anyio internals, so it is best effort: a layout
    change costs deferred cleanup, never an exception.
    """
    try:
        pool = client._transport._pool  # type: ignore[attr-defined]
        connections = list(pool.connections)
    except Exception:  # pragma: no cover - depends on httpcore internals
        return
    for connection in connections:
        try:
            stream = connection._connection._network_stream
            for attribute in ("_stream", "transport_stream", "_transport"):
                inner = getattr(stream, attribute, None)
                if inner is not None:
                    stream = inner
            socket = getattr(stream, "_sock", None)
            if socket is not None:
                socket.close()
        except Exception:  # pragma: no cover - depends on httpcore internals
            continue


class LoopResourceRegistry:
    """Resolves the resources belonging to the running loop.

    Keys are held weakly, so a loop dropped without being closed takes its
    resources with it. A loop that is closed but still referenced is reaped on
    the next lookup miss, which is the churn a restarting host loop produces.
    """

    def __init__(self) -> None:
        self._by_loop: (
            "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, LoopResources]"
        ) = weakref.WeakKeyDictionary()
        # Two loops can look up resources from two threads at the same moment.
        self._guard = threading.Lock()

    def __len__(self) -> int:
        return len(self._by_loop)

    def current(self) -> LoopResources:
        """Resources for the running loop, built on that loop's first use."""
        loop = asyncio.get_running_loop()
        with self._guard:
            resources = self._by_loop.get(loop)
            if resources is not None:
                return resources
            self._reap()
            resources = LoopResources()
            self._by_loop[loop] = resources
            logger.debug("EESession: opened resources for loop %r", loop)
            return resources

    def _reap(self) -> None:
        """Drop the resources of every loop that can no longer run. Call held."""
        for loop in [loop for loop in self._by_loop if loop.is_closed()]:
            resources = self._by_loop.pop(loop, None)
            if resources is None:
                continue
            _discard(resources)
            logger.debug("EESession: reaped resources of closed loop %r", loop)

    def drain(self) -> List[Tuple[asyncio.AbstractEventLoop, LoopResources]]:
        """Forget every loop, returning those whose client still needs closing.

        Resources of loops that cannot run are released here. The rest are
        handed back, because only the loop that opened a transport may close it.
        """
        with self._guard:
            entries = list(self._by_loop.items())
            self._by_loop = weakref.WeakKeyDictionary()
        pending = []
        for loop, resources in entries:
            if loop.is_closed() or not loop.is_running():
                _discard(resources)
            else:
                pending.append((loop, resources))
        return pending


def _discard(resources: LoopResources) -> None:
    if resources.client is not None:
        release_sockets(resources.client)
        resources.client = None
    resources.cache.clear()

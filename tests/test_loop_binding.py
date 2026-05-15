"""Tests for EESession event-loop binding contract (issue #14).

Verifies:
1. Normal same-loop use
2. Rejection of concurrent second-loop use
3. Rebinding only after the previous loop is fully idle
4. Wrong-loop aclose() behavior
5. Pending cache-task handling during attempted rebinding
"""

import asyncio
import threading

import pytest

from eeclient.cache import CacheEntry
from eeclient.client import EESession
from eeclient.exceptions import EELoopError


@pytest.fixture(autouse=True)
def _set_sepal_host(monkeypatch):
    """EESession requires SEPAL_HOST when using sepal headers."""
    monkeypatch.setenv("SEPAL_HOST", "test.sepal.io")


# ---------------------------------------------------------------------------
# Test 1: Normal same-loop use
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_same_loop_use(dummy_headers):
    """Session binds on first async call; subsequent calls on the same loop work."""
    session = EESession(sepal_headers=dummy_headers)
    assert session._bound_loop is None

    # First async entry point binds the session
    session._ensure_bound()
    loop = asyncio.get_running_loop()
    assert session._bound_loop is loop
    assert session._inflight is not None
    assert session._auth_refresh_lock is not None
    assert session._client_lock is not None

    # Second call on the same loop is a no-op
    session._ensure_bound()
    assert session._bound_loop is loop


# ---------------------------------------------------------------------------
# Test 2: Rejection of concurrent second-loop use
# ---------------------------------------------------------------------------


def test_concurrent_second_loop_rejected(dummy_headers):
    """Using the session from a second loop while the first has active work raises."""
    loop_a = asyncio.new_event_loop()
    session = EESession(sepal_headers=dummy_headers)

    # Bind to loop A and simulate active work (acquire a semaphore slot)
    async def bind_and_hold():
        session._ensure_bound()
        await session._inflight.acquire()

    loop_a.run_until_complete(bind_and_hold())
    assert session._has_active_work()

    # Try to use from loop B in another thread
    error = None

    def run_on_loop_b():
        nonlocal error
        loop_b = asyncio.new_event_loop()
        try:

            async def attempt_use():
                session._ensure_bound()

            loop_b.run_until_complete(attempt_use())
        except EELoopError as e:
            error = e
        finally:
            loop_b.close()

    t = threading.Thread(target=run_on_loop_b)
    t.start()
    t.join()

    assert error is not None
    assert "active work" in str(error)

    # Cleanup
    loop_a.run_until_complete(_release_semaphore(session))
    loop_a.close()


async def _release_semaphore(session):
    session._inflight.release()


# ---------------------------------------------------------------------------
# Test 3: Rebinding after previous loop is fully idle
# ---------------------------------------------------------------------------


def test_rebind_after_idle(dummy_headers):
    """Rebind succeeds when old loop has no active work."""
    loop_a = asyncio.new_event_loop()
    session = EESession(sepal_headers=dummy_headers)

    async def use_on_loop():
        session._ensure_bound()

    loop_a.run_until_complete(use_on_loop())
    assert session._bound_loop is loop_a
    assert not session._has_active_work()

    # Rebind to loop B
    loop_b = asyncio.new_event_loop()
    loop_b.run_until_complete(use_on_loop())
    assert session._bound_loop is loop_b

    loop_a.close()
    loop_b.close()


# ---------------------------------------------------------------------------
# Test 4: Wrong-loop aclose() behavior
# ---------------------------------------------------------------------------


def test_aclose_wrong_loop_raises(dummy_headers):
    """aclose() from a different loop raises EELoopError; client is NOT closed."""
    loop_a = asyncio.new_event_loop()
    session = EESession(sepal_headers=dummy_headers)

    async def bind_session():
        session._ensure_bound()

    loop_a.run_until_complete(bind_session())

    # Try aclose from loop B
    loop_b = asyncio.new_event_loop()
    error = None

    async def close_on_b():
        nonlocal error
        try:
            await session.aclose()
        except EELoopError as e:
            error = e

    loop_b.run_until_complete(close_on_b())

    assert error is not None
    assert "aclose()" in str(error)

    loop_a.close()
    loop_b.close()


# ---------------------------------------------------------------------------
# Test 5: Pending cache-task blocks rebinding
# ---------------------------------------------------------------------------


def test_pending_cache_task_blocks_rebind(dummy_headers):
    """An in-flight cache task prevents rebinding to a new loop."""
    loop_a = asyncio.new_event_loop()
    session = EESession(sepal_headers=dummy_headers)

    async def bind_and_add_pending_task():
        session._ensure_bound()
        # Insert a cache entry with an unresolved task
        future = asyncio.get_running_loop().create_future()
        task = asyncio.ensure_future(future)
        session._assets_cache._cache["test_key"] = CacheEntry(task=task)

    loop_a.run_until_complete(bind_and_add_pending_task())
    assert session._assets_cache.has_pending_tasks()

    # Try to rebind from loop B
    loop_b = asyncio.new_event_loop()
    error = None

    async def try_rebind():
        nonlocal error
        try:
            session._ensure_bound()
        except EELoopError as e:
            error = e

    loop_b.run_until_complete(try_rebind())

    assert error is not None
    assert "active work" in str(error)

    # Cleanup: resolve the future so the task completes
    async def resolve():
        for entry in session._assets_cache._cache.values():
            if entry.task and not entry.task.done():
                entry.task.cancel()
                try:
                    await entry.task
                except asyncio.CancelledError:
                    pass

    loop_a.run_until_complete(resolve())
    loop_a.close()
    loop_b.close()

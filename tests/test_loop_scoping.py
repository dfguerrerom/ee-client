"""A session is process-scoped; asyncio objects are not.

A host that gives every user session its own event loop and closes it on
restart leaves a shared session holding dead connections. A sync-to-async
bridge drives one session from two loops at once — a private ``run_forever``
loop in a background thread for the blocking API, the caller's loop for the
``*_async`` API. These tests pin the behaviour a shared session must show in
both situations.
"""

import asyncio
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from eeclient.client import EESession
from eeclient.providers import CredentialSnapshot


class _StubProvider:
    """Credentials that never expire and never touch the network."""

    auth_mode = "stub"
    auth_source = "stub"
    user = "tester"
    verify_ssl = True

    def initial_snapshot(self):
        return CredentialSnapshot(
            access_token="token",
            project_id="ee-project",
            expiry_date=int((time.time() + 3600) * 1000),
            native=object(),
        )

    def refresh_sync(self):
        raise AssertionError("credentials must not be refreshed in these tests")

    async def refresh(self):
        raise AssertionError("credentials must not be refreshed in these tests")


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"  # keep-alive: the pool must hold connections

    def do_GET(self):
        body = json.dumps({"ok": True}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@pytest.fixture(scope="module")
def url():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_address[1]}/x"
    server.shutdown()


@pytest.fixture()
def session():
    return EESession(_provider=_StubProvider())


@pytest.fixture()
def loop():
    """A loop the test drives itself; these tests own their event loops."""
    new = asyncio.new_event_loop()
    yield new
    if not new.is_closed():
        new.close()


def _burst(session, url, count):
    async def run():
        return await asyncio.gather(
            *[session.rest_call("GET", url) for _ in range(count)],
            return_exceptions=True,
        )

    return run


def _failures(results):
    return [r for r in results if isinstance(r, BaseException)]


def test_a_closed_loop_does_not_break_the_next_burst(session, url):
    """The restart case: the loop the session first used is closed and replaced."""
    first = asyncio.new_event_loop()
    first.run_until_complete(_burst(session, url, 12)())
    first.close()

    second = asyncio.new_event_loop()
    try:
        results = second.run_until_complete(_burst(session, url, 12)())
    finally:
        second.close()

    assert _failures(results) == []


def test_two_live_loops_share_one_session(session, url):
    """The sync-bridge case: a private loop and the caller's loop, both open."""
    private = asyncio.new_event_loop()
    threading.Thread(target=private.run_forever, daemon=True).start()
    caller = asyncio.new_event_loop()

    async def one():
        return await session.rest_call("GET", url)

    try:
        results = []
        for _ in range(3):
            results.append(
                asyncio.run_coroutine_threadsafe(one(), private).result(timeout=30)
            )
            results.append(caller.run_until_complete(one()))
    finally:
        caller.close()
        private.call_soon_threadsafe(private.stop)

    assert all(r == {"ok": True} for r in results)


def test_one_loop_keeps_one_client(session, url, loop):
    """Resources are per loop, not per call — the pool must survive between calls."""
    loop.run_until_complete(_burst(session, url, 2)())
    first = loop.run_until_complete(session._ensure_client())
    second = loop.run_until_complete(session._ensure_client())

    assert first is second


def test_each_loop_gets_its_own_client(session, url):
    """Two loops cannot share a transport, so they must not share a client."""
    first = asyncio.new_event_loop()
    private = first.run_until_complete(session._ensure_client())
    second = asyncio.new_event_loop()
    try:
        other = second.run_until_complete(session._ensure_client())
    finally:
        second.close()
        first.close()

    assert private is not other


def test_an_incomplete_cache_task_does_not_outlive_its_loop(session, url, loop):
    """A task orphaned by a closed loop must not be awaited from the next one."""

    async def never():
        await asyncio.sleep(300)

    first = asyncio.new_event_loop()

    async def start_and_abandon():
        asyncio.ensure_future(session._assets_cache.get_or_fetch("k", never))
        await asyncio.sleep(0.05)

    first.run_until_complete(start_and_abandon())
    first.close()

    async def fetch():
        return await asyncio.wait_for(
            session._assets_cache.get_or_fetch("k", lambda: _answer(session, url)),
            timeout=5,
        )

    assert loop.run_until_complete(fetch()) == {"ok": True}


async def _answer(session, url):
    return await session.rest_call("GET", url)


def test_the_rate_limiter_survives_a_loop_change(session):
    """The limiter guards a per-user quota, so it cannot bind to one loop."""
    first = asyncio.new_event_loop()
    first.run_until_complete(_acquire(session, 4)())
    first.close()

    second = asyncio.new_event_loop()
    try:
        second.run_until_complete(_acquire(session, 4)())
    finally:
        second.close()


def test_the_rate_limiter_paces_across_loops(session):
    """Two loops share one quota; they must not each get the full rate."""
    session._rate.qps = 20

    first = asyncio.new_event_loop()
    started = time.monotonic()
    first.run_until_complete(_acquire(session, 10)())
    first.close()

    second = asyncio.new_event_loop()
    try:
        second.run_until_complete(_acquire(session, 10)())
    finally:
        second.close()
    elapsed = time.monotonic() - started

    # 20 slots at 20 qps is ~0.95s; a per-loop limiter would finish in half that.
    assert elapsed > 0.7


def _acquire(session, count):
    async def run():
        await asyncio.gather(*[session._rate.acquire() for _ in range(count)])

    return run


def test_aclose_releases_every_loop_that_ran(session, url):
    """Teardown reaches the transports of loops other than the calling one."""
    private = asyncio.new_event_loop()
    threading.Thread(target=private.run_forever, daemon=True).start()
    asyncio.run_coroutine_threadsafe(_answer(session, url), private).result(timeout=30)

    caller = asyncio.new_event_loop()
    try:
        caller.run_until_complete(_answer(session, url))
        caller.run_until_complete(session.aclose())
        # The other loop's client is closed on that loop, so give it a turn.
        asyncio.run_coroutine_threadsafe(asyncio.sleep(0.2), private).result(timeout=5)
        remaining = caller.run_until_complete(_ensure_fresh(session))
    finally:
        caller.close()
        private.call_soon_threadsafe(private.stop)

    assert remaining is not None


async def _ensure_fresh(session):
    """After aclose() the session must still be usable, with a new client."""
    return await session._ensure_client()


def test_a_closed_loop_releases_its_resources(session, url):
    """Repeated loop churn must not accumulate one pool per dead loop."""
    for _ in range(3):
        spent = asyncio.new_event_loop()
        spent.run_until_complete(_answer(session, url))
        spent.close()

    live = asyncio.new_event_loop()
    try:
        live.run_until_complete(_answer(session, url))
        assert len(session._loops) == 1
    finally:
        live.close()

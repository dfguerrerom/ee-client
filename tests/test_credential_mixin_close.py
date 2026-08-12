import pytest

from eeclient.client import EESession
from eeclient.credential_mixin import CredentialMixin


class _StubProvider:
    """Minimal CredentialProvider: no snapshot, no network."""

    auth_mode = "stub"
    auth_source = "stub"
    user = "tester"
    verify_ssl = True

    def initial_snapshot(self):
        return None

    def refresh_sync(self):
        raise AssertionError("refresh must not be called by close()")

    async def refresh(self):
        raise AssertionError("refresh must not be called by close()")


class _SpyService:
    def __init__(self):
        self.closed = 0

    def close(self):
        self.closed += 1


def test_close_closes_the_service_and_drops_the_reference():
    holder = CredentialMixin(provider=_StubProvider())
    service = _SpyService()
    holder._service = service

    holder.close()

    assert service.closed == 1
    assert holder._service is None


def test_close_is_idempotent():
    holder = CredentialMixin(provider=_StubProvider())
    service = _SpyService()
    holder._service = service

    holder.close()
    holder.close()

    assert service.closed == 1


def test_close_without_a_service_is_a_no_op():
    CredentialMixin(provider=_StubProvider()).close()  # must not raise


def test_close_does_not_touch_an_async_transport():
    """EESession inherits close(); its httpx client still needs `await aclose()`."""
    session = EESession(_provider=_StubProvider())
    sentinel = object()
    session._client = sentinel

    session.close()

    assert session._client is sentinel


@pytest.mark.asyncio
async def test_aclose_is_the_complete_teardown():
    """aclose() must also release the sync side, or it leaks what close() owns."""
    session = EESession(_provider=_StubProvider())
    service = _SpyService()
    session._service = service

    await session.aclose()

    assert service.closed == 1
    assert session._service is None
    assert session._client is None

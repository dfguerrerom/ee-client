"""Gated integration test: headerless EESession() from EARTHENGINE_TOKEN.

Named to avoid conftest's ``test_integration_*`` collect-ignore glob, and skipped
unless EARTHENGINE_TOKEN is present (set in CI, typically a service account).
"""

import asyncio
import os

import pytest

from eeclient.client import EESession

pytestmark = pytest.mark.skipif(
    not os.getenv("EARTHENGINE_TOKEN"),
    reason="requires EARTHENGINE_TOKEN (service account) in CI",
)


def test_headerless_session_authenticates_from_earthengine_token(monkeypatch, tmp_path):
    # Non-SEPAL context (non-sepal-user home). SEPAL_HOST may be set; it must NOT
    # gate the token path (D11). Isolate home so no local cred file interferes.
    monkeypatch.setattr("eeclient.providers.Path.home", lambda: tmp_path)
    (tmp_path / ".config/earthengine").mkdir(parents=True)

    session = EESession()  # bare -> resolves EARTHENGINE_TOKEN, refresh deferred
    assert session._credentials is None  # not refreshed at construction (D6)

    headers = asyncio.run(session.get_headers()).model_dump(by_alias=True)
    assert headers["Authorization"].startswith("Bearer ")
    assert headers["x-goog-user-project"]
    asyncio.run(session.aclose())


def test_explicit_from_earthengine_token():
    session = EESession.from_earthengine_token()
    assert session.access_token
    assert session.project_id

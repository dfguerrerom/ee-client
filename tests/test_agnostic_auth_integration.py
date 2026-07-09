"""Gated integration test: EESession.from_default() from EARTHENGINE_TOKEN.

Named to avoid conftest's ``test_integration_*`` collect-ignore glob, and skipped
unless EARTHENGINE_TOKEN is present (set in CI, typically a service account).
"""

import os

import pytest

from eeclient.client import EESession

pytestmark = pytest.mark.skipif(
    not os.getenv("EARTHENGINE_TOKEN"),
    reason="requires EARTHENGINE_TOKEN (service account) in CI",
)


def test_from_default_authenticates_from_earthengine_token(monkeypatch, tmp_path):
    # Non-SEPAL context (non-sepal-user home). SEPAL_HOST may be set; it must NOT
    # gate the token path (D11). Isolate home so no local cred file interferes.
    monkeypatch.setattr("eeclient.providers.Path.home", lambda: tmp_path)
    (tmp_path / ".config/earthengine").mkdir(parents=True)

    session = EESession.from_default()  # explicit env resolution, refresh deferred
    assert session._credentials is None  # non-blocking construction (deferred)

    session.set_credentials_sync()  # trigger the real token mint
    assert session.access_token
    assert session.project_id
    headers = session.get_current_headers().model_dump(by_alias=True)
    assert headers["Authorization"].startswith("Bearer ")


def test_explicit_from_earthengine_token():
    session = EESession.from_earthengine_token()
    assert session.access_token
    assert session.project_id

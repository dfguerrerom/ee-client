import json
from datetime import datetime
from unittest import mock

import pytest

from eeclient.client import EESession


def _fake_creds():
    c = mock.Mock()
    c.token = "AT"
    c.expiry = datetime(2030, 1, 1)
    c.refresh.side_effect = lambda req: None
    return c


# --- Factories ---
def test_from_google_credentials_eager_and_headers():
    s = EESession.from_google_credentials(_fake_creds(), project="p")
    assert s.access_token == "AT" and s.project_id == "p"
    h = s.get_current_headers().model_dump(by_alias=True)
    assert h["Authorization"] == "Bearer AT"
    assert h["x-goog-user-project"] == "p"
    assert h["Username"] == "local_user"


def test_from_service_account():
    with mock.patch("eeclient.providers.service_account") as sa:
        sa.Credentials.from_service_account_info.return_value = _fake_creds()
        s = EESession.from_service_account(
            {"type": "service_account", "project_id": "sp"}
        )
    assert s.project_id == "sp" and s.auth_mode == "service_account"


def test_from_application_default():
    with mock.patch(
        "eeclient.providers.google.auth.default",
        return_value=(_fake_creds(), "adcproj"),
    ):
        s = EESession.from_application_default()
    assert s.project_id == "adcproj" and s.auth_mode == "adc"


def test_from_earthengine_token_env(monkeypatch):
    monkeypatch.setenv(
        "EARTHENGINE_TOKEN", json.dumps({"refresh_token": "rt", "project": "p"})
    )
    with mock.patch("eeclient.providers.oauth_credentials") as oc:
        oc.Credentials.return_value = _fake_creds()
        s = EESession.from_earthengine_token()
    assert s.project_id == "p" and s.auth_mode == "oauth"


# --- Backward compatibility: SEPAL session construction, no network ---
def test_sepal_session_construction_no_network(dummy_headers, monkeypatch):
    monkeypatch.setenv("SEPAL_HOST", "sepal.example.org")
    s = EESession(dummy_headers)  # googleTokens present -> no download
    assert s.auth_mode == "sepal"
    assert s.project_id == "ee-project"
    assert s.access_token == "test_token"
    assert s.is_expired() is True  # dummy expiry = 1 ms
    assert s.user == "admin"
    h = s.get_current_headers().model_dump(by_alias=True)
    assert h["Username"] == "admin" and h["Authorization"] == "Bearer test_token"


def test_sepal_credential_mixin_alias():
    from eeclient.sepal_credential_mixin import CredentialMixin, SepalCredentialMixin

    assert SepalCredentialMixin is CredentialMixin


# --- Explicit-only: a bare EESession() must not auto-resolve ---
def test_bare_session_requires_a_source():
    from eeclient.exceptions import EEClientError

    with pytest.raises(EEClientError):
        EESession()  # no headers, no provider -> caller must specify directly


def test_from_default_resolves_earthengine_token(monkeypatch, tmp_path):
    monkeypatch.setattr("eeclient.providers.Path.home", lambda: tmp_path)
    (tmp_path / ".config/earthengine").mkdir(parents=True)
    monkeypatch.setenv(
        "EARTHENGINE_TOKEN", json.dumps({"refresh_token": "rt", "project": "p"})
    )
    with mock.patch("eeclient.providers.oauth_credentials") as oc:
        oc.Credentials.return_value = _fake_creds()
        s = EESession.from_default()
        assert s.auth_mode == "oauth"
        assert s._credentials is None  # deferred: no refresh at construction
        s.set_credentials_sync()  # explicit sync refresh
    assert s.access_token == "AT" and s.project_id == "p"

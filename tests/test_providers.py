import json
import time
from datetime import datetime, timezone
from unittest import mock

import pytest

from eeclient.providers import (
    DEFAULT_SCOPES,
    CredentialSnapshot,
    GoogleAuthProvider,
    SepalFileProvider,
    SepalSessionProvider,
    _credentials_from_earthengine_token,
    _credentials_from_mapping,
    _expiry_to_epoch_ms,
)


# --- Task 1: foundations ---
def test_default_scopes_include_drive():
    assert "https://www.googleapis.com/auth/earthengine" in DEFAULT_SCOPES
    assert "https://www.googleapis.com/auth/cloud-platform" in DEFAULT_SCOPES
    assert "https://www.googleapis.com/auth/drive" in DEFAULT_SCOPES  # D7: Drive export


def test_snapshot_fields():
    snap = CredentialSnapshot(
        access_token="t", project_id="p", expiry_date=1, native=object()
    )
    assert (snap.access_token, snap.project_id, snap.expiry_date) == ("t", "p", 1)


# --- Task 2: builders ---
def test_expiry_utc_ms():
    dt = datetime(2030, 1, 1, 0, 0, 0)  # naive
    expected = int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
    assert _expiry_to_epoch_ms(dt) == expected


def test_expiry_none_is_future():
    assert _expiry_to_epoch_ms(None) > int(time.time() * 1000)


def test_mapping_service_account_shape():
    data = {"type": "service_account", "project_id": "sa-proj"}
    with mock.patch("eeclient.providers.service_account") as sa:
        sa.Credentials.from_service_account_info.return_value = "SACREDS"
        creds, project = _credentials_from_mapping(data, DEFAULT_SCOPES)
    assert creds == "SACREDS" and project == "sa-proj"


def test_mapping_oauth_shape_uses_ee_defaults():
    data = {"refresh_token": "rt", "project": "oauth-proj"}
    with mock.patch("eeclient.providers.oauth_credentials") as oc:
        oc.Credentials.return_value = "OAUTHCREDS"
        creds, project = _credentials_from_mapping(data, DEFAULT_SCOPES)
        kwargs = oc.Credentials.call_args.kwargs
    assert creds == "OAUTHCREDS" and project == "oauth-proj"
    assert kwargs["refresh_token"] == "rt"
    assert kwargs["client_id"] and kwargs["client_secret"] and kwargs["token_uri"]


def test_earthengine_token_bare_refresh_string():
    with mock.patch("eeclient.providers.oauth_credentials") as oc:
        oc.Credentials.return_value = "OAUTHCREDS"
        creds, _ = _credentials_from_earthengine_token(
            "just-a-refresh-token", DEFAULT_SCOPES
        )
    assert creds == "OAUTHCREDS"


# --- Task 3: GoogleAuthProvider ---
def _fake_creds(token="AT", expiry=datetime(2030, 1, 1)):
    c = mock.Mock()
    c.token = token
    c.expiry = expiry

    def _refresh(req):  # simulate google-auth mutating token/expiry in place
        c.token = token
        c.expiry = expiry

    c.refresh.side_effect = _refresh
    return c


def test_google_provider_refresh_sync_builds_snapshot():
    prov = GoogleAuthProvider(_fake_creds(), "proj", auth_mode="service_account")
    snap = prov.refresh_sync()
    assert isinstance(snap, CredentialSnapshot)
    assert snap.access_token == "AT" and snap.project_id == "proj"
    assert snap.expiry_date > 0
    assert prov.initial_snapshot() is None  # deferred (D6)


def test_google_provider_project_from_env(monkeypatch):
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "envproj")
    snap = GoogleAuthProvider(_fake_creds(), None).refresh_sync()
    assert snap.project_id == "envproj"


def test_google_provider_missing_project_raises(monkeypatch):
    monkeypatch.delenv("GOOGLE_CLOUD_PROJECT", raising=False)
    from eeclient.exceptions import EEClientError

    with pytest.raises(EEClientError):
        GoogleAuthProvider(_fake_creds(), None).refresh_sync()


@pytest.mark.asyncio
async def test_google_provider_async_refresh():
    prov = GoogleAuthProvider(_fake_creds(), "proj")
    snap = await prov.refresh()
    assert snap.access_token == "AT"


# --- Task 4: SepalFileProvider ---
def test_sepal_file_provider_reads(tmp_path):
    p = tmp_path / "creds"
    p.write_text(
        json.dumps(
            {
                "accessToken": "FT",
                "accessTokenExpiryDate": 9999999999999,
                "projectId": "fp",
            }
        )
    )
    prov = SepalFileProvider(p)
    snap = prov.initial_snapshot()
    assert snap.access_token == "FT" and snap.project_id == "fp"
    assert prov.auth_mode == "file"


# --- Task 5: SepalSessionProvider ---
SEPAL_HEADERS = {
    "cookie": ["SEPAL-SESSIONID=s:abc;"],
    "sepal-user": [
        '{"id":1,"username":"admin","googleTokens":{"accessToken":"ST",'
        '"accessTokenExpiryDate":9999999999999,"projectId":"sp"},"status":"ACTIVE"}'
    ],
}


def test_sepal_session_initial_snapshot_from_headers(monkeypatch):
    monkeypatch.setenv("SEPAL_HOST", "sepal.example.org")
    prov = SepalSessionProvider(SEPAL_HEADERS)
    snap = prov.initial_snapshot()
    assert snap.access_token == "ST" and snap.project_id == "sp"
    assert prov.auth_mode == "sepal" and prov.user == "admin"

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Protocol, Tuple

import google.auth
from google.auth.transport.requests import Request as _GoogleRequest
from google.oauth2 import credentials as oauth_credentials
from google.oauth2 import service_account
from ee import oauth as ee_oauth

from eeclient.exceptions import EEClientError

log = logging.getLogger("eeclient")

# Mirrors ee.oauth.SCOPES so a session behaves like a normal `earthengine authenticate`
# login — notably `drive`, without which this package's Drive export would break (D7).
DEFAULT_SCOPES = [
    "https://www.googleapis.com/auth/earthengine",
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/devstorage.full_control",
]


@dataclass
class CredentialSnapshot:
    """Normalized credential state produced by every provider."""

    access_token: str
    project_id: str
    expiry_date: int  # epoch milliseconds, UTC
    native: object  # underlying creds object (truthy)


class CredentialProvider(Protocol):
    auth_mode: str
    user: str
    verify_ssl: bool

    def initial_snapshot(self) -> Optional[CredentialSnapshot]: ...

    def refresh_sync(self) -> CredentialSnapshot: ...

    async def refresh(self) -> CredentialSnapshot: ...


# ---------------------------------------------------------------------------
# Credential builders
# ---------------------------------------------------------------------------
def _expiry_to_epoch_ms(expiry: Optional[datetime]) -> int:
    """Convert a google-auth expiry (naive UTC datetime) to epoch milliseconds.

    google-auth stores ``Credentials.expiry`` as a naive datetime already in
    UTC; a bare ``.timestamp()`` would wrongly assume local time. When expiry is
    unknown, return a near-future value so the refresh loop still triggers.
    """
    if expiry is None:
        return int((time.time() + 55 * 60) * 1000)
    return int(expiry.replace(tzinfo=timezone.utc).timestamp() * 1000)


def _service_account_credentials(info_or_path, scopes) -> Tuple[object, Optional[str]]:
    """Build service-account credentials from a dict or a key-file path."""
    if isinstance(info_or_path, (str, Path)):
        creds = service_account.Credentials.from_service_account_file(
            str(info_or_path), scopes=scopes
        )
        project = getattr(creds, "project_id", None)
    else:
        creds = service_account.Credentials.from_service_account_info(
            info_or_path, scopes=scopes
        )
        project = info_or_path.get("project_id")
    return creds, project


def _credentials_from_mapping(data: dict, scopes) -> Tuple[object, Optional[str]]:
    """Route a credential mapping to SA or OAuth credentials.

    A service-account key (``type == "service_account"``) yields SA credentials;
    anything else is treated as an OAuth refresh-token payload, filling
    client id/secret and token URI from ``ee.oauth`` defaults when absent.
    """
    if data.get("type") == "service_account":
        return _service_account_credentials(data, scopes)
    creds = oauth_credentials.Credentials(
        None,
        refresh_token=data["refresh_token"],
        token_uri=data.get("token_uri", ee_oauth.TOKEN_URI),
        client_id=data.get("client_id", ee_oauth.CLIENT_ID),
        client_secret=data.get("client_secret", ee_oauth.CLIENT_SECRET),
        scopes=data.get("scopes", scopes),
    )
    project = data.get("project") or data.get("quota_project_id")
    return creds, project


def _credentials_from_earthengine_token(
    raw: str, scopes
) -> Tuple[object, Optional[str]]:
    """Build credentials from the EARTHENGINE_TOKEN convention.

    Accepts a JSON service-account key, a JSON OAuth payload, or a bare
    refresh-token string.
    """
    raw = (raw or "").strip()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        data = {"refresh_token": raw}  # tolerate a bare refresh-token string
    return _credentials_from_mapping(data, scopes)


def _oauth_credentials_from_ee_file(scopes) -> Tuple[object, Optional[str]]:
    """Build OAuth credentials from ``~/.config/earthengine/credentials``.

    Uses ``ee.oauth.get_credentials_arguments()`` — a pure, no-network parser.
    """
    args = ee_oauth.get_credentials_arguments()
    creds = oauth_credentials.Credentials(
        None,
        refresh_token=args["refresh_token"],
        token_uri=args["token_uri"],
        client_id=args["client_id"],
        client_secret=args["client_secret"],
        scopes=args.get("scopes") or scopes,
    )
    return creds, args.get("quota_project_id")


def _application_default_credentials(scopes) -> Tuple[object, Optional[str]]:
    """Resolve Application Default Credentials (explicit opt-in only)."""
    creds, project = google.auth.default(scopes=scopes)
    return creds, project


# ---------------------------------------------------------------------------
# Providers
# ---------------------------------------------------------------------------
class GoogleAuthProvider:
    """Wraps a live google.auth Credentials (service account, OAuth, or ADC)."""

    def __init__(
        self, credentials, project_id, *, auth_mode="oauth", user="local_user"
    ):
        self._creds = credentials
        self._project_id = project_id
        self.auth_mode = auth_mode
        self.user = user
        self.verify_ssl = True

    def initial_snapshot(self) -> Optional[CredentialSnapshot]:
        # Defer the (networked) token refresh to first use (D6).
        return None

    def _resolved_project(self) -> str:
        project = self._project_id or os.getenv("GOOGLE_CLOUD_PROJECT")
        if not project:
            raise EEClientError(
                "No Google Cloud project for these credentials. Pass project=… "
                "or set GOOGLE_CLOUD_PROJECT."
            )
        return project

    def _snapshot(self) -> CredentialSnapshot:
        return CredentialSnapshot(
            access_token=self._creds.token,
            project_id=self._resolved_project(),
            expiry_date=_expiry_to_epoch_ms(getattr(self._creds, "expiry", None)),
            native=self._creds,
        )

    def refresh_sync(self) -> CredentialSnapshot:
        self._creds.refresh(_GoogleRequest())
        return self._snapshot()

    async def refresh(self) -> CredentialSnapshot:
        await asyncio.to_thread(self._creds.refresh, _GoogleRequest())
        return self._snapshot()

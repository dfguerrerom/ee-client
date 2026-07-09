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
import httpx
import requests
from google.auth.transport.requests import Request as _GoogleRequest
from google.oauth2 import credentials as oauth_credentials
from google.oauth2 import service_account
from ee import oauth as ee_oauth

from eeclient.exceptions import (
    CredentialsFileNotFoundError,
    CredentialsResolutionError,
    EEClientError,
    SepalCredentialsUnavailableError,
)
from eeclient.helpers import should_verify_tls
from eeclient.models import GoogleTokens, SepalHeaders

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


def _google_auth_mode(creds) -> str:
    if isinstance(creds, service_account.Credentials):
        return "service_account"
    return "oauth"


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


class SepalFileProvider:
    """Reads a SEPAL-provisioned GoogleTokens JSON file (cannot self-refresh)."""

    def __init__(self, path):
        self.credentials_path = Path(path)
        self.auth_mode = "file"
        self.user = "local_user"
        self.verify_ssl = True

    def _read(self) -> CredentialSnapshot:
        if not self.credentials_path.exists():
            raise CredentialsFileNotFoundError(str(self.credentials_path))
        content = self.credentials_path.read_text().strip()
        if not content:
            raise CredentialsFileNotFoundError(str(self.credentials_path))
        try:
            data = json.loads(content)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in credentials file: {e}")
        tokens = GoogleTokens.model_validate(data)
        if not tokens.access_token:
            raise ValueError("No access token available in credentials file")
        return CredentialSnapshot(
            access_token=tokens.access_token,
            project_id=tokens.project_id,
            expiry_date=tokens.access_token_expiry_date,
            native=tokens,
        )

    def initial_snapshot(self) -> Optional[CredentialSnapshot]:
        return self._read()

    def refresh_sync(self) -> CredentialSnapshot:
        return self._read()

    async def refresh(self) -> CredentialSnapshot:
        return self._read()


class SepalSessionProvider:
    """Downloads GoogleTokens from the SEPAL session API (headers + cookie)."""

    def __init__(self, sepal_headers):
        self.max_retries = 3
        self.auth_mode = "sepal"
        self.sepal_host = os.getenv("SEPAL_HOST")
        if not self.sepal_host:
            raise ValueError("SEPAL_HOST environment variable not set")
        self.sepal_headers = SepalHeaders.model_validate(sepal_headers)
        self.sepal_session_id = self.sepal_headers.cookies["SEPAL-SESSIONID"]
        self.sepal_user_data = self.sepal_headers.sepal_user
        self.user = self.sepal_user_data.username
        self.sepal_api_download_url = (
            f"https://{self.sepal_host}/api/user-files/download/"
            "?path=%2F.config%2Fearthengine%2Fcredentials"
        )
        self.verify_ssl = should_verify_tls(self.sepal_host)
        self._google_tokens = self.sepal_user_data.google_tokens

    def _snapshot(self, tokens: GoogleTokens) -> CredentialSnapshot:
        return CredentialSnapshot(
            access_token=tokens.access_token,
            project_id=tokens.project_id,
            expiry_date=tokens.access_token_expiry_date,
            native=tokens,
        )

    def initial_snapshot(self) -> Optional[CredentialSnapshot]:
        if self._google_tokens:
            return self._snapshot(self._google_tokens)
        return None

    async def refresh(self) -> CredentialSnapshot:
        attempt = 0
        last_status = None
        cookies = httpx.Cookies()
        cookies.set("SEPAL-SESSIONID", self.sepal_session_id)
        while attempt < self.max_retries:
            attempt += 1
            try:
                async with httpx.AsyncClient(
                    cookies=cookies,
                    verify=self.verify_ssl,
                    limits=httpx.Limits(
                        max_connections=100, max_keepalive_connections=50
                    ),
                ) as client:
                    response = await client.get(self.sepal_api_download_url)
                last_status = response.status_code
                if response.status_code == 200:
                    return self._snapshot(GoogleTokens.model_validate(response.json()))
                elif response.status_code == 500:
                    raise SepalCredentialsUnavailableError(500)
            except Exception as e:
                log.error(
                    f"Attempt {attempt}/{self.max_retries} refreshing "
                    f"SEPAL credentials failed: {e}"
                )
            await asyncio.sleep(2**attempt)
        raise ValueError(
            f"Failed to retrieve credentials from SEPAL after "
            f"{self.max_retries} attempts, last status code: {last_status}"
        )

    def refresh_sync(self) -> CredentialSnapshot:
        attempt = 0
        last_status = None
        session = requests.Session()
        session.cookies.set("SEPAL-SESSIONID", self.sepal_session_id)
        session.verify = self.verify_ssl
        try:
            while attempt < self.max_retries:
                attempt += 1
                try:
                    response = session.get(self.sepal_api_download_url)
                    last_status = response.status_code
                    if response.status_code == 200:
                        return self._snapshot(
                            GoogleTokens.model_validate(response.json())
                        )
                    elif response.status_code == 500:
                        raise SepalCredentialsUnavailableError(500)
                except Exception as e:
                    log.error(
                        f"Attempt {attempt}/{self.max_retries} refreshing "
                        f"SEPAL credentials failed: {e}"
                    )
                time.sleep(2**attempt)
        finally:
            session.close()
        raise ValueError(
            f"Failed to retrieve credentials from SEPAL after "
            f"{self.max_retries} attempts, last status code: {last_status}"
        )


# ---------------------------------------------------------------------------
# Default resolution (home-gated, all-local, no implicit ADC — D10/D11)
# ---------------------------------------------------------------------------
def _is_sepal_context() -> bool:
    return "sepal-user" in Path.home().name


def resolve_default_provider() -> CredentialProvider:
    """Resolve a provider for a headerless ``EESession()`` — all-local, no ADC.

    In a SEPAL context (``sepal-user`` home) this is SEPAL-file-only and fails
    closed. Otherwise it walks local sources; ADC is never probed here (D10).
    """
    ee_dir = Path.home() / ".config" / "earthengine"

    if _is_sepal_context():
        sepal_file = ee_dir / "credentials"
        if sepal_file.exists():
            return SepalFileProvider(sepal_file)
        raise CredentialsResolutionError(
            f"SEPAL credentials not found at {sepal_file}. In a SEPAL context "
            "machine credentials are not used (fail closed)."
        )

    # Note: ``~/.config/earthengine/sepal_credentials`` is intentionally NOT a
    # source here — it is a hand-created, non-refreshable artifact, not a real
    # credential the library should auto-discover (issue #12 exists to replace it).
    raw = os.getenv("EARTHENGINE_TOKEN")
    if raw:
        creds, project = _credentials_from_earthengine_token(raw, DEFAULT_SCOPES)
        return GoogleAuthProvider(creds, project, auth_mode=_google_auth_mode(creds))

    ee_file = ee_dir / "credentials"
    if ee_file.exists():
        creds, project = _oauth_credentials_from_ee_file(DEFAULT_SCOPES)
        return GoogleAuthProvider(creds, project, auth_mode="oauth")

    raise CredentialsResolutionError(
        "No local credential source found (tried EARTHENGINE_TOKEN and the "
        f"Earth Engine OAuth file {ee_file}). ADC is not used implicitly — call "
        "EESession.from_application_default() to use Application Default "
        "Credentials."
    )

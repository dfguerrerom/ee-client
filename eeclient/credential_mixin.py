import logging
import time
from typing import Optional

from eeclient.exceptions import EEClientError
from eeclient.models import SepalHeaders
from eeclient.providers import CredentialSnapshot, SepalSessionProvider

log = logging.getLogger("eeclient")

# SEPAL-specific attributes forwarded from a provider onto the session for
# backward compatibility (present on SepalSessionProvider / SepalFileProvider,
# absent -> None for google-auth providers).
_FORWARDED = (
    "sepal_headers",
    "sepal_user_data",
    "sepal_host",
    "sepal_session_id",
    "sepal_api_download_url",
    "credentials_path",
)


class CredentialMixin:
    """Holds a :class:`CredentialProvider` and applies its snapshots.

    The provider (SEPAL session, SEPAL file, or google-auth) owns the actual
    credential logic; this mixin normalizes the result onto the session's
    ``access_token`` / ``project_id`` / ``expiry_date`` / ``_credentials``
    surface and delegates refresh.
    """

    def __init__(self, sepal_headers: Optional[SepalHeaders] = None, *, provider=None):
        self.max_retries = 3
        self._credentials = None
        self._service = None  # backward compatibility

        if provider is not None:
            self._provider = provider
        elif sepal_headers is not None:
            self._provider = SepalSessionProvider(sepal_headers)
        else:
            raise EEClientError(
                "EESession requires a credential source. Call "
                "EESession.from_default() to resolve credentials from the "
                "environment, or use an explicit EESession.from_*() factory."
            )

        prov = self._provider
        self.auth_mode = prov.auth_mode
        self.auth_source = getattr(prov, "auth_source", None)
        self.user = prov.user
        self.verify_ssl = getattr(prov, "verify_ssl", True)
        for name in _FORWARDED:
            setattr(self, name, getattr(prov, name, None))

        self.access_token = None
        self.project_id = None
        self.expiry_date = 0

        snap = prov.initial_snapshot()
        if snap is not None:
            self._apply_snapshot(snap, initial=True)

        self.logger = logging.getLogger(f"eeclient.{self.user}")

    def _apply_snapshot(
        self, snap: CredentialSnapshot, *, initial: bool = False
    ) -> None:
        self.access_token = snap.access_token
        self.expiry_date = snap.expiry_date
        self._credentials = snap.native
        # Preserve enforce_project_id: on refresh, don't overwrite an enforced
        # project; on initial population always set it.
        enforce = getattr(self, "enforce_project_id", False)
        if initial or not (enforce and self.project_id):
            self.project_id = snap.project_id

    def is_expired(self) -> bool:
        """Returns if a token is about to expire."""
        return (self.expiry_date / 1000) - time.time() < 60

    def needs_credentials_refresh(self) -> bool:
        """Returns if credentials need to be refreshed (missing or expired)."""
        return not self._credentials or self.is_expired()

    async def set_credentials(self) -> None:
        """Refresh credentials via the active provider (async)."""
        self._apply_snapshot(await self._provider.refresh())

    def set_credentials_sync(self) -> None:
        """Refresh credentials via the active provider (sync)."""
        self._apply_snapshot(self._provider.refresh_sync())


# Backward-compat alias: SEPAL is now one provider among several.
SepalCredentialMixin = CredentialMixin

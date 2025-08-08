from typing import Any, Dict, Literal, Optional

import os
import time
import asyncio
import httpx
import logging
from contextlib import asynccontextmanager

from eeclient.exceptions import EEClientError, EERestException
from eeclient.models import GEEHeaders, GoogleTokens, SepalHeaders
from eeclient.sepal_credential_mixin import SepalCredentialMixin

import eeclient.export as _export_module
import eeclient.data as _operations_module
import eeclient.tasks as _tasks_module

from eeclient.interfaces import (
    _ModuleProxy,
    ExportProtocol,
    OperationsProtocol,
    TasksProtocol,
    expose_module_methods,
)

logger = logging.getLogger("eeclient")

# Default values that won't raise exceptions during import
EARTH_ENGINE_API_URL = "https://earthengine.googleapis.com/v1alpha"

# These will be set properly when EESession is initialized
SEPAL_HOST = os.getenv("SEPAL_HOST")
SEPAL_API_DOWNLOAD_URL = None
VERIFY_SSL = True


class EESession(SepalCredentialMixin):
    def __init__(self, sepal_headers: SepalHeaders, enforce_project_id: bool = True):
        """Session that handles two scenarios to set the headers for Earth Engine API

        It can be initialized with the headers sent by SEPAL or with the
        credentials and project

        Args:
            sepal_headers (SepalHeaders): The headers sent by SEPAL
            enforce_project_id (bool, optional): If set, it cannot be changed.
                Defaults to True.

        Raises:
            ValueError: If SEPAL_HOST environment variable is not set
        """
        super().__init__(sepal_headers)

        self.max_retries = 3
        self._credentials = None
        self.enforce_project_id = enforce_project_id

        # Create user-specific logger
        self.logger = logging.getLogger(f"eeclient.{self.user}")

        # Initialize credentials from the initial tokens
        if self._google_tokens:
            self._credentials = self._google_tokens
        else:
            self._credentials = None
            self.project_id = None
            self.logger.debug(
                "No credentials found in initial headers. "
                "Call initialize() or use create() to fetch credentials."
            )

    async def initialize(self) -> "EESession":
        """Asynchronously initialize the session by fetching credentials if needed.

        This method can be called after creating a session to ensure it has
        valid credentials.

        Returns:
            EESession: The initialized session (self)
        """
        if not self._credentials:
            await self.set_credentials()
        return self

    @classmethod
    async def create(cls, sepal_headers: SepalHeaders, enforce_project_id: bool = True):
        """Asynchronously create an EESession instance.

        Args:
            sepal_headers (SepalHeaders): The headers sent by SEPAL
            enforce_project_id (bool, optional): If set, it cannot be changed.
                Defaults to True.

        Returns:
            EESession: An initialized session with valid credentials
        """
        session = cls(sepal_headers, enforce_project_id)
        return await session.initialize()

    async def get_assets_folder(self) -> str:
        if self.needs_credentials_refresh():
            await self.set_credentials()
        return f"projects/{self.project_id}/assets/"

    def is_expired(self) -> bool:
        """Returns if a token is about to expire"""
        return (self.expiry_date / 1000) - time.time() < 60

    def needs_credentials_refresh(self) -> bool:
        """Returns if credentials need to be refreshed (missing or expired)"""
        return not self._credentials or self.is_expired()

    def get_current_headers(self) -> GEEHeaders:
        """Get current headers without refreshing credentials"""
        if not self._credentials:
            raise EEClientError("No credentials available")

        self.logger.debug(f"Getting headers with project id: {self.project_id}")

        data = {
            "x-goog-user-project": self.project_id,
            "Authorization": f"Bearer {self._credentials.access_token}",
            "Username": self.sepal_user_data.username,
        }

        return GEEHeaders.model_validate(data)

    async def get_headers(self) -> GEEHeaders:
        """Async method to get headers, refreshing credentials if needed"""
        if self.needs_credentials_refresh():
            await self.set_credentials()
        return self.get_current_headers()

    @asynccontextmanager
    async def get_client(self):
        """Context manager for an HTTP client using the current headers.
        A new client is created each time to ensure fresh headers."""

        timeout = httpx.Timeout(connect=60.0, read=360.0, write=60.0, pool=60.0)
        headers = await self.get_headers()
        # Use the model_dump with by_alias=True to keep the original HTTP header names
        headers = headers.model_dump(by_alias=True)  # type: ignore
        limits = httpx.Limits(max_connections=100, max_keepalive_connections=50)
        async_client = httpx.AsyncClient(
            headers=headers, timeout=timeout, limits=limits
        )
        try:
            yield async_client
        finally:
            await async_client.aclose()

    async def set_credentials(self) -> None:
        """
        Refresh credentials asynchronously.
        Uses its own HTTP client (thus bypassing get_headers) to avoid recursion.
        """
        self.logger.debug(
            "Token is expired or about to expire; attempting to refresh credentials."
        )
        attempt = 0
        credentials_url = self.sepal_api_download_url

        # Prepare cookies for authentication.
        sepal_cookies = httpx.Cookies()
        sepal_cookies.set("SEPAL-SESSIONID", self.sepal_session_id)

        last_status = None

        while attempt < self.max_retries:
            attempt += 1
            try:
                async with httpx.AsyncClient(
                    cookies=sepal_cookies,
                    verify=self.verify_ssl,
                    limits=httpx.Limits(
                        max_connections=100, max_keepalive_connections=50
                    ),
                ) as client:
                    self.logger.debug(f"Attempt {attempt} to refresh credentials.")
                    response = await client.get(credentials_url)

                last_status = response.status_code

                if response.status_code == 200:
                    self._credentials = GoogleTokens.model_validate(response.json())
                    self.expiry_date = self._credentials.access_token_expiry_date
                    self.project_id = (
                        self._credentials.project_id
                        if self.enforce_project_id
                        else self.project_id
                    )
                    self.logger.debug(
                        f"Successfully refreshed credentials "
                        f"!{self._credentials}==================. {self.project_id}"
                    )
                    return
                else:
                    self.logger.debug(
                        f"Attempt {attempt}/{self.max_retries} failed with "
                        f"status code: {response.status_code}."
                    )
            except Exception as e:
                self.logger.error(
                    f"Attempt {attempt}/{self.max_retries} "
                    f"encountered an exception: {e}"
                )
            await asyncio.sleep(2**attempt)  # Exponential backoff

        raise ValueError(
            f"Failed to retrieve credentials after {self.max_retries} attempts, "
            f"last status code: {last_status}"
        )

    async def rest_call(
        self,
        method: Literal["GET", "POST", "DELETE"],
        url: str,
        data: Optional[Dict] = None,
        params: Optional[Dict] = None,
        max_attempts: int = 4,
        initial_wait: float = 1,
        max_wait: float = 60,
    ) -> Dict[str, Any]:
        """Async REST call with retry logic"""

        attempt = 0
        last_error = None

        while attempt < max_attempts:
            try:
                async with self.get_client() as client:
                    url_with_project = self.set_url_project(url)

                    if "assets" not in url_with_project:
                        # Do not log assets requests
                        self.logger.debug(
                            f"Making async  {method} request to {url_with_project}"
                        )
                    response = await client.request(
                        method, url_with_project, json=data, params=params
                    )

                    if response.status_code >= 400:
                        if "application/json" in response.headers.get(
                            "Content-Type", ""
                        ):
                            error_data = response.json().get("error", {})
                            self.logger.error(
                                f"Request failed with error: {error_data}"
                            )
                            raise EERestException(error_data)
                        else:
                            error_data = {
                                "code": response.status_code,
                                "message": response.reason_phrase
                                or "Unknown HTTP error",
                                "status": response.status_code,
                            }
                            self.logger.error(
                                f"Request failed with HTTP error: {error_data}"
                            )
                            raise EERestException(error_data)

                    try:
                        return response.json()
                    except Exception as e:
                        self.logger.error(f"Error parsing JSON response: {str(e)}")
                        self.logger.debug(f"Response content: {response.text[:500]}...")
                        raise EERestException(
                            {
                                "code": 500,
                                "message": f"Invalid JSON response: {str(e)}",
                                "status": response.status_code,
                            }
                        )

            except EERestException as e:
                last_error = e
                if e.code in [429, 401, 503, 502, 504]:
                    # Retry for rate limits, auth issues, and service unavailability
                    error_type = (
                        "Rate limit exceeded"
                        if e.code == 429
                        else "Unauthorized"
                        if e.code == 401
                        else "Service unavailable"
                    )
                    attempt += 1
                    wait_time = min(initial_wait * (2**attempt), max_wait)

                    self.logger.debug(
                        f"{error_type}. Attempt {attempt}/{max_attempts}. "
                        f"Waiting {wait_time} seconds..."
                    )

                    if e.code == 401:
                        await self.set_credentials()

                    await asyncio.sleep(wait_time)
                    continue
                else:
                    self.logger.error(f"EERestException: {e}")
                    raise

            except httpx.HTTPError as e:
                # Explicitly handle network-related errors
                last_error = e
                attempt += 1
                wait_time = min(initial_wait * (2**attempt), max_wait)

                error_type = type(e).__name__
                self.logger.error(
                    f"Network error ({error_type}) on attempt "
                    f"{attempt}/{max_attempts}: {str(e)}"
                )

                if isinstance(
                    e,
                    (
                        httpx.ConnectTimeout,
                        httpx.ReadTimeout,
                        httpx.ConnectError,
                        httpx.ReadError,
                        httpx.WriteError,
                        httpx.PoolTimeout,
                    ),
                ):
                    self.logger.info(
                        f"Transient network error, retrying in {wait_time} seconds"
                    )
                else:
                    self.logger.warning(f"Non-transient network error: {str(e)}")

                if attempt >= max_attempts:
                    self.logger.error(
                        f"Max retry attempts reached for network error: {str(e)}"
                    )
                    raise

                await asyncio.sleep(wait_time)

            except Exception as e:
                last_error = e
                error_type = type(e).__name__
                self.logger.error(
                    f"Unexpected error in rest_call ({error_type}): " f"{str(e)}"
                )

                import traceback

                self.logger.debug(f"Traceback: {traceback.format_exc()}")

                # Only retry for potentially recoverable errors
                if error_type in ["JSONDecodeError", "TimeoutError", "ConnectionError"]:
                    attempt += 1
                    wait_time = min(initial_wait * (2**attempt), max_wait)
                    if attempt < max_attempts:
                        self.logger.info(
                            f"Potentially recoverable error, retrying in "
                            f"{wait_time} seconds"
                        )
                        await asyncio.sleep(wait_time)
                        continue

                raise

        # If we've reached here, we've exhausted all retries
        if last_error is None:
            # This should not happen normally but handle it defensively
            raise EERestException(
                {
                    "code": 500,
                    "message": "Max retry attempts reached with unknown error",
                }
            )
        elif isinstance(last_error, EERestException):
            if last_error.code in [429, 401]:
                raise EERestException(
                    {
                        "code": last_error.code,
                        "message": f"Max retry attempts reached: {last_error.message}",
                    }
                )
            else:
                raise last_error
        else:
            raise EERestException(
                {
                    "code": 500,
                    "message": f"Max retry attempts reached: {str(last_error)}",
                }
            )

    def set_url_project(self, url: str) -> str:
        """Set the API URL with the project id"""

        return url.format(
            earth_engine_api_url=EARTH_ENGINE_API_URL, project=self.project_id
        )

    @property
    def export(self) -> ExportProtocol:
        return _ModuleProxy(self, _export_module)  # type: ignore

    @property
    def operations(self) -> OperationsProtocol:
        return _ModuleProxy(self, _operations_module)  # type: ignore

    @property
    def tasks(self) -> TasksProtocol:
        return _ModuleProxy(self, _tasks_module)  # type: ignore


expose_module_methods(_ModuleProxy, _export_module)
expose_module_methods(_ModuleProxy, _operations_module)
expose_module_methods(_ModuleProxy, _tasks_module)

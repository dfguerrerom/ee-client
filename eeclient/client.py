from typing import Any, Dict, Literal, Optional

import os
import asyncio
import httpx
import logging
from contextlib import asynccontextmanager

from eeclient.exceptions import EEClientError, EERestException
from eeclient.models import GEEHeaders, SepalHeaders
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


class SimpleRateLimiter:
    def __init__(self, qps: float | None):
        self.qps = qps
        self._lock = asyncio.Lock()
        self._next = 0.0

    async def acquire(self):
        if not self.qps or self.qps <= 0:
            return
        async with self._lock:
            now = asyncio.get_running_loop().time()
            wait = max(0.0, self._next - now)
            if wait:
                await asyncio.sleep(wait)
            self._next = max(now, self._next) + 1.0 / self.qps


class EESession(SepalCredentialMixin):
    def __init__(
        self,
        sepal_headers: Optional[SepalHeaders] = None,
        enforce_project_id: bool = True,
    ):
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
        self._inflight = asyncio.BoundedSemaphore(30)
        self._rate = SimpleRateLimiter(60)

        self._auth_refresh_lock = asyncio.Lock()

        self._client: httpx.AsyncClient | None = None
        self._client_lock = asyncio.Lock()

        self.enforce_project_id = enforce_project_id
        super().__init__(sepal_headers)

        self.logger = logging.getLogger(f"eeclient.{self.user}")
        self.logger.debug(
            "EESession initialized"
            if self._credentials
            else (
                "EESession created without credentials. "
                "Call initialize() or use create() to fetch credentials."
            )
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
    async def create(
        cls,
        sepal_headers: Optional[SepalHeaders] = None,
        enforce_project_id: bool = True,
    ):
        """Asynchronously create an EESession instance.

        Args:
            sepal_headers (Optional[SepalHeaders]): The headers sent by SEPAL.
                If None, will use file-based authentication.
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

    def get_current_headers(self) -> GEEHeaders:
        """Get current headers without refreshing credentials"""
        if not self._credentials:
            raise EEClientError("No credentials available")

        self.logger.debug(f"Getting headers with project id: {self.project_id}")

        # Get username based on authentication mode
        username = (
            self.sepal_user_data.username
            if hasattr(self, "sepal_user_data") and self.sepal_user_data
            else self.user
        )

        data = {
            "x-goog-user-project": self.project_id,
            "Authorization": f"Bearer {self._credentials.access_token}",
            "Username": username,
        }

        return GEEHeaders.model_validate(data)

    async def get_headers(self):
        # Only one task refreshes the token; others wait briefly.
        if self.needs_credentials_refresh():
            async with self._auth_refresh_lock:
                if self.needs_credentials_refresh():  # double-check after lock
                    await self.set_credentials()
        return self.get_current_headers()

    async def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None:
            async with self._client_lock:
                if self._client is None:
                    self._client = httpx.AsyncClient(
                        http2=True,
                        timeout=httpx.Timeout(connect=60, read=360, write=60, pool=60),
                        limits=httpx.Limits(
                            max_connections=40, max_keepalive_connections=20
                        ),
                        verify=getattr(self, "verify_ssl", True),
                    )
        return self._client

    @asynccontextmanager
    async def get_client(self):
        client = await self._ensure_client()
        yield client

    async def aclose(self):
        if self._client is not None:
            await self._client.aclose()
            self._client = None

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
                headers = (await self.get_headers()).model_dump(by_alias=True)
                url_with_project = self.set_url_project(url)

                async with self._inflight:
                    await self._rate.acquire()
                    async with self.get_client() as client:

                        if "assets" not in url_with_project:
                            # Do not log assets requests
                            self.logger.debug(
                                f"Making async  {method} request to {url_with_project}"
                            )
                        response = await client.request(
                            method,
                            url_with_project,
                            json=data,
                            params=params,
                            headers=headers,
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
                            self.logger.debug(
                                f"Response content: {response.text[:500]}..."
                            )
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
                    f"Unexpected error in rest_call ({error_type}): {str(e)}"
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

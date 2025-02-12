import asyncio
from contextlib import asynccontextmanager
from functools import wraps
import os
import time
from typing import Any, Dict, Literal, Optional
import json
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_result


from eeclient.logger import logger
from eeclient.exceptions import EEClientError, EERestException
from eeclient.typing import GEEHeaders, SepalHeaders
from eeclient.data import get_info, get_map_id, get_asset

SEPAL_HOST = os.getenv("SEPAL_HOST")
if not SEPAL_HOST:
    raise ValueError("SEPAL_HOST environment variable not set")
EARTH_ENGINE_API_URL = "https://earthengine.googleapis.com/v1alpha"
SEPAL_API_DOWNLOAD_URL = f"https://{SEPAL_HOST}/api/user-files/download/?path=%2F.config%2Fearthengine%2Fcredentials"
VERIFY_SSL = (
    not SEPAL_HOST == "host.docker.internal" or not SEPAL_HOST == "danielg.sepal.io"
)
VERIFY_SSL = False


def parse_cookie_string(cookie_string):
    cookies = {}
    for pair in cookie_string.split(";"):
        key_value = pair.strip().split("=", 1)
        if len(key_value) == 2:
            key, value = key_value
            cookies[key] = value
    return cookies


def should_retry(exception: Exception) -> bool:
    """Check if the exception is due to rate limiting"""
    if isinstance(exception, EERestException):
        return exception.code == 429
    return False


def sync_wrapper(async_func):
    """Decorator to run async functions synchronously when needed"""

    @wraps(async_func)
    def wrapper(*args, **kwargs):
        return asyncio.run(async_func(*args, **kwargs))

    return wrapper


class AsyncEESession:
    def __init__(self, sepal_headers: SepalHeaders):
        """Session that handles two scenarios to set the headers for the Earth Engine API

        It can be initialized with the headers sent by SEPAL or with the credentials and project

        """
        self.expiry_date = 0
        self.retry_count = 0
        self.max_retries = 3

        self.sepal_headers = sepal_headers
        self.sepal_cookies = parse_cookie_string(sepal_headers["cookie"][0])
        self.sepal_user_data = json.loads(sepal_headers["sepal-user"][0])  # type: ignore
        self.sepal_username = self.sepal_user_data["username"]

        if not self.sepal_user_data["googleTokens"]:
            raise EEClientError(
                "Authentication required: Please authenticate via sepal. See https://docs.sepal.io/en/latest/setup/gee.html."
            )

        self.project_id = self.sepal_user_data["googleTokens"]["projectId"]
        self._async_client = None

        # Initialize credentials from the initial tokens
        self._initialize_credentials()

    def _initialize_credentials(self):
        """Initialize credentials from the initial Google tokens"""
        _google_tokens = self.sepal_user_data.get("googleTokens")
        if not _google_tokens:
            raise EEClientError(
                "Authentication required: Please authenticate via sepal."
            )
        self.expiry_date = _google_tokens["accessTokenExpiryDate"]
        self._credentials = {
            "access_token": _google_tokens["accessToken"],
            "access_token_expiry_date": _google_tokens["accessTokenExpiryDate"],
            "project_id": _google_tokens["projectId"],
            "sepal_user": self.sepal_username,
        }

    def is_expired(self) -> bool:
        """Returns if a token is about to expire"""
        expired = self.expiry_date / 1000 - time.time() < 60
        self.retry_count += 1 if expired else 0
        return expired

    def get_current_headers(self) -> GEEHeaders:
        """Get current headers without refreshing credentials"""
        if not self._credentials:
            raise EEClientError("No credentials available")

        access_token = self._credentials["access_token"]
        return {
            "x-goog-user-project": self.project_id,
            "Authorization": f"Bearer {access_token}",
            "Username": self.sepal_username,
        }

    async def get_headers(self) -> GEEHeaders:
        """Async method to get headers, refreshing credentials if needed"""
        if self.is_expired():
            await self.set_credentials()
        return self.get_current_headers()

    @asynccontextmanager
    async def get_client(self):
        """Context manager for async client with proper session handling"""
        if self._async_client is None:
            timeout = httpx.Timeout(connect=60.0, read=300.0, write=60.0, pool=60.0)
            headers = await self.get_headers()
            self._async_client = httpx.AsyncClient(
                headers=headers, timeout=timeout, verify=VERIFY_SSL
            )
        try:
            yield self._async_client
        finally:
            if self._async_client is not None:
                await self._async_client.aclose()
                self._async_client = None

    async def set_credentials(self) -> None:
        """Async credential refresh"""
        logger.debug(
            "Token is expired or about to expire; attempting to refresh credentials."
        )
        self.retry_count = 0
        credentials_url = SEPAL_API_DOWNLOAD_URL

        sepal_cookies = httpx.Cookies()
        sepal_cookies.set("SEPAL-SESSIONID", self.sepal_cookies["SEPAL-SESSIONID"])

        last_status = None

        while self.retry_count < self.max_retries:
            async with self.get_client() as client:
                # Update client headers with cookie
                client.cookies.update(sepal_cookies)
                response = await client.get(credentials_url)

            last_status = response.status_code

            if response.status_code == 200:
                self._credentials = response.json()
                self.expiry_date = self._credentials["access_token_expiry_date"]
                # Update client headers with new credentials
                if self._async_client:
                    self._async_client.headers.update(self.get_current_headers())
                logger.debug("Successfully refreshed credentials.")
                break
            else:
                self.retry_count += 1
                logger.debug(
                    f"Retry {self.retry_count}/{self.max_retries} failed "
                    f"with status code: {response.status_code}."
                )
        else:
            raise ValueError(
                f"Failed to retrieve credentials after {self.max_retries} retries, "
                f"last status code: {last_status}"
            )

    async def rest_call(
        self,
        method: Literal["GET", "POST"],
        url: str,
        data: Optional[Dict] = None,
        max_attempts: int = 5,
        initial_wait: float = 1,
        max_wait: float = 60,
    ) -> Dict[str, Any]:
        """Async REST call with retry logic"""

        async def _make_request():
            try:
                url_with_project = self.set_url_project(url)
                logger.debug(f"Making async {method} request to {url_with_project}")

                # Use the managed client
                async with self.get_client() as client:
                    response = await client.request(method, url_with_project, json=data)

                    if response.status_code >= 400:
                        if "application/json" in response.headers.get(
                            "Content-Type", ""
                        ):
                            error_data = response.json().get("error", {})
                            logger.debug(f"Request failed with error: {error_data}")
                            raise EERestException(error_data)
                        else:
                            error_data = {
                                "code": response.status_code,
                                "message": response.reason_phrase,
                            }
                            logger.debug(f"Request failed with error: {error_data}")
                            raise EERestException(error_data)

                    return response.json()

            except EERestException as e:
                return e

        attempt = 0
        while attempt < max_attempts:
            result = await _make_request()
            if isinstance(result, EERestException):
                if result.code == 429:  # Rate limit exceeded
                    attempt += 1
                    wait_time = min(initial_wait * (2**attempt), max_wait)
                    logger.debug(
                        f"Rate limit exceeded. Attempt {attempt}/{max_attempts}. "
                        f"Waiting {wait_time} seconds..."
                    )
                    await asyncio.sleep(wait_time)
                else:
                    raise result
            else:
                return result

        raise EERestException({"code": 429, "message": "Max retry attempts reached"})

    @property
    def operations(self):
        # Return an object that bundles operations, passing self as the session.
        return _Operations(self)


class _Operations:
    def __init__(self, session):
        self._session = session

    async def get_assets(
        self, parent: str = "projects/earthengine-public/assets"
    ) -> Dict:
        """Async - List assets in a folder/collection"""
        url = f"{{EARTH_ENGINE_API_URL}}/{parent}:listAssets"
        return await self._session.rest_call("GET", url)

    async def get_asset(self, asset_id: str) -> Dict:
        """Async - Get asset metadata"""
        url = f"{{EARTH_ENGINE_API_URL}}/projects/{{project}}/assets/{asset_id}"
        return await self._session.rest_call("GET", url)

    # Sync wrappers
    get_assets_sync = sync_wrapper(get_assets)
    get_asset_sync = sync_wrapper(get_asset)

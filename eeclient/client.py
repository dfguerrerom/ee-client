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


class EESession:
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

    @property
    def headers(self) -> Optional[GEEHeaders]:
        return self.get_session_headers()

    def is_expired(self) -> bool:
        """Returns if a token is about to expire"""

        # The expiration date is in milliseconds
        expired = self.expiry_date / 1000 - time.time() < 60
        self.retry_count += 1 if expired else 0

        return expired

    def get_session_headers(self) -> GEEHeaders:
        """Get EE session headers"""

        self.set_gee_credentials()

        access_token = self._credentials["access_token"]

        return {
            "x-goog-user-project": self.project_id,
            "Authorization": f"Bearer {access_token}",
            "Username": self.sepal_username,
        }

    def set_gee_credentials(self) -> None:
        """Get the credentials from SEPAL session"""

        if not hasattr(self, "_credentials"):
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

        if self.is_expired():
            logger.debug(
                "Token is expired or about to expire; attempting to refresh credentials."
            )
            self.retry_count = 0
            credentials_url = SEPAL_API_DOWNLOAD_URL

            sepal_cookies = httpx.Cookies()
            sepal_cookies.set("SEPAL-SESSIONID", self.sepal_cookies["SEPAL-SESSIONID"])

            last_status = None

            while self.retry_count < self.max_retries:
                with httpx.Client(cookies=sepal_cookies, verify=VERIFY_SSL) as client:
                    response = client.get(credentials_url)
                last_status = response.status_code

                if response.status_code == 200:
                    self._credentials = response.json()
                    self.expiry_date = self._credentials["access_token_expiry_date"]
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

    def rest_call(
        self,
        method: Literal["GET", "POST"],
        url: str,
        data: Optional[Dict] = None,
        max_attempts: int = 5,
        initial_wait: float = 1,
        max_wait: float = 60,
    ) -> Dict[str, Any]:
        """Make a call to the Earth Engine REST API with retry logic"""

        timeout = httpx.Timeout(connect=60.0, read=300.0, write=60.0, pool=60.0)

        @retry(
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(multiplier=initial_wait, max=max_wait),
            retry=retry_if_result(should_retry),
            before_sleep=lambda retry_state: logger.debug(
                f"Rate limit exceeded. Attempt {retry_state.attempt_number}/{max_attempts}. "
                f"Waiting {retry_state.next_action.sleep if retry_state.next_action else 'unknown'} seconds..."
            ),
        )
        def _make_request():
            try:
                url_with_project = self.set_url_project(url)
                logger.debug(f"Making a {method} request to {url_with_project}")
                logger.debug(f"HEADERS: {self.headers}")

                with httpx.Client(headers=self.headers, timeout=timeout) as client:  # type: ignore
                    response = client.request(method, url_with_project, json=data)

                if response.status_code >= 400:
                    if "application/json" in response.headers.get("Content-Type", ""):
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
                return e  # Return the exception for retry evaluation

        result = _make_request()
        if isinstance(result, Exception):
            raise result
        return result

    def set_url_project(self, url: str) -> str:
        """Set the API URL with the project id"""

        return url.format(
            EARTH_ENGINE_API_URL=EARTH_ENGINE_API_URL, project=self.project_id
        )

    @property
    def operations(self):
        # Return an object that bundles operations, passing self as the session.
        return _Operations(self)


class _Operations:
    def __init__(self, session):
        self._session = session

    def get_info(self, ee_object=None, workloadTag=None, serialized_object=None):
        return get_info(
            self._session,
            ee_object,
            workloadTag,
            serialized_object,
        )

    def get_map_id(self, ee_image, vis_params={}, bands=None, format=None):
        return get_map_id(self._session, ee_image, vis_params, bands, format)

    def get_asset(self, ee_asset_id):
        return get_asset(self._session, ee_asset_id)

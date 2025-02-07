import os
import time
from typing import Any, Dict, Literal, Optional
import json
import httpx
from eeclient.exceptions import EERestException
from eeclient.typing import GEEHeaders, SepalHeaders
from eeclient.data import get_info, get_map_id, get_asset

EARTH_ENGINE_API_URL = "https://earthengine.googleapis.com/v1alpha/"
SEPAL_HOST = os.getenv("SEPAL_HOST")
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


class EESession:
    def __init__(self, sepal_headers: SepalHeaders):
        """Session that handles two scenarios to set the headers for the Earth Engine API

        It can be initialized with the headers sent by SEPAL or with the credentials and project

        """
        self.expiry_date = None
        self.tries = 0

        self.retry_count = 0
        self.max_retries = 3

        self.sepal_headers = sepal_headers
        self.sepal_cookies = parse_cookie_string(sepal_headers["cookie"][0])

        self.sepal_user_data = json.loads(sepal_headers["sepal-user"][0])  # type: ignore

        self.sepal_username = self.sepal_user_data["username"]
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

        if self.tries == 0:
            # This happens with the first request
            _google_tokens = self.sepal_user_data["googleTokens"]
            self.expiry_date = _google_tokens["accessTokenExpiryDate"]
            self.tries += 1

            if not self.is_expired():
                self._credentials = {
                    "access_token": _google_tokens["accessToken"],
                    "access_token_expiry_date": _google_tokens["accessTokenExpiryDate"],
                    "project_id": _google_tokens["projectId"],
                    "sepal_user": self.sepal_username,
                }

        if self.is_expired():
            if self.retry_count < self.max_retries:

                credentials_url = SEPAL_API_DOWNLOAD_URL

                sepal_cookies = httpx.Cookies()
                sepal_cookies.set(
                    "SEPAL-SESSIONID", self.sepal_cookies["SEPAL-SESSIONID"]
                )

                with httpx.Client(cookies=sepal_cookies, verify=VERIFY_SSL) as client:

                    response = client.get(credentials_url)
                    if response.status_code == 200 and response:
                        self.retry_count = 0
                        self._credentials = response.json()
                        self.expiry_date = self._credentials["access_token_expiry_date"]
                    else:
                        self.retry_count += 1
                        raise ValueError(
                            f"Failed to retrieve credentials, status code: {response.status_code}"
                        )

    def rest_call(
        self,
        method: Literal["GET", "POST"],
        url: str,
        data: Optional[Dict] = None,  # type: ignore
    ) -> Dict[str, Any]:
        """Make a call to the Earth Engine REST API"""

        url = self.set_url_project(url)

        with httpx.Client(headers=self.headers) as client:  # type: ignore
            response = client.request(method, url, json=data)

        if response.status_code >= 400:
            if "application/json" in response.headers.get("Content-Type", ""):
                raise EERestException(response.json().get("error", {}))
            else:
                raise EERestException(
                    {
                        "code": response.status_code,
                        "message": response.reason_phrase,
                    }
                )

        return response.json()

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

    def get_info(self, ee_object, workloadTag=None):
        return get_info(self._session, ee_object, workloadTag)

    def get_map_id(self, ee_image, vis_params=None, bands=None, format=None):
        return get_map_id(self._session, ee_image, vis_params, bands, format)

    def get_asset(self, ee_asset_id):
        return get_asset(self._session, ee_asset_id)

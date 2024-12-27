import os
from pathlib import Path
import time
from typing import Any, Dict, Literal, Optional
from urllib.parse import urljoin
import json
import httpx
from eeclient.exceptions import EERestException
from eeclient.typing import (
    GEEHeaders,
    GoogleTokens,
    SepalHeaders,
    GEECredentials,
)

EARTH_ENGINE_API_URL = "https://earthengine.googleapis.com/v1alpha/"
SEPAL_HOST = os.getenv("SEPAL_HOST")
SEPAL_API_DOWNLOAD_URL = f"https://{SEPAL_HOST}/api/user-files/download/?path=%2F.config%2Fearthengine%2Fcredentials"
VERIFY_SSL = not SEPAL_HOST == "danielg.sepal.io"


def parse_cookie_string(cookie_string):
    """
    Parse a cookie string into a dictionary.

    Args:
        cookie_string (str): The cookie string to parse.

    Returns:
        dict: A dictionary with cookie names as keys and cookie values as values.
    """
    return dict(pair.split("=", 1) for pair in cookie_string.split("; "))


class EESession:
    def __init__(self, sepal_headers: SepalHeaders):
        """Session that handles two scenarios to set the headers for the Earth Engine API

        It can be initialized with the headers sent by SEPAL or with the credentials and project

        Args:
            ee_project (str): The project id of the Earth Engine project
            credentials (Union[PathLike, dict]): The credentials to use for the session

        """
        self.tries = 0

        self.retry_count = 0
        self.max_retries = 3

        self.sepal_headers = sepal_headers
        self.sepal_cookies = parse_cookie_string(sepal_headers["cookie"][0])

        self.sepal_user = json.loads(sepal_headers["sepal-user"][0])  # type: ignore

        self.sepal_username = self.sepal_user["username"]
        self.project_id = self.sepal_user["googleTokens"]["projectId"]

    @property
    def headers(self) -> Optional[GEEHeaders]:
        return self.get_session_headers()

    def is_expired(self, expiry_date: int) -> bool:
        """Returns if a token is about to expire"""

        expired = expiry_date - time.time() < 60
        self.retry_count += 1 if expired else 0

        return expired

    def get_session_headers(self) -> GEEHeaders:
        """Get EE session headers"""

        credentials = self.get_gee_credentials()

        access_token = credentials["access_token"]
        expiry_date = credentials["access_token_expiry_date"]

        if self.is_expired(expiry_date):
            self.retry_count += 1
            if self.retry_count < self.max_retries:
                return self.get_session_headers()

        return {
            "x-goog-user-project": self.project_id,
            "Authorization": f"Bearer {access_token}",
            "Username": self.sepal_username,
        }

    def get_gee_credentials(self) -> GEECredentials:
        """Get the credentials from SEPAL session"""

        if self.tries == 0:
            # This happens with the first request
            _google_tokens = self.sepal_user["googleTokens"]
            expiry_date = _google_tokens["accessTokenExpiryDate"]

            if not self.is_expired(expiry_date):
                self.tries += 1

                return {
                    "access_token": _google_tokens["accessToken"],
                    "access_token_expiry_date": _google_tokens["accessTokenExpiryDate"],
                    "project_id": _google_tokens["projectId"],
                    "sepal_user": self.sepal_username,
                }

        credentials_url = SEPAL_API_DOWNLOAD_URL

        sepal_cookies = httpx.Cookies()
        sepal_cookies.set("JSESSIONID", self.sepal_cookies["JSESSIONID"])
        sepal_cookies.set("SEPAL-SESSIONID", self.sepal_cookies["SEPAL-SESSIONID"])

        with httpx.Client(cookies=sepal_cookies, verify=VERIFY_SSL) as client:
            response = client.get(credentials_url)
            if response.status_code == 200 and response.content:
                credentials = response.json()
            else:
                raise ValueError(
                    f"Failed to retrieve credentials, status code: {response.status_code}, content: {response.content}"
                )
            return credentials

    def rest_call(
        self,
        method: Literal["GET", "POST"],
        url: str,
        data: Optional[Dict] = None,  # type: ignore
    ) -> Dict[str, Any]:
        """Make a call to the Earth Engine REST API"""

        url = self.set_url_project(url)

        if self.headers:

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

        return {}

    def set_url_project(self, url: str) -> str:
        """Set the API URL with the project id"""

        return url.format(
            EARTH_ENGINE_API_URL=EARTH_ENGINE_API_URL, project=self.project_id
        )

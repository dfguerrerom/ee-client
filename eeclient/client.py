import time
from typing import Any, Dict, Literal, Optional
from urllib.parse import urljoin

import httpx
from eeclient.exceptions import EERestException
from eeclient.typing import (
    GEEHeaders,
    GoogleTokens,
    SepalHeaders,
    GEECredentials,
)


EARTH_ENGINE_API_URL = "https://earthengine.googleapis.com/v1alpha/"
SEPAL_HOST = "https://sepal.io/api/user-files/download"
CREDENTIALS_FILE_PATH = "%2F.config%2Fearthengine%2Fcredentials"


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
        self.sepal_cookies = sepal_headers["cookies"]
        self.sepal_user = sepal_headers["username"]
        self.project_id = sepal_headers["googleTokens"]["projectId"]

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
            "Username": self.sepal_user,
        }

    def get_gee_credentials(self) -> GEECredentials:
        """Get the credentials from SEPAL session"""

        if self.tries == 0:
            # This happens with the first request
            google_tokens: GoogleTokens = self.sepal_headers["googleTokens"]
            expiry_date = google_tokens["accessTokenExpiryDate"]

            if not self.is_expired(expiry_date):
                self.tries += 1
                return {
                    "access_token": google_tokens["accessToken"],
                    "access_token_expiry_date": google_tokens["accessTokenExpiryDate"],
                    "project_id": google_tokens["projectId"],
                    "sepal_user": self.sepal_user,
                }

        credentials_url = urljoin(SEPAL_HOST, f"?path={CREDENTIALS_FILE_PATH}")

        sepal_cookies = httpx.Cookies()
        sepal_cookies.set("JSESSIONID", self.sepal_cookies["JSESSIONID"])
        sepal_cookies.set("SEPAL-SESSIONID", self.sepal_cookies["SEPAL-SESSIONID"])

        with httpx.Client(cookies=sepal_cookies) as client:
            response = client.get(credentials_url)
            credentials = response.json()
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

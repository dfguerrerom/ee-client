import json
from pathlib import Path
import time
from typing import Literal, Union
import httpx
from ee.oauth import CLIENT_ID, CLIENT_SECRET, SCOPES, get_credentials_path
from eeclient.aioclient import PathLike
from eeclient.exceptions import EERestException
from eeclient.typing import GoogleTokens, SepalHeaders

HEADERS = json.loads((Path(__file__).parent / "config.json").read_text())


class Session:
    def __init__(
        self,
        ee_project: str,
        credentials: Union[PathLike, dict],
        sepal_headers: Union[SepalHeaders, None] = None,
    ):
        """Session that handles two scenarios to set the headers for the Earth Engine API

        It can be initialized with the headers sent by SEPAL or with the credentials and project

        Args:
            ee_project (str): The project id of the Earth Engine project
            credentials (Union[PathLike, dict]): The credentials to use for the session
            sepal_headers (Union[SepalHeaders, None], optional): The headers sent by SEPAL. Defaults to None.
        """
        self.headers = None
        self.project = ee_project
        self.credentials = None

        # If initializing from credentials and project, load credentials
        if not sepal_headers:
            if not ee_project or not credentials:
                raise ValueError(
                    "Both ee_project and credentials must be provided when from_headers is False."
                )

            if isinstance(credentials, (str, Path)) or not credentials:
                credentials_path = credentials or get_credentials_path()
                credentials = json.loads(Path(credentials_path).read_text())

            self.credentials = credentials
            self.headers = {
                "x-goog-user-project": ee_project,
                "Authorization": f"Bearer {self._get_access_token()}",
            }

        else:
            self.sepal_headers = sepal_headers
            self.headers = self.get_headers()

    @headers.setter
    def headers(self, headers):
        self.headers = headers

    @headers.getter
    def headers(self):

        if self.from_sepal:
            return self.get_headers()

        return self.headers

    def _set_url_project(self, url):
        """Set the project in the url"""

        return url.format(project=self.project)

    @property
    def is_expired(expiry_date: int) -> bool:
        """Returns if a token is about to expire"""

        return expiry_date - time.time() < 60

    def get_headers(self):
        """Set the headers from SEPAL"""

        if self.sepal_headers:

            google_tokens: GoogleTokens = self.sepal_headers.get("googleTokens")

            username = google_tokens.get("username")
            expiry_date = google_tokens.get("accessTokenExpiryDate")
            project_id = google_tokens.get("projectId")

            if not self.is_expired(expiry_date):

                access_token = google_tokens.get("accessToken")

            else:
                self.sepal_headers = self.refresh_sepal_google_tokens(username)
                return self.get_headers()

            return {
                "x-goog-user-project": project_id,
                "Authorization": f"Bearer {access_token}",
            }

        return self.headers

    def get_sepal_google_tokens(self, username: str) -> GoogleTokens:
        """"""

        credentials_endpoint = "/api/users/{username}/credentials"
        url = f"https://sepal.io{credentials_endpoint}"

        with httpx.Client(headers=self.sepal_headers) as client:

            response = client.get(url)

        return response.json()

    def rest_call(
        self,
        method: Literal["GET", "POST"],
        url: str,
        data: dict = None,
    ):
        """Make a call to the Earth Engine REST API"""

        url = self._set_url_project(url)

        with httpx.Client(headers=self.headers) as client:

            response = (
                client.post(url, data=data) if method == "POST" else client.get(url)
            )

        if response.status_code >= 400:
            if "application/json" in response.headers.get("Content-Type", ""):
                raise EERestException(response.json().get("error", {}))
            else:
                raise EERestException(
                    {"code": response.status_code, "message": response.reason}
                )

        return response.json()

    def _get_access_token(self):
        """Get the access token from the refresh token using the credentials file"""

        url = "https://oauth2.googleapis.com/token"

        with httpx.Client() as client:

            response = client.post(
                url,
                data={
                    "client_id": self.credentials["client_id"],
                    "client_secret": self.credentials["client_secret"],
                    "refresh_token": self.credentials["refresh_token"],
                    "grant_type": "refresh_token",
                },
            )

        return response.json().get("access_token")

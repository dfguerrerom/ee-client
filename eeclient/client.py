import json
from pathlib import Path
import time
from typing import Literal, Optional, Union

import httpx
from eeclient.exceptions import EERestException
from eeclient.typing import Credentials, GEEHeaders, GoogleTokens, SepalHeaders


def read_credentials(credentials_path: Union[str, Path]) -> Credentials:
    """Read the credentials from a file"""

    return json.loads(Path(str(credentials_path)).read_text())


class EESession:
    def __init__(
        self,
        sepal_headers: Optional[SepalHeaders] = None,
        ee_project: Optional[str] = None,
        credentials_path: Union[Path, str, None] = None,
        credentials_dict: Optional[Credentials] = None,
    ):
        """Session that handles two scenarios to set the headers for the Earth Engine API

        It can be initialized with the headers sent by SEPAL or with the credentials and project

        Args:
            ee_project (str): The project id of the Earth Engine project
            credentials (Union[PathLike, dict]): The credentials to use for the session
            sepal_headers (Union[SepalHeaders, None], optional): The headers sent by SEPAL. Defaults to None.
        """
        self._headers: Optional[GEEHeaders] = None

        self.project = ee_project
        self.credentials_path = credentials_path
        self.credentials_dict = credentials_dict

        self.sepal_headers = sepal_headers

        # If initializing from credentials and project, load credentials
        if not self.sepal_headers:
            if not ee_project or not credentials_path or not credentials_dict:
                raise ValueError(
                    "Both ee_project and credentials must be provided when from_headers is False."
                )

            credentials = credentials_dict or read_credentials(credentials_path)
            self.credentials = credentials

            self._headers = {
                "x-goog-user-project": ee_project,
                "Authorization": f"Bearer {self._get_access_token()}",
            }

        else:
            self._headers = self.get_headers()

    @property
    def headers(self) -> Optional[GEEHeaders]:
        return self._headers

    @headers.setter
    def headers(self, headers: Optional[SepalHeaders]) -> None:
        self.sepal_headers = headers
        if self.sepal_headers:
            self._headers = self.get_headers()

    def _set_url_project(self, url: str) -> str:
        """Set the project in the url"""
        return url.format(project=self.project)

    def is_expired(self, expiry_date: int) -> bool:
        """Returns if a token is about to expire"""
        return expiry_date - time.time() < 60

    def get_headers(self) -> GEEHeaders:
        """Set the headers from SEPAL"""

        if self.sepal_headers:
            google_tokens: GoogleTokens = self.sepal_headers["googleTokens"]

            username = self.sepal_headers["username"]
            expiry_date = google_tokens["accessTokenExpiryDate"]
            project_id = google_tokens["projectId"]

            if not self.is_expired(expiry_date):
                access_token = google_tokens["accessToken"]
            else:
                self.sepal_headers = self.refresh_sepal_google_tokens(username)
                return self.get_headers()

            return {
                "x-goog-user-project": project_id,
                "Authorization": f"Bearer {access_token}",
            }

        if self._headers:
            return self._headers
        else:
            raise ValueError("Headers are not set.")

    # def get_sepal_google_tokens(self, username: str) -> GoogleTokens:

    #     if self.sepal_headers:
    #         credentials_endpoint = f"/api/users/{username}/cr/edentials"
    #         # url = f"https://sepal.io{credentials_endpoint}"

    #         # with httpx.Client(headers=self.sepal_headers) as client:
    #         #     response = client.get(url)

    # data = response.json()
    # google_tokens = GoogleTokens(
    #     accessToken=data["accessToken"],
    #     refreshToken=data["refreshToken"],
    #     accessTokenExpiryDate=data["accessTokenExpiryDate"],
    #     REFRESH_IF_EXPIRES_IN_MINUTES=data["REFRESH_IF_EXPIRES_IN_MINUTES"],
    #     projectId=data["projectId"],
    #     legacyProject=data["legacyProject"],
    # )
    # return google_tokens

    def rest_call(
        self,
        method: Literal["GET", "POST"],
        url: str,
        data: Optional[dict] = None,  # type: ignore
    ):
        """Make a call to the Earth Engine REST API"""
        url = self._set_url_project(url)

        if self.headers:

            with httpx.Client(headers=self.headers) as client:
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

    def _get_access_token(self) -> str:
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

    def refresh_sepal_google_tokens(self, username: str) -> SepalHeaders:
        # Implement this method
        ...

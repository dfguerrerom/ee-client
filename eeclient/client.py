import json
import os
from pathlib import Path
import time
from typing import Any, Dict, Literal, Optional, Union, cast

import httpx
import requests
from requests.auth import HTTPBasicAuth
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
        test=False,
    ):
        """Session that handles two scenarios to set the headers for the Earth Engine API

        It can be initialized with the headers sent by SEPAL or with the credentials and project

        Args:
            ee_project (str): The project id of the Earth Engine project
            credentials (Union[PathLike, dict]): The credentials to use for the session

        """
        self._headers: Optional[GEEHeaders] = None
        self.test = test

        self.project_id = ee_project
        self.credentials_path = credentials_path
        self.credentials_dict = credentials_dict

        self.sepal_headers = sepal_headers

        if test:
            self._headers = self.get_headers()

        # If initializing from credentials and project, load credentials
        elif not self.sepal_headers:
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

        elif self.sepal_headers:
            self._headers = self.get_headers()

    @property
    def headers(self) -> Optional[GEEHeaders]:
        return self.get_headers()

    @headers.setter
    def headers(self, headers: Optional[SepalHeaders]) -> None:
        print("setting headers")
        self.sepal_headers = headers
        if self.sepal_headers:
            self._headers = self.get_headers()

    def _set_url_project(self, url: str) -> str:
        """Set the project in the url"""

        return url.format(project=self.project_id)

    def is_expired(self, expiry_date: int) -> bool:
        """Returns if a token is about to expire"""
        print("Checking if token is expired")

        return expiry_date - time.time() < 60

    def get_headers(self) -> GEEHeaders:
        """Set the headers from SEPAL"""

        if self.test and not self.sepal_headers:
            self.headers = self.get_fresh_sepal_headers()

        if self.sepal_headers:

            username = self.sepal_headers["username"]
            google_tokens: GoogleTokens = self.sepal_headers["googleTokens"]

            expiry_date = google_tokens["accessTokenExpiryDate"]
            self.project_id = google_tokens["projectId"]
            print("project_id", self.project_id)

            if not self.is_expired(expiry_date):
                access_token = google_tokens["accessToken"]
            else:
                print(
                    "Expired token... refreshing",
                )
                self.sepal_headers = self.get_fresh_sepal_headers()
                return self.get_headers()

            return {
                "x-goog-user-project": self.project_id,
                "Authorization": f"Bearer {access_token}",
            }

        if self._headers:
            return self._headers
        else:
            raise ValueError("Headers are not set.")

    def get_fresh_sepal_headers(self) -> SepalHeaders:
        """This is temporary until the sepal API is implemented"""

        sepal_password = os.getenv("SEPAL_PASSWORD", "")
        sepal_username = os.getenv("SEPAL_USER", "")

        auth = httpx.BasicAuth(username=sepal_username, password=sepal_password)

        credentials_file = f"https://sepal.io/api/user-files/download?path=%2F.config%2Fearthengine%2Fcredentials"

        with httpx.Client() as client:
            response = client.get(credentials_file, auth=auth)
            credentials = response.json()
            print(credentials)

            return {
                "id": 1,
                "username": "dguerrero",
                "googleTokens": {
                    "accessToken": credentials["access_token"],
                    "accessTokenExpiryDate": credentials["access_token_expiry_date"],
                    "projectId": credentials["project_id"],
                    "refreshToken": "",
                    "REFRESH_IF_EXPIRES_IN_MINUTES": 10,
                    "legacyProject": "",
                },
                "status": "ACTIVE",
                "roles": ["USER"],
                "systemUser": False,
                "admin": False,
            }

    def rest_call(
        self,
        method: Literal["GET", "POST"],
        url: str,
        data: Optional[Dict] = None,  # type: ignore
    ) -> Dict[str, Any]:
        """Make a call to the Earth Engine REST API"""

        url = self._set_url_project(url)
        print("url", url)

        if self.headers:

            # Explicitly cast `data` to `Optional[dict]`
            # data_json = cast(Optional[Dict[str, Any]], data)

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

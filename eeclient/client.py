import json
import os
from pathlib import Path
import time
from typing import Any, Dict, Literal, Optional, Union, cast

import httpx
import requests
from requests.auth import HTTPBasicAuth
from eeclient.exceptions import EERestException
from eeclient.typing import (
    Credentials,
    GEEHeaders,
    GoogleTokens,
    SepalHeaders,
    GEECredentials,
)


EARTH_ENGINE_API_URL = "https://earthengine.googleapis.com/v1alpha/"


class EESession:
    def __init__(
        self,
        sepal_user: Optional[str] = None,
        sepal_headers: Optional[SepalHeaders] = None,
        ee_project: Optional[str] = None,
        credentials_path: Union[Path, str, None] = None,
        credentials_dict: Optional[Credentials] = None,
        test=False,
        force_refresh=False,
    ):
        """Session that handles two scenarios to set the headers for the Earth Engine API

        It can be initialized with the headers sent by SEPAL or with the credentials and project

        Args:
            ee_project (str): The project id of the Earth Engine project
            credentials (Union[PathLike, dict]): The credentials to use for the session

        """
        self.credentials__path = credentials_path
        self.sepal_user = sepal_user
        self._headers: Optional[GEEHeaders] = None
        self.test = test
        self.force_refresh = force_refresh

        self.retry_count = 0
        self.max_retries = 3

        self.project_id = ee_project
        self.credentials_dict = credentials_dict

        self.sepal_headers = sepal_headers

        if test:
            self._headers = self.get_headers()

        if self.sepal_user:
            self.set_user_credentials_path(self.sepal_user)

        elif not self.sepal_headers:
            if not ee_project or not credentials_path or not credentials_dict:
                raise ValueError(
                    "Both ee_project and credentials must be provided when from_headers is False."
                )

        else:
            self._headers = self.get_headers()

    def set_user_credentials_path(self, sepal_user: str):
        """Read the credentials from an user"""

        users_path = Path("userHomes")
        user_path = users_path.joinpath(sepal_user)
        self.credentials_path = str(
            user_path.joinpath(".config/earthengine/credentials")
        )

    @staticmethod
    def read_credentials(credentials_path: Union[str, Path]) -> GEECredentials:
        """Read the credentials from a file"""

        return json.loads(Path(str(credentials_path)).read_text())

    @property
    def headers(self) -> Optional[GEEHeaders]:
        return self.get_headers()

    @headers.setter
    def headers(self, headers: Optional[SepalHeaders]) -> None:
        self.sepal_headers = headers
        if self.sepal_headers:
            self._headers = self.get_headers()

    def is_expired(self, expiry_date: int) -> bool:
        """Returns if a token is about to expire"""

        expired = expiry_date - time.time() < 60
        self.retry_count += 1 if expired else 0

        return expired

    def get_headers(self) -> GEEHeaders:
        """Set the headers from SEPAL"""

        if self.retry_count >= self.max_retries:
            raise ValueError("Maximum retry attempts reached.")

        if self.test and not self.sepal_headers:
            self.headers = self.get_fresh_sepal_headers()

        if self.sepal_headers:

            if self.force_refresh:
                self.sepal_headers = self.get_fresh_sepal_headers()

            username = self.sepal_headers["username"]

            google_tokens: GoogleTokens = self.sepal_headers["googleTokens"]

            self.project_id = google_tokens["projectId"]
            expiry_date = google_tokens["accessTokenExpiryDate"]

            if not self.is_expired(expiry_date):
                access_token = google_tokens["accessToken"]
            else:
                print("Expired token... refreshing")
                self.sepal_headers = self.get_fresh_sepal_headers()
                return self.get_headers()

        elif self.sepal_user:

            # We assume this file will be automatically updated by SEPAL
            credentials = self.read_credentials(self.credentials_path)
            project_id = credentials["project_id"]
            access_token = credentials["access_token"]
            expiry_date = credentials["access_token_expiry_date"]

            if not self.is_expired(expiry_date):
                access_token = credentials["access_token"]
            else:
                print("Expired token... refreshing")
                return self.get_headers()

            return {
                "x-goog-user-project": project_id,
                "Authorization": f"Bearer {access_token}",
                "Username": self.sepal_user,
            }

        if self._headers:
            return self._headers

        else:
            raise ValueError("Headers are not set.")

    def get_credentials_from_env(self) -> GEEHeaders:
        """Get the headers from the environment variables"""

        sepal_password = os.getenv("SEPAL_PASSWORD", "")
        sepal_username = os.getenv("SEPAL_USER", "")

        auth = httpx.BasicAuth(username=sepal_username, password=sepal_password)

        credentials_file = f"https://sepal.io/api/user-files/download?path=%2F.config%2Fearthengine%2Fcredentials"

        with httpx.Client() as client:
            response = client.get(credentials_file, auth=auth)
            credentials = response.json()
            return credentials

    def get_fresh_sepal_headers(self) -> SepalHeaders:
        """This is temporary until the sepal API is implemented"""

        # THIS METHOD IS JUST FOR TESTING PURPOSES

        sepal_password = os.getenv("SEPAL_PASSWORD", "")
        sepal_username = os.getenv("SEPAL_USER", "")

        auth = httpx.BasicAuth(username=sepal_username, password=sepal_password)

        credentials_file = f"https://sepal.io/api/user-files/download?path=%2F.config%2Fearthengine%2Fcredentials"

        with httpx.Client() as client:
            response = client.get(credentials_file, auth=auth)
            credentials = response.json()

            print("Fresh credentials", credentials)
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

        url = self.set_url_project(url)

        if self.headers:
            print("((((((((((((((((((((()))))))))))))))))))))", self.headers)

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

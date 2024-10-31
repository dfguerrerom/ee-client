import json
from pathlib import Path
from typing import Literal, Union
import httpx
from ee.oauth import CLIENT_ID, CLIENT_SECRET, SCOPES, get_credentials_path
from eeclient.aioclient import PathLike
from eeclient.exceptions import EERestException


class Session:
    def __init__(
        self,
        ee_project: str,
        credentials: Union[PathLike, dict],
        from_headers: bool = False,
    ):
        """Session

        Args:
            ee_project str
        """

        self.from_headers = from_headers
        self.project = ee_project
        self.credentials = None

        # If initializing from credentials and project, load credentials
        if not from_headers:
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
            self.client = httpx.Client(headers=self.headers)
        else:
            headers = self.get_headers()
            self.client = httpx.Client()

    def _set_url_project(self, url):
        """Set the project in the url"""

        return url.format(project=self.project)

    def get_headers():
        """Get the headers from gee and pass them"""

        return json.loads((Path(__file__).parent / "config.json").read_text())

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
        """Get the access token from the refresh token"""

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

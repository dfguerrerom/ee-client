from eeclient.models import SepalHeaders, GoogleTokens
import os
import logging
import json
import time
import asyncio
import httpx
import requests
from pathlib import Path
from typing import Optional

log = logging.getLogger("eeclient")


class SepalCredentialMixin:
    def __init__(self, sepal_headers: Optional[SepalHeaders] = None):
        log.debug("Initializing SepalCredentialMixin")

        self.max_retries = 3
        self._credentials = None
        self.auth_mode = "sepal" if sepal_headers else "file"

        if sepal_headers:
            self._init_sepal_mode(sepal_headers)
        else:
            self._init_file_mode()

        self.logger = logging.getLogger(f"eeclient.{self.user}")

        # For backward compatibility (some code might expect this attribute)
        self._service = None

    def _init_sepal_mode(self, sepal_headers: SepalHeaders):
        """Initialize SEPAL authentication mode"""
        self.sepal_host = os.getenv("SEPAL_HOST")
        if not self.sepal_host:
            raise ValueError("SEPAL_HOST environment variable not set")

        self.sepal_headers = SepalHeaders.model_validate(sepal_headers)
        self.sepal_session_id = self.sepal_headers.cookies["SEPAL-SESSIONID"]
        self.sepal_user_data = self.sepal_headers.sepal_user
        self.user = self.sepal_user_data.username

        self.sepal_api_download_url = (
            f"https://{self.sepal_host}/api/user-files/download/"
            "?path=%2F.config%2Fearthengine%2Fcredentials"
        )
        self.verify_ssl = not (
            self.sepal_host == "host.docker.internal"
            or self.sepal_host == "danielg.sepal.io"
        )

        self._google_tokens = self.sepal_user_data.google_tokens
        if self._google_tokens:
            self._credentials = self._google_tokens
            self.access_token = self._google_tokens.access_token
            self.project_id = self._google_tokens.project_id
            self.expiry_date = self._google_tokens.access_token_expiry_date
        else:
            self.access_token = None
            self.project_id = None
            self.expiry_date = 0

    def _init_file_mode(self):
        """Initialize file-based authentication mode"""
        home_path = Path.home()
        credentials_file = (
            ".config/earthengine/credentials"
            if "sepal-user" in home_path.name
            else ".config/earthengine/sepal_credentials"
        )
        self.credentials_path = home_path / credentials_file
        self.user = "local_user"

        # No SEPAL session info for file-based credentials
        self.sepal_session_id = None
        self.sepal_host = None
        self.sepal_api_download_url = None
        self.verify_ssl = True

        # Load initial credentials
        self._load_credentials_from_file()

    def _load_credentials_from_file(self):
        """Load credentials from file and update internal state"""
        if not self.credentials_path.exists():
            raise ValueError(f"Credentials file not found at {self.credentials_path}")

        try:
            credentials_data = json.loads(self.credentials_path.read_text())
            self._credentials = GoogleTokens.model_validate(credentials_data)
            self.access_token = self._credentials.access_token
            self.project_id = self._credentials.project_id
            self.expiry_date = self._credentials.access_token_expiry_date

            if not self.access_token:
                raise ValueError("No access token available in credentials file")

        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in credentials file: {e}")
        except Exception as e:
            raise ValueError(f"Error loading credentials from file: {e}")

    def is_expired(self) -> bool:
        """Returns if a token is about to expire"""
        return (self.expiry_date / 1000) - time.time() < 60

    def needs_credentials_refresh(self) -> bool:
        """Returns if credentials need to be refreshed (missing or expired)"""
        return not self._credentials or self.is_expired()

    async def set_credentials(self) -> None:
        """
        Refresh credentials based on the authentication mode.
        """
        if self.auth_mode == "sepal":
            await self._refresh_credentials_from_sepal()
        else:
            await self._refresh_credentials_from_file()

    async def _refresh_credentials_from_sepal(self) -> None:
        """Refresh credentials via SEPAL API"""
        if not self.sepal_session_id or not self.sepal_api_download_url:
            raise ValueError("SEPAL session information not available")

        self.logger.debug(
            "Token is expired or about to expire; "
            "attempting to refresh credentials from SEPAL."
        )
        attempt = 0
        credentials_url = self.sepal_api_download_url
        last_status = None

        # Prepare cookies for authentication
        sepal_cookies = httpx.Cookies()
        sepal_cookies.set("SEPAL-SESSIONID", self.sepal_session_id)

        while attempt < self.max_retries:
            attempt += 1
            try:
                async with httpx.AsyncClient(
                    cookies=sepal_cookies,
                    verify=self.verify_ssl,
                    limits=httpx.Limits(
                        max_connections=100, max_keepalive_connections=50
                    ),
                ) as client:
                    self.logger.debug(
                        f"Attempt {attempt} to refresh credentials from SEPAL."
                    )
                    response = await client.get(credentials_url)

                last_status = response.status_code

                if response.status_code == 200:
                    self._credentials = GoogleTokens.model_validate(response.json())
                    self.expiry_date = self._credentials.access_token_expiry_date
                    self.access_token = self._credentials.access_token
                    # Don't override project_id if enforce_project_id is set
                    if not hasattr(self, "enforce_project_id") or not getattr(
                        self, "enforce_project_id", False
                    ):
                        self.project_id = self._credentials.project_id
                    self.logger.debug(
                        f"Successfully refreshed credentials from SEPAL. "
                        f"Project: {self.project_id}"
                    )
                    return
                else:
                    self.logger.debug(
                        f"Attempt {attempt}/{self.max_retries} failed with "
                        f"status code: {response.status_code}."
                    )
            except Exception as e:
                self.logger.error(
                    f"Attempt {attempt}/{self.max_retries} "
                    f"encountered an exception: {e}"
                )
            await asyncio.sleep(2**attempt)  # Exponential backoff

        raise ValueError(
            f"Failed to retrieve credentials from SEPAL after "
            f"{self.max_retries} attempts, last status code: {last_status}"
        )

    async def _refresh_credentials_from_file(self) -> None:
        """Refresh credentials by re-reading from file"""
        self.logger.debug(
            "Token is expired or about to expire; "
            "attempting to refresh credentials from file."
        )
        attempt = 0

        while attempt < self.max_retries:
            attempt += 1
            try:
                self.logger.debug(
                    f"Attempt {attempt} to refresh credentials from file."
                )

                # Re-read credentials from file
                self._load_credentials_from_file()

                # Check if the new credentials are still expired
                if not self.is_expired():
                    self.logger.debug(
                        f"Successfully refreshed credentials from file. "
                        f"Project: {self.project_id}"
                    )
                    return
                else:
                    self.logger.debug(
                        f"Attempt {attempt}/{self.max_retries}: "
                        f"Credentials from file are still expired."
                    )

            except Exception as e:
                self.logger.error(
                    f"Attempt {attempt}/{self.max_retries} "
                    f"encountered an exception while reading file: {e}"
                )

            # Wait before retrying (the file might be updated externally)
            if attempt < self.max_retries:
                wait_time = 2**attempt
                self.logger.debug(f"Waiting {wait_time} seconds before retry...")
                await asyncio.sleep(wait_time)

        raise ValueError(
            f"Failed to retrieve valid credentials from file after "
            f"{self.max_retries} attempts. File may not be automatically "
            f"updated or credentials are permanently expired."
        )

    def set_credentials_sync(self) -> None:
        """
        Refresh credentials synchronously based on the authentication mode.
        """
        if self.auth_mode == "sepal":
            self._refresh_credentials_from_sepal_sync()
        else:
            self._refresh_credentials_from_file_sync()

    def _refresh_credentials_from_sepal_sync(self) -> None:
        """Refresh credentials via SEPAL API synchronously"""
        if not self.sepal_session_id or not self.sepal_api_download_url:
            raise ValueError("SEPAL session information not available")

        self.logger.debug(
            "Token is expired or about to expire; "
            "attempting to refresh credentials from SEPAL (sync)."
        )
        attempt = 0
        credentials_url = self.sepal_api_download_url
        last_status = None

        # Prepare session with cookies for authentication
        session = requests.Session()
        session.cookies.set("SEPAL-SESSIONID", self.sepal_session_id)
        session.verify = self.verify_ssl

        while attempt < self.max_retries:
            attempt += 1
            try:
                self.logger.debug(
                    f"Attempt {attempt} to refresh credentials from SEPAL (sync)."
                )
                response = session.get(credentials_url)
                last_status = response.status_code

                if response.status_code == 200:
                    self._credentials = GoogleTokens.model_validate(response.json())
                    self.expiry_date = self._credentials.access_token_expiry_date
                    self.access_token = self._credentials.access_token
                    # Don't override project_id if enforce_project_id is set
                    if not hasattr(self, "enforce_project_id") or not getattr(
                        self, "enforce_project_id", False
                    ):
                        self.project_id = self._credentials.project_id
                    self.logger.debug(
                        f"Successfully refreshed credentials from SEPAL (sync). "
                        f"Project: {self.project_id}"
                    )
                    return
                else:
                    self.logger.debug(
                        f"Attempt {attempt}/{self.max_retries} failed with "
                        f"status code: {response.status_code}."
                    )
            except Exception as e:
                self.logger.error(
                    f"Attempt {attempt}/{self.max_retries} "
                    f"encountered an exception: {e}"
                )

            # Wait before retrying (exponential backoff)
            if attempt < self.max_retries:
                wait_time = 2**attempt
                self.logger.debug(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)

        session.close()
        raise ValueError(
            f"Failed to retrieve credentials from SEPAL after "
            f"{self.max_retries} attempts, last status code: {last_status}"
        )

    def _refresh_credentials_from_file_sync(self) -> None:
        """Refresh credentials by re-reading from file synchronously"""
        self.logger.debug(
            "Token is expired or about to expire; "
            "attempting to refresh credentials from file (sync)."
        )
        attempt = 0

        while attempt < self.max_retries:
            attempt += 1
            try:
                self.logger.debug(
                    f"Attempt {attempt} to refresh credentials from file (sync)."
                )

                # Re-read credentials from file
                self._load_credentials_from_file()

                # Check if the new credentials are still expired
                if not self.is_expired():
                    self.logger.debug(
                        f"Successfully refreshed credentials from file (sync). "
                        f"Project: {self.project_id}"
                    )
                    return
                else:
                    self.logger.debug(
                        f"Attempt {attempt}/{self.max_retries}: "
                        f"Credentials from file are still expired."
                    )

            except Exception as e:
                self.logger.error(
                    f"Attempt {attempt}/{self.max_retries} "
                    f"encountered an exception while reading file: {e}"
                )

            # Wait before retrying (the file might be updated externally)
            if attempt < self.max_retries:
                wait_time = 2**attempt
                self.logger.debug(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)

        raise ValueError(
            f"Failed to retrieve valid credentials from file after "
            f"{self.max_retries} attempts. File may not be automatically "
            f"updated or credentials are permanently expired."
        )

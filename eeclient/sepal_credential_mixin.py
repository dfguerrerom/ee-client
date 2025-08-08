from eeclient.models import SepalHeaders
import os
import logging

log = logging.getLogger("eeclient")


class SepalCredentialMixin:
    def __init__(self, sepal_headers: SepalHeaders):
        log.debug("Initializing GDriveInterface with SEPAL headers")
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
            self.access_token = self._google_tokens.access_token
            self.project_id = self._google_tokens.project_id
            self.expiry_date = self._google_tokens.access_token_expiry_date
        else:
            self.access_token = None
            self.project_id = None
            self.expiry_date = 0

import json
from typing import Union
import requests
from pathlib import Path
from ee.oauth import CLIENT_ID, CLIENT_SECRET, SCOPES, get_credentials_path

from aiogoogle import Aiogoogle, GoogleAPI


class PathLike:
    pass


class AioEE(Aiogoogle):

    def __init__(self, credentials: Union[PathLike, dict] = None):

        if isinstance(credentials, (str, Path)) or not credentials:
            credentials_path = credentials or get_credentials_path()
            credentials = json.loads(Path(credentials_path).read_text())

        client_creds = {
            "client_id": credentials.get("client_id", CLIENT_ID),
            "client_secret": credentials.get("client_secret", CLIENT_SECRET),
            "scopes": credentials.get("scopes", SCOPES),
        }

        user_creds = {
            "refresh_token": credentials.get("refresh_token"),
        }

        super().__init__(user_creds=user_creds, client_creds=client_creds)

        discovery_url = "https://earthengine.googleapis.com/$discovery/rest?version=v1"
        discovery_document = requests.get(discovery_url).json()
        self.ee_api = GoogleAPI(discovery_document, validate=True)

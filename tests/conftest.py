import os
import uuid

import pytest
import requests

from eeclient.typing import GoogleTokens, SepalHeaders, SepalUser
from eeclient.logger import logger


@pytest.fixture(scope="session")
def sepal_headers():
    return _get_sepal_headers()


def _get_sepal_headers():
    sepal_user = os.getenv("LOCAL_SEPAL_USER")
    sepal_password = os.getenv("LOCAL_SEPAL_PASSWORD")
    sepal_host = os.getenv("SEPAL_HOST")
    if not sepal_user or not sepal_password:
        raise ValueError("SEPAL_USER and SEPAL_PASSWORD must be set")

    # do the request with a basic auth
    response = requests.get(
        f"https://{sepal_host}/api/user-files/download/?path=%2F.config%2Fearthengine%2Fcredentials",
        auth=(sepal_user, sepal_password),
        verify=False,
    )
    logger.debug(f"Initializing session with headers: {response.cookies}")
    sepal_user = SepalUser(
        id=1,
        username=sepal_user,
        google_tokens=GoogleTokens.model_validate(response.json()),
        status="active",
        roles=["USER"],
        system_user=False,
        admin=False,
    )

    # replace the project_id with the one from the environment
    sepal_user.google_tokens.project_id = "sepal-ui-421413"

    sepal_headers = {
        "cookie": dict(response.cookies),
        "sepal-user": sepal_user,
    }

    return SepalHeaders.model_validate(sepal_headers)


@pytest.fixture()
def dummy_headers():
    return {
        "cookie": ["SEPAL-SESSIONID=s:random;"],
        "sepal-user": [
            '{"id":10001,"username":"admin","googleTokens":{"accessToken":"test_token","refreshToken":"test_refresh","accessTokenExpiryDate":1,"projectId":"ee-project","legacyProject":false},"status":"ACTIVE","roles":["application_admin"],"systemUser":false,"admin":true}'
        ],
    }


@pytest.fixture()
def dummy_headers_no_project_id():
    return {
        "cookie": ["SEPAL-SESSIONID=s:random;"],
        "sepal-user": [
            '{"id":10001,"username":"admin","googleTokens":{"accessToken":"test_token","refreshToken":"test_refresh","accessTokenExpiryDate":1,"projectId":"","legacyProject":false},"status":"ACTIVE","roles":["application_admin"],"systemUser":false,"admin":true}'
        ],
    }


@pytest.fixture()
def dummy_headers_no_google_tokens():
    return {
        "cookie": ["SEPAL-SESSIONID=s:random;"],
        "sepal-user": [
            '{"id":10001,"username":"admin","googleTokens":null,"status":"ACTIVE","roles":["application_admin"],"systemUser":false,"admin":true}'
        ],
    }


@pytest.fixture(scope="session")
def hash() -> str:
    """Create a hash for each test instance.

    Returns:
        the hash string
    """
    return uuid.uuid4().hex[:6]


if __name__ == "__main__":
    print(_get_sepal_headers())

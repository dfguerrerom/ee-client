import os
import uuid

import pytest
import requests

from eeclient.helpers import get_sepal_headers_from_auth
from eeclient.models import SepalHeaders, SepalUser
from eeclient.logger import logger


@pytest.fixture(scope="session")
def sepal_headers():
    return get_sepal_headers_from_auth()


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

import sys


import pytest

from eeclient.exceptions import EEClientError
from eeclient.models import SepalHeaders

sys.path.append("..")


def test_headers(dummy_headers):

    sepal_headers = SepalHeaders.model_validate(dummy_headers)
    assert sepal_headers.cookies == {"SEPAL-SESSIONID": "s:random"}
    assert sepal_headers.sepal_user.id == 10001
    assert sepal_headers.sepal_user.username == "admin"
    assert sepal_headers.sepal_user.google_tokens
    assert sepal_headers.sepal_user.google_tokens.access_token
    assert sepal_headers.sepal_user.google_tokens.refresh_token
    assert sepal_headers.sepal_user.google_tokens.project_id == "ee-project"


def test_without_sepal_user(dummy_headers_no_project_id):

    with pytest.raises(EEClientError):
        SepalHeaders.model_validate(dummy_headers_no_project_id)

import sys


from eeclient.client import EESession

sys.path.append("..")


def test_init_client(sepal_headers):

    sepal_session = EESession(sepal_headers=sepal_headers)
    assert sepal_session
    assert sepal_session.project_id == sepal_headers.sepal_user.google_tokens.project_id


def test_is_expired(sepal_headers, dummy_headers):

    sepal_session = EESession(sepal_headers=sepal_headers)
    assert sepal_session.is_expired() == False

    sepal_session = EESession(sepal_headers=dummy_headers)
    assert sepal_session.is_expired() == True

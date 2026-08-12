import json
from pathlib import Path
from unittest import mock

import pytest

from eeclient.exceptions import EEClientError
from eeclient.providers import GoogleAuthProvider, resolve_default_provider


@pytest.fixture
def home(tmp_path, monkeypatch):
    """A non-SEPAL home with an empty ~/.config/earthengine and no token.

    Two knobs, because resolution uses two: ``$HOME`` drives
    ``ee.oauth.get_credentials_path()`` (via ``os.path.expanduser``), which is
    the file that gets classified and loaded; ``Path.home()`` drives the SEPAL
    context check. Patch only one and a test silently reads the real home.
    """
    monkeypatch.setattr("eeclient.providers.Path.home", lambda: tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path))
    (tmp_path / ".config/earthengine").mkdir(parents=True)
    monkeypatch.delenv("EARTHENGINE_TOKEN", raising=False)
    return tmp_path


def test_non_sepal_earthengine_token_selected(home, monkeypatch):
    monkeypatch.setenv(
        "EARTHENGINE_TOKEN", json.dumps({"refresh_token": "rt", "project": "p"})
    )
    prov = resolve_default_provider()
    assert isinstance(prov, GoogleAuthProvider) and prov.auth_mode == "oauth"


def test_non_sepal_no_source_raises_with_adc_hint(home):
    with pytest.raises(EEClientError) as e:
        resolve_default_provider()
    assert "from_application_default" in str(e.value)


def test_no_implicit_adc_probe(home):
    # google.auth.default must never be called by the default resolver (D10).
    with mock.patch(
        "eeclient.providers.google.auth.default",
        side_effect=AssertionError("probed!"),
    ):
        with pytest.raises(EEClientError):
            resolve_default_provider()


def test_sepal_context_fail_closed(home, monkeypatch):
    # A sepal-user home + broken SEPAL file + EARTHENGINE_TOKEN present:
    # must raise (fail closed), NOT use the machine token.
    sepal_home = Path(str(home) + "-sepal-user")
    (sepal_home / ".config/earthengine").mkdir(parents=True)
    monkeypatch.setattr("eeclient.providers.Path.home", lambda: sepal_home)
    monkeypatch.setenv(
        "EARTHENGINE_TOKEN", json.dumps({"refresh_token": "rt", "project": "p"})
    )
    with pytest.raises(EEClientError):
        resolve_default_provider()


def test_sepal_host_alone_does_not_gate(home, monkeypatch):
    # SEPAL_HOST set but non-sepal-user home => still agnostic (D11).
    monkeypatch.setenv("SEPAL_HOST", "sepal.example.org")
    monkeypatch.setenv(
        "EARTHENGINE_TOKEN", json.dumps({"refresh_token": "rt", "project": "p"})
    )
    prov = resolve_default_provider()
    assert isinstance(prov, GoogleAuthProvider)


def _write_sepal_artifact(home):
    (home / ".config/earthengine/sepal_credentials").write_text(
        '{"accessToken":"stale","accessTokenExpiryDate":1,"projectId":"p"}'
    )


def test_leftover_sepal_credentials_does_not_shadow_live_source(home, monkeypatch):
    # A hand-created sepal_credentials artifact must NOT be picked over a live source.
    _write_sepal_artifact(home)
    monkeypatch.setenv(
        "EARTHENGINE_TOKEN", json.dumps({"refresh_token": "rt", "project": "p"})
    )
    prov = resolve_default_provider()
    assert isinstance(prov, GoogleAuthProvider)  # the token wins, not the SEPAL file


def test_leftover_sepal_credentials_is_never_selected(home):
    # Only the hand-created artifact present + no live source -> raise (not select it).
    _write_sepal_artifact(home)
    with pytest.raises(EEClientError):
        resolve_default_provider()


def test_from_default_auth_source_is_earthengine_token(home, monkeypatch):
    monkeypatch.setenv(
        "EARTHENGINE_TOKEN", json.dumps({"refresh_token": "rt", "project": "p"})
    )
    assert resolve_default_provider().auth_source == "earthengine_token"


def _write_credentials(home, payload: dict) -> Path:
    path = home / ".config/earthengine/credentials"
    path.write_text(json.dumps(payload))
    return path


class _ResolverStub:
    auth_mode = "stub"
    auth_source = "stub"
    user = "tester"
    verify_ssl = True

    def initial_snapshot(self):
        return None

    def refresh_sync(self):
        raise AssertionError("not used")

    async def refresh(self):
        raise AssertionError("not used")


def test_service_account_credentials_file_is_refused_by_default(home):
    from eeclient.exceptions import ServiceAccountFileRefusedError

    _write_credentials(home, {"type": "service_account", "project_id": "p"})
    with mock.patch("eeclient.providers._service_account_credentials") as sa:
        with pytest.raises(ServiceAccountFileRefusedError) as e:
            resolve_default_provider()
    sa.assert_not_called()
    assert "allow_service_account_file" in str(e.value)


def test_service_account_file_wins_classification_over_a_refresh_token(home):
    # A file carrying both markers must still be classified/refused as SA —
    # the type check must run before the refresh_token check, not after.
    from eeclient.exceptions import ServiceAccountFileRefusedError

    _write_credentials(
        home, {"type": "service_account", "project_id": "p", "refresh_token": "rt"}
    )
    with mock.patch("eeclient.providers._service_account_credentials") as sa:
        with pytest.raises(ServiceAccountFileRefusedError):
            resolve_default_provider()
    sa.assert_not_called()


def test_service_account_credentials_file_opt_in_builds_that_provider(home):
    _write_credentials(home, {"type": "service_account", "project_id": "p"})
    with mock.patch(
        "eeclient.providers._service_account_credentials",
        return_value=(object(), "p"),
    ):
        prov = resolve_default_provider(allow_service_account_file=True)
    assert isinstance(prov, GoogleAuthProvider)
    assert prov.auth_mode == "service_account"
    assert prov.auth_source == "ee_service_account_file"


def test_unparsable_credentials_file_raises_unrecognized(home):
    from eeclient.exceptions import CredentialsFileUnrecognizedError

    (home / ".config/earthengine/credentials").write_text("not json at all")
    with pytest.raises(CredentialsFileUnrecognizedError):
        resolve_default_provider()


def test_binary_credentials_file_raises_unrecognized(home):
    # Not valid UTF-8, so read_text() raises UnicodeDecodeError (a ValueError,
    # not a JSONDecodeError) — must still classify as "unknown", not escape.
    from eeclient.exceptions import CredentialsFileUnrecognizedError

    (home / ".config/earthengine/credentials").write_bytes(b"\xff\xfe\x00\x01\x80")
    with pytest.raises(CredentialsFileUnrecognizedError):
        resolve_default_provider()


def test_credentials_file_without_refresh_token_raises_unrecognized(home):
    from eeclient.exceptions import CredentialsFileUnrecognizedError

    _write_credentials(home, {"client_id": "cid"})
    with pytest.raises(CredentialsFileUnrecognizedError):
        resolve_default_provider()


def test_unrecognized_file_error_is_a_resolution_error_and_names_the_path(home):
    # `except CredentialsResolutionError` around from_default() must cover every
    # way resolution can fail, and the message must not mention SEPAL.
    from eeclient.exceptions import (
        CredentialsFileUnrecognizedError,
        CredentialsResolutionError,
    )

    path = _write_credentials(home, {"client_id": "cid"})
    assert issubclass(CredentialsFileUnrecognizedError, CredentialsResolutionError)
    with pytest.raises(CredentialsResolutionError) as e:
        resolve_default_provider()
    assert str(path) in str(e.value)
    assert "SEPAL" not in str(e.value)


def test_unreadable_credentials_file_surfaces_the_os_error(home):
    # A file that exists but cannot be read is an environment problem, not a
    # malformed credential — don't launder PermissionError into "unrecognized".
    from eeclient.exceptions import (
        CredentialsFileUnrecognizedError,
        CredentialsResolutionError,
    )

    path = _write_credentials(home, {"refresh_token": "rt"})
    denied = PermissionError(13, "Permission denied")
    with mock.patch.object(Path, "read_text", side_effect=denied):
        with pytest.raises(CredentialsResolutionError) as e:
            resolve_default_provider()
    assert not isinstance(e.value, CredentialsFileUnrecognizedError)
    assert isinstance(e.value.__cause__, PermissionError)
    assert str(path) in str(e.value)


def test_oauth_credentials_file_still_resolves(home):
    _write_credentials(home, {"refresh_token": "rt", "project": "p"})
    prov = resolve_default_provider()
    assert isinstance(prov, GoogleAuthProvider)
    assert prov.auth_source == "ee_oauth_file"


def test_from_default_forwards_the_service_account_opt_in():
    from eeclient.client import EESession

    with mock.patch("eeclient.providers.resolve_default_provider") as resolver:
        resolver.return_value = _ResolverStub()
        EESession.from_default(allow_service_account_file=True)
    assert resolver.call_args.kwargs == {"allow_service_account_file": True}


def test_from_default_refuses_a_service_account_file_by_default(home):
    from eeclient.client import EESession
    from eeclient.exceptions import ServiceAccountFileRefusedError

    _write_credentials(home, {"type": "service_account", "project_id": "p"})
    with pytest.raises(ServiceAccountFileRefusedError):
        EESession.from_default()

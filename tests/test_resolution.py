import json
from pathlib import Path
from unittest import mock

import pytest

from eeclient.exceptions import EEClientError
from eeclient.providers import GoogleAuthProvider, resolve_default_provider


@pytest.fixture
def home(tmp_path, monkeypatch):
    """A non-SEPAL home with an empty ~/.config/earthengine and no token."""
    monkeypatch.setattr("eeclient.providers.Path.home", lambda: tmp_path)
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

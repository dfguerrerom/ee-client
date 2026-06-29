"""TLS verification policy for ee-client.

Security regression guard: TLS must be verified by default for every host,
including the maintainer's former personal dev host. Verification may only be
skipped for known local-dev hosts (self-signed) or via an explicit opt-in env
var — never for arbitrary or production hosts.
"""

import pytest

from eeclient.helpers import should_verify_tls

INSECURE_ENV = "EECLIENT_INSECURE_TLS"


@pytest.fixture(autouse=True)
def _clear_insecure_env(monkeypatch):
    monkeypatch.delenv(INSECURE_ENV, raising=False)


@pytest.mark.parametrize(
    "host",
    [
        "sepal.io",
        "danielg.sepal.io",  # former hardcoded insecure host — must now verify
        "danielg.sepal.io.attacker.com",  # must not match via substring
        "anything.example.com",
        None,  # file-based auth (no host)
    ],
)
def test_verifies_by_default(host):
    assert should_verify_tls(host) is True


def test_local_docker_host_is_skipped():
    # Local Docker host serves a self-signed cert and cannot be meaningfully
    # MITM'd; skipping verification there is the one defensible exception.
    assert should_verify_tls("host.docker.internal") is False


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "Yes"])
def test_explicit_opt_in_disables_verification(monkeypatch, value):
    monkeypatch.setenv(INSECURE_ENV, value)
    assert should_verify_tls("danielg.sepal.io") is False
    assert should_verify_tls("sepal.io") is False


@pytest.mark.parametrize("value", ["", "0", "false", "no", "off"])
def test_non_truthy_opt_in_keeps_verification(monkeypatch, value):
    monkeypatch.setenv(INSECURE_ENV, value)
    assert should_verify_tls("sepal.io") is True

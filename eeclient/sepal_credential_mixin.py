"""Backward-compatibility shim.

The mixin moved to :mod:`eeclient.credential_mixin` (SEPAL is now one provider
among several). ``SepalCredentialMixin`` remains importable here as an alias of
``CredentialMixin`` for existing consumers.
"""

from eeclient.credential_mixin import (  # noqa: F401
    CredentialMixin,
    SepalCredentialMixin,
)

"""``rest_call(raw=True)`` hands back the response body undecoded.

``image:computePixels`` answers with an encoded image, not a document, so the
JSON decode at the end of ``rest_call`` has to be skippable. These tests drive
a real local HTTP server rather than a mock, so the bytes make the whole round
trip through httpx exactly as they would against Earth Engine.
"""

import asyncio
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from eeclient.client import EESession
from eeclient.exceptions import EERestException
from eeclient.providers import CredentialSnapshot

# a TIFF magic number followed by bytes that are not valid UTF-8, so a decode
# attempt anywhere along the way would fail loudly instead of silently passing
TIFF_BYTES = b"II*\x00" + bytes(range(256))


class _StubProvider:
    """Credentials that never expire and never touch the network."""

    auth_mode = "stub"
    auth_source = "stub"
    user = "tester"
    verify_ssl = True

    def initial_snapshot(self):
        return CredentialSnapshot(
            access_token="token",
            project_id="ee-project",
            expiry_date=int((time.time() + 3600) * 1000),
            native=object(),
        )

    def refresh_sync(self):
        raise AssertionError("credentials must not be refreshed in these tests")

    async def refresh(self):
        raise AssertionError("credentials must not be refreshed in these tests")


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        self.rfile.read(length)

        if self.path.endswith("/boom"):
            body = json.dumps({"error": {"code": 400, "message": "bad grid"}}).encode()
            self.send_response(400)
            self.send_header("Content-Type", "application/json")
        else:
            body = TIFF_BYTES
            self.send_response(200)
            self.send_header("Content-Type", "image/tiff")

        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@pytest.fixture(scope="module")
def base_url():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


@pytest.fixture()
def session():
    return EESession(_provider=_StubProvider())


def test_raw_returns_the_body_verbatim(session, base_url):
    result = asyncio.run(session.rest_call("POST", f"{base_url}/pixels", raw=True))

    assert isinstance(result, bytes)
    assert result == TIFF_BYTES


def test_without_raw_the_same_body_is_rejected_as_json(session, base_url):
    """The guard that makes `raw` necessary: bytes are not a document."""
    with pytest.raises(EERestException) as excinfo:
        asyncio.run(session.rest_call("POST", f"{base_url}/pixels"))

    assert "Invalid JSON response" in str(excinfo.value)


def test_raw_still_raises_on_an_error_response(session, base_url):
    """A failure is JSON even when success is not, and must not reach the caller
    as bytes."""
    with pytest.raises(EERestException) as excinfo:
        asyncio.run(session.rest_call("POST", f"{base_url}/boom", raw=True))

    assert excinfo.value.code == 400

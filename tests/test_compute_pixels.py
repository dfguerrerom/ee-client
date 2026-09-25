from unittest.mock import AsyncMock, patch

import pytest

from eeclient.data import compute_pixels_async

FAKE_EXPRESSION = {"values": {}, "result": "0"}
GRID = {
    "dimensions": {"width": 256, "height": 256},
    "affineTransform": {
        "scaleX": 0.00025,
        "shearX": 0,
        "translateX": 16.0,
        "shearY": 0,
        "scaleY": -0.00025,
        "translateY": 1.4,
    },
    "crsCode": "EPSG:4326",
}


def _client(payload=b"\x49\x49\x2a\x00tiff bytes"):
    client = AsyncMock()
    client.rest_call = AsyncMock(return_value=payload)
    return client


async def _compute(client, **kwargs):
    with patch("eeclient.data.serializer.encode", return_value=FAKE_EXPRESSION):
        return await compute_pixels_async(client=client, ee_image=object(), **kwargs)


@pytest.mark.asyncio
async def test_returns_the_bytes_the_service_sent():
    client = _client()

    result = await _compute(client, grid=GRID, bands=["lossyear"])

    assert result == b"\x49\x49\x2a\x00tiff bytes"


@pytest.mark.asyncio
async def test_posts_to_the_compute_pixels_endpoint_asking_for_raw_bytes():
    client = _client()

    await _compute(client, grid=GRID, bands=["lossyear"])

    assert client.rest_call.await_count == 1
    args, kwargs = client.rest_call.call_args
    assert args[0] == "POST"
    assert args[1].endswith("/image:computePixels")
    # the response is an image, so it must not be decoded as JSON
    assert kwargs["raw"] is True


@pytest.mark.asyncio
async def test_sends_expression_grid_bands_and_format():
    client = _client()

    await _compute(client, grid=GRID, bands=["lossyear"])

    payload = client.rest_call.call_args.kwargs["data"]
    assert payload["expression"] == FAKE_EXPRESSION
    assert payload["fileFormat"] == "GEO_TIFF"
    assert payload["grid"] == GRID
    assert payload["bandIds"] == ["lossyear"]


@pytest.mark.asyncio
async def test_omits_the_optional_fields_when_they_are_not_given():
    client = _client()

    await _compute(client)

    payload = client.rest_call.call_args.kwargs["data"]
    assert set(payload) == {"expression", "fileFormat"}


@pytest.mark.asyncio
async def test_passes_visualization_options_and_workload_tag_through():
    client = _client()
    vis = {"ranges": [{"min": 0, "max": 23}]}

    await _compute(client, visualization_options=vis, workload_tag="sbae")

    payload = client.rest_call.call_args.kwargs["data"]
    assert payload["visualizationOptions"] == vis
    assert payload["workloadTag"] == "sbae"


@pytest.mark.asyncio
async def test_a_single_band_string_is_accepted_as_a_band_list():
    client = _client()

    await _compute(client, bands="lossyear")

    assert client.rest_call.call_args.kwargs["data"]["bandIds"] == ["lossyear"]

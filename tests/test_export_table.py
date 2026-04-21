from unittest.mock import AsyncMock, patch

import pytest

from eeclient.export.table import (
    AssetOptions,
    DriveDestination,
    DriveOptions,
    EarthEngineDestination,
    ExportOptions,
    TableFileFormat,
    table_to_asset_async,
)


def test_asset_export_options_serialize_with_correct_key():
    opts = ExportOptions(
        expression={"x": 1},
        asset_export_options=AssetOptions(
            earth_engine_destination=EarthEngineDestination(name="projects/p/assets/t")
        ),
    )
    payload = opts.model_dump(by_alias=True, exclude_none=True)
    assert "assetExportOptions" in payload
    assert "driveExportOptions" not in payload
    assert payload["assetExportOptions"]["earthEngineDestination"]["name"] == (
        "projects/p/assets/t"
    )


def test_drive_export_options_serialize_as_file_export_options():
    opts = ExportOptions(
        expression={"x": 1},
        file_export_options=DriveOptions(
            file_format=TableFileFormat.CSV,
            drive_destination=DriveDestination(filename_prefix="out"),
        ),
    )
    payload = opts.model_dump(by_alias=True, exclude_none=True)
    assert "fileExportOptions" in payload
    assert "driveExportOptions" not in payload
    assert "assetExportOptions" not in payload


@pytest.mark.asyncio
async def test_table_to_asset_async_sends_asset_export_options():
    client = AsyncMock()
    client.rest_call = AsyncMock(
        return_value={
            "name": "projects/p/operations/X",
            "metadata": {
                "@type": "type.googleapis.com/google.earthengine.v1.OperationMetadata",
                "description": "myExportTableTask",
                "priority": 100,
                "createTime": "2026-04-20T00:00:00Z",
                "type": "EXPORT_FEATURES",
            },
            "done": False,
        }
    )

    fake_expression = {"values": {}, "result": "0"}
    with patch(
        "eeclient.export.table.serializer.encode", return_value=fake_expression
    ), patch("eeclient.export.table.encodable.Encodable", object):
        await table_to_asset_async(
            client=client,
            collection=object(),
            asset_id="projects/p/assets/t",
        )

    assert client.rest_call.await_count == 1
    _, kwargs = client.rest_call.call_args
    payload = kwargs["data"]
    assert "assetExportOptions" in payload
    assert "driveExportOptions" not in payload
    assert payload["assetExportOptions"]["earthEngineDestination"]["name"] == (
        "projects/p/assets/t"
    )

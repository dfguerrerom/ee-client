from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from eeclient.export.image import (
    DriveDestination,
    DriveOptions,
    ImageFileFormat,
    _export_image,
)


def test_drive_options_accepts_legacy_geotiff_alias():
    opts = DriveOptions(
        file_format="GeoTIFF",
        drive_destination=DriveDestination(filename_prefix="out"),
    )
    assert opts.file_format == ImageFileFormat.GEO_TIFF


def test_drive_options_accepts_canonical_value():
    opts = DriveOptions(
        file_format="GEO_TIFF",
        drive_destination=DriveDestination(filename_prefix="out"),
    )
    assert opts.file_format == ImageFileFormat.GEO_TIFF


def test_drive_options_rejects_unknown_string():
    with pytest.raises(ValueError):
        DriveOptions(
            file_format="NotAFormat",
            drive_destination=DriveDestination(filename_prefix="out"),
        )


@pytest.mark.asyncio
async def test_export_image_uses_clipped_image_from_selection_and_scale():
    """Regression test for issue #20.

    `ee.Image._apply_selection_and_scale` returns a new image wrapped in
    `clipToBoundsAndScale(geometry=region)`. The export flow must serialize
    that clipped image, not the original, or GEE rejects the task as
    "Unable to export unbounded image".
    """
    original_image = MagicMock(name="original_image")
    crs_applied_image = MagicMock(name="crs_applied_image")
    clipped_image = MagicMock(name="clipped_image")

    original_image._apply_crs_and_affine.return_value = (
        crs_applied_image,
        {"region": "geom", "scale": 30},
        False,
    )
    crs_applied_image._apply_selection_and_scale.return_value = (
        clipped_image,
        {},
    )

    client = MagicMock()
    client.rest_call = AsyncMock(
        return_value={"name": "projects/p/operations/op", "metadata": {}}
    )

    drive_options = DriveOptions(
        file_format=ImageFileFormat.GEO_TIFF,
        drive_destination=DriveDestination(filename_prefix="out"),
    )

    with (
        patch("eeclient.export.image.serializer.encode") as mock_encode,
        patch("eeclient.export.image.Task.model_validate") as mock_validate,
    ):
        mock_encode.return_value = {"result": "encoded"}
        mock_validate.return_value = MagicMock()

        await _export_image(
            client=client,
            image=original_image,
            drive_options=drive_options,
            region="geom",
            scale=30,
        )

    # The serialized expression must be the clipped image, not the original
    # or the intermediate crs-applied image.
    mock_encode.assert_called_once()
    encoded_image = mock_encode.call_args.args[0]
    assert encoded_image is clipped_image
